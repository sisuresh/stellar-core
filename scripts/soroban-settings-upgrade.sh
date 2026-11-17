#!/usr/bin/env bash
#
# End-to-end Soroban settings upgrade for futurenet or testnet.
#
# This is `settings-helper.sh` with the manual steps automated: it looks up the
# sequence number from the network's RPC (rather than Horizon), generates the
# four settings-upgrade transactions with `stellar-core get-settings-upgrade-txs`,
# submits each one to the RPC and waits for it to be applied. On futurenet it
# then verifies and schedules the upgrade on each validator over `kubectl exec`.
# On testnet, and with --no-schedule, it stops after submitting and prints the
# ConfigUpgradeSetKey for the validator operators to arm.
#
# By default it generates a fresh source account and funds it from friendbot on
# every run, so nothing needs to be set up after a network reset. Pass
# --secret to use an existing account instead.
#
# Several settings files can be given when scheduling. Each is run to
# completion -- submitted, armed, and confirmed live on every validator --
# before the next one starts, so a phase that depends on the previous one's
# limits is built against them. With --no-schedule, each phase but the last
# waits (polling the RPC) for someone else to arm it and for it to take effect.
#
# Usage:
#   scripts/soroban-settings-upgrade.sh <settings.json> [more.json ...] [options]
#   scripts/soroban-settings-upgrade.sh --schedule-only <ConfigUpgradeSetKey> [options]
#
# The whole futurenet reset flow, in one command:
# ./scripts/soroban-settings-upgrade.sh \
# soroban-settings/testnet_settings_enable_upgrades.json \
# soroban-settings/testnet_settings_upgrade.json \
# --core-bin src/stellar-core --yes
#
# Submit only, e.g. from CI:
# ./scripts/soroban-settings-upgrade.sh --network testnet --no-schedule \
# soroban-settings/testnet_settings_upgrade.json --core-bin src/stellar-core
#
# Requires: stellar-core (built), stellar-xdr, curl, jq; to schedule, also
# kubectl and a running sshuttle

set -euo pipefail

NETWORK="futurenet"
RPC_URL="${RPC_URL:-}"
PASSPHRASE="${PASSPHRASE:-}"
FRIENDBOT_URL="${FRIENDBOT_URL:-}"

NAMESPACE="futurenet-validators"
PODS="futurenet-validator-core-0 futurenet-validator-core-1 futurenet-validator-core-2"
CONTAINER="core"
HTTP_PORT="11626"

# The validators live in dev-eks, which is only reachable through the sshuttle
# tunnel. Leave KUBE_CONTEXT empty to auto-pick the dev-eks/* context.
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBE_TIMEOUT="20s"

RESOURCE_FEE="100"
UPGRADE_IN="90s"
UPGRADE_TIME=""
CORE_BIN=""
SCHEDULE_ONLY_KEY=""
ASSUME_YES="false"
DRY_RUN="false"
NO_SCHEDULE="false"

RPC_TIMEOUT="30"
TX_WAIT_SECONDS="120"
ACCOUNT_WAIT_SECONDS="60"
APPLY_WAIT_SECONDS="300"
POLL_INTERVAL="2"

SECRET_KEY=""
PUBLIC_KEY=""
SETTINGS_FILES=""
ARMED_AT=""

usage()
{
    # Print the header comment block, minus the shebang.
    awk 'NR < 3 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "$0"
    cat <<'EOF'

Options:
  --network NAME          futurenet (default) or testnet. Sets the RPC,
                          passphrase and friendbot defaults. Scheduling is only
                          supported on futurenet.
  --secret SK            Use this source account instead of generating and
                          funding a fresh one
  --resource-fee N        Additional resource fee added to every tx (default: 100)
  --upgrade-in DURATION   Schedule this far out, as 90s / 5m / a bare number of
                          minutes (default: 90s). The clock starts when you
                          answer the confirmation prompt, not before it.
  --upgrade-time TIME     Explicit upgrade time, e.g. 2026-08-12T01:25:00Z
  --schedule-only KEY     Skip tx generation/submission; schedule an already
                          submitted ConfigUpgradeSetKey on the validators
  --no-schedule           Submit each phase's transactions but arm nothing
  --context NAME          kube context (default: the dev-eks/* one in your kubeconfig)
  --namespace NS          Kubernetes namespace (default: futurenet-validators)
  --pods "P1 P2 P3"       Validator pods (default: futurenet-validator-core-{0,1,2})
  --container NAME        Container within the pod (default: pod's default)
  --core-bin PATH         stellar-core binary (default: ./stellar-core, src/stellar-core, or $PATH)
  --rpc URL               RPC endpoint (default: the --network's public RPC)
  --passphrase STR        Network passphrase (default: the --network's)
  --apply-timeout SECS    How long to wait for a phase to take effect before
                          giving up (default: 300)
  --dry-run               Print what would happen; submit nothing, schedule nothing.
                          Still creates and funds the source account, since the
                          transactions cannot be built without one.
  --yes                   Don't prompt before arming the validators
  -h, --help              This message
EOF
}

log()  { printf '%s\n' "$*" >&2; }
step() { printf '\n=== %s ===\n' "$*" >&2; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }

confirm()
{
    [ "$ASSUME_YES" = "true" ] && return 0
    local reply
    printf '%s [y/N] ' "$1" >&2
    read -r reply </dev/tty || die "no tty available for confirmation; re-run with --yes"
    case "$reply" in
        y|Y|yes|YES) return 0 ;;
        *) die "aborted" ;;
    esac
}

while [ $# -gt 0 ]; do
    case "$1" in
        --network)        NETWORK="$2"; shift 2 ;;
        --secret)         SECRET_KEY="$2"; shift 2 ;;
        --resource-fee)   RESOURCE_FEE="$2"; shift 2 ;;
        --upgrade-in)     UPGRADE_IN="$2"; shift 2 ;;
        --upgrade-time)   UPGRADE_TIME="$2"; shift 2 ;;
        --schedule-only)  SCHEDULE_ONLY_KEY="$2"; shift 2 ;;
        --no-schedule)    NO_SCHEDULE="true"; shift ;;
        --context)        KUBE_CONTEXT="$2"; shift 2 ;;
        --namespace)      NAMESPACE="$2"; shift 2 ;;
        --pods)           PODS="$2"; shift 2 ;;
        --container)      CONTAINER="$2"; shift 2 ;;
        --core-bin)       CORE_BIN="$2"; shift 2 ;;
        --rpc)            RPC_URL="$2"; shift 2 ;;
        --passphrase)     PASSPHRASE="$2"; shift 2 ;;
        --apply-timeout)  APPLY_WAIT_SECONDS="$2"; shift 2 ;;
        --dry-run)        DRY_RUN="true"; shift ;;
        --yes|-y)         ASSUME_YES="true"; shift ;;
        -h|--help)        usage; exit 0 ;;
        -*)               die "unknown option: $1" ;;
        *)
            case "$1" in *[[:space:]]*) die "settings paths must not contain spaces: $1" ;; esac
            SETTINGS_FILES="$SETTINGS_FILES $1"
            shift ;;
    esac
done

# ---------------------------------------------------------------- preflight --

case "$NETWORK" in
    futurenet)
        : "${RPC_URL:=https://rpc-futurenet.stellar.org}"
        : "${PASSPHRASE:=Test SDF Future Network ; October 2022}"
        : "${FRIENDBOT_URL:=https://friendbot-futurenet.stellar.org}" ;;
    testnet)
        : "${RPC_URL:=https://soroban-testnet.stellar.org}"
        : "${PASSPHRASE:=Test SDF Network ; September 2015}"
        : "${FRIENDBOT_URL:=https://friendbot.stellar.org}" ;;
    *)
        die "unknown --network '$NETWORK' (expected futurenet or testnet)" ;;
esac

# The validator pods and kube context below are futurenet's; testnet validators
# are armed by their operators.
if [ "$NETWORK" != "futurenet" ] && [ "$NO_SCHEDULE" = "false" ]; then
    die "scheduling is only automated for futurenet; pass --no-schedule for $NETWORK"
fi

for tool in curl jq; do
    command -v "$tool" >/dev/null 2>&1 || die "$tool not found in PATH"
done
if [ "$NO_SCHEDULE" = "false" ]; then
    command -v kubectl >/dev/null 2>&1 || die "kubectl not found in PATH"
fi

if [ -z "$SCHEDULE_ONLY_KEY" ]; then
    [ -n "$SETTINGS_FILES" ] || { usage; exit 1; }
    command -v stellar-xdr >/dev/null 2>&1 || die "stellar-xdr not found in PATH"
    # Validate every file up front: a typo in the last one shouldn't surface
    # after the earlier phases have already been applied to the network.
    for f in $SETTINGS_FILES; do
        [ -r "$f" ] || die "cannot read settings file: $f"
        stellar-xdr encode --type ConfigUpgradeSet "$f" >/dev/null 2>&1 \
            || die "$f is not a valid ConfigUpgradeSet"
    done

    if [ -z "$CORE_BIN" ]; then
        for candidate in ./stellar-core ./src/stellar-core; do
            [ -x "$candidate" ] && { CORE_BIN="$candidate"; break; }
        done
        [ -z "$CORE_BIN" ] && CORE_BIN="$(command -v stellar-core || true)"
    fi
    [ -n "$CORE_BIN" ] && [ -x "$CORE_BIN" ] || die "stellar-core binary not found; pass --core-bin PATH"
fi

# --------------------------------------------------------------------- rpc --

rpc()
{
    local method="$1" params="${2:-null}" body resp
    body="$(jq -nc --arg m "$method" --argjson p "$params" \
        '{jsonrpc:"2.0", id:1, method:$m, params:$p}')"
    resp="$(curl -sS -m "$RPC_TIMEOUT" -X POST "$RPC_URL" \
        -H 'Content-Type: application/json' -d "$body")" \
        || die "RPC request failed: $method"
    [ -n "$resp" ] || die "empty RPC response for $method"
    if printf '%s' "$resp" | jq -e 'has("error")' >/dev/null; then
        die "RPC error for $method: $(printf '%s' "$resp" | jq -c '.error')"
    fi
    printf '%s' "$resp" | jq -c '.result'
}

decode_xdr()
{
    # decode_xdr <type> <base64>; best effort, never fatal
    printf '%s' "$2" | stellar-xdr decode --type "$1" --output json 2>/dev/null || printf '%s' "$2"
}

account_entry_xdr()
{
    # Echoes the base64 AccountEntry, or nothing if the account does not exist.
    local pk="$1" key
    key="$(jq -nc --arg a "$pk" '{account:{account_id:$a}}' \
        | stellar-xdr encode --type LedgerKey)"
    rpc getLedgerEntries "$(jq -nc --arg k "$key" '{keys:[$k]}')" \
        | jq -r '.entries[0].xdr // empty'
}

account_seq_num()
{
    local pk="$1" entry
    entry="$(account_entry_xdr "$pk")"
    [ -n "$entry" ] || die "account $pk not found on $RPC_URL"
    printf '%s' "$entry" \
        | stellar-xdr decode --type LedgerEntryData --output json \
        | jq -r '.account.seq_num'
}

create_and_fund_account()
{
    # Sets SECRET_KEY / PUBLIC_KEY to a fresh friendbot-funded account.
    local seed_out http waited=0
    seed_out="$("$CORE_BIN" gen-seed 2>/dev/null)" || die "stellar-core gen-seed failed"
    SECRET_KEY="$(printf '%s\n' "$seed_out" | awk '/^Secret seed:/ { print $NF }')"
    PUBLIC_KEY="$(printf '%s\n' "$seed_out" | awk '/^Public:/ { print $NF }')"
    [ -n "$SECRET_KEY" ] && [ -n "$PUBLIC_KEY" ] \
        || die "could not parse stellar-core gen-seed output"

    log "generated source account: $PUBLIC_KEY"
    # CI run logs can be public, and ::add-mask:: races with stderr output, so
    # never print the seed there. It is only useful for a local --secret re-run.
    if [ "${GITHUB_ACTIONS:-}" != "true" ]; then
        log "  secret seed: $SECRET_KEY"
        log "  (keep this until the upgrade lands -- if the run dies partway through,"
        log "   re-run with --secret to reuse the account rather than starting over)"
    fi

    http="$(curl -sS -m 60 -o /dev/null -w '%{http_code}' "$FRIENDBOT_URL/?addr=$PUBLIC_KEY")" \
        || die "friendbot request to $FRIENDBOT_URL failed"
    [ "$http" = "200" ] || die "friendbot returned HTTP $http for $PUBLIC_KEY"

    while [ "$waited" -lt "$ACCOUNT_WAIT_SECONDS" ]; do
        if [ -n "$(account_entry_xdr "$PUBLIC_KEY")" ]; then
            log "  funded"
            return 0
        fi
        sleep "$POLL_INTERVAL"
        waited=$((waited + POLL_INTERVAL))
    done
    die "account $PUBLIC_KEY did not appear on $RPC_URL within ${ACCOUNT_WAIT_SECONDS}s of funding"
}

wait_for_tx()
{
    # wait_for_tx <label> <hex hash>
    local label="$1" hash="$2" waited=0 res status
    while [ "$waited" -lt "$TX_WAIT_SECONDS" ]; do
        res="$(rpc getTransaction "$(jq -nc --arg h "$hash" '{hash:$h}')")"
        status="$(printf '%s' "$res" | jq -r '.status')"
        case "$status" in
            SUCCESS)
                log "  $label: SUCCESS (ledger $(printf '%s' "$res" | jq -r '.ledger'))"
                return 0 ;;
            FAILED)
                log "  $label: FAILED"
                log "  result: $(decode_xdr TransactionResult "$(printf '%s' "$res" | jq -r '.resultXdr // empty')")"
                die "$label failed on-chain; the sequence number was consumed, so re-run the script from the top (it will pick up the new sequence number)" ;;
            NOT_FOUND)
                sleep "$POLL_INTERVAL"
                waited=$((waited + POLL_INTERVAL)) ;;
            *)
                die "unexpected getTransaction status for $label: $status" ;;
        esac
    done
    die "$label ($hash) not applied within ${TX_WAIT_SECONDS}s"
}

submit_tx()
{
    # submit_tx <label> <base64 envelope> <expected hex hash>
    local label="$1" env="$2" hash="$3" res status attempt=0
    log "$label: submitting (hash $hash)"
    while :; do
        res="$(rpc sendTransaction "$(jq -nc --arg t "$env" '{transaction:$t}')")"
        status="$(printf '%s' "$res" | jq -r '.status')"
        case "$status" in
            PENDING|DUPLICATE)
                break ;;
            TRY_AGAIN_LATER)
                attempt=$((attempt + 1))
                [ "$attempt" -ge 5 ] && die "$label: RPC kept returning TRY_AGAIN_LATER"
                log "  $label: TRY_AGAIN_LATER, retrying"
                sleep 5 ;;
            ERROR)
                log "  error: $(decode_xdr TransactionResult "$(printf '%s' "$res" | jq -r '.errorResultXdr // empty')")"
                die "$label rejected by RPC" ;;
            *)
                die "$label: unexpected sendTransaction status: $status" ;;
        esac
    done
    wait_for_tx "$label" "$hash"
}

# ----------------------------------------------------------------- kubectl --

kctl() { kubectl --context "$KUBE_CONTEXT" "$@"; }

kexec()
{
    local pod="$1"; shift
    if [ -n "$CONTAINER" ]; then
        kctl -n "$NAMESPACE" exec "$pod" -c "$CONTAINER" -- "$@"
    else
        kctl -n "$NAMESPACE" exec "$pod" -- "$@"
    fi
}

resolve_context()
{
    local contexts
    contexts="$(kubectl config get-contexts -o name 2>/dev/null)" \
        || die "could not read kubeconfig"
    if [ -n "$KUBE_CONTEXT" ]; then
        printf '%s\n' "$contexts" | grep -qxF "$KUBE_CONTEXT" \
            || die "kube context '$KUBE_CONTEXT' not found. Available:
$contexts"
    else
        # The current context is usually ssc-eks; futurenet lives in dev-eks.
        KUBE_CONTEXT="$(printf '%s\n' "$contexts" | grep '^dev-eks/' | head -1)"
        [ -n "$KUBE_CONTEXT" ] \
            || die "no dev-eks/* context in your kubeconfig; pass --context. Available:
$contexts"
    fi
    log "kube context:    $KUBE_CONTEXT"
}

check_cluster()
{
    local why=""
    if ! kctl --request-timeout "$KUBE_TIMEOUT" -n "$NAMESPACE" get pods >/dev/null 2>&1; then
        pgrep -f sshuttle >/dev/null 2>&1 \
            || why=" sshuttle does not appear to be running, and dev-eks is only reachable through it."
        die "cannot reach namespace $NAMESPACE via kube context $KUBE_CONTEXT.$why"
    fi
}

pod_http()
{
    # pod_http <pod> <path> [curl args...]  -- args are passed as argv, no shell quoting
    local pod="$1" path="$2"; shift 2
    kexec "$pod" curl -sS -G "http://localhost:${HTTP_PORT}/${path}" "$@"
}

check_pods()
{
    local pod
    for pod in $PODS; do
        kctl -n "$NAMESPACE" get pod "$pod" >/dev/null 2>&1 \
            || die "pod $pod not found in namespace $NAMESPACE"
        kexec "$pod" sh -c 'command -v curl >/dev/null 2>&1' \
            || die "curl is not available inside $pod; run the printed commands by hand"
    done
}

live_settings()
{
    # Current upgradeable settings on a node, as normalized ConfigUpgradeSet JSON.
    pod_http "$1" 'sorobaninfo?format=upgrade_xdr' \
        | stellar-xdr decode --type ConfigUpgradeSet --output json
}

settings_applied()
{
    # True when every entry of the proposal appears verbatim in the live
    # settings. The proposal is a subset of all settings, so this is a subset
    # test, not equality. Both sides are normalized by stellar-xdr, so key
    # order and number formatting can't produce a false negative.
    local proposal="$1" pod="$2" live
    live="$(live_settings "$pod")" || return 1
    jq -e -n --argjson p "$proposal" --argjson l "$live" \
        '[$p.updated_entry[] | IN($l.updated_entry[])] | all' >/dev/null
}

wait_for_applied()
{
    # Block until the upgrade is live on *every* validator.
    local proposal="$1" waited=0 pod pending
    step "Waiting for the upgrade to take effect"
    while [ "$waited" -lt "$APPLY_WAIT_SECONDS" ]; do
        pending=""
        for pod in $PODS; do
            if ! settings_applied "$proposal" "$pod"; then
                pending="$pending $pod"
            fi
        done
        if [ -z "$pending" ]; then
            log "  applied on all validators"
            return 0
        fi
        sleep "$POLL_INTERVAL"
        waited=$((waited + POLL_INTERVAL))
    done
    die "upgrade did not take effect within ${APPLY_WAIT_SECONDS}s; still pending on:$pending"
}

settings_live_via_rpc()
{
    # True when every entry of the proposal matches the network's current
    # ConfigSetting ledger entries, read through the RPC. Each proposal entry's
    # union arm name is also its ConfigSettingID, which keys the ledger entry.
    local proposal="$1" keys live
    keys="$(printf '%s' "$proposal" | jq -r '.updated_entry[] | keys[0]' \
        | while read -r id; do
            jq -nc --arg id "$id" '{config_setting:{config_setting_id:$id}}' \
                | stellar-xdr encode --type LedgerKey
            printf '\n'
          done | jq -Rsc 'split("\n") | map(select(length > 0))')" || return 1
    live="$(rpc getLedgerEntries "$(jq -nc --argjson k "$keys" '{keys:$k}')" \
        | jq -r '.entries[]?.xdr' \
        | while read -r xdr; do
            printf '%s' "$xdr" | stellar-xdr decode --type LedgerEntryData --output json
            printf '\n'
          done | jq -sc 'map(.config_setting)')" || return 1
    jq -e -n --argjson p "$proposal" --argjson l "$live" \
        '[$p.updated_entry[] | IN($l[])] | all' >/dev/null
}

wait_for_live_via_rpc()
{
    # Block until someone else has armed the upgrade and it has taken effect.
    local proposal="$1" label="$2" waited=0
    step "$label: waiting up to ${APPLY_WAIT_SECONDS}s for the upgrade to be armed and applied"
    log "arm the ConfigUpgradeSetKey above on the validators; the next phase starts once it is live"
    while [ "$waited" -lt "$APPLY_WAIT_SECONDS" ]; do
        if settings_live_via_rpc "$proposal"; then
            log "  $label is live on $RPC_URL"
            return 0
        fi
        sleep 10
        waited=$((waited + 10))
    done
    die "$label was not applied within ${APPLY_WAIT_SECONDS}s; arm it, then re-run with only the remaining settings files"
}

to_seconds()
{
    # Accepts "90s", "10m", or a bare number of minutes.
    case "$1" in
        *s) printf '%s\n' "${1%s}" ;;
        *m) printf '%s\n' "$(( ${1%m} * 60 ))" ;;
        *)  printf '%s\n' "$(( $1 * 60 ))" ;;
    esac
}

future_time()
{
    local secs
    secs="$(to_seconds "$1")"
    if date -u -v+"${secs}"S +%Y-%m-%dT%H:%M:%SZ >/dev/null 2>&1; then
        date -u -v+"${secs}"S +%Y-%m-%dT%H:%M:%SZ            # BSD/macOS
    else
        date -u -d "+${secs} seconds" +%Y-%m-%dT%H:%M:%SZ    # GNU
    fi
}

# ------------------------------------------------------------- phase steps --

# Set by generate_and_submit for the phase in flight.
PHASE_KEY=""
PHASE_PROPOSAL=""

generate_and_submit()
{
    local file="$1" label="$2" xdr seq output
    step "$label: preparing $file"

    xdr="$(stellar-xdr encode --type ConfigUpgradeSet "$file")" \
        || die "failed to encode $file as a ConfigUpgradeSet"
    PHASE_PROPOSAL="$(printf '%s' "$xdr" \
        | stellar-xdr decode --type ConfigUpgradeSet --output json)"

    seq="$(account_seq_num "$PUBLIC_KEY")"
    log "sequence number: $seq"
    log "resource fee:    +$RESOURCE_FEE"

    # Same invocation as scripts/settings-helper.sh. Output is 9 lines:
    # restore env, restore hash, upload env, upload hash, create env, create hash,
    # invoke env, invoke hash, ConfigUpgradeSetKey.
    output="$(printf '%s\n' "$SECRET_KEY" \
        | "$CORE_BIN" get-settings-upgrade-txs "$PUBLIC_KEY" "$seq" "$PASSPHRASE" \
            --xdr "$xdr" --signtxs --add-resource-fee "$RESOURCE_FEE")"
    [ "$(printf '%s\n' "$output" | wc -l | tr -d ' ')" -eq 9 ] \
        || die "unexpected get-settings-upgrade-txs output:
$output"

    PHASE_KEY="$(printf '%s\n' "$output" | sed -n '9p')"
    log "ConfigUpgradeSetKey: $PHASE_KEY"

    if [ "$DRY_RUN" = "true" ]; then
        log "--- transactions that would be submitted ---"
        log "restore  $(printf '%s\n' "$output" | sed -n '2p')"
        log "  $(printf '%s\n' "$output" | sed -n '1p')"
        log "upload   $(printf '%s\n' "$output" | sed -n '4p')"
        log "  $(printf '%s\n' "$output" | sed -n '3p')"
        log "create   $(printf '%s\n' "$output" | sed -n '6p')"
        log "  $(printf '%s\n' "$output" | sed -n '5p')"
        log "invoke   $(printf '%s\n' "$output" | sed -n '8p')"
        log "  $(printf '%s\n' "$output" | sed -n '7p')"
        return 0
    fi

    step "$label: submitting transactions"
    # These use consecutive sequence numbers, so they must be applied in order.
    submit_tx "1/4 restore" "$(printf '%s\n' "$output" | sed -n '1p')" \
                            "$(printf '%s\n' "$output" | sed -n '2p')"
    submit_tx "2/4 upload"  "$(printf '%s\n' "$output" | sed -n '3p')" \
                            "$(printf '%s\n' "$output" | sed -n '4p')"
    submit_tx "3/4 create"  "$(printf '%s\n' "$output" | sed -n '5p')" \
                            "$(printf '%s\n' "$output" | sed -n '6p')"
    submit_tx "4/4 invoke"  "$(printf '%s\n' "$output" | sed -n '7p')" \
                            "$(printf '%s\n' "$output" | sed -n '8p')"
}

verify_on_nodes()
{
    local key="$1" pod out
    step "Verifying the proposed settings on the validators"
    for pod in $PODS; do
        log "--- $pod: dumpproposedsettings ---"
        out="$(pod_http "$pod" dumpproposedsettings --data-urlencode "blob=$key")" \
            || die "dumpproposedsettings failed on $pod"
        printf '%s\n' "$out"
        # Check positively for the settings dump: a bad key comes back as
        # "configUpgradeSet is missing or invalid", and a malformed one as a JSON
        # {"exception": ...}, so matching on error strings is easy to get wrong.
        case "$out" in
            *ConfigSettingsEntries*) ;;
            *) die "$pod does not see a valid upgrade set for $key" ;;
        esac
    done
}

arm_on_nodes()
{
    # Echoes nothing; sets ARMED_AT.
    local key="$1" pod
    if [ -n "$UPGRADE_TIME" ]; then
        step "Scheduling the upgrade for $UPGRADE_TIME (UTC)"
        confirm "Arm ${PODS// /, } for a settings upgrade at $UPGRADE_TIME?"
        ARMED_AT="$UPGRADE_TIME"
    else
        step "Scheduling the upgrade for $UPGRADE_IN from now"
        # Resolve the timestamp only after the prompt is answered -- with a short
        # window like 90s, time spent sitting at the prompt would otherwise put
        # the upgrade time in the past before any node is armed.
        confirm "Arm ${PODS// /, } for a settings upgrade $UPGRADE_IN from now?"
        ARMED_AT="$(future_time "$UPGRADE_IN")"
        log "upgrade time: $ARMED_AT (UTC)"
    fi

    for pod in $PODS; do
        log "--- $pod: upgrades?mode=set ---"
        pod_http "$pod" "upgrades?mode=set&upgradetime=$ARMED_AT" \
            --data-urlencode "configupgradesetkey=$key" \
            || die "failed to arm $pod (the other nodes may already be armed)"
        printf '\n'
    done
}

# ------------------------------------------------------------------- phases --

# Check cluster reachability up front: finding out that the tunnel is down
# *after* submitting four transactions would mean burning them for nothing.
if [ "$NO_SCHEDULE" = "false" ]; then
    resolve_context
    if [ "$DRY_RUN" = "false" ]; then
        check_cluster
        check_pods
    fi
fi

if [ -n "$SCHEDULE_ONLY_KEY" ]; then
    [ -z "$SETTINGS_FILES" ] \
        || die "--schedule-only arms an already-submitted key; drop the settings files"
    verify_on_nodes "$SCHEDULE_ONLY_KEY"
    if [ "$DRY_RUN" = "true" ]; then
        for pod in $PODS; do
            printf "kubectl --context %s -n %s exec %s -c %s -- curl -sG 'http://localhost:%s/upgrades?mode=set&upgradetime=%s' --data-urlencode 'configupgradesetkey=%s'\n" \
                "$KUBE_CONTEXT" "$NAMESPACE" "$pod" "$CONTAINER" "$HTTP_PORT" \
                "${UPGRADE_TIME:-$(future_time "$UPGRADE_IN")}" "$SCHEDULE_ONLY_KEY"
        done
        exit 0
    fi
    arm_on_nodes "$SCHEDULE_ONLY_KEY"
    step "Done"
    log "ConfigUpgradeSetKey: $SCHEDULE_ONLY_KEY"
    log "Upgrade time (UTC):  $ARMED_AT"
    exit 0
fi

TOTAL=0
for f in $SETTINGS_FILES; do TOTAL=$((TOTAL + 1)); done

if [ "$TOTAL" -gt 1 ] && [ -n "$UPGRADE_TIME" ]; then
    die "--upgrade-time is a single absolute timestamp and cannot apply to $TOTAL settings files; use --upgrade-in"
fi

setup_account()
{
    step "Source account"
    if [ -n "$SECRET_KEY" ]; then
        PUBLIC_KEY="$("$CORE_BIN" convert-id "$SECRET_KEY" \
            | awk '/^PublicKey:/{f=1; next} f && /strKey:/{print $NF; exit}')"
        [ -n "$PUBLIC_KEY" ] || die "could not derive public key from --secret"
        log "using account: $PUBLIC_KEY"
    else
        create_and_fund_account
    fi
    log "network: $PASSPHRASE"
    log "rpc:     $RPC_URL"
}

# One account funds every phase; each phase re-reads its sequence number.
setup_account

INDEX=0
for SETTINGS_JSON in $SETTINGS_FILES; do
    INDEX=$((INDEX + 1))
    LABEL="phase $INDEX/$TOTAL"

    generate_and_submit "$SETTINGS_JSON" "$LABEL"

    if [ "$DRY_RUN" = "true" ]; then
        if [ "$TOTAL" -gt 1 ] && [ "$INDEX" -eq "$TOTAL" ]; then
            log ""
            log "note: nothing is submitted in a dry run, so every phase above was"
            log "      built against the same sequence number. In a real run each"
            log "      phase reads the sequence number back from the RPC first."
        fi
        continue
    fi

    if [ "$NO_SCHEDULE" = "true" ]; then
        log ""
        log "$LABEL: submitted, not scheduled ($SETTINGS_JSON)"
        log "ConfigUpgradeSetKey: $PHASE_KEY"
        if [ "$NETWORK" = "futurenet" ]; then
            log "  schedule it with: $0 --schedule-only $PHASE_KEY"
        fi
        log "  or on each validator: curl -G 'http://localhost:11626/upgrades?mode=set&upgradetime=YYYY-MM-DDTHH:MM:SSZ' --data-urlencode 'configupgradesetkey=$PHASE_KEY'"
        if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
            {
                printf '### %s settings upgrade submitted\n\n' "$NETWORK"
                printf 'Settings file: `%s`\n\n' "$SETTINGS_JSON"
                printf 'ConfigUpgradeSetKey:\n```\n%s\n```\n' "$PHASE_KEY"
                printf 'Check it on a validator, then arm it with an agreed upgrade time:\n```\n'
                printf "curl -G 'http://localhost:11626/dumpproposedsettings' --data-urlencode 'blob=%s'\n" "$PHASE_KEY"
                printf "curl -G 'http://localhost:11626/upgrades?mode=set&upgradetime=YYYY-MM-DDTHH:MM:SSZ' --data-urlencode 'configupgradesetkey=%s'\n" "$PHASE_KEY"
                printf '```\n'
            } >> "$GITHUB_STEP_SUMMARY"
        fi
        # The step summary only renders once the step ends; an annotation
        # shows up on the run page while a later phase is waiting on this one.
        if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
            printf '::notice title=Arm %s (%s)::%s\n' "$LABEL" "$SETTINGS_JSON" "$PHASE_KEY"
        fi
        # Nobody here arms the upgrade, so wait for someone else to before
        # building the next phase, which may need the limits this one raises.
        if [ "$INDEX" -lt "$TOTAL" ]; then
            wait_for_live_via_rpc "$PHASE_PROPOSAL" "$LABEL"
        fi
        continue
    fi

    verify_on_nodes "$PHASE_KEY"
    arm_on_nodes "$PHASE_KEY"

    # Block until it is actually live before building the next phase, which may
    # depend on the limits this one raises.
    wait_for_applied "$PHASE_PROPOSAL"
    log "$LABEL done: $SETTINGS_JSON applied at $ARMED_AT (UTC)"
done

step "Done"
if [ "$DRY_RUN" = "true" ] || [ "$NO_SCHEDULE" = "true" ]; then
    exit 0
fi
log "$TOTAL settings upgrade(s) applied on ${PODS// /, }"
log "Current settings: kubectl --context $KUBE_CONTEXT -n $NAMESPACE exec ${PODS%% *} -c $CONTAINER -- curl -sG 'http://localhost:$HTTP_PORT/sorobaninfo?format=detailed'"
