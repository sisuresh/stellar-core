#!/usr/bin/env bash
#
# Copies a network's live Soroban cost parameters into a settings file.
#
# A protocol upgrade appends entries for new cost types and can recalibrate
# existing ones, and validators reject a cost-params upgrade whose entry count
# doesn't match the protocol. This rewrites the file's
# contract_cost_params_cpu_instructions and contract_cost_params_memory_bytes
# entries with the network's current values, read through its RPC, and leaves
# every other setting alone.
#
# Usage:
#   scripts/sync-soroban-settings-cost-params.sh [options]
#
# Options:
#   --network NAME   testnet (default) or futurenet
#   --rpc URL        RPC endpoint (default: the --network's public RPC)
#   --file PATH      Settings file (default: soroban-settings/testnet_settings_upgrade.json)
#
# Prints a summary of what changed; the file is left untouched if nothing did.
#
# Requires: stellar-xdr, curl, jq

set -euo pipefail

NETWORK="testnet"
RPC_URL=""
FILE="soroban-settings/testnet_settings_upgrade.json"
IDS="contract_cost_params_cpu_instructions contract_cost_params_memory_bytes"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
    case "$1" in
        --network) NETWORK="$2"; shift 2 ;;
        --rpc)     RPC_URL="$2"; shift 2 ;;
        --file)    FILE="$2"; shift 2 ;;
        -h|--help) awk 'NR < 3 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "$0"; exit 0 ;;
        *)         die "unknown argument: $1" ;;
    esac
done

case "$NETWORK" in
    testnet)   : "${RPC_URL:=https://soroban-testnet.stellar.org}" ;;
    futurenet) : "${RPC_URL:=https://rpc-futurenet.stellar.org}" ;;
    *)         die "unknown --network '$NETWORK' (expected testnet or futurenet)" ;;
esac

for tool in curl jq stellar-xdr; do
    command -v "$tool" >/dev/null 2>&1 || die "$tool not found in PATH"
done
[ -r "$FILE" ] || die "cannot read $FILE"

rpc()
{
    local method="$1" params="${2:-null}" resp
    resp="$(curl -sS --retry 3 -m 30 -X POST "$RPC_URL" -H 'Content-Type: application/json' \
        -d "$(jq -nc --arg m "$method" --argjson p "$params" '{jsonrpc:"2.0", id:1, method:$m, params:$p}')")" \
        || die "RPC request failed: $method"
    printf '%s' "$resp" | jq -e 'has("result")' >/dev/null \
        || die "RPC error for $method: $(printf '%s' "$resp" | jq -c '.error // .')"
    printf '%s' "$resp" | jq -c '.result'
}

PROTOCOL="$(rpc getNetwork | jq -r '.protocolVersion')"

keys="$(for id in $IDS; do
    jq -nc --arg id "$id" '{config_setting:{config_setting_id:$id}}' | stellar-xdr encode --type LedgerKey
    printf '\n'
done | jq -Rsc 'split("\n") | map(select(length > 0))')"

# {"contract_cost_params_cpu_instructions": [...], "contract_cost_params_memory_bytes": [...]}
live="$(rpc getLedgerEntries "$(jq -nc --argjson k "$keys" '{keys:$k}')" \
    | jq -r '.entries[]?.xdr' \
    | while read -r xdr; do
        printf '%s' "$xdr" | stellar-xdr decode --type LedgerEntryData --output json
        printf '\n'
      done | jq -sc 'map(.config_setting) | add')"
for id in $IDS; do
    printf '%s' "$live" | jq -e --arg id "$id" 'has($id)' >/dev/null \
        || die "$RPC_URL returned no $id entry"
done

summary="$(jq -r --argjson live "$live" '
    [.updated_entry[] | to_entries[0] | select($live[.key] != null)] as $mine
    | if ($mine | length) != ($live | length) then
        error("the file must contain both cost-params entries to sync them")
      else
        $mine[] | .key as $id | .value as $old | $live[$id] as $new
        | [range(0; [($old | length), ($new | length)] | min) | select($old[.] != $new[.])] as $changed
        | "\($id): \($old | length) -> \($new | length) entries"
          + (if ($changed | length) > 0 then ", changed cost types \($changed | map(tostring) | join(", "))" else "" end)
          + (if ($new | length) > ($old | length) then ", added cost types \($old | length)-\(($new | length) - 1)" else "" end)
          + (if ($new | length) < ($old | length) then ", removed cost types \($new | length)-\(($old | length) - 1)" else "" end)
      end' "$FILE")" || die "could not compare $FILE with the live settings"

updated="$(jq --argjson live "$live" \
    '.updated_entry |= map(with_entries(if $live[.key] != null then .value = $live[.key] else . end))' "$FILE")"
# Encoding the result checks it is still a valid ConfigUpgradeSet.
printf '%s' "$updated" | stellar-xdr encode --type ConfigUpgradeSet >/dev/null \
    || die "the updated file is not a valid ConfigUpgradeSet"

echo "$NETWORK is on protocol $PROTOCOL ($RPC_URL)"
if [ "$(jq -c . "$FILE")" = "$(printf '%s' "$updated" | jq -c .)" ]; then
    echo "$FILE: cost params already match $NETWORK"
    exit 0
fi
printf '%s\n' "$summary"

# Keep the file's existing trailing-newline convention so the diff is only the values.
if [ -n "$(tail -c 1 "$FILE")" ]; then
    printf '%s' "$updated" > "$FILE"
else
    printf '%s\n' "$updated" > "$FILE"
fi
echo "updated $FILE"
