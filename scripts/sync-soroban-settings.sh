#!/usr/bin/env bash
#
# Rewrites a settings file with a network's live Soroban settings.
#
# After a protocol upgrade, the network's settings can include new cost types,
# recalibrated cost params, and new upgradeable settings, and validators reject
# a settings upgrade that doesn't match the protocol. This reads every
# ConfigSetting ledger entry through the network's RPC and writes the
# upgradeable ones to the file in ConfigSettingID order, so applying the file
# after a network reset reproduces the network's current settings.
#
# Usage:
#   scripts/sync-soroban-settings.sh [options]
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

# Stored as ConfigSetting entries but never set by an upgrade; must match
# SorobanNetworkConfig::isNonUpgradeableConfigSettingEntry.
NON_UPGRADEABLE='["live_soroban_state_size_window","eviction_iterator","frozen_ledger_keys","freeze_bypass_txs"]'

# ConfigSettingIDs to look up. Ids with no ledger entry are simply not
# returned, so this leaves room for settings added by future protocols.
MAX_ID=63

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

# A ConfigSetting LedgerKey is the CONFIG_SETTING (8) discriminant followed by
# the int32 ConfigSettingID. Building it by number rather than by name lets
# this find settings that this stellar-xdr doesn't know the name of.
config_keys()
{
    jq -nc --argjson max "$1" '[range(0; $max + 1) | [0,0,0,8, 0,0,0,.] | implode | @base64]'
}

# The RPC rejects the whole request if it contains an id its XDR doesn't know,
# naming the first such index, so drop the ids from there on and retry.
max_id="$MAX_ID"
while :; do
    resp="$(curl -sS --retry 3 -m 30 -X POST "$RPC_URL" -H 'Content-Type: application/json' \
        -d "$(jq -nc --argjson k "$(config_keys "$max_id")" \
            '{jsonrpc:"2.0", id:1, method:"getLedgerEntries", params:{keys:$k}}')")" \
        || die "RPC request failed: getLedgerEntries"
    bad="$(printf '%s' "$resp" \
        | jq -r '.error.message // "" | capture("cannot unmarshal key value .* at index (?<i>[0-9]+)").i // empty')"
    [ -n "$bad" ] && [ "$bad" -gt 0 ] && [ "$bad" -le "$max_id" ] || break
    max_id=$((bad - 1))
done
printf '%s' "$resp" | jq -e 'has("result")' >/dev/null \
    || die "RPC error for getLedgerEntries: $(printf '%s' "$resp" | jq -c '.error // .')"

# One {"id": n, "entry": {...}} per live entry, sorted by id.
live="$(printf '%s' "$resp" | jq -c '.result' \
    | jq -c '.entries[]? | {id: (.key | @base64d | explode | .[7]), xdr}' \
    | while read -r row; do
        id="$(printf '%s' "$row" | jq -r '.id')"
        entry="$(printf '%s' "$row" | jq -r '.xdr' \
            | stellar-xdr decode --type LedgerEntryData --output json 2>/dev/null)" \
            || die "cannot decode ConfigSettingID $id; use a stellar-xdr that supports protocol $PROTOCOL"
        printf '%s' "$entry" | jq -c --argjson id "$id" '{id: $id, entry: .config_setting}'
      done | jq -sc 'sort_by(.id)')"
[ "$(printf '%s' "$live" | jq 'length')" -gt 0 ] || die "$RPC_URL returned no ConfigSetting entries"

updated="$(jq --argjson live "$live" --argjson skip "$NON_UPGRADEABLE" '
    .updated_entry = [$live[].entry | select((keys[0] | IN($skip[])) | not)]' "$FILE")"
# Encoding the result checks it is still a valid ConfigUpgradeSet.
printf '%s' "$updated" | stellar-xdr encode --type ConfigUpgradeSet >/dev/null \
    || die "the updated file is not a valid ConfigUpgradeSet"

echo "$NETWORK is on protocol $PROTOCOL ($RPC_URL)"
if [ "$(jq -c . "$FILE")" = "$(printf '%s' "$updated" | jq -c .)" ]; then
    echo "$FILE already matches $NETWORK"
    exit 0
fi

# One line per setting that was added, removed or changed. Array settings (the
# cost params) list the changed and added indices; object settings list the
# changed fields.
jq -rn --slurpfile old "$FILE" --argjson new "$updated" '
    def bykey: map(to_entries[0]) | from_entries;
    ($old[0].updated_entry | bykey) as $o | ($new.updated_entry | bykey) as $n
    | (($o | keys) + ($n | keys) | unique)[] as $k
    | if $o[$k] == null then "\($k): added"
      elif $n[$k] == null then "\($k): removed"
      elif $o[$k] == $n[$k] then empty
      elif ($n[$k] | type) == "array" then
        [range(0; [($o[$k] | length), ($n[$k] | length)] | min) | select($o[$k][.] != $n[$k][.])] as $c
        | "\($k): \($o[$k] | length) -> \($n[$k] | length) entries"
          + (if ($c | length) > 0 then ", changed \($c | map(tostring) | join(", "))" else "" end)
          + (if ($n[$k] | length) > ($o[$k] | length) then ", added \($o[$k] | length)-\(($n[$k] | length) - 1)" else "" end)
      elif ($n[$k] | type) == "object" then
        "\($k): changed \([($n[$k] | keys[]) as $f | select($o[$k][$f] != $n[$k][$f]) | $f] | join(", "))"
      else "\($k): \($o[$k]) -> \($n[$k])"
      end'

# Keep the file's existing trailing-newline convention so the diff is only the values.
if [ -n "$(tail -c 1 "$FILE")" ]; then
    printf '%s' "$updated" > "$FILE"
else
    printf '%s\n' "$updated" > "$FILE"
fi
echo "updated $FILE"
