#!/usr/bin/env bash
#
# Rewrites a settings file with a network's live Soroban settings.
#
# After a protocol upgrade, the network's settings can include new cost types,
# recalibrated cost params, and new upgradeable settings, and validators reject
# a settings upgrade that doesn't match the protocol. This reads every
# upgradeable ConfigSetting ledger entry through the network's RPC and writes
# them to the file in ConfigSettingID order, so applying the file after a
# network reset reproduces the network's current settings.
#
# The setting types come from stellar-xdr's ConfigSettingID enum, and the
# non-upgradeable ones are read from stellar-core's own
# SorobanNetworkConfig::isNonUpgradeableConfigSettingEntry, so neither list is
# kept here.
#
# Usage:
#   scripts/sync-soroban-settings.sh [options]
#
# Options:
#   --network NAME    testnet (default) or futurenet
#   --rpc URL         RPC endpoint (default: the --network's public RPC)
#   --file PATH       Settings file (default: soroban-settings/testnet_settings_upgrade.json)
#   --core-src PATH   stellar-core source defining isNonUpgradeableConfigSettingEntry
#                     (default: src/ledger/NetworkConfig.cpp)
#
# Prints a summary of what changed; the file is left untouched if nothing did.
#
# Requires: stellar-xdr (at least the network's protocol version), curl, jq

set -euo pipefail

NETWORK="testnet"
RPC_URL=""
FILE="soroban-settings/testnet_settings_upgrade.json"
CORE_SRC="src/ledger/NetworkConfig.cpp"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
    case "$1" in
        --network)  NETWORK="$2"; shift 2 ;;
        --rpc)      RPC_URL="$2"; shift 2 ;;
        --file)     FILE="$2"; shift 2 ;;
        --core-src) CORE_SRC="$2"; shift 2 ;;
        -h|--help)  awk 'NR < 3 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "$0"; exit 0 ;;
        *)          die "unknown argument: $1" ;;
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
[ -r "$CORE_SRC" ] || die "cannot read $CORE_SRC"

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

# stellar-xdr's major version is the protocol its XDR covers. An older one
# wouldn't know the setting types a newer protocol added.
XDR_VERSION="$(stellar-xdr version | awk 'NR == 1 { print $2 }')"
[ "${XDR_VERSION%%.*}" -ge "$PROTOCOL" ] \
    || die "$NETWORK is on protocol $PROTOCOL but stellar-xdr $XDR_VERSION only covers protocol ${XDR_VERSION%%.*}; use a newer stellar-xdr"

# Every ConfigSettingID, in id order.
ALL_IDS="$(stellar-xdr types schema --type ConfigSettingId | jq -c '.enum')"
[ "$(printf '%s' "$ALL_IDS" | jq 'length')" -gt 0 ] \
    || die "could not list ConfigSettingIDs from stellar-xdr"

# The ids core never sets by upgrade, read from the body of
# isNonUpgradeableConfigSettingEntry(ConfigSettingID const&). CONFIG_SETTING_FOO_BAR
# in core is foo_bar in stellar-xdr's JSON.
SKIP="$(awk '
    /SorobanNetworkConfig::isNonUpgradeableConfigSettingEntry\(/ { sig = 1; next }
    sig && /ConfigSettingID const/ { body = 1; sig = 0; next }
    sig { sig = 0 }
    body && /^}/ { exit }
    body' "$CORE_SRC" \
    | { grep -o 'CONFIG_SETTING_[A-Z0-9_]*' || true; } \
    | sed 's/^CONFIG_SETTING_//' | tr 'A-Z' 'a-z' \
    | jq -Rsc 'split("\n") | map(select(length > 0))')"
[ "$(printf '%s' "$SKIP" | jq 'length')" -gt 0 ] \
    || die "found no non-upgradeable settings in isNonUpgradeableConfigSettingEntry in $CORE_SRC; has it moved?"
unknown="$(jq -nr --argjson s "$SKIP" --argjson a "$ALL_IDS" '$s - $a | join(", ")')"
[ -z "$unknown" ] \
    || die "$CORE_SRC lists settings stellar-xdr $XDR_VERSION doesn't know: $unknown"

WANT="$(jq -nc --argjson s "$SKIP" --argjson a "$ALL_IDS" '$a - $s')"

keys="$(printf '%s' "$WANT" | jq -r '.[]' | while read -r id; do
    jq -nc --arg id "$id" '{config_setting:{config_setting_id:$id}}' | stellar-xdr encode --type LedgerKey
    printf '\n'
done | jq -Rsc 'split("\n") | map(select(length > 0))')"

# Upgradeable settings with a ledger entry (the *_delta ids only exist inside
# upgrades and have none), in id order.
live="$(rpc getLedgerEntries "$(jq -nc --argjson k "$keys" '{keys:$k}')" \
    | jq -r '.entries[]?.xdr' \
    | while read -r xdr; do
        printf '%s' "$xdr" | stellar-xdr decode --type LedgerEntryData --output json
        printf '\n'
      done | jq -sc --argjson a "$ALL_IDS" \
        'map(.config_setting) | sort_by(keys[0] as $k | $a | index($k))')"
[ "$(printf '%s' "$live" | jq 'length')" -gt 0 ] || die "$RPC_URL returned no ConfigSetting entries"

updated="$(jq --argjson live "$live" '.updated_entry = $live' "$FILE")"
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
