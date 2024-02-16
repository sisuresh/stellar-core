#!/bin/sh

SECRET_KEY="$1"
PUBLIC_KEY=$(src/stellar-core convert-id "$SECRET_KEY" | sed -n '4p' | awk '{print $NF}')

OUTPUT="$(echo $SECRET_KEY | src/stellar-core get-settings-upgrade-txs "$PUBLIC_KEY" "$2" "$3" --xdr $(stellar-xdr encode --type ConfigUpgradeSet $4) --signtxs)"

curl -G 'http://localhost:11626/tx' --data-urlencode 'blob=$(echo "$OUTPUT" | sed -n '1p')'
echo "-----"

echo "curl -G 'http://localhost:11626/tx' --data-urlencode 'blob=$(echo "$OUTPUT" | sed -n '3p')'"
echo "-----"

echo "curl -G 'http://localhost:11626/tx' --data-urlencode 'blob=$(echo "$OUTPUT" | sed -n '5p')'"
echo "-----"

echo "curl -G 'http://localhost:11626/dumpproposedsettings' --data-urlencode 'blob=$(echo "$OUTPUT" | sed -n '7p')'"
echo "-----"

echo "curl -G 'http://localhost:11626/upgrades?mode=set&upgradetime=YYYY-MM-DDT01:25:00Z' --data-urlencode 'configupgradesetkey=$(echo "$OUTPUT" | sed -n '7p')'"
