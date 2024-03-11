#!/bin/sh
set -e

SECRET_KEY="$1"
PUBLIC_KEY=$(src/stellar-core convert-id "$SECRET_KEY" | sed -n '4p' | awk '{print $NF}')
echo "PUBLIC_KEY is $PUBLIC_KEY"

PUBNET_HORIZON="https://horizon.stellar.org/accounts"
PUBNET_PASSPHRASE="Public Global Stellar Network ; September 2015"

TESTNET_HORIZON="https://horizon-testnet.stellar.org/accounts"
TESTNET_PASSPHRASE="Test SDF Network ; September 2015"

HORIZON=""

#choose which horizon to hit for the SEQ_NUM. If Horizon is down, remove this code and manually set SEQ_NUM below
if [ "$2" == "$PUBNET_PASSPHRASE" ]
then
HORIZON=$PUBNET_HORIZON
elif [ "$2" == "$TESTNET_PASSPHRASE" ]
then
HORIZON=$TESTNET_HORIZON
else
echo "invalid passphrase"
fi


# get seq num
SEQ_NUM="$(curl "$HORIZON/$PUBLIC_KEY" | grep "\"sequence\":" | sed 's/[^0-9]//g')"
re='^[0-9]+$'
if ! [[ $SEQ_NUM =~ $re ]] ; then
   echo "Error: SEQ_NUM not retrieved. Your account might not be funded, or Horizon might be down. Hardcode the SEQ_NUM below and remove the horizon code." >&2; exit 1
fi

echo "$SEQ_NUM"
OUTPUT="$(echo $SECRET_KEY | src/stellar-core get-settings-upgrade-txs "$PUBLIC_KEY" "$SEQ_NUM" "$2" --xdr $(stellar-xdr encode --type ConfigUpgradeSet $3) --signtxs)"

if [ "$2" == "$PUBNET_PASSPHRASE" ]
then
echo "Dump the transactions for pubnet instead of submitting!!!"

echo "curl -G 'http://localhost:11626/tx' --data-urlencode 'blob=$(echo "$OUTPUT" | sed -n '1p')'"
echo "-----"

echo "curl -G 'http://localhost:11626/tx' --data-urlencode 'blob=$(echo "$OUTPUT" | sed -n '3p')'"
echo "-----"

echo "curl -G 'http://localhost:11626/tx' --data-urlencode 'blob=$(echo "$OUTPUT" | sed -n '5p')'"
echo "-----"

echo "curl -G 'http://localhost:11626/dumpproposedsettings' --data-urlencode 'blob=$(echo "$OUTPUT" | sed -n '7p')'"
echo "-----"
else
curl -G 'http://localhost:11626/tx' --data-urlencode "blob=$(echo "$OUTPUT" | sed -n '1p')"
echo "-----"
sleep 7s

curl -G 'http://localhost:11626/tx' --data-urlencode "blob=$(echo "$OUTPUT" | sed -n '3p')"
echo "-----"
sleep 7s

curl -G 'http://localhost:11626/tx' --data-urlencode "blob=$(echo "$OUTPUT" | sed -n '5p')"
echo "-----"
sleep 7s

curl -G 'http://localhost:11626/dumpproposedsettings' --data-urlencode "blob=$(echo "$OUTPUT" | sed -n '7p')"
echo "-----"
fi

echo "distribute the following command with the upgradetime set to an agreed upon point in the future"
echo "curl -G 'http://localhost:11626/upgrades?mode=set&upgradetime=YYYY-MM-DDT01:25:00Z' --data-urlencode 'configupgradesetkey=$(echo "$OUTPUT" | sed -n '7p')'"
