// Copyright 2025 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#if defined(__SIZEOF_INT128__) || defined(_GLIBCXX_USE_INT128)

#include "invariant/EventsAreConsistentWithEntryDiffs.h"
#include "crypto/SHA.h"
#include "invariant/InvariantManager.h"
#include "ledger/LedgerTxn.h"
#include "main/Application.h"
#include "util/GlobalChecks.h"
#include <fmt/format.h>
#include <numeric>

namespace stellar
{

struct AggregatedEvents
{
    mutable UnorderedMap<LedgerKey, UnorderedMap<Asset, __int128>>
        mEventAmounts;
    UnorderedMap<Hash, Asset> mStellarAssetContractIDs;
};

static __int128_t
getAmountFromData(SCVal const& data)
{
    __int128_t amount = 0;
    if (data.type() == SCV_I128)
    {
        auto const& amountVal = data.i128();
        amount = amountVal.hi;
        amount <<= 64;
        amount |= amountVal.lo;
    }
    else if (data.type() == SCV_MAP && data.map())
    {
        auto const& map = *data.map();
        for (auto const& entry : map)
        {
            if (entry.key.type() == SCV_SYMBOL && entry.key.sym() == "amount" &&
                entry.val.type() == SCV_I128)
            {
                amount = entry.val.i128().hi;
                amount <<= 64;
                amount |= entry.val.i128().lo;
            }
        }
    }

    return amount;
}

static __int128_t
consumeAmount(
    LedgerKey const& lk, Asset const& asset,
    UnorderedMap<LedgerKey, UnorderedMap<Asset, __int128>>& eventAmounts)
{
    auto lkAssetMapIt = eventAmounts.find(lk);
    if (lkAssetMapIt == eventAmounts.end())
    {
        return 0;
    }

    auto& lkAssetMap = lkAssetMapIt->second;
    auto assetAmountIt = lkAssetMap.find(asset);
    if (assetAmountIt == lkAssetMap.end())
    {
        return 0;
    }

    __int128_t res = assetAmountIt->second;

    // Now remove this value from the map
    lkAssetMap.erase(assetAmountIt);
    if (lkAssetMap.empty())
    {
        eventAmounts.erase(lkAssetMapIt);
    }
    return res;
}

static std::string
calculateDeltaBalance(AggregatedEvents const& agg, LedgerEntry const* current,
                      LedgerEntry const* previous)
{
    releaseAssert(current || previous);
    auto lk = current ? LedgerEntryKey(*current) : LedgerEntryKey(*previous);
    auto let = current ? current->data.type() : previous->data.type();

    switch (let)
    {
    case ACCOUNT:
    {
        Asset native(ASSET_TYPE_NATIVE);
        auto eventDiff = consumeAmount(lk, native, agg.mEventAmounts);

        auto entryDiff = (current ? current->data.account().balance : 0) -
                         (previous ? previous->data.account().balance : 0);

        return entryDiff == eventDiff ? ""
                                      : "Account diff does not match events";
    }
    case TRUSTLINE:
    {
        auto const& tlAsset = current ? current->data.trustLine().asset
                                      : previous->data.trustLine().asset;

        Asset asset;
        switch (tlAsset.type())
        {
        case stellar::ASSET_TYPE_CREDIT_ALPHANUM4:
            asset.alphaNum4() = tlAsset.alphaNum4();
            break;
        case stellar::ASSET_TYPE_CREDIT_ALPHANUM12:
            asset.alphaNum12() = tlAsset.alphaNum12();
            break;
        case stellar::ASSET_TYPE_NATIVE:
        case stellar::ASSET_TYPE_POOL_SHARE:
        default:
            return "Invalid asset in trustline";
        }

        auto const& trustlineOwner = current
                                         ? current->data.trustLine().accountID
                                         : previous->data.trustLine().accountID;

        LedgerKey accKey(ACCOUNT);
        accKey.account().accountID = trustlineOwner;
        auto eventDiff = consumeAmount(accKey, asset, agg.mEventAmounts);

        auto entryDiff = (current ? current->data.trustLine().balance : 0) -
                         (previous ? previous->data.trustLine().balance : 0);

        return entryDiff == eventDiff ? ""
                                      : "Trustline diff does not match events";
    }
    case OFFER:
        break;
    case DATA:
        break;
    case CLAIMABLE_BALANCE:
    {
        auto const& asset = current ? current->data.claimableBalance().asset
                                    : previous->data.claimableBalance().asset;

        auto eventDiff = consumeAmount(lk, asset, agg.mEventAmounts);
        auto entryDiff =
            (current ? current->data.claimableBalance().amount : 0) -
            (previous ? previous->data.claimableBalance().amount : 0);

        return entryDiff == eventDiff
                   ? ""
                   : "ClaimableBalance diff does not match events";
    }
    case LIQUIDITY_POOL:
    {
        auto const* currentBody =
            current ? &current->data.liquidityPool().body.constantProduct()
                    : nullptr;
        auto const* previousBody =
            previous ? &previous->data.liquidityPool().body.constantProduct()
                     : nullptr;

        auto const& assetA =
            (currentBody ? currentBody : previousBody)->params.assetA;
        auto const& assetB =
            (currentBody ? currentBody : previousBody)->params.assetB;

        int64_t entryADiff = (currentBody ? currentBody->reserveA : 0) -
                             (previousBody ? previousBody->reserveA : 0);

        int64_t entryBDiff = (currentBody ? currentBody->reserveB : 0) -
                             (previousBody ? previousBody->reserveB : 0);

        auto eventADiff = consumeAmount(lk, assetA, agg.mEventAmounts);
        auto eventBDiff = consumeAmount(lk, assetB, agg.mEventAmounts);

        return entryADiff == eventADiff || entryBDiff == eventBDiff
                   ? ""
                   : "ClaimableBalance diff does not match events";
    }
    case CONTRACT_DATA:
    {
        auto const& contractData = current ? current->data.contractData()
                                           : previous->data.contractData();

        if (contractData.contract.type() != SC_ADDRESS_TYPE_CONTRACT)
        {
            return "";
        }

        auto assetIt = agg.mStellarAssetContractIDs.find(
            contractData.contract.contractId());
        if (assetIt == agg.mStellarAssetContractIDs.end())
        {
            return "";
        }

        auto const& asset = assetIt->second;

        auto getAmount = [](LedgerEntry const* entry) -> __int128_t {
            if (!entry)
            {
                return 0;
            }

            // TODO: Do we need this validation?
            // The amount should be the first entry in the SCMap
            auto const& val = entry->data.contractData().val;
            if (val.type() == SCV_MAP && val.map() && val.map()->size() != 0)
            {
                auto const& amountEntry = val.map()->at(0);
                return getAmountFromData(amountEntry.val);
            }
            return 0;
        };

        auto eventDiff = consumeAmount(lk, asset, agg.mEventAmounts);
        auto entryDiff = getAmount(current) - getAmount(previous);

        return entryDiff == eventDiff
                   ? ""
                   : "ContractData diff does not match events";
    }
    case CONTRACT_CODE:
        break;
    case CONFIG_SETTING:
        break;
    case TTL:
        break;
    }
    return "";
}

static std::string
verifyEventsDelta(AggregatedEvents const& agg,
                  std::shared_ptr<InternalLedgerEntry const> const& genCurrent,
                  std::shared_ptr<InternalLedgerEntry const> const& genPrevious)
{
    auto type = genCurrent ? genCurrent->type() : genPrevious->type();
    if (type == InternalLedgerEntryType::LEDGER_ENTRY)
    {
        auto const* current = genCurrent ? &genCurrent->ledgerEntry() : nullptr;
        auto const* previous =
            genPrevious ? &genPrevious->ledgerEntry() : nullptr;

        return calculateDeltaBalance(agg, current, previous);
    }
    return "";
}

// TODO: move to TransactionUtils
static LedgerKey
addressToLedgerKey(SCAddress const& address)
{
    LedgerKey lk;
    switch (address.type())
    {
    case SC_ADDRESS_TYPE_ACCOUNT:
        lk.type(ACCOUNT);
        lk.account().accountID = address.accountId();
        break;
    case SC_ADDRESS_TYPE_MUXED_ACCOUNT:
        lk.type(ACCOUNT);
        lk.account().accountID.ed25519() = address.muxedAccount().ed25519;
        break;
    case SC_ADDRESS_TYPE_CONTRACT:
        lk.type(CONTRACT_CODE);
        lk.contractCode().hash = address.contractId();
        break;
    case SC_ADDRESS_TYPE_CLAIMABLE_BALANCE:
        lk.type(CLAIMABLE_BALANCE);
        lk.claimableBalance().balanceID = address.claimableBalanceId();
        break;
    case SC_ADDRESS_TYPE_LIQUIDITY_POOL:
        lk.type(LIQUIDITY_POOL);
        lk.liquidityPool().liquidityPoolID = address.liquidityPoolId();
        break;
    default:
        break;
    }

    return lk;
}

static AggregatedEvents
aggregateEventDiffs(Hash const& networkID,
                    std::vector<ContractEvent> const& events)
{
    AggregatedEvents res;
    for (auto const& event : events)
    {
        if (!event.contractID)
        {
            continue;
        }
        auto const& topics = event.body.v0().topics;
        Asset asset;
        auto assetIt = res.mStellarAssetContractIDs.find(*event.contractID);
        if (assetIt != res.mStellarAssetContractIDs.end())
        {
            asset = assetIt->second;
        }
        else
        {
            if (topics.empty())
            {
                continue;
            }

            // The last topic will be the SEP-0011 asset string for the SAC
            auto assetVal = topics.at(topics.size() - 1);
            if (assetVal.type() != SCV_STRING)
            {
                continue;
            }
            auto const& assetStr = assetVal.str();
            if (assetStr == "native")
            {
                asset.type(ASSET_TYPE_NATIVE);
            }
            else
            {
                auto delimPos = assetStr.find(':');
                if (delimPos == std::string::npos ||
                    delimPos == assetStr.size() - 1)
                {
                    continue;
                }
                auto issuerStr = assetStr.substr(delimPos + 1, assetStr.size());

                PublicKey issuer;

                try
                {
                    issuer = KeyUtils::fromStrKey<PublicKey>(issuerStr);
                }
                catch (std::invalid_argument)
                {
                    continue;
                }

                auto assetName = assetStr.substr(0, delimPos);
                if (assetName.size() <= 4)
                {
                    asset.type(ASSET_TYPE_CREDIT_ALPHANUM4);
                    strToAssetCode(asset.alphaNum4().assetCode, assetName);
                    asset.alphaNum4().issuer = issuer;
                }
                else if (assetName.size() <= 12)
                {
                    asset.type(ASSET_TYPE_CREDIT_ALPHANUM12);
                    strToAssetCode(asset.alphaNum12().assetCode, assetName);
                    asset.alphaNum12().issuer = issuer;
                }
                else
                {
                    continue;
                }

                // The protocol version check is only used for pooShareAssets,
                // and we aren't passing one in here, so the value does not
                // matter
                if (!isAssetValid<Asset>(asset, 0))
                {
                    continue;
                }
            }
            auto hash = getAssetContractID(networkID, asset);
            if (hash != *event.contractID)
            {
                continue;
            }
            res.mStellarAssetContractIDs.emplace(*event.contractID, asset);
        }
        // at this point, we have verified that this is an SAC event

        auto eventNameVal = topics.at(0);
        if (eventNameVal.type() != SCV_SYMBOL)
        {
            continue;
        }

        if (eventNameVal.sym() == "transfer")
        {
            if (topics.size() != 4)
            {
                continue;
            }

            // TODO: Do we need to check all types?
            auto const& fromVal = topics.at(1);
            auto const& toVal = topics.at(2);

            auto fromLk = addressToLedgerKey(fromVal.address());
            auto toLk = addressToLedgerKey(toVal.address());

            auto amount = getAmountFromData(event.body.v0().data);

            // If the events are sane, we should never overflow.
            // TODO: Handle the overflow case anyways?
            res.mEventAmounts[fromLk][asset] -= amount;
            res.mEventAmounts[toLk][asset] += amount;
        }
        else if (eventNameVal.sym() == "mint")
        {
            SCVal toVal;
            if (topics.size() == 4)
            {
                toVal = topics.at(2);
            }
            else if (topics.size() == 3)
            {
                toVal = topics.at(1);
            }
            else
            {
                continue;
            }

            auto toLk = addressToLedgerKey(toVal.address());
            auto amount = getAmountFromData(event.body.v0().data);
            res.mEventAmounts[toLk][asset] += amount;
        }
        else if (eventNameVal.sym() == "burn" ||
                 eventNameVal.sym() == "clawback")
        {
            SCVal fromVal;
            if (topics.size() == 4)
            {
                fromVal = topics.at(2);
            }
            else if (topics.size() == 3)
            {
                fromVal = topics.at(1);
            }
            else
            {
                continue;
            }

            auto fromLk = addressToLedgerKey(fromVal.address());
            auto amount = getAmountFromData(event.body.v0().data);
            res.mEventAmounts[fromLk][asset] += amount;
        }
    }
    return res;
}

EventsAreConsistentWithEntryDiffs::EventsAreConsistentWithEntryDiffs(
    Hash const& networkID)
    : Invariant(false), mNetworkID(networkID)
{
}

std::shared_ptr<Invariant>
EventsAreConsistentWithEntryDiffs::registerInvariant(Application& app)
{
    return app.getInvariantManager()
        .registerInvariant<EventsAreConsistentWithEntryDiffs>(
            app.getNetworkID());
}

std::string
EventsAreConsistentWithEntryDiffs::getName() const
{
    return "EventsAreConsistentWithEntryDiffs";
}

// Note that this invariant only verifies balance changes in the context of an
// operation. The fee events should not be accounted for here.
std::string
EventsAreConsistentWithEntryDiffs::checkOnOperationApply(
    Operation const& operation, OperationResult const& result,
    LedgerTxnDelta const& ltxDelta, std::vector<ContractEvent> const& events)
{
    auto aggregatedEventAmounts = aggregateEventDiffs(mNetworkID, events);

    for (auto const& delta : ltxDelta.entry)
    {
        auto res =
            verifyEventsDelta(aggregatedEventAmounts, delta.second.current,
                              delta.second.previous);
        if (!res.empty())
        {
            return res;
        }
    }

    if (!aggregatedEventAmounts.mEventAmounts.empty())
    {
        return "Some event diffs not consumed";
    }
    return {};
}
}

#endif