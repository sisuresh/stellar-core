// Copyright 2025 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "invariant/EventsAreConsistentWithEntryDiffs.h"
#include "crypto/SHA.h"
#include "invariant/InvariantManager.h"
#include "ledger/LedgerTxn.h"
#include "main/Application.h"
#include "util/GlobalChecks.h"
#include <fmt/format.h>
#include <numeric>
#include "rust/RustBridge.h"

namespace stellar
{

static const CxxI128 I128ZERO{0, 0};

struct AggregatedEvents
{
    mutable UnorderedMap<LedgerKey, UnorderedMap<Asset, CxxI128>>
        mEventAmounts;
    UnorderedMap<Hash, Asset> mStellarAssetContractIDs;

    void addAssetBalance(LedgerKey const& lk, Asset const& asset, CxxI128 const& amount);

    void subtractAssetBalance(LedgerKey const& lk, Asset const& asset, CxxI128 const& amount);
};

void AggregatedEvents::addAssetBalance(LedgerKey const& lk, Asset const& asset, CxxI128 const& amount) {
    mEventAmounts[lk][asset] = rust_bridge::i128_add(mEventAmounts[lk][asset], amount);
}

void AggregatedEvents::subtractAssetBalance(LedgerKey const& lk, Asset const& asset, CxxI128 const& amount) {
    mEventAmounts[lk][asset] = rust_bridge::i128_sub(mEventAmounts[lk][asset], amount);
}

static CxxI128
getAmountFromData(SCVal const& data)
{
    if (data.type() == SCV_I128)
    {
        auto const& amountVal = data.i128();
        return CxxI128{amountVal.hi, amountVal.lo};
    }
    else if (data.type() == SCV_MAP && data.map())
    {
        auto const& map = *data.map();
        for (auto const& entry : map)
        {
            if (entry.key.type() == SCV_SYMBOL && entry.key.sym() == "amount" &&
                entry.val.type() == SCV_I128)
            {
                auto const& amountVal = entry.val.i128();
                return CxxI128{amountVal.hi, amountVal.lo};
            }
        }
    }
    return I128ZERO;
}

static CxxI128
consumeAmount(
    LedgerKey const& lk, Asset const& asset,
    UnorderedMap<LedgerKey, UnorderedMap<Asset, CxxI128>>& eventAmounts)
{
    // If the key is ContractData, then it is a balance entry. We need to
    // convert it to the ledger key for the address that the balance belongs
    // too.
    LedgerKey ledgerKeyToLookup;
    if (lk.type() == CONTRACT_DATA)
    {
        if (lk.contractData().key.type() != SCV_VEC)
        {
            return I128ZERO;
        }

        auto const& vec = lk.contractData().key.vec();
        if (!vec || vec->size() != 2)
        {
            return I128ZERO;
        }

        auto const& addr = vec->at(1);

        if (addr.type() != SCV_ADDRESS)
        {
            return I128ZERO;
        }

        ledgerKeyToLookup = addressToLedgerKey(addr.address());
    }
    else
    {
        ledgerKeyToLookup = lk;
    }

    auto lkAssetMapIt = eventAmounts.find(ledgerKeyToLookup);
    if (lkAssetMapIt == eventAmounts.end())
    {
        return I128ZERO;
    }

    auto& lkAssetMap = lkAssetMapIt->second;
    auto assetAmountIt = lkAssetMap.find(asset);
    if (assetAmountIt == lkAssetMap.end())
    {
        return I128ZERO;
    }

    auto res = assetAmountIt->second;

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

        return entryDiff == (int64)eventDiff.lo ? ""
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
            asset.type(ASSET_TYPE_CREDIT_ALPHANUM4);
            asset.alphaNum4() = tlAsset.alphaNum4();
            break;
        case stellar::ASSET_TYPE_CREDIT_ALPHANUM12:
            asset.type(ASSET_TYPE_CREDIT_ALPHANUM12);
            asset.alphaNum12() = tlAsset.alphaNum12();
            break;
        case stellar::ASSET_TYPE_POOL_SHARE:
            return "";
        case stellar::ASSET_TYPE_NATIVE:
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

        return entryDiff == (int64) eventDiff.lo ? ""
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

        return entryDiff == (int64) eventDiff.lo
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

        return entryADiff == (int64) eventADiff.lo && entryBDiff == (int64) eventBDiff.lo
                   ? ""
                   : "LiquidityPool diff does not match events";
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

        auto getAmount = [](LedgerEntry const* entry) -> CxxI128 {
            if (!entry)
            {
                return I128ZERO;
            }

            // Make sure this is the balance entry and not an allowance
            auto const& dataKey = entry->data.contractData().key;
            if (dataKey.type() != SCV_VEC || !dataKey.vec() ||
                dataKey.vec()->size() != 2)
            {
                return I128ZERO;
            }
            auto const& name = dataKey.vec()->at(0);
            if (name.type() != SCV_SYMBOL || name.sym() != "Balance")
            {
                return I128ZERO;
            }

            // The amount should be the first entry in the SCMap
            auto const& val = entry->data.contractData().val;
            if (val.type() == SCV_MAP && val.map() && val.map()->size() != 0)
            {
                auto const& amountEntry = val.map()->at(0);
                return getAmountFromData(amountEntry.val);
            }
            return I128ZERO;
        };

        auto eventDiff = consumeAmount(lk, asset, agg.mEventAmounts);
        auto entryDiff = rust_bridge::i128_sub(getAmount(current), getAmount(previous));
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
            auto maybeAsset = isFromSAC(event, networkID);
            if (maybeAsset)
            {
                asset = *maybeAsset;
                res.mStellarAssetContractIDs.emplace(*event.contractID, asset);
            }
            else
            {
                continue;
            }
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

            auto const& fromVal = topics.at(1);
            auto const& toVal = topics.at(2);

            auto fromLk = addressToLedgerKey(fromVal.address());
            auto toLk = addressToLedgerKey(toVal.address());

            auto amount = getAmountFromData(event.body.v0().data);

            // If the events are sane, we should never overflow.
            // TODO: Handle the overflow case anyways?
            res.subtractAssetBalance(fromLk, asset, amount);
            res.addAssetBalance(toLk, asset, amount);
        }
        else if (eventNameVal.sym() == "mint")
        {
            if (topics.size() != 3)
            {
                continue;
            }

            auto toVal = topics.at(1);

            auto toLk = addressToLedgerKey(toVal.address());
            auto amount = getAmountFromData(event.body.v0().data);
            res.addAssetBalance(toLk, asset, amount);
        }
        else if (eventNameVal.sym() == "burn" ||
                 eventNameVal.sym() == "clawback")
        {
            if (topics.size() != 3)
            {
                continue;
            }

            auto fromVal = topics.at(1);

            auto fromLk = addressToLedgerKey(fromVal.address());
            auto amount = getAmountFromData(event.body.v0().data);
            res.subtractAssetBalance(fromLk, asset, amount);
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

    for (auto const& kvp : aggregatedEventAmounts.mEventAmounts)
    {
        for (auto const& kvp2 : kvp.second)
        {
            if (kvp2.second != I128ZERO)
            {
                return "Some event diffs not consumed";
            }
        }
    }
    return {};
}
}