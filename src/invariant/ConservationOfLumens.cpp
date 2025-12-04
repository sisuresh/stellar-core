// Copyright 2017 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "invariant/ConservationOfLumens.h"
#include "bucket/BucketInputIterator.h"
#include "bucket/BucketSnapshot.h"
#include "bucket/BucketSnapshotManager.h"
#include "bucket/HotArchiveBucket.h"
#include "bucket/LiveBucket.h"
#include "bucket/SearchableBucketList.h"
#include "crypto/SHA.h"
#include "invariant/InvariantManager.h"
#include "ledger/LedgerManager.h"
#include "ledger/LedgerTxn.h"
#include "ledger/LedgerTypeUtils.h"
#include "main/Application.h"
#include "transactions/TransactionUtils.h"
#include "util/GlobalChecks.h"
#include "util/LogSlowExecution.h"
#include "util/XDRCereal.h"
#include "util/types.h"
#include "xdr/Stellar-ledger-entries.h"
#include <fmt/format.h>
#include <numeric>

namespace stellar
{

static std::optional<int64_t>
calculateDeltaBalance(AssetContractInfo const& lumenContractInfo,
                      LedgerEntry const* current, LedgerEntry const* previous)
{
    releaseAssert(current || previous);
    auto currentBalance =
        current ? getAssetBalance(*current, Asset(ASSET_TYPE_NATIVE),
                                  lumenContractInfo)
                : std::optional<int64_t>{0};
    auto previousBalance =
        previous ? getAssetBalance(*previous, Asset(ASSET_TYPE_NATIVE),
                                   lumenContractInfo)
                 : std::optional<int64_t>{0};
    if (!currentBalance || !previousBalance)
    {
        // something went wrong trying to get the balance. Fail the invariant.
        return std::nullopt;
    }

    return (currentBalance ? *currentBalance : 0) -
           (previousBalance ? *previousBalance : 0);
}

static std::optional<int64_t>
calculateDeltaBalance(
    AssetContractInfo const& lumenContractInfo,
    std::shared_ptr<InternalLedgerEntry const> const& genCurrent,
    std::shared_ptr<InternalLedgerEntry const> const& genPrevious)
{
    auto type = genCurrent ? genCurrent->type() : genPrevious->type();
    if (type == InternalLedgerEntryType::LEDGER_ENTRY)
    {
        auto const* current = genCurrent ? &genCurrent->ledgerEntry() : nullptr;
        auto const* previous =
            genPrevious ? &genPrevious->ledgerEntry() : nullptr;

        return calculateDeltaBalance(lumenContractInfo, current, previous);
    }
    return 0;
}

ConservationOfLumens::ConservationOfLumens(
    AssetContractInfo const& lumenContractInfo)
    : Invariant(false), mLumenContractInfo(lumenContractInfo)
{
}

std::shared_ptr<Invariant>
ConservationOfLumens::registerInvariant(Application& app)
{
    Asset native(ASSET_TYPE_NATIVE);
    // We need to keep track of lumens in the Stellar Asset Contract, so
    // calculate the lumen contractID, the key of the Balance entry, and the
    // amount field within that entry.
    auto lumenInfo = getAssetContractInfo(native, app.getNetworkID());

    return app.getInvariantManager().registerInvariant<ConservationOfLumens>(
        lumenInfo);
}

std::string
ConservationOfLumens::getName() const
{
    return "ConservationOfLumens";
}

std::string
ConservationOfLumens::checkOnOperationApply(
    Operation const& operation, OperationResult const& result,
    LedgerTxnDelta const& ltxDelta, std::vector<ContractEvent> const& events,
    AppConnector&)
{
    auto const& lhCurr = ltxDelta.header.current;
    auto const& lhPrev = ltxDelta.header.previous;

    int64_t deltaTotalCoins = lhCurr.totalCoins - lhPrev.totalCoins;
    int64_t deltaFeePool = lhCurr.feePool - lhPrev.feePool;

    int64_t deltaBalances = 0;
    for (auto const& entryPair : ltxDelta.entry)
    {
        auto const& entryDelta = entryPair.second;
        auto delta = stellar::calculateDeltaBalance(
            mLumenContractInfo, entryDelta.current, entryDelta.previous);
        if (!delta)
        {
            return "Could not calculate lumen balance delta for an entry";
        }

        // Check for overflow and underflow
        if (*delta > 0 &&
            deltaBalances > std::numeric_limits<int64_t>::max() - *delta)
        {
            return "Overflow detected when adding to deltaBalances";
        }
        if (*delta < 0 &&
            deltaBalances < std::numeric_limits<int64_t>::min() - *delta)
        {
            return "Underflow detected when adding to deltaBalances";
        }

        deltaBalances += *delta;
    }

    if (result.tr().type() == INFLATION)
    {
        int64_t inflationPayouts =
            std::accumulate(result.tr().inflationResult().payouts().begin(),
                            result.tr().inflationResult().payouts().end(),
                            static_cast<int64_t>(0),
                            [](int64_t lhs, InflationPayout const& rhs) {
                                return lhs + rhs.amount;
                            });
        if (deltaTotalCoins != inflationPayouts + deltaFeePool)
        {
            return fmt::format(
                FMT_STRING(
                    "LedgerHeader totalCoins change ({:d}) did not match"
                    " feePool change ({:d}) plus inflation payouts ({:d})"),
                deltaTotalCoins, deltaFeePool, inflationPayouts);
        }
        if (deltaBalances != inflationPayouts)
        {
            return fmt::format(
                FMT_STRING("LedgerEntry account balances change ({:d}) "
                           "did not match inflation payouts ({:d})"),
                deltaBalances, inflationPayouts);
        }
    }
    else
    {
        if (deltaTotalCoins != 0)
        {
            return fmt::format(
                FMT_STRING("LedgerHeader totalCoins changed from {:d} to"
                           " {:d} without inflation"),
                lhPrev.totalCoins, lhCurr.totalCoins);
        }
        if (deltaFeePool != 0)
        {
            return fmt::format(
                FMT_STRING("LedgerHeader feePool changed from {:d} to"
                           " {:d} without inflation"),
                lhPrev.feePool, lhCurr.feePool);
        }
        if (deltaBalances != 0)
        {
            return fmt::format(
                FMT_STRING("LedgerEntry account balances changed by"
                           " {:d} without inflation"),
                deltaBalances);
        }
    }
    return {};
}

// Helper function that processes an entry if it hasn't been seen before.
// Returns true on success, false on error (with error logged).
static bool
processEntryIfNew(LedgerEntry const& entry, LedgerKey const& key,
                  std::unordered_set<LedgerKey>& seenKeys, Asset const& asset,
                  AssetContractInfo const& assetContractInfo,
                  int64_t& sumBalance)
{
    auto [_, wasInserted] = seenKeys.emplace(key);

    // Only process if this is the newest version of the entry
    if (!wasInserted)
    {
        return true; // Already seen, skip
    }

    auto balance = getAssetBalance(entry, asset, assetContractInfo);

    if (!balance)
    {
        CLOG_ERROR(Tx,
                   "ConservationOfLumens: getAssetBalance overflow for key: {}",
                   xdrToCerealString(key, "ledger_key"));
        return false;
    }

    if (*balance > 0)
    {
        if (!addBalance(sumBalance, *balance))
        {
            CLOG_ERROR(
                Tx, "ConservationOfLumens: Overflow adding balance for key: {}",
                xdrToCerealString(key, "ledger_key"));
            return false;
        }
    }

    return true;
}

static bool
canHoldNative(LedgerEntryType type)
{
    switch (type)
    {
    case ACCOUNT:
    case CLAIMABLE_BALANCE:
    case CONTRACT_DATA:
    case LIQUIDITY_POOL:
        return true;
    default:
        return false;
    }
}

static Loop
scanLiveBucket(LiveBucketSnapshot const& bucket,
               std::unordered_set<LedgerKey>& seenKeys, Asset const& asset,
               AssetContractInfo const& assetContractInfo, int64_t& sumBalance,
               std::function<bool()> const& isStopping)
{
    for (LiveBucketInputIterator iter(bucket.getRawBucket()); iter; ++iter)
    {
        // Allow early termination if application is stopping
        if (isStopping())
        {
            return Loop::COMPLETE;
        }

        auto const& be = *iter;
        if (be.type() == LIVEENTRY || be.type() == INITENTRY)
        {
            if (!canHoldNative(be.liveEntry().data.type()))
            {
                continue;
            }
            if (!processEntryIfNew(be.liveEntry(),
                                   LedgerEntryKey(be.liveEntry()), seenKeys,
                                   asset, assetContractInfo, sumBalance))
            {
                return Loop::COMPLETE;
            }
        }
        else if (be.type() == DEADENTRY && canHoldNative(be.deadEntry().type()))
        {
            seenKeys.emplace(be.deadEntry());
        }
    }
    return Loop::INCOMPLETE;
}

static Loop
scanHotArchiveBucket(HotArchiveBucketSnapshot const& bucket,
                     std::unordered_set<LedgerKey>& seenKeys,
                     Asset const& asset,
                     AssetContractInfo const& assetContractInfo,
                     int64_t& sumBalance,
                     std::function<bool()> const& isStopping)
{
    for (HotArchiveBucketInputIterator iter(bucket.getRawBucket()); iter;
         ++iter)
    {
        // Allow early termination if application is stopping
        if (isStopping())
        {
            return Loop::COMPLETE;
        }

        auto const& be = *iter;
        if (be.type() == HOT_ARCHIVE_ARCHIVED)
        {
            if (!processEntryIfNew(be.archivedEntry(),
                                   LedgerEntryKey(be.archivedEntry()), seenKeys,
                                   asset, assetContractInfo, sumBalance))
            {
                return Loop::COMPLETE;
            }
        }
        else if (be.type() == HOT_ARCHIVE_LIVE)
        {
            // HOT_ARCHIVE_LIVE means entry was restored from archive,
            // so mark it as seen (shadowing any archived versions)
            seenKeys.emplace(be.key());
        }
    }
    return Loop::INCOMPLETE;
}

std::string
ConservationOfLumens::checkSnapshot(
    CompleteConstLedgerStatePtr ledgerState,
    InMemorySorobanState const& inMemorySnapshot,
    std::function<bool()> isStopping)
{
    LogSlowExecution logSlow("ConservationOfLumens::checkSnapshot",
                             LogSlowExecution::Mode::AUTOMATIC_RAII, "took",
                             std::chrono::seconds(300));

    auto liveSnapshot = ledgerState->getBucketSnapshot();
    auto hotArchiveSnapshot = ledgerState->getHotArchiveSnapshot();
    auto const& header = liveSnapshot->getLedgerHeader();

    Asset nativeAsset(ASSET_TYPE_NATIVE);

    int64_t sumBalance = 0;

    // Start with the fee pool from the ledger header
    if (!addBalance(sumBalance, header.feePool))
    {
        return fmt::format(
            FMT_STRING("ConservationOfLumens invariant failed: "
                       "Fee pool balance overflowed when added to total. "
                       "Current sum: {}, Fee pool: {}"),
            sumBalance, header.feePool);
    }

    // Scan the Live BucketList for native balances using loopAllBuckets
    {
        std::unordered_set<LedgerKey> seenKeys;
        liveSnapshot->loopAllBuckets([&seenKeys, &nativeAsset, &sumBalance,
                                      &isStopping,
                                      this](LiveBucketSnapshot const& bucket) {
            return scanLiveBucket(bucket, seenKeys, nativeAsset,
                                  mLumenContractInfo, sumBalance, isStopping);
        });
    }

    // Check if we should stop before scanning hot archive
    if (isStopping())
    {
        return std::string{};
    }

    // Scan the Hot Archive for native balances using loopAllBuckets
    {
        std::unordered_set<LedgerKey> seenKeys;
        hotArchiveSnapshot->loopAllBuckets(
            [&seenKeys, &nativeAsset, &sumBalance, &isStopping,
             this](HotArchiveBucketSnapshot const& bucket) {
                return scanHotArchiveBucket(bucket, seenKeys, nativeAsset,
                                            mLumenContractInfo, sumBalance,
                                            isStopping);
            });
    }

    // We stopped early, so it's likely we didn't finish scanning everything
    if (isStopping())
    {
        return std::string{};
    }

    // Compare the calculated total with totalCoins from the ledger header
    if (sumBalance != header.totalCoins)
    {
        return fmt::format(
            FMT_STRING("ConservationOfLumens invariant failed: "
                       "Total native asset supply mismatch. "
                       "Calculated from buckets: {}, Expected (totalCoins): "
                       "{}, Difference: {}"),
            sumBalance, header.totalCoins, header.totalCoins - sumBalance);
    }
    return std::string{};
}
}
