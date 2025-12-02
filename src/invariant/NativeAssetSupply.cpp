// Copyright 2025 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "invariant/NativeAssetSupply.h"
#include "bucket/BucketInputIterator.h"
#include "bucket/BucketSnapshot.h"
#include "bucket/BucketSnapshotManager.h"
#include "bucket/HotArchiveBucket.h"
#include "bucket/LiveBucket.h"
#include "bucket/SearchableBucketList.h"
#include "invariant/InvariantManager.h"
#include "ledger/LedgerManager.h"
#include "ledger/LedgerTypeUtils.h"
#include "main/Application.h"
#include "transactions/TransactionUtils.h"
#include "util/GlobalChecks.h"
#include "util/LogSlowExecution.h"
#include "util/XDRCereal.h"
#include "util/types.h"
#include "xdr/Stellar-ledger-entries.h"
#include <fmt/format.h>

namespace stellar
{
static Loop
scanLiveBucket(LiveBucketSnapshot const& bucket,
               UnorderedSet<LedgerKey>& seenKeys, Asset const& asset,
               AssetContractInfo const& assetContractInfo, int64_t& sumBalance)
{
    // Iterate through all entries in this bucket
    for (LiveBucketInputIterator iter(bucket.getRawBucket()); iter; ++iter)
    {
        auto const& be = *iter;
        if (be.type() == LIVEENTRY || be.type() == INITENTRY)
        {
            auto lk = LedgerEntryKey(be.liveEntry());
            auto [_, wasInserted] = seenKeys.emplace(lk);

            // Only process if this is the newest version of the entry
            if (wasInserted)
            {
                auto balance =
                    getAssetBalance(be.liveEntry(), asset, assetContractInfo);

                if (!balance)
                {
                    CLOG_ERROR(Tx,
                               "NativeAssetSupply: getAssetBalance "
                               "overflow for live key: {}",
                               xdrToCerealString(lk, "ledger_key"));
                    return Loop::COMPLETE;
                }
                if (balance && *balance > 0)
                {
                    if (!addBalance(sumBalance, *balance))
                    {
                        CLOG_ERROR(Tx,
                                   "NativeAssetSupply: Overflow adding "
                                   "balance for live key: {}",
                                   xdrToCerealString(lk, "ledger_key"));
                        return Loop::COMPLETE;
                    }
                }
            }
        }
        else if (be.type() == DEADENTRY)
        {
            seenKeys.emplace(be.deadEntry());
        }
    }
    return Loop::INCOMPLETE;
}

static Loop
scanHotArchiveBucket(HotArchiveBucketSnapshot const& bucket,
                     UnorderedSet<LedgerKey>& seenKeys, Asset const& asset,
                     AssetContractInfo const& assetContractInfo,
                     int64_t& sumBalance)
{
    // Iterate through all entries in this bucket
    for (HotArchiveBucketInputIterator iter(bucket.getRawBucket()); iter;
         ++iter)
    {
        auto const& be = *iter;
        if (be.type() == HOT_ARCHIVE_ARCHIVED)
        {
            auto lk = LedgerEntryKey(be.archivedEntry());
            auto [_, wasInserted] = seenKeys.emplace(lk);

            // Only process if this is the newest version of the entry
            if (wasInserted)
            {
                auto const& entry = be.archivedEntry();
                auto balance =
                    getAssetBalance(entry, asset, assetContractInfo);
                if (balance && *balance > 0)
                {
                    if (!addBalance(sumBalance, *balance))
                    {
                        CLOG_ERROR(Tx,
                                   "NativeAssetSupply: Overflow adding "
                                   "balance for archived key: {}",
                                   xdrToCerealString(lk, "ledger_key"));
                        return Loop::COMPLETE;
                    }
                }
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

NativeAssetSupply::NativeAssetSupply(
    AssetContractInfo const& nativeContractInfo)
    : Invariant(true), mNativeContractInfo(nativeContractInfo)
{
}

std::string
NativeAssetSupply::checkSnapshot(CompleteConstLedgerStatePtr ledgerState,
                                 InMemorySorobanState const& inMemorySnapshot)
{
    CLOG_ERROR(Tx, "ENTERING CHECK SNAPSHOT FOR NATIVE ASSET SUPPLY INVARIANT");

    LogSlowExecution logSlow("NativeAssetSupply::checkSnapshot",
                             LogSlowExecution::Mode::AUTOMATIC_RAII, "took",
                             std::chrono::seconds(30));

    auto liveSnapshot = ledgerState->getBucketSnapshot();
    auto hotArchiveSnapshot = ledgerState->getHotArchiveSnapshot();
    auto const& header = liveSnapshot->getLedgerHeader();

    Asset nativeAsset(ASSET_TYPE_NATIVE);

    int64_t sumBalance = 0;

    // Start with the fee pool from the ledger header
    if (!addBalance(sumBalance, header.feePool))
    {
        return fmt::format(
            FMT_STRING("NativeAssetSupply invariant failed: "
                       "Fee pool balance overflowed when added to total. "
                       "Current sum: {}, Fee pool: {}"),
            sumBalance, header.feePool);
    }

    // Scan the Live BucketList for native balances using loopAllBuckets
    UnorderedSet<LedgerKey> seenKeys;
    liveSnapshot->loopAllBuckets(
        [&seenKeys, &nativeAsset, &sumBalance, this](
            LiveBucketSnapshot const& bucket) {
            return scanLiveBucket(bucket, seenKeys, nativeAsset,
                                  mNativeContractInfo, sumBalance);
        });

    // Scan the Hot Archive for native balances using loopAllBuckets
    seenKeys.clear();
    hotArchiveSnapshot->loopAllBuckets(
        [&seenKeys, &nativeAsset, &sumBalance, this](
            HotArchiveBucketSnapshot const& bucket) {
            return scanHotArchiveBucket(bucket, seenKeys, nativeAsset,
                                        mNativeContractInfo, sumBalance);
        });

    // Compare the calculated total with totalCoins from the ledger header
    if (sumBalance != header.totalCoins)
    {
        return fmt::format(
            FMT_STRING("NativeAssetSupply invariant failed: "
                       "Total native asset supply mismatch. "
                       "Calculated from buckets: {}, Expected (totalCoins): "
                       "{}, Difference: {}"),
            sumBalance, header.totalCoins, header.totalCoins - sumBalance);
    }

    CLOG_ERROR(Tx,
               "!!! LEAVING CHECK SNAPSHOT FOR NATIVE ASSET SUPPLY INVARIANT");

    return std::string{};
}

std::shared_ptr<Invariant>
NativeAssetSupply::registerInvariant(Application& app)
{
    Asset nativeAsset(ASSET_TYPE_NATIVE);
    auto nativeInfo = getAssetContractInfo(nativeAsset, app.getNetworkID());

    return app.getInvariantManager().registerInvariant<NativeAssetSupply>(
        nativeInfo);
}

std::string
NativeAssetSupply::getName() const
{
    return "NativeAssetSupply";
}
}
