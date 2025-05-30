// Copyright 2024 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "bucket/BucketSnapshotManager.h"
#include "bucket/BucketUtils.h"
#include "bucket/HotArchiveBucket.h"
#include "bucket/LiveBucket.h"
#include "bucket/SearchableBucketList.h"
#include "main/Application.h"
#include "util/GlobalChecks.h"
#include "util/XDRStream.h" // IWYU pragma: keep

#include "medida/meter.h"
#include "medida/metrics_registry.h"
#include "xdr/Stellar-ledger-entries.h"
#include <shared_mutex>
#include <xdrpp/types.h>

namespace stellar
{

BucketSnapshotManager::BucketSnapshotManager(
    Application& app, SnapshotPtrT<LiveBucket>&& snapshot,
    SnapshotPtrT<HotArchiveBucket>&& hotArchiveSnapshot,
    uint32_t numLiveHistoricalSnapshots)
    : mApp(app)
    , mCurrLiveSnapshot(std::move(snapshot))
    , mCurrHotArchiveSnapshot(std::move(hotArchiveSnapshot))
    , mLiveHistoricalSnapshots()
    , mHotArchiveHistoricalSnapshots()
    , mNumHistoricalSnapshots(numLiveHistoricalSnapshots)
    , mBulkLoadMeter(app.getMetrics().NewMeter(
          {"bucketlistDB", "query", "loads"}, "query"))
    , mBloomMisses(app.getMetrics().NewMeter(
          {"bucketlistDB", "bloom", "misses"}, "bloom"))
    , mBloomLookups(app.getMetrics().NewMeter(
          {"bucketlistDB", "bloom", "lookups"}, "bloom"))
{
    releaseAssert(threadIsMain());
    releaseAssert(mCurrLiveSnapshot);
    releaseAssert(mCurrHotArchiveSnapshot);

    // Initialize point load timers for each LedgerEntry type
    for (auto t : xdr::xdr_traits<LedgerEntryType>::enum_values())
    {
        auto const& label = xdr::xdr_traits<LedgerEntryType>::enum_name(
            static_cast<LedgerEntryType>(t));
        auto& metric =
            mApp.getMetrics().NewTimer({"bucketlistDB", "point", label});
        mPointTimers.emplace(static_cast<LedgerEntryType>(t), metric);
    }
}

template <class BucketT>
std::map<uint32_t, SnapshotPtrT<BucketT>>
copyHistoricalSnapshots(
    std::map<uint32_t, SnapshotPtrT<BucketT>> const& snapshots)
{
    std::map<uint32_t, SnapshotPtrT<BucketT>> copiedSnapshots;
    for (auto const& [ledgerSeq, snap] : snapshots)
    {
        copiedSnapshots.emplace(
            ledgerSeq,
            std::make_unique<BucketListSnapshot<BucketT> const>(*snap));
    }
    return copiedSnapshots;
}

SearchableSnapshotConstPtr
BucketSnapshotManager::copySearchableLiveBucketListSnapshot() const
{
    // Can't use std::make_shared due to private constructor
    return std::shared_ptr<SearchableLiveBucketListSnapshot>(
        new SearchableLiveBucketListSnapshot(
            *this,
            std::make_unique<BucketListSnapshot<LiveBucket>>(
                *mCurrLiveSnapshot),
            copyHistoricalSnapshots(mLiveHistoricalSnapshots)));
}

SearchableHotArchiveSnapshotConstPtr
BucketSnapshotManager::copySearchableHotArchiveBucketListSnapshot() const
{
    releaseAssert(mCurrHotArchiveSnapshot);
    // Can't use std::make_shared due to private constructor
    return std::shared_ptr<SearchableHotArchiveBucketListSnapshot>(
        new SearchableHotArchiveBucketListSnapshot(
            *this,
            std::make_unique<BucketListSnapshot<HotArchiveBucket>>(
                *mCurrHotArchiveSnapshot),
            copyHistoricalSnapshots(mHotArchiveHistoricalSnapshots)));
}

medida::Timer&
BucketSnapshotManager::recordBulkLoadMetrics(std::string const& label,
                                             size_t numEntries) const
{
    // For now, only keep metrics for the main thread. We can decide on what
    // metrics make sense when more background services are added later.

    if (numEntries != 0)
    {
        mBulkLoadMeter.Mark(numEntries);
    }

    auto iter = mBulkTimers.find(label);
    if (iter == mBulkTimers.end())
    {
        auto& metric =
            mApp.getMetrics().NewTimer({"bucketlistDB", "bulk", label});
        iter = mBulkTimers.emplace(label, metric).first;
    }

    return iter->second;
}

void
BucketSnapshotManager::maybeCopySearchableBucketListSnapshot(
    SearchableSnapshotConstPtr& snapshot)
{
    // The canonical snapshot held by the BucketSnapshotManager is not being
    // modified. Rather, a thread is checking it's copy against the canonical
    // snapshot, so use a shared lock.
    std::shared_lock<std::shared_mutex> lock(mSnapshotMutex);

    if (!snapshot ||
        snapshot->getLedgerSeq() < mCurrLiveSnapshot->getLedgerSeq())
    {
        snapshot = copySearchableLiveBucketListSnapshot();
    }
}

void
BucketSnapshotManager::maybeCopySearchableHotArchiveBucketListSnapshot(
    SearchableHotArchiveSnapshotConstPtr& snapshot)
{
    // The canonical snapshot held by the BucketSnapshotManager is not being
    // modified. Rather, a thread is checking it's copy against the canonical
    // snapshot, so use a shared lock.
    std::shared_lock<std::shared_mutex> lock(mSnapshotMutex);

    if (!snapshot ||
        snapshot->getLedgerSeq() < mCurrHotArchiveSnapshot->getLedgerSeq())
    {
        snapshot = copySearchableHotArchiveBucketListSnapshot();
    }
}

void
BucketSnapshotManager::updateCurrentSnapshot(
    SnapshotPtrT<LiveBucket>&& liveSnapshot,
    SnapshotPtrT<HotArchiveBucket>&& hotArchiveSnapshot)
{
    auto updateSnapshot = [numHistoricalSnapshots = mNumHistoricalSnapshots](
                              auto& currentSnapshot, auto& historicalSnapshots,
                              auto&& newSnapshot) {
        releaseAssert(newSnapshot);
        releaseAssert(!currentSnapshot || newSnapshot->getLedgerSeq() >=
                                              currentSnapshot->getLedgerSeq());

        // First update historical snapshots
        if (numHistoricalSnapshots != 0)
        {
            // If historical snapshots are full, delete the oldest one
            if (historicalSnapshots.size() == numHistoricalSnapshots)
            {
                historicalSnapshots.erase(historicalSnapshots.begin());
            }

            historicalSnapshots.emplace(currentSnapshot->getLedgerSeq(),
                                        std::move(currentSnapshot));
            currentSnapshot = nullptr;
        }

        currentSnapshot.swap(newSnapshot);
    };

    // Updating the BucketSnapshotManager canonical snapshot, must lock
    // exclusively for write access.
    std::unique_lock<std::shared_mutex> lock(mSnapshotMutex);
    updateSnapshot(mCurrLiveSnapshot, mLiveHistoricalSnapshots, liveSnapshot);
    updateSnapshot(mCurrHotArchiveSnapshot, mHotArchiveHistoricalSnapshots,
                   hotArchiveSnapshot);
}

void
BucketSnapshotManager::startPointLoadTimer() const
{
    releaseAssert(threadIsMain());
    releaseAssert(!mTimerStart);
    mTimerStart = mApp.getClock().now();
}

void
BucketSnapshotManager::endPointLoadTimer(LedgerEntryType t,
                                         bool bloomMiss) const
{
    releaseAssert(threadIsMain());
    releaseAssert(mTimerStart);
    auto duration = mApp.getClock().now() - *mTimerStart;
    mTimerStart.reset();

    // We expect about 0.1% of lookups to encounter a bloom miss. To avoid noise
    // in disk performance metrics, we only track metrics for entries that did
    // not encounter a bloom miss.
    if (!bloomMiss)
    {
        auto iter = mPointTimers.find(t);
        releaseAssert(iter != mPointTimers.end());
        iter->second.Update(duration);
    }
}
}