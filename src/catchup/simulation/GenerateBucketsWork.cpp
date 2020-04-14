// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "catchup/simulation/GenerateBucketsWork.h"
#include "bucket/Bucket.h"
#include "bucket/BucketInputIterator.h"
#include "bucket/BucketList.h"
#include "bucket/BucketManager.h"
#include "bucket/BucketOutputIterator.h"
#include "crypto/SecretKey.h"
#include "crypto/SignerKey.h"
#include "history/HistoryArchive.h"
#include "util/SimulationUtils.h"
#include <list>

namespace stellar
{

GenerateBucketsWork::GenerateBucketsWork(
    Application& app, std::map<std::string, std::shared_ptr<Bucket>>& buckets,
    HistoryArchiveState const& applyState, uint32_t multiplier)
    : BasicWork(app, "generate-buckets", BasicWork::RETRY_NEVER)
    , mApp(app)
    , mBuckets(buckets)
    , mApplyState(applyState)
    , mMultiplier(multiplier)
    , mLevel(0)
{
}

void
GenerateBucketsWork::onReset()
{
    mLevel = 0;
    mGeneratedApplyState = {};
    mGeneratedApplyState.currentLedger = mApplyState.currentLedger;
    mPrevSnap.reset();
    mBuckets.clear();
}

std::shared_ptr<Bucket>
GenerateBucketsWork::getBucketAndUpdateHAS(Hash const& bucketHash, bool isCurr)
{
    auto bucket = mApp.getBucketManager().getBucketByHash(bucketHash);
    Hash emptyHash;
    if (bucket->getHash() != emptyHash)
    {
        CLOG(INFO, "History") << "Simulating " << (isCurr ? "curr" : "snap")
                              << " bucketlist level: " << mLevel;
        auto newBucket = generateNewBucket(bucket, isCurr);
        auto hash = binToHex(newBucket->getHash());
        CLOG(INFO, "History")
            << "Generated bucket: " << hexAbbrev(newBucket->getHash());
        auto& level = mGeneratedApplyState.currentBuckets[mLevel];
        if (isCurr)
        {
            level.curr = hash;
        }
        else
        {
            level.snap = hash;
            mPrevSnap = newBucket;
        }
        return newBucket;
    }
    return bucket;
}

BasicWork::State
GenerateBucketsWork::onRun()
{
    auto const& currentLevel = mApplyState.currentBuckets.at(mLevel);
    mGeneratedApplyState.currentBuckets[mLevel].next.clear();

    try
    {
        auto curr = getBucketAndUpdateHAS(hexToBin256(currentLevel.curr),
                                          /* isCurr */ true);
        setFutureBucket(curr);
        getBucketAndUpdateHAS(hexToBin256(currentLevel.snap),
                              /* isCurr */ false);
    }
    catch (std::runtime_error const& e)
    {
        CLOG(ERROR, "History") << "Unable to generate bucket: " << e.what();
        return BasicWork::State::WORK_FAILURE;
    }

    if (mLevel < BucketList::kNumLevels - 1)
    {
        ++mLevel;
        return BasicWork::State::WORK_RUNNING;
    }

    return BasicWork::State::WORK_SUCCESS;
}

void
GenerateBucketsWork::setFutureBucket(std::shared_ptr<Bucket> const& curr)
{
    auto currLedger = mApplyState.currentLedger;

    if (mPrevSnap && mLevel > 0)
    {
        auto preparedCurr = BucketList::mergeWithEmptyCurr(currLedger, mLevel)
                                ? std::make_shared<Bucket>()
                                : curr;

        auto snapVersion = Bucket::getBucketVersion(mPrevSnap);
        if (snapVersion < Bucket::FIRST_PROTOCOL_SHADOWS_REMOVED)
        {
            // Note: here we use fake empty shadows, since we can't really
            // reconstruct the exact shadows, plus it should not really matter
            // anyway
            std::vector<std::shared_ptr<Bucket>> shadows;
            mGeneratedApplyState.currentBuckets[mLevel].next = FutureBucket(
                mApp, curr, mPrevSnap, shadows, snapVersion, false, mLevel);
        }
    }
}

HistoryArchiveState const&
GenerateBucketsWork::getGeneratedHAS()
{
    mGeneratedApplyState.resolveAllFutures();
    assert(mGeneratedApplyState.containsValidBuckets(mApp));
    return mGeneratedApplyState;
}

std::shared_ptr<Bucket>
GenerateBucketsWork::generateNewBucket(std::shared_ptr<Bucket> const& bucket,
                                       bool isCurr)
{
    std::vector<LedgerEntry> initEntries, liveEntries;
    std::vector<LedgerKey> deadEntries;
    BucketInputIterator iter(bucket);
    uint32_t ledgerVersion = iter.getMetadata().ledgerVersion;

    // Deconstruct the existing bucket to use for simulated bucket generation
    for (; iter; ++iter)
    {
        auto entry = *iter;
        switch (entry.type())
        {
        case METAENTRY:
            break;
        case INITENTRY:
            initEntries.emplace_back(entry.liveEntry());
            break;
        case LIVEENTRY:
            liveEntries.emplace_back(entry.liveEntry());
            break;
        case DEADENTRY:
            deadEntries.emplace_back(entry.deadEntry());
            break;
        }
    }

    // Since the total number of live and dead entries is known, avoid
    // re-allocating vectors inside the loop (std::vector::clear() does not
    // change vector capacity)
    std::vector<LedgerEntry> newInitEntries;
    std::vector<LedgerEntry> newLiveEntries;
    std::vector<LedgerKey> newDeadEntries;

    newInitEntries.reserve(initEntries.size());
    newLiveEntries.reserve(liveEntries.size());
    newDeadEntries.reserve(deadEntries.size());

    std::list<std::shared_ptr<Bucket>> simulated{bucket};

    for (uint32_t count = 1; count < mMultiplier; count++)
    {
        SimulationUtils::generateLiveEntries(newInitEntries, initEntries,
                                             count);
        SimulationUtils::generateLiveEntries(newLiveEntries, liveEntries,
                                             count);
        SimulationUtils::generateDeadEntries(newDeadEntries, deadEntries,
                                             count);

        auto nextBucket =
            Bucket::fresh(mApp.getBucketManager(), ledgerVersion,
                          newInitEntries, newLiveEntries, newDeadEntries,
                          /* countMergeEvents */ false,
                          mApp.getClock().getIOContext(), /* doFsync */ false);
        simulated.emplace_back(nextBucket);

        newInitEntries.clear();
        newLiveEntries.clear();
        newDeadEntries.clear();
    }

    // Create bucket with injected entries
    auto toInject = getEntriesToInject(mLevel, isCurr);
    if (!toInject.empty())
    {
        auto injectionBucket = Bucket::fresh(
            mApp.getBucketManager(), ledgerVersion, toInject, {}, {},
            /* countMergeEvents */ false, mApp.getClock().getIOContext(),
            /* doFsync */ false);
        simulated.emplace_back(injectionBucket);
    }

    std::vector<FutureBucket> fbs;
    // Check when all intermediate merges have completed: `simulated` has
    // reduced to a single bucket and there are no merges in-progress
    while (simulated.size() > 1 || !fbs.empty())
    {
        while (simulated.size() > 1)
        {
            auto b1 = simulated.front();
            simulated.pop_front();
            auto b2 = simulated.front();
            simulated.pop_front();
            std::vector<std::shared_ptr<Bucket>> shadows;
            fbs.emplace_back(mApp, b1, b2, shadows,
                             Config::CURRENT_LEDGER_PROTOCOL_VERSION, false,
                             mLevel);
        }

        for (auto it = fbs.begin(); it != fbs.end();)
        {
            if (it->mergeComplete())
            {
                simulated.emplace_back(it->resolve());
                it = fbs.erase(it);
            }
            else
            {
                ++it;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    mBuckets[binToHex(simulated.front()->getHash())] = simulated.front();

    // Forget any intermediate buckets produced
    mApp.getBucketManager().forgetUnreferencedBuckets();

    return simulated.front();
}

void
GenerateBucketsWork::onSuccess()
{
    // Persist HAS file to avoid re-generating same buckets
    getGeneratedHAS().save("simulate-" + HistoryArchiveState::baseName());
}

std::vector<LedgerEntry>
GenerateBucketsWork::getEntriesToInject(uint32_t level, bool isCurr)
{
    return {};
}
}
