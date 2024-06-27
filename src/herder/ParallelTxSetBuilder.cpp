// Copyright 2024 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "herder/ParallelTxSetBuilder.h"
#include "herder/SurgePricingUtils.h"
#include "herder/TxSetFrame.h"
#include "transactions/TransactionFrameBase.h"
#include "util/BitSet.h"

#include <unordered_set>

namespace stellar
{
namespace
{

struct ParallelPartitionConfig
{
    ParallelPartitionConfig(Config const& cfg,
                            SorobanNetworkConfig const& sorobanCfg)
        : mStageCount(
              std::max(cfg.SOROBAN_PHASE_STAGE_COUNT, static_cast<uint32_t>(1)))
        , mThreadsPerStage(sorobanCfg.ledgerMaxParallelThreads())
        , mInstructionsPerThread(sorobanCfg.ledgerMaxInstructions() /
                                 mStageCount)
    {
    }

    uint64_t
    instructionsPerStage() const
    {
        return mInstructionsPerThread * mThreadsPerStage;
    }

    uint32_t mStageCount = 0;
    uint32_t mThreadsPerStage = 0;
    uint64_t mInstructionsPerThread = 0;
};

struct BuilderTx : public SurgePricingQueueTxBase
{
    size_t mId;
    TransactionFrameBasePtr mTx;
    uint32_t mInstructions = 0;
    BitSet mReadOnlyFootprint;
    BitSet mReadWriteFootprint;

    BuilderTx(TransactionFrameBasePtr tx, size_t txId,
              UnorderedMap<LedgerKey, size_t> const& entryIdMap)
        : mTx(tx), mId(txId), mInstructions(tx->sorobanResources().instructions)
    {
        auto const& footprint = tx->sorobanResources().footprint;
        for (auto const& key : footprint.readOnly)
        {
            mReadOnlyFootprint.set(entryIdMap.at(key));
        }
        for (auto const& key : footprint.readWrite)
        {
            mReadWriteFootprint.set(entryIdMap.at(key));
        }
    }

    TransactionFrameBasePtr
    getTx() const override
    {
        releaseAssert(mTx);
        return mTx;
    }
};

struct Cluster
{
    uint64_t mInstructions = 0;
    BitSet mReadOnlyEntries;
    BitSet mReadWriteEntries;
    BitSet mTxIds;
    size_t mBinId = 0;

    explicit Cluster(BuilderTx const& tx) : mInstructions(tx.mInstructions)
    {
        mReadOnlyEntries.inplaceUnion(tx.mReadOnlyFootprint);
        mReadWriteEntries.inplaceUnion(tx.mReadWriteFootprint);
        mTxIds.set(tx.mId);
    }

    void
    merge(Cluster const& other)
    {
        mInstructions += other.mInstructions;
        mReadOnlyEntries.inplaceUnion(other.mReadOnlyEntries);
        mReadWriteEntries.inplaceUnion(other.mReadWriteEntries);
        mTxIds.inplaceUnion(other.mTxIds);
    }
};

class Stage
{
  public:
    Stage(ParallelPartitionConfig cfg) : mConfig(cfg)
    {
        mBinPacking.resize(mConfig.mThreadsPerStage);
        mBinInstructions.resize(mConfig.mThreadsPerStage);
    }

    bool
    tryAdd(BuilderTx const& tx)
    {
        ZoneScoped;
        if (mInstructions + tx.mInstructions > mConfig.instructionsPerStage())
        {
            return false;
        }

        auto conflictingClusters = getConflictingClusters(tx);

        bool packed = false;
        auto newClusters = createNewClusters(tx, conflictingClusters, packed);
        releaseAssert(!newClusters.empty());
        if (newClusters.back().mInstructions > mConfig.mInstructionsPerThread)
        {
            return false;
        }
        if (packed)
        {
            mClusters = newClusters;
            mInstructions += tx.mInstructions;
            return true;
        }

        std::vector<uint64_t> newBinInstructions;
        auto newPacking = binPacking(newClusters, newBinInstructions);
        if (newPacking.empty())
        {
            return false;
        }
        mClusters = newClusters;
        mBinPacking = newPacking;
        mInstructions += tx.mInstructions;
        mBinInstructions = newBinInstructions;
        return true;
    }

    void
    visitAllTransactions(std::function<void(size_t, size_t)> visitor) const
    {
        for (auto const& cluster : mClusters)
        {
            size_t txId = 0;
            while (cluster.mTxIds.nextSet(txId))
            {
                visitor(cluster.mBinId, txId);
                ++txId;
            }
        }
    }

  private:
    std::unordered_set<Cluster const*>
    getConflictingClusters(BuilderTx const& tx) const
    {
        std::unordered_set<Cluster const*> conflictingClusters;
        for (Cluster const& cluster : mClusters)
        {
            bool isConflicting = tx.mReadOnlyFootprint.intersectionCount(
                                     cluster.mReadWriteEntries) > 0 ||
                                 tx.mReadWriteFootprint.intersectionCount(
                                     cluster.mReadOnlyEntries) > 0 ||
                                 tx.mReadWriteFootprint.intersectionCount(
                                     cluster.mReadWriteEntries) > 0;
            if (isConflicting)
            {
                conflictingClusters.insert(&cluster);
            }
        }
        return conflictingClusters;
    }

    std::vector<Cluster>
    createNewClusters(BuilderTx const& tx,
                      std::unordered_set<Cluster const*> const& txConflicts,
                      bool& packed)
    {
        std::vector<Cluster> newClusters;
        newClusters.reserve(mClusters.size());
        for (auto const& cluster : mClusters)
        {
            if (txConflicts.find(&cluster) == txConflicts.end())
            {
                newClusters.push_back(cluster);
            }
        }

        newClusters.emplace_back(tx);
        for (auto const* cluster : txConflicts)
        {
            newClusters.back().merge(*cluster);
        }

        if (newClusters.back().mInstructions > mConfig.mInstructionsPerThread)
        {
            return newClusters;
        }

        for (auto const& cluster : txConflicts)
        {
            mBinInstructions[cluster->mBinId] -= cluster->mInstructions;
            mBinPacking[cluster->mBinId].inplaceDifference(cluster->mTxIds);
        }

        packed = false;

        for (size_t binId = 0; binId < mConfig.mThreadsPerStage; ++binId)
        {
            if (mBinInstructions[binId] + newClusters.back().mInstructions <=
                mConfig.mInstructionsPerThread)
            {
                mBinInstructions[binId] += newClusters.back().mInstructions;
                mBinPacking[binId].inplaceUnion(newClusters.back().mTxIds);
                newClusters.back().mBinId = binId;
                packed = true;
                break;
            }
        }
        if (!packed)
        {
            for (auto const& cluster : txConflicts)
            {
                mBinInstructions[cluster->mBinId] += cluster->mInstructions;
                mBinPacking[cluster->mBinId].inplaceUnion(cluster->mTxIds);
            }
        }

        return newClusters;
    }

    std::vector<BitSet>
    binPacking(std::vector<Cluster>& clusters,
               std::vector<uint64_t>& binInsns) const
    {
        std::sort(clusters.begin(), clusters.end(),
                  [](auto const& a, auto const& b) {
                      return a.mInstructions > b.mInstructions;
                  });
        size_t const binCount = mConfig.mThreadsPerStage;
        std::vector<BitSet> bins(binCount);
        binInsns.resize(binCount);
        for (auto& cluster : clusters)
        {
            bool packed = false;
            for (int i = 0; i < binCount; ++i)
            {
                if (binInsns[i] + cluster.mInstructions <=
                    mConfig.mInstructionsPerThread)
                {
                    binInsns[i] += cluster.mInstructions;
                    bins[i].inplaceUnion(cluster.mTxIds);
                    cluster.mBinId = i;
                    packed = true;
                    break;
                }
            }
            if (!packed)
            {
                return std::vector<BitSet>();
            }
        }
        return bins;
    }

    std::vector<Cluster> mClusters;
    std::vector<BitSet> mBinPacking;
    std::vector<uint64_t> mBinInstructions;
    int64_t mInstructions = 0;
    ParallelPartitionConfig mConfig;
};

} // namespace

TxStageFrameList
buildSurgePricedParallelSorobanPhase(
    TxFrameList const& txFrames, Config const& cfg,
    SorobanNetworkConfig const& sorobanCfg,
    std::shared_ptr<SurgePricingLaneConfig> laneConfig,
    std::vector<bool>& hadTxNotFittingLane)
{
    ZoneScoped;
    UnorderedMap<LedgerKey, size_t> entryIdMap;

    if (txFrames.size() > 1)
    {
        int t = 0;
    }
    auto addToMap = [&entryIdMap](LedgerKey const& key) {
        auto sz = entryIdMap.size();
        entryIdMap.emplace(key, sz);
    };
    for (auto const& txFrame : txFrames)
    {
        auto const& footprint = txFrame->sorobanResources().footprint;
        for (auto const& key : footprint.readOnly)
        {
            addToMap(key);
        }
        for (auto const& key : footprint.readWrite)
        {
            addToMap(key);
        }
    }

    std::vector<std::shared_ptr<BuilderTx>> txs;
    txs.reserve(txFrames.size());
    for (auto const& txFrame : txFrames)
    {
        txs.emplace_back(
            std::make_shared<BuilderTx>(txFrame, txs.size(), entryIdMap));
    }

    SurgePricingPriorityQueue queue(
        /* isHighestPriority */ true, laneConfig,
        stellar::rand_uniform<size_t>(0, std::numeric_limits<size_t>::max()));
    for (auto const& tx : txs)
    {
        queue.add(tx);
    }

    ParallelPartitionConfig partitionCfg(cfg, sorobanCfg);
    std::vector<Stage> stages(partitionCfg.mStageCount, partitionCfg);

    auto visitor = [&stages](SurgePricingQueueTxBase const& txStack) {
        bool added = false;
        for (auto& stage : stages)
        {
            if (stage.tryAdd(static_cast<BuilderTx const&>(txStack)))
            {
                added = true;
                break;
            }
        }
        if (added)
        {
            return SurgePricingPriorityQueue::VisitTxResult::PROCESSED;
        }
        return SurgePricingPriorityQueue::VisitTxResult::REJECTED;
    };

    std::vector<Resource> laneLeftUntilLimit;
    queue.popTopTxs(/* allowGaps */ true, visitor, laneLeftUntilLimit,
                    hadTxNotFittingLane);
    releaseAssert(hadTxNotFittingLane.size() == 1);

    TxStageFrameList resStages;
    resStages.reserve(stages.size());
    for (auto const& stage : stages)
    {
        auto& resStage = resStages.emplace_back();
        resStage.resize(partitionCfg.mThreadsPerStage);

        stage.visitAllTransactions(
            [&resStage, &txFrames](size_t threadId, size_t txId) {
                resStage[threadId].push_back(txFrames[txId]);
            });
        while (!resStage.empty() && resStage.back().empty())
        {
            resStage.pop_back();
        }
    }
    while (!resStages.empty() && resStages.back().empty())
    {
        resStages.pop_back();
    }

    return resStages;
}

} // namespace stellar
