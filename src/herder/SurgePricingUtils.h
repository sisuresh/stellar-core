#pragma once

// Copyright 2021 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include <set>

#include "transactions/TransactionFrameBase.h"

namespace stellar
{
// Compare fee/numOps between `l` and `r`.
// Returns -1 if `l` is strictly less than `r`, `1` if it's strictly greater and
// 0 when both are equal.
int feeRate3WayCompare(int64_t lFeeBid, uint32_t lNbOps, int64_t rFeeBid,
                       uint32_t rNbOps);

// Compute the fee bid that `tx` should have in order to beat
// a transaction `ref` with fee bid `refFeeBid` and `refNbOps` operations.
int64_t computeBetterFee(TransactionFrameBase const& tx, int64_t refFeeBid,
                         uint32_t refNbOps);

// Interface for a stack-like container holding transactions.
// This needs to be implemented by any user of `SurgePricingPriorityQueue`, as
// it operates on stacks and not bare transactions.
//
// This interface is not strictly a stack as it just defines the 'pop' operation
// and leaves the element additions up to the implementer. So the naming here is
// mostly to be in contrast with `SurgePricingPriorityQueue`.
class SurgePricingQueueTxBase
{
  public:
    virtual TransactionFrameBasePtr getTx() const = 0;
    virtual ~SurgePricingQueueTxBase() = default;
};

// A simple stack that holds a single transaction.
class SurgePricingQueueTx : public SurgePricingQueueTxBase
{
  public:
    SurgePricingQueueTx(TransactionFrameBasePtr tx) : mTx(tx)
    {
    }

    TransactionFrameBasePtr
    getTx() const override
    {
        releaseAssert(mTx);
        return mTx;
    }

  private:
    TransactionFrameBasePtr mTx;
};

using SurgePricingQueueTxBasePtr = std::shared_ptr<SurgePricingQueueTxBase>;

// Configuration for multi-lane transaction limiting and surge pricing.
//
// This configuration defines how many 'lanes' are there to compare and limit
// transactions based on (potentially multi-dimensional) resources.
//
// The configuration has the following semantics:
// - There exists at least one 'lane' for transactions.
// - Each transaction belongs to exactly one lane.
// - Every lane has a resource limit associated with it.
// - The lane '0' is always considered to be 'generic' lane. It defines the
//   maximum total number of resources allowed, i.e. transactions from *all*
//   the lanes must fit into it. Transactions from this lane can take place of
//   transactions from any other lane.
// - Lanes '1' and following are considered to be 'limited'. Besides fitting
//   into 'generic' lane, transactions from these lanes must also fit into
//   their lane's limit. Also transactions from 'limited' lanes may have their
//   own base fee that is not lower than the base fee for 'generic' lane.
//
// To summarize, this config defines the following invariants for some set of
// transactions:
// - `sum(tx.resources) <= laneLimits[0]`
// - `for each lane >= 1: sum(tx[lane].resources) <= laneLimits[lane]`
class SurgePricingLaneConfig
{
  public:
    // Returns a a lane the transaction belongs to.
    virtual size_t getLane(TransactionFrameBase const& tx) const = 0;
    // Returns per-lane limits.
    virtual std::vector<Resource> const& getLaneLimits() const = 0;
    // Updates the limit of the generic lane. This is needed due to
    // protocol upgrades (other lane limits are currently updated via
    // configuration).
    virtual void updateGenericLaneLimit(Resource const& limit) = 0;

    virtual Resource getTxResources(TransactionFrameBase const& tx) = 0;

    virtual ~SurgePricingLaneConfig() = default;
};

// Lane configuration that optionally defines a limited lane for transactions
// that have DEX operations (manager offer, path payments).
class DexLimitingLaneConfig : public SurgePricingLaneConfig
{
  public:
    // Index of the DEX limited lane.
    static constexpr size_t DEX_LANE = 1;

    // Creates the config. `Limit` is the total number of operations allowed
    // (lane '0'). `dexLimit` when non-`nullopt` defines a limited lane for
    // transactions with DEX operations (otherwise only one lane is created).
    DexLimitingLaneConfig(Resource limit, std::optional<Resource> dexLimit);

    size_t getLane(TransactionFrameBase const& tx) const override;
    std::vector<Resource> const& getLaneLimits() const override;
    virtual void updateGenericLaneLimit(Resource const& limit) override;
    virtual Resource getTxResources(TransactionFrameBase const& tx) override;

  private:
    std::vector<Resource> mLaneLimits;
    bool const mUseByteLimit;
};

class SorobanGenericLaneConfig : public SurgePricingLaneConfig
{
  public:
    SorobanGenericLaneConfig(Resource limit);

    size_t getLane(TransactionFrameBase const& tx) const override;
    std::vector<Resource> const& getLaneLimits() const override;
    virtual void updateGenericLaneLimit(Resource const& limit) override;
    virtual Resource getTxResources(TransactionFrameBase const& tx) override;

  private:
    std::vector<Resource> mLaneLimits;
};

// Priority queue-like data structure that allows to store transactions ordered
// by fee rate and perform operations that respect the lane limits specified by
// `SurgePricingLaneConfig`.
//
// The queue operates on transaction stacks and only the top transactions of the
// stacks are compared. This allows to e.g. order transactions while preserving
// the seq num order.
class SurgePricingPriorityQueue
{
  public:
    // Index of the 'generic' lane. This has to be '0' and is used for clarity.
    static constexpr size_t GENERIC_LANE = 0;

    // Helper that uses `SurgePricingPriorityQueue` to greedily select the
    // maximum subset of transactions from `txStacks` with maximum fee ratios
    // within the limits specified by `laneConfig`.
    // The greedy ordering optimizes for the maximal fee ratio first, then for
    // the output resource count.
    // Transactions will be popped from the input `txStacks`, so after this call
    // `txStacks` will contain all the remaining transactions.
    // `hadTxNotFittingLane` is an output parameter that for every lane will
    // identify whether there was a transaction that didn't fit into that lane's
    // limit.
    static std::vector<TransactionFrameBasePtr> getMostTopTxsWithinLimits(
        std::vector<SurgePricingQueueTxBasePtr> const& txStacks,
        std::shared_ptr<SurgePricingLaneConfig> laneConfig,
        std::vector<bool>& hadTxNotFittingLane);

    // Returns total number of resources in all the stacks in this queue.
    Resource totalResources() const;

    // Returns total number of resources in the provided lane of the queue.
    Resource laneResources(size_t lane) const;

    // Result of visiting the transaction stack in the `visitTopTxs`.
    // This serves as a callback output to let the queue know how to process the
    // visited stack.
    enum class VisitTxResult
    {
        SKIPPED,
        // Top transaction of the stack should be popped, but not counted
        // towards the lane limits.
        REJECTED,
        // Top transaction of the stack should be popped and counted towards the
        // lane limits.
        PROCESSED
    };

    // Helper that uses `SurgePricingPriorityQueue` to visit the top (by fee
    // rate) transactions in the `txStacks` until `laneConfig` limits are
    // reached. The visiting process will end for a lane as soon as there is a
    // transaction that causes the limit to be exceeded.
    // `comparisonSeed` is used to break the comparison ties.
    // `visitor` should process the `TxStack` and provide an action to do with
    // that stack.
    // `laneResourcesLeftUntilLimit` is an output parameter that for each lane
    // will contain the number of resources left until lane's limit is reached.
    void visitTopTxs(
        std::vector<SurgePricingQueueTxBasePtr> const& txs,
        std::function<VisitTxResult(SurgePricingQueueTxBase const&)> const&
            visitor,
        std::vector<Resource>& laneResourcesLeftUntilLimit);

    // Creates a `SurgePricingPriorityQueue` for the provided lane
    // configuration.
    // `isHighestPriority` defines the comparison order: when it's `true` the
    // highest fee rate transactions are considered to be at the top, otherwise
    // the lowest fee rate transaction is at the top.
    SurgePricingPriorityQueue(
        bool isHighestPriority,
        std::shared_ptr<SurgePricingLaneConfig> laneConfig,
        size_t comparisonSeed);

    // Adds a `TxStack` to this queue. The queue has ownership of the stack and
    // may pop transactions from it.
    void add(SurgePricingQueueTxBasePtr tx);
    // Erases a `TxStack` from this queue.
    void erase(SurgePricingQueueTxBasePtr tx);

    // Checks whether a provided transaction could fit into this queue without
    // violating the `laneConfig` limits while evicting some lower fee rate
    // `TxStacks` from the queue.
    // Returns whether transaction can be fit and if not, returns the minimum
    // required fee to possibly fit.
    // `txDiscount` is a number of resources to subtract from tx's
    // resources when estimating the total resource counts.
    // `txStacksToEvict` is an output parameter that will contain all the stacks
    // that need to be evicted in order to fit `tx`. The `bool` argument
    // indicates whether this `TxStack` has been evicted due to lane's limit (as
    // opposed to 'generic' lane's limit).
    std::pair<bool, int64_t> canFitWithEviction(
        TransactionFrameBase const& tx, std::optional<Resource> txDiscount,
        std::vector<std::pair<SurgePricingQueueTxBasePtr, bool>>& txsToEvict)
        const;

    // Generalized method for visiting and popping the top transactions in the
    // queue until the lane limits are reached.
    // This is a destructive method that removes all or most of the queue
    // elements and thus should be used with care.
    void popTopTxs(
        bool allowGaps,
        std::function<VisitTxResult(SurgePricingQueueTxBase const&)> const&
            visitor,
        std::vector<Resource>& laneResourcesLeftUntilLimit,
        std::vector<bool>& hadTxNotFittingLane);

  private:
    class TxComparator
    {
      public:
        TxComparator(bool isGreater, size_t seed);

        bool operator()(SurgePricingQueueTxBasePtr const& tx1,
                        SurgePricingQueueTxBasePtr const& tx2) const;

        bool compareFeeOnly(TransactionFrameBase const& tx1,
                            TransactionFrameBase const& tx2) const;
        bool compareFeeOnly(int64_t tx1Bid, uint32_t tx1Ops, int64_t tx2Bid,
                            uint32_t tx2Ops) const;
        bool isGreater() const;

      private:
        bool txLessThan(SurgePricingQueueTxBase const& tx1,
                             SurgePricingQueueTxBase const& tx2) const;

        bool const mIsGreater;
        size_t mSeed;
    };

    using TxSet = std::set<SurgePricingQueueTxBasePtr, TxComparator>;
    using LaneIter = std::pair<size_t, TxSet::iterator>;

    // Iterator for walking the queue from top to bottom, possibly restricted
    // only to some lanes. The actual ordering is defined by
    // `isHighestPriority`.
    class Iterator
    {
      public:
        Iterator(SurgePricingPriorityQueue const& parent,
                 std::vector<LaneIter> const& iters);

        SurgePricingQueueTxBasePtr operator*() const;
        // Gets the iterator of the `TxStackSet` corresponding to the current
        // value.
        LaneIter getInnerIter() const;
        bool isEnd() const;
        // Advances this iterator to the next value.
        void advance();
        // Removes a lane corresponding to the current value of the iterator.
        void dropLane();

      private:
        std::vector<LaneIter>::iterator getMutableInnerIter() const;

        SurgePricingPriorityQueue const& mParent;
        std::vector<LaneIter> mutable mIters;
    };

    void erase(Iterator const& it);
    void erase(size_t lane, SurgePricingPriorityQueue::TxSet::iterator iter);

    Iterator getTop() const;

    TxComparator const mComparator;
    std::shared_ptr<SurgePricingLaneConfig> mLaneConfig;
    std::vector<Resource> const& mLaneLimits;

    std::vector<Resource> mLaneCurrentCount;

    std::vector<TxSet> mTxSets;
};

} // namespace stellar
