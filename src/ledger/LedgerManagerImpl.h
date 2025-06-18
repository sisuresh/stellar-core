#pragma once

// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0
#include "util/asio.h"

#include "history/HistoryManager.h"
#include "ledger/LedgerCloseMetaFrame.h"
#include "ledger/LedgerManager.h"
#include "ledger/NetworkConfig.h"
#include "ledger/SharedModuleCacheCompiler.h"
#include "ledger/SorobanMetrics.h"
#include "main/PersistentState.h"
#include "rust/RustBridge.h"
#include "transactions/ParallelApplyStage.h"
#include "transactions/TransactionFrame.h"
#include "util/XDRStream.h"
#include "xdr/Stellar-ledger.h"
#include <filesystem>
#include <optional>
#include <string>

/*
Holds the current ledger
Applies the tx set to the last ledger to get the next one
Hands the old ledger off to the history
*/

namespace medida
{
class Timer;
class Counter;
class Histogram;
class Buckets;
}

namespace stellar
{
class AbstractLedgerTxn;
class Application;
class Database;
class LedgerTxnHeader;
class BasicWork;
class ParallelLedgerInfo;

class LedgerManagerImpl : public LedgerManager
{
  protected:
    Application& mApp;
    std::unique_ptr<XDROutputFileStream> mMetaStream;
    std::unique_ptr<XDROutputFileStream> mMetaDebugStream;
    std::weak_ptr<BasicWork> mFlushAndRotateMetaDebugWork;
    std::filesystem::path mMetaDebugPath;

  private:
    struct LedgerApplyMetrics
    {
        SorobanMetrics mSorobanMetrics;
        medida::Timer& mTransactionApply;
        medida::Histogram& mTransactionCount;
        medida::Histogram& mOperationCount;
        medida::Histogram& mPrefetchHitRate;
        medida::Timer& mLedgerClose;
        medida::Buckets& mLedgerAgeClosed;
        medida::Counter& mLedgerAge;
        medida::Counter& mTransactionApplySucceeded;
        medida::Counter& mTransactionApplyFailed;
        medida::Counter& mSorobanTransactionApplySucceeded;
        medida::Counter& mSorobanTransactionApplyFailed;
        medida::Meter& mMetaStreamBytes;
        medida::Timer& mMetaStreamWriteTime;
        LedgerApplyMetrics(medida::MetricsRegistry& registry);
    };

    // Any state that apply needs to access through the app connector should go
    // here, at very least just to make it clear what is being accessed by which
    // threads. We may try to further encapsulate it.
    struct ApplyState
    {
        LedgerApplyMetrics mMetrics;

        // Latest Soroban config during apply (should not be used outside of
        // application, as it may be in half-valid state). Note that access to
        // this variable is not synchronized, since it should only be used by
        // one thread (main or ledger close).
        std::shared_ptr<SorobanNetworkConfig> mSorobanNetworkConfig;

        // The current reusable / inter-ledger soroban module cache.
        ::rust::Box<rust_bridge::SorobanModuleCache> mModuleCache;

        // Manager object that (re)builds the module cache in background
        // threads. Only non-nullptr when there's a background compilation in
        // progress.
        std::unique_ptr<SharedModuleCacheCompiler> mCompiler;

        // Protocol versions to compile each contract for in the module cache.
        std::vector<uint32_t> mModuleCacheProtocols;

        // Number of threads to use for compilation (cached from config).
        size_t mNumCompilationThreads;

        // Kicks off (on auxiliary threads) compilation of all contracts in the
        // provided snapshot, for ledger protocols starting at minLedgerVersion
        // and running through to Config::CURRENT_LEDGER_PROTOCOL_VERSION (to
        // enable upgrades).
        void startCompilingAllContracts(SearchableSnapshotConstPtr snap,
                                        uint32_t minLedgerVersion);

        // Finishes a compilation started by `startCompilingAllContracts`.
        void finishPendingCompilation();

        // Equivalent to calling `startCompilingAllContracts` followed by
        // `finishPendingCompilation`.
        void compileAllContractsInLedger(SearchableSnapshotConstPtr snap,
                                         uint32_t minLedgerVersion);

        // Estimates the size of the arena underlying the module cache's shared
        // wasmi engine, from metrics, and rebuilds if it has likely built up a
        // lot of dead space inside of it.
        void maybeRebuildModuleCache(SearchableSnapshotConstPtr snap,
                                     uint32_t minLedgerVersion);

        // Evicts a single contract from the module cache, if it is present.
        // This should be done whenever a contract LE is evicted from the
        // live BL.
        void evictFromModuleCache(uint32_t ledgerVersion,
                                  EvictedStateVectors const& evictedState);

        // Adds all contracts in the provided set of LEs to the module cache.
        // This should be called as entries are added to the live bucketlist.
        void addAnyContractsToModuleCache(uint32_t ledgerVersion,
                                          std::vector<LedgerEntry> const& le);

        ApplyState(Application& app);
    };

    // This state is private to the apply thread and holds work-in-progress
    // that gets accessed via the AppConnector, from inside transactions.
    ApplyState mApplyState;

    // Cached LCL state output from last apply (or loaded from DB on startup).
    CompleteConstLedgerStatePtr mLastClosedLedgerState;

    VirtualClock::time_point mLastClose;

    // Use mutex to guard ledger state during apply
    mutable std::recursive_mutex mLedgerStateMutex;

    medida::Timer& mCatchupDuration;

    std::unique_ptr<LedgerCloseMetaFrame> mNextMetaToEmit;

    // Use in the context of parallel ledger apply to indicate background thread
    // is currently closing a ledger or has ledgers queued to apply.
    bool mCurrentlyApplyingLedger{false};

    static std::vector<MutableTxResultPtr> processFeesSeqNums(
        ApplicableTxSetFrame const& txSet, AbstractLedgerTxn& ltxOuter,
        std::unique_ptr<LedgerCloseMetaFrame> const& ledgerCloseMeta,
        LedgerCloseData const& ledgerData);

    void processResultAndMeta(
        std::unique_ptr<LedgerCloseMetaFrame> const& ledgerCloseMeta,
        uint32_t txIndex, TransactionMetaBuilder& txMetaBuilder,
        TransactionFrameBase const& tx,
        MutableTransactionResultBase const& result,
        TransactionResultSet& txResultSet);

    TransactionResultSet applyTransactions(
        ApplicableTxSetFrame const& txSet,
        std::vector<MutableTxResultPtr> const& mutableTxResults,
        AbstractLedgerTxn& ltx,
        std::unique_ptr<LedgerCloseMetaFrame> const& ledgerCloseMeta);

    void
    applyParallelPhase(TxSetPhaseFrame const& phase,
                       std::vector<ApplyStage>& applyStages,
                       std::vector<MutableTxResultPtr> const& mutableTxResults,
                       int& index, AbstractLedgerTxn& ltx, bool enableTxMeta,
                       Hash const& sorobanBasePrngSeed);

    void applySequentialPhase(
        TxSetPhaseFrame const& phase,
        std::vector<MutableTxResultPtr> const& mutableTxResults, int& index,
        AbstractLedgerTxn& ltx, bool enableTxMeta,
        Hash const& sorobanBasePrngSeed,
        std::unique_ptr<LedgerCloseMetaFrame> const& ledgerCloseMeta,
        TransactionResultSet& txResultSet);

    void processPostTxSetApply(
        std::vector<TxSetPhaseFrame> const& phases,
        std::vector<ApplyStage> const& applyStages, AbstractLedgerTxn& ltx,
        std::unique_ptr<LedgerCloseMetaFrame> const& ledgerCloseMeta,
        TransactionResultSet& txResultSet);

    std::pair<RestoredKeys, std::unique_ptr<ThreadEntryMap>>
    applyThread(AppConnector& app, std::unique_ptr<ThreadEntryMap> entryMap,
                Cluster const& cluster, Config const& config,
                SorobanNetworkConfig const& sorobanConfig,
                ParallelLedgerInfo ledgerInfo, Hash sorobanBasePrngSeed);

    std::pair<std::vector<RestoredKeys>,
              std::vector<std::unique_ptr<ThreadEntryMap>>>
    applySorobanStageClustersInParallel(
        AppConnector& app, AbstractLedgerTxn& ltx, ApplyStage const& stage,
        ThreadEntryMap const& entryMap, Hash const& sorobanBasePrngSeed,
        Config const& config, SorobanNetworkConfig const& sorobanConfig,
        ParallelLedgerInfo const& ledgerInfo);

    void addAllRestoredKeysToLedgerTxn(
        std::vector<RestoredKeys> const& threadRestoredKeys,
        AbstractLedgerTxn& ltx);
    void checkAllTxBundleInvariantsAndCallProcessPostApply(
        AppConnector& app, ApplyStage const& stage, Config const& config,
        ParallelLedgerInfo const& ledgerInfo, AbstractLedgerTxn& ltx);

    void writeDirtyMapEntriesToGlobalEntryMap(
        std::vector<std::unique_ptr<ThreadEntryMap>> const& entryMapsByCluster,
        ThreadEntryMap& globalEntryMap,
        std::unordered_set<LedgerKey> const& isInReadWriteSet);

    void writeGlobalEntryMapToLedgerTxn(AbstractLedgerTxn& ltx,
                                        ThreadEntryMap const& globalEntryMap);

    void applySorobanStage(AppConnector& app, AbstractLedgerTxn& ltx,
                           ThreadEntryMap& entryMap, ApplyStage const& stage,
                           Hash const& sorobanBasePrngSeed);

    void applySorobanStages(AppConnector& app, AbstractLedgerTxn& ltx,
                            std::vector<ApplyStage> const& stages,
                            Hash const& sorobanBasePrngSeed);

    // initialLedgerVers must be the ledger version at the start of the ledger.
    // On the ledger in which a protocol upgrade from vN to vN + 1 occurs,
    // initialLedgerVers must be vN.
    CompleteConstLedgerStatePtr sealLedgerTxnAndStoreInBucketsAndDB(
        AbstractLedgerTxn& ltx,
        std::unique_ptr<LedgerCloseMetaFrame> const& ledgerCloseMeta,
        uint32_t initialLedgerVers);

    HistoryArchiveState
    storePersistentStateAndLedgerHeaderInDB(LedgerHeader const& header,
                                            bool appendToCheckpoint);
    static void prefetchTransactionData(AbstractLedgerTxnParent& rootLtx,
                                        ApplicableTxSetFrame const& txSet,
                                        Config const& config);
    static void prefetchTxSourceIds(AbstractLedgerTxnParent& rootLtx,
                                    ApplicableTxSetFrame const& txSet,
                                    Config const& config);

    State mState;

#ifdef BUILD_TESTS
    std::vector<TransactionMetaFrame> mLastLedgerTxMeta;
    std::optional<LedgerCloseMetaFrame> mLastLedgerCloseMeta;
#endif

    void setState(State s);

    void emitNextMeta();

    // Publishes soroban metrics, including select network config limits as well
    // as the actual ledger usage.
    void publishSorobanMetrics();

    // Update cached last closed ledger state values managed by this class.
    void
    advanceLastClosedLedgerState(CompleteConstLedgerStatePtr newLedgerState);

  protected:
    // initialLedgerVers must be the ledger version at the start of the ledger
    // and currLedgerVers is the ledger version in the current ltx header. These
    // values are the same except on the ledger in which a protocol upgrade from
    // vN to vN + 1 occurs. initialLedgerVers must be vN and currLedgerVers must
    // be vN + 1.

    // NB: LedgerHeader is a copy here to prevent footguns in case ltx
    // invalidates any header references
    virtual void sealLedgerTxnAndTransferEntriesToBucketList(
        AbstractLedgerTxn& ltx,
        std::unique_ptr<LedgerCloseMetaFrame> const& ledgerCloseMeta,
        LedgerHeader lh, uint32_t initialLedgerVers);

    // Update bucket list snapshot, and construct LedgerState return
    // value, which contains all information relevant to ledger state (HAS,
    // ledger header, network config, bucketlist snapshot).
    CompleteConstLedgerStatePtr
    advanceBucketListSnapshotAndMakeLedgerState(LedgerHeader const& header,
                                                HistoryArchiveState const& has);
    void logTxApplyMetrics(AbstractLedgerTxn& ltx, size_t numTxs,
                           size_t numOps);

  public:
    LedgerManagerImpl(Application& app);

    // Reloads the network configuration from the ledger.
    // This needs to be called every time a ledger is closed.
    // This call is read-only and hence `ltx` can be read-only.
    void updateSorobanNetworkConfigForApply(AbstractLedgerTxn& ltx) override;
    void moveToSynced() override;
    void beginApply() override;
    State getState() const override;
    std::string getStateHuman() const override;

    void valueExternalized(LedgerCloseData const& ledgerData,
                           bool isLatestSlot) override;

    uint32_t getLastMaxTxSetSize() const override;
    uint32_t getLastMaxTxSetSizeOps() const override;
    Resource maxLedgerResources(bool isSoroban) override;
    Resource maxSorobanTransactionResources() override;
    int64_t getLastMinBalance(uint32_t ownerCount) const override;
    uint32_t getLastReserve() const override;
    uint32_t getLastTxFee() const override;
    uint32_t getLastClosedLedgerNum() const override;
    SorobanNetworkConfig const& getLastClosedSorobanNetworkConfig() override;
    SorobanNetworkConfig const& getSorobanNetworkConfigForApply() override;

    bool hasLastClosedSorobanNetworkConfig() const override;

#ifdef BUILD_TESTS
    SorobanNetworkConfig& getMutableSorobanNetworkConfigForApply() override;
    void mutateSorobanNetworkConfigForApply(
        std::function<void(SorobanNetworkConfig&)> const& f) override;
    std::vector<TransactionMetaFrame> const&
    getLastClosedLedgerTxMeta() override;
    std::optional<LedgerCloseMetaFrame> const&
    getLastClosedLedgerCloseMeta() override;
    TransactionResultSet mLatestTxResultSet{};
    void storeCurrentLedgerForTest(LedgerHeader const& header) override;
    std::function<void()> mAdvanceLedgerStateAndPublishOverride;
#endif

    uint64_t secondsSinceLastLedgerClose() const override;
    void syncMetrics() override;

    void startNewLedger(LedgerHeader const& genesisLedger);
    void startNewLedger() override;
    void loadLastKnownLedger(bool restoreBucketlist) override;

    LedgerHeaderHistoryEntry const& getLastClosedLedgerHeader() const override;

    HistoryArchiveState getLastClosedLedgerHAS() const override;

    Database& getDatabase() override;

    void startCatchup(CatchupConfiguration configuration,
                      std::shared_ptr<HistoryArchive> archive) override;

    void applyLedger(LedgerCloseData const& ledgerData,
                     bool calledViaExternalize) override;
    void advanceLedgerStateAndPublish(
        uint32_t ledgerSeq, bool calledViaExternalize,
        LedgerCloseData const& ledgerData,
        CompleteConstLedgerStatePtr newLedgerState) override;
    void ledgerCloseComplete(uint32_t lcl, bool calledViaExternalize,
                             LedgerCloseData const& ledgerData);
    void
    setLastClosedLedger(LedgerHeaderHistoryEntry const& lastClosed) override;

    void manuallyAdvanceLedgerHeader(LedgerHeader const& header) override;

    void setupLedgerCloseMetaStream();
    void maybeResetLedgerCloseMetaDebugStream(uint32_t ledgerSeq);

    SorobanMetrics& getSorobanMetrics() override;
    SearchableSnapshotConstPtr getLastClosedSnaphot() const override;
    virtual bool
    isApplying() const override
    {
        return mCurrentlyApplyingLedger;
    }
    ::rust::Box<rust_bridge::SorobanModuleCache> getModuleCache() override;
};
}
