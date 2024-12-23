// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "ledger/LedgerManagerImpl.h"
#include "bucket/BucketList.h"
#include "bucket/BucketManager.h"
#include "catchup/AssumeStateWork.h"
#include "crypto/Hex.h"
#include "crypto/KeyUtils.h"
#include "crypto/SHA.h"
#include "crypto/SecretKey.h"
#include "database/Database.h"
#include "herder/Herder.h"
#include "herder/HerderPersistence.h"
#include "herder/LedgerCloseData.h"
#include "herder/TxSetFrame.h"
#include "herder/Upgrades.h"
#include "history/HistoryManager.h"
#include "invariant/InvariantDoesNotHold.h"
#include "invariant/InvariantManager.h"
#include "ledger/FlushAndRotateMetaDebugWork.h"
#include "ledger/LedgerHeaderUtils.h"
#include "ledger/LedgerRange.h"
#include "ledger/LedgerTxn.h"
#include "ledger/LedgerTxnEntry.h"
#include "ledger/LedgerTxnHeader.h"
#include "ledger/LedgerTypeUtils.h"
#include "main/Application.h"
#include "main/Config.h"
#include "main/ErrorMessages.h"
#include "overlay/OverlayManager.h"
#include "transactions/MutableTransactionResult.h"
#include "transactions/OperationFrame.h"
#include "transactions/TransactionFrameBase.h"
#include "transactions/TransactionMetaFrame.h"
#include "transactions/TransactionSQL.h"
#include "transactions/TransactionUtils.h"
#include "util/DebugMetaUtils.h"
#include "util/Fs.h"
#include "util/GlobalChecks.h"
#include "util/LogSlowExecution.h"
#include "util/Logging.h"
#include "util/ProtocolVersion.h"
#include "util/XDRCereal.h"
#include "util/XDROperators.h"
#include "util/XDRStream.h"
#include "work/WorkScheduler.h"

#include <fmt/format.h>

#include "xdr/Stellar-ledger.h"
#include "xdr/Stellar-transaction.h"
#include "xdrpp/types.h"

#include "medida/buckets.h"
#include "medida/counter.h"
#include "medida/meter.h"
#include "medida/metrics_registry.h"
#include "medida/timer.h"
#include <Tracy.hpp>

#include <chrono>
#include <numeric>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <thread>

/*
The ledger module:
    1) gets the externalized tx set
    2) applies this set to the last closed ledger
    3) sends the changed entries to the BucketList
    4) saves the changed entries to SQL
    5) saves the ledger hash and header to SQL
    6) sends the new ledger hash and the tx set to the history
    7) sends the new ledger hash and header to the Herder


catching up to network:
    1) Wait for SCP to tell us what the network is on now
    2) Pull history log or static deltas from history archive
    3) Replay or force-apply deltas, depending on catchup mode

*/
using namespace std;

namespace stellar
{

const uint32_t LedgerManager::GENESIS_LEDGER_SEQ = 1;
const uint32_t LedgerManager::GENESIS_LEDGER_VERSION = 0;
const uint32_t LedgerManager::GENESIS_LEDGER_BASE_FEE = 100;
const uint32_t LedgerManager::GENESIS_LEDGER_BASE_RESERVE = 100000000;
const uint32_t LedgerManager::GENESIS_LEDGER_MAX_TX_SIZE = 100;
const int64_t LedgerManager::GENESIS_LEDGER_TOTAL_COINS = 1000000000000000000;

std::unique_ptr<LedgerManager>
LedgerManager::create(Application& app)
{
    return std::make_unique<LedgerManagerImpl>(app);
}

std::string
LedgerManager::ledgerAbbrev(LedgerHeader const& header)
{
    return ledgerAbbrev(header, xdrSha256(header));
}

std::string
LedgerManager::ledgerAbbrev(uint32_t seq, uint256 const& hash)
{
    std::ostringstream oss;
    oss << "[seq=" << seq << ", hash=" << hexAbbrev(hash) << "]";
    return oss.str();
}

std::string
LedgerManager::ledgerAbbrev(LedgerHeader const& header, uint256 const& hash)
{
    return ledgerAbbrev(header.ledgerSeq, hash);
}

std::string
LedgerManager::ledgerAbbrev(LedgerHeaderHistoryEntry const& he)
{
    return ledgerAbbrev(he.header, he.hash);
}

LedgerManagerImpl::LedgerManagerImpl(Application& app)
    : mApp(app)
    , mSorobanMetrics(app.getMetrics())
    , mTransactionApply(
          app.getMetrics().NewTimer({"ledger", "transaction", "apply"}))
    , mTransactionCount(
          app.getMetrics().NewHistogram({"ledger", "transaction", "count"}))
    , mOperationCount(
          app.getMetrics().NewHistogram({"ledger", "operation", "count"}))
    , mPrefetchHitRate(
          app.getMetrics().NewHistogram({"ledger", "prefetch", "hit-rate"}))
    , mLedgerClose(app.getMetrics().NewTimer({"ledger", "ledger", "close"}))
    , mLedgerAgeClosed(app.getMetrics().NewBuckets(
          {"ledger", "age", "closed"}, {5000.0, 7000.0, 10000.0, 20000.0}))
    , mLedgerAge(
          app.getMetrics().NewCounter({"ledger", "age", "current-seconds"}))
    , mTransactionApplySucceeded(
          app.getMetrics().NewCounter({"ledger", "apply", "success"}))
    , mTransactionApplyFailed(
          app.getMetrics().NewCounter({"ledger", "apply", "failure"}))
    , mSorobanTransactionApplySucceeded(
          app.getMetrics().NewCounter({"ledger", "apply-soroban", "success"}))
    , mSorobanTransactionApplyFailed(
          app.getMetrics().NewCounter({"ledger", "apply-soroban", "failure"}))
    , mMetaStreamBytes(
          app.getMetrics().NewMeter({"ledger", "metastream", "bytes"}, "byte"))
    , mMetaStreamWriteTime(
          app.getMetrics().NewTimer({"ledger", "metastream", "write"}))
    , mLastClose(mApp.getClock().now())
    , mCatchupDuration(
          app.getMetrics().NewTimer({"ledger", "catchup", "duration"}))
    , mState(LM_BOOTING_STATE)

{
    setupLedgerCloseMetaStream();
}

void
LedgerManagerImpl::moveToSynced()
{
    setState(LM_SYNCED_STATE);
}

void
LedgerManagerImpl::setState(State s)
{
    if (s != getState())
    {
        std::string oldState = getStateHuman();
        mState = s;
        mApp.syncOwnMetrics();
        CLOG_INFO(Ledger, "Changing state {} -> {}", oldState, getStateHuman());
        if (mState != LM_CATCHING_UP_STATE)
        {
            mApp.getCatchupManager().logAndUpdateCatchupStatus(true);
        }

        if (mState == LM_CATCHING_UP_STATE && !mStartCatchup)
        {
            mStartCatchup = std::make_unique<VirtualClock::time_point>(
                mApp.getClock().now());
        }
        else if (mState == LM_SYNCED_STATE && mStartCatchup)
        {
            std::chrono::nanoseconds duration =
                mApp.getClock().now() - *mStartCatchup;
            mCatchupDuration.Update(duration);
            CLOG_DEBUG(Perf, "Caught up to the network in {} seconds",
                       std::chrono::duration<double>(duration).count());
        }
    }
}

LedgerManager::State
LedgerManagerImpl::getState() const
{
    return mState;
}

std::string
LedgerManagerImpl::getStateHuman() const
{
    static std::array<const char*, LM_NUM_STATE> stateStrings = std::array{
        "LM_BOOTING_STATE", "LM_SYNCED_STATE", "LM_CATCHING_UP_STATE"};
    return std::string(stateStrings[getState()]);
}

LedgerHeader
LedgerManager::genesisLedger()
{
    LedgerHeader result;
    // all fields are initialized by default to 0
    // set the ones that are not 0
    result.ledgerVersion = GENESIS_LEDGER_VERSION;
    result.baseFee = GENESIS_LEDGER_BASE_FEE;
    result.baseReserve = GENESIS_LEDGER_BASE_RESERVE;
    result.maxTxSetSize = GENESIS_LEDGER_MAX_TX_SIZE;
    result.totalCoins = GENESIS_LEDGER_TOTAL_COINS;
    result.ledgerSeq = GENESIS_LEDGER_SEQ;
    return result;
}

void
LedgerManagerImpl::startNewLedger(LedgerHeader const& genesisLedger)
{
    auto ledgerTime = mLedgerClose.TimeScope();
    SecretKey skey = SecretKey::fromSeed(mApp.getNetworkID());

    LedgerTxn ltx(mApp.getLedgerTxnRoot(), false);
    auto const& cfg = mApp.getConfig();

    ltx.loadHeader().current() = genesisLedger;
    if (cfg.USE_CONFIG_FOR_GENESIS)
    {
        SorobanNetworkConfig::initializeGenesisLedgerForTesting(
            cfg.TESTING_UPGRADE_LEDGER_PROTOCOL_VERSION, ltx, mApp);
    }

    LedgerEntry rootEntry;
    rootEntry.lastModifiedLedgerSeq = 1;
    rootEntry.data.type(ACCOUNT);
    auto& rootAccount = rootEntry.data.account();
    rootAccount.accountID = skey.getPublicKey();
    rootAccount.thresholds[0] = 1;
    rootAccount.balance = genesisLedger.totalCoins;
    ltx.create(rootEntry);

    CLOG_INFO(Ledger, "Established genesis ledger, closing");
    CLOG_INFO(Ledger, "Root account: {}", skey.getStrKeyPublic());
    CLOG_INFO(Ledger, "Root account seed: {}", skey.getStrKeySeed().value);
    ledgerClosed(ltx, /*ledgerCloseMeta*/ nullptr, /*initialLedgerVers*/ 0);
    ltx.commit();
}

void
LedgerManagerImpl::startNewLedger()
{
    auto ledger = genesisLedger();
    auto const& cfg = mApp.getConfig();
    if (cfg.USE_CONFIG_FOR_GENESIS)
    {
        ledger.ledgerVersion = cfg.TESTING_UPGRADE_LEDGER_PROTOCOL_VERSION;
        ledger.baseFee = cfg.TESTING_UPGRADE_DESIRED_FEE;
        ledger.baseReserve = cfg.TESTING_UPGRADE_RESERVE;
        ledger.maxTxSetSize = cfg.TESTING_UPGRADE_MAX_TX_SET_SIZE;
    }

    startNewLedger(ledger);
}

static void
setLedgerTxnHeader(LedgerHeader const& lh, Application& app)
{
    LedgerTxn ltx(app.getLedgerTxnRoot());
    ltx.loadHeader().current() = lh;
    ltx.commit();
}

void
LedgerManagerImpl::loadLastKnownLedger(bool restoreBucketlist,
                                       bool isLedgerStateReady)
{
    ZoneScoped;

    // Step 1. Load LCL state from the DB and extract latest ledger hash
    string lastLedger =
        mApp.getPersistentState().getState(PersistentState::kLastClosedLedger);

    if (lastLedger.empty())
    {
        throw std::runtime_error(
            "No reference in DB to any last closed ledger");
    }

    CLOG_INFO(Ledger, "Last closed ledger (LCL) hash is {}", lastLedger);
    Hash lastLedgerHash = hexToBin256(lastLedger);

    // Step 2. Restore LedgerHeader from DB based on the ledger hash derived
    // earlier, or verify we're at genesis if in no-history mode
    std::optional<LedgerHeader> latestLedgerHeader;
    if (mApp.getConfig().MODE_STORES_HISTORY_LEDGERHEADERS)
    {
        if (mRebuildInMemoryState)
        {
            LedgerHeader lh;
            CLOG_INFO(Ledger,
                      "Setting empty ledger while core rebuilds state: {}",
                      ledgerAbbrev(lh));
            setLedgerTxnHeader(lh, mApp);
            latestLedgerHeader = lh;
        }
        else
        {
            auto currentLedger =
                LedgerHeaderUtils::loadByHash(getDatabase(), lastLedgerHash);
            if (!currentLedger)
            {
                throw std::runtime_error("Could not load ledger from database");
            }
            HistoryArchiveState has = getLastClosedLedgerHAS();
            if (currentLedger->ledgerSeq != has.currentLedger)
            {
                throw std::runtime_error("Invalid database state: last known "
                                         "ledger does not agree with HAS");
            }

            CLOG_INFO(Ledger, "Loaded LCL header from database: {}",
                      ledgerAbbrev(*currentLedger));
            setLedgerTxnHeader(*currentLedger, mApp);
            latestLedgerHeader = *currentLedger;
        }
    }
    else
    {
        // In no-history mode, this method should only be called when
        // the LCL is genesis.
        releaseAssertOrThrow(mLastClosedLedger.hash == lastLedgerHash);
        releaseAssertOrThrow(mLastClosedLedger.header.ledgerSeq ==
                             GENESIS_LEDGER_SEQ);
        CLOG_INFO(Ledger, "LCL is genesis: {}",
                  ledgerAbbrev(mLastClosedLedger));
        latestLedgerHeader = mLastClosedLedger.header;
    }

    releaseAssert(latestLedgerHeader.has_value());

    // Step 3. Restore BucketList if we're doing a full core startup
    // (startServices=true), OR when using BucketListDB
    if (restoreBucketlist || mApp.getConfig().isUsingBucketListDB())
    {
        HistoryArchiveState has = getLastClosedLedgerHAS();
        auto missing = mApp.getBucketManager().checkForMissingBucketsFiles(has);
        auto pubmissing = mApp.getHistoryManager()
                              .getMissingBucketsReferencedByPublishQueue();
        missing.insert(missing.end(), pubmissing.begin(), pubmissing.end());
        if (!missing.empty())
        {
            CLOG_ERROR(Ledger,
                       "{} buckets are missing from bucket directory '{}'",
                       missing.size(), mApp.getBucketManager().getBucketDir());
            throw std::runtime_error("Bucket directory is corrupt");
        }

        if (mApp.getConfig().MODE_ENABLES_BUCKETLIST)
        {
            // Only restart merges in full startup mode. Many modes in core
            // (standalone offline commands, in-memory setup) do not need to
            // spin up expensive merge processes.
            auto assumeStateWork =
                mApp.getWorkScheduler().executeWork<AssumeStateWork>(
                    has, latestLedgerHeader->ledgerVersion, restoreBucketlist);
            if (assumeStateWork->getState() == BasicWork::State::WORK_SUCCESS)
            {
                CLOG_INFO(Ledger, "Assumed bucket-state for LCL: {}",
                          ledgerAbbrev(*latestLedgerHeader));
            }
            else
            {
                // Work should only fail during graceful shutdown
                releaseAssertOrThrow(mApp.isStopping());
            }
        }
    }

    // Step 4. Restore LedgerManager's internal state
    advanceLedgerPointers(*latestLedgerHeader);

    if (protocolVersionStartsFrom(latestLedgerHeader->ledgerVersion,
                                  SOROBAN_PROTOCOL_VERSION))
    {
        if (isLedgerStateReady)
        {
            // Step 5. If ledger state is ready and core is in v20, load network
            // configs right away
            LedgerTxn ltx(mApp.getLedgerTxnRoot());
            updateNetworkConfig(ltx);
        }
        else
        {
            // In some modes, e.g. in-memory, core's state is rebuilt
            // asynchronously via catchup. In this case, we're not able to load
            // the network config at this time, and instead must let catchup do
            // it when ready.
            CLOG_INFO(Ledger,
                      "Ledger state is being rebuilt, network config will "
                      "be loaded once the rebuild is done");
        }
    }
}

bool
LedgerManagerImpl::rebuildingInMemoryState()
{
    return mRebuildInMemoryState;
}

void
LedgerManagerImpl::setupInMemoryStateRebuild()
{
    if (!mRebuildInMemoryState)
    {
        LedgerHeader lh;
        HistoryArchiveState has;
        auto& ps = mApp.getPersistentState();
        ps.setState(PersistentState::kLastClosedLedger,
                    binToHex(xdrSha256(lh)));
        ps.setState(PersistentState::kHistoryArchiveState, has.toString());
        ps.setState(PersistentState::kLastSCPData, "");
        ps.setState(PersistentState::kLastSCPDataXDR, "");
        ps.setState(PersistentState::kLedgerUpgrades, "");
        mRebuildInMemoryState = true;
    }
}

Database&
LedgerManagerImpl::getDatabase()
{
    return mApp.getDatabase();
}

uint32_t
LedgerManagerImpl::getLastMaxTxSetSize() const
{
    return mLastClosedLedger.header.maxTxSetSize;
}

uint32_t
LedgerManagerImpl::getLastMaxTxSetSizeOps() const
{
    auto n = mLastClosedLedger.header.maxTxSetSize;
    return protocolVersionStartsFrom(mLastClosedLedger.header.ledgerVersion,
                                     ProtocolVersion::V_11)
               ? n
               : (n * MAX_OPS_PER_TX);
}

Resource
LedgerManagerImpl::maxLedgerResources(bool isSoroban)
{
    ZoneScoped;

    if (isSoroban)
    {
        auto conf = getSorobanNetworkConfig();
        std::vector<int64_t> limits = {conf.ledgerMaxTxCount(),
                                       conf.ledgerMaxInstructions(),
                                       conf.ledgerMaxTransactionSizesBytes(),
                                       conf.ledgerMaxReadBytes(),
                                       conf.ledgerMaxWriteBytes(),
                                       conf.ledgerMaxReadLedgerEntries(),
                                       conf.ledgerMaxWriteLedgerEntries()};
        return Resource(limits);
    }
    else
    {
        uint32_t maxOpsLedger = getLastMaxTxSetSizeOps();
        return Resource(maxOpsLedger);
    }
}

Resource
LedgerManagerImpl::maxSorobanTransactionResources()
{
    ZoneScoped;

    auto const& conf = mApp.getLedgerManager().getSorobanNetworkConfig();
    int64_t const opCount = 1;
    std::vector<int64_t> limits = {opCount,
                                   conf.txMaxInstructions(),
                                   conf.txMaxSizeBytes(),
                                   conf.txMaxReadBytes(),
                                   conf.txMaxWriteBytes(),
                                   conf.txMaxReadLedgerEntries(),
                                   conf.txMaxWriteLedgerEntries()};
    return Resource(limits);
}

int64_t
LedgerManagerImpl::getLastMinBalance(uint32_t ownerCount) const
{
    auto const& lh = mLastClosedLedger.header;
    if (protocolVersionIsBefore(lh.ledgerVersion, ProtocolVersion::V_9))
        return (2 + ownerCount) * lh.baseReserve;
    else
        return (2LL + ownerCount) * int64_t(lh.baseReserve);
}

uint32_t
LedgerManagerImpl::getLastReserve() const
{
    return mLastClosedLedger.header.baseReserve;
}

uint32_t
LedgerManagerImpl::getLastTxFee() const
{
    return mLastClosedLedger.header.baseFee;
}

LedgerHeaderHistoryEntry const&
LedgerManagerImpl::getLastClosedLedgerHeader() const
{
    return mLastClosedLedger;
}

HistoryArchiveState
LedgerManagerImpl::getLastClosedLedgerHAS()
{
    ZoneScoped;

    string hasString = mApp.getPersistentState().getState(
        PersistentState::kHistoryArchiveState);
    HistoryArchiveState has;
    has.fromString(hasString);
    return has;
}

uint32_t
LedgerManagerImpl::getLastClosedLedgerNum() const
{
    return mLastClosedLedger.header.ledgerSeq;
}

SorobanNetworkConfig&
LedgerManagerImpl::getSorobanNetworkConfigInternal()
{
    releaseAssert(mSorobanNetworkConfig);
    return *mSorobanNetworkConfig;
}

SorobanNetworkConfig const&
LedgerManagerImpl::getSorobanNetworkConfig()
{
    return getSorobanNetworkConfigInternal();
}

bool
LedgerManagerImpl::hasSorobanNetworkConfig() const
{
    return mSorobanNetworkConfig.has_value();
}

#ifdef BUILD_TESTS
SorobanNetworkConfig&
LedgerManagerImpl::getMutableSorobanNetworkConfig()
{
    return getSorobanNetworkConfigInternal();
}
std::vector<TransactionMetaFrame> const&
LedgerManagerImpl::getLastClosedLedgerTxMeta()
{
    return mLastLedgerTxMeta;
}
#endif

SorobanMetrics&
LedgerManagerImpl::getSorobanMetrics()
{
    return mSorobanMetrics;
}

void
LedgerManagerImpl::publishSorobanMetrics()
{
    releaseAssert(mSorobanNetworkConfig);
    // first publish the network config limits
    mSorobanMetrics.mConfigContractDataKeySizeBytes.set_count(
        mSorobanNetworkConfig->maxContractDataKeySizeBytes());
    mSorobanMetrics.mConfigMaxContractDataEntrySizeBytes.set_count(
        mSorobanNetworkConfig->maxContractDataEntrySizeBytes());
    mSorobanMetrics.mConfigMaxContractSizeBytes.set_count(
        mSorobanNetworkConfig->maxContractSizeBytes());
    mSorobanMetrics.mConfigTxMaxSizeByte.set_count(
        mSorobanNetworkConfig->txMaxSizeBytes());
    mSorobanMetrics.mConfigTxMaxCpuInsn.set_count(
        mSorobanNetworkConfig->txMaxInstructions());
    mSorobanMetrics.mConfigTxMemoryLimitBytes.set_count(
        mSorobanNetworkConfig->txMemoryLimit());
    mSorobanMetrics.mConfigTxMaxReadLedgerEntries.set_count(
        mSorobanNetworkConfig->txMaxReadLedgerEntries());
    mSorobanMetrics.mConfigTxMaxReadBytes.set_count(
        mSorobanNetworkConfig->txMaxReadBytes());
    mSorobanMetrics.mConfigTxMaxWriteLedgerEntries.set_count(
        mSorobanNetworkConfig->txMaxWriteLedgerEntries());
    mSorobanMetrics.mConfigTxMaxWriteBytes.set_count(
        mSorobanNetworkConfig->txMaxWriteBytes());
    mSorobanMetrics.mConfigMaxContractEventsSizeBytes.set_count(
        mSorobanNetworkConfig->txMaxContractEventsSizeBytes());
    mSorobanMetrics.mConfigLedgerMaxTxCount.set_count(
        mSorobanNetworkConfig->ledgerMaxTxCount());
    mSorobanMetrics.mConfigLedgerMaxInstructions.set_count(
        mSorobanNetworkConfig->ledgerMaxInstructions());
    mSorobanMetrics.mConfigLedgerMaxTxsSizeByte.set_count(
        mSorobanNetworkConfig->ledgerMaxTransactionSizesBytes());
    mSorobanMetrics.mConfigLedgerMaxReadLedgerEntries.set_count(
        mSorobanNetworkConfig->ledgerMaxReadLedgerEntries());
    mSorobanMetrics.mConfigLedgerMaxReadBytes.set_count(
        mSorobanNetworkConfig->ledgerMaxReadBytes());
    mSorobanMetrics.mConfigLedgerMaxWriteEntries.set_count(
        mSorobanNetworkConfig->ledgerMaxWriteLedgerEntries());
    mSorobanMetrics.mConfigLedgerMaxWriteBytes.set_count(
        mSorobanNetworkConfig->ledgerMaxWriteBytes());
    mSorobanMetrics.mConfigBucketListTargetSizeByte.set_count(
        mSorobanNetworkConfig->bucketListTargetSizeBytes());
    mSorobanMetrics.mConfigFeeWrite1KB.set_count(
        mSorobanNetworkConfig->feeWrite1KB());

    // then publish the actual ledger usage
    mSorobanMetrics.publishAndResetLedgerWideMetrics();
}

// called by txherder
void
LedgerManagerImpl::valueExternalized(LedgerCloseData const& ledgerData)
{
    ZoneScoped;

    // Capture LCL before we do any processing (which may trigger ledger close)
    auto lcl = getLastClosedLedgerNum();

    CLOG_INFO(Ledger,
              "Got consensus: [seq={}, prev={}, txs={}, ops={}, sv: {}]",
              ledgerData.getLedgerSeq(),
              hexAbbrev(ledgerData.getTxSet()->previousLedgerHash()),
              ledgerData.getTxSet()->sizeTxTotal(),
              ledgerData.getTxSet()->sizeOpTotalForLogging(),
              stellarValueToString(mApp.getConfig(), ledgerData.getValue()));

    auto st = getState();
    if (st != LedgerManager::LM_BOOTING_STATE &&
        st != LedgerManager::LM_CATCHING_UP_STATE &&
        st != LedgerManager::LM_SYNCED_STATE)
    {
        releaseAssert(false);
    }

    closeLedgerIf(ledgerData);

    auto& cm = mApp.getCatchupManager();

    cm.processLedger(ledgerData);

    // We set the state to synced
    // if we have closed the latest ledger we have heard of.
    bool appliedLatest = false;
    if (cm.getLargestLedgerSeqHeard() == getLastClosedLedgerNum())
    {
        setState(LM_SYNCED_STATE);
        appliedLatest = true;
    }

    // New ledger(s) got closed, notify Herder
    if (getLastClosedLedgerNum() > lcl)
    {
        CLOG_DEBUG(Ledger,
                   "LedgerManager::valueExternalized LCL advanced {} -> {}",
                   lcl, getLastClosedLedgerNum());
        mApp.getHerder().lastClosedLedgerIncreased(appliedLatest);
    }
}

void
LedgerManagerImpl::closeLedgerIf(LedgerCloseData const& ledgerData)
{
    ZoneScoped;
    if (mLastClosedLedger.header.ledgerSeq + 1 == ledgerData.getLedgerSeq())
    {
        auto& cm = mApp.getCatchupManager();
        // if catchup work is running, we don't want ledger manager to close
        // this ledger and potentially cause issues.
        if (cm.isCatchupInitialized() && !cm.catchupWorkIsDone())
        {
            CLOG_INFO(
                Ledger,
                "Can't close ledger: {}  in LM because catchup is running",
                ledgerAbbrev(mLastClosedLedger));
            return;
        }

        closeLedger(ledgerData);
        CLOG_INFO(Ledger, "Closed ledger: {}", ledgerAbbrev(mLastClosedLedger));
    }
    else if (ledgerData.getLedgerSeq() <= mLastClosedLedger.header.ledgerSeq)
    {
        CLOG_INFO(
            Ledger,
            "Skipping close ledger: local state is {}, more recent than {}",
            mLastClosedLedger.header.ledgerSeq, ledgerData.getLedgerSeq());
    }
    else
    {
        if (mState != LM_CATCHING_UP_STATE)
        {
            // Out of sync, buffer what we just heard and start catchup.
            CLOG_INFO(
                Ledger, "Lost sync, local LCL is {}, network closed ledger {}",
                mLastClosedLedger.header.ledgerSeq, ledgerData.getLedgerSeq());
        }

        setState(LM_CATCHING_UP_STATE);
    }
}

void
LedgerManagerImpl::startCatchup(
    CatchupConfiguration configuration, std::shared_ptr<HistoryArchive> archive,
    std::set<std::shared_ptr<Bucket>> bucketsToRetain)
{
    ZoneScoped;
    setState(LM_CATCHING_UP_STATE);
    mApp.getCatchupManager().startCatchup(configuration, archive,
                                          bucketsToRetain);
}

uint64_t
LedgerManagerImpl::secondsSinceLastLedgerClose() const
{
    uint64_t ct = getLastClosedLedgerHeader().header.scpValue.closeTime;
    if (ct == 0)
    {
        return 0;
    }
    uint64_t now = mApp.timeNow();
    return (now > ct) ? (now - ct) : 0;
}

void
LedgerManagerImpl::syncMetrics()
{
    mLedgerAge.set_count(secondsSinceLastLedgerClose());
    mApp.syncOwnMetrics();
}

void
LedgerManagerImpl::emitNextMeta()
{
    ZoneScoped;

    releaseAssert(mNextMetaToEmit);
    releaseAssert(mMetaStream || mMetaDebugStream);
    auto timer = LogSlowExecution("MetaStream write",
                                  LogSlowExecution::Mode::AUTOMATIC_RAII,
                                  "took", std::chrono::milliseconds(100));
    auto streamWrite = mMetaStreamWriteTime.TimeScope();
    if (mMetaStream)
    {
        size_t written = 0;
        mMetaStream->writeOne(mNextMetaToEmit->getXDR(), nullptr, &written);
        mMetaStream->flush();
        mMetaStreamBytes.Mark(written);
    }
    if (mMetaDebugStream)
    {
        mMetaDebugStream->writeOne(mNextMetaToEmit->getXDR());
        // Flush debug meta in case there's a crash later in commit (in which
        // case we'd lose the data in internal buffers). This way we preserve
        // the meta for problematic ledgers that is vital for diagnostics.
        mMetaDebugStream->flush();
    }
    mNextMetaToEmit.reset();
}

/*
    This is the main method that closes the current ledger based on
the close context that was computed by SCP or by the historical module
during replays.

*/
void
LedgerManagerImpl::closeLedger(LedgerCloseData const& ledgerData)
{
#ifdef BUILD_TESTS
    mLastLedgerTxMeta.clear();
#endif
    ZoneScoped;
    auto ledgerTime = mLedgerClose.TimeScope();
    LogSlowExecution closeLedgerTime{"closeLedger",
                                     LogSlowExecution::Mode::MANUAL, "",
                                     std::chrono::milliseconds::max()};

    LedgerTxn ltx(mApp.getLedgerTxnRoot());
    auto header = ltx.loadHeader();
    auto initialLedgerVers = header.current().ledgerVersion;
    ++header.current().ledgerSeq;
    header.current().previousLedgerHash = mLastClosedLedger.hash;
    CLOG_DEBUG(Ledger, "starting closeLedger() on ledgerSeq={}",
               header.current().ledgerSeq);

    ZoneValue(static_cast<int64_t>(header.current().ledgerSeq));

    auto now = mApp.getClock().now();
    mLedgerAgeClosed.Update(now - mLastClose);
    mLastClose = now;
    mLedgerAge.set_count(0);

    TxSetXDRFrameConstPtr txSet = ledgerData.getTxSet();

    // If we do not support ledger version, we can't apply that ledger, fail!
    if (header.current().ledgerVersion >
        mApp.getConfig().LEDGER_PROTOCOL_VERSION)
    {
        CLOG_ERROR(Ledger, "Unknown ledger version: {}",
                   header.current().ledgerVersion);
        CLOG_ERROR(Ledger, "{}", UPGRADE_STELLAR_CORE);
        throw std::runtime_error(fmt::format(
            FMT_STRING("cannot apply ledger with not supported version: {:d}"),
            header.current().ledgerVersion));
    }

    if (txSet->previousLedgerHash() != getLastClosedLedgerHeader().hash)
    {
        CLOG_ERROR(Ledger, "TxSet mismatch: LCD wants {}, LCL is {}",
                   ledgerAbbrev(ledgerData.getLedgerSeq() - 1,
                                txSet->previousLedgerHash()),
                   ledgerAbbrev(getLastClosedLedgerHeader()));

        CLOG_ERROR(Ledger, "{}",
                   xdrToCerealString(getLastClosedLedgerHeader(), "Full LCL"));
        CLOG_ERROR(Ledger, "{}", POSSIBLY_CORRUPTED_LOCAL_DATA);

        throw std::runtime_error("txset mismatch");
    }

    if (txSet->getContentsHash() != ledgerData.getValue().txSetHash)
    {
        CLOG_ERROR(
            Ledger,
            "Corrupt transaction set: TxSet hash is {}, SCP value reports {}",
            binToHex(txSet->getContentsHash()),
            binToHex(ledgerData.getValue().txSetHash));
        CLOG_ERROR(Ledger, "{}", POSSIBLY_CORRUPTED_QUORUM_SET);

        throw std::runtime_error("corrupt transaction set");
    }

    auto const& sv = ledgerData.getValue();
    header.current().scpValue = sv;

    maybeResetLedgerCloseMetaDebugStream(header.current().ledgerSeq);
    auto applicableTxSet = txSet->prepareForApply(mApp);

    if (applicableTxSet == nullptr)
    {
        CLOG_ERROR(
            Ledger,
            "Corrupt transaction set: TxSet cannot be prepared for apply",
            binToHex(txSet->getContentsHash()),
            binToHex(ledgerData.getValue().txSetHash));
        CLOG_ERROR(Ledger, "{}", POSSIBLY_CORRUPTED_QUORUM_SET);
        throw std::runtime_error("transaction set cannot be processed");
    }

    // In addition to the _canonical_ LedgerResultSet hashed into the
    // LedgerHeader, we optionally collect an even-more-fine-grained record of
    // the ledger entries modified by each tx during tx processing in a
    // LedgerCloseMeta, for streaming to attached clients (typically: horizon).
    std::unique_ptr<LedgerCloseMetaFrame> ledgerCloseMeta;
    if (mMetaStream || mMetaDebugStream)
    {
        if (mNextMetaToEmit)
        {
            releaseAssert(mNextMetaToEmit->ledgerHeader().hash ==
                          getLastClosedLedgerHeader().hash);
            emitNextMeta();
        }
        releaseAssert(!mNextMetaToEmit);
        // Write to a local variable rather than a member variable first: this
        // enables us to discard incomplete meta and retry, should anything in
        // this method throw.
        ledgerCloseMeta = std::make_unique<LedgerCloseMetaFrame>(
            header.current().ledgerVersion);
        ledgerCloseMeta->reserveTxProcessing(applicableTxSet->sizeTxTotal());
        ledgerCloseMeta->populateTxSet(*txSet);
    }

    // first, prefetch source accounts for txset, then charge fees
    prefetchTxSourceIds(*applicableTxSet);
    auto const mutableTxResults =
        processFeesSeqNums(*applicableTxSet, ltx, ledgerCloseMeta);

    auto txResultSet = applyTransactions(*applicableTxSet, mutableTxResults,
                                         ltx, ledgerCloseMeta);
    if (mApp.getConfig().MODE_STORES_HISTORY_MISC)
    {
        storeTxSet(mApp.getDatabase(), ltx.loadHeader().current().ledgerSeq,
                   *txSet);
    }

    ltx.loadHeader().current().txSetResultHash = xdrSha256(txResultSet);

    // apply any upgrades that were decided during consensus
    // this must be done after applying transactions as the txset
    // was validated before upgrades
    for (size_t i = 0; i < sv.upgrades.size(); i++)
    {
        LedgerUpgrade lupgrade;
        LedgerSnapshot ls(ltx);
        auto valid =
            Upgrades::isValidForApply(sv.upgrades[i], lupgrade, mApp, ls);
        switch (valid)
        {
        case Upgrades::UpgradeValidity::VALID:
            break;
        case Upgrades::UpgradeValidity::XDR_INVALID:
        {
            CLOG_ERROR(Ledger, "Unknown upgrade at index {}", i);
            continue;
        }
        case Upgrades::UpgradeValidity::INVALID:
        {
            CLOG_ERROR(Ledger, "Invalid upgrade at index {}: {}", i,
                       xdrToCerealString(lupgrade, "LedgerUpgrade"));
            continue;
        }
        }

        try
        {
            LedgerTxn ltxUpgrade(ltx);
            Upgrades::applyTo(lupgrade, mApp, ltxUpgrade);

            auto ledgerSeq = ltxUpgrade.loadHeader().current().ledgerSeq;
            LedgerEntryChanges changes = ltxUpgrade.getChanges();
            if (ledgerCloseMeta)
            {
                auto& up = ledgerCloseMeta->upgradesProcessing();
                up.emplace_back();
                UpgradeEntryMeta& uem = up.back();
                uem.upgrade = lupgrade;
                uem.changes = changes;
            }
            // Note: Index from 1 rather than 0 to match the behavior of
            // storeTransaction.
            if (mApp.getConfig().MODE_STORES_HISTORY_MISC)
            {
                Upgrades::storeUpgradeHistory(getDatabase(), ledgerSeq,
                                              lupgrade, changes,
                                              static_cast<int>(i + 1));
            }
            ltxUpgrade.commit();
        }
        catch (std::runtime_error& e)
        {
            CLOG_ERROR(Ledger, "Exception during upgrade: {}", e.what());
        }
        catch (...)
        {
            CLOG_ERROR(Ledger, "Unknown exception during upgrade");
        }
    }
    auto maybeNewVersion = ltx.loadHeader().current().ledgerVersion;
    auto ledgerSeq = ltx.loadHeader().current().ledgerSeq;
    if (protocolVersionStartsFrom(maybeNewVersion, SOROBAN_PROTOCOL_VERSION))
    {
        updateNetworkConfig(ltx);
    }

    ledgerClosed(ltx, ledgerCloseMeta, initialLedgerVers);

    if (ledgerData.getExpectedHash() &&
        *ledgerData.getExpectedHash() != mLastClosedLedger.hash)
    {
        throw std::runtime_error("Local node's ledger corrupted during close");
    }

    if (mMetaStream || mMetaDebugStream)
    {
        releaseAssert(ledgerCloseMeta);
        ledgerCloseMeta->ledgerHeader() = mLastClosedLedger;

        // At this point we've got a complete meta and we can store it to the
        // member variable: if we throw while committing below, we will at worst
        // emit duplicate meta, when retrying.
        mNextMetaToEmit = std::move(ledgerCloseMeta);

        // If the LedgerCloseData provided an expected hash, then we validated
        // it above.
        if (!mApp.getConfig().EXPERIMENTAL_PRECAUTION_DELAY_META ||
            ledgerData.getExpectedHash())
        {
            emitNextMeta();
        }
    }

    // The next 5 steps happen in a relatively non-obvious, subtle order.
    // This is unfortunate and it would be nice if we could make it not
    // be so subtle, but for the time being this is where we are.
    //
    // 1. Queue any history-checkpoint to the database, _within_ the current
    //    transaction. This way if there's a crash after commit and before
    //    we've published successfully, we'll re-publish on restart.
    //
    // 2. Commit the current transaction.
    //
    // 3. Start background eviction scan for the next ledger, _after_ the commit
    //    so that it takes its snapshot of network setting from the
    //    committed state.
    //
    // 4. Start any queued checkpoint publishing, _after_ the commit so that
    //    it takes its snapshot of history-rows from the committed state, but
    //    _before_ we GC any buckets (because this is the step where the
    //    bucket refcounts are incremented for the duration of the publish).
    //
    // 5. GC unreferenced buckets. Only do this once publishes are in progress.

    // step 1
    auto& hm = mApp.getHistoryManager();
    hm.maybeQueueHistoryCheckpoint();

    // step 2
    ltx.commit();

    // step 3
    if (protocolVersionStartsFrom(initialLedgerVers,
                                  SOROBAN_PROTOCOL_VERSION) &&
        mApp.getConfig().isUsingBackgroundEviction())
    {
        mApp.getBucketManager().startBackgroundEvictionScan(ledgerSeq + 1);
    }

    // step 4
    hm.publishQueuedHistory();
    hm.logAndUpdatePublishStatus();

    // step 5
    mApp.getBucketManager().forgetUnreferencedBuckets();

    if (!mApp.getConfig().OP_APPLY_SLEEP_TIME_WEIGHT_FOR_TESTING.empty())
    {
        // Sleep for a parameterized amount of time in simulation mode
        std::discrete_distribution<uint32> distribution(
            mApp.getConfig().OP_APPLY_SLEEP_TIME_WEIGHT_FOR_TESTING.begin(),
            mApp.getConfig().OP_APPLY_SLEEP_TIME_WEIGHT_FOR_TESTING.end());
        std::chrono::microseconds sleepFor{0};
        auto txSetSizeOp = applicableTxSet->sizeOpTotal();
        for (size_t i = 0; i < txSetSizeOp; i++)
        {
            sleepFor +=
                mApp.getConfig()
                    .OP_APPLY_SLEEP_TIME_DURATION_FOR_TESTING[distribution(
                        gRandomEngine)];
        }
        std::chrono::microseconds applicationTime =
            closeLedgerTime.checkElapsedTime();
        if (applicationTime < sleepFor)
        {
            sleepFor -= applicationTime;
            CLOG_DEBUG(Perf, "Simulate application: sleep for {} microseconds",
                       sleepFor.count());
            std::this_thread::sleep_for(sleepFor);
        }
    }

    std::chrono::duration<double> ledgerTimeSeconds = ledgerTime.Stop();
    CLOG_DEBUG(Perf, "Applied ledger in {} seconds", ledgerTimeSeconds.count());
    FrameMark;
}

void
LedgerManagerImpl::deleteOldEntries(Database& db, uint32_t ledgerSeq,
                                    uint32_t count)
{
    ZoneScoped;
    soci::transaction txscope(db.getSession());
    db.clearPreparedStatementCache();
    LedgerHeaderUtils::deleteOldEntries(db, ledgerSeq, count);
    deleteOldTransactionHistoryEntries(db, ledgerSeq, count);
    HerderPersistence::deleteOldEntries(db, ledgerSeq, count);
    Upgrades::deleteOldEntries(db, ledgerSeq, count);
    db.clearPreparedStatementCache();
    txscope.commit();
}

void
LedgerManagerImpl::deleteNewerEntries(Database& db, uint32_t ledgerSeq)
{
    ZoneScoped;
    soci::transaction txscope(db.getSession());
    db.clearPreparedStatementCache();

    // as we use this method only when we apply buckets, we have to preserve
    // data for everything but ledger header
    LedgerHeaderUtils::deleteNewerEntries(db, ledgerSeq);
    // for other data we delete data *after*
    ++ledgerSeq;
    deleteNewerTransactionHistoryEntries(db, ledgerSeq);
    HerderPersistence::deleteNewerEntries(db, ledgerSeq);
    Upgrades::deleteNewerEntries(db, ledgerSeq);
    db.clearPreparedStatementCache();
    txscope.commit();
}

void
LedgerManagerImpl::setLastClosedLedger(
    LedgerHeaderHistoryEntry const& lastClosed, bool storeInDB)
{
    ZoneScoped;
    LedgerTxn ltx(mApp.getLedgerTxnRoot());
    auto header = ltx.loadHeader();
    header.current() = lastClosed.header;
    storeCurrentLedger(header.current(), storeInDB);
    ltx.commit();

    mRebuildInMemoryState = false;
    advanceLedgerPointers(lastClosed.header);
    LedgerTxn ltx2(mApp.getLedgerTxnRoot(), false,
                   TransactionMode::READ_ONLY_WITHOUT_SQL_TXN);
    if (protocolVersionStartsFrom(ltx2.loadHeader().current().ledgerVersion,
                                  SOROBAN_PROTOCOL_VERSION))
    {
        mApp.getLedgerManager().updateNetworkConfig(ltx2);
    }
}

void
LedgerManagerImpl::manuallyAdvanceLedgerHeader(LedgerHeader const& header)
{
    if (!mApp.getConfig().MANUAL_CLOSE || !mApp.getConfig().RUN_STANDALONE)
    {
        throw std::logic_error(
            "May only manually advance ledger header sequence number with "
            "MANUAL_CLOSE and RUN_STANDALONE");
    }
    advanceLedgerPointers(header, false);
}

void
LedgerManagerImpl::setupLedgerCloseMetaStream()
{
    ZoneScoped;

    if (mMetaStream)
    {
        throw std::runtime_error("LedgerManagerImpl already streaming");
    }
    auto& cfg = mApp.getConfig();
    if (cfg.METADATA_OUTPUT_STREAM != "")
    {
        // We can't be sure we're writing to a stream that supports fsync;
        // pipes typically error when you try. So we don't do it.
        mMetaStream = std::make_unique<XDROutputFileStream>(
            mApp.getClock().getIOContext(),
            /*fsyncOnClose=*/false);
        std::regex fdrx("^fd:([0-9]+)$");
        std::smatch sm;
        if (std::regex_match(cfg.METADATA_OUTPUT_STREAM, sm, fdrx))
        {
            int fd = std::stoi(sm[1]);
            CLOG_INFO(Ledger, "Streaming metadata to file descriptor {}", fd);
            mMetaStream->fdopen(fd);
        }
        else
        {
            CLOG_INFO(Ledger, "Streaming metadata to '{}'",
                      cfg.METADATA_OUTPUT_STREAM);
            mMetaStream->open(cfg.METADATA_OUTPUT_STREAM);
        }
    }
}
void
LedgerManagerImpl::maybeResetLedgerCloseMetaDebugStream(uint32_t ledgerSeq)
{
    ZoneScoped;

    if (mApp.getConfig().METADATA_DEBUG_LEDGERS != 0)
    {
        if (mMetaDebugStream)
        {
            if (!metautils::isDebugSegmentBoundary(ledgerSeq))
            {
                // If we've got a stream open and aren't at a reset boundary,
                // just return -- keep streaming into it.
                return;
            }

            // If we have an open stream and there is already a flush-and-rotate
            // work _running_ (measured by the existence of a shared_ptr at the
            // end of the weak_ptr), we're in a slightly awkward position since
            // we can't synchronously close the stream (that would fsync) and we
            // can't asynchronously close the stream either (that would race
            // against the running work that is presumably flushing-and-rotating
            // the _previous_ stream). So we just skip an iteration of launching
            // flush-and-rotate work, and try again on the next boundary.
            auto existing = mFlushAndRotateMetaDebugWork.lock();
            if (existing && !existing->isDone())
            {
                CLOG_DEBUG(Ledger,
                           "Skipping flush-and-rotate of {} since work already "
                           "running",
                           mMetaDebugPath.string());
                return;
            }

            // If we are resetting and already have a stream, hand it off to the
            // flush-and-rotate work to finish up with.
            mFlushAndRotateMetaDebugWork =
                mApp.getWorkScheduler()
                    .scheduleWork<FlushAndRotateMetaDebugWork>(
                        mMetaDebugPath, std::move(mMetaDebugStream),
                        mApp.getConfig().METADATA_DEBUG_LEDGERS);
            mMetaDebugPath.clear();
        }

        // From here on we're starting a new stream, whether it's the first
        // such stream or a replacement for the one we just handed off to
        // flush-and-rotate. Either way, we should not have an existing one!
        releaseAssert(!mMetaDebugStream);
        auto tmpStream = std::make_unique<XDROutputFileStream>(
            mApp.getClock().getIOContext(),
            /*fsyncOnClose=*/true);

        auto metaDebugPath = metautils::getMetaDebugFilePath(
            mApp.getBucketManager().getBucketDir(), ledgerSeq);
        releaseAssert(metaDebugPath.has_parent_path());
        try
        {
            if (fs::mkpath(metaDebugPath.parent_path().string()))
            {
                // Skip any files for the same ledger. This is useful in case of
                // a crash-and-restart, where core emits duplicate meta (which
                // isn't garbage-collected until core gets unstuck, risking a
                // disk bloat).
                auto const regexForLedger =
                    metautils::getDebugMetaRegexForLedger(ledgerSeq);
                auto files = fs::findfiles(metaDebugPath.parent_path().string(),
                                           [&](std::string const& file) {
                                               return std::regex_match(
                                                   file, regexForLedger);
                                           });
                if (files.empty())
                {
                    CLOG_DEBUG(Ledger, "Streaming debug metadata to '{}'",
                               metaDebugPath.string());
                    tmpStream->open(metaDebugPath.string());

                    // If we get to this line, the stream is open.
                    mMetaDebugStream = std::move(tmpStream);
                    mMetaDebugPath = metaDebugPath;
                }
            }
            else
            {
                CLOG_WARNING(Ledger,
                             "Failed to make directory '{}' for debug metadata",
                             metaDebugPath.parent_path().string());
            }
        }
        catch (std::runtime_error& e)
        {
            CLOG_WARNING(Ledger,
                         "Failed to open debug metadata stream '{}': {}",
                         metaDebugPath.string(), e.what());
        }
    }
}

void
LedgerManagerImpl::advanceLedgerPointers(LedgerHeader const& header,
                                         bool debugLog)
{
    auto ledgerHash = xdrSha256(header);

    if (debugLog)
    {
        CLOG_DEBUG(Ledger, "Advancing LCL: {} -> {}",
                   ledgerAbbrev(mLastClosedLedger),
                   ledgerAbbrev(header, ledgerHash));
    }

    auto prevLedgerSeq = mLastClosedLedger.header.ledgerSeq;
    mLastClosedLedger.hash = ledgerHash;
    mLastClosedLedger.header = header;

    if (mApp.getConfig().isUsingBucketListDB() &&
        header.ledgerSeq != prevLedgerSeq)
    {
        mApp.getBucketManager()
            .getBucketSnapshotManager()
            .updateCurrentSnapshot(std::make_unique<BucketListSnapshot>(
                mApp.getBucketManager().getBucketList(), header));
    }
}

void
LedgerManagerImpl::updateNetworkConfig(AbstractLedgerTxn& rootLtx)
{
    ZoneScoped;

    uint32_t ledgerVersion{};
    {
        LedgerTxn ltx(rootLtx, false,
                      TransactionMode::READ_ONLY_WITHOUT_SQL_TXN);
        ledgerVersion = ltx.loadHeader().current().ledgerVersion;
    }

    if (protocolVersionStartsFrom(ledgerVersion, SOROBAN_PROTOCOL_VERSION))
    {
        if (!mSorobanNetworkConfig)
        {
            mSorobanNetworkConfig = std::make_optional<SorobanNetworkConfig>();
        }
        mSorobanNetworkConfig->loadFromLedger(
            rootLtx, mApp.getConfig().CURRENT_LEDGER_PROTOCOL_VERSION,
            ledgerVersion);
        publishSorobanMetrics();
    }
    else
    {
        throw std::runtime_error("Protocol version is before 20: "
                                 "cannot load Soroban network config");
    }
}

static bool
mergeOpInTx(std::vector<Operation> const& ops)
{
    for (auto const& op : ops)
    {
        if (op.body.type() == ACCOUNT_MERGE)
        {
            return true;
        }
    }
    return false;
}

std::vector<MutableTxResultPtr>
LedgerManagerImpl::processFeesSeqNums(
    ApplicableTxSetFrame const& txSet, AbstractLedgerTxn& ltxOuter,
    std::unique_ptr<LedgerCloseMetaFrame> const& ledgerCloseMeta)
{
    ZoneScoped;
    std::vector<MutableTxResultPtr> txResults;
    txResults.reserve(txSet.sizeTxTotal());
    CLOG_DEBUG(Ledger, "processing fees and sequence numbers");
    int index = 0;
    try
    {
        LedgerTxn ltx(ltxOuter);
        auto header = ltx.loadHeader().current();
        std::map<AccountID, SequenceNumber> accToMaxSeq;

        bool mergeSeen = false;
        for (auto const& phase : txSet.getPhasesInApplyOrder())
        {
            for (auto const& tx : phase)
            {
                LedgerTxn ltxTx(ltx);
                txResults.push_back(
                    tx->processFeeSeqNum(ltxTx, txSet.getTxBaseFee(tx)));

                if (protocolVersionStartsFrom(
                        ltxTx.loadHeader().current().ledgerVersion,
                        ProtocolVersion::V_19))
                {
                    auto res =
                        accToMaxSeq.emplace(tx->getSourceID(), tx->getSeqNum());
                    if (!res.second)
                    {
                        res.first->second =
                            std::max(res.first->second, tx->getSeqNum());
                    }

                    if (mergeOpInTx(tx->getRawOperations()))
                    {
                        mergeSeen = true;
                    }
                }

                LedgerEntryChanges changes = ltxTx.getChanges();
                if (ledgerCloseMeta)
                {
                    ledgerCloseMeta->pushTxProcessingEntry();
                    ledgerCloseMeta->setLastTxProcessingFeeProcessingChanges(
                        changes);
                }
                ++index;
                ltxTx.commit();
            }
        }
        if (protocolVersionStartsFrom(ltx.loadHeader().current().ledgerVersion,
                                      ProtocolVersion::V_19) &&
            mergeSeen)
        {
            for (const auto& [accountID, seqNum] : accToMaxSeq)
            {
                auto ltxe = loadMaxSeqNumToApply(ltx, accountID);
                if (!ltxe)
                {
                    InternalLedgerEntry gle(
                        InternalLedgerEntryType::MAX_SEQ_NUM_TO_APPLY);
                    gle.maxSeqNumToApplyEntry().sourceAccount = accountID;
                    gle.maxSeqNumToApplyEntry().maxSeqNum = seqNum;

                    auto res = ltx.create(gle);
                    if (!res)
                    {
                        throw std::runtime_error("create failed");
                    }
                }
                else
                {
                    throw std::runtime_error(
                        "found unexpected MAX_SEQ_NUM_TO_APPLY");
                }
            }
        }

        ltx.commit();
    }
    catch (std::exception& e)
    {
        CLOG_FATAL(Ledger, "processFeesSeqNums error @ {} : {}", index,
                   e.what());
        CLOG_FATAL(Ledger, "{}", REPORT_INTERNAL_BUG);
        throw;
    }

    return txResults;
}

void
LedgerManagerImpl::prefetchTxSourceIds(ApplicableTxSetFrame const& txSet)
{
    ZoneScoped;
    if (mApp.getConfig().PREFETCH_BATCH_SIZE > 0)
    {
        UnorderedSet<LedgerKey> keys;
        for (auto const& phase : txSet.getPhases())
        {
            for (auto const& tx : phase)
            {
                tx->insertKeysForFeeProcessing(keys);
            }
        }
        mApp.getLedgerTxnRoot().prefetchClassic(keys);
    }
}

void
LedgerManagerImpl::prefetchTransactionData(ApplicableTxSetFrame const& txSet)
{
    ZoneScoped;
    if (mApp.getConfig().PREFETCH_BATCH_SIZE > 0)
    {
        UnorderedSet<LedgerKey> sorobanKeys;
        auto lkMeter = make_unique<LedgerKeyMeter>();
        UnorderedSet<LedgerKey> classicKeys;
        for (auto const& phase : txSet.getPhases())
        {
            for (auto const& tx : phase)
            {
                if (tx->isSoroban())
                {
                    if (mApp.getConfig().isUsingBucketListDB())
                    {
                        tx->insertKeysForTxApply(sorobanKeys, lkMeter.get());
                    }
                }
                else
                {
                    tx->insertKeysForTxApply(classicKeys, nullptr);
                }
            }
        }
        // Prefetch classic and soroban keys separately for greater
        // visibility into the performance of each mode.
        if (mApp.getConfig().isUsingBucketListDB())
        {
            if (!sorobanKeys.empty())
            {
                mApp.getLedgerTxnRoot().prefetchSoroban(sorobanKeys,
                                                        lkMeter.get());
            }
        }
        mApp.getLedgerTxnRoot().prefetchClassic(classicKeys);
    }
}

UnorderedMap<LedgerKey, bool/*isTtlForReadOnlyEntry*/>
getStageTtlFootprint(ApplyStage const& stage)
{
    UnorderedMap<LedgerKey, bool> stageFootprint;
    auto getFootprint = [&stageFootprint](xdr::xvector<LedgerKey> const& keys, bool isReadOnly){
        for (auto const& lk : keys)
        {
            if(!isSorobanEntry(lk))
            {
                continue;
            }
            auto it = stageFootprint.emplace(getTTLKey(lk), isReadOnly);
            // If key already exists, overwrite isReadOnly if we see the key in a readWrite set
            if(!it.second && !isReadOnly)
            {
                it.first->second = false;
            }
        }
    };

    for (auto const& thread : stage)
    {
        for (auto const& txBundle : thread)
        {
            getFootprint(txBundle.tx->sorobanResources().footprint.readOnly, true);
            getFootprint(txBundle.tx->sorobanResources().footprint.readWrite, false);
        }
    }

    return stageFootprint;
}

ThreadEntryMap
LedgerManagerImpl::collectEntries(AbstractLedgerTxn& ltx, Thread const& txs)
{
    ThreadEntryMap entryMap;
    auto getEntries = [&](TransactionFrameBasePtr tx, MutableTxResultPtr resPayload, xdr::xvector<LedgerKey> const& keys, uint32_t& readBytes) {
        for (auto const& lk : keys)
        {
            if(!resPayload->isSuccess())
            {
                // This tx has already failed, so no need to load anything
                break;
            }
            
            auto ltxe = ltx.loadWithoutRecord(lk);
            if (ltxe)
            {
                readBytes += static_cast<uint32_t>(xdr::xdr_size(ltxe.current()));
                entryMap.emplace(
                    lk, ThreadEntry{ltxe.current(), false});

                if (isSorobanEntry(lk))
                {
                    auto ttlKey = getTTLKey(lk);
                    auto ttlLtxe = ltx.loadWithoutRecord(ttlKey);
                    // TTL entry must exist
                    releaseAssertOrThrow(ttlLtxe);

                    entryMap.emplace(
                        ttlKey,
                        ThreadEntry{ttlLtxe.current(), false});
                }
            }
            else
            {
                entryMap.emplace(
                    lk, ThreadEntry{std::nullopt, false});

                if (isSorobanEntry(lk))
                {
                    auto ttlKey = getTTLKey(lk);
                    auto ttlLtxe = ltx.loadWithoutRecord(ttlKey);
                    // TTL entry must not exist
                    releaseAssertOrThrow(!ttlLtxe);
                    entryMap.emplace(
                        ttlKey, ThreadEntry{std::nullopt, false});
                }
            }

            if(tx->sorobanResources().readBytes < readBytes)
            {
                // fast fail this tx now that we know the readBytes limit has been exceeded.
                resPayload->setInnermostResultCode(txFAILED);
                
                auto& sorobanOpResult = resPayload->getOpResultAt(0);
                auto opType = tx->getRawOperations().at(0).body.type();
                switch(opType)
                {
                    case INVOKE_HOST_FUNCTION:
                        sorobanOpResult.tr().invokeHostFunctionResult().code(INVOKE_HOST_FUNCTION_RESOURCE_LIMIT_EXCEEDED);
                    break;
                    case EXTEND_FOOTPRINT_TTL:
                        sorobanOpResult.tr().extendFootprintTTLResult().code(EXTEND_FOOTPRINT_TTL_RESOURCE_LIMIT_EXCEEDED);
                    break;
                    case RESTORE_FOOTPRINT:
                        sorobanOpResult.tr().restoreFootprintResult().code(RESTORE_FOOTPRINT_RESOURCE_LIMIT_EXCEEDED);
                    break;
                    default:
                        throw std::runtime_error("unknown soroban op type type");
                }
                
                break;
            }
        }
    };

    for (auto const& txBundle : txs)
    {
        uint32_t readBytes = 0;
        getEntries(txBundle.tx, txBundle.resPayload, txBundle.tx->sorobanResources().footprint.readOnly, readBytes);
        getEntries(txBundle.tx, txBundle.resPayload, txBundle.tx->sorobanResources().footprint.readWrite, readBytes);
    }

    return entryMap;
}

UnorderedMap<LedgerKey, uint32_t>
LedgerManagerImpl::applyThread(ThreadEntryMap& entryMap, Thread const& thread, UnorderedMap<LedgerKey, bool> const& stageTtlFootprint/*Make sure this is thread safe*/,
                               Config const& config,
                               SorobanNetworkConfig const& sorobanConfig,
                               ParallelLedgerInfo const& ledgerInfo,
                               Hash const& sorobanBasePrngSeed,
                               SorobanMetrics& sorobanMetrics)
{
    // TTL extensions for keys that don't appear in any RW set can't be
    // observable between transactions, so we track them separately and as a
    // cost optimization, we add them up and apply the cumulative bump at the
    // end.

    UnorderedMap<LedgerKey, uint32_t> readOnlyTtlExtensions;

    for (auto const& txBundle : thread)
    {
        releaseAssertOrThrow(txBundle.resPayload);

        SHA256 txSubSeedSha;
        txSubSeedSha.add(sorobanBasePrngSeed);
        txSubSeedSha.add(xdr::xdr_to_opaque(txBundle.txNum));
        Hash txSubSeed = txSubSeedSha.finish();

        auto res = txBundle.tx->parallelApply(
            entryMap, config, sorobanConfig, ledgerInfo, txBundle.resPayload,
            sorobanMetrics, txSubSeed, txBundle.meta);

        if (res.mSuccess)
        {
            // now apply the entry changes to entryMap
            for (auto const& entry : res.mModifiedEntryMap)
            {
                auto const& lk = entry.first;
                auto const& updatedLe = entry.second;

                auto it = entryMap.find(lk);
                releaseAssertOrThrow(it != entryMap.end());

                auto opType = txBundle.tx->getRawOperations().at(0).body.type();

                auto footprintIt = stageTtlFootprint.find(lk);

                if (opType != RESTORE_FOOTPRINT && lk.type() == TTL &&
                    it->second.mLedgerEntry && updatedLe && footprintIt != stageTtlFootprint.end() &&
                    footprintIt->second/*isReadOnly*/)
                {
                    // TODO: Can the ttl be unchanged here?
                    releaseAssertOrThrow(
                        updatedLe->data.ttl().liveUntilLedgerSeq >
                        it->second.mLedgerEntry->data.ttl().liveUntilLedgerSeq);

                    uint32_t delta =
                        updatedLe->data.ttl().liveUntilLedgerSeq -
                        it->second.mLedgerEntry->data.ttl().liveUntilLedgerSeq;

                    auto ttlIt = readOnlyTtlExtensions.find(lk);
                    if (ttlIt != readOnlyTtlExtensions.end())
                    {
                        // We will cap the ttl bump to the max network setting
                        // later when updating the ledger entry
                        ttlIt->second = UINT32_MAX - ttlIt->second < delta
                                            ? UINT32_MAX
                                            : ttlIt->second + delta;
                    }
                    else
                    {
                        readOnlyTtlExtensions.emplace(lk, delta);
                    }
                }
                else
                {
                    // readOnlyTtlExtensions should only be used for keys that
                    // never appear in a RW set in this thread
                    releaseAssertOrThrow(readOnlyTtlExtensions.count(lk) == 0);

                    // A entry deletion will be marked by a nullopt le.
                    // Set the dirty bit so it'll be written to ltx later.
                    it->second = {updatedLe, true};
                }
            }
        }
        else
        {
            releaseAssertOrThrow(
                txBundle.resPayload->getResultCode() == txFAILED ||
                txBundle.resPayload->getResultCode() == txINTERNAL_ERROR);
        }
        txBundle.mDelta = res.mDelta;
    }

    return readOnlyTtlExtensions;
}

ParallelLedgerInfo
getParallelLedgerInfo(Application& app, AbstractLedgerTxn& ltx)
{
    auto const& lh = ltx.loadHeader().current();
    return {lh.ledgerVersion, lh.ledgerSeq, lh.baseReserve,
            lh.scpValue.closeTime, app.getNetworkID()};
}

void
LedgerManagerImpl::applySorobanStage(Application& app, AbstractLedgerTxn& ltx,
                                     ApplyStage const& stage,
                                     Hash const& sorobanBasePrngSeed)
{
    for (auto const& thread : stage)
    {
        for (auto const& txBundle : thread)
        {
            txBundle.tx->preParallelApply(app, ltx, txBundle.meta,
                                          txBundle.resPayload, true);
        }
    }

    auto const& config = app.getConfig();
    auto const& sorobanConfig =
        app.getLedgerManager().getSorobanNetworkConfig();
    auto& sorobanMetrics = app.getLedgerManager().getSorobanMetrics();

    auto stageFootprint = getStageTtlFootprint(stage);

    std::vector<ThreadEntryMap> entryMapsByThread;
    for (auto const& thread : stage)
    {
        entryMapsByThread.emplace_back(collectEntries(ltx, thread));
    }

    std::vector<std::future<UnorderedMap<LedgerKey, uint32_t>>> roTtlDeltas;

    auto ledgerInfo = getParallelLedgerInfo(app, ltx);
    for (size_t i = 0; i < stage.size(); ++i)
    {
        auto& entryMapByThread = entryMapsByThread.at(i);

        auto const& thread = stage.at(i);

        // TODO: We need unique prng seeds per tx instead of
        // sorobanBasePrngSeed!!!
        // TODO: entryMap should be moved in.
        // TODO: Use thread pool
        roTtlDeltas.emplace_back(std::async(
            &LedgerManagerImpl::applyThread, this, std::ref(entryMapByThread),
            std::ref(thread), std::ref(stageFootprint), config, sorobanConfig, std::ref(ledgerInfo),
            sorobanBasePrngSeed, std::ref(sorobanMetrics)));
    }

    UnorderedMap<LedgerKey, uint32_t> cumulativeRoTtlDeltas;
    for (auto& roTtlDeltaFuture : roTtlDeltas)
    {
        auto roDeltas = roTtlDeltaFuture.get();
        for (auto const& delta : roDeltas)
        {
            auto it = cumulativeRoTtlDeltas.emplace(delta.first, delta.second);
            if (!it.second)
            {
                it.first->second = UINT32_MAX - it.first->second < delta.second
                                       ? UINT32_MAX
                                       : it.first->second + delta.second;
            }
        }
    }

    LedgerTxn ltxInner(ltx);
    for (auto const& thread : stage)
    {
        for (auto const& txBundle : thread)
        {
            // First check the invariants
            if (txBundle.resPayload->isSuccess())
            {
                try
                {
                    // Soroban transactions don't have access to the ledger
                    // header, so they can't modify it. Pass in the current
                    // header as both current and previous.
                    txBundle.mDelta->header.current =
                        ltxInner.loadHeader().current();
                    txBundle.mDelta->header.previous =
                        ltxInner.loadHeader().current();
                    app.getInvariantManager().checkOnOperationApply(
                        txBundle.tx->getRawOperations().at(0),
                        txBundle.resPayload->getOpResultAt(0),
                        *txBundle.mDelta);
                }
                catch (InvariantDoesNotHold& e)
                {
                    printErrorAndAbort(
                        "Invariant failure while applying operations: ",
                        e.what());
                }
            }

            txBundle.tx->processPostApply(mApp, ltxInner, txBundle.meta,
                                          txBundle.resPayload);

            // We only increase the internal-error metric count if the
            // ledger is a newer version.
            if (txBundle.resPayload->getResultCode() == txINTERNAL_ERROR &&
                ledgerInfo.getLedgerVersion() >=
                    config.LEDGER_PROTOCOL_MIN_VERSION_INTERNAL_ERROR_REPORT)
            {
                auto& internalErrorCounter = app.getMetrics().NewCounter(
                    {"ledger", "transaction", "internal-error"});
                internalErrorCounter.inc();
            }
        }
    }
    // TODO: Look into adding invariants checking for conflicting writes between
    // clusters
    for (auto const& threadEntryMap : entryMapsByThread)
    {
        for (auto const& entry : threadEntryMap)
        {
            // Only update if dirty bit is set
            if (!entry.second.isDirty)
            {
                continue;
            }

            if (entry.second.mLedgerEntry)
            {
                auto const& updatedEntry = *entry.second.mLedgerEntry;
                auto ltxe = ltxInner.load(entry.first);
                if (ltxe)
                {
                    if (ltxe.current().data.type() == TTL)
                    {
                        auto currLiveUntil =
                            ltxe.current().data.ttl().liveUntilLedgerSeq;
                        releaseAssertOrThrow(
                            updatedEntry.data.ttl().liveUntilLedgerSeq >=
                            currLiveUntil);
                    }
                    ltxe.current() = updatedEntry;
                }
                else
                {
                    ltxInner.create(updatedEntry);
                }
            }
            else
            {
                auto ltxe = ltxInner.load(entry.first);
                if (ltxe)
                {
                    ltxInner.erase(entry.first);
                }
            }
        }
    }

    // Do RO ttl extensions without a RW tx in the same
    // stage here
    for (auto const& kvp : cumulativeRoTtlDeltas)
    {
        auto ltxe = ltxInner.load(kvp.first);
        // The entry has to exist because this key doesn't exist in any RW set.
        releaseAssertOrThrow(ltxe);

        auto currentTTL = ltxe.current().data.ttl().liveUntilLedgerSeq;
        auto cumulativeTTL = UINT32_MAX - currentTTL < kvp.second
                                 ? UINT32_MAX
                                 : currentTTL + kvp.second;

        auto maxEntryTTL = app.getLedgerManager()
                               .getSorobanNetworkConfig()
                               .stateArchivalSettings()
                               .maxEntryTTL;
        ltxe.current().data.ttl().liveUntilLedgerSeq =
            std::min(maxEntryTTL - 1, cumulativeTTL);
    }
    ltxInner.commit();
}

void
LedgerManagerImpl::applySorobanStages(Application& app, AbstractLedgerTxn& ltx,
                                      std::vector<ApplyStage> const& stages,
                                      Hash const& sorobanBasePrngSeed)
{
    for (auto const& stage : stages)
    {
        applySorobanStage(app, ltx, stage, sorobanBasePrngSeed);
    }
}

TransactionResultSet
LedgerManagerImpl::applyTransactions(
    ApplicableTxSetFrame const& txSet,
    std::vector<MutableTxResultPtr> const& mutableTxResults,
    AbstractLedgerTxn& ltx,
    std::unique_ptr<LedgerCloseMetaFrame> const& ledgerCloseMeta)
{
    ZoneNamedN(txsZone, "applyTransactions", true);
    size_t numTxs = txSet.sizeTxTotal();
    size_t numOps = txSet.sizeOpTotal();
    releaseAssert(numTxs == mutableTxResults.size());
    int index = 0;

    // Record counts
    if (numTxs > 0)
    {
        mTransactionCount.Update(static_cast<int64_t>(numTxs));
        TracyPlot("ledger.transaction.count", static_cast<int64_t>(numTxs));

        mOperationCount.Update(static_cast<int64_t>(numOps));
        TracyPlot("ledger.operation.count", static_cast<int64_t>(numOps));
        CLOG_INFO(Tx, "applying ledger {} ({})",
                  ltx.loadHeader().current().ledgerSeq, txSet.summary());
    }
    TransactionResultSet txResultSet;
    txResultSet.results.reserve(numTxs);

    prefetchTransactionData(txSet);
    auto phases = txSet.getPhasesInApplyOrder();
    Hash sorobanBasePrngSeed = txSet.getContentsHash();
    uint64_t txNum{0};
    uint64_t txSucceeded{0};
    uint64_t txFailed{0};
    uint64_t sorobanTxSucceeded{0};
    uint64_t sorobanTxFailed{0};
    size_t resultIndex = 0;
    for (auto const& phase : phases)
    {
        if (phase.isParallel())
        {
            auto txSetStages = phase.getParallelStages();

            std::vector<ApplyStage> applyStages;
            applyStages.resize(txSetStages.size());

            for (size_t i = 0; i < txSetStages.size(); ++i)
            {
                auto const& stage = txSetStages[i];
                auto& applyStage = applyStages[i];
                applyStage.resize(stage.size());

                for (size_t j = 0; j < stage.size(); ++j)
                {
                    auto const& thread = stage[j];
                    auto& applyThread = applyStage[j];

                    for (auto const& tx : thread)
                    {
                        //std::cout << xdrToCerealString(tx->getEnvelope(), "fsd") << std::endl;
                        /* std::cout << "THREAD SIZE " << thread.size()
                                  << "  stage " << i << " thread " << j
                                  << std::endl
                                  << std::endl; */
                        TransactionMetaFrame tm(
                            ltx.loadHeader().current().ledgerVersion);
                        auto num = txNum++;
                        auto mutableTxResult =
                            mutableTxResults.at(num);
                        applyThread.emplace_back(tx, mutableTxResult, tm, num);
                    }
                }
            }

            applySorobanStages(mApp, ltx, applyStages, sorobanBasePrngSeed);

            for (auto const& stage : applyStages)
            {
                for (auto const& thread : stage)
                {
                    for (auto const& txBundle : thread)
                    {
                        // TODO: The following code can probably be
                        // de-duplicated with the not-parallel apply path
                        TransactionResultPair results;
                        results.transactionHash =
                            txBundle.tx->getContentsHash();
                        results.result = txBundle.resPayload->getResult();
                        if (results.result.result.code() ==
                            TransactionResultCode::txSUCCESS)
                        {
                            if (txBundle.tx->isSoroban())
                            {
                                ++sorobanTxSucceeded;
                            }
                            ++txSucceeded;
                        }
                        else
                        {
                            if (txBundle.tx->isSoroban())
                            {
                                ++sorobanTxFailed;
                            }
                            ++txFailed;
                        }

                        txResultSet.results.emplace_back(results);

#ifdef BUILD_TESTS
                        mLastLedgerTxMeta.push_back(txBundle.meta);
#endif

                        if (ledgerCloseMeta)
                        {
                            ledgerCloseMeta->setTxProcessingMetaAndResultPair(
                                txBundle.meta.getXDR(), std::move(results),
                                txBundle.txNum);
                        }
                        if (mApp.getConfig().MODE_STORES_HISTORY_MISC)
                        {
                            auto ledgerSeq =
                                ltx.loadHeader().current().ledgerSeq;
                            storeTransaction(mApp.getDatabase(), ledgerSeq,
                                             txBundle.tx,
                                             txBundle.meta.getXDR(),
                                             txResultSet, mApp.getConfig());
                        }
                    }
                }
            }
        }
        else
        {
            for (auto const& tx : phase)
            {
                ZoneNamedN(txZone, "applyTransaction", true);
                auto mutableTxResult = mutableTxResults.at(resultIndex++);

                auto txTime = mTransactionApply.TimeScope();
                TransactionMetaFrame tm(
                    ltx.loadHeader().current().ledgerVersion);
                CLOG_DEBUG(Tx, " tx#{} = {} ops={} txseq={} (@ {})", index,
                           hexAbbrev(tx->getContentsHash()),
                           tx->getNumOperations(), tx->getSeqNum(),
                           mApp.getConfig().toShortString(tx->getSourceID()));

                Hash subSeed = sorobanBasePrngSeed;
                // If tx can use the seed, we need to compute a sub-seed for it.
                if (tx->isSoroban())
                {
                    SHA256 subSeedSha;
                    subSeedSha.add(sorobanBasePrngSeed);
                    subSeedSha.add(xdr::xdr_to_opaque(txNum));
                    subSeed = subSeedSha.finish();
                }
                ++txNum;

                tx->apply(mApp, ltx, tm, mutableTxResult, subSeed);
                tx->processPostApply(mApp, ltx, tm, mutableTxResult);
                TransactionResultPair results;
                results.transactionHash = tx->getContentsHash();
                results.result = mutableTxResult->getResult();
                if (results.result.result.code() ==
                    TransactionResultCode::txSUCCESS)
                {
                    if (tx->isSoroban())
                    {
                        ++sorobanTxSucceeded;
                    }
                    ++txSucceeded;
                }
                else
                {
                    if (tx->isSoroban())
                    {
                        ++sorobanTxFailed;
                    }
                    ++txFailed;
                }

                // First gather the TransactionResultPair into the TxResultSet
                // for hashing into the ledger header.
                txResultSet.results.emplace_back(results);
#ifdef BUILD_TESTS
                mLastLedgerTxMeta.push_back(tm);
#endif
                // Then potentially add that TRP and its associated
                // TransactionMeta into the associated slot of any
                // LedgerCloseMeta we're collecting.
                if (ledgerCloseMeta)
                {
                    ledgerCloseMeta->setTxProcessingMetaAndResultPair(
                        tm.getXDR(), std::move(results), index);
                }

                // Then finally store the results and meta into the txhistory
                // table. if we're running in a mode that has one.
                //
                // Note to future: when we eliminate the txhistory for
                // archiving, the next step can be removed.
                //
                // Also note: for historical reasons the history tables number
                // txs counting from 1, not 0. We preserve this for the time
                // being in case anyone depends on it.
                ++index;
                if (mApp.getConfig().MODE_STORES_HISTORY_MISC)
                {
                    auto ledgerSeq = ltx.loadHeader().current().ledgerSeq;
                    storeTransaction(mApp.getDatabase(), ledgerSeq, tx,
                                     tm.getXDR(), txResultSet,
                                     mApp.getConfig());
                }
            }
        }
    }

    mTransactionApplySucceeded.inc(txSucceeded);
    mTransactionApplyFailed.inc(txFailed);
    mSorobanTransactionApplySucceeded.inc(sorobanTxSucceeded);
    mSorobanTransactionApplyFailed.inc(sorobanTxFailed);
    logTxApplyMetrics(ltx, numTxs, numOps);
    return txResultSet;
}

void
LedgerManagerImpl::logTxApplyMetrics(AbstractLedgerTxn& ltx, size_t numTxs,
                                     size_t numOps)
{
    auto ledgerSeq = ltx.loadHeader().current().ledgerSeq;
    auto hitRate = mApp.getLedgerTxnRoot().getPrefetchHitRate() * 100;

    CLOG_DEBUG(Ledger, "Ledger: {} txs: {}, ops: {}, prefetch hit rate (%): {}",
               ledgerSeq, numTxs, numOps, hitRate);

    // We lose a bit of precision here, as medida only accepts int64_t
    mPrefetchHitRate.Update(std::llround(hitRate));
    TracyPlot("ledger.prefetch.hit-rate", hitRate);
}

void
LedgerManagerImpl::storeCurrentLedger(LedgerHeader const& header,
                                      bool storeHeader)
{
    ZoneScoped;

    Hash hash = xdrSha256(header);
    releaseAssert(!isZero(hash));
    mApp.getPersistentState().setState(PersistentState::kLastClosedLedger,
                                       binToHex(hash));

    BucketList bl;
    if (mApp.getConfig().MODE_ENABLES_BUCKETLIST)
    {
        bl = mApp.getBucketManager().getBucketList();
    }
    // Store the current HAS in the database; this is really just to
    // checkpoint the bucketlist so we can survive a restart and re-attach
    // to the buckets.
    HistoryArchiveState has(header.ledgerSeq, bl,
                            mApp.getConfig().NETWORK_PASSPHRASE);

    mApp.getPersistentState().setState(PersistentState::kHistoryArchiveState,
                                       has.toString());

    if (mApp.getConfig().MODE_STORES_HISTORY_LEDGERHEADERS && storeHeader)
    {
        LedgerHeaderUtils::storeInDatabase(mApp.getDatabase(), header);
    }
}

// NB: This is a separate method so a testing subclass can override it.
void
LedgerManagerImpl::transferLedgerEntriesToBucketList(
    AbstractLedgerTxn& ltx,
    std::unique_ptr<LedgerCloseMetaFrame> const& ledgerCloseMeta,
    LedgerHeader lh, uint32_t initialLedgerVers)
{
    ZoneScoped;
    std::vector<LedgerEntry> initEntries, liveEntries;
    std::vector<LedgerKey> deadEntries;
    auto blEnabled = mApp.getConfig().MODE_ENABLES_BUCKETLIST;

    // Since snapshots are stored in a LedgerEntry, need to snapshot before
    // sealing the ledger with ltx.getAllEntries
    //
    // Any V20 features must be behind initialLedgerVers check, see comment
    // in LedgerManagerImpl::ledgerClosed
    if (blEnabled &&
        protocolVersionStartsFrom(initialLedgerVers, SOROBAN_PROTOCOL_VERSION))
    {
        {
            auto keys = ltx.getAllTTLKeysWithoutSealing();
            LedgerTxn ltxEvictions(ltx);

            if (mApp.getConfig().isUsingBackgroundEviction())
            {
                mApp.getBucketManager().resolveBackgroundEvictionScan(
                    ltxEvictions, lh.ledgerSeq, keys);
            }
            else
            {
                mApp.getBucketManager().scanForEvictionLegacy(ltxEvictions,
                                                              lh.ledgerSeq);
            }

            if (ledgerCloseMeta)
            {
                ledgerCloseMeta->populateEvictedEntries(
                    ltxEvictions.getChanges());
            }
            ltxEvictions.commit();
        }

        getSorobanNetworkConfigInternal().maybeSnapshotBucketListSize(
            lh.ledgerSeq, ltx, mApp);
    }

    ltx.getAllEntries(initEntries, liveEntries, deadEntries);
    if (blEnabled)
    {
        mApp.getBucketManager().addBatch(mApp, lh, initEntries, liveEntries,
                                         deadEntries);
    }
}

void
LedgerManagerImpl::ledgerClosed(
    AbstractLedgerTxn& ltx,
    std::unique_ptr<LedgerCloseMetaFrame> const& ledgerCloseMeta,
    uint32_t initialLedgerVers)
{
    ZoneScoped;
    auto ledgerSeq = ltx.loadHeader().current().ledgerSeq;
    auto currLedgerVers = ltx.loadHeader().current().ledgerVersion;
    CLOG_TRACE(Ledger,
               "sealing ledger {} with version {}, sending to bucket list",
               ledgerSeq, currLedgerVers);

    // There is a subtle bug in the upgrade path that wasn't noticed until
    // protocol 20. For a ledger that upgrades from protocol vN to vN+1,
    // there are two different assumptions in different parts of the
    // ledger-close path:
    //   - In closeLedger we mostly treat the ledger as being on vN, eg.
    //   during
    //     tx apply and LCM construction.
    //   - In the final stage, when we call ledgerClosed, we pass vN+1
    //   because
    //     the upgrade completed and modified the ltx header, and we fish
    //     the protocol out of the ltx header
    // Before LedgerCloseMetaV1, this inconsistency was mostly harmless
    // since LedgerCloseMeta was not modified after the LTX header was
    // modified. However, starting with protocol 20, LedgerCloseMeta is
    // modified after updating the ltx header when populating BucketList
    // related meta. This means that this function will attempt to call
    // LedgerCloseMetaV1 functions, but ledgerCloseMeta is actually a
    // LedgerCloseMetaV0 because it was constructed with the previous
    // protocol version prior to the upgrade. Due to this, we must check the
    // initial protocol version of ledger instead of the ledger version of
    // the current ltx header, which may have been modified via an upgrade.
    transferLedgerEntriesToBucketList(
        ltx, ledgerCloseMeta, ltx.loadHeader().current(), initialLedgerVers);
    if (ledgerCloseMeta &&
        protocolVersionStartsFrom(initialLedgerVers, SOROBAN_PROTOCOL_VERSION))
    {
        ledgerCloseMeta->setNetworkConfiguration(
            getSorobanNetworkConfig(),
            mApp.getConfig().EMIT_LEDGER_CLOSE_META_EXT_V1);
    }

    ltx.unsealHeader([this](LedgerHeader& lh) {
        mApp.getBucketManager().snapshotLedger(lh);
        storeCurrentLedger(lh, /* storeHeader */ true);
        advanceLedgerPointers(lh);
    });
}
}
