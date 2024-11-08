#include "simulation/ApplyLoad.h"

#include <numeric>

#include "bucket/test/BucketTestUtils.h"
#include "herder/Herder.h"
#include "ledger/LedgerManager.h"
#include "test/TxTests.h"
#include "transactions/MutableTransactionResult.h"
#include "transactions/TransactionBridge.h"
#include "transactions/TransactionUtils.h"

#include "herder/HerderImpl.h"

#include "medida/meter.h"
#include "medida/metrics_registry.h"

#include "util/XDRCereal.h"
#include "xdrpp/printer.h"
#include <crypto/SHA.h>

namespace stellar
{

ApplyLoad::ApplyLoad(Application& app, uint64_t ledgerMaxInstructions,
                     uint64_t ledgerMaxReadLedgerEntries,
                     uint64_t ledgerMaxReadBytes,
                     uint64_t ledgerMaxWriteLedgerEntries,
                     uint64_t ledgerMaxWriteBytes, uint64_t ledgerMaxTxCount,
                     uint64_t ledgerMaxTransactionsSizeBytes)
    : mTxGenerator(app)
    , mApp(app)
    , mNumAccounts(
          ledgerMaxTxCount * SOROBAN_TRANSACTION_QUEUE_SIZE_MULTIPLIER + 1)
    , mTxCountUtilization(
          mApp.getMetrics().NewHistogram({"soroban", "benchmark", "tx-count"}))
    , mInstructionUtilization(
          mApp.getMetrics().NewHistogram({"soroban", "benchmark", "ins"}))
    , mTxSizeUtilization(
          mApp.getMetrics().NewHistogram({"soroban", "benchmark", "tx-size"}))
    , mReadByteUtilization(
          mApp.getMetrics().NewHistogram({"soroban", "benchmark", "read-byte"}))
    , mWriteByteUtilization(mApp.getMetrics().NewHistogram(
          {"soroban", "benchmark", "write-byte"}))
    , mReadEntryUtilization(mApp.getMetrics().NewHistogram(
          {"soroban", "benchmark", "read-entry"}))
    , mWriteEntryUtilization(mApp.getMetrics().NewHistogram(
          {"soroban", "benchmark", "write-entry"}))
{

    auto rootTestAccount = TestAccount::createRoot(mApp);
    mRoot = std::make_shared<TestAccount>(rootTestAccount);
    releaseAssert(mTxGenerator.loadAccount(mRoot));

    mUpgradeConfig.maxContractSizeBytes = 65536;
    mUpgradeConfig.maxContractDataKeySizeBytes = 250;
    mUpgradeConfig.maxContractDataEntrySizeBytes = 65536;
    mUpgradeConfig.ledgerMaxInstructions = ledgerMaxInstructions;
    mUpgradeConfig.txMaxInstructions = 100000000;
    mUpgradeConfig.txMemoryLimit = 41943040;
    mUpgradeConfig.ledgerMaxReadLedgerEntries = ledgerMaxReadLedgerEntries;
    mUpgradeConfig.ledgerMaxReadBytes = ledgerMaxReadBytes;
    mUpgradeConfig.ledgerMaxWriteLedgerEntries = ledgerMaxWriteLedgerEntries;
    mUpgradeConfig.ledgerMaxWriteBytes = ledgerMaxWriteBytes;
    mUpgradeConfig.ledgerMaxTxCount = ledgerMaxTxCount;
    mUpgradeConfig.txMaxReadLedgerEntries = 100;
    mUpgradeConfig.txMaxReadBytes = 200000;
    mUpgradeConfig.txMaxWriteLedgerEntries = 50;
    mUpgradeConfig.txMaxWriteBytes = 66560;
    mUpgradeConfig.txMaxContractEventsSizeBytes = 8198;
    mUpgradeConfig.ledgerMaxTransactionsSizeBytes =
        ledgerMaxTransactionsSizeBytes;
    mUpgradeConfig.txMaxSizeBytes = 132'096;
    mUpgradeConfig.bucketListSizeWindowSampleSize = 30;
    mUpgradeConfig.evictionScanSize = 100000;
    mUpgradeConfig.startingEvictionScanLevel = 7;
    // Increase the default TTL and reduce the rent rate in order to avoid the
    // state archival and too high rent fees. The apply load test is generally
    // not concerned about the resource fees.
    mUpgradeConfig.minPersistentTTL = 1'000'000'000;
    mUpgradeConfig.minTemporaryTTL = 1'000'000'000;
    mUpgradeConfig.maxEntryTTL = 1'000'000'001;
    mUpgradeConfig.persistentRentRateDenominator = 1'000'000'000'000LL;
    mUpgradeConfig.tempRentRateDenominator = 1'000'000'000'000LL;

    setupAccountsAndUpgradeProtocol();

    setupUpgradeContract();

    upgradeSettings();

    setupLoadContract();
}

void
ApplyLoad::closeLedger(std::vector<TransactionFrameBasePtr> const& txs,
                       xdr::xvector<UpgradeType, 6> const& upgrades)
{
    auto txSet = makeTxSetFromTransactions(txs, mApp, 0, UINT64_MAX);

    auto sv =
        mApp.getHerder().makeStellarValue(txSet.first->getContentsHash(), 1,
                                          upgrades, mApp.getConfig().NODE_SEED);

    stellar::txtest::closeLedger(mApp, txs, /* strictOrder */ false, upgrades);
}

void
ApplyLoad::setupAccountsAndUpgradeProtocol()
{
    auto const& lm = mApp.getLedgerManager();
    // pass in false for initialAccounts so we fund new account with a lower
    // balance, allowing the creation of more accounts.
    std::vector<Operation> creationOps = mTxGenerator.createAccounts(
        0, mNumAccounts, lm.getLastClosedLedgerNum() + 1, false);

    for (size_t i = 0; i < creationOps.size(); i += MAX_OPS_PER_TX)
    {
        std::vector<TransactionFrameBaseConstPtr> txs;

        size_t end_id = std::min(i + MAX_OPS_PER_TX, creationOps.size());
        std::vector<Operation> currOps(creationOps.begin() + i,
                                       creationOps.begin() + end_id);
        txs.push_back(mTxGenerator.createTransactionFramePtr(
            mRoot, currOps, false, std::nullopt));

        closeLedger(txs);
    }
}

void
ApplyLoad::setupUpgradeContract()
{
    auto wasm = rust_bridge::get_write_bytes();
    xdr::opaque_vec<> wasmBytes;
    wasmBytes.assign(wasm.data.begin(), wasm.data.end());

    LedgerKey contractCodeLedgerKey;
    contractCodeLedgerKey.type(CONTRACT_CODE);
    contractCodeLedgerKey.contractCode().hash = sha256(wasmBytes);

    mUpgradeCodeKey = contractCodeLedgerKey;

    SorobanResources uploadResources;
    uploadResources.instructions = 2'000'000;
    uploadResources.readBytes = wasmBytes.size() + 500;
    uploadResources.writeBytes = wasmBytes.size() + 500;

    auto const& lm = mApp.getLedgerManager();
    auto uploadTx = mTxGenerator.createUploadWasmTransaction(
        lm.getLastClosedLedgerNum() + 1, 0, wasmBytes, contractCodeLedgerKey,
        std::nullopt, uploadResources);

    closeLedger({uploadTx.second});

    auto salt = sha256("upgrade contract salt preimage");

    auto createTx = mTxGenerator.createContractTransaction(
        lm.getLastClosedLedgerNum() + 1, 0, contractCodeLedgerKey,
        wasmBytes.size() + 160, salt, std::nullopt);
    closeLedger({createTx.second});

    mUpgradeInstanceKey =
        createTx.second->sorobanResources().footprint.readWrite.back();

    releaseAssert(mTxGenerator.getApplySorobanSuccess().count() == 2);
}

// To upgrade settings, just modify mUpgradeConfig and then call
// upgradeSettings()
void
ApplyLoad::upgradeSettings()
{
    auto const& lm = mApp.getLedgerManager();
    auto upgradeBytes =
        mTxGenerator.getConfigUpgradeSetFromLoadConfig(mUpgradeConfig);

    SorobanResources resources;
    resources.instructions = 1'250'000;
    resources.readBytes = 3'100;
    resources.writeBytes = 3'100;

    auto invokeTx = mTxGenerator.invokeSorobanCreateUpgradeTransaction(
        lm.getLastClosedLedgerNum() + 1, 0, upgradeBytes, mUpgradeCodeKey,
        mUpgradeInstanceKey, std::nullopt, resources);

    auto upgradeSetKey = mTxGenerator.getConfigUpgradeSetKey(
        mUpgradeConfig,
        mUpgradeInstanceKey.contractData().contract.contractId());

    auto upgrade = xdr::xvector<UpgradeType, 6>{};
    auto ledgerUpgrade = LedgerUpgrade{LEDGER_UPGRADE_CONFIG};
    ledgerUpgrade.newConfig() = upgradeSetKey;
    auto v = xdr::xdr_to_opaque(ledgerUpgrade);
    upgrade.push_back(UpgradeType{v.begin(), v.end()});

    closeLedger({invokeTx.second}, upgrade);
}

void
ApplyLoad::setupLoadContract()
{
    auto wasm = rust_bridge::get_test_wasm_loadgen();
    xdr::opaque_vec<> wasmBytes;
    wasmBytes.assign(wasm.data.begin(), wasm.data.end());

    LedgerKey contractCodeLedgerKey;
    contractCodeLedgerKey.type(CONTRACT_CODE);
    contractCodeLedgerKey.contractCode().hash = sha256(wasmBytes);

    mLoadCodeKey = contractCodeLedgerKey;

    int64_t currApplySorobanSuccess =
        mTxGenerator.getApplySorobanSuccess().count();

    auto const& lm = mApp.getLedgerManager();
    auto uploadTx = mTxGenerator.createUploadWasmTransaction(
        lm.getLastClosedLedgerNum() + 1, 0, wasmBytes, contractCodeLedgerKey,
        std::nullopt);

    closeLedger({uploadTx.second});

    auto salt = sha256("Load contract");

    auto createTx = mTxGenerator.createContractTransaction(
        lm.getLastClosedLedgerNum() + 1, 0, contractCodeLedgerKey,
        wasmBytes.size() + 160, salt, std::nullopt);
    closeLedger({createTx.second});

    releaseAssert(mTxGenerator.getApplySorobanSuccess().count() -
                      currApplySorobanSuccess ==
                  2);
    releaseAssert(mTxGenerator.getApplySorobanFailure().count() == 0);

    auto instanceKey =
        createTx.second->sorobanResources().footprint.readWrite.back();

    mLoadInstance.readOnlyKeys.emplace_back(mLoadCodeKey);
    mLoadInstance.readOnlyKeys.emplace_back(instanceKey);
    mLoadInstance.contractID = instanceKey.contractData().contract;
    mLoadInstance.contractEntriesSize =
        footprintSize(mApp, mLoadInstance.readOnlyKeys);

    auto lh = mApp.getLedgerManager().getLastClosedLedgerHeader().header;
    auto& bl = mApp.getBucketManager().getBucketList();
    auto const& cfg = mApp.getConfig();

    uint64_t currentKey = 0;

    // int const blLedgers = 1'000'000;
    // int const blWriteFrequency = 1000;
    // int const blLedgers = 1;
    // int const blWriteFrequency = 1000;
    // int const lastBatchEntries = 100;
    // int const lastBatchThreshold = 300;
    LedgerEntry baseLe;
    baseLe.data.type(CONTRACT_DATA);
    baseLe.data.contractData().contract = mLoadInstance.contractID;
    baseLe.data.contractData().key.type(SCV_U64);
    baseLe.data.contractData().key.u64() = 0;
    baseLe.data.contractData().durability = ContractDataDurability::PERSISTENT;
    baseLe.data.contractData().val.type(SCV_BYTES);
    mDataEntrySize = xdr::xdr_size(baseLe);
    // Add some padding to reach the configured LE size.
    if (mDataEntrySize <
        mApp.getConfig().APPLY_LOAD_DATA_ENTRY_SIZE_FOR_TESTING)
    {
        baseLe.data.contractData().val.bytes().resize(
            mApp.getConfig().APPLY_LOAD_DATA_ENTRY_SIZE_FOR_TESTING -
            mDataEntrySize);
        mDataEntrySize =
            mApp.getConfig().APPLY_LOAD_DATA_ENTRY_SIZE_FOR_TESTING;
    }
    else
    {
        CLOG_WARNING(Perf,
                     "Apply load generated entry size is larger than "
                     "APPLY_LOAD_DATA_ENTRY_SIZE_FOR_TESTING: {} > {}",
                     mApp.getConfig().APPLY_LOAD_DATA_ENTRY_SIZE_FOR_TESTING,
                     mDataEntrySize);
    }

    for (uint32_t i = 0; i < cfg.APPLY_LOAD_BL_SIMULATED_LEDGERS; ++i)
    {
        if (i % 1000 == 0)
        {
            CLOG_INFO(Bucket, "Generating BL ledger {}, levels thus far", i);
            for (uint32_t j = 0; j < BucketList::kNumLevels; ++j)
            {
                auto const& lev = bl.getLevel(j);
                auto currSz = BucketTestUtils::countEntries(lev.getCurr());
                auto snapSz = BucketTestUtils::countEntries(lev.getSnap());
                CLOG_INFO(Bucket, "Level {}: {} = {} + {}", j, currSz + snapSz,
                          currSz, snapSz);
            }
        }
        lh.ledgerSeq++;
        std::vector<LedgerEntry> initEntries;
        bool isLastBatch = i >= cfg.APPLY_LOAD_BL_SIMULATED_LEDGERS -
                                    cfg.APPLY_LOAD_BL_LAST_BATCH_LEDGERS;
        if (i % cfg.APPLY_LOAD_BL_WRITE_FREQUENCY == 0 || isLastBatch)
        {
            uint32_t entryCount = isLastBatch
                                      ? cfg.APPLY_LOAD_BL_LAST_BATCH_SIZE
                                      : cfg.APPLY_LOAD_BL_BATCH_SIZE;
            for (uint32_t j = 0; j < entryCount; j++)
            {
                LedgerEntry le = baseLe;
                le.lastModifiedLedgerSeq = lh.ledgerSeq;
                le.data.contractData().key.u64() = currentKey++;
                initEntries.push_back(le);

                LedgerEntry ttlEntry;
                ttlEntry.data.type(TTL);
                ttlEntry.lastModifiedLedgerSeq = lh.ledgerSeq;
                ttlEntry.data.ttl().keyHash = xdrSha256(LedgerEntryKey(le));
                ttlEntry.data.ttl().liveUntilLedgerSeq = 1'000'000'000;
                initEntries.push_back(ttlEntry);
            }
        }
        bl.addBatch(mApp, lh.ledgerSeq, lh.ledgerVersion, initEntries, {}, {});
    }    
    lh.ledgerSeq++;
    mDataEntryCount = currentKey;
    CLOG_INFO(Bucket, "Final generated bucket list levels");
    for (uint32_t i = 0; i < BucketList::kNumLevels; ++i)
    {
        auto const& lev = bl.getLevel(i);
        auto currSz = BucketTestUtils::countEntries(lev.getCurr());
        auto snapSz = BucketTestUtils::countEntries(lev.getSnap());
        CLOG_INFO(Bucket, "Level {}: {} = {} + {}", i, currSz + snapSz, currSz,
                  snapSz);
    }
    mApp.getBucketManager().snapshotLedger(lh);
    {
        LedgerTxn ltx(mApp.getLedgerTxnRoot());
        ltx.loadHeader().current() = lh;
        mApp.getLedgerManager().manuallyAdvanceLedgerHeader(
            ltx.loadHeader().current());
        ltx.commit();
    }
    mApp.getLedgerManager().storeCurrentLedgerForTest(lh);
    mApp.getHerder().forceSCPStateIntoSyncWithLastClosedLedger();
    closeLedger({}, {});
}

void
ApplyLoad::benchmark()
{
    auto& lm = mApp.getLedgerManager();
    std::vector<TransactionFrameBasePtr> txs;

    auto resources = multiplyByDouble(
        lm.maxLedgerResources(true), SOROBAN_TRANSACTION_QUEUE_SIZE_MULTIPLIER);

    // Save a snapshot so we can calculate what % we used up.
    auto const resourcesSnapshot = resources;

    auto const& accounts = mTxGenerator.getAccounts();
    std::vector<uint64_t> shuffledAccounts(accounts.size());
    std::iota(shuffledAccounts.begin(), shuffledAccounts.end(), 0);
    stellar::shuffle(std::begin(shuffledAccounts), std::end(shuffledAccounts),
                     gRandomEngine);

    bool limitHit = false;
    for (auto accountIndex : shuffledAccounts)
    {
        auto it = accounts.find(accountIndex);
        releaseAssert(it != accounts.end());

        auto tx = mTxGenerator.invokeSorobanLoadTransactionV2(
            lm.getLastClosedLedgerNum() + 1, it->first, mLoadInstance,
            mDataEntryCount, mDataEntrySize, 1'000'000);

        {
            LedgerTxn ltx(mApp.getLedgerTxnRoot());
            auto res = tx.second->checkValid(mApp.getAppConnector(), ltx, 0, 0,
                                             UINT64_MAX);
            releaseAssert((res && res->isSuccess()));
        }

        if (!anyGreater(tx.second->getResources(false), resources))
        {
            resources -= tx.second->getResources(false);
        }
        else
        {
            for (size_t i = 0; i < resources.size(); ++i)
            {
                auto type = static_cast<Resource::Type>(i);
                if (tx.second->getResources(false).getVal(type) >
                    resources.getVal(type))
                {
                    CLOG_INFO(Perf, "Ledger {} limit hit during tx generation",
                              Resource::getStringFromType(type));
                    limitHit = true;
                }
            }

            break;
        }

        txs.emplace_back(tx.second);
    }

    // If this assert fails, it most likely means that we ran out of
    // accounts, which should not happen.
    releaseAssert(limitHit);

    mTxCountUtilization.Update(
        (1.0 - (resources.getVal(Resource::Type::OPERATIONS) * 1.0 /
                resourcesSnapshot.getVal(Resource::Type::OPERATIONS))) *
        100000.0);
    mInstructionUtilization.Update(
        (1.0 - (resources.getVal(Resource::Type::INSTRUCTIONS) * 1.0 /
                resourcesSnapshot.getVal(Resource::Type::INSTRUCTIONS))) *
        100000.0);
    mTxSizeUtilization.Update(
        (1.0 - (resources.getVal(Resource::Type::TX_BYTE_SIZE) * 1.0 /
                resourcesSnapshot.getVal(Resource::Type::TX_BYTE_SIZE))) *
        100000.0);
    mReadByteUtilization.Update(
        (1.0 - (resources.getVal(Resource::Type::READ_BYTES) * 1.0 /
                resourcesSnapshot.getVal(Resource::Type::READ_BYTES))) *
        100000.0);
    mWriteByteUtilization.Update(
        (1.0 - (resources.getVal(Resource::Type::WRITE_BYTES) * 1.0 /
                resourcesSnapshot.getVal(Resource::Type::WRITE_BYTES))) *
        100000.0);
    mReadEntryUtilization.Update(
        (1.0 -
         (resources.getVal(Resource::Type::READ_LEDGER_ENTRIES) * 1.0 /
          resourcesSnapshot.getVal(Resource::Type::READ_LEDGER_ENTRIES))) *
        100000.0);
    mWriteEntryUtilization.Update(
        (1.0 -
         (resources.getVal(Resource::Type::WRITE_LEDGER_ENTRIES) * 1.0 /
          resourcesSnapshot.getVal(Resource::Type::WRITE_LEDGER_ENTRIES))) *
        100000.0);

    closeLedger(txs);
}

double
ApplyLoad::successRate()
{
    return mTxGenerator.getApplySorobanSuccess().count() * 1.0 /
           (mTxGenerator.getApplySorobanSuccess().count() +
            mTxGenerator.getApplySorobanFailure().count());
}

medida::Histogram const&
ApplyLoad::getTxCountUtilization()
{
    return mTxCountUtilization;
}
medida::Histogram const&
ApplyLoad::getInstructionUtilization()
{
    return mInstructionUtilization;
}
medida::Histogram const&
ApplyLoad::getTxSizeUtilization()
{
    return mTxSizeUtilization;
}
medida::Histogram const&
ApplyLoad::getReadByteUtilization()
{
    return mReadByteUtilization;
}
medida::Histogram const&
ApplyLoad::getWriteByteUtilization()
{
    return mWriteByteUtilization;
}
medida::Histogram const&
ApplyLoad::getReadEntryUtilization()
{
    return mReadEntryUtilization;
}
medida::Histogram const&
ApplyLoad::getWriteEntryUtilization()
{
    return mWriteEntryUtilization;
}

}
