// Copyright 2023 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "SorobanTxTestUtils.h"
#include "lib/catch.hpp"
#include "test/TestAccount.h"
#include "test/TestUtils.h"
#include "test/TxTests.h"
#include "transactions/InvokeHostFunctionOpFrame.h"
#include "transactions/TransactionUtils.h"

namespace stellar
{

namespace txtest
{
SCVal
makeWasmRefScContractCode(Hash const& hash)
{
    SCVal val(SCValType::SCV_CONTRACT_INSTANCE);
    val.instance().executable.type(
        ContractExecutableType::CONTRACT_EXECUTABLE_WASM);
    val.instance().executable.wasm_hash() = hash;
    return val;
}

SCAddress
makeContractAddress(Hash const& hash)
{
    SCAddress addr(SC_ADDRESS_TYPE_CONTRACT);
    addr.contractId() = hash;
    return addr;
}

SCVal
makeContractAddressSCVal(SCAddress const& address)
{
    SCVal val(SCValType::SCV_ADDRESS);
    val.address() = address;
    return val;
}

SCAddress
makeAccountAddress(AccountID const& accountID)
{
    SCAddress addr(SC_ADDRESS_TYPE_ACCOUNT);
    addr.accountId() = accountID;
    return addr;
}

SCVal
makeI32(int32_t i32)
{
    SCVal val(SCV_I32);
    val.i32() = i32;
    return val;
}

SCVal
makeI128(uint64_t u64)
{
    Int128Parts p;
    p.hi = 0;
    p.lo = u64;

    SCVal val(SCV_I128);
    val.i128() = p;
    return val;
}

SCSymbol
makeSymbol(std::string const& str)
{
    SCSymbol val;
    val.assign(str.begin(), str.end());
    return val;
}

SCVal
makeU32(uint32_t u32)
{
    SCVal val(SCV_U32);
    val.u32() = u32;
    return val;
}

SCVal
makeBytes(SCBytes bytes)
{
    SCVal val(SCV_BYTES);
    val.bytes() = bytes;
    return val;
}

SorobanAuthorizationEntry
getInvocationAuth(InvokeContractArgs const& args)
{
    SorobanAuthorizedInvocation ai;
    ai.function.type(SOROBAN_AUTHORIZED_FUNCTION_TYPE_CONTRACT_FN);
    ai.function.contractFn() = args;

    SorobanAuthorizationEntry a;
    a.credentials.type(SOROBAN_CREDENTIALS_SOURCE_ACCOUNT);
    a.rootInvocation = ai;

    return a;
}

int64_t
getContractBalance(Application& app, SCAddress const& contractID,
                   SCVal const& accountVal)
{
    LedgerKey balanceKey(CONTRACT_DATA);
    balanceKey.contractData().contract = contractID;

    SCVec balanceVec = {makeSymbolSCVal("Balance"), accountVal};
    SCVal balanceVal(SCValType::SCV_VEC);
    balanceVal.vec().activate() = balanceVec;
    balanceKey.contractData().key = balanceVal;
    balanceKey.contractData().durability = ContractDataDurability::PERSISTENT;

    LedgerTxn ltx(app.getLedgerTxnRoot());
    auto ltxe = ltx.load(balanceKey);
    if (!ltxe)
    {
        return 0;
    }
    auto const& balance =
        ltxe.current().data.contractData().val.map()->at(0).val.i128();

    // Contract balances can be greater than INT64_MAX, but for these tests
    // we'll assume balances < INT64_MAX.
    REQUIRE(balance.hi == 0);
    return balance.lo;
}

void
submitTx(Application& app, Operation const& op,
         SorobanResources const& resources, uint32_t inclusionFee,
         uint32_t resourceFee)
{
    // submit operation
    auto root = TestAccount::createRoot(app);
    auto tx =
        sorobanTransactionFrameFromOps(app.getNetworkID(), root, {op}, {},
                                       resources, inclusionFee, resourceFee);
    LedgerTxn ltx(app.getLedgerTxnRoot());
    TransactionMetaFrame txm(ltx.loadHeader().current().ledgerVersion);
    REQUIRE(tx->checkValid(app, ltx, 0, 0, 0));
    REQUIRE(tx->apply(app, ltx, txm));
    ltx.commit();
}

void
submitTxToUploadWasm(Application& app, Operation const& op,
                     SorobanResources const& resources,
                     Hash const& expectedWasmHash,
                     xdr::opaque_vec<> const& expectedWasm,
                     uint32_t inclusionFee, uint32_t resourceFee)
{
    submitTx(app, op, resources, inclusionFee, resourceFee);

    // verify contract code is correct
    LedgerTxn ltx2(app.getLedgerTxnRoot());
    auto ltxe = loadContractCode(ltx2, expectedWasmHash);
    REQUIRE(ltxe);
    REQUIRE(ltxe.current().data.contractCode().code == expectedWasm);
}

void
submitTxToCreateContract(Application& app, Operation const& op,
                         SorobanResources const& resources,
                         Hash const& contractID, SCVal const& executableKey,
                         Hash const& expectedWasmHash, uint32_t inclusionFee,
                         uint32_t resourceFee)
{
    // submit operation
    auto root = TestAccount::createRoot(app);
    auto tx =
        sorobanTransactionFrameFromOps(app.getNetworkID(), root, {op}, {},
                                       resources, inclusionFee, resourceFee);
    LedgerTxn ltx(app.getLedgerTxnRoot());
    TransactionMetaFrame txm(ltx.loadHeader().current().ledgerVersion);
    REQUIRE(tx->checkValid(app, ltx, 0, 0, 0));
    REQUIRE(tx->apply(app, ltx, txm));
    ltx.commit();

    // verify contract code reference is correct
    LedgerTxn ltx2(app.getLedgerTxnRoot());
    SCAddress contract = makeContractAddress(contractID);
    auto ltxe = loadContractData(ltx2, contract, executableKey,
                                 CONTRACT_INSTANCE_CONTRACT_DURABILITY);
    REQUIRE(ltxe);

    auto const& cd = ltxe.current().data.contractData();
    REQUIRE(cd.durability == CONTRACT_INSTANCE_CONTRACT_DURABILITY);
    REQUIRE(cd.val == makeWasmRefScContractCode(expectedWasmHash));
}

int64_t
ContractInvocationTest::computeFeePerIncrement(int64_t resourceVal,
                                               int64_t feeRate,
                                               int64_t increment)
{
    // ceiling division for (resourceVal * feeRate) / increment
    int64_t num = (resourceVal * feeRate);
    return (num + increment - 1) / increment;
};

void
ContractInvocationTest::invokeArchivalOp(TransactionFrameBasePtr tx,
                                         int64_t expectedRefundableFeeCharged,
                                         bool expectSuccess)
{
    if (!expectSuccess)
    {
        REQUIRE(!isTxValid(tx));
        return;
    }

    int64_t initBalance = getRoot().getBalance();
    txCheckValid(tx);

    // Initially we store in result the charge for resources plus
    // minimum inclusion  fee bid (currently equivalent to the network
    // `baseFee` of 100).
    int64_t baseCharged = (tx->getFullFee() - tx->getInclusionFee()) + 100;
    REQUIRE(tx->getResult().feeCharged == baseCharged);
    // Charge the fee.
    {
        LedgerTxn ltx(mApp->getLedgerTxnRoot());
        // Imitate surge pricing by charging at a higher rate than base
        // fee.
        tx->processFeeSeqNum(ltx, 300);
        ltx.commit();
    }
    // The resource and the base fee are charged, with additional
    // surge pricing fee.
    int64_t balanceAfterFeeCharged = getRoot().getBalance();
    auto actuallyCharged = baseCharged + /* surge pricing additional fee */ 200;
    REQUIRE(initBalance - balanceAfterFeeCharged == actuallyCharged);
    auto nonRefundableResourceFee = sorobanResourceFee(
        *mApp, tx->sorobanResources(), xdr::xdr_size(tx->getEnvelope()), 0);
    auto expectedChargedAfterRefund =
        nonRefundableResourceFee + expectedRefundableFeeCharged + 300;

    auto txm = invokeTx(tx, /*expectSuccess=*/true);
    auto changesAfter = txm->getChangesAfter();
    REQUIRE(changesAfter.size() == 2);
    REQUIRE(changesAfter[1].updated().data.account().balance -
                changesAfter[0].state().data.account().balance ==
            actuallyCharged - expectedChargedAfterRefund);

    // The account should receive a refund for unspent refundable fee.
    REQUIRE(getRoot().getBalance() - balanceAfterFeeCharged ==
            actuallyCharged - expectedChargedAfterRefund);
}

// Returns createOp, contractID pair
static std::pair<Operation, Hash>
createOpCommon(Application& app, SorobanResources& createResources,
               LedgerKey const& contractCodeLedgerKey, TestAccount& source,
               SCVal& scContractSourceRefKey)
{
    // Deploy the contract instance
    ContractIDPreimage idPreimage(CONTRACT_ID_PREIMAGE_FROM_ADDRESS);
    idPreimage.fromAddress().address.type(SC_ADDRESS_TYPE_ACCOUNT);
    idPreimage.fromAddress().address.accountId().ed25519() =
        source.getPublicKey().ed25519();
    idPreimage.fromAddress().salt = sha256("salt");
    HashIDPreimage fullPreImage;
    fullPreImage.type(ENVELOPE_TYPE_CONTRACT_ID);
    fullPreImage.contractID().contractIDPreimage = idPreimage;
    fullPreImage.contractID().networkID = app.getNetworkID();
    auto contractID = xdrSha256(fullPreImage);

    Operation createOp;
    createOp.body.type(INVOKE_HOST_FUNCTION);
    auto& createHF = createOp.body.invokeHostFunctionOp().hostFunction;
    createHF.type(HOST_FUNCTION_TYPE_CREATE_CONTRACT);
    auto& createContractArgs = createHF.createContract();
    createContractArgs.contractIDPreimage = idPreimage;
    createContractArgs.executable.type(CONTRACT_EXECUTABLE_WASM);
    createContractArgs.executable.wasm_hash() =
        contractCodeLedgerKey.contractCode().hash;

    SorobanAuthorizationEntry auth;
    auth.credentials.type(SOROBAN_CREDENTIALS_SOURCE_ACCOUNT);
    auth.rootInvocation.function.type(
        SOROBAN_AUTHORIZED_FUNCTION_TYPE_CREATE_CONTRACT_HOST_FN);
    auth.rootInvocation.function.createContractHostFn().contractIDPreimage =
        idPreimage;
    auth.rootInvocation.function.createContractHostFn().executable.type(
        CONTRACT_EXECUTABLE_WASM);
    auth.rootInvocation.function.createContractHostFn().executable.wasm_hash() =
        contractCodeLedgerKey.contractCode().hash;
    createOp.body.invokeHostFunctionOp().auth = {auth};

    LedgerKey contractSourceRefLedgerKey;
    contractSourceRefLedgerKey.type(CONTRACT_DATA);
    contractSourceRefLedgerKey.contractData().contract.type(
        SC_ADDRESS_TYPE_CONTRACT);
    contractSourceRefLedgerKey.contractData().contract.contractId() =
        contractID;
    contractSourceRefLedgerKey.contractData().key = scContractSourceRefKey;
    contractSourceRefLedgerKey.contractData().durability =
        CONTRACT_INSTANCE_CONTRACT_DURABILITY;

    createResources.footprint.readOnly = {contractCodeLedgerKey};
    createResources.footprint.readWrite = {contractSourceRefLedgerKey};
    return {createOp, contractID};
}

void
WasmContractInvocationTest::deployContractWithSourceAccountWithResources(
    SorobanResources uploadResources, SorobanResources createResources)
{
    // Upload contract code
    Operation uploadOp;
    uploadOp.body.type(INVOKE_HOST_FUNCTION);
    auto& uploadHF = uploadOp.body.invokeHostFunctionOp().hostFunction;
    uploadHF.type(HOST_FUNCTION_TYPE_UPLOAD_CONTRACT_WASM);
    uploadHF.wasm().assign(mWasm.data.begin(), mWasm.data.end());

    LedgerKey contractCodeLedgerKey;
    contractCodeLedgerKey.type(CONTRACT_CODE);
    contractCodeLedgerKey.contractCode().hash = sha256(uploadHF.wasm());
    uploadResources.footprint.readWrite = {contractCodeLedgerKey};
    auto uploadResourceFee = sorobanResourceFee(*mApp, uploadResources,
                                                1000 + mWasm.data.size(), 40) +
                             40'000;
    submitTxToUploadWasm(*mApp, uploadOp, uploadResources,
                         contractCodeLedgerKey.contractCode().hash,
                         uploadHF.wasm(), 1'000, uploadResourceFee);

    // Check TTLs for contract code
    auto const& ses = getNetworkCfg().stateArchivalSettings();
    auto expectedLiveUntilLedger = ses.minPersistentTTL + getLedgerSeq() - 1;
    checkTTL(contractCodeLedgerKey, expectedLiveUntilLedger);

    // Deploy the contract instance
    SCVal scContractSourceRefKey(SCValType::SCV_LEDGER_KEY_CONTRACT_INSTANCE);
    auto [createOp, contractID] =
        createOpCommon(*mApp, createResources, contractCodeLedgerKey, mRoot,
                       scContractSourceRefKey);

    auto createResourceFee =
        sorobanResourceFee(*mApp, createResources, 1000, 40) + 40'000;
    submitTxToCreateContract(
        *mApp, createOp, createResources, contractID, scContractSourceRefKey,
        contractCodeLedgerKey.contractCode().hash, 1000, createResourceFee);

    // Check TTLs for contract instance
    checkTTL(contractCodeLedgerKey, expectedLiveUntilLedger);

    // ContractSourceRefLedgerKey == readWrite[0]
    mContractKeys.emplace_back(createResources.footprint.readWrite[0]);
    mContractKeys.emplace_back(contractCodeLedgerKey);
    mContractID = mContractKeys[0].contractData().contract;
}

ContractInvocationTest::ContractInvocationTest(
    Config cfg, bool useTestLimits,
    std::function<void(SorobanNetworkConfig&)> cfgModifyFn)
    : mApp(createTestApplication(mClock, cfg))
    , mRoot(TestAccount::createRoot(*mApp))
    , mDummyAccount(mRoot.create("dummyAcc",
                                 mApp->getLedgerManager().getLastMinBalance(1)))
{
    if (useTestLimits)
    {
        overrideSorobanNetworkConfigForTest(*mApp);
    }

    modifySorobanNetworkConfig(*mApp, cfgModifyFn);
}

TestAccount&
ContractInvocationTest::getRoot()
{
    // TestAccount caches the next seqno in-memory, assuming all invoked TXs
    // succeed. This is not true for these tests, so we load the seqno from
    // disk to circumvent the cache.
    mRoot.loadSequenceNumber();
    return mRoot;
}

SorobanNetworkConfig const&
ContractInvocationTest::getNetworkCfg()
{
    return mApp->getLedgerManager().getSorobanNetworkConfig();
}

uint32_t
ContractInvocationTest::getLedgerSeq()
{
    LedgerTxn ltx(mApp->getLedgerTxnRoot());
    return ltx.loadHeader().current().ledgerSeq;
}

int64_t
ContractInvocationTest::getRentFeeForBytes(int64_t entrySize, uint32_t extendTo,
                                           bool isPersistent)
{
    auto const& cfg = getNetworkCfg();
    auto num = entrySize * cfg.feeWrite1KB() * extendTo;
    auto storageCoef =
        isPersistent ? cfg.stateArchivalSettings().persistentRentRateDenominator
                     : cfg.stateArchivalSettings().tempRentRateDenominator;

    auto denom = DATA_SIZE_1KB_INCREMENT * storageCoef;

    // Ceiling division
    return (num + denom - 1) / denom;
}

int64_t
ContractInvocationTest::getTTLEntryWriteFee()
{
    auto const& cfg = getNetworkCfg();
    LedgerEntry le;
    le.data.type(TTL);

    auto writeSize = xdr::xdr_size(le);
    auto writeFee = computeFeePerIncrement(writeSize, cfg.feeWrite1KB(),
                                           DATA_SIZE_1KB_INCREMENT);
    writeFee += cfg.feeWriteLedgerEntry();
    return writeFee;
}

int64_t
ContractInvocationTest::getRentFeeForExtension(
    xdr::xvector<LedgerKey> const& keys, uint32_t newLifetime)
{
    int64_t rentFee = 0;
    int64_t ttlBytes = 0;
    size_t numExtensions = 0;

    for (auto const& key : keys)
    {
        LedgerTxn ltx(mApp->getLedgerTxnRoot());
        auto ledgerSeq = ltx.getHeader().ledgerSeq;

        auto ttlKey = getTTLKey(key);
        auto ttlLtxe = ltx.loadWithoutRecord(ttlKey);
        releaseAssert(ttlLtxe);

        uint32_t extendTo = 0;

        TTLEntry const& ttlEntry = ttlLtxe.current().data.ttl();

        if (isLive(ttlLtxe.current(), ledgerSeq))
        {
            auto newLiveUntilLedger = ledgerSeq + newLifetime;
            if (ttlEntry.liveUntilLedgerSeq >= newLiveUntilLedger)
            {
                continue;
            }

            extendTo = newLiveUntilLedger - ttlEntry.liveUntilLedgerSeq;

            LedgerEntry le;
            le.data.type(TTL);
            ttlBytes += xdr::xdr_size(le);
            ++numExtensions;
        }
        else
        {
            // Non-live entries are skipped, pay no rent fee
            continue;
        }

        auto txle = ltx.loadWithoutRecord(key);
        releaseAssert(txle);

        auto entrySize = xdr::xdr_size(txle.current());

        rentFee +=
            getRentFeeForBytes(entrySize, extendTo, isPersistentEntry(key));
    }

    // Now charge for the TTL writes
    auto const& cfg = getNetworkCfg();
    rentFee += (cfg.feeWriteLedgerEntry() * numExtensions);
    rentFee += computeFeePerIncrement(ttlBytes, cfg.feeWrite1KB(),
                                      DATA_SIZE_1KB_INCREMENT);

    return rentFee;
}

int64_t
ContractInvocationTest::getTxSizeFees(TransactionFrameBasePtr tx)
{
    auto const& cfg = getNetworkCfg();
    int64_t txSize = static_cast<uint32>(xdr::xdr_size(tx->getEnvelope()));
    int64_t historicalFee =
        computeFeePerIncrement(txSize + TX_BASE_RESULT_SIZE,
                               cfg.mFeeHistorical1KB, DATA_SIZE_1KB_INCREMENT);

    int64_t bandwidthFee = computeFeePerIncrement(
        txSize, cfg.feeTransactionSize1KB(), DATA_SIZE_1KB_INCREMENT);

    return historicalFee + bandwidthFee;
}

int64_t
ContractInvocationTest::getComputeFee(SorobanResources const& resources)
{
    auto const& cfg = getNetworkCfg();
    return computeFeePerIncrement(resources.instructions,
                                  cfg.feeRatePerInstructionsIncrement(),
                                  INSTRUCTION_INCREMENT);
}

int64_t
ContractInvocationTest::getEntryReadFee(SorobanResources const& resources)
{
    auto const& cfg = getNetworkCfg();
    return (resources.footprint.readOnly.size() +
            resources.footprint.readWrite.size()) *
           cfg.feeReadLedgerEntry();
}

int64_t
ContractInvocationTest::getEntryWriteFee(SorobanResources const& resources)
{
    auto const& cfg = getNetworkCfg();
    return resources.footprint.readWrite.size() * cfg.feeWriteLedgerEntry();
}

int64_t
ContractInvocationTest::getReadBytesFee(SorobanResources const& resources)
{
    auto const& cfg = getNetworkCfg();
    return computeFeePerIncrement(resources.readBytes, cfg.feeRead1KB(),
                                  DATA_SIZE_1KB_INCREMENT);
}

int64_t
ContractInvocationTest::getWriteBytesFee(SorobanResources const& resources)
{
    auto const& cfg = getNetworkCfg();
    return computeFeePerIncrement(resources.writeBytes, cfg.feeWrite1KB(),
                                  DATA_SIZE_1KB_INCREMENT);
}

uint32_t
ContractInvocationTest::computeResourceFee(SorobanResources const& resources,
                                           SCSymbol const& functionName,
                                           std::vector<SCVal> const& args)
{
    Operation op;
    op.body.type(INVOKE_HOST_FUNCTION);
    auto& ihf = op.body.invokeHostFunctionOp().hostFunction;
    ihf.type(HOST_FUNCTION_TYPE_INVOKE_CONTRACT);
    ihf.invokeContract().contractAddress = mContractID;
    ihf.invokeContract().functionName = functionName;
    ihf.invokeContract().args.assign(args.begin(), args.end());

    auto dummyTx = sorobanTransactionFrameFromOps(
        mApp->getNetworkID(), mDummyAccount, {op}, {}, resources, 1000, 123);
    auto txSize = xdr::xdr_size(dummyTx->getEnvelope());
    return sorobanResourceFee(*mApp, resources, txSize, 0);
}

WasmContractInvocationTest::WasmContractInvocationTest(
    RustBuf const& wasm, bool deployContract, Config cfg, bool useTestLimits,
    std::function<void(SorobanNetworkConfig&)> cfgModifyFn)
    : ContractInvocationTest(cfg, useTestLimits, cfgModifyFn), mWasm(wasm)
{
    if (deployContract)
    {
        // Deploy with default resources
        SorobanResources uploadResources{};
        uploadResources.instructions = 200'000 + (mWasm.data.size() * 6000);
        uploadResources.readBytes = 1000;
        uploadResources.writeBytes = 5000;

        SorobanResources createResources{};
        createResources.instructions = 200'000;
        createResources.readBytes = 5000;
        createResources.writeBytes = 5000;

        deployWithResources(uploadResources, createResources);
    }
}

void
WasmContractInvocationTest::deployWithResources(
    SorobanResources const& uploadResources,
    SorobanResources const& createResources)
{
    if (!mContractKeys.empty())
    {
        throw "Wasm already uploaded";
    }

    deployContractWithSourceAccountWithResources(uploadResources,
                                                 createResources);
}

TransactionFrameBasePtr
WasmContractInvocationTest::getCreateTx(TestAccount& acc)
{
    if (mContractKeys.empty())
    {
        throw "Must deploy test contract before calling getCreateTx";
    }

    SorobanResources createResources{};
    createResources.instructions = 200'000;
    createResources.readBytes = 5000;
    createResources.writeBytes = 5000;

    SCVal scContractSourceRefKey(SCValType::SCV_LEDGER_KEY_CONTRACT_INSTANCE);
    auto [createOp, contractID] = createOpCommon(
        *mApp, createResources, mContractKeys[1], acc, scContractSourceRefKey);
    auto createResourceFee =
        sorobanResourceFee(*mApp, createResources, 1000, 40) + 40'000;

    return sorobanTransactionFrameFromOps(mApp->getNetworkID(), acc, {createOp},
                                          {}, createResources, 1000,
                                          createResourceFee);
}

TransactionFrameBasePtr
WasmContractInvocationTest::createUploadWasmTx(TestAccount& source,
                                               uint32_t resourceFee)
{
    SorobanResources uploadResources{};
    uploadResources.instructions = 200'000 + (mWasm.data.size() * 6000);
    uploadResources.readBytes = 1000;
    uploadResources.writeBytes = 5000;

    // Upload contract code
    Operation uploadOp;
    uploadOp.body.type(INVOKE_HOST_FUNCTION);
    auto& uploadHF = uploadOp.body.invokeHostFunctionOp().hostFunction;
    uploadHF.type(HOST_FUNCTION_TYPE_UPLOAD_CONTRACT_WASM);
    uploadHF.wasm().assign(mWasm.data.begin(), mWasm.data.end());

    LedgerKey contractCodeLedgerKey;
    contractCodeLedgerKey.type(CONTRACT_CODE);
    contractCodeLedgerKey.contractCode().hash = sha256(uploadHF.wasm());
    uploadResources.footprint.readWrite = {contractCodeLedgerKey};

    return sorobanTransactionFrameFromOps(mApp->getNetworkID(), source,
                                          {uploadOp}, {}, uploadResources, 100,
                                          resourceFee);
}

AssetContractInvocationTest::AssetContractInvocationTest(
    Asset const& asset, Config cfg, bool useTestLimits,
    std::function<void(SorobanNetworkConfig&)> cfgModifyFn)
    : ContractInvocationTest(cfg, useTestLimits, cfgModifyFn), mAsset(asset)
{
    HashIDPreimage preImage;
    preImage.type(ENVELOPE_TYPE_CONTRACT_ID);
    preImage.contractID().contractIDPreimage.type(
        CONTRACT_ID_PREIMAGE_FROM_ASSET);
    preImage.contractID().contractIDPreimage.fromAsset() = mAsset;
    preImage.contractID().networkID = mApp->getNetworkID();
    auto contractID = makeContractAddress(xdrSha256(preImage));

    Operation createOp;
    createOp.body.type(INVOKE_HOST_FUNCTION);
    auto& createHF = createOp.body.invokeHostFunctionOp();
    createHF.hostFunction.type(HOST_FUNCTION_TYPE_CREATE_CONTRACT);
    auto& createContractArgs = createHF.hostFunction.createContract();

    ContractExecutable exec;
    exec.type(CONTRACT_EXECUTABLE_STELLAR_ASSET);
    createContractArgs.contractIDPreimage.type(CONTRACT_ID_PREIMAGE_FROM_ASSET);
    createContractArgs.contractIDPreimage.fromAsset() = mAsset;
    createContractArgs.executable = exec;

    SorobanResources createResources;
    createResources.instructions = 400'000;
    createResources.readBytes = 1000;
    createResources.writeBytes = 1000;

    LedgerKey contractExecutableKey(CONTRACT_DATA);
    contractExecutableKey.contractData().contract = contractID;
    contractExecutableKey.contractData().key =
        SCVal(SCValType::SCV_LEDGER_KEY_CONTRACT_INSTANCE);
    contractExecutableKey.contractData().durability =
        ContractDataDurability::PERSISTENT;

    createResources.footprint.readWrite = {contractExecutableKey};

    submitTx(*mApp, createOp, createResources, 100, DEFAULT_TEST_RESOURCE_FEE);

    mContractKeys.emplace_back(contractExecutableKey);
    mContractID = contractID;
}

LedgerKey
AssetContractInvocationTest::makeBalanceKey(AccountID const& acc)
{
    LedgerKey balanceKey;
    if (mAsset.type() == ASSET_TYPE_NATIVE)
    {
        balanceKey.type(ACCOUNT);
        balanceKey.account().accountID = acc;
    }
    else
    {
        balanceKey.type(TRUSTLINE);
        balanceKey.trustLine().accountID = acc;
        balanceKey.trustLine().asset = assetToTrustLineAsset(mAsset);
    }

    return balanceKey;
}

LedgerKey
AssetContractInvocationTest::makeContractDataBalanceKey(SCAddress const& addr)
{
    SCVal val(SCV_ADDRESS);
    val.address() = addr;

    LedgerKey balanceKey(CONTRACT_DATA);
    balanceKey.contractData().contract = mContractID;

    SCVec balance = {makeSymbolSCVal("Balance"), val};
    SCVal balanceVal(SCValType::SCV_VEC);
    balanceVal.vec().activate() = balance;
    balanceKey.contractData().key = balanceVal;
    balanceKey.contractData().durability = ContractDataDurability::PERSISTENT;

    return balanceKey;
}

LedgerKey
AssetContractInvocationTest::makeBalanceKey(SCAddress const& addr)
{
    if (addr.type() == SC_ADDRESS_TYPE_ACCOUNT)
    {
        return makeBalanceKey(addr.accountId());
    }
    else
    {
        return makeContractDataBalanceKey(addr);
    }
}

int64_t
AssetContractInvocationTest::getBalance(SCAddress const& addr)
{
    SCVal val(SCV_ADDRESS);
    val.address() = addr;

    return addr.type() == SC_ADDRESS_TYPE_ACCOUNT
               ? txtest::getBalance(*mApp, addr.accountId(), mAsset)
               : getContractBalance(*mApp, mContractID, val);
}

void
AssetContractInvocationTest::transfer(TestAccount& fromAcc,
                                      SCAddress const& toAddr, int64_t amount,
                                      bool expectSuccess)
{
    SCVal toVal(SCV_ADDRESS);
    toVal.address() = toAddr;

    SCVal fromVal(SCV_ADDRESS);
    fromVal.address() = makeAccountAddress(fromAcc.getPublicKey());

    LedgerKey fromBalanceKey = makeBalanceKey(fromAcc.getPublicKey());
    LedgerKey toBalanceKey = makeBalanceKey(toAddr);

    auto fn = makeSymbol("transfer");
    Operation transfer;
    transfer.body.type(INVOKE_HOST_FUNCTION);
    auto& ihf = transfer.body.invokeHostFunctionOp().hostFunction;
    ihf.type(HOST_FUNCTION_TYPE_INVOKE_CONTRACT);
    ihf.invokeContract().contractAddress = mContractID;
    ihf.invokeContract().functionName = fn;
    ihf.invokeContract().args = {fromVal, toVal, makeI128(amount)};

    transfer.body.invokeHostFunctionOp().auth = {
        getInvocationAuth(ihf.invokeContract())};

    SorobanResources resources;
    resources.instructions = 2'000'000;
    resources.readBytes = 2000;
    resources.writeBytes = 1072;

    resources.footprint.readOnly = mContractKeys;

    bool fromIsIssuer = false;
    bool toIsIssuer = false;
    if (mAsset.type() != ASSET_TYPE_NATIVE)
    {
        LedgerKey issuerLedgerKey(ACCOUNT);
        issuerLedgerKey.account().accountID = getIssuer(mAsset);
        resources.footprint.readOnly.emplace_back(issuerLedgerKey);

        if (getIssuer(mAsset) == fromAcc.getPublicKey())
        {
            fromIsIssuer = true;
        }
        else
        {
            resources.footprint.readWrite.emplace_back(fromBalanceKey);
        }

        if (toAddr.type() == SC_ADDRESS_TYPE_ACCOUNT &&
            getIssuer(mAsset) == toAddr.accountId())
        {
            toIsIssuer = true;
        }
        else
        {
            resources.footprint.readWrite.emplace_back(toBalanceKey);
        }
    }
    else
    {
        resources.footprint.readWrite = {fromBalanceKey, toBalanceKey};
    }

    auto preTransferFromBalance = mAsset.type() == ASSET_TYPE_NATIVE
                                      ? fromAcc.getBalance()
                                      : fromAcc.getTrustlineBalance(mAsset);
    auto preTransferToBalance = getBalance(toAddr);

    // submit operation
    auto tx = sorobanTransactionFrameFromOps(mApp->getNetworkID(), fromAcc,
                                             {transfer}, {}, resources, 100,
                                             DEFAULT_TEST_RESOURCE_FEE);
    invokeTx(tx, expectSuccess, false);
    if (expectSuccess)
    {
        auto postTransferFromBalance =
            mAsset.type() == ASSET_TYPE_NATIVE
                ? fromAcc.getBalance()
                : fromAcc.getTrustlineBalance(mAsset);

        auto postTransferToBalance = getBalance(toAddr);

        REQUIRE((fromIsIssuer ||
                 postTransferFromBalance == preTransferFromBalance - amount));
        REQUIRE((toIsIssuer ||
                 postTransferToBalance - amount == preTransferToBalance));

        {
            // From is an account so it should never have a contract data
            // balance
            LedgerTxn ltx(mApp->getLedgerTxnRoot());
            REQUIRE(!ltx.load(makeContractDataBalanceKey(fromVal.address())));
        }

        if (toIsIssuer)
        {
            // make sure we didn't create an entry for the issuer
            LedgerTxn ltx(mApp->getLedgerTxnRoot());
            REQUIRE(!ltx.load(makeContractDataBalanceKey(toAddr)));
        }
    }
}

void
AssetContractInvocationTest::mint(TestAccount& admin, SCAddress const& toAddr,
                                  int64_t amount, bool expectSuccess)
{
    SCVal toVal(SCV_ADDRESS);
    toVal.address() = toAddr;

    LedgerKey toBalanceKey = makeBalanceKey(toAddr);

    auto fn = makeSymbol("mint");
    Operation mint;
    mint.body.type(INVOKE_HOST_FUNCTION);
    auto& ihf = mint.body.invokeHostFunctionOp().hostFunction;
    ihf.type(HOST_FUNCTION_TYPE_INVOKE_CONTRACT);
    ihf.invokeContract().contractAddress = mContractID;
    ihf.invokeContract().functionName = fn;
    ihf.invokeContract().args = {toVal, makeI128(amount)};

    mint.body.invokeHostFunctionOp().auth = {
        getInvocationAuth(ihf.invokeContract())};

    SorobanResources resources;
    resources.instructions = 2'000'000;
    resources.readBytes = 2000;
    resources.writeBytes = 1072;

    resources.footprint.readOnly = mContractKeys;

    if (mAsset.type() != ASSET_TYPE_NATIVE)
    {
        LedgerKey issuerLedgerKey(ACCOUNT);
        issuerLedgerKey.account().accountID = getIssuer(mAsset);
        resources.footprint.readOnly.emplace_back(issuerLedgerKey);
    }

    resources.footprint.readWrite = {toBalanceKey};

    auto preMintBalance = getBalance(toAddr);

    // submit operation
    auto tx = sorobanTransactionFrameFromOps(mApp->getNetworkID(), admin,
                                             {mint}, {}, resources, 100,
                                             DEFAULT_TEST_RESOURCE_FEE);

    invokeTx(tx, expectSuccess, false);
    if (expectSuccess)
    {
        auto postMintBalance = getBalance(toAddr);
        REQUIRE(postMintBalance - amount == preMintBalance);
    }
}

void
AssetContractInvocationTest::burn(TestAccount& from, int64_t amount,
                                  bool expectSuccess)
{
    auto fromAddr = makeAccountAddress(from.getPublicKey());

    SCVal fromVal(SCV_ADDRESS);
    fromVal.address() = fromAddr;

    LedgerKey fromBalanceKey;
    if (mAsset.type() == ASSET_TYPE_NATIVE)
    {
        fromBalanceKey.type(ACCOUNT);
        fromBalanceKey.account().accountID = fromAddr.accountId();
    }
    else
    {
        fromBalanceKey.type(TRUSTLINE);
        fromBalanceKey.trustLine().accountID = fromAddr.accountId();
        fromBalanceKey.trustLine().asset = assetToTrustLineAsset(mAsset);
    }

    auto fn = makeSymbol("burn");
    Operation burn;
    burn.body.type(INVOKE_HOST_FUNCTION);
    auto& ihf = burn.body.invokeHostFunctionOp().hostFunction;
    ihf.type(HOST_FUNCTION_TYPE_INVOKE_CONTRACT);
    ihf.invokeContract().contractAddress = mContractID;
    ihf.invokeContract().functionName = fn;
    ihf.invokeContract().args = {fromVal, makeI128(amount)};

    burn.body.invokeHostFunctionOp().auth = {
        getInvocationAuth(ihf.invokeContract())};

    SorobanResources resources;
    resources.instructions = 2'000'000;
    resources.readBytes = 2000;
    resources.writeBytes = 1072;

    resources.footprint.readOnly = mContractKeys;

    bool isIssuer = getIssuer(mAsset) == from.getPublicKey();
    if (!isIssuer)
    {
        resources.footprint.readWrite = {fromBalanceKey};
    }

    auto preBurnBalance = getBalance(fromAddr);

    // submit operation
    auto tx = sorobanTransactionFrameFromOps(mApp->getNetworkID(), from, {burn},
                                             {}, resources, 100,
                                             DEFAULT_TEST_RESOURCE_FEE);

    invokeTx(tx, expectSuccess, false);

    if (expectSuccess)
    {
        auto postBurnBalance = getBalance(fromAddr);
        REQUIRE(preBurnBalance - amount == postBurnBalance);
    }
}

void
AssetContractInvocationTest::clawback(TestAccount& admin,
                                      SCAddress const& fromAddr, int64_t amount,
                                      bool expectSuccess)
{
    SCVal fromVal(SCV_ADDRESS);
    fromVal.address() = fromAddr;

    LedgerKey fromBalanceKey = makeBalanceKey(fromAddr);

    auto fn = makeSymbol("clawback");
    Operation clawback;
    clawback.body.type(INVOKE_HOST_FUNCTION);
    auto& ihf = clawback.body.invokeHostFunctionOp().hostFunction;
    ihf.type(HOST_FUNCTION_TYPE_INVOKE_CONTRACT);
    ihf.invokeContract().contractAddress = mContractID;
    ihf.invokeContract().functionName = fn;
    ihf.invokeContract().args = {fromVal, makeI128(amount)};

    clawback.body.invokeHostFunctionOp().auth = {
        getInvocationAuth(ihf.invokeContract())};

    SorobanResources resources;
    resources.instructions = 2'000'000;
    resources.readBytes = 2000;
    resources.writeBytes = 1072;

    resources.footprint.readOnly = mContractKeys;

    if (mAsset.type() != ASSET_TYPE_NATIVE)
    {
        LedgerKey issuerLedgerKey(ACCOUNT);
        issuerLedgerKey.account().accountID = getIssuer(mAsset);
        resources.footprint.readOnly.emplace_back(issuerLedgerKey);
    }

    resources.footprint.readWrite = {fromBalanceKey};
    auto preClawbackBalance = getBalance(fromAddr);

    // submit operation
    auto tx = sorobanTransactionFrameFromOps(mApp->getNetworkID(), admin,
                                             {clawback}, {}, resources, 100,
                                             DEFAULT_TEST_RESOURCE_FEE);

    invokeTx(tx, expectSuccess, false);
    if (expectSuccess)
    {
        auto postClawbackBalance = getBalance(fromAddr);
        REQUIRE(preClawbackBalance - amount == postClawbackBalance);
    }
}

TransactionFrameBasePtr
ContractInvocationTest::createInvokeTx(SorobanResources const& resources,
                                       SCSymbol const& functionName,
                                       std::vector<SCVal> const& args,
                                       uint32_t inclusionFee,
                                       uint32_t resourceFee,
                                       std::shared_ptr<TestAccount> source)
{
    Operation op;
    op.body.type(INVOKE_HOST_FUNCTION);
    auto& ihf = op.body.invokeHostFunctionOp().hostFunction;
    ihf.type(HOST_FUNCTION_TYPE_INVOKE_CONTRACT);
    ihf.invokeContract().contractAddress = mContractID;
    ihf.invokeContract().functionName = functionName;
    ihf.invokeContract().args.assign(args.begin(), args.end());
    auto& acc = source ? *source : getRoot();

    return sorobanTransactionFrameFromOps(mApp->getNetworkID(), acc, {op}, {},
                                          resources, inclusionFee, resourceFee);
}

TransactionFrameBasePtr
ContractInvocationTest::createExtendOpTx(SorobanResources const& resources,
                                         uint32_t extendTo, uint32_t fee,
                                         uint32_t refundableFee,
                                         std::shared_ptr<TestAccount> source)
{
    Operation op;
    op.body.type(EXTEND_FOOTPRINT_TTL);
    op.body.extendFootprintTTLOp().extendTo = extendTo;
    auto& acc = source ? *source : getRoot();
    return sorobanTransactionFrameFromOps(mApp->getNetworkID(), acc, {op}, {},
                                          resources, fee, refundableFee);
}

TransactionFrameBasePtr
ContractInvocationTest::createRestoreTx(SorobanResources const& resources,
                                        uint32_t fee, uint32_t refundableFee,
                                        std::shared_ptr<TestAccount> source)
{
    Operation op;
    op.body.type(RESTORE_FOOTPRINT);
    auto& acc = source ? *source : getRoot();
    return sorobanTransactionFrameFromOps(mApp->getNetworkID(), acc, {op}, {},
                                          resources, fee, refundableFee);
}

void
ContractInvocationTest::txCheckValid(TransactionFrameBasePtr tx)
{
    LedgerTxn ltx(mApp->getLedgerTxnRoot());
    REQUIRE(tx->checkValid(*mApp, ltx, 0, 0, 0));
}

bool
ContractInvocationTest::isTxValid(TransactionFrameBasePtr tx)
{
    LedgerTxn ltx(mApp->getLedgerTxnRoot());
    auto ret = tx->checkValid(*mApp, ltx, 0, 0, 0);
    return ret;
}

std::shared_ptr<TransactionMetaFrame>
ContractInvocationTest::invokeTx(TransactionFrameBasePtr tx, bool expectSuccess,
                                 bool processPostApply)
{
    LedgerTxn ltx(mApp->getLedgerTxnRoot());
    auto txm = std::make_shared<TransactionMetaFrame>(
        ltx.loadHeader().current().ledgerVersion);

    REQUIRE(tx->checkValid(*mApp, ltx, 0, 0, 0));
    if (expectSuccess)
    {
        REQUIRE(tx->apply(*mApp, ltx, *txm));
    }
    else
    {
        REQUIRE(!tx->apply(*mApp, ltx, *txm));
    }

    if (processPostApply)
    {
        tx->processPostApply(*mApp, ltx, *txm);
    }

    ltx.commit();
    return txm;
}

void
ContractInvocationTest::checkTTL(LedgerKey const& k,
                                 uint32_t expectedLiveUntilLedger)
{
    LedgerTxn ltx(mApp->getLedgerTxnRoot());
    auto ltxe = ltx.loadWithoutRecord(k);
    REQUIRE(ltxe);

    auto ttlKey = getTTLKey(k);
    auto ttlLtxe = ltx.loadWithoutRecord(ttlKey);
    REQUIRE(ttlLtxe);
    REQUIRE(ttlLtxe.current().data.ttl().liveUntilLedgerSeq ==
            expectedLiveUntilLedger);
}

bool
ContractInvocationTest::isEntryLive(LedgerKey const& k, uint32_t ledgerSeq)
{
    auto ttlKey = getTTLKey(k);
    LedgerTxn ltx(mApp->getLedgerTxnRoot());
    auto ttlLtxe = ltx.loadWithoutRecord(ttlKey);
    REQUIRE(ttlLtxe);
    return isLive(ttlLtxe.current(), ledgerSeq);
}

void
ContractInvocationTest::restoreOp(xdr::xvector<LedgerKey> const& readWrite,
                                  int64_t expectedRefundableFeeCharged,
                                  bool expectSuccess)
{
    SorobanResources resources;
    resources.footprint.readWrite = readWrite;
    resources.instructions = 0;
    resources.readBytes = 10'000;
    resources.writeBytes = 10'000;

    auto resourceFee = 300'000 + 40'000 * readWrite.size();
    auto tx = createRestoreTx(resources, 1'000, resourceFee);
    invokeArchivalOp(tx, expectedRefundableFeeCharged, expectSuccess);
}

void
ContractInvocationTest::extendOp(
    xdr::xvector<LedgerKey> const& readOnly, uint32_t extendTo,
    bool expectSuccess,
    std::optional<uint32_t> expectedRefundableChargeOverride)
{
    int64_t expectedRefundableFeeCharged = 0;
    if (expectedRefundableChargeOverride)
    {
        expectedRefundableFeeCharged = *expectedRefundableChargeOverride;
    }
    else
    {
        expectedRefundableFeeCharged +=
            getRentFeeForExtension(readOnly, extendTo);
    }

    SorobanResources extendResources;
    extendResources.footprint.readOnly = readOnly;
    extendResources.instructions = 0;
    extendResources.readBytes = 10'000;
    extendResources.writeBytes = 0;

    auto resourceFee = DEFAULT_TEST_RESOURCE_FEE * readOnly.size();
    auto tx = createExtendOpTx(extendResources, extendTo, 1'000, resourceFee);
    invokeArchivalOp(tx, expectedRefundableFeeCharged, expectSuccess);
}

void
ContractStorageInvocationTest::put(std::string const& key,
                                   ContractDataDurability type, uint64_t val,
                                   bool expectSuccess)
{
    auto keySymbol = makeSymbolSCVal(key);
    auto storageLK = contractDataKey(mContractID, keySymbol, type);
    putWithFootprint(key, type, val, mContractKeys, {storageLK}, expectSuccess);
}

void
ContractStorageInvocationTest::putWithFootprint(
    std::string const& key, ContractDataDurability type, uint64_t val,
    xdr::xvector<LedgerKey> const& readOnly,
    xdr::xvector<LedgerKey> const& readWrite, bool expectSuccess,
    uint32_t writeBytes, uint32_t refundableFee)
{
    std::string funcStr = type == ContractDataDurability::TEMPORARY
                              ? "put_temporary"
                              : "put_persistent";

    auto keySymbol = makeSymbolSCVal(key);
    auto valU64 = makeU64SCVal(val);
    auto storageLK = contractDataKey(mContractID, keySymbol, type);

    SorobanResources resources;
    resources.footprint.readOnly = readOnly;
    resources.footprint.readWrite = readWrite;
    resources.instructions = 4'000'000;
    resources.readBytes = 10'000;
    resources.writeBytes = writeBytes;

    auto resourceFee =
        computeResourceFee(resources, makeSymbol(funcStr), {keySymbol, valU64});
    resourceFee += refundableFee;
    auto tx = createInvokeTx(resources, makeSymbol(funcStr),
                             {keySymbol, valU64}, 1'000, resourceFee);
    invokeTx(tx, /*expectSuccess=*/expectSuccess, false);
}

uint64_t
ContractStorageInvocationTest::get(std::string const& key,
                                   ContractDataDurability type,
                                   bool expectSuccess)
{
    auto lk = contractDataKey(mContractID, makeSymbolSCVal(key), type);
    auto readOnly = mContractKeys;
    readOnly.emplace_back(lk);
    return getWithFootprint(key, type, readOnly, {}, expectSuccess);
}

uint64_t
ContractStorageInvocationTest::getWithFootprint(
    std::string const& key, ContractDataDurability type,
    xdr::xvector<LedgerKey> const& readOnly,
    xdr::xvector<LedgerKey> const& readWrite, bool expectSuccess,
    uint32_t readBytes)
{
    std::string funcStr = type == ContractDataDurability::TEMPORARY
                              ? "get_temporary"
                              : "get_persistent";

    auto keySymbol = makeSymbolSCVal(key);
    auto storageLK = contractDataKey(mContractID, keySymbol, type);

    SorobanResources resources;
    resources.footprint.readOnly = readOnly;
    resources.footprint.readWrite = readWrite;
    resources.instructions = 4'000'000;
    resources.readBytes = readBytes;
    resources.writeBytes = 0;

    auto resourceFee =
        computeResourceFee(resources, makeSymbol(funcStr), {keySymbol});
    // Default refundable fee
    resourceFee += 40'000;
    auto tx = createInvokeTx(resources, makeSymbol(funcStr), {keySymbol}, 1'000,
                             resourceFee);
    auto txm = invokeTx(tx, expectSuccess, false);
    if (expectSuccess)
    {
        return txm->getXDR().v3().sorobanMeta->returnValue.u64();
    }

    return 0;
}

bool
ContractStorageInvocationTest::has(std::string const& key,
                                   ContractDataDurability type,
                                   bool expectSuccess)
{
    auto lk = contractDataKey(mContractID, makeSymbolSCVal(key), type);
    auto readOnly = mContractKeys;
    readOnly.emplace_back(lk);
    return hasWithFootprint(key, type, readOnly, {}, expectSuccess);
}

bool
ContractStorageInvocationTest::hasWithFootprint(
    std::string const& key, ContractDataDurability type,
    xdr::xvector<LedgerKey> const& readOnly,
    xdr::xvector<LedgerKey> const& readWrite, bool expectSuccess)
{
    std::string funcStr = type == ContractDataDurability::TEMPORARY
                              ? "has_temporary"
                              : "has_persistent";

    auto keySymbol = makeSymbolSCVal(key);
    auto storageLK = contractDataKey(mContractID, keySymbol, type);

    SorobanResources resources;
    resources.footprint.readOnly = readOnly;
    resources.footprint.readWrite = readWrite;
    resources.instructions = 4'000'000;
    resources.readBytes = 10'000;
    resources.writeBytes = 0;

    auto resourceFee =
        computeResourceFee(resources, makeSymbol(funcStr), {keySymbol});
    // Default refundable fee
    resourceFee += 40'000;

    auto tx = createInvokeTx(resources, makeSymbol(funcStr), {keySymbol}, 1'000,
                             resourceFee);
    auto txm = invokeTx(tx, /*expectSuccess=*/expectSuccess, false);
    if (expectSuccess)
    {
        return txm->getXDR().v3().sorobanMeta->returnValue.b();
    }

    return false;
}

void
ContractStorageInvocationTest::del(std::string const& key,
                                   ContractDataDurability type)
{
    auto lk = contractDataKey(mContractID, makeSymbolSCVal(key), type);
    delWithFootprint(key, type, mContractKeys, {lk}, true);
}

void
ContractStorageInvocationTest::delWithFootprint(
    std::string const& key, ContractDataDurability type,
    xdr::xvector<LedgerKey> const& readOnly,
    xdr::xvector<LedgerKey> const& readWrite, bool expectSuccess)
{
    std::string funcStr = type == ContractDataDurability::TEMPORARY
                              ? "del_temporary"
                              : "del_persistent";

    auto keySymbol = makeSymbolSCVal(key);
    auto storageLK = contractDataKey(mContractID, keySymbol, type);

    SorobanResources resources;
    resources.footprint.readOnly = readOnly;
    resources.footprint.readWrite = readWrite;
    resources.instructions = 4'000'000;
    resources.readBytes = 10'000;
    resources.writeBytes = 0;

    auto resourceFee =
        computeResourceFee(resources, makeSymbol(funcStr), {keySymbol});
    // Default refundable fee
    resourceFee += 40'000;
    auto tx = createInvokeTx(resources, makeSymbol(funcStr), {keySymbol}, 1'000,
                             resourceFee);
    invokeTx(tx, expectSuccess, false);
}

void
ContractStorageInvocationTest::checkTTL(std::string const& key,
                                        ContractDataDurability type,
                                        uint32_t expectedLiveUntilLedger)
{
    auto keySymbol = makeSymbolSCVal(key);
    auto lk = contractDataKey(mContractID, keySymbol, type);
    checkTTL(lk, expectedLiveUntilLedger);
}

bool
ContractStorageInvocationTest::isEntryLive(std::string const& key,
                                           ContractDataDurability type,
                                           uint32_t ledgerSeq)
{
    auto keySymbol = makeSymbolSCVal(key);
    auto lk = contractDataKey(mContractID, keySymbol, type);
    return isEntryLive(lk, ledgerSeq);
}

void
ContractStorageInvocationTest::extendHostFunction(std::string const& key,
                                                  ContractDataDurability type,
                                                  uint32_t threshold,
                                                  uint32_t extendTo,
                                                  bool expectSuccess)
{
    auto keySymbol = makeSymbolSCVal(key);
    auto thresholdU32 = makeU32(threshold);
    auto extendToU32 = makeU32(extendTo);

    std::string funcStr = type == ContractDataDurability::TEMPORARY
                              ? "extend_temporary"
                              : "extend_persistent";

    SorobanResources resources;
    resources.footprint.readOnly = mContractKeys;
    resources.footprint.readOnly.emplace_back(
        contractDataKey(mContractID, keySymbol, type));
    resources.instructions = 4'000'000;
    resources.readBytes = 10'000;
    resources.writeBytes = 1000;

    auto resourceFee = computeResourceFee(
        resources, makeSymbol(funcStr), {keySymbol, thresholdU32, extendToU32});
    // Default refundable fee
    resourceFee += 40'000;

    auto tx = createInvokeTx(resources, makeSymbol(funcStr),
                             {keySymbol, thresholdU32, extendToU32}, 1'000,
                             resourceFee);
    invokeTx(tx, expectSuccess, /*processPostApply=*/false);
}

void
ContractStorageInvocationTest::resizeStorageAndExtend(
    std::string const& key, uint32_t numKiloBytes, uint32_t thresh,
    uint32_t extendTo, uint32_t writeBytes, uint32_t refundableFee,
    bool expectSuccess)
{
    auto keySymbol = makeSymbolSCVal(key);
    auto storageLK = contractDataKey(mContractID, keySymbol,
                                     ContractDataDurability::PERSISTENT);

    auto numKiloBytesU32 = makeU32(numKiloBytes);
    auto threshU32 = makeU32(thresh);
    auto extendToU32 = makeU32(extendTo);

    std::string funcStr = "replace_with_bytes_and_extend";

    SorobanResources resources;
    resources.footprint.readOnly = mContractKeys;
    resources.footprint.readWrite = {storageLK};
    resources.instructions = 4'000'000;
    resources.readBytes = 10'000;
    resources.writeBytes = writeBytes;

    auto resourceFee = computeResourceFee(
        resources, makeSymbol(funcStr),
        {keySymbol, numKiloBytesU32, threshU32, extendToU32});
    resourceFee += refundableFee;

    auto tx =
        createInvokeTx(resources, makeSymbol(funcStr),
                       {keySymbol, numKiloBytesU32, threshU32, extendToU32},
                       1'000, resourceFee);
    invokeTx(tx, /*expectSuccess=*/expectSuccess, false);
}
}
}
