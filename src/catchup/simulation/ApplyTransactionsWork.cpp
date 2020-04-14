// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "catchup/simulation/ApplyTransactionsWork.h"
#include "crypto/Hex.h"
#include "crypto/SignerKey.h"
#include "herder/LedgerCloseData.h"
#include "herder/simulation/ExactSimulationTxSetFrame.h"
#include "herder/simulation/SimulationTxSetFrame.h"
#include "ledger/LedgerManagerImpl.h"
#include "ledger/LedgerRange.h"
#include "ledger/LedgerTxn.h"
#include "transactions/SignatureUtils.h"
#include "transactions/TransactionBridge.h"
#include "transactions/TransactionSQL.h"
#include "transactions/TransactionUtils.h"
#include "util/SimulationUtils.h"
#include "util/format.h"
#include "xdrpp/printer.h"

namespace stellar
{

ApplyTransactionsWork::ApplyTransactionsWork(
    Application& app, TmpDir const& downloadDir, LedgerRange const& range,
    std::string const& networkPassphrase, uint32_t desiredOperations,
    bool upgrade, uint32_t multiplier, bool verifyResults)
    : BasicWork(app, "apply-transactions", RETRY_NEVER)
    , mDownloadDir(downloadDir)
    , mRange(range)
    , mNetworkID(sha256(networkPassphrase))
    , mTransactionHistory{}
    , mTransactionIter(mTransactionHistory.txSet.txs.cend())
    , mResultHistory{}
    , mResultIter(mResultHistory.txResultSet.results.cend())
    , mMaxOperations(desiredOperations)
    , mUpgradeProtocol(upgrade)
    , mMultiplier(multiplier)
    , mVerifyResults(verifyResults)
{
    if (mMultiplier == 0)
    {
        throw std::runtime_error("Invalid multiplier!");
    }

    auto const& lcl = mApp.getLedgerManager().getLastClosedLedgerHeader();
    if (mUpgradeProtocol &&
        lcl.header.ledgerVersion + 1 != Config::CURRENT_LEDGER_PROTOCOL_VERSION)
    {
        throw std::runtime_error("Invalid ledger version: can only force "
                                 "upgrade for consecutive versions");
    }
}

static void
checkOperationResults(xdr::xvector<OperationResult> const& expected,
                      xdr::xvector<OperationResult> const& actual)
{
    assert(expected.size() == actual.size());
    for (size_t i = 0; i < expected.size(); i++)
    {
        if (expected[i].code() != actual[i].code())
        {
            CLOG(ERROR, "History")
                << fmt::format("Expected operation result {} but got {}",
                               xdr::xdr_to_string(expected[i].code()),
                               xdr::xdr_to_string(actual[i].code()));
            continue;
        }

        if (expected[i].code() != opINNER)
        {
            continue;
        }

        auto const& expectedOpRes = expected[i].tr();
        auto const& actualOpRes = actual[i].tr();

        assert(expectedOpRes.type() == actualOpRes.type());

        auto check = [&](int expectedCode, int actualCode) {
            auto success = expectedCode >= 0 && actualCode >= 0;
            auto fail = expectedCode < 0 && actualCode < 0;
            return success || fail;
        };

        bool match = false;
        switch (expectedOpRes.type())
        {
        case CREATE_ACCOUNT:
            match = check(actualOpRes.createAccountResult().code(),
                          expectedOpRes.createAccountResult().code());
            break;
        case PAYMENT:
            match = check(actualOpRes.paymentResult().code(),
                          expectedOpRes.paymentResult().code());
            break;
        case PATH_PAYMENT_STRICT_RECEIVE:
            match =
                check(actualOpRes.pathPaymentStrictReceiveResult().code(),
                      expectedOpRes.pathPaymentStrictReceiveResult().code());
            break;
        case PATH_PAYMENT_STRICT_SEND:
            match = check(actualOpRes.pathPaymentStrictSendResult().code(),
                          expectedOpRes.pathPaymentStrictSendResult().code());
            break;
        case MANAGE_SELL_OFFER:
            match = check(actualOpRes.manageSellOfferResult().code(),
                          expectedOpRes.manageSellOfferResult().code());
            break;
        case MANAGE_BUY_OFFER:
            match = check(actualOpRes.manageBuyOfferResult().code(),
                          expectedOpRes.manageBuyOfferResult().code());
            break;
        case CREATE_PASSIVE_SELL_OFFER:
            match = check(actualOpRes.createPassiveSellOfferResult().code(),
                          expectedOpRes.createPassiveSellOfferResult().code());
            break;
        case SET_OPTIONS:
            match = check(actualOpRes.setOptionsResult().code(),
                          expectedOpRes.setOptionsResult().code());
            break;
        case CHANGE_TRUST:
            match = check(actualOpRes.changeTrustResult().code(),
                          expectedOpRes.changeTrustResult().code());
            break;
        case ALLOW_TRUST:
            match = check(actualOpRes.allowTrustResult().code(),
                          expectedOpRes.allowTrustResult().code());
            break;
        case ACCOUNT_MERGE:
            match = check(actualOpRes.accountMergeResult().code(),
                          expectedOpRes.accountMergeResult().code());
            break;
        case MANAGE_DATA:
            match = check(actualOpRes.manageDataResult().code(),
                          expectedOpRes.manageDataResult().code());
            break;
        case INFLATION:
            match = check(actualOpRes.inflationResult().code(),
                          expectedOpRes.inflationResult().code());
            break;
        case BUMP_SEQUENCE:
            match = check(actualOpRes.bumpSeqResult().code(),
                          expectedOpRes.bumpSeqResult().code());
            break;
        default:
            throw std::runtime_error("Unknown operation type");
        }

        if (!match)
        {
            CLOG(ERROR, "History")
                << fmt::format("Expected operation result: {}",
                               xdr::xdr_to_string(expectedOpRes));
            CLOG(ERROR, "History") << fmt::format(
                "Actual operation result: {}", xdr::xdr_to_string(actualOpRes));
        }
    }
}

void
ApplyTransactionsWork::checkResults(
    Application& app, TransactionResultSet const& actualResults,
    std::vector<TransactionResultPair> const& expectedResults) const
{
    assert(actualResults.results.size() == expectedResults.size());
    for (size_t i = 0; i < expectedResults.size(); i++)
    {
        assert(actualResults.results[i].transactionHash ==
               expectedResults[i].transactionHash);

        auto const& dbRes = actualResults.results[i].result.result;
        auto const& archiveRes = expectedResults[i].result.result;

        if (!(dbRes == archiveRes))
        {
            CLOG(ERROR, "History") << fmt::format(
                "StrictCheck - Expected result {} does "
                "not agree with {} for tx {}",
                xdr::xdr_to_string(archiveRes), xdr::xdr_to_string(dbRes),
                binToHex(expectedResults[i].transactionHash));
        }

        /* if (dbRes.code() != archiveRes.code())
        {
            CLOG(ERROR, "History") << fmt::format(
                "Expected result code {} does not agree with {} for tx {}",
                xdr::xdr_to_string(archiveRes.code()),
                xdr::xdr_to_string(dbRes.code()),
                binToHex(expectedResults[i].transactionHash));
        }
        else if (dbRes.code() == txFEE_BUMP_INNER_FAILED ||
                 dbRes.code() == txFEE_BUMP_INNER_SUCCESS)
        {

            if (dbRes.innerResultPair().result.result.code() !=
                archiveRes.innerResultPair().result.result.code())
            {
                CLOG(ERROR, "History") << fmt::format(
                    "Expected result code {} does not agree with {} for "
                    "fee-bump inner tx {}",
                    xdr::xdr_to_string(
                        archiveRes.innerResultPair().result.result.code()),
                    xdr::xdr_to_string(
                        dbRes.innerResultPair().result.result.code()),
                    binToHex(archiveRes.innerResultPair().transactionHash));
            }
            else if (dbRes.innerResultPair().result.result.code() == txFAILED ||
                     dbRes.innerResultPair().result.result.code() == txSUCCESS)
            {
                checkOperationResults(
                    archiveRes.innerResultPair().result.result.results(),
                    dbRes.innerResultPair().result.result.results());
            }
        }
        else if (dbRes.code() == txFAILED || dbRes.code() == txSUCCESS)
        {
            checkOperationResults(archiveRes.results(), dbRes.results());
        } */
    }
}

static bool
hasSig(PublicKey const& account,
       xdr::xvector<DecoratedSignature, 20> const& sigs, Hash const& hash)
{
    // Is the signature of this account present in the envelope we're
    // simulating?
    return std::any_of(
        sigs.begin(), sigs.end(), [&](DecoratedSignature const& sig) {
            return PubKeyUtils::verifySig(account, sig.signature, hash);
        });
}

void
ApplyTransactionsWork::addSignerKeys(
    AccountID const& acc, AbstractLedgerTxn& ltx, std::set<SecretKey>& keys,
    xdr::xvector<DecoratedSignature, 20> const& sigs, uint32_t n)
{
    auto const& txHash = mResultIter->transactionHash;

    if (hasSig(acc, sigs, txHash))
    {
        keys.emplace(SimulationUtils::getNewSecret(acc, n));
    }

    auto account = stellar::loadAccount(ltx, acc);
    if (!account)
    {
        return;
    }

    for (auto const& signer : account.current().data.account().signers)
    {
        if (signer.key.type() == SIGNER_KEY_TYPE_ED25519)
        {
            auto pubKey = KeyUtils::convertKey<PublicKey>(signer.key);
            if (hasSig(pubKey, sigs, txHash))
            {
                keys.emplace(SimulationUtils::getNewSecret(pubKey, n));
            }
        }
    }
}

void
ApplyTransactionsWork::mutateTxSourceAccounts(TransactionEnvelope& env,
                                              AbstractLedgerTxn& ltx,
                                              std::set<SecretKey>& keys,
                                              uint32_t n)
{
    auto const& sigs = txbridge::getSignaturesInner(env);
    auto addSignerAndReplaceID = [&](AccountID& acc) {
        addSignerKeys(acc, ltx, keys, sigs, n);
        SimulationUtils::updateAccountID(acc, n);
    };

    // Depending on the envelope type, update sourceAccount and maybe feeSource
    AccountID acc;
    switch (env.type())
    {
    case ENVELOPE_TYPE_TX_V0:
        // Wrap raw Ed25519 key in an AccountID
        acc.type(PUBLIC_KEY_TYPE_ED25519);
        acc.ed25519() = env.v0().tx.sourceAccountEd25519;
        addSignerKeys(acc, ltx, keys, sigs, n);
        env.v0().tx.sourceAccountEd25519 =
            SimulationUtils::getNewSecret(acc, n).getPublicKey().ed25519();
        break;
    case ENVELOPE_TYPE_TX:
        addSignerAndReplaceID(env.v1().tx.sourceAccount);
        break;
    case ENVELOPE_TYPE_TX_FEE_BUMP:
        // Note: handle inner transaction only, outer signatures will be handled
        // separately
        assert(env.feeBump().tx.innerTx.type() == ENVELOPE_TYPE_TX);
        addSignerAndReplaceID(env.feeBump().tx.innerTx.v1().tx.sourceAccount);
        break;
    default:
        throw std::runtime_error("Unknown envelope type");
    }
}

void
ApplyTransactionsWork::mutateOperations(TransactionEnvelope& env,
                                        AbstractLedgerTxn& ltx,
                                        std::set<SecretKey>& keys, uint32_t n)
{
    auto& ops = txbridge::getOperations(env);
    auto const& sigs = txbridge::getSignaturesInner(env);

    for (auto& op : ops)
    {
        // Add signer keys where needed before simulating the operation
        if (op.sourceAccount)
        {
            addSignerKeys(*op.sourceAccount, ltx, keys, sigs, n);
        }

        // Prior to protocol 10, it is possible that we might have just added a
        // signer; check if their signature is required
        if (op.body.type() == SET_OPTIONS && op.body.setOptionsOp().signer)
        {
            auto signer = *op.body.setOptionsOp().signer;
            if (signer.key.type() == SIGNER_KEY_TYPE_ED25519)
            {
                auto signerKey = KeyUtils::convertKey<PublicKey>(signer.key);
                if (hasSig(signerKey, sigs, mResultIter->transactionHash))
                {
                    keys.emplace(SimulationUtils::getNewSecret(signerKey, n));
                }
            }
        }
        SimulationUtils::updateOperation(op, n);
    }
}

uint32_t
ApplyTransactionsWork::scaleLedger(
    std::vector<TransactionEnvelope>& transactions,
    std::vector<TransactionResultPair>& results,
    std::vector<UpgradeType>& upgrades, uint32_t n)
{
    assert(mTransactionIter != mTransactionHistory.txSet.txs.cend());
    assert(mResultIter != mResultHistory.txResultSet.results.cend());

    auto const& env = mUpgradeProtocol
                          ? txbridge::convertForV13(*mTransactionIter)
                          : *mTransactionIter;
    TransactionEnvelope newEnv = env;

    // No mutation needed, simply return existing transactions and results
    if (n == 0)
    {
        transactions.emplace_back(newEnv);
        results.emplace_back(*mResultIter);
        return txbridge::getOperations(newEnv).size();
    }

    // Keep track of accounts that need to sign
    std::set<SecretKey> keys;

    // First, update transaction source accounts
    LedgerTxn ltx(mApp.getLedgerTxnRoot());
    mutateTxSourceAccounts(newEnv, ltx, keys, n);
    mutateOperations(newEnv, ltx, keys, n);

    auto simulateSigs = [&](xdr::xvector<DecoratedSignature, 20>& sigs,
                            std::set<SecretKey> const& keys) {
        auto txFrame = TransactionFrameBase::makeTransactionFromWire(
            mApp.getNetworkID(), newEnv);
        auto hash = txFrame->getContentsHash();
        sigs.clear();
        std::transform(
            keys.begin(), keys.end(), std::back_inserter(sigs),
            [&](SecretKey const& k) { return SignatureUtils::sign(k, hash); });
        return hash;
    };

    // Handle v0 and v1 tx signatures, or fee-bump inner tx
    // Note: for fee-bump transactions, set inner tx signatures first
    // to ensure the right hash
    auto newTxHash = simulateSigs(txbridge::getSignaturesInner(newEnv), keys);

    // Second, if fee-bump tx, handle outer tx signatures
    if (newEnv.type() == ENVELOPE_TYPE_TX_FEE_BUMP)
    {
        std::set<SecretKey> outerTxKeys;
        auto& outerSigs = newEnv.feeBump().signatures;
        addSignerKeys(newEnv.feeBump().tx.feeSource, ltx, outerTxKeys,
                      outerSigs, n);
        SimulationUtils::updateAccountID(newEnv.feeBump().tx.feeSource, n);
        newTxHash = simulateSigs(outerSigs, outerTxKeys);
    }

    // These are not exactly accurate, but sufficient to check result codes
    auto newRes = *mResultIter;
    newRes.transactionHash = newTxHash;

    results.emplace_back(newRes);
    transactions.emplace_back(newEnv);

    return txbridge::getOperations(newEnv).size();
}

bool
ApplyTransactionsWork::getNextLedgerFromHistoryArchive()
{
    if (mStream->getNextLedger(mHeaderHistory, mTransactionHistory,
                               mResultHistory))
    {
        // Derive transaction apply order from the results
        std::unordered_map<Hash, TransactionEnvelope> transactions;
        for (auto const& tx : mTransactionHistory.txSet.txs)
        {
            auto txFrame = TransactionFrameBase::makeTransactionFromWire(
                mApp.getNetworkID(), tx);
            transactions[txFrame->getContentsHash()] = tx;
        }

        mTransactionHistory.txSet.txs.clear();
        for (auto const& result : mResultHistory.txResultSet.results)
        {
            auto it = transactions.find(result.transactionHash);
            assert(it != transactions.end());
            mTransactionHistory.txSet.txs.emplace_back(it->second);
        }
        mTransactionIter = mTransactionHistory.txSet.txs.cbegin();
        mResultIter = mResultHistory.txResultSet.results.cbegin();
        return true;
    }
    return false;
}

bool
ApplyTransactionsWork::getNextLedger(
    std::vector<TransactionEnvelope>& transactions,
    std::vector<TransactionResultPair>& results,
    std::vector<UpgradeType>& upgrades)
{
    transactions.clear();
    results.clear();
    upgrades.clear();

    if (mTransactionIter == mTransactionHistory.txSet.txs.cend())
    {
        if (!getNextLedgerFromHistoryArchive())
        {
            return false;
        }
    }

    size_t nOps = 0;
    auto moreOps = [&]() {
        return mMaxOperations == 0 || (nOps < mMaxOperations);
    };

    while (true)
    {
        // sustained: mMaxOperations > 0,
        // scaled ledger: avoid checking nOps < mMaxOperations, mMaxOperations
        // = 0
        while (mTransactionIter != mTransactionHistory.txSet.txs.cend() &&
               moreOps())
        {
            for (uint32_t count = 0; count < mMultiplier; count++)
            {
                nOps += scaleLedger(transactions, results, upgrades, count);
            }

            ++mTransactionIter;
            ++mResultIter;
        }

        if (mTransactionIter != mTransactionHistory.txSet.txs.cend() ||
            mMaxOperations == 0)
        {
            break;
        }

        if (!getNextLedgerFromHistoryArchive())
        {
            break;
        }

        upgrades = mHeaderHistory.header.scpValue.upgrades;
        upgrades.erase(
            std::remove_if(upgrades.begin(), upgrades.end(),
                           [](auto const& opaqueUpgrade) {
                               LedgerUpgrade upgrade;
                               xdr::xdr_from_opaque(opaqueUpgrade, upgrade);
                               return (upgrade.type() ==
                                       LEDGER_UPGRADE_MAX_TX_SET_SIZE);
                           }),
            upgrades.end());
        if (!upgrades.empty())
        {
            break;
        }
    }

    mutateTransactions(transactions);
    return true;
}

void
ApplyTransactionsWork::onReset()
{
    // If ledgerVersion < 11 then we need to support at least mMaxOperations
    // transactions to guarantee we can support mMaxOperations operations no
    // matter how they are distributed (worst case one per transaction).
    //
    // If ledgerVersion >= 11 then we need to support at least mMaxOperations
    // operations.
    //
    // So we can do the same upgrade in both cases.
    LedgerTxn ltx(mApp.getLedgerTxnRoot());
    auto header = ltx.loadHeader();
    header.current().maxTxSetSize =
        std::max(header.current().maxTxSetSize, mMaxOperations);
    if (mUpgradeProtocol)
    {
        header.current().ledgerVersion =
            Config::CURRENT_LEDGER_PROTOCOL_VERSION;
    }
    ltx.commit();

    // Prepare the HistoryArchiveStream
    mStream = std::make_unique<HistoryArchiveStream>(mDownloadDir, mRange,
                                                     mApp.getHistoryManager());
}

BasicWork::State
ApplyTransactionsWork::onRun()
{
    std::vector<TransactionEnvelope> transactions;
    std::vector<TransactionResultPair> results;
    std::vector<UpgradeType> upgrades;
    if (!getNextLedger(transactions, results, upgrades))
    {
        return State::WORK_SUCCESS;
    }

    modifyLedgerBeforeClosing(transactions, results);

    auto& lm = mApp.getLedgerManager();
    auto const& lclHeader = lm.getLastClosedLedgerHeader();
    auto const& header = lclHeader.header;

    StellarValue sv;
    std::shared_ptr<AbstractTxSetFrameForApply> txSet;
    if (mMultiplier == 1 && mMaxOperations == 0)
    {
        TransactionSet txSetXDR;
        txSetXDR.previousLedgerHash = lclHeader.hash;
        txSetXDR.txs.insert(txSetXDR.txs.begin(), transactions.begin(),
                            transactions.end());
        txSet =
            std::make_shared<ExactSimulationTxSetFrame>(mNetworkID, txSetXDR);

        sv.closeTime = mHeaderHistory.header.scpValue.closeTime;
    }
    else
    {
        // When creating SimulationTxSetFrame, we only want to use mMultiplier
        // when generating transactions to handle offer creation (mapping
        // created offer id to a simulated one). When simulating pre-generated
        // transactions, we already have relevant offer ids in transaction
        // results
        txSet = std::make_shared<SimulationTxSetFrame>(
            mNetworkID, lclHeader.hash, transactions, results, mMultiplier);

        sv.closeTime = header.scpValue.closeTime + 1;
    }
    sv.txSetHash = txSet->getContentsHash();
    sv.upgrades.insert(sv.upgrades.begin(), upgrades.begin(), upgrades.end());

    LedgerCloseData closeData(header.ledgerSeq + 1, txSet, sv);
    lm.closeLedger(closeData);
    if (mVerifyResults)
    {
        auto actualResults =
            getTransactionHistoryResults(mApp.getDatabase(), header.ledgerSeq);
        checkResults(mApp, actualResults, results);
    }
    return State::WORK_RUNNING;
}

bool
ApplyTransactionsWork::onAbort()
{
    return true;
}

void
ApplyTransactionsWork::mutateTransactions(
    std::vector<TransactionEnvelope>& transactions)
{
}

void
ApplyTransactionsWork::modifyLedgerBeforeClosing(
    std::vector<TransactionEnvelope> const& transactions,
    std::vector<TransactionResultPair> const& results)
{
}
}
