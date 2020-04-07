// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "catchup/simulation/ApplyTransactionsWork.h"
#include "crypto/Hex.h"
#include "crypto/SignerKey.h"
#include "herder/LedgerCloseData.h"
#include "herder/simulation/SimulationTxSetFrame.h"
#include "ledger/LedgerManagerImpl.h"
#include "ledger/LedgerRange.h"
#include "ledger/LedgerTxn.h"
#include "transactions/SignatureUtils.h"
#include "transactions/TransactionUtils.h"
#include "util/SimulationUtils.h"
#include "util/format.h"

namespace stellar
{

ApplyTransactionsWork::ApplyTransactionsWork(
    Application& app, TmpDir const& downloadDir, LedgerRange const& range,
    std::string const& networkPassphrase, uint32_t desiredOperations,
    uint32_t multiplier, bool verifyResults)
    : BasicWork(app, "apply-transactions", RETRY_NEVER)
    , mDownloadDir(downloadDir)
    , mRange(range)
    , mNetworkID(sha256(networkPassphrase))
    , mTransactionHistory{}
    , mTransactionIter(mTransactionHistory.txSet.txs.cend())
    , mResultHistory{}
    , mResultIter(mResultHistory.txResultSet.results.cend())
    , mMaxOperations(desiredOperations)
    , mMultiplier(multiplier)
    , mVerifyResults(verifyResults)
{
    if (mMultiplier == 0)
    {
        throw std::runtime_error("Invalid multiplier!");
    }
}

static void
checkResults(Application& app, uint32_t ledger,
             std::vector<TransactionResultPair> const& results)
{
    auto resSet = TransactionFrame::getTransactionHistoryResults(
        app.getDatabase(), ledger);

    assert(resSet.results.size() == results.size());
    for (size_t i = 0; i < results.size(); i++)
    {
        auto const& res = results[i];
        assert(res.transactionHash == resSet.results[i].transactionHash);

        auto const& dbRes = resSet.results[i].result;
        if (dbRes.result.code() != res.result.result.code())
        {
            auto msg = fmt::format(
                "Expected result code {} does not agree with {} for tx {}",
                res.result.result.code(), dbRes.result.code(),
                binToHex(res.transactionHash));
            CLOG(ERROR, "History") << msg;
        }
    }
}

bool
ApplyTransactionsWork::hasSig(PublicKey const& account,
                              TransactionEnvelope const& oldEnvelope,
                              Hash const& hash)
{
    // Is the signature of this account present in the envelope we're
    // simulating?
    if (!mUsedPubKeys.insert(account).second)
    {
        return false;
    }

    for (auto const& sig : oldEnvelope.signatures)
    {
        if (PubKeyUtils::verifySig(account, sig.signature, hash))
        {
            return true;
        }
    }
    return false;
}

void
ApplyTransactionsWork::addSignerKeys(AccountID const& acc,
                                     AbstractLedgerTxn& ltx,
                                     std::vector<SecretKey>& keys,
                                     TransactionEnvelope const& oldEnvelope,
                                     uint32_t n)
{
    auto opKey = SimulationUtils::getNewSecret(acc, n);
    auto const& txHash = mResultIter->transactionHash;

    if (hasSig(acc, oldEnvelope, txHash))
    {
        keys.emplace_back(opKey);
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
            if (hasSig(pubKey, oldEnvelope, txHash))
            {
                keys.emplace_back(SimulationUtils::getNewSecret(pubKey, n));
            }
        }
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

    LedgerTxn ltx(mApp.getLedgerTxnRoot());

    // No amplification needed, simply return existing transactions and results
    if (n == 0)
    {
        transactions.emplace_back(*mTransactionIter);
        results.emplace_back(*mResultIter);
        return mTransactionIter->v0().tx.operations.size();
    }

    auto const& env = *mTransactionIter;
    TransactionEnvelope newEnv = env;
    Transaction& newTx = newEnv.tx;

    // Keep track of accounts that need to sign
    std::vector<SecretKey> keys;
    newEnv.signatures.clear();

    addSignerKeys(env.tx.sourceAccount, ltx, keys, env, n);
    SimulationUtils::updateAccountID(newTx.sourceAccount, n);

    for (auto& op : newTx.operations)
    {
        // Add signer keys where needed before simulating the operation
        if (op.sourceAccount)
        {
            addSignerKeys(*op.sourceAccount, ltx, keys, env, n);
        }

        // Prior to protocol 10, it is possible that we might have just added a
        // signer; check if their signature is required
        if (op.body.type() == SET_OPTIONS && op.body.setOptionsOp().signer)
        {
            auto signer = *op.body.setOptionsOp().signer;
            if (signer.key.type() == SIGNER_KEY_TYPE_ED25519)
            {
                auto signerKey = KeyUtils::convertKey<PublicKey>(signer.key);
                if (hasSig(signerKey, env, mResultIter->transactionHash))
                {
                    keys.emplace_back(
                        SimulationUtils::getNewSecret(signerKey, n));
                }
            }
        }
        SimulationUtils::updateOperation(op, n);
    }

    // Add proper signatures, and setup results (for later verification)
    auto newTxHash = sha256(
        xdr::xdr_to_opaque(mApp.getNetworkID(), ENVELOPE_TYPE_TX, newTx));
    for (auto const& k : keys)
    {
        newEnv.signatures.push_back(SignatureUtils::sign(k, newTxHash));
    }

    // In most cases, number of signatures should be equal. Simulation does
    // not handle pre-signed transactions, so it is possible that we have
    // less signatures
    assert(newEnv.signatures.size() <= env.signatures.size());

    // These are not exactly accurate, but sufficient to check result codes
    auto newRes = *mResultIter;
    newRes.transactionHash = newTxHash;

    results.emplace_back(newRes);
    transactions.emplace_back(newEnv);

    // Reset pubkeys we remembered as the transaction has been processed
    mUsedPubKeys.clear();

    return newTx.operations.size();
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
            auto hash = sha256(xdr::xdr_to_opaque(mApp.getNetworkID(),
                                                  ENVELOPE_TYPE_TX, tx.tx));
            transactions[hash] = tx;
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
            return true;
        }

        if (!getNextLedgerFromHistoryArchive())
        {
            return true;
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
            return true;
        }
    }
}

void
ApplyTransactionsWork::onReset()
{
    // Upgrade max transaction set size if necessary
    auto& lm = mApp.getLedgerManager();
    auto const& lclHeader = lm.getLastClosedLedgerHeader();
    auto const& header = lclHeader.header;

    // If ledgerVersion < 11 then we need to support at least mMaxOperations
    // transactions to guarantee we can support mMaxOperations operations no
    // matter how they are distributed (worst case one per transaction).
    //
    // If ledgerVersion >= 11 then we need to support at least mMaxOperations
    // operations.
    //
    // So we can do the same upgrade in both cases.
    if (header.maxTxSetSize < mMaxOperations)
    {
        LedgerUpgrade upgrade(LEDGER_UPGRADE_MAX_TX_SET_SIZE);
        upgrade.newMaxTxSetSize() = mMaxOperations;
        auto opaqueUpgrade = xdr::xdr_to_opaque(upgrade);

        TransactionSet txSetXDR;
        txSetXDR.previousLedgerHash = lclHeader.hash;
        auto txSet = std::make_shared<TxSetFrame>(mNetworkID, txSetXDR);

        StellarValue sv;
        sv.txSetHash = txSet->getContentsHash();
        sv.closeTime = header.scpValue.closeTime + 1;
        sv.upgrades.emplace_back(opaqueUpgrade.begin(), opaqueUpgrade.end());

        LedgerCloseData closeData(header.ledgerSeq + 1, txSet, sv);
        lm.closeLedger(closeData);
    }

    // Prepare the HistoryArchiveStream
    mStream = std::make_unique<HistoryArchiveStream>(mDownloadDir, mRange,
                                                     mApp.getHistoryManager());
    mUsedPubKeys.clear();
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

    auto& lm = mApp.getLedgerManager();
    auto const& lclHeader = lm.getLastClosedLedgerHeader();
    auto const& header = lclHeader.header;

    // When creating SimulationTxSetFrame, we only want to use mMultiplier when
    // generating transactions to handle offer creation (mapping created offer
    // id to a simulated one). When simulating pre-generated transactions, we
    // already have relevant offer ids in transaction results
    auto txSet = std::make_shared<SimulationTxSetFrame>(
        mNetworkID, lclHeader.hash, transactions, results, mMultiplier);

    StellarValue sv;
    sv.txSetHash = txSet->getContentsHash();
    sv.closeTime = header.scpValue.closeTime + 1;
    sv.upgrades.insert(sv.upgrades.begin(), upgrades.begin(), upgrades.end());

    LedgerCloseData closeData(header.ledgerSeq + 1, txSet, sv);
    lm.closeLedger(closeData);
    if (mVerifyResults)
    {
        checkResults(mApp, header.ledgerSeq, results);
    }
    return State::WORK_RUNNING;
}

bool
ApplyTransactionsWork::onAbort()
{
    return true;
}
}
