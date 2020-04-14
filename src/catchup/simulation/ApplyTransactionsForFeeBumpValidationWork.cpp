// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "catchup/simulation/ApplyTransactionsForFeeBumpValidationWork.h"
#include "transactions/SignatureUtils.h"
#include "transactions/TransactionBridge.h"
#include "transactions/TransactionUtils.h"

namespace stellar
{

ApplyTransactionsForFeeBumpValidationWork::
    ApplyTransactionsForFeeBumpValidationWork(
        Application& app, TmpDir const& downloadDir, LedgerRange const& range,
        std::string const& networkPassphrase, uint32_t desiredOperations,
        bool upgrade, uint32_t multiplier, bool verifyResults,
        SecretKey const& key)
    : ApplyTransactionsWork(app, downloadDir, range, networkPassphrase,
                            desiredOperations, upgrade, multiplier,
                            verifyResults)
    , mApp(app)
    , mNetworkID(sha256(networkPassphrase))
    , mKey(key)
{
}

void
ApplyTransactionsForFeeBumpValidationWork::sign(FeeBumpTransactionEnvelope& env)
{
    env.signatures.emplace_back(SignatureUtils::sign(
        mKey, sha256(xdr::xdr_to_opaque(mNetworkID, ENVELOPE_TYPE_TX_FEE_BUMP,
                                        env.tx))));
}

void
ApplyTransactionsForFeeBumpValidationWork::mutateTransactions(
    std::vector<TransactionEnvelope>& transactions)
{
    for (auto& tx : transactions)
    {
        TransactionEnvelope fb(ENVELOPE_TYPE_TX_FEE_BUMP);
        fb.feeBump().tx.innerTx.type(ENVELOPE_TYPE_TX);
        fb.feeBump().tx.innerTx.v1() = txbridge::convertForV13(tx).v1();
        fb.feeBump().tx.feeSource = mKey.getPublicKey();
        fb.feeBump().tx.fee = 2 * fb.feeBump().tx.innerTx.v1().tx.fee;
        sign(fb.feeBump());
        tx = fb;
    }
}

void
ApplyTransactionsForFeeBumpValidationWork::modifyLedgerBeforeClosing(
    std::vector<TransactionEnvelope> const& transactions,
    std::vector<TransactionResultPair> const& results)
{
    LedgerTxn ltx(mApp.getLedgerTxnRoot());

    assert(transactions.size() == results.size());
    for (size_t i = 0; i < results.size(); ++i)
    {
        auto& tx = transactions[i].feeBump().tx.innerTx.v1().tx;
        auto acc = stellar::loadAccount(ltx, tx.sourceAccount);
        assert(acc);
        acc.current().data.account().balance -= results[i].result.feeCharged;
    }

    ltx.commit();
}

void
ApplyTransactionsForFeeBumpValidationWork::checkResults(
    Application& app, TransactionResultSet const& actualResults,
    std::vector<TransactionResultPair> const& expectedResults) const
{
    TransactionResultSet innerResults;
    innerResults.results.reserve(actualResults.results.size());
    for (auto const& result : actualResults.results)
    {
        TransactionResultPair trp;
        xdr::xdr_from_opaque(
            xdr::xdr_to_opaque(result.result.result.innerResultPair()), trp);
        innerResults.results.emplace_back(trp);
    }
    ApplyTransactionsWork::checkResults(app, innerResults, expectedResults);
}
}
