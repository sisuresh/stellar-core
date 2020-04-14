// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "catchup/simulation/ApplyTransactionsWork.h"

namespace stellar
{

class ApplyTransactionsForFeeBumpValidationWork : public ApplyTransactionsWork
{
    Application& mApp;
    Hash const mNetworkID;
    SecretKey const mKey;

    void sign(FeeBumpTransactionEnvelope& env);

  public:
    ApplyTransactionsForFeeBumpValidationWork(
        Application& app, TmpDir const& downloadDir, LedgerRange const& range,
        std::string const& networkPassphrase, uint32_t desiredOperations,
        bool upgrade, uint32_t multiplier, bool verifyResults,
        SecretKey const& key);

  protected:
    void
    mutateTransactions(std::vector<TransactionEnvelope>& transactions) override;

    void modifyLedgerBeforeClosing(
        std::vector<TransactionEnvelope> const& transactions,
        std::vector<TransactionResultPair> const& results) override;

    void checkResults(Application& app,
                      TransactionResultSet const& actualResults,
                      std::vector<TransactionResultPair> const& expectedResults)
        const override;
};
}
