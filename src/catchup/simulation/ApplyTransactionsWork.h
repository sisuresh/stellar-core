// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "catchup/simulation/HistoryArchiveStream.h"
#include "ledger/LedgerTxn.h"
#include "work/Work.h"
#include "xdr/Stellar-ledger.h"

namespace stellar
{
struct LedgerRange;

class ApplyTransactionsWork : public BasicWork
{
    TmpDir const& mDownloadDir;
    LedgerRange const mRange;

    std::unique_ptr<HistoryArchiveStream> mStream;
    LedgerHeaderHistoryEntry mHeaderHistory;
    TransactionHistoryEntry mTransactionHistory;
    std::vector<TransactionEnvelope>::const_iterator mTransactionIter;
    TransactionHistoryResultEntry mResultHistory;
    std::vector<TransactionResultPair>::const_iterator mResultIter;

    uint32_t const mMaxOperations;
    uint32_t const mMultiplier;
    bool const mVerifyResults;

    bool getNextLedgerFromHistoryArchive();

    bool getNextLedger(std::vector<TransactionEnvelope>& transactions,
                       std::vector<TransactionResultPair>& results,
                       std::vector<UpgradeType>& upgrades);

    uint32_t scaleLedger(std::vector<TransactionEnvelope>& transactions,
                         std::vector<TransactionResultPair>& results,
                         std::vector<UpgradeType>& upgrades, uint32_t n);

    void addSignerKeys(AccountID const& acc, AbstractLedgerTxn& ltx,
                       std::set<SecretKey>& keys,
                       xdr::xvector<DecoratedSignature, 20> const& sigs,
                       uint32_t n);

    void mutateTxSourceAccounts(TransactionEnvelope& env,
                                AbstractLedgerTxn& ltx,
                                std::set<SecretKey>& keys, uint32_t n);
    void mutateOperations(TransactionEnvelope& env, AbstractLedgerTxn& ltx,
                          std::set<SecretKey>& keys, uint32_t n);

  public:
    ApplyTransactionsWork(Application& app, TmpDir const& downloadDir,
                          LedgerRange const& range,
                          std::string const& networkPassphrase,
                          uint32_t desiredOperations, uint32_t multiplier,
                          bool verifyResults);

  protected:
    Hash const mNetworkID;

    virtual void processSimulatedTx(
        AbstractLedgerTxn& ltx, TransactionResultPair const& txResPair,
        TransactionEnvelope& originalEnv, TransactionEnvelope& simulatedEnv,
        std::vector<TransactionEnvelope>& transactions,
        std::vector<TransactionResultPair>& results, uint32_t n);

    void onReset() override;
    State onRun() override;
    bool onAbort() override;
};
}
