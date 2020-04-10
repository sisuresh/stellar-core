// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "catchup/simulation/ApplyTransactionsWork.h"

namespace stellar
{
class MaintainLiabilitiesTestWork : public ApplyTransactionsWork
{
    SecretKey mTestAccount;
    int64_t mTestAccountSeqNum;

    std::map<PublicKey, std::map<PublicKey, std::set<Asset>>>
    getUsedIssuerAssets(TransactionEnvelope& env, AbstractLedgerTxn& ltx) const;

    void getSecretKeyMap(AbstractLedgerTxn& ltx, TransactionEnvelope& env,
                         int n,
                         std::unordered_map<PublicKey, SecretKey>& newKeyMap);
    void getKeysFromOps(TransactionEnvelope& env, uint32_t n,
                        std::unordered_map<PublicKey, SecretKey>& newKeyMap);
    void
    getKeysFromAccount(AccountID const& acc, AbstractLedgerTxn& ltx, uint32_t n,
                       std::unordered_map<PublicKey, SecretKey>& newKeyMap);

  public:
    MaintainLiabilitiesTestWork(Application& app, TmpDir const& downloadDir,
                                LedgerRange const& range,
                                std::string const& networkPassphrase,
                                uint32_t desiredOperations, uint32_t multiplier,
                                bool verifyResults);
    virtual ~MaintainLiabilitiesTestWork() = default;

  protected:
    void processSimulatedTx(AbstractLedgerTxn& ltx,
                            TransactionResultPair const& txResPair,
                            TransactionEnvelope& originalEnv,
                            TransactionEnvelope& simulatedEnv,
                            std::vector<TransactionEnvelope>& transactions,
                            std::vector<TransactionResultPair>& results,
                            uint32_t n) override;

    void onReset() override;
};
}