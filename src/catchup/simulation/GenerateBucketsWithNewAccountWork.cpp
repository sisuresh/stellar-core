// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "catchup/simulation/GenerateBucketsWithNewAccountWork.h"
#include "crypto/SHA.h"

namespace stellar
{

uint256 const GenerateBucketsWithNewAccountWork::TEST_ACCOUNT_SEED(
    sha256("TEST_ACCOUNT_SEED"));

GenerateBucketsWithNewAccountWork::GenerateBucketsWithNewAccountWork(
    Application& app, std::map<std::string, std::shared_ptr<Bucket>>& buckets,
    HistoryArchiveState const& applyState, uint32_t multiplier)
    : GenerateBucketsWork(app, buckets, applyState, multiplier)
{
}

std::vector<LedgerEntry>
GenerateBucketsWithNewAccountWork::entriesToInject()
{
    LedgerEntry newAccountEntry;
    newAccountEntry.data.type(ACCOUNT);
    auto& newAccount = newAccountEntry.data.account();
    newAccount.thresholds[0] = 1;
    newAccount.accountID =
        SecretKey::fromSeed(TEST_ACCOUNT_SEED).getPublicKey();
    newAccount.seqNum = 1;
    newAccount.balance = 1000000000;

    return {newAccountEntry};
}
}
