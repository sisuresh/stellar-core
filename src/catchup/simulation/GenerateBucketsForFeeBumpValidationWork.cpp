// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "catchup/simulation/GenerateBucketsForFeeBumpValidationWork.h"
#include "bucket/BucketList.h"
#include "ledger/LedgerManager.h"

namespace stellar
{

GenerateBucketsForFeeBumpValidationWork::
    GenerateBucketsForFeeBumpValidationWork(
        Application& app,
        std::map<std::string, std::shared_ptr<Bucket>>& buckets,
        HistoryArchiveState const& applyState, uint32_t multiplier,
        PublicKey const& key)
    : GenerateBucketsWork(app, buckets, applyState, multiplier), mKey(key)
{
}

std::vector<LedgerEntry>
GenerateBucketsForFeeBumpValidationWork::getEntriesToInject(uint32_t level,
                                                            bool isCurr)
{
    if (level == BucketList::kNumLevels - 1 && isCurr)
    {
        LedgerEntry le;
        le.data.type(ACCOUNT);
        auto& acc = le.data.account();

        le.lastModifiedLedgerSeq = 1;
        acc.accountID = mKey;
        acc.balance = LedgerManager::GENESIS_LEDGER_TOTAL_COINS;
        acc.seqNum = 1;
        acc.thresholds[0] = 1;

        return {le};
    }
    return {};
}
}
