// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "catchup/simulation/GenerateBucketsWork.h"

namespace stellar
{

class Bucket;
class TmpDir;

class GenerateBucketsWithNewAccountWork : public GenerateBucketsWork
{
  public:
    static uint256 const TEST_ACCOUNT_SEED;

    GenerateBucketsWithNewAccountWork(
        Application& app,
        std::map<std::string, std::shared_ptr<Bucket>>& buckets,
        HistoryArchiveState const& applyState, uint32_t multiplier);
    virtual ~GenerateBucketsWithNewAccountWork() = default;

  protected:
    std::vector<LedgerEntry> entriesToInject() override;
};
}