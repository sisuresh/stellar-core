// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "catchup/simulation/GenerateBucketsWork.h"

namespace stellar
{

class GenerateBucketsForFeeBumpValidationWork : public GenerateBucketsWork
{
    PublicKey const mKey;

  public:
    GenerateBucketsForFeeBumpValidationWork(
        Application& app,
        std::map<std::string, std::shared_ptr<Bucket>>& buckets,
        HistoryArchiveState const& applyState, uint32_t multiplier,
        PublicKey const& key);

  protected:
    std::vector<LedgerEntry> getEntriesToInject(uint32_t level,
                                                bool isCurr) override;
};
}
