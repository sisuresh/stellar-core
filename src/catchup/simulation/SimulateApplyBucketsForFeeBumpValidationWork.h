// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "catchup/simulation/SimulateApplyBucketsWork.h"

namespace stellar
{

class SimulateApplyBucketsForFeeBumpValidationWork
    : public SimulateApplyBucketsWork
{
    PublicKey const mKey;

  public:
    SimulateApplyBucketsForFeeBumpValidationWork(
        Application& app, uint32_t multiplier, uint32_t ledger,
        TmpDir const& tmpDir, std::shared_ptr<HistoryArchiveState> has,
        PublicKey const& key);

  protected:
    std::shared_ptr<GenerateBucketsWork> makeGenerateBucketsWork(
        Application& app,
        std::map<std::string, std::shared_ptr<Bucket>>& generatedBuckets,
        HistoryArchiveState const& has, uint32_t multiplier) override;
};
}
