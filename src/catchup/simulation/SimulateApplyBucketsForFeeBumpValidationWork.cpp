// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "catchup/simulation/SimulateApplyBucketsForFeeBumpValidationWork.h"
#include "catchup/simulation/GenerateBucketsForFeeBumpValidationWork.h"

namespace stellar
{
SimulateApplyBucketsForFeeBumpValidationWork::
    SimulateApplyBucketsForFeeBumpValidationWork(
        Application& app, uint32_t multiplier, uint32_t ledger,
        TmpDir const& tmpDir, std::shared_ptr<HistoryArchiveState> has,
        PublicKey const& key)
    : SimulateApplyBucketsWork(app, multiplier, ledger, tmpDir), mKey(key)
{
}

std::shared_ptr<GenerateBucketsWork>
SimulateApplyBucketsForFeeBumpValidationWork::makeGenerateBucketsWork(
    Application& app,
    std::map<std::string, std::shared_ptr<Bucket>>& generatedBuckets,
    HistoryArchiveState const& has, uint32_t multiplier)
{
    return std::make_shared<GenerateBucketsForFeeBumpValidationWork>(
        app, generatedBuckets, has, multiplier, mKey);
}
}
