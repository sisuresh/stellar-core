// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "herder/TxSetFrame.h"

namespace stellar
{

class ExactSimulationTxSetFrame : public TxSetFrame
{
    std::vector<TransactionFrameBasePtr> mApplyOrderTransactions;

  public:
    ExactSimulationTxSetFrame(Hash const& networkID,
                              TransactionSet const& xdrSet);

    std::vector<TransactionFrameBasePtr> sortForApply() override;
};
}
