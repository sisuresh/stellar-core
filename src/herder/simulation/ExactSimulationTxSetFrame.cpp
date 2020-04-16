// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "herder/simulation/ExactSimulationTxSetFrame.h"

namespace stellar
{

ExactSimulationTxSetFrame::ExactSimulationTxSetFrame(
    Hash const& networkID, TransactionSet const& xdrSet)
    : TxSetFrame(networkID, xdrSet)
{
    for (auto const& env : xdrSet.txs)
    {
        auto tx = TransactionFrameBase::makeTransactionFromWire(networkID, env);
        mApplyOrderTransactions.push_back(tx);
    }
}

std::vector<TransactionFrameBasePtr>
ExactSimulationTxSetFrame::sortForApply()
{
    return mApplyOrderTransactions;
}
}
