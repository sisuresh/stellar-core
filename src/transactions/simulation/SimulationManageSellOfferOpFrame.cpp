// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/simulation/SimulationManageSellOfferOpFrame.h"
#include "ledger/LedgerTxn.h"
#include "util/SimulationUtils.h"

namespace stellar
{
SimulationManageSellOfferOpFrame::SimulationManageSellOfferOpFrame(
    Operation const& op, OperationResult& res, TransactionFrame& parentTx,
    OperationResult const& simulationResult, uint32_t count)
    : ManageSellOfferOpFrame(op, res, parentTx)
    , mSimulationResult(simulationResult)
    , mCount(count)
{
}

int64_t
SimulationManageSellOfferOpFrame::getOfferID(LedgerTxnHeader& header)
{
    return SimulationUtils::getNewOfferID(mSimulationResult, mCount);
}
}
