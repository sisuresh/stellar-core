// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/simulation/SimulationCreatePassiveSellOfferOpFrame.h"
#include "ledger/LedgerTxn.h"
#include "util/SimulationUtils.h"

namespace stellar
{

SimulationCreatePassiveSellOfferOpFrame::
    SimulationCreatePassiveSellOfferOpFrame(
        Operation const& op, OperationResult& res, TransactionFrame& parentTx,
        OperationResult const& simulationResult, uint32_t count)
    : CreatePassiveSellOfferOpFrame(op, res, parentTx)
    , mSimulationResult(simulationResult)
    , mCount(count)
{
}

int64_t
SimulationCreatePassiveSellOfferOpFrame::generateNewOfferID(
    LedgerTxnHeader& header)
{
    return SimulationUtils::getNewOfferID(mSimulationResult, mCount);
}
}
