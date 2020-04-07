// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/FeeBumpTransactionFrame.h"
#include "transactions/simulation/SimulationTransactionFrame.h"

namespace stellar
{
class SimulationFeeBumpTransactionFrame : public FeeBumpTransactionFrame
{
    TransactionResult mSimulationResult;
    std::shared_ptr<SimulationTransactionFrame> mSimulationInnerTx;

  public:
    SimulationFeeBumpTransactionFrame(Hash const& networkID,
                                      TransactionEnvelope const& envelope,
                                      TransactionResult simulationResult,
                                      uint32_t count);
    SimulationFeeBumpTransactionFrame(TransactionFrame const&) = delete;
    SimulationFeeBumpTransactionFrame() = delete;

    virtual ~SimulationFeeBumpTransactionFrame()
    {
    }

    int64_t getFee(LedgerHeader const& header, int64_t baseFee) const override;
    void processFeeSeqNum(AbstractLedgerTxn& ltx, int64_t baseFee) override;

    TransactionFramePtr getInnerTx() const override;
};
}
