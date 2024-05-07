#pragma once

// Copyright 2023 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/OperationFrame.h"
#include "xdr/Stellar-transaction.h"

namespace stellar
{
class AbstractLedgerTxn;

class ExtendFootprintTTLOpFrame : public OperationFrame
{
    ExtendFootprintTTLResult&
    innerResult()
    {
        return mResult.tr().extendFootprintTTLResult();
    }

    ExtendFootprintTTLOp const& mExtendFootprintTTLOp;

  public:
    ExtendFootprintTTLOpFrame(Operation const& op, OperationResult& res,
                              TransactionFrame& parentTx);

    bool isOpSupported(LedgerHeader const& header) const override;

    bool doApply(AbstractLedgerTxn& ltx) override;
    bool doApply(Application& app, AbstractLedgerTxn& ltx,
                 Hash const& sorobanBasePrngSeed,
                 TransactionResultPayload& resPayload) override;

    bool doCheckValid(SorobanNetworkConfig const& networkConfig,
                      Config const& appConfig, uint32_t ledgerVersion,
                      TransactionResultPayload& resPayload) override;
    bool doCheckValid(uint32_t ledgerVersion) override;

    void
    insertLedgerKeysToPrefetch(UnorderedSet<LedgerKey>& keys) const override;

    static ExtendFootprintTTLResultCode
    getInnerCode(OperationResult const& res)
    {
        return res.tr().extendFootprintTTLResult().code();
    }

    virtual bool isSoroban() const override;

    ThresholdLevel getThresholdLevel() const override;
};
}
