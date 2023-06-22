#pragma once

// Copyright 2023 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#ifdef ENABLE_NEXT_PROTOCOL_VERSION_UNSAFE_FOR_PRODUCTION
#include "transactions/OperationFrame.h"
#include "xdr/Stellar-transaction.h"

namespace stellar
{
class AbstractLedgerTxn;

class RestoreFootprintOpFrame : public OperationFrame
{
    RestoreFootprintResult&
    innerResult()
    {
        return mResult.tr().restoreFootprintResult();
    }

    RestoreFootprintOp const& mRestoreFootprintOp;

  public:
    RestoreFootprintOpFrame(Operation const& op, OperationResult& res,
                            TransactionFrame& parentTx);

    bool isOpSupported(LedgerHeader const& header) const override;

    bool doApply(AbstractLedgerTxn& ltx) override;
    bool doApply(Application& app, AbstractLedgerTxn& ltx,
                 Hash const& sorobanBasePrngSeed) override;

    bool doCheckValid(SorobanNetworkConfig const& config,
                      uint32_t ledgerVersion) override;
    bool doCheckValid(uint32_t ledgerVersion) override;

    void
    insertLedgerKeysToPrefetch(UnorderedSet<LedgerKey>& keys) const override;

    static RestoreFootprintResultCode
    getInnerCode(OperationResult const& res)
    {
        return res.tr().restoreFootprintResult().code();
    }

    virtual bool isSoroban() const override;
};
}
#endif // ENABLE_NEXT_PROTOCOL_VERSION_UNSAFE_FOR_PRODUCTION
