// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "transactions/OperationFrame.h"

namespace stellar
{

class BeginSponsoringFutureReservesOpFrame : public OperationFrame
{
    bool isOpSupported(LedgerHeader const& header) const override;

    BeginSponsoringFutureReservesResult&
    innerResult() const
    {
        return mResult.tr().beginSponsoringFutureReservesResult();
    }
    BeginSponsoringFutureReservesOp const& mBeginSponsoringFutureReservesOp;

    void createSponsorship(AbstractLedgerTxn& ltx) const;
    void createSponsorshipCounter(AbstractLedgerTxn& ltx) const;

  public:
    BeginSponsoringFutureReservesOpFrame(Operation const& op,
                                         OperationResult& res,
                                         TransactionFrame const& parentTx);

    bool doApply(AbstractLedgerTxn& ltx, OperationResult& res) const override;
    bool doCheckValid(uint32_t ledgerVersion,
                      OperationResult& res) const override;

    static BeginSponsoringFutureReservesResultCode
    getInnerCode(OperationResult const& res)
    {
        return res.tr().beginSponsoringFutureReservesResult().code();
    }
};
}
