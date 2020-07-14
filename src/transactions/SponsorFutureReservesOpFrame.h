// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "transactions/OperationFrame.h"

namespace stellar
{

class SponsorFutureReservesOpFrame : public OperationFrame
{
    ThresholdLevel getThresholdLevel() const override;
    bool isVersionSupported(uint32_t protocolVersion) const override;

    SponsorFutureReservesResult&
    innerResult()
    {
        return mResult.tr().sponsorFutureReservesResult();
    }
    SponsorFutureReservesOp const& mSponsorFutureReservesOp;

    void createSponsorship(AbstractLedgerTxn& ltx);
    void createSponsorshipCounter(AbstractLedgerTxn& ltx);

  public:
    SponsorFutureReservesOpFrame(Operation const& op, OperationResult& res,
                                 TransactionFrame& parentTx);

    bool doApply(AbstractLedgerTxn& ltx) override;
    bool doCheckValid(uint32_t ledgerVersion) override;

    static SponsorFutureReservesResultCode
    getInnerCode(OperationResult const& res)
    {
        return res.tr().sponsorFutureReservesResult().code();
    }
};
}
