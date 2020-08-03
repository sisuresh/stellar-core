// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "transactions/OperationFrame.h"

namespace stellar
{

class ConfirmAndClearSponsorOpFrame : public OperationFrame
{
    bool isVersionSupported(uint32_t protocolVersion) const override;

    ConfirmAndClearSponsorResult&
    innerResult()
    {
        return mResult.tr().confirmAndClearSponsorResult();
    }

  public:
    ConfirmAndClearSponsorOpFrame(Operation const& op, OperationResult& res,
                                  TransactionFrame& parentTx);

    bool doApply(AbstractLedgerTxn& ltx) override;
    bool doCheckValid(uint32_t ledgerVersion) override;

    static ConfirmAndClearSponsorResultCode
    getInnerCode(OperationResult const& res)
    {
        return res.tr().confirmAndClearSponsorResult().code();
    }
};
}
