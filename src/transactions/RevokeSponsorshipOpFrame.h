// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "transactions/OperationFrame.h"

namespace stellar
{
enum class SponsorshipResult;

class RevokeSponsorshipOpFrame : public OperationFrame
{
    bool isOpSupported(LedgerHeader const& header) const override;

    RevokeSponsorshipResult&
    innerResult() const
    {
        return mResult.tr().revokeSponsorshipResult();
    }
    RevokeSponsorshipOp const& mRevokeSponsorshipOp;

    bool processSponsorshipResult(SponsorshipResult sr) const;

    bool updateLedgerEntrySponsorship(AbstractLedgerTxn& ltx) const;
    bool updateSignerSponsorship(AbstractLedgerTxn& ltx) const;

    bool tryRemoveEntrySponsorship(AbstractLedgerTxn& ltx,
                                   LedgerTxnHeader const& header,
                                   LedgerEntry& le, LedgerEntry& sponsoringAcc,
                                   LedgerEntry& sponsoredAcc) const;
    bool tryEstablishEntrySponsorship(AbstractLedgerTxn& ltx,
                                      LedgerTxnHeader const& header,
                                      LedgerEntry& le,
                                      LedgerEntry& sponsoringAcc,
                                      LedgerEntry& sponsoredAcc) const;

  public:
    RevokeSponsorshipOpFrame(Operation const& op, OperationResult& res,
                             TransactionFrame const& parentTx);

    bool doApply(AbstractLedgerTxn& ltx, OperationResult& res) const override;
    bool doCheckValid(uint32_t ledgerVersion,
                      OperationResult& res) const override;

    static RevokeSponsorshipResultCode
    getInnerCode(OperationResult const& res)
    {
        return res.tr().revokeSponsorshipResult().code();
    }
};
}
