// Copyright 2015 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "transactions/PathPaymentOpFrameBase.h"

namespace stellar
{

class PathPaymentStrictReceiveOpFrame : public PathPaymentOpFrameBase
{
    PathPaymentStrictReceiveOp const& mPathPayment;

    PathPaymentStrictReceiveResult&
    innerResult() const
    {
        return mResult.tr().pathPaymentStrictReceiveResult();
    }

  public:
    PathPaymentStrictReceiveOpFrame(Operation const& op, OperationResult& res,
                                    TransactionFrame const& parentTx);

    bool doApply(AbstractLedgerTxn& ltx, OperationResult& res) const override;
    bool doCheckValid(uint32_t ledgerVersion,
                      OperationResult& res) const override;

    bool checkTransfer(int64_t maxSend, int64_t amountSend, int64_t maxRecv,
                       int64_t amountRecv) const override;

    Asset const& getSourceAsset() const override;
    Asset const& getDestAsset() const override;
    MuxedAccount const& getDestMuxedAccount() const override;
    xdr::xvector<Asset, 5> const& getPath() const override;

    void setResultSuccess() const override;
    void setResultMalformed() const override;
    void setResultUnderfunded() const override;
    void setResultSourceNoTrust() const override;
    void setResultSourceNotAuthorized() const override;
    void setResultNoDest() const override;
    void setResultDestNoTrust() const override;
    void setResultDestNotAuthorized() const override;
    void setResultLineFull() const override;
    void setResultNoIssuer(Asset const& asset) const override;
    void setResultTooFewOffers() const override;
    void setResultOfferCrossSelf() const override;
    void setResultConstraintNotMet() const override;

    static PathPaymentStrictReceiveResultCode
    getInnerCode(OperationResult const& res)
    {
        return res.tr().pathPaymentStrictReceiveResult().code();
    }
};
}
