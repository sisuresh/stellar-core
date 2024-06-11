#pragma once

// Copyright 2015 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/ManageOfferOpFrameBase.h"

namespace stellar
{

class AbstractLedgerTxn;

class ManageSellOfferOpFrame : public ManageOfferOpFrameBase
{
    ManageSellOfferOp const& mManageSellOffer;

    ManageSellOfferResult&
    innerResult() const
    {
        return mResult.tr().manageSellOfferResult();
    }

  public:
    ManageSellOfferOpFrame(Operation const& op, OperationResult& res,
                           TransactionFrame const& parentTx);
    ManageSellOfferOpFrame(Operation const& op, OperationResult& res,
                           TransactionFrame const& parentTx, bool passive);

    bool isAmountValid() const override;
    bool isDeleteOffer() const override;

    int64_t getOfferBuyingLiabilities() const override;
    int64_t getOfferSellingLiabilities() const override;

    void applyOperationSpecificLimits(int64_t& maxSheepSend, int64_t sheepSent,
                                      int64_t& maxWheatReceive,
                                      int64_t wheatReceived) const override;
    void
    getExchangeParametersBeforeV10(int64_t& maxSheepSend,
                                   int64_t& maxWheatReceive) const override;

    ManageOfferSuccessResult& getSuccessResult() const override;

    void setResultSuccess() const override;
    void setResultMalformed() const override;
    void setResultSellNoTrust() const override;
    void setResultBuyNoTrust() const override;
    void setResultSellNotAuthorized() const override;
    void setResultBuyNotAuthorized() const override;
    void setResultLineFull() const override;
    void setResultUnderfunded() const override;
    void setResultCrossSelf() const override;
    void setResultSellNoIssuer() const override;
    void setResultBuyNoIssuer() const override;
    void setResultNotFound() const override;
    void setResultLowReserve() const override;

    static ManageSellOfferResultCode
    getInnerCode(OperationResult const& res)
    {
        return res.tr().manageSellOfferResult().code();
    }
};
}
