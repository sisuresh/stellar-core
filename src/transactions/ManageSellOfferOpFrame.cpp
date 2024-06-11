// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "util/asio.h"
#include "transactions/ManageSellOfferOpFrame.h"
#include "OfferExchange.h"
#include "database/Database.h"
#include "ledger/LedgerTxn.h"
#include "ledger/LedgerTxnEntry.h"
#include "ledger/LedgerTxnHeader.h"
#include "ledger/TrustLineWrapper.h"
#include "main/Application.h"
#include "transactions/TransactionUtils.h"
#include "util/Logging.h"
#include "util/XDROperators.h"
#include "util/types.h"

// convert from sheep to wheat
// selling sheep
// buying wheat

namespace stellar
{

using namespace std;

ManageSellOfferOpFrame::ManageSellOfferOpFrame(Operation const& op,
                                               OperationResult& res,
                                               TransactionFrame const& parentTx)
    : ManageSellOfferOpFrame(op, res, parentTx, false)
{
}

ManageSellOfferOpFrame::ManageSellOfferOpFrame(Operation const& op,
                                               OperationResult& res,
                                               TransactionFrame const& parentTx,
                                               bool passive)
    : ManageOfferOpFrameBase(op, res, parentTx,
                             op.body.manageSellOfferOp().selling,
                             op.body.manageSellOfferOp().buying,
                             op.body.manageSellOfferOp().offerID,
                             op.body.manageSellOfferOp().price, passive)
    , mManageSellOffer(mOperation.body.manageSellOfferOp())
{
}

bool
ManageSellOfferOpFrame::isAmountValid() const
{
    return mManageSellOffer.amount >= 0;
}

bool
ManageSellOfferOpFrame::isDeleteOffer() const
{
    return mManageSellOffer.amount == 0;
}

int64_t
ManageSellOfferOpFrame::getOfferBuyingLiabilities() const
{
    auto res = exchangeV10WithoutPriceErrorThresholds(
        mManageSellOffer.price, mManageSellOffer.amount, INT64_MAX, INT64_MAX,
        INT64_MAX, RoundingType::NORMAL);
    return res.numSheepSend;
}

int64_t
ManageSellOfferOpFrame::getOfferSellingLiabilities() const
{
    auto res = exchangeV10WithoutPriceErrorThresholds(
        mManageSellOffer.price, mManageSellOffer.amount, INT64_MAX, INT64_MAX,
        INT64_MAX, RoundingType::NORMAL);
    return res.numWheatReceived;
}

void
ManageSellOfferOpFrame::applyOperationSpecificLimits(
    int64_t& maxSheepSend, int64_t sheepSent, int64_t& maxWheatReceive,
    int64_t wheatReceived) const
{
    maxSheepSend = std::min(mManageSellOffer.amount - sheepSent, maxSheepSend);
}

void
ManageSellOfferOpFrame::getExchangeParametersBeforeV10(
    int64_t& maxSheepSend, int64_t& maxWheatReceive) const
{
    int64_t maxSheepBasedOnWheat;
    if (!bigDivide(maxSheepBasedOnWheat, maxWheatReceive,
                   mManageSellOffer.price.d, mManageSellOffer.price.n,
                   ROUND_DOWN))
    {
        maxSheepBasedOnWheat = INT64_MAX;
    }

    maxSheepSend =
        std::min({maxSheepSend, maxSheepBasedOnWheat, mManageSellOffer.amount});
}

ManageOfferSuccessResult&
ManageSellOfferOpFrame::getSuccessResult() const
{
    return mResult.tr().manageSellOfferResult().success();
}

void
ManageSellOfferOpFrame::setResultSuccess() const
{
    mResult.tr().manageSellOfferResult().code(MANAGE_SELL_OFFER_SUCCESS);
}

void
ManageSellOfferOpFrame::setResultMalformed() const
{
    mResult.tr().manageSellOfferResult().code(MANAGE_SELL_OFFER_MALFORMED);
}

void
ManageSellOfferOpFrame::setResultSellNoTrust() const
{
    mResult.tr().manageSellOfferResult().code(MANAGE_SELL_OFFER_SELL_NO_TRUST);
}

void
ManageSellOfferOpFrame::setResultBuyNoTrust() const
{
    mResult.tr().manageSellOfferResult().code(MANAGE_SELL_OFFER_BUY_NO_TRUST);
}

void
ManageSellOfferOpFrame::setResultSellNotAuthorized() const
{
    mResult.tr().manageSellOfferResult().code(
        MANAGE_SELL_OFFER_SELL_NOT_AUTHORIZED);
}

void
ManageSellOfferOpFrame::setResultBuyNotAuthorized() const
{
    mResult.tr().manageSellOfferResult().code(
        MANAGE_SELL_OFFER_BUY_NOT_AUTHORIZED);
}

void
ManageSellOfferOpFrame::setResultLineFull() const
{
    mResult.tr().manageSellOfferResult().code(MANAGE_SELL_OFFER_LINE_FULL);
}

void
ManageSellOfferOpFrame::setResultUnderfunded() const
{
    mResult.tr().manageSellOfferResult().code(MANAGE_SELL_OFFER_UNDERFUNDED);
}

void
ManageSellOfferOpFrame::setResultCrossSelf() const
{
    mResult.tr().manageSellOfferResult().code(MANAGE_SELL_OFFER_CROSS_SELF);
}

void
ManageSellOfferOpFrame::setResultSellNoIssuer() const
{
    mResult.tr().manageSellOfferResult().code(MANAGE_SELL_OFFER_SELL_NO_ISSUER);
}

void
ManageSellOfferOpFrame::setResultBuyNoIssuer() const
{
    mResult.tr().manageSellOfferResult().code(MANAGE_SELL_OFFER_BUY_NO_ISSUER);
}

void
ManageSellOfferOpFrame::setResultNotFound() const
{
    mResult.tr().manageSellOfferResult().code(MANAGE_SELL_OFFER_NOT_FOUND);
}

void
ManageSellOfferOpFrame::setResultLowReserve() const
{
    mResult.tr().manageSellOfferResult().code(MANAGE_SELL_OFFER_LOW_RESERVE);
}
}
