#pragma once

// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/OfferExchange.h"
#include "transactions/OperationFrame.h"

namespace stellar
{
class AbstractLedgerTxn;

class PathPaymentOpFrameBase : public OperationFrame
{
  protected:
    bool convert(AbstractLedgerTxn& ltx, int64_t maxOffersToCross,
                 Asset const& sendAsset, int64_t maxSend, int64_t& amountSend,
                 Asset const& recvAsset, int64_t maxRecv, int64_t& amountRecv,
                 RoundingType round, std::vector<ClaimAtom>& offerTrail) const;

    bool shouldBypassIssuerCheck(std::vector<Asset> const& path) const;

    bool updateSourceBalance(AbstractLedgerTxn& ltx, OperationResult& res,
                             int64_t amount, bool bypassIssuerCheck,
                             bool doesSourceAccountExist) const;

    bool updateDestBalance(AbstractLedgerTxn& ltx, int64_t amount,
                           bool bypassIssuerCheck) const;

    bool checkIssuer(AbstractLedgerTxn& ltx, Asset const& asset) const;

  public:
    PathPaymentOpFrameBase(Operation const& op, OperationResult& res,
                           TransactionFrame const& parentTx);

    void
    insertLedgerKeysToPrefetch(UnorderedSet<LedgerKey>& keys) const override;

    bool isDexOperation() const override;

    virtual bool checkTransfer(int64_t maxSend, int64_t amountSend,
                               int64_t maxRecv, int64_t amountRecv) const = 0;

    virtual Asset const& getSourceAsset() const = 0;
    virtual Asset const& getDestAsset() const = 0;
    AccountID getDestID() const;
    virtual MuxedAccount const& getDestMuxedAccount() const = 0;
    virtual xdr::xvector<Asset, 5> const& getPath() const = 0;

    virtual void setResultSuccess() const = 0;
    virtual void setResultMalformed() const = 0;
    virtual void setResultUnderfunded() const = 0;
    virtual void setResultSourceNoTrust() const = 0;
    virtual void setResultSourceNotAuthorized() const = 0;
    virtual void setResultNoDest() const = 0;
    virtual void setResultDestNoTrust() const = 0;
    virtual void setResultDestNotAuthorized() const = 0;
    virtual void setResultLineFull() const = 0;
    virtual void setResultNoIssuer(Asset const& asset) const = 0;
    virtual void setResultTooFewOffers() const = 0;
    virtual void setResultOfferCrossSelf() const = 0;
    virtual void setResultConstraintNotMet() const = 0;
};
}
