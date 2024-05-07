// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/FeeBumpTransactionFrame.h"
#include "crypto/Hex.h"
#include "crypto/SHA.h"
#include "crypto/SignerKey.h"
#include "crypto/SignerKeyUtils.h"
#include "ledger/LedgerManager.h"
#include "ledger/LedgerTxn.h"
#include "ledger/LedgerTxnEntry.h"
#include "ledger/LedgerTxnHeader.h"
#include "main/Application.h"
#include "transactions/SignatureChecker.h"
#include "transactions/SignatureUtils.h"
#include "transactions/SponsorshipUtils.h"
#include "transactions/TransactionMetaFrame.h"
#include "transactions/TransactionResultPayload.h"
#include "transactions/TransactionUtils.h"
#include "util/GlobalChecks.h"
#include "util/ProtocolVersion.h"
#include "util/numeric128.h"
#include "xdrpp/marshal.h"

#include <numeric>

namespace stellar
{

TransactionEnvelope
FeeBumpTransactionFrame::convertInnerTxToV1(TransactionEnvelope const& envelope)
{
    TransactionEnvelope e(ENVELOPE_TYPE_TX);
    e.v1() = envelope.feeBump().tx.innerTx.v1();
    return e;
}

bool
FeeBumpTransactionFrame::hasDexOperations() const
{
    return mInnerTx->hasDexOperations();
}

bool
FeeBumpTransactionFrame::isSoroban() const
{
    return mInnerTx->isSoroban();
}

SorobanResources const&
FeeBumpTransactionFrame::sorobanResources() const
{
    return mInnerTx->sorobanResources();
}

FeeBumpTransactionFrame::FeeBumpTransactionFrame(
    Hash const& networkID, TransactionEnvelope const& envelope)
    : mEnvelope(envelope)
    , mInnerTx(std::make_shared<TransactionFrame>(networkID,
                                                  convertInnerTxToV1(envelope)))
    , mNetworkID(networkID)
{
}

#ifdef BUILD_TESTS
FeeBumpTransactionFrame::FeeBumpTransactionFrame(
    Hash const& networkID, TransactionEnvelope const& envelope,
    TransactionFramePtr innerTx)
    : mEnvelope(envelope), mInnerTx(innerTx), mNetworkID(networkID)
{
}
#endif

static void
updateResult(TransactionResultPayload& resPayload,
             TransactionFrameBasePtr innerTx)
{
    releaseAssert(resPayload.isFeeBump());
    if (resPayload.getInnerResult().result.code() == txSUCCESS)
    {
        resPayload.getResult().result.code(txFEE_BUMP_INNER_SUCCESS);
    }
    else
    {
        resPayload.getResult().result.code(txFEE_BUMP_INNER_FAILED);
    }

    auto& irp = resPayload.getResult().result.innerResultPair();
    irp.transactionHash = innerTx->getContentsHash();

    auto const& res = resPayload.getInnerResult();
    auto& innerRes = irp.result;
    innerRes.feeCharged = res.feeCharged;
    innerRes.result.code(res.result.code());
    switch (innerRes.result.code())
    {
    case txSUCCESS:
    case txFAILED:
        innerRes.result.results() = res.result.results();
        break;
    default:
        break;
    }
}

bool
FeeBumpTransactionFrame::apply(Application& app, AbstractLedgerTxn& ltx,
                               TransactionMetaFrame& meta,
                               TransactionResultPayload& resPayload,
                               Hash const& sorobanBasePrngSeed) const
{
    try
    {
        LedgerTxn ltxTx(ltx);
        removeOneTimeSignerKeyFromFeeSource(ltxTx);
        meta.pushTxChangesBefore(ltxTx.getChanges());
        ltxTx.commit();
    }
    catch (std::exception& e)
    {
        printErrorAndAbort("Exception after processing fees but before "
                           "processing sequence number: ",
                           e.what());
    }
    catch (...)
    {
        printErrorAndAbort("Unknown exception after processing fees but before "
                           "processing sequence number");
    }

    try
    {
        bool res = mInnerTx->apply(app, ltx, meta, resPayload, false,
                                   sorobanBasePrngSeed);
        // If this throws, then we may not have the correct TransactionResult so
        // we must crash.
        // Note that even after updateResult is called here, feeCharged will not
        // be accurate for Soroban transactions until
        // FeeBumpTransactionFrame::processPostApply is called.
        updateResult(resPayload, mInnerTx);
        return res;
    }
    catch (std::exception& e)
    {
        printErrorAndAbort("Exception while applying inner transaction: ",
                           e.what());
    }
    catch (...)
    {
        printErrorAndAbort(
            "Unknown exception while applying inner transaction");
    }
}

void
FeeBumpTransactionFrame::processPostApply(
    Application& app, AbstractLedgerTxn& ltx, TransactionMetaFrame& meta,
    TransactionResultPayload& resPayload) const
{
    // We must forward the Fee-bump source so the refund is applied to the
    // correct account
    // Note that we are not calling TransactionFrame::processPostApply, so if
    // any logic is added there, we would have to reason through if that logic
    // should also be reflected here.
    int64_t refund =
        mInnerTx->processRefund(app, ltx, meta, getFeeSourceID(), resPayload);

    // The result codes and a feeCharged without the refund are set in
    // updateResult in FeeBumpTransactionFrame::apply. At this point, feeCharged
    // is set correctly on the inner transaction, so update the feeBump result.
    if (protocolVersionStartsFrom(ltx.loadHeader().current().ledgerVersion,
                                  ProtocolVersion::V_21) &&
        isSoroban())
    {
        // First update feeCharged of the inner result on the feeBump using
        // mInnerTx
        {
            auto& irp = resPayload.getResult().result.innerResultPair();
            auto& innerRes = irp.result;
            innerRes.feeCharged = resPayload.getInnerResult().feeCharged;

            // Now set the updated feeCharged on the fee bump.
            resPayload.getResult().feeCharged -= refund;
        }
    }
}

bool
FeeBumpTransactionFrame::checkSignature(SignatureChecker& signatureChecker,
                                        LedgerTxnEntry const& account,
                                        int32_t neededWeight) const
{
    auto& acc = account.current().data.account();
    std::vector<Signer> signers;
    if (acc.thresholds[0])
    {
        auto signerKey = KeyUtils::convertKey<SignerKey>(acc.accountID);
        signers.push_back(Signer(signerKey, acc.thresholds[0]));
    }
    signers.insert(signers.end(), acc.signers.begin(), acc.signers.end());

    return signatureChecker.checkSignature(signers, neededWeight);
}

bool
FeeBumpTransactionFrame::checkValid(Application& app,
                                    AbstractLedgerTxn& ltxOuter,
                                    TransactionResultPayload& resPayload,
                                    SequenceNumber current,
                                    uint64_t lowerBoundCloseTimeOffset,
                                    uint64_t upperBoundCloseTimeOffset) const
{
    if (!XDRProvidesValidFee())
    {
        resPayload.initializeFeeBumpResult();
        resPayload.getResult().result.code(txMALFORMED);
        return false;
    }

    LedgerTxn ltx(ltxOuter);
    int64_t minBaseFee = ltx.loadHeader().current().baseFee;
    resetResults(ltx.loadHeader().current(), minBaseFee, false, resPayload);

    SignatureChecker signatureChecker{ltx.loadHeader().current().ledgerVersion,
                                      getContentsHash(),
                                      mEnvelope.feeBump().signatures};
    if (commonValid(signatureChecker, ltx, false, resPayload) !=
        ValidationType::kFullyValid)
    {
        return false;
    }

    releaseAssert(resPayload.isFeeBump());
    if (!signatureChecker.checkAllSignaturesUsed())
    {
        resPayload.getResult().result.code(txBAD_AUTH_EXTRA);
        return false;
    }

    bool res = mInnerTx->checkValidWithOptionallyChargedFee(
        app, ltx, resPayload, current, false, lowerBoundCloseTimeOffset,
        upperBoundCloseTimeOffset);

    updateResult(resPayload, mInnerTx);
    return res;
}

bool
FeeBumpTransactionFrame::checkSorobanResourceAndSetError(
    Application& app, uint32_t ledgerVersion,
    TransactionResultPayload& resPayload) const
{
    return mInnerTx->checkSorobanResourceAndSetError(app, ledgerVersion,
                                                     resPayload);
}

bool
FeeBumpTransactionFrame::commonValidPreSeqNum(
    AbstractLedgerTxn& ltx, TransactionResultPayload& resPayload) const
{
    // this function does validations that are independent of the account state
    //    (stay true regardless of other side effects)

    releaseAssert(resPayload.isFeeBump());
    auto header = ltx.loadHeader();
    if (protocolVersionIsBefore(header.current().ledgerVersion,
                                ProtocolVersion::V_13))
    {
        resPayload.getResult().result.code(txNOT_SUPPORTED);
        return false;
    }
    auto inclusionFee = getInclusionFee();
    auto minInclusionFee = getMinInclusionFee(*this, header.current());
    if (inclusionFee < minInclusionFee)
    {
        resPayload.getResult().result.code(txINSUFFICIENT_FEE);
        return false;
    }
    // While in theory it should be possible to bump a Soroban
    // transaction with negative inclusion fee (this is unavoidable
    // when Soroban resource fee exceeds uint32), we still won't
    // consider the inner transaction valid. So we return early here
    // in order to have `bigMultiply` below not crash.
    if (mInnerTx->getInclusionFee() < 0)
    {
        resPayload.getResult().result.code(txFEE_BUMP_INNER_FAILED);
        return false;
    }
    auto const& lh = header.current();
    // Make sure that fee bump is actually happening, i.e. that the
    // inclusion fee per operation in this envelope is higher than
    // the one in the inner envelope.
    uint128_t v1 =
        bigMultiply(getInclusionFee(), getMinInclusionFee(*mInnerTx, lh));
    uint128_t v2 =
        bigMultiply(mInnerTx->getInclusionFee(), getMinInclusionFee(*this, lh));
    if (v1 < v2)
    {
        if (!bigDivide128(resPayload.getResult().feeCharged, v2,
                          getMinInclusionFee(*mInnerTx, lh),
                          Rounding::ROUND_UP))
        {
            resPayload.getResult().feeCharged = INT64_MAX;
        }
        resPayload.getResult().result.code(txINSUFFICIENT_FEE);
        return false;
    }

    if (!stellar::loadAccount(ltx, getFeeSourceID()))
    {
        resPayload.getResult().result.code(txNO_ACCOUNT);
        return false;
    }

    return true;
}

FeeBumpTransactionFrame::ValidationType
FeeBumpTransactionFrame::commonValid(SignatureChecker& signatureChecker,
                                     AbstractLedgerTxn& ltxOuter, bool applying,
                                     TransactionResultPayload& resPayload) const
{
    releaseAssert(resPayload.isFeeBump());
    LedgerTxn ltx(ltxOuter);
    ValidationType res = ValidationType::kInvalid;

    if (!commonValidPreSeqNum(ltx, resPayload))
    {
        return res;
    }

    auto feeSource = stellar::loadAccount(ltx, getFeeSourceID());
    if (!checkSignature(
            signatureChecker, feeSource,
            feeSource.current().data.account().thresholds[THRESHOLD_LOW]))
    {
        resPayload.getResult().result.code(txBAD_AUTH);
        return res;
    }

    res = ValidationType::kInvalidPostAuth;

    auto header = ltx.loadHeader();
    // if we are in applying mode fee was already deduced from signing account
    // balance, if not, we need to check if after that deduction this account
    // will still have minimum balance
    int64_t feeToPay = applying ? 0 : getFullFee();
    // don't let the account go below the reserve after accounting for
    // liabilities
    if (getAvailableBalance(header, feeSource) < feeToPay)
    {
        resPayload.getResult().result.code(txINSUFFICIENT_BALANCE);
        return res;
    }

    return ValidationType::kFullyValid;
}

TransactionEnvelope const&
FeeBumpTransactionFrame::getEnvelope() const
{
    return mEnvelope;
}

#ifdef BUILD_TESTS
TransactionEnvelope&
FeeBumpTransactionFrame::getMutableEnvelope() const
{
    return mEnvelope;
}

void
FeeBumpTransactionFrame::clearCached() const
{
    Hash zero;
    mContentsHash = zero;
    mFullHash = zero;
    mInnerTx->clearCached();
}
#endif

FeeBumpTransactionFrame const&
FeeBumpTransactionFrame::toFeeBumpTransactionFrame() const
{
    return *this;
}

TransactionFrame const&
FeeBumpTransactionFrame::toTransactionFrame() const
{
    return *mInnerTx;
}

int64_t
FeeBumpTransactionFrame::getFullFee() const
{
    return mEnvelope.feeBump().tx.fee;
}

int64
FeeBumpTransactionFrame::declaredSorobanResourceFee() const
{
    return mInnerTx->declaredSorobanResourceFee();
}

int64_t
FeeBumpTransactionFrame::getInclusionFee() const
{
    if (isSoroban())
    {
        return getFullFee() - declaredSorobanResourceFee();
    }
    return getFullFee();
}

bool
FeeBumpTransactionFrame::XDRProvidesValidFee() const
{
    if (getFullFee() < 0)
    {
        return false;
    }
    return mInnerTx->XDRProvidesValidFee();
}

int64_t
FeeBumpTransactionFrame::getFee(LedgerHeader const& header,
                                std::optional<int64_t> baseFee,
                                bool applying) const
{
    if (!baseFee)
    {
        return getFullFee();
    }
    int64_t flatFee = 0;
    if (mInnerTx->isSoroban())
    {
        flatFee = mInnerTx->declaredSorobanResourceFee();
    }
    int64_t adjustedFee = *baseFee * std::max<int64_t>(1, getNumOperations());
    if (applying)
    {
        return flatFee + std::min<int64_t>(getInclusionFee(), adjustedFee);
    }
    else
    {
        return flatFee + adjustedFee;
    }
}

Hash const&
FeeBumpTransactionFrame::getContentsHash() const
{
    if (isZero(mContentsHash))
    {
        mContentsHash = sha256(xdr::xdr_to_opaque(
            mNetworkID, ENVELOPE_TYPE_TX_FEE_BUMP, mEnvelope.feeBump().tx));
    }
    return mContentsHash;
}

Hash const&
FeeBumpTransactionFrame::getFullHash() const
{
    if (isZero(mFullHash))
    {
        mFullHash = sha256(xdr::xdr_to_opaque(mEnvelope));
    }
    return mFullHash;
}

Hash const&
FeeBumpTransactionFrame::getInnerFullHash() const
{
    return mInnerTx->getFullHash();
}

uint32_t
FeeBumpTransactionFrame::getNumOperations() const
{
    return mInnerTx->getNumOperations() + 1;
}

Resource
FeeBumpTransactionFrame::getResources(bool useByteLimitInClassic) const
{
    auto res = mInnerTx->getResources(useByteLimitInClassic);
    res.setVal(Resource::Type::OPERATIONS, getNumOperations());
    return res;
}

std::vector<Operation> const&
FeeBumpTransactionFrame::getRawOperations() const
{
    return mInnerTx->getRawOperations();
}

SequenceNumber
FeeBumpTransactionFrame::getSeqNum() const
{
    return mInnerTx->getSeqNum();
}

AccountID
FeeBumpTransactionFrame::getFeeSourceID() const
{
    return toAccountID(mEnvelope.feeBump().tx.feeSource);
}

AccountID
FeeBumpTransactionFrame::getSourceID() const
{
    return mInnerTx->getSourceID();
}

std::optional<SequenceNumber const> const
FeeBumpTransactionFrame::getMinSeqNum() const
{
    return mInnerTx->getMinSeqNum();
}

Duration
FeeBumpTransactionFrame::getMinSeqAge() const
{
    return mInnerTx->getMinSeqAge();
}

uint32
FeeBumpTransactionFrame::getMinSeqLedgerGap() const
{
    return mInnerTx->getMinSeqLedgerGap();
}

void
FeeBumpTransactionFrame::insertKeysForFeeProcessing(
    UnorderedSet<LedgerKey>& keys) const
{
    keys.emplace(accountKey(getFeeSourceID()));
    mInnerTx->insertKeysForFeeProcessing(keys);
}

void
FeeBumpTransactionFrame::insertKeysForTxApply(
    UnorderedSet<LedgerKey>& keys) const
{
    mInnerTx->insertKeysForTxApply(keys);
}

void
FeeBumpTransactionFrame::processFeeSeqNum(
    AbstractLedgerTxn& ltx, std::optional<int64_t> baseFee,
    TransactionResultPayload& resPayload) const
{
    resetResults(ltx.loadHeader().current(), baseFee, true, resPayload);
    releaseAssert(resPayload.isFeeBump());

    auto feeSource = stellar::loadAccount(ltx, getFeeSourceID());
    if (!feeSource)
    {
        throw std::runtime_error("Unexpected database state");
    }
    auto& acc = feeSource.current().data.account();

    auto header = ltx.loadHeader();

    int64_t& fee = resPayload.getResult().feeCharged;
    if (fee > 0)
    {
        fee = std::min(acc.balance, fee);
        // Note: TransactionUtil addBalance checks that reserve plus liabilities
        // are respected. In this case, we allow it to fall below that since it
        // will be caught later in commonValid.
        stellar::addBalance(acc.balance, -fee);
        header.current().feePool += fee;
    }
}

void
FeeBumpTransactionFrame::removeOneTimeSignerKeyFromFeeSource(
    AbstractLedgerTxn& ltx) const
{
    auto account = stellar::loadAccount(ltx, getFeeSourceID());
    if (!account)
    {
        return; // probably account was removed due to merge operation
    }

    auto header = ltx.loadHeader();
    auto signerKey = SignerKeyUtils::preAuthTxKey(*this);
    auto& signers = account.current().data.account().signers;
    auto findRes = findSignerByKey(signers.begin(), signers.end(), signerKey);
    if (findRes.second)
    {
        removeSignerWithPossibleSponsorship(ltx, header, findRes.first,
                                            account);
    }
}

void
FeeBumpTransactionFrame::resetResults(
    LedgerHeader const& header, std::optional<int64_t> baseFee, bool applying,
    TransactionResultPayload& resPayload) const
{
    mInnerTx->resetResults(header, baseFee, applying, resPayload);

    resPayload.initializeFeeBumpResult();
    resPayload.getResult().result.code(txFEE_BUMP_INNER_SUCCESS);

    // feeCharged is updated accordingly to represent the cost of the
    // transaction regardless of the failure modes.
    resPayload.getResult().feeCharged = getFee(header, baseFee, applying);
}

std::shared_ptr<StellarMessage const>
FeeBumpTransactionFrame::toStellarMessage() const
{
    auto msg = std::make_shared<StellarMessage>();
    msg->type(TRANSACTION);
    msg->transaction() = mEnvelope;
    return msg;
}
}
