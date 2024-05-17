// Copyright 2024 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/TransactionResultPayload.h"
#include "transactions/OperationFrame.h"
#include "transactions/TransactionFrame.h"
#include "transactions/TransactionUtils.h"

#include <Tracy.hpp>

namespace stellar
{

TransactionResultPayloadImpl::TransactionResultPayloadImpl(
    TransactionFrame const& tx, int64_t feeCharged)
{
    auto const& envelope = tx.getEnvelope();
    auto const& ops = envelope.type() == ENVELOPE_TYPE_TX_V0
                          ? envelope.v0().tx.operations
                          : envelope.v1().tx.operations;

    // pre-allocates the results for all operations
    mTxResult.result.code(txSUCCESS);
    mTxResult.result.results().resize(static_cast<uint32_t>(ops.size()));

    mOpFrames.clear();

    // bind operations to the results
    for (size_t i = 0; i < ops.size(); i++)
    {
        mOpFrames.push_back(
            OperationFrame::makeHelper(ops[i], mTxResult.result.results()[i],
                                       tx, static_cast<uint32_t>(i)));
    }

    mTxResult.feeCharged = feeCharged;

    // resets Soroban related fields
    if (tx.isSoroban())
    {
        mSorobanExtension = SorobanData();
    }
}

TransactionResult&
TransactionResultPayloadImpl::getResult()
{
    return mTxResult;
}

TransactionResult const&
TransactionResultPayloadImpl::getResult() const
{
    return mTxResult;
}

TransactionResultCode
TransactionResultPayloadImpl::getResultCode() const
{
    return getResult().result.code();
}

bool
TransactionResultPayloadImpl::consumeRefundableSorobanResources(
    uint32_t contractEventSizeBytes, int64_t rentFee, uint32_t protocolVersion,
    SorobanNetworkConfig const& sorobanConfig, Config const& cfg,
    TransactionFrame const& tx)
{
    ZoneScoped;
    releaseAssertOrThrow(tx.isSoroban());
    releaseAssertOrThrow(mSorobanExtension);
    auto& consumedContractEventsSizeBytes =
        mSorobanExtension->mConsumedContractEventsSizeBytes;
    consumedContractEventsSizeBytes += contractEventSizeBytes;

    auto& consumedRentFee = mSorobanExtension->mConsumedRentFee;
    auto& consumedRefundableFee = mSorobanExtension->mConsumedRefundableFee;
    consumedRentFee += rentFee;
    consumedRefundableFee += rentFee;

    // mFeeRefund was set in apply
    auto& feeRefund = mSorobanExtension->mFeeRefund;
    if (feeRefund < consumedRentFee)
    {
        pushApplyTimeDiagnosticError(
            cfg, SCE_BUDGET, SCEC_EXCEEDED_LIMIT,
            "refundable resource fee was not sufficient to cover the ledger "
            "storage rent: {} > {}",
            {makeU64SCVal(consumedRentFee), makeU64SCVal(feeRefund)});
        return false;
    }
    feeRefund -= consumedRentFee;

    FeePair consumedFee = TransactionFrame::computeSorobanResourceFee(
        protocolVersion, tx.sorobanResources(),
        static_cast<uint32>(
            tx.getResources(false).getVal(Resource::Type::TX_BYTE_SIZE)),
        consumedContractEventsSizeBytes, sorobanConfig, cfg);
    consumedRefundableFee += consumedFee.refundable_fee;
    if (feeRefund < consumedFee.refundable_fee)
    {
        pushApplyTimeDiagnosticError(
            cfg, SCE_BUDGET, SCEC_EXCEEDED_LIMIT,
            "refundable resource fee was not sufficient to cover the events "
            "fee after paying for ledger storage rent: {} > {}",
            {makeU64SCVal(consumedFee.refundable_fee),
             makeU64SCVal(feeRefund)});
        return false;
    }
    feeRefund -= consumedFee.refundable_fee;
    return true;
}

void
TransactionResultPayloadImpl::setSorobanConsumedNonRefundableFee(int64_t fee)
{
    releaseAssertOrThrow(mSorobanExtension);
    mSorobanExtension->mConsumedNonRefundableFee = fee;
}

int64_t
TransactionResultPayloadImpl::getSorobanFeeRefund() const
{
    releaseAssertOrThrow(mSorobanExtension);
    return mSorobanExtension->mFeeRefund;
}

void
TransactionResultPayloadImpl::setSorobanFeeRefund(int64_t fee)
{
    releaseAssertOrThrow(mSorobanExtension);
    mSorobanExtension->mFeeRefund = fee;
}

std::vector<std::shared_ptr<OperationFrame>> const&
TransactionResultPayloadImpl::getOpFrames() const
{
    return mOpFrames;
}

xdr::xvector<DiagnosticEvent> const&
TransactionResultPayloadImpl::getDiagnosticEvents() const
{
    static xdr::xvector<DiagnosticEvent> const empty;
    if (mSorobanExtension)
    {
        return mSorobanExtension->mDiagnosticEvents;
    }
    else
    {
        return empty;
    }
}

std::shared_ptr<InternalLedgerEntry const>&
TransactionResultPayloadImpl::getCachedAccountPtr()
{
    return mCachedAccount;
}

void
TransactionResultPayloadImpl::pushContractEvents(
    xdr::xvector<ContractEvent> const& evts)
{
    releaseAssertOrThrow(mSorobanExtension);
    mSorobanExtension->mEvents = evts;
}

void
TransactionResultPayloadImpl::pushDiagnosticEvents(
    xdr::xvector<DiagnosticEvent> const& evts)
{
    releaseAssertOrThrow(mSorobanExtension);
    auto& des = mSorobanExtension->mDiagnosticEvents;
    des.insert(des.end(), evts.begin(), evts.end());
}

void
TransactionResultPayloadImpl::pushDiagnosticEvent(DiagnosticEvent const& evt)
{
    releaseAssertOrThrow(mSorobanExtension);
    mSorobanExtension->mDiagnosticEvents.emplace_back(evt);
}

void
TransactionResultPayloadImpl::pushSimpleDiagnosticError(
    Config const& cfg, SCErrorType ty, SCErrorCode code, std::string&& message,
    xdr::xvector<SCVal>&& args)
{
    releaseAssertOrThrow(mSorobanExtension);

    ContractEvent ce;
    ce.type = DIAGNOSTIC;
    ce.body.v(0);

    SCVal sym = makeSymbolSCVal("error"), err;
    err.type(SCV_ERROR);
    err.error().type(ty);
    err.error().code() = code;
    ce.body.v0().topics.assign({std::move(sym), std::move(err)});

    if (args.empty())
    {
        ce.body.v0().data.type(SCV_STRING);
        ce.body.v0().data.str().assign(std::move(message));
    }
    else
    {
        ce.body.v0().data.type(SCV_VEC);
        ce.body.v0().data.vec().activate();
        ce.body.v0().data.vec()->reserve(args.size() + 1);
        ce.body.v0().data.vec()->emplace_back(
            makeStringSCVal(std::move(message)));
        std::move(std::begin(args), std::end(args),
                  std::back_inserter(*ce.body.v0().data.vec()));
    }
    DiagnosticEvent evt(false, std::move(ce));
    pushDiagnosticEvent(evt);
}

void
TransactionResultPayloadImpl::pushApplyTimeDiagnosticError(
    Config const& cfg, SCErrorType ty, SCErrorCode code, std::string&& message,
    xdr::xvector<SCVal>&& args)
{
    if (!cfg.ENABLE_SOROBAN_DIAGNOSTIC_EVENTS)
    {
        return;
    }
    pushSimpleDiagnosticError(cfg, ty, code, std::move(message),
                              std::move(args));
}

void
TransactionResultPayloadImpl::pushValidationTimeDiagnosticError(
    Config const& cfg, SCErrorType ty, SCErrorCode code, std::string&& message,
    xdr::xvector<SCVal>&& args)
{
    if (!cfg.ENABLE_DIAGNOSTICS_FOR_TX_SUBMISSION)
    {
        return;
    }
    pushSimpleDiagnosticError(cfg, ty, code, std::move(message),
                              std::move(args));
}

void
TransactionResultPayloadImpl::setReturnValue(SCVal const& returnValue)
{
    releaseAssertOrThrow(mSorobanExtension);
    mSorobanExtension->mReturnValue = returnValue;
}

void
TransactionResultPayloadImpl::publishSuccessDiagnosticsToMeta(
    TransactionMetaFrame& meta, Config const& cfg)
{
    releaseAssertOrThrow(mSorobanExtension);
    meta.pushContractEvents(std::move(mSorobanExtension->mEvents));
    meta.pushDiagnosticEvents(std::move(mSorobanExtension->mDiagnosticEvents));
    meta.setReturnValue(std::move(mSorobanExtension->mReturnValue));
    if (cfg.EMIT_SOROBAN_TRANSACTION_META_EXT_V1)
    {
        meta.setSorobanFeeInfo(mSorobanExtension->mConsumedNonRefundableFee,
                               mSorobanExtension->mConsumedRefundableFee,
                               mSorobanExtension->mConsumedRentFee);
    }
}

void
TransactionResultPayloadImpl::publishFailureDiagnosticsToMeta(
    TransactionMetaFrame& meta, Config const& cfg)
{
    releaseAssertOrThrow(mSorobanExtension);
    meta.pushDiagnosticEvents(std::move(mSorobanExtension->mDiagnosticEvents));
    if (cfg.EMIT_SOROBAN_TRANSACTION_META_EXT_V1)
    {
        meta.setSorobanFeeInfo(mSorobanExtension->mConsumedNonRefundableFee,
                               /* totalRefundableFeeSpent */ 0,
                               /* rentFeeCharged */ 0);
    }
}

FeeBumpTransactionResultPayload::FeeBumpTransactionResultPayload(
    TransactionResultPayloadPtr innerResultPayload)
    : mInnerResultPayload(innerResultPayload)
{
    releaseAssertOrThrow(mInnerResultPayload);
}

void
FeeBumpTransactionResultPayload::updateResult(TransactionFrameBasePtr innerTx)
{
    releaseAssert(mInnerResultPayload);
    if (mInnerResultPayload->getResultCode() == txSUCCESS)
    {
        getResult().result.code(txFEE_BUMP_INNER_SUCCESS);
    }
    else
    {
        getResult().result.code(txFEE_BUMP_INNER_FAILED);
    }

    auto& feeBumpIrp = getResult().result.innerResultPair();
    feeBumpIrp.transactionHash = innerTx->getContentsHash();

    auto const& innerTxRes = mInnerResultPayload->getResult();
    auto& feeBumpIrpRes = feeBumpIrp.result;
    feeBumpIrpRes.feeCharged = innerTxRes.feeCharged;
    feeBumpIrpRes.result.code(innerTxRes.result.code());
    switch (feeBumpIrpRes.result.code())
    {
    case txSUCCESS:
    case txFAILED:
        feeBumpIrpRes.result.results() = innerTxRes.result.results();
        break;
    default:
        break;
    }
}

void
FeeBumpTransactionResultPayload::setInnerResultPayload(
    TransactionResultPayloadPtr innerResultPayload)
{
    releaseAssertOrThrow(innerResultPayload);
    mInnerResultPayload = innerResultPayload;
}

TransactionResultPayloadPtr
FeeBumpTransactionResultPayload::getInnerResultPayload()
{
    releaseAssertOrThrow(mInnerResultPayload);
    return mInnerResultPayload;
}

TransactionResult&
FeeBumpTransactionResultPayload::getResult()
{
    return mTxResult;
}

TransactionResult const&
FeeBumpTransactionResultPayload::getResult() const
{
    return mTxResult;
}

TransactionResultCode
FeeBumpTransactionResultPayload::getResultCode() const
{
    return getResult().result.code();
}

std::vector<std::shared_ptr<OperationFrame>> const&
FeeBumpTransactionResultPayload::getOpFrames() const
{
    releaseAssertOrThrow(mInnerResultPayload);
    return mInnerResultPayload->getOpFrames();
}

xdr::xvector<DiagnosticEvent> const&
FeeBumpTransactionResultPayload::getDiagnosticEvents() const
{
    releaseAssertOrThrow(mInnerResultPayload);
    return mInnerResultPayload->getDiagnosticEvents();
}

std::shared_ptr<InternalLedgerEntry const>&
FeeBumpTransactionResultPayload::getCachedAccountPtr()
{
    releaseAssertOrThrow(mInnerResultPayload);
    return mInnerResultPayload->getCachedAccountPtr();
}

bool
FeeBumpTransactionResultPayload::consumeRefundableSorobanResources(
    uint32_t contractEventSizeBytes, int64_t rentFee, uint32_t protocolVersion,
    SorobanNetworkConfig const& sorobanConfig, Config const& cfg,
    TransactionFrame const& tx)
{
    releaseAssertOrThrow(mInnerResultPayload);
    return mInnerResultPayload->consumeRefundableSorobanResources(
        contractEventSizeBytes, rentFee, protocolVersion, sorobanConfig, cfg,
        tx);
}

void
FeeBumpTransactionResultPayload::setSorobanConsumedNonRefundableFee(int64_t fee)
{
    releaseAssertOrThrow(mInnerResultPayload);
    mInnerResultPayload->setSorobanConsumedNonRefundableFee(fee);
}

int64_t
FeeBumpTransactionResultPayload::getSorobanFeeRefund() const
{
    releaseAssertOrThrow(mInnerResultPayload);
    return mInnerResultPayload->getSorobanFeeRefund();
}

void
FeeBumpTransactionResultPayload::setSorobanFeeRefund(int64_t fee)
{
    releaseAssertOrThrow(mInnerResultPayload);
    mInnerResultPayload->setSorobanFeeRefund(fee);
}

void
FeeBumpTransactionResultPayload::pushContractEvents(
    xdr::xvector<ContractEvent> const& evts)
{
    releaseAssertOrThrow(mInnerResultPayload);
    mInnerResultPayload->pushContractEvents(evts);
}

void
FeeBumpTransactionResultPayload::pushDiagnosticEvents(
    xdr::xvector<DiagnosticEvent> const& evts)
{
    releaseAssertOrThrow(mInnerResultPayload);
    mInnerResultPayload->pushDiagnosticEvents(evts);
}

void
FeeBumpTransactionResultPayload::setReturnValue(SCVal const& returnValue)
{
    releaseAssertOrThrow(mInnerResultPayload);
    mInnerResultPayload->setReturnValue(returnValue);
}

void
FeeBumpTransactionResultPayload::pushDiagnosticEvent(
    DiagnosticEvent const& ecvt)
{
    releaseAssertOrThrow(mInnerResultPayload);
    mInnerResultPayload->pushDiagnosticEvent(ecvt);
}

void
FeeBumpTransactionResultPayload::pushSimpleDiagnosticError(
    Config const& cfg, SCErrorType ty, SCErrorCode code, std::string&& message,
    xdr::xvector<SCVal>&& args)
{
    releaseAssertOrThrow(mInnerResultPayload);
    mInnerResultPayload->pushSimpleDiagnosticError(
        cfg, ty, code, std::move(message), std::move(args));
}

void
FeeBumpTransactionResultPayload::pushApplyTimeDiagnosticError(
    Config const& cfg, SCErrorType ty, SCErrorCode code, std::string&& message,
    xdr::xvector<SCVal>&& args)
{
    releaseAssertOrThrow(mInnerResultPayload);
    mInnerResultPayload->pushApplyTimeDiagnosticError(
        cfg, ty, code, std::move(message), std::move(args));
}

void
FeeBumpTransactionResultPayload::pushValidationTimeDiagnosticError(
    Config const& cfg, SCErrorType ty, SCErrorCode code, std::string&& message,
    xdr::xvector<SCVal>&& args)
{
    releaseAssertOrThrow(mInnerResultPayload);
    mInnerResultPayload->pushValidationTimeDiagnosticError(
        cfg, ty, code, std::move(message), std::move(args));
}

void
FeeBumpTransactionResultPayload::publishSuccessDiagnosticsToMeta(
    TransactionMetaFrame& meta, Config const& cfg)
{
    releaseAssertOrThrow(mInnerResultPayload);
    mInnerResultPayload->publishSuccessDiagnosticsToMeta(meta, cfg);
}

void
FeeBumpTransactionResultPayload::publishFailureDiagnosticsToMeta(
    TransactionMetaFrame& meta, Config const& cfg)
{
    releaseAssertOrThrow(mInnerResultPayload);
    mInnerResultPayload->publishFailureDiagnosticsToMeta(meta, cfg);
}
}