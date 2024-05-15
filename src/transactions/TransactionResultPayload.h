#pragma once

// Copyright 2024 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "util/NonCopyable.h"
#include "util/types.h"

#include <memory>

namespace stellar
{

class Config;
class OperationFrame;
class InternalLedgerEntry;
class TransactionFrame;
class TransactionMetaFrame;
class SorobanNetworkConfig;

class TransactionResultPayload;
using TransactionResultPayloadPtr = std::shared_ptr<TransactionResultPayload>;

// This class holds all mutable state that is associated with a transaction.
class TransactionResultPayload
    : public NonMovableOrCopyable
#ifdef BUILD_TESTS
    ,
      public std::enable_shared_from_this<TransactionResultPayload>
#endif
{
  private:
    struct SorobanData
    {
        xdr::xvector<ContractEvent> mEvents;
        xdr::xvector<DiagnosticEvent> mDiagnosticEvents;
        SCVal mReturnValue;
        // Size of the emitted Soroban events.
        uint32_t mConsumedContractEventsSizeBytes{};
        int64_t mFeeRefund{};
        int64_t mConsumedNonRefundableFee{};
        int64_t mConsumedRentFee{};
        int64_t mConsumedRefundableFee{};
    };

    TransactionResult mTxResult;
    std::optional<TransactionResult> mOuterFeeBumpResult;
    std::vector<std::shared_ptr<OperationFrame>> mOpFrames;
    std::optional<SorobanData> mSorobanExtension;

    std::shared_ptr<InternalLedgerEntry const> mCachedAccount;

    TransactionResultPayload(TransactionFrame const& tx, int64_t feeCharged);

  public:
    static TransactionResultPayloadPtr create(TransactionFrame const& tx,
                                              int64_t feeCharged);
    void initializeFeeBumpResult();
    void initializeSorobanExtension();

    bool isFeeBump() const;

    // Returns the inner most result.
    TransactionResult& getInnerResult();

    // Returns the outer most result. If payload refers to a Fee Bump TX, the
    // fee bump result is returned. Otherwise, the inner TX result is returned.
    TransactionResult& getResult();
    TransactionResult const& getResult() const;
    TransactionResultCode getResultCode() const;

    std::vector<std::shared_ptr<OperationFrame>> const& getOpFrames() const;
    xdr::xvector<DiagnosticEvent> const& getDiagnosticEvents() const;
    std::shared_ptr<InternalLedgerEntry const>& getCachedAccountPtr();

    bool consumeRefundableSorobanResources(
        uint32_t contractEventSizeBytes, int64_t rentFee,
        uint32_t protocolVersion, SorobanNetworkConfig const& sorobanConfig,
        Config const& cfg, TransactionFrame const& tx);

    int64_t& getSorobanConsumedNonRefundableFee();
    int64_t& getSorobanFeeRefund();

    void pushContractEvents(xdr::xvector<ContractEvent> const& evts);
    void pushDiagnosticEvents(xdr::xvector<DiagnosticEvent> const& evts);
    void setReturnValue(SCVal const& returnValue);
    void pushDiagnosticEvent(DiagnosticEvent const& ecvt);
    void pushSimpleDiagnosticError(Config const& cfg, SCErrorType ty,
                                   SCErrorCode code, std::string&& message,
                                   xdr::xvector<SCVal>&& args);
    void pushApplyTimeDiagnosticError(Config const& cfg, SCErrorType ty,
                                      SCErrorCode code, std::string&& message,
                                      xdr::xvector<SCVal>&& args = {});
    void pushValidationTimeDiagnosticError(Config const& cfg, SCErrorType ty,
                                           SCErrorCode code,
                                           std::string&& message,
                                           xdr::xvector<SCVal>&& args = {});
    void publishSuccessDiagnosticsToMeta(TransactionMetaFrame& meta,
                                         Config const& cfg);
    void publishFailureDiagnosticsToMeta(TransactionMetaFrame& meta,
                                         Config const& cfg);

#ifdef BUILD_TESTS
    TransactionResultPayloadPtr
    getShared()
    {
        return shared_from_this();
    }
#endif
};
}