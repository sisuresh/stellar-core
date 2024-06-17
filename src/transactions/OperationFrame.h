#pragma once

// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "ledger/LedgerHashUtils.h"
#include "ledger/LedgerManager.h"
#include "ledger/NetworkConfig.h"
#include "overlay/StellarXDR.h"
#include "util/types.h"
#include <medida/metrics_registry.h>
#include <memory>

namespace medida
{
class MetricsRegistry;
}

namespace stellar
{
class AbstractLedgerTxn;
class LedgerManager;
class LedgerTxnEntry;
class LedgerTxnHeader;

class SignatureChecker;
class TransactionFrame;
class TransactionResultPayloadBase;

enum class ThresholdLevel
{
    LOW,
    MEDIUM,
    HIGH
};

class OperationFrame
{
  protected:
    Operation const& mOperation;
    TransactionFrame const& mParentTx;
    OperationResult& mResult;

    virtual bool doCheckValid(SorobanNetworkConfig const& config,
                              Config const& appConfig, uint32_t ledgerVersion,
                              TransactionResultPayloadBase& resPayload);
    virtual bool doCheckValid(uint32_t ledgerVersion) = 0;

    virtual bool doApply(Application& app, AbstractLedgerTxn& ltx,
                         Hash const& sorobanBasePrngSeed,
                         TransactionResultPayloadBase& resPayload);
    virtual bool doApply(AbstractLedgerTxn& ltx,
                         TransactionResultPayloadBase& resPayload) = 0;

    virtual std::pair<bool, ModifiedEntryMap> doApplyParallel(
        ClusterEntryMap const& entryMap, // Must not be shared between threads!
        Config const& appConfig, SorobanNetworkConfig const& sorobanConfig,
        Hash const& sorobanBasePrngSeed, CxxLedgerInfo const& ledgerInfo,
        TransactionResultPayloadBase& resPayload,
        SorobanMetrics& sorobanMetrics /*temporary*/, uint32_t ledgerSeq,
        uint32_t ledgerVersion);

    // returns the threshold this operation requires
    virtual ThresholdLevel getThresholdLevel() const;

    // returns true if the operation is supported given a protocol version and
    // header flags
    virtual bool isOpSupported(LedgerHeader const& header) const;

    LedgerTxnEntry loadSourceAccount(AbstractLedgerTxn& ltx,
                                     LedgerTxnHeader const& header,
                                     TransactionResultPayloadBase& resPayload);

    // given an operation, gives a default value representing "success" for the
    // result
    void resetResultSuccess();

  public:
    static std::shared_ptr<OperationFrame>
    makeHelper(Operation const& op, OperationResult& res,
               TransactionFrame const& parentTx, uint32_t index);

    OperationFrame(Operation const& op, OperationResult& res,
                   TransactionFrame const& parentTx);
    OperationFrame(OperationFrame const&) = delete;
    virtual ~OperationFrame() = default;

    bool checkSignature(SignatureChecker& signatureChecker,
                        AbstractLedgerTxn& ltx,
                        TransactionResultPayloadBase& resPayload,
                        bool forApply);

    AccountID getSourceID() const;

    OperationResult&
    getResult() const
    {
        return mResult;
    }
    OperationResultCode getResultCode() const;

    bool checkValid(Application& app, SignatureChecker& signatureChecker,
                    AbstractLedgerTxn& ltxOuter, bool forApply,
                    TransactionResultPayloadBase& resPayload);

    bool apply(Application& app, SignatureChecker& signatureChecker,
               AbstractLedgerTxn& ltx, Hash const& sorobanBasePrngSeed,
               TransactionResultPayloadBase& resPayload);

    std::pair<bool, ModifiedEntryMap> applyParallel(
        ClusterEntryMap const& entryMap, // Must not be shared between threads!,
        Config const& config, SorobanNetworkConfig const& sorobanConfig,
        CxxLedgerInfo const& ledgerInfo,
        TransactionResultPayloadBase& resPayload,
        SorobanMetrics& sorobanMetrics, Hash const& sorobanBasePrngSeed,
        uint32_t ledgerSeq, uint32_t ledgerVersion);

    Operation const&
    getOperation() const
    {
        return mOperation;
    }

    virtual void
    insertLedgerKeysToPrefetch(UnorderedSet<LedgerKey>& keys) const;

    virtual bool isDexOperation() const;

    virtual bool isSoroban() const;
};
}
