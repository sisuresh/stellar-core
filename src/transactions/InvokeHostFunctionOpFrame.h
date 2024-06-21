#pragma once

// Copyright 2022 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "rust/RustBridge.h"
#include "transactions/OperationFrame.h"
#include "xdr/Stellar-transaction.h"
#include <medida/metrics_registry.h>

namespace stellar
{
class AbstractLedgerTxn;
class TransactionResultPayloadBase;

static constexpr ContractDataDurability CONTRACT_INSTANCE_ENTRY_DURABILITY =
    ContractDataDurability::PERSISTENT;

class InvokeHostFunctionOpFrame : public OperationFrame
{
    InvokeHostFunctionResult&
    innerResult()
    {
        return mResult.tr().invokeHostFunctionResult();
    }

    // TODO: Temporarily templated while we have both HostFunctionMetrics and
    // ParallelHostFunctionMetrics
    template <typename MetricsT>
    void maybePopulateDiagnosticEvents(
        Config const& cfg, InvokeHostFunctionOutput const& output,
        MetricsT const& metrics, TransactionResultPayloadBase& resPayload);

    InvokeHostFunctionOp const& mInvokeHostFunction;

  public:
    InvokeHostFunctionOpFrame(Operation const& op, OperationResult& res,
                              TransactionFrame const& parentTx);

    bool isOpSupported(LedgerHeader const& header) const override;

    bool doApply(AbstractLedgerTxn& ltx,
                 TransactionResultPayloadBase& resPayload) override;
    bool doApply(Application& app, AbstractLedgerTxn& ltx,
                 Hash const& sorobanBasePrngSeed,
                 TransactionResultPayloadBase& resPayload) override;

    ParallelOpReturnVal doApplyParallel(
        ClusterEntryMap const& entryMap, // Must not be shared between threads!
        Config const& appConfig, SorobanNetworkConfig const& sorobanConfig,
        Hash const& sorobanBasePrngSeed, CxxLedgerInfo const& ledgerInfo,
        TransactionResultPayloadBase& resPayload, uint32_t ledgerSeq,
        uint32_t ledgerVersion) override;

    bool doCheckValid(SorobanNetworkConfig const& config,
                      Config const& appConfig, uint32_t ledgerVersion,
                      TransactionResultPayloadBase& resPayload) override;
    bool doCheckValid(uint32_t ledgerVersion) override;

    void
    insertLedgerKeysToPrefetch(UnorderedSet<LedgerKey>& keys) const override;

    static InvokeHostFunctionResultCode
    getInnerCode(OperationResult const& res)
    {
        return res.tr().invokeHostFunctionResult().code();
    }

    virtual bool isSoroban() const override;
};
}
