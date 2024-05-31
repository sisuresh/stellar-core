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
class MutableTransactionResultBase;

static constexpr ContractDataDurability CONTRACT_INSTANCE_ENTRY_DURABILITY =
    ContractDataDurability::PERSISTENT;

class InvokeHostFunctionOpFrame : public OperationFrame
{
    InvokeHostFunctionResult&
    innerResult(OperationResult& res) const
    {
        return res.tr().invokeHostFunctionResult();
    }

    // TODO: Temporarily templated while we have both HostFunctionMetrics and
    // ParallelHostFunctionMetrics
    template <typename MetricsT>
    void maybePopulateDiagnosticEvents(Config const& cfg,
                                       InvokeHostFunctionOutput const& output,
                                       MetricsT const& metrics,
                                       SorobanTxData& sorobanData) const;

    InvokeHostFunctionOp const& mInvokeHostFunction;

  public:
    InvokeHostFunctionOpFrame(Operation const& op,
                              TransactionFrame const& parentTx);

    bool isOpSupported(LedgerHeader const& header) const override;

    bool doApply(AbstractLedgerTxn& ltx, OperationResult& res) const override;
    bool doApplyForSoroban(Application& app, AbstractLedgerTxn& ltx,
                           Hash const& sorobanBasePrngSeed,
                           OperationResult& res,
                           SorobanTxData& sorobanData) const override;

    bool doCheckValidForSoroban(SorobanNetworkConfig const& networkConfig,
                                Config const& appConfig, uint32_t ledgerVersion,
                                OperationResult& res,
                                SorobanTxData& sorobanData) const override;
    bool doCheckValid(uint32_t ledgerVersion,
                      OperationResult& res) const override;

    ParallelOpReturnVal doApplyParallel(
        ClusterEntryMap const& entryMap, // Must not be shared between threads!
        Config const& appConfig, SorobanNetworkConfig const& sorobanConfig,
        Hash const& sorobanBasePrngSeed, CxxLedgerInfo const& ledgerInfo,
        OperationResult& res, SorobanTxData& sorobanData, uint32_t ledgerSeq,
        uint32_t ledgerVersion) const override;

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
