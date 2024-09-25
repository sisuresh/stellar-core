#pragma once

// Copyright 2023 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/OperationFrame.h"
#include "xdr/Stellar-transaction.h"

namespace stellar
{
class AbstractLedgerTxn;
class MutableTransactionResultBase;

class RestoreFootprintOpFrame : public OperationFrame
{
    RestoreFootprintResult&
    innerResult(OperationResult& res) const
    {
        return res.tr().restoreFootprintResult();
    }

    RestoreFootprintOp const& mRestoreFootprintOp;

  public:
    RestoreFootprintOpFrame(Operation const& op,
                            TransactionFrame const& parentTx);

    bool isOpSupported(LedgerHeader const& header) const override;

    bool doApply(Application& app, AbstractLedgerTxn& ltx,
                 Hash const& sorobanBasePrngSeed, OperationResult& res,
                 std::shared_ptr<SorobanTxData> sorobanData) const override;
    bool doCheckValidForSoroban(SorobanNetworkConfig const& networkConfig,
                                Config const& appConfig, uint32_t ledgerVersion,
                                OperationResult& res,
                                SorobanTxData& sorobanData) const override;
    bool doCheckValid(uint32_t ledgerVersion,
                      OperationResult& res) const override;

    ParallelOpReturnVal doApplyParallel(
        ThreadEntryMap const& entryMap, // Must not be shared between threads!
        Config const& appConfig, SorobanNetworkConfig const& sorobanConfig,
        Hash const& sorobanBasePrngSeed, CxxLedgerInfo const& ledgerInfo,
        SorobanMetrics& sorobanMetrics, OperationResult& res,
        SorobanTxData& sorobanData, uint32_t ledgerSeq,
        uint32_t ledgerVersion) const override;

    void
    insertLedgerKeysToPrefetch(UnorderedSet<LedgerKey>& keys) const override;

    static RestoreFootprintResultCode
    getInnerCode(OperationResult const& res)
    {
        return res.tr().restoreFootprintResult().code();
    }

    virtual bool isSoroban() const override;

    ThresholdLevel getThresholdLevel() const override;
};
}
