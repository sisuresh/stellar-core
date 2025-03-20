// Copyright 2022 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/TransactionMetaFrame.h"
#include "crypto/SHA.h"
#include "transactions/TransactionFrameBase.h"
#include "util/GlobalChecks.h"
#include "util/MetaUtils.h"
#include "util/ProtocolVersion.h"
#include <iterator>
#include <xdrpp/xdrpp/marshal.h>

namespace
{
void
setSorobanMetaFeeInfo(stellar::SorobanTransactionMetaExt& sorobanMetaExt,
                      int64_t nonRefundableFeeSpent,
                      int64_t totalRefundableFeeSpent, int64_t rentFeeCharged)
{
    auto& ext = sorobanMetaExt.v1();
    ext.totalNonRefundableResourceFeeCharged = nonRefundableFeeSpent;
    ext.totalRefundableResourceFeeCharged = totalRefundableFeeSpent;
    ext.rentFeeCharged = rentFeeCharged;
}

template <typename T>
void
vecAppend(xdr::xvector<T>& a, xdr::xvector<T>&& b)
{
    std::move(b.begin(), b.end(), std::back_inserter(a));
}

}

namespace stellar
{

TransactionMetaFrame::TransactionMetaFrame(uint32_t protocolVersion,
                                           bool backfillStellarAssetEvents)
{
    // The TransactionMeta v() switch can be in 5 positions 0, 1, 2, 3, 4. We do
    // not support 0 or 1 at all -- core does not produce it anymore and we have
    // no obligation to consume it under any circumstance -- so this class just
    // switches between cases 2, 3 and 4.
    if (protocolVersionStartsFrom(protocolVersion, ProtocolVersion::V_23) ||
        backfillStellarAssetEvents)
    {
        mVersion = 4;
    }
    else if (protocolVersionStartsFrom(protocolVersion,
                                       SOROBAN_PROTOCOL_VERSION))
    {
        mVersion = 3;
    }
    else
    {
        mVersion = 2;
    }
    mTransactionMeta.v(mVersion);
}

size_t
TransactionMetaFrame::getNumChangesBefore() const
{
    switch (mTransactionMeta.v())
    {
    case 2:
        return mTransactionMeta.v2().txChangesBefore.size();
    case 3:
        return mTransactionMeta.v3().txChangesBefore.size();
    case 4:
        return mTransactionMeta.v4().txChangesBefore.size();
    default:
        releaseAssert(false);
    }
}

LedgerEntryChanges
TransactionMetaFrame::getChangesBefore() const
{
    switch (mTransactionMeta.v())
    {
    case 2:
        return mTransactionMeta.v2().txChangesBefore;
    case 3:
        return mTransactionMeta.v3().txChangesBefore;
    case 4:
        return mTransactionMeta.v4().txChangesBefore;
    default:
        releaseAssert(false);
    }
}

LedgerEntryChanges
TransactionMetaFrame::getChangesAfter() const
{
    switch (mTransactionMeta.v())
    {
    case 2:
        return mTransactionMeta.v2().txChangesAfter;
    case 3:
        return mTransactionMeta.v3().txChangesAfter;
    case 4:
        return mTransactionMeta.v4().txChangesAfter;
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaFrame::pushTxChangesBefore(LedgerEntryChanges&& changes)
{
    switch (mTransactionMeta.v())
    {
    case 2:
        vecAppend(mTransactionMeta.v2().txChangesBefore, std::move(changes));
        break;
    case 3:
        vecAppend(mTransactionMeta.v3().txChangesBefore, std::move(changes));
        break;
    case 4:
        vecAppend(mTransactionMeta.v4().txChangesBefore, std::move(changes));
        break;
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaFrame::clearOperationMetas()
{
    switch (mTransactionMeta.v())
    {
    case 2:
        mTransactionMeta.v2().operations.clear();
        break;
    case 3:
        mTransactionMeta.v3().operations.clear();
        break;
    case 4:
        mTransactionMeta.v4().operations.clear();
        break;
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaFrame::pushOperationMetas(OperationMetaWrapper&& opMetas)
{
    switch (mTransactionMeta.v())
    {
    case 2:
        vecAppend(mTransactionMeta.v2().operations, opMetas.convertToXDR());
        break;
    case 3:
        vecAppend(mTransactionMeta.v3().operations, opMetas.convertToXDR());
        break;
    case 4:
        vecAppend(mTransactionMeta.v4().operations, opMetas.convertToXDRV2());
        break;
    default:
        releaseAssert(false);
    }
}

size_t
TransactionMetaFrame::getNumOperations() const
{
    switch (mTransactionMeta.v())
    {
    case 2:
        return mTransactionMeta.v2().operations.size();
    case 3:
        return mTransactionMeta.v3().operations.size();
    case 4:
        return mTransactionMeta.v4().operations.size();
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaFrame::pushTxChangesAfter(LedgerEntryChanges&& changes)
{
    switch (mTransactionMeta.v())
    {
    case 2:
        vecAppend(mTransactionMeta.v2().txChangesAfter, std::move(changes));
        break;
    case 3:
        vecAppend(mTransactionMeta.v3().txChangesAfter, std::move(changes));
        break;
    case 4:
        vecAppend(mTransactionMeta.v4().txChangesAfter, std::move(changes));
        break;
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaFrame::clearTxChangesAfter()
{
    switch (mTransactionMeta.v())
    {
    case 2:
        mTransactionMeta.v2().txChangesAfter.clear();
        break;
    case 3:
        mTransactionMeta.v3().txChangesAfter.clear();
        break;
    case 4:
        mTransactionMeta.v4().txChangesAfter.clear();
        break;
    default:
        releaseAssert(false);
    }
}

// TODO: consolidate the events pushing at tx level and op level and
// sorobantxmeta levels

void
TransactionMetaFrame::pushTxContractEvents(xdr::xvector<ContractEvent>&& events)
{
    switch (mTransactionMeta.v())
    {
    case 2:
        // Do nothing, until v3 we don't create events.
        break;
    case 3:
        mTransactionMeta.v3().sorobanMeta.activate().events = std::move(events);
        break;
    case 4:
        mTransactionMeta.v4().events = std::move(events);
        break;
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaFrame::pushTxDiagnosticEvents(
    xdr::xvector<DiagnosticEvent>&& events)
{
    switch (mTransactionMeta.v())
    {
    case 2:
        // Do nothing, until v3 we don't create events.
        break;
    case 3:
        mTransactionMeta.v3().sorobanMeta.activate().diagnosticEvents =
            std::move(events);
        break;
    case 4:
        mTransactionMeta.v4().txDiagnosticEvents = std::move(events);
        break;
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaFrame::setReturnValue(SCVal&& returnValue)
{
    switch (mTransactionMeta.v())
    {
    case 2:
        // Do nothing, until v3 we don't call into contracts.
        break;
    case 3:
        mTransactionMeta.v3().sorobanMeta.activate().returnValue =
            std::move(returnValue);
        break;
    case 4:
        mTransactionMeta.v4().sorobanMeta.activate().returnValue =
            std::move(returnValue);
        break;
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaFrame::setSorobanFeeInfo(int64_t nonRefundableFeeSpent,
                                        int64_t totalRefundableFeeSpent,
                                        int64_t rentFeeCharged)
{
    switch (mTransactionMeta.v())
    {
    case 2:
        // Do nothing, until v3 we don't call into contracts.
        break;
    case 3:
        setSorobanMetaFeeInfo(mTransactionMeta.v3().sorobanMeta.activate().ext,
                              nonRefundableFeeSpent, totalRefundableFeeSpent,
                              rentFeeCharged);
        break;
    case 4:
        setSorobanMetaFeeInfo(mTransactionMeta.v4().sorobanMeta.activate().ext,
                              nonRefundableFeeSpent, totalRefundableFeeSpent,
                              rentFeeCharged);
        break;
    default:
        releaseAssert(false);
    }
}

TransactionMeta const&
TransactionMetaFrame::getXDR() const
{
    return mTransactionMeta;
}
}
