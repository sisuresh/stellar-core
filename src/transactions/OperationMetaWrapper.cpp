// Copyright 2025 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/OperationMetaWrapper.h"

namespace stellar
{

OperationMetaWrapper::OperationMetaWrapper(uint32_t reserve_size)
{
    mInner.reserve(reserve_size);
}

void
OperationMetaWrapper::push(LedgerEntryChanges&& lec,
                           xdr::xvector<ContractEvent>&& ces)
{
    mInner.emplace_back(std::move(lec), std::move(ces));
}

xdr::xvector<stellar::OperationMeta>
OperationMetaWrapper::convertToXDR()
{
    xdr::xvector<stellar::OperationMeta> result;
    result.reserve(mInner.size());
    for (auto const& inner : mInner)
    {
        result.emplace_back(std::move(inner.mLeChanges));
    }
    return result;
}

xdr::xvector<stellar::OperationMetaV2>
OperationMetaWrapper::convertToXDRV2()
{
    xdr::xvector<stellar::OperationMetaV2> result;
    result.reserve(mInner.size());
    for (auto const& inner : mInner)
    {
        result.emplace_back(ExtensionPoint(), std::move(inner.mLeChanges),
                            std::move(inner.mContractEvents));
    }
    return result;
}

xdr::xvector<ContractEvent>
OperationMetaWrapper::flushContractEvents()
{
    size_t total_size = 0;
    for (auto const& op : mInner)
    {
        total_size += op.mContractEvents.size();
    }

    xdr::xvector<ContractEvent> result;
    result.reserve(total_size);
    for (auto& op : mInner)
    {
        std::move(op.mContractEvents.begin(), op.mContractEvents.end(),
                  std::back_inserter(result));
        op.mContractEvents.clear();
    }
    return result;
}

}