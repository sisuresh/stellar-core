#pragma once

// Copyright 2025 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "xdr/Stellar-ledger.h"
#include <memory>

namespace stellar
{

class OperationMetaWrapper
{
  private:
    struct OpMetaInner
    {
        LedgerEntryChanges mLeChanges;
        xdr::xvector<ContractEvent> mContractEvents;
        xdr::xvector<DiagnosticEvent> mDiagnosticEvents;

        OpMetaInner(LedgerEntryChanges&& lec, xdr::xvector<ContractEvent>&& ces,
                    xdr::xvector<DiagnosticEvent>&& des)
            : mLeChanges(lec), mContractEvents(ces), mDiagnosticEvents(des)
        {
        }
    };

    std::vector<OpMetaInner> mInner;

  public:
    OperationMetaWrapper(uint32_t reserve_size);

    void push(LedgerEntryChanges&& lec, xdr::xvector<ContractEvent>&& ces,
              xdr::xvector<DiagnosticEvent>&& des);

    xdr::xvector<stellar::OperationMeta> convertToXDR();

    xdr::xvector<stellar::OperationMetaV2> convertToXDRV2();

    xdr::xvector<ContractEvent> flushContractEvents();

    xdr::xvector<DiagnosticEvent> flushDiagnosticEvents();
};

}