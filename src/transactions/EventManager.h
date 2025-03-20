#pragma once

// Copyright 2025 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "main/Config.h"
#include "xdr/Stellar-ledger-entries.h"
#include "xdr/Stellar-ledger.h"
#include "xdr/Stellar-transaction.h"

namespace stellar
{

class EventManager
{

  private:
    xdr::xvector<ContractEvent> mContractEvents;
    xdr::xvector<DiagnosticEvent> mDiagnosticEvents;

  public:
    void pushContractEvents(xdr::xvector<ContractEvent> const& evts);

    void pushDiagnosticEvents(xdr::xvector<DiagnosticEvent> const& evts);

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

    xdr::xvector<DiagnosticEvent> const&
    getDiagnosticEvents() const
    {
        return mDiagnosticEvents;
    }

    xdr::xvector<ContractEvent>&&
    flushContractEvents()
    {
        return std::move(mContractEvents);
    };

    xdr::xvector<DiagnosticEvent>&&
    flushDiagnosticEvents()
    {
        return std::move(mDiagnosticEvents);
    };
};

}