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
class EventManager;
using EventManagerPtr = std::shared_ptr<EventManager>;
class TransactionFrameBase;
using TransactionFrameBaseConstPtr =
    std::shared_ptr<TransactionFrameBase const>;

class EventManager
{
  private:
    xdr::xvector<ContractEvent> mContractEvents;
    xdr::xvector<DiagnosticEvent> mDiagnosticEvents;
    uint32_t mProtocolVersion;
    Config const& mConfig;
    TransactionFrameBaseConstPtr mTx;

    void pushSimpleDiagnosticError(SCErrorType ty, SCErrorCode code,
                                   std::string&& message,
                                   xdr::xvector<SCVal>&& args);

  public:
    EventManager(uint32_t protocolVersion, Config const& config,
                 TransactionFrameBaseConstPtr tx);

    void pushContractEvents(xdr::xvector<ContractEvent> const& evts);

    void pushDiagnosticEvents(xdr::xvector<DiagnosticEvent> const& evts);

    static void pushDiagnosticError(EventManagerPtr const& ptr, SCErrorType ty,
                                    SCErrorCode code, std::string&& message,
                                    xdr::xvector<SCVal>&& args);

    void pushApplyTimeDiagnosticError(SCErrorType ty, SCErrorCode code,
                                      std::string&& message,
                                      xdr::xvector<SCVal>&& args = {});

    static void pushValidationTimeDiagnosticError(
        EventManagerPtr const& ptr, SCErrorType ty, SCErrorCode code,
        std::string&& message, xdr::xvector<SCVal>&& args = {});

    void eventsForClaimAtoms(Hash const& networkID, MuxedAccount const& source,
                             xdr::xvector<stellar::ClaimAtom> const& claimAtoms,
                             Memo const& memo);

    // This will check if the issuer is involved, and emit a mint/burn instead
    // of a transfer if so
    void eventForTransferWithIssuerCheck(Hash const& networkID,
                                         Asset const& asset,
                                         SCAddress const& from,
                                         SCAddress const& to, int64 amount,
                                         Memo const& memo);

    //TODO: Maybe make the transfer, mint, and burn methods private if possible?

    // Adds a new "transfer" contractEvent in the form of:
    // contract: asset, topics: ["transfer", from:Address, to:Address,
    // sep0011_asset:String], data: { amount:i128 }
    void newTransferEvent(Hash const& networkID, Asset const& asset,
                          SCAddress const& from, SCAddress const& to,
                          int64 amount, Memo const& memo);

    // contract: asset, topics: ["mint", to:Address, sep0011_asset:String],
    // data: { amount:i128 }
    void newMintEvent(Hash const& networkID, Asset const& asset,
                      SCAddress const& to, int64 amount);

    // contract: asset, topics: ["burn", from:Address, sep0011_asset:String],
    // data: { amount:i128 }
    void newBurnEvent(Hash const& networkID, Asset const& asset,
                      SCAddress const& from, int64 amount);

    // contract: asset, topics: ["clawback", from:Address,
    // sep0011_asset:String], data: { amount:i128 }
    void newClawbackEvent(Hash const& networkID, Asset const& asset,
                          SCAddress const& from, int64 amount);

    // contract: asset, topics: ["set_authorized", id:Address,
    // sep0011_asset:String], data: { authorize:bool }
    void newSetAuthorizedEvent(Hash const& networkID, Asset const& asset,
                          AccountID const& id, bool authorize);

#ifdef BUILD_TESTS
    xdr::xvector<DiagnosticEvent> const&
    getDiagnosticEvents() const
    {
        return mDiagnosticEvents;
    }
#endif

    void flushContractEvents(xdr::xvector<ContractEvent>& buf);

    void flushDiagnosticEvents(xdr::xvector<DiagnosticEvent>& buf);

    bool
    is_empty() const
    {
        return mContractEvents.empty() && mDiagnosticEvents.empty();
    }

    void
    clear()
    {
        mContractEvents.clear();
        mDiagnosticEvents.clear();
    }
};

}