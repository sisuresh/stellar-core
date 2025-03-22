#include "transactions/EventManager.h"
#include "crypto/KeyUtils.h"
#include "transactions/TransactionFrameBase.h"
#include "transactions/TransactionUtils.h"
#include "util/types.h"

namespace stellar
{

EventManager::EventManager(uint32_t protocolVersion, Config const& config,
                           TransactionFrameBaseConstPtr tx)
    : mProtocolVersion(protocolVersion), mConfig(config), mTx(tx)
{
}

void
EventManager::pushContractEvents(xdr::xvector<ContractEvent> const& evts)
{
    auto& des = mContractEvents;
    des.insert(des.end(), evts.begin(), evts.end());
}

void
EventManager::pushDiagnosticEvents(xdr::xvector<DiagnosticEvent> const& evts)
{
    auto& des = mDiagnosticEvents;
    des.insert(des.end(), evts.begin(), evts.end());
}

void
EventManager::pushSimpleDiagnosticError(SCErrorType ty, SCErrorCode code,
                                        std::string&& message,
                                        xdr::xvector<SCVal>&& args)
{
    ContractEvent ce;
    ce.type = DIAGNOSTIC;
    ce.body.v(0);

    SCVal sym = makeSymbolSCVal("error"), err;
    err.type(SCV_ERROR);
    err.error().type(ty);
    err.error().code() = code;
    ce.body.v0().topics.assign({std::move(sym), std::move(err)});

    if (args.empty())
    {
        ce.body.v0().data.type(SCV_STRING);
        ce.body.v0().data.str().assign(std::move(message));
    }
    else
    {
        ce.body.v0().data.type(SCV_VEC);
        ce.body.v0().data.vec().activate();
        ce.body.v0().data.vec()->reserve(args.size() + 1);
        ce.body.v0().data.vec()->emplace_back(
            makeStringSCVal(std::move(message)));
        std::move(std::begin(args), std::end(args),
                  std::back_inserter(*ce.body.v0().data.vec()));
    }
    mDiagnosticEvents.emplace_back(false, std::move(ce));
}

void
EventManager::pushDiagnosticError(EventManagerPtr const& ptr, SCErrorType ty,
                                  SCErrorCode code, std::string&& message,
                                  xdr::xvector<SCVal>&& args)
{
    if (ptr)
    {
        ptr->pushSimpleDiagnosticError(ty, code, std::move(message),
                                       std::move(args));
    }
}

void
EventManager::pushApplyTimeDiagnosticError(SCErrorType ty, SCErrorCode code,
                                           std::string&& message,
                                           xdr::xvector<SCVal>&& args)
{
    if (mConfig.ENABLE_SOROBAN_DIAGNOSTIC_EVENTS)
    {
        pushSimpleDiagnosticError(ty, code, std::move(message),
                                  std::move(args));
    }
}

void
EventManager::pushValidationTimeDiagnosticError(EventManagerPtr const& ptr,
                                                SCErrorType ty,
                                                SCErrorCode code,
                                                std::string&& message,
                                                xdr::xvector<SCVal>&& args)
{
    if (ptr && ptr->mConfig.ENABLE_DIAGNOSTICS_FOR_TX_SUBMISSION)
    {
        ptr->pushSimpleDiagnosticError(ty, code, std::move(message),
                                       std::move(args));
    }
}

void
EventManager::flushContractEvents(xdr::xvector<ContractEvent>& buf)
{
    std::move(mContractEvents.begin(), mContractEvents.end(),
              std::back_inserter(buf));
    mContractEvents.clear();
};

void
EventManager::flushDiagnosticEvents(xdr::xvector<DiagnosticEvent>& buf)
{
    std::move(mDiagnosticEvents.begin(), mDiagnosticEvents.end(),
              std::back_inserter(buf));
    mDiagnosticEvents.clear();
};

} // namespace stellar