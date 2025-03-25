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
EventManager::newTransferEvent(Hash const& networkID, Asset const& asset, MuxedAccount const& from, MuxedAccount const& to, int64 amount, Memo const& memo)
{
    ContractEvent ev;
    ev.type = ContractEventType::CONTRACT;
    ev.contractID.activate() = getAssetContractID(networkID, asset);

    SCVec topics = {
        makeSymbolSCVal("transfer"),
        makeAccountIDSCVal(toAccountID(from)),
        makeAccountIDSCVal(toAccountID(to)),
        makeSep0011AssetStringSCVal(asset)
    };
    ev.body.v0().topics = topics;

    // mux follows order of precedence
    // no mux no memo -- data is just i128
    // else data is an scmap
    // 

    bool is_from_mux = from.type() == CryptoKeyType::KEY_TYPE_ED25519;
    bool is_to_mux = to.type() == CryptoKeyType::KEY_TYPE_ED25519;
    bool has_memo = memo.type() != MemoType::MEMO_NONE;
    
    SCVal amountVal = makeI128SCVal(amount);

    if (!is_from_mux && !is_to_mux && !has_memo) 
    {
        ev.body.v0().data = amountVal;
    } 
    else 
    {
        // data is ScMap
        SCVal data(SCV_MAP);
        SCMap& dataMap = data.map().activate();

        SCMapEntry amountEntry;
        amountEntry.key = makeSymbolSCVal("amount");
        amountEntry.val = amountVal;
        dataMap.push_back(amountEntry);

        if (is_from_mux) 
        {
            SCMapEntry fromEntry;
            fromEntry.key = makeSymbolSCVal("from_muxed_id");
            fromEntry.val = makeMuxIDSCVal(from);
            dataMap.push_back(fromEntry);
        }

        if (is_to_mux) 
        {
            SCMapEntry toEntry;
            toEntry.key = makeSymbolSCVal("to_muxed_id");
            toEntry.val = makeMuxIDSCVal(to);
            dataMap.push_back(toEntry);
        }
        else if (has_memo)
        {
            SCMapEntry toEntry;
            toEntry.key = makeSymbolSCVal("to_muxed_id");
            toEntry.val = makeClassicMemoSCVal(memo);
            dataMap.push_back(toEntry);
        }

        ev.body.v0().data = data;
    }
    mContractEvents.push_back(std::move(ev));
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