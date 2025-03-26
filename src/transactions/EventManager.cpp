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
EventManager::eventsForClaimAtoms(Hash const& networkID, MuxedAccount const& source, xdr::xvector<stellar::ClaimAtom> const& claimAtoms, Memo const& memo)
{
    auto sourceSCAddress = accountToSCAddress(source);

    for (auto const& atom : claimAtoms)
    {
        switch (atom.type())
        {
        case CLAIM_ATOM_TYPE_V0:
        {
            SCAddress seller(SC_ADDRESS_TYPE_ACCOUNT);
            seller.accountId().ed25519() = atom.v0().sellerEd25519;

            auto amountToSeller = atom.v0().amountBought;
            auto assetToSeller = atom.v0().assetBought;

            auto amountToSource = atom.v0().amountSold;
            auto assetToSource = atom.v0().assetSold;

            eventForTransferWithIssuerCheck(
                networkID, assetToSeller, sourceSCAddress, seller,
                amountToSeller, memo);
            eventForTransferWithIssuerCheck(
                networkID, assetToSource, seller, sourceSCAddress,
                amountToSource, memo);
        }
        case CLAIM_ATOM_TYPE_ORDER_BOOK:
        {
            auto seller = accountToSCAddress(atom.orderBook().sellerID);

            auto amountToSeller = atom.orderBook().amountBought;
            auto assetToSeller = atom.orderBook().assetBought;

            auto amountToSource = atom.orderBook().amountSold;
            auto assetToSource = atom.orderBook().assetSold;

            eventForTransferWithIssuerCheck(
                networkID, assetToSeller, sourceSCAddress, seller,
                amountToSeller, memo);
            eventForTransferWithIssuerCheck(
                networkID, assetToSource, seller, sourceSCAddress,
                amountToSource, memo);
        }
        case CLAIM_ATOM_TYPE_LIQUIDITY_POOL:
        {
            auto poolID = liquidityPoolIDToSCAddress(
                atom.liquidityPool().liquidityPoolID);

            auto amountToPool = atom.liquidityPool().amountBought;
            auto assetToPool = atom.liquidityPool().assetBought;

            auto amountFromPool = atom.liquidityPool().amountSold;
            auto assetFromPool = atom.liquidityPool().assetSold;

            eventForTransferWithIssuerCheck(
                networkID, assetToPool, sourceSCAddress, poolID,
                amountToPool, memo);
            eventForTransferWithIssuerCheck(
                networkID, assetFromPool, poolID, sourceSCAddress,
                amountFromPool, memo);
        }
        }
    }
}

void
EventManager::eventForTransferWithIssuerCheck(Hash const& networkID,
                                              Asset const& asset,
                                              SCAddress const& from,
                                              SCAddress const& to, int64 amount,
                                              Memo const& memo)
{
    auto fromIsIssuer = isIssuer(from, asset);
    auto toIsIssuer = isIssuer(to, asset);

    if (fromIsIssuer && toIsIssuer)
    {
        newTransferEvent(networkID, asset, from, to, amount, memo);
    }
    else if (fromIsIssuer)
    {
        newMintEvent(networkID, asset, to, amount);
    }
    else if (toIsIssuer)
    {
        newBurnEvent(networkID, asset, from, amount);
    }
    else
    {
        newTransferEvent(networkID, asset, from, to, amount, memo);
    }
}

void
EventManager::newTransferEvent(Hash const& networkID, Asset const& asset,
                               SCAddress const& from, SCAddress const& to,
                               int64 amount, Memo const& memo)
{
    ContractEvent ev;
    ev.type = ContractEventType::CONTRACT;
    ev.contractID.activate() = getAssetContractID(networkID, asset);

    SCVec topics = {makeSymbolSCVal("transfer"),
                    makeAddressSCVal(getAddressWithDroppedMuxedInfo(from)),
                    makeAddressSCVal(getAddressWithDroppedMuxedInfo(to)),
                    makeSep0011AssetStringSCVal(asset)};
    ev.body.v0().topics = topics;

    // mux follows order of precedence
    // no mux no memo -- data is just i128
    // else data is an scmap
    //

    bool is_from_mux = from.type() == SC_ADDRESS_TYPE_MUXED_ACCOUNT;
    bool is_to_mux = to.type() == SC_ADDRESS_TYPE_MUXED_ACCOUNT;
    bool is_to_non_mux_account = to.type() == SC_ADDRESS_TYPE_ACCOUNT;
    bool has_memo = memo.type() != MemoType::MEMO_NONE;

    SCVal amountVal = makeI128SCVal(amount);

    // TODO: Make sure the logic below matches CAP-0067
    if (!is_from_mux && !is_to_mux && (!has_memo || is_to_non_mux_account))
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
            fromEntry.val = makeMuxIDSCVal(from.muxedAccount());
            dataMap.push_back(fromEntry);
        }

        if (is_to_mux)
        {
            SCMapEntry toEntry;
            toEntry.key = makeSymbolSCVal("to_muxed_id");
            toEntry.val = makeMuxIDSCVal(to.muxedAccount());
            dataMap.push_back(toEntry);
        }
        else if (has_memo && is_to_non_mux_account)
        {
            SCMapEntry toEntry;
            toEntry.key = makeSymbolSCVal("to_muxed_id");
            toEntry.val = makeClassicMemoSCVal(memo);
            dataMap.push_back(toEntry);
        }

        ev.body.v0().data = data;
    }
    mContractEvents.emplace_back(std::move(ev));
}

void
EventManager::newMintEvent(Hash const& networkID, Asset const& asset,
                           SCAddress const& to, int64 amount)
{
    ContractEvent ev;
    ev.type = ContractEventType::CONTRACT;
    ev.contractID.activate() = getAssetContractID(networkID, asset);

    SCVec topics = {makeSymbolSCVal("mint"),
                    makeAddressSCVal(getAddressWithDroppedMuxedInfo(to)),
                    makeSep0011AssetStringSCVal(asset)};
    ev.body.v0().topics = topics;

    ev.body.v0().data = makeI128SCVal(amount);

    mContractEvents.emplace_back(std::move(ev));
}

void
EventManager::newBurnEvent(Hash const& networkID, Asset const& asset,
                           SCAddress const& from, int64 amount)
{
    ContractEvent ev;
    ev.type = ContractEventType::CONTRACT;
    ev.contractID.activate() = getAssetContractID(networkID, asset);

    SCVec topics = {makeSymbolSCVal("burn"),
                    makeAddressSCVal(getAddressWithDroppedMuxedInfo(from)),
                    makeSep0011AssetStringSCVal(asset)};
    ev.body.v0().topics = topics;

    ev.body.v0().data = makeI128SCVal(amount);

    mContractEvents.emplace_back(std::move(ev));
}

void
EventManager::newClawbackEvent(Hash const& networkID, Asset const& asset,
                      SCAddress const& from, int64 amount)
{
    ContractEvent ev;
    ev.type = ContractEventType::CONTRACT;
    ev.contractID.activate() = getAssetContractID(networkID, asset);

    SCVec topics = {makeSymbolSCVal("clawback"),
                    makeAddressSCVal(getAddressWithDroppedMuxedInfo(from)),
                    makeSep0011AssetStringSCVal(asset)};
    ev.body.v0().topics = topics;

    ev.body.v0().data = makeI128SCVal(amount);

    mContractEvents.emplace_back(std::move(ev));
}

void
EventManager::newSetAuthorizedEvent(Hash const& networkID, Asset const& asset,
                          AccountID const& id, bool authorize)
{
    ContractEvent ev;
    ev.type = ContractEventType::CONTRACT;
    ev.contractID.activate() = getAssetContractID(networkID, asset);

    SCVec topics = {makeSymbolSCVal("set_authorized"),
                    makeAccountIDSCVal(id),
                    makeSep0011AssetStringSCVal(asset)};
    ev.body.v0().topics = topics;

    SCVal val(SCV_BOOL);
    val.b() = authorize;
    
    ev.body.v0().data = val;

    mContractEvents.emplace_back(std::move(ev));
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