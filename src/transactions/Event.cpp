#include "transactions/Event.h"
#include "transactions/TransactionUtils.h"
#include "util/types.h"
#include "crypto/KeyUtils.h"

namespace stellar {
ContractEvent 
transfer(Hash const& networkID, Asset const& asset, MuxedAccount const& from, MuxedAccount const& to, int64 amount, Memo const& memo)
{
    
    // emit an event in such format: // contract: asset, topics: ["transfer", from:Address, to:Address, sep0011_asset:String], data: { amount:i128 }
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
    
    return ev;
}

} // namespace stellar