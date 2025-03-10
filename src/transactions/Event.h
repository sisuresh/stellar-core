#pragma once

#include "xdr/Stellar-ledger-entries.h"
#include "xdr/Stellar-ledger.h"
#include "xdr/Stellar-transaction.h"

// Copyright 2025 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

namespace stellar
{

// contract: asset, topics: ["transfer", from:Address, to:Address, sep0011_asset:String], data: { amount:i128 }
ContractEvent transfer(Hash const& networkID, Asset const& asset, MuxedAccount const& from, MuxedAccount const& to, int64 amount, Memo const& memo);

// ContractEvent mint();

// ContractEvent burn();

// ContractEvent clawback();


}