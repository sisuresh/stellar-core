// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include "crypto/SecretKey.h"
#include "xdr/Stellar-ledger-entries.h"
#include "xdr/Stellar-ledger.h"
#include "xdr/Stellar-transaction.h"

namespace stellar
{
namespace SimulationUtils
{
SecretKey getNewSecret(AccountID const& key, uint32_t n);
int64_t getNewOfferID(int64_t offerId, uint32_t n);
int64_t getNewOfferID(OperationResult const& result, uint32_t n);
SecretKey updateAccountID(AccountID& acc, uint32_t n);
SignerKey getNewEd25519Signer(Signer const& signer, uint32_t n);
void updateOperation(Operation& op, uint32_t);
void replaceIssuer(Asset& asset, uint32_t n);
void generateLiveEntries(std::vector<LedgerEntry>& entries,
                         std::vector<LedgerEntry> const& oldEntries,
                         uint32_t count);
void generateDeadEntries(std::vector<LedgerKey>& keys,
                         std::vector<LedgerKey> const& oldKeys, uint32_t count);
}
}