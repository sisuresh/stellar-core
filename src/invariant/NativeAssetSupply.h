#pragma once

// Copyright 2025 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "invariant/Invariant.h"
#include "transactions/TransactionUtils.h"

namespace stellar
{
class Application;

class NativeAssetSupply : public Invariant
{
  public:
    NativeAssetSupply(AssetContractInfo const& nativeContractInfo);
    static std::shared_ptr<Invariant> registerInvariant(Application& app);

    virtual std::string getName() const override;

    virtual std::string
    checkSnapshot(CompleteConstLedgerStatePtr ledgerState,
                  InMemorySorobanState const& inMemorySnapshot) override;

  private:
    AssetContractInfo const mNativeContractInfo;
};
}
