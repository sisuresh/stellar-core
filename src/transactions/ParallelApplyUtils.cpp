// Copyright 2025 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/ParallelApplyUtils.h"
#include "transactions/MutableTransactionResult.h"

namespace stellar
{

std::unordered_set<LedgerKey>
getReadWriteKeysForStage(ApplyStage const& stage)
{
    std::unordered_set<LedgerKey> res;

    for (auto const& txBundle : stage)
    {
        for (auto const& lk :
             txBundle.getTx()->sorobanResources().footprint.readWrite)
        {
            res.emplace(lk);
            if (isSorobanEntry(lk))
            {
                res.emplace(getTTLKey(lk));
            }
        }
    }
    return res;
}

std::unique_ptr<ThreadEntryMap>
collectEntries(AppConnector& app, AbstractLedgerTxn& ltx,
               Cluster const& cluster)
{
    auto entryMap = std::make_unique<ThreadEntryMap>();
    for (auto const& txBundle : cluster)
    {
        if (txBundle.getResPayload().isSuccess())
        {
            txBundle.getTx()->preloadEntriesForParallelApply(
                app, app.getSorobanMetrics(), ltx, *entryMap,
                txBundle.getResPayload(),
                txBundle.getEffects().getMeta().getDiagnosticEventManager());
        }
    }

    return entryMap;
}

void
setDelta(ThreadEntryMap const& entryMap,
         OpModifiedEntryMap const& opModifiedEntryMap,
         ParallelLedgerInfo const& ledgerInfo, TxEffects& effects)
{
    for (auto const& newUpdates : opModifiedEntryMap)
    {
        auto const& lk = newUpdates.first;
        auto const& le = newUpdates.second;

        // Any key the op updates should also be in entryMap because the
        // keys were taken from the footprint (the ttl keys were added
        // as well)
        auto prev = entryMap.find(lk);
        releaseAssertOrThrow(prev != entryMap.end());

        auto prevLe = prev->second.mLedgerEntry;

        LedgerTxnDelta::EntryDelta entryDelta;
        if (prevLe)
        {
            entryDelta.previous =
                std::make_shared<InternalLedgerEntry>(*prevLe);
        }
        if (le)
        {
            auto deltaLe = *le;
            // This is for the invariants check in LedgerManager
            deltaLe.lastModifiedLedgerSeq = ledgerInfo.getLedgerSeq();

            entryDelta.current = std::make_shared<InternalLedgerEntry>(deltaLe);
        }

        effects.setDeltaEntry(lk, entryDelta);
    }
}
}