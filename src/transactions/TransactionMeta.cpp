// Copyright 2022 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include <iterator>
#include <type_traits>
#include <variant>
#include <xdrpp/xdrpp/marshal.h>

#include "crypto/SHA.h"
#include "ledger/LedgerTypeUtils.h"
#include "transactions/MutableTransactionResult.h"
#include "transactions/OperationFrame.h"
#include "transactions/TransactionFrameBase.h"
#include "transactions/TransactionMeta.h"
#include "util/GlobalChecks.h"
#include "util/MetaUtils.h"
#include "util/ProtocolVersion.h"
#include "xdr/Stellar-ledger.h"

namespace stellar
{

namespace
{

template <typename T>
void
vecAppend(xdr::xvector<T>& a, xdr::xvector<T>&& b)
{
    std::move(b.begin(), b.end(), std::back_inserter(a));
}

// Some operation meta needs to be modified
// to be consumed by downstream systems. In particular, restoration is
// (mostly) logically a new entry creation from the perspective of ltx and
// stellar-core as a whole, but this change type is reclassified to
// LEDGER_ENTRY_RESTORED for easier consumption downstream.
/* LedgerEntryChanges
processOpLedgerEntryChanges(Config const& cfg, OperationFrame const& op,
                            AbstractLedgerTxn& ltx, uint32_t protocolVersion)*/
LedgerEntryChanges
processOpLedgerEntryChanges(Config const& cfg, OperationFrame const& op,
                            LedgerEntryChanges const& initialChanges, stellar::UnorderedMap<stellar::LedgerKey, stellar::LedgerEntry> const& hotArchiveRestores,stellar::UnorderedMap<stellar::LedgerKey, stellar::LedgerEntry> const& liveRestores, uint32_t protocolVersion, uint32_t ledgerSeq)
{
    //auto changes = ltx.getChanges();
    auto changes = initialChanges;
    bool needToProcess =
        (op.getOperation().body.type() == OperationType::RESTORE_FOOTPRINT ||
         op.getOperation().body.type() ==
             OperationType::INVOKE_HOST_FUNCTION) &&
        (protocolVersionStartsFrom(protocolVersion,
                                   AUTO_RESTORE_PROTOCOL_VERSION) ||
         cfg.BACKFILL_RESTORE_META);
    if (!needToProcess)
    {
        return changes;
    }

    //auto const& hotArchiveRestores = ltx.getRestoredHotArchiveKeys();
    //auto const& liveRestores = ltx.getRestoredLiveBucketListKeys();

    // Entry was restored from the hot archive and modified, so we need to
    // construct and insert a RESTORE change with the restored value.
    // Note: for meta ordering stability, it's nice for this to be ordered. The
    // fields below are for lookup only.
    std::set<LedgerKey> stateChangesToAdd;

    // Entry was restored from the live BucketList and modified, so we need to
    // convert the STATE change with the original state to a RESTORE change.
    std::unordered_set<LedgerKey> stateChangesToConvert;

    // Entry was restored from the live BucketList, but rewritten (auto
    // restore). The original meta will have both a STATE and UPDATED change,
    // so we need to remove the STATE change. The UPDATED change will be
    // converted to a RESTORE change below.
    std::unordered_set<LedgerKey> stateChangesToRemove;

    // Depending on whether the restored entry is still in the live
    // BucketList (has not yet been evicted), or has been evicted and is in
    // the hot archive, meta will be handled differently as follows:
    //
    // Entry restore from Hot Archive:
    // Meta before changes:
    //     Data/Code/TTL: LEDGER_ENTRY_CREATED(newValue, newLastModified)
    // Meta after changes:
    //     Data/Code/TTL:
    //        if oldValue != newValue: (i.e. restored then modified)
    //          LEDGER_ENTRY_RESTORED(oldValue, newLastModified),
    //          LEDGER_ENTRY_UPDATED(newValue, newLastModified)
    //        else (i.e. restored and not modified)
    //          LEDGER_ENTRY_RESTORED(oldValue, newLastModified)
    //
    // Entry restore from Live BucketList (autorestore):
    // For autorestore, all restored entries are re-written, even if they
    // haven't been modified. This means we always have meta for both the TTL
    // and code/data entry as follows:
    // Meta before changes:
    //     Data/Code/TTL:
    //        if oldValue != newValue: (i.e. restored then modified)
    //          LEDGER_ENTRY_STATE(oldValue, oldLastModified),
    //          LEDGER_ENTRY_UPDATED(newValue, newLastModified)
    //        else (i.e. restored and not modified)
    //          LEDGER_ENTRY_STATE(oldValue, oldLastModified),
    //          LEDGER_ENTRY_UPDATED(oldValue, newLastModified)
    //
    // Meta after changes:
    //     Data/Code/TTL:
    //        if oldValue != newValue: (i.e. restored then modified)
    //          LEDGER_ENTRY_RESTORED(oldValue, newLastModified),
    //          LEDGER_ENTRY_UPDATED(newValue, newLastModified)
    //        else
    //          LEDGER_ENTRY_RESTORED(oldValue, newLastModified)
    //
    // Entry restore from Live BucketList (RestoreOp):
    // For RestoreOp from the live BucketList, only the TTL value is modified,
    // so we don't have meta for the code/data entry, and the
    // lastModifiedLedgerSeq of the code/data entry is not updated.
    // Meta before changes:
    //     TTL: LEDGER_ENTRY_STATE(oldValue, oldLastModified),
    //          LEDGER_ENTRY_UPDATED(newValue, newLastModified)
    // Meta after changes:
    //     TTL: LEDGER_ENTRY_RESTORED(newValue, newLastModified)
    //     Data/Code: LEDGER_ENTRY_RESTORED(oldValue, oldLastModified)
    //
    // Subtle: with the exception of the RestoreOp data/code from live
    // BucketList, all other RESTORE meta should have the current ledger as
    // the lastModifiedLedgerSeq.

    // First, iterate through existing meta and change everything we need to
    // update.
    for (auto& change : changes)
    {
        if (change.type() == LEDGER_ENTRY_CREATED)
        {
            if (change.created().data.type() == TTL ||
                isPersistentEntry(change.created().data))
            {
                // For entry creation meta, we only need to check for Hot
                // Archive restores
                auto key = LedgerEntryKey(change.created());
                releaseAssertOrThrow(liveRestores.find(key) ==
                                     liveRestores.end());

                auto hotRestoreIter = hotArchiveRestores.find(key);
                if (hotRestoreIter != hotArchiveRestores.end())
                {
                    // Entry was only restored during the TX, change create to
                    // restore
                    auto le = change.created();
                    if (le.data == hotRestoreIter->second.data)
                    {
                        change.type(LEDGER_ENTRY_RESTORED);
                        change.restored() = le;
                    }
                    else
                    {
                        // Entry was restored and modified during the TX, change
                        // create to a modify and add original value as restored
                        change.type(LEDGER_ENTRY_UPDATED);
                        change.updated() = le;
                        stateChangesToAdd.insert(key);
                    }
                }
            }
        }
        else if (change.type() == LEDGER_ENTRY_UPDATED)
        {
            if (change.updated().data.type() == TTL ||
                isPersistentEntry(change.updated().data))
            {
                // Only entries restored from the live BucketList can have
                // UPDATED meta, Hot Archive entries will have CREATED meta.
                auto key = LedgerEntryKey(change.updated());
                releaseAssertOrThrow(hotArchiveRestores.find(key) ==
                                     hotArchiveRestores.end());

                auto liveRestoreIter = liveRestores.find(key);
                if (liveRestoreIter != liveRestores.end())
                {
                    // The entry was restored from the live BucketList. Either:
                    // 1. The entry was only restored and not modified.
                    //    In this case, original meta will be STATE(value),
                    //    UPDATED(value). We will change the UPDATED to RESTORED
                    //    below and just delete the STATE change.
                    if (change.updated().data == liveRestoreIter->second.data)
                    {
                        auto le = change.updated();
                        change.type(LEDGER_ENTRY_RESTORED);
                        change.restored() = le;
                        stateChangesToRemove.insert(key);
                        continue;
                    }

                    // 2. The entry was restored and then modified.
                    //    In this case, meta will emit STATE(oldValue),
                    //    UPDATED(newValue). We will convert the STATE to
                    //    RESTORED and leave the UPDATED change as is.
                    stateChangesToConvert.insert(key);
                }
            }
        }
    }

    // First remove and convert STATE changes
    for (auto iter = changes.begin(); iter != changes.end();)
    {
        if (iter->type() == LEDGER_ENTRY_STATE)
        {
            auto lk = LedgerEntryKey(iter->state());
            if (stateChangesToRemove.find(lk) != stateChangesToRemove.end())
            {
                releaseAssertOrThrow(stateChangesToConvert.find(lk) ==
                                     stateChangesToConvert.end());
                releaseAssertOrThrow(stateChangesToAdd.find(lk) ==
                                     stateChangesToAdd.end());
                iter = changes.erase(iter);
                continue;
            }

            if (stateChangesToConvert.find(lk) != stateChangesToConvert.end())
            {
                releaseAssertOrThrow(stateChangesToAdd.find(lk) ==
                                     stateChangesToAdd.end());

                auto le = iter->state();
                iter->type(LEDGER_ENTRY_RESTORED);
                iter->restored() = le;

                // For consistency between live and hot archive restores,
                // restores lastModifiedLedgerSeq should be the current ledger
                iter->restored().lastModifiedLedgerSeq = ledgerSeq;
            }
        }

        ++iter;
    }

    for (auto const& key : stateChangesToAdd)
    {
        auto le = hotArchiveRestores.at(key);
        LedgerEntryChange change;
        change.type(LEDGER_ENTRY_RESTORED);
        change.restored() = le;
        change.restored().lastModifiedLedgerSeq = ledgerSeq;
        changes.push_back(change);
    }

    if (op.getOperation().body.type() == OperationType::RESTORE_FOOTPRINT)
    {
        // RestoreOp will create both the TTL and Code/Data entry in the hot
        // archive case (which was converted to RESTORE above). However, when
        // restoring from live BucketList, only the TTL value will be modified,
        // so we have to manually insert the RESTORED meta for the Code/Data
        // entry here.
        for (auto const& [key, entry] : liveRestores)
        {
            if (key.type() == TTL)
            {
                continue;
            }
            releaseAssertOrThrow(isPersistentEntry(key));

            LedgerEntryChange change;
            change.type(LEDGER_ENTRY_RESTORED);
            change.restored() = entry;
            changes.push_back(change);
        }

        // RestoreOp can't modify entries
        releaseAssertOrThrow(stateChangesToAdd.empty());
        releaseAssertOrThrow(stateChangesToConvert.empty());
    }

    return changes;
}
} // namespace

void
OperationMetaBuilder::setLedgerChanges(LedgerEntryChanges const& initialChanges, stellar::UnorderedMap<stellar::LedgerKey, stellar::LedgerEntry> const& hotArchiveRestores,stellar::UnorderedMap<stellar::LedgerKey, stellar::LedgerEntry> const& liveRestores, uint32_t ledgerSeq)
{
    if (!mEnabled)
    {
        return;
    }
    std::visit(
        [&initialChanges, &hotArchiveRestores, &liveRestores, ledgerSeq, this](auto&& meta) {
            meta.get().changes = processOpLedgerEntryChanges(
                mConfig, mOp, initialChanges, hotArchiveRestores, liveRestores,
                mProtocolVersion, ledgerSeq);
        },
        mMeta);
}

void
OperationMetaBuilder::setSorobanReturnValue(SCVal const& val)
{
    if (!mEnabled)
    {
        return;
    }
    releaseAssertOrThrow(!mSorobanReturnValue);
    mSorobanReturnValue.emplace(val);
}

OpEventManager&
OperationMetaBuilder::getEventManager()
{
    return mEventManager;
}

DiagnosticEventManager&
OperationMetaBuilder::getDiagnosticEventManager()
{
    return mDiagnosticEventManager;
}

OperationMetaBuilder::OperationMetaBuilder(
    Config const& cfg, bool metaEnabled, OperationMeta& meta,
    OperationFrame const& op, uint32_t protocolVersion, Hash const& networkID,
    Config const& config, DiagnosticEventManager& diagnosticEventManager)
    : mEnabled(metaEnabled)
    , mProtocolVersion(protocolVersion)
    , mOp(op)
    , mMeta(meta)
    , mEventManager(metaEnabled, op.isSoroban(), protocolVersion, networkID,
                    op.getTxMemo(), config)
    , mDiagnosticEventManager(diagnosticEventManager)
    , mConfig(cfg)
{
}

OperationMetaBuilder::OperationMetaBuilder(
    Config const& cfg, bool metaEnabled, OperationMetaV2& meta,
    OperationFrame const& op, uint32_t protocolVersion, Hash const& networkID,
    Config const& config, DiagnosticEventManager& diagnosticEventManager)
    : mEnabled(metaEnabled)
    , mProtocolVersion(protocolVersion)
    , mOp(op)
    , mMeta(meta)
    , mEventManager(metaEnabled, op.isSoroban(), protocolVersion, networkID,
                    op.getTxMemo(), config)
    , mDiagnosticEventManager(diagnosticEventManager)
    , mConfig(cfg)
{
}

bool
OperationMetaBuilder::maybeFinalizeOpEvents()
{
    return std::visit(
        [this](auto&& meta) {
            using T = std::decay_t<decltype(meta)>;
            if constexpr (std::is_same_v<
                              T, std::reference_wrapper<OperationMetaV2>>)
            {
                meta.get().events = mEventManager.finalize();
                return true;
            }
            return false;
        },
        mMeta);
}

TransactionMetaBuilder::TransactionMetaWrapper::TransactionMetaWrapper(
    uint32_t protocolVersion, Config const& config)
{
    int version = 0;
    // The TransactionMeta v() switch can be in 5 positions 0, 1, 2, 3, 4. We do
    // not support 0 or 1 at all -- core does not produce it anymore and we have
    // no obligation to consume it under any circumstance -- so this class just
    // switches between cases 2, 3 and 4.
    if (protocolVersionStartsFrom(protocolVersion, ProtocolVersion::V_23) ||
        config.BACKFILL_STELLAR_ASSET_EVENTS)
    {
        version = 4;
    }
    else if (protocolVersionStartsFrom(protocolVersion,
                                       SOROBAN_PROTOCOL_VERSION))
    {
        version = 3;
    }
    else
    {
        version = 2;
    }
    mTransactionMeta.v(version);
}

LedgerEntryChanges&
TransactionMetaBuilder::TransactionMetaWrapper::getChangesBefore()
{
    switch (mTransactionMeta.v())
    {
    case 2:
        return mTransactionMeta.v2().txChangesBefore;
    case 3:
        return mTransactionMeta.v3().txChangesBefore;
    case 4:
        return mTransactionMeta.v4().txChangesBefore;
        break;
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaBuilder::TransactionMetaWrapper::setOperationMetas(
    xdr::xvector<OperationMeta>&& opMetas)
{
    switch (mTransactionMeta.v())
    {
    case 2:
        mTransactionMeta.v2().operations = std::move(opMetas);
        break;
    case 3:
        mTransactionMeta.v3().operations = std::move(opMetas);
        break;
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaBuilder::TransactionMetaWrapper::setOperationMetas(
    xdr::xvector<OperationMetaV2>&& opMetas)
{
    switch (mTransactionMeta.v())
    {
    case 4:
        mTransactionMeta.v4().operations = std::move(opMetas);
        break;
    default:
        releaseAssert(false);
    }
}

LedgerEntryChanges&
TransactionMetaBuilder::TransactionMetaWrapper::getChangesAfter()
{
    switch (mTransactionMeta.v())
    {
    case 2:
        return mTransactionMeta.v2().txChangesAfter;
    case 3:
        return mTransactionMeta.v3().txChangesAfter;
    case 4:
        return mTransactionMeta.v4().txChangesAfter;
    default:
        releaseAssert(false);
    }
}

SorobanTransactionMetaExt&
TransactionMetaBuilder::TransactionMetaWrapper::getSorobanMetaExt()
{
    switch (mTransactionMeta.v())
    {
    case 3:
        return mTransactionMeta.v3().sorobanMeta.activate().ext;
    case 4:
        return mTransactionMeta.v4().sorobanMeta.activate().ext;
    // Calling this before Soroban meta ext is available is a bug.
    case 2:
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaBuilder::TransactionMetaWrapper::setDiagnosticEvents(
    xdr::xvector<DiagnosticEvent>&& events)
{
    switch (mTransactionMeta.v())
    {
    case 3:
        mTransactionMeta.v3().sorobanMeta.activate().diagnosticEvents =
            std::move(events);
        break;
    case 4:
        mTransactionMeta.v4().diagnosticEvents = std::move(events);
        break;
    // It's a bug to call this when diagnostic events are not supported.
    case 2:
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaBuilder::TransactionMetaWrapper::maybeSetContractEventsAtTxLevel(
    xdr::xvector<ContractEvent>&& events)
{
    switch (mTransactionMeta.v())
    {
    case 2:
        // Do nothing, until v3 we don't create events.
        break;
    case 3:
        mTransactionMeta.v3().sorobanMeta.activate().events = std::move(events);
        break;
    case 4:
        // Do nothing, v4 contract events live in the operation meta
        break;
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaBuilder::TransactionMetaWrapper::maybeActivateSorobanMeta(
    bool success)
{
    switch (mTransactionMeta.v())
    {
    case 3:
        // In v3 meta we used to always have Soroban meta activated,
        // even in case of failure.
        mTransactionMeta.v3().sorobanMeta.activate();
        break;
    case 4:
        // From v4 we omit the meta for failed txs (it only contains
        // info related to success).
        if (success)
        {
            mTransactionMeta.v4().sorobanMeta.activate();
        }
        break;
    // It's a bug to call this when Soroban meta is not supported.
    case 2:
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaBuilder::TransactionMetaWrapper::setReturnValue(
    SCVal const& returnValue)
{
    switch (mTransactionMeta.v())
    {
    case 3:
        mTransactionMeta.v3().sorobanMeta.activate().returnValue = returnValue;
        break;
    case 4:
        mTransactionMeta.v4().sorobanMeta.activate().returnValue.activate() =
            returnValue;
        break;
    // Setting Soroban return value prior to meta v3 is a bug.
    case 2:
    default:
        releaseAssert(false);
    }
}

void
TransactionMetaBuilder::TransactionMetaWrapper::setTransactionEvents(
    xdr::xvector<TransactionEvent>&& events)
{
    switch (mTransactionMeta.v())
    {
    case 4:
        mTransactionMeta.v4().events = std::move(events);
        break;
    // Setting transaction events before meta v4 is a bug.
    case 2:
    case 3:
    default:
        releaseAssert(false);
    }
}

#ifdef BUILD_TESTS
TransactionMetaFrame::TransactionMetaFrame(TransactionMeta const& meta)
    : mTransactionMeta(meta)
{
}
size_t
TransactionMetaFrame::getNumOperations() const
{
    switch (mTransactionMeta.v())
    {
    case 2:
        return mTransactionMeta.v2().operations.size();
    case 3:
        return mTransactionMeta.v3().operations.size();
    case 4:
        return mTransactionMeta.v4().operations.size();
    default:
        releaseAssert(false);
    }
}

size_t
TransactionMetaFrame::getNumChangesBefore() const
{
    switch (mTransactionMeta.v())
    {
    case 2:
        return mTransactionMeta.v2().txChangesBefore.size();
    case 3:
        return mTransactionMeta.v3().txChangesBefore.size();
    case 4:
        return mTransactionMeta.v4().txChangesBefore.size();
    default:
        releaseAssert(false);
    }
}

LedgerEntryChanges
TransactionMetaFrame::getChangesBefore() const
{
    switch (mTransactionMeta.v())
    {
    case 2:
        return mTransactionMeta.v2().txChangesBefore;
    case 3:
        return mTransactionMeta.v3().txChangesBefore;
    case 4:
        return mTransactionMeta.v4().txChangesBefore;
    default:
        releaseAssert(false);
    }
}

LedgerEntryChanges
TransactionMetaFrame::getChangesAfter() const
{
    switch (mTransactionMeta.v())
    {
    case 2:
        return mTransactionMeta.v2().txChangesAfter;
    case 3:
        return mTransactionMeta.v3().txChangesAfter;
    case 4:
        return mTransactionMeta.v4().txChangesAfter;
    default:
        releaseAssert(false);
    }
}

SCVal const&
TransactionMetaFrame::getReturnValue() const
{
    switch (mTransactionMeta.v())
    {
    case 2:
        throw std::runtime_error("Return value not available for v2 meta");
    case 3:
        return mTransactionMeta.v3().sorobanMeta->returnValue;
    case 4:
        releaseAssert(mTransactionMeta.v4().sorobanMeta->returnValue);
        return *mTransactionMeta.v4().sorobanMeta->returnValue;
    default:
        releaseAssert(false);
    }
}

xdr::xvector<DiagnosticEvent> const&
TransactionMetaFrame::getDiagnosticEvents() const
{
    switch (mTransactionMeta.v())
    {
    case 2:
        throw std::runtime_error("Diagnostic events not available for v2 meta");
    case 3:
        return mTransactionMeta.v3().sorobanMeta->diagnosticEvents;
    case 4:
        return mTransactionMeta.v4().diagnosticEvents;
    default:
        releaseAssert(false);
    }
}

xdr::xvector<ContractEvent> const&
TransactionMetaFrame::getOpEventsAtOp(size_t opIdx) const
{
    switch (mTransactionMeta.v())
    {
    case 2:
    case 3:
        throw std::runtime_error(
            "Operation events not available for v2/v3 meta");
    case 4:
        return mTransactionMeta.v4().operations.at(opIdx).events;
    default:
        releaseAssert(false);
    }
}

xdr::xvector<TransactionEvent> const&
TransactionMetaFrame::getTxEvents() const
{
    switch (mTransactionMeta.v())
    {
    case 2:
    case 3:
        throw std::runtime_error(
            "Transaction-level contract events not available for v2/v3 meta");
    case 4:
        return mTransactionMeta.v4().events;
    default:
        releaseAssert(false);
    }
}

xdr::xvector<ContractEvent>
TransactionMetaFrame::getSorobanContractEvents() const
{
    switch (mTransactionMeta.v())
    {
    case 2:
        throw std::runtime_error("Contract events not available for v2 meta");
    case 3:
        return mTransactionMeta.v3().sorobanMeta->events;
    case 4:
        if (mTransactionMeta.v4().operations.empty())
        {
            return xdr::xvector<ContractEvent>{};
        }
        else if (mTransactionMeta.v4().operations.size() == 1)
        {
            return mTransactionMeta.v4().operations.at(0).events;
        }
        else
        {
            throw std::runtime_error("Operation meta size can only be 0 or 1 "
                                     "in a Soroban transaction");
        }
    default:
        releaseAssert(false);
    }
}

LedgerEntryChanges const&
TransactionMetaFrame::getLedgerEntryChangesAtOp(size_t opIdx) const
{
    switch (mTransactionMeta.v())
    {
    case 2:
        return mTransactionMeta.v2().operations.at(opIdx).changes;
    case 3:
        return mTransactionMeta.v3().operations.at(opIdx).changes;
    case 4:
        return mTransactionMeta.v4().operations.at(opIdx).changes;
    default:
        releaseAssert(false);
    }
}

TransactionMeta const&
TransactionMetaFrame::getXDR() const
{
    return mTransactionMeta;
}

#endif

TransactionMetaBuilder::TransactionMetaBuilder(bool metaEnabled,
                                               TransactionFrameBase const& tx,
                                               uint32_t protocolVersion,
                                               AppConnector const& app)
    : mTransactionMeta(protocolVersion, app.getConfig())
    , mTxEventManager(metaEnabled, protocolVersion, app.getNetworkID(),
                      app.getConfig())
    , mDiagnosticEventManager(DiagnosticEventManager::createForApply(
          metaEnabled, tx, app.getConfig()))
    , mIsSoroban(tx.isSoroban())
    , mEnabled(metaEnabled)
    , mSorobanMetaExtEnabled(
          app.getConfig().EMIT_SOROBAN_TRANSACTION_META_EXT_V1)
{
    auto const& operationFrames = tx.getOperationFrames();
    size_t numOperations = operationFrames.size();
    mOperationMetaBuilders.reserve(numOperations);

    switch (mTransactionMeta.mTransactionMeta.v())
    {
    case 2:
    case 3:
    {
        auto& opMeta = mOperationMetas.emplace<xdr::xvector<OperationMeta>>();
        opMeta.resize(numOperations);
        for (size_t i = 0; i < numOperations; ++i)
        {
            mOperationMetaBuilders.emplace_back(OperationMetaBuilder(
                app.getConfig(), metaEnabled, opMeta[i], *operationFrames[i],
                protocolVersion, app.getNetworkID(), app.getConfig(),
                mDiagnosticEventManager));
        }
        break;
    }
    case 4:
    {
        auto& opMeta = mOperationMetas.emplace<xdr::xvector<OperationMetaV2>>();
        opMeta.resize(numOperations);
        for (size_t i = 0; i < numOperations; ++i)
        {
            mOperationMetaBuilders.emplace_back(OperationMetaBuilder(
                app.getConfig(), metaEnabled, opMeta[i], *operationFrames[i],
                protocolVersion, app.getNetworkID(), app.getConfig(),
                mDiagnosticEventManager));
        }
        break;
    }
    default:
        releaseAssert(false);
    }
}

TxEventManager&
TransactionMetaBuilder::getTxEventManager()
{
    return mTxEventManager;
}

OperationMetaBuilder&
TransactionMetaBuilder::getOperationMetaBuilderAt(size_t i)
{
    return mOperationMetaBuilders.at(i);
}

void
TransactionMetaBuilder::pushTxChangesBefore(AbstractLedgerTxn& changesBeforeLtx)
{
    maybePushChanges(changesBeforeLtx, mTransactionMeta.getChangesBefore());
}

void
TransactionMetaBuilder::pushTxChangesAfter(AbstractLedgerTxn& changesAfterLtx)
{
    maybePushChanges(changesAfterLtx, mTransactionMeta.getChangesAfter());
}

void
TransactionMetaBuilder::setNonRefundableResourceFee(int64_t fee)
{
    if (mEnabled && mSorobanMetaExtEnabled)
    {
        releaseAssert(!mFinalized);
        auto& metaExt = mTransactionMeta.getSorobanMetaExt();
        metaExt.v(1);
        auto& sorobanMeta = metaExt.v1();
        sorobanMeta.totalNonRefundableResourceFeeCharged = fee;
    }
}

void
TransactionMetaBuilder::maybeSetRefundableFeeMeta(
    std::optional<RefundableFeeTracker> const& refundableFeeTracker)
{
    if (mEnabled && refundableFeeTracker && mSorobanMetaExtEnabled)
    {
        releaseAssert(!mFinalized);
        auto& metaExt = mTransactionMeta.getSorobanMetaExt();
        metaExt.v(1);
        auto& sorobanMeta = metaExt.v1();
        sorobanMeta.rentFeeCharged = refundableFeeTracker->getConsumedRentFee();
        sorobanMeta.totalRefundableResourceFeeCharged =
            refundableFeeTracker->getConsumedRefundableFee();
    }
}

DiagnosticEventManager&
TransactionMetaBuilder::getDiagnosticEventManager()
{
    return mDiagnosticEventManager;
}

TransactionMeta
TransactionMetaBuilder::finalize(bool success)
{
    // Finalizing the meta only makes sense when it's enabled in the first
    // place.
    releaseAssert(mEnabled);
    releaseAssert(!mFinalized);
    mFinalized = true;

    if (mIsSoroban)
    {
        // Maybe activate Soroban meta, depending on the meta version
        // and transaction success.
        mTransactionMeta.maybeActivateSorobanMeta(success);
    }

    // Operation meta is only populated when transaction succeeds.
    if (success)
    {
        // Some of the Soroban meta is sometimes set at the transaction level,
        // even though semantically it belongs to the operation. Here we
        // reconcile this information back into transaction meta.
        if (mIsSoroban)
        {
            // Currently there can only be 1 operation per Soroban transaction.
            releaseAssert(mOperationMetaBuilders.size() == 1);
            auto& opMetaBuilder = mOperationMetaBuilders[0];

            if (opMetaBuilder.mSorobanReturnValue)
            {
                mTransactionMeta.setReturnValue(
                    *opMetaBuilder.mSorobanReturnValue);
            }
            // Events have to be finalized either at operation level (to put
            // them into the op meta), or at transaction level for v1
            // OperationMeta.
            if (!opMetaBuilder.maybeFinalizeOpEvents())
            {
                mTransactionMeta.maybeSetContractEventsAtTxLevel(
                    opMetaBuilder.getEventManager().finalize());
            }
        }
        else
        {
            // For classic transactions we only need to finalize the events
            // at operation level (if events are enabled at all).
            for (auto& opMetaBuilder : mOperationMetaBuilders)
            {
                opMetaBuilder.maybeFinalizeOpEvents();
            }
        }

        std::visit(
            [this](auto&& metas) {
                mTransactionMeta.setOperationMetas(std::move(metas));
            },
            mOperationMetas);
    }
    // Write transaction-level events if they are enabled. Note, that this
    // doesn't depend on success of the transaction, as fees are charged
    // even for failed transactions.
    if (mTxEventManager.isEnabled())
    {
        mTransactionMeta.setTransactionEvents(mTxEventManager.finalize());
    }
    // Write diagnostic events if they're enabled. Note, that this doesn't
    // depend on the success of the transaction, as diagnostic events should
    // also be populated for the failed transactions.
    if (mDiagnosticEventManager.isEnabled())
    {
        mTransactionMeta.setDiagnosticEvents(
            mDiagnosticEventManager.finalize());
    }
    return std::move(mTransactionMeta.mTransactionMeta);
}

void
TransactionMetaBuilder::maybePushChanges(AbstractLedgerTxn& changesLtx,
                                         LedgerEntryChanges& destChanges)
{
    if (mEnabled)
    {
        releaseAssert(!mFinalized);
        vecAppend(destChanges, changesLtx.getChanges());
    }
}
}
