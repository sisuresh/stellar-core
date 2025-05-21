// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include <optional>

#include "ledger/LedgerHashUtils.h"
#include "ledger/LedgerStateSnapshot.h"
#include "ledger/NetworkConfig.h"
#include "main/Config.h"
#include "overlay/StellarXDR.h"
#include "util/TxResource.h"
#include "util/UnorderedSet.h"
#include "util/types.h"
#include <optional>

#include "ledger/SorobanMetrics.h"

namespace stellar
{
class AbstractLedgerTxn;
class Application;
class Database;
class OperationFrame;
class TransactionFrame;
class FeeBumpTransactionFrame;
class AppConnector;
class ParallelLedgerInfo;
class TxEffects;

class MutableTransactionResultBase;
using MutableTxResultPtr = std::unique_ptr<MutableTransactionResultBase>;

class TransactionMetaBuilder;
class DiagnosticEventManager;

class TransactionFrameBase;
using TransactionFrameBasePtr = std::shared_ptr<TransactionFrameBase const>;
using TransactionFrameBaseConstPtr =
    std::shared_ptr<TransactionFrameBase const>;

// Tracks entry updates within an operation. If the transaction succeeds, the
// ThreadEntryMap should be updated with the entries from the
// OpModifiedEntryMap.
using OpModifiedEntryMap = UnorderedMap<LedgerKey, std::optional<LedgerEntry>>;

// Used to track the current state of an entry within a thread. Can be updated
// by successful transactions.
struct ThreadEntry
{
    // Will not be set if the entry doesn't exist, or if no tx was able to load
    // it due to hitting read limits.
    std::optional<LedgerEntry> mLedgerEntry;
    bool isDirty;
};

// This is a map of all entries that will be read and/or written within a
// specific thread. applyThread can modify the entries in this map to reflect
// changes made by the transactions applied by that thread. Once all threads
// return, the updates from each threads entry map should be commited to
// LedgerTxn.
using ThreadEntryMap = UnorderedMap<LedgerKey, ThreadEntry>;

// Returned by each parallel transaction. It will contain the entries modified
// by the transaction, the success status of the transaction, and the keys
// restored.
class ParallelTxReturnVal
{
  public:
    ParallelTxReturnVal(bool success,
                        OpModifiedEntryMap const&& modifiedEntryMap)
        : mSuccess(success), mModifiedEntryMap(std::move(modifiedEntryMap))
    {
    }
    ParallelTxReturnVal(bool success,
                        OpModifiedEntryMap const&& modifiedEntryMap,
                        RestoredKeys const&& restoredKeys)
        : mSuccess(success)
        , mModifiedEntryMap(std::move(modifiedEntryMap))
        , mRestoredKeys(std::move(restoredKeys))
    {
    }

    bool
    getSuccess() const
    {
        return mSuccess;
    }
    OpModifiedEntryMap const&
    getModifiedEntryMap() const
    {
        return mModifiedEntryMap;
    }
    RestoredKeys const&
    getRestoredKeys() const
    {
        return mRestoredKeys;
    }

  private:
    bool mSuccess;
    // This will contain a key for every entry modified by a transaction
    OpModifiedEntryMap mModifiedEntryMap;
    RestoredKeys mRestoredKeys;
};

class TransactionFrameBase
{
  public:
    static TransactionFrameBasePtr
    makeTransactionFromWire(Hash const& networkID,
                            TransactionEnvelope const& env);

    virtual bool apply(AppConnector& app, AbstractLedgerTxn& ltx,
                       TransactionMetaBuilder& meta,
                       MutableTransactionResultBase& txResult,
                       Hash const& sorobanBasePrngSeed = Hash{}) const = 0;

    virtual void preloadEntriesForParallelApply(
        AppConnector& app, SorobanMetrics& sorobanMetrics,
        AbstractLedgerTxn& ltx, ThreadEntryMap& entryMap,
        MutableTransactionResultBase& txResult, DiagnosticEventManager& diagnosticEvents) const = 0;
    virtual void preParallelApply(AppConnector& app, AbstractLedgerTxn& ltx,
                                    TransactionMetaBuilder& meta,
                                    MutableTransactionResultBase& resPayload) const = 0;

    virtual ParallelTxReturnVal parallelApply(
        AppConnector& app,
        ThreadEntryMap const& entryMap, // Must not be shared between threads!,
        Config const& config, SorobanNetworkConfig const& sorobanConfig,
        ParallelLedgerInfo const& ledgerInfo, MutableTransactionResultBase& resPayload,
        SorobanMetrics& sorobanMetrics, Hash const& sorobanBasePrngSeed,
        TxEffects& effects) const = 0;

    virtual MutableTxResultPtr
    checkValid(AppConnector& app, LedgerSnapshot const& ls,
               SequenceNumber current, uint64_t lowerBoundCloseTimeOffset,
               uint64_t upperBoundCloseTimeOffset,
               DiagnosticEventManager& diagnosticEvents) const = 0;
    virtual bool
    checkSorobanResources(SorobanNetworkConfig const& cfg,
                          uint32_t ledgerVersion,
                          DiagnosticEventManager& diagnosticEvents) const = 0;

    virtual MutableTxResultPtr
    createTxErrorResult(TransactionResultCode txErrorCode) const = 0;

    virtual MutableTxResultPtr createValidationSuccessResult() const = 0;

    virtual TransactionEnvelope const& getEnvelope() const = 0;

#ifdef BUILD_TESTS
    virtual TransactionEnvelope& getMutableEnvelope() const = 0;
    virtual void clearCached() const = 0;
    virtual bool isTestTx() const = 0;
#endif

    // Returns the total fee of this transaction, including the 'flat',
    // non-market part.
    virtual int64_t getFullFee() const = 0;
    // Returns the part of the full fee used to make decisions as to
    // whether this transaction should be included into ledger.
    virtual int64_t getInclusionFee() const = 0;
    virtual int64_t getFee(LedgerHeader const& header,
                           std::optional<int64_t> baseFee,
                           bool applying) const = 0;

    virtual Hash const& getContentsHash() const = 0;
    virtual Hash const& getFullHash() const = 0;

    virtual uint32_t getNumOperations() const = 0;
    virtual std::vector<std::shared_ptr<OperationFrame const>> const&
    getOperationFrames() const = 0;
    virtual Resource getResources(bool useByteLimitInClassic,
                                  uint32_t ledgerVersion) const = 0;

    virtual std::vector<Operation> const& getRawOperations() const = 0;

    virtual SequenceNumber getSeqNum() const = 0;
    virtual AccountID getFeeSourceID() const = 0;
    virtual AccountID getSourceID() const = 0;
    virtual std::optional<SequenceNumber const> const getMinSeqNum() const = 0;
    virtual Duration getMinSeqAge() const = 0;
    virtual uint32 getMinSeqLedgerGap() const = 0;

    virtual void
    insertKeysForFeeProcessing(UnorderedSet<LedgerKey>& keys) const = 0;
    virtual void insertKeysForTxApply(UnorderedSet<LedgerKey>& keys,
                                      LedgerKeyMeter* lkMeter) const = 0;

    virtual MutableTxResultPtr
    processFeeSeqNum(AbstractLedgerTxn& ltx,
                     std::optional<int64_t> baseFee) const = 0;

    // After this transaction has been applied
    virtual void
    processPostApply(AppConnector& app, AbstractLedgerTxn& ltx,
                     TransactionMetaBuilder& meta,
                     MutableTransactionResultBase& txResult) const = 0;

    // After all transactions have been applied
    virtual void
    processPostTxSetApply(AppConnector& app, AbstractLedgerTxn& ltx,
                          MutableTransactionResultBase& txResult,
                          TxEventManager& txEventManager) const = 0;

    virtual std::shared_ptr<StellarMessage const> toStellarMessage() const = 0;

    virtual bool hasDexOperations() const = 0;

    virtual bool isSoroban() const = 0;
    virtual SorobanResources const& sorobanResources() const = 0;
    virtual SorobanTransactionData::_ext_t const& getResourcesExt() const = 0;
    virtual int64 declaredSorobanResourceFee() const = 0;
    virtual bool XDRProvidesValidFee() const = 0;
};
}
