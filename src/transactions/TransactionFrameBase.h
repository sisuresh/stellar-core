// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include <optional>

#include "ledger/LedgerHashUtils.h"
#include "ledger/NetworkConfig.h"
#include "main/Config.h"
#include "overlay/StellarXDR.h"
#include "transactions/TransactionMetaFrame.h"
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

class MutableTransactionResultBase;
using MutableTxResultPtr = std::shared_ptr<MutableTransactionResultBase>;

class TransactionFrameBase;
using TransactionFrameBasePtr = std::shared_ptr<TransactionFrameBase const>;
using TransactionFrameBaseConstPtr =
    std::shared_ptr<TransactionFrameBase const>;

using ModifiedEntryMap = UnorderedMap<LedgerKey, std::optional<LedgerEntry>>;
using ThreadEntryMap =
    UnorderedMap<LedgerKey,
                 std::pair<std::optional<LedgerEntry>, bool /*dirty*/>>;

struct ParallelOpReturnVal
{
    bool mSuccess{false};
    ModifiedEntryMap mModifiedEntryMap;
    std::shared_ptr<LedgerTxnDelta> mDelta;
};

// temporary. Remove this
class TxBundle
{
  public:
    TxBundle(TransactionFrameBasePtr tx, MutableTxResultPtr resPayload,
             TransactionMetaFrame const& meta)
        : tx(tx), resPayload(resPayload), meta(meta)
    {
    }
    TransactionFrameBasePtr tx;
    MutableTxResultPtr resPayload;

    // TODO: Stop using mutable
    mutable TransactionMetaFrame meta;
    mutable std::shared_ptr<LedgerTxnDelta> mDelta;
};

typedef std::vector<TxBundle> Thread;
typedef std::vector<Thread> Stage;
typedef UnorderedMap<LedgerKey, TTLEntry> TTLs;

class TransactionFrameBase
{
  public:
    static TransactionFrameBasePtr
    makeTransactionFromWire(Hash const& networkID,
                            TransactionEnvelope const& env);

    virtual bool apply(Application& app, AbstractLedgerTxn& ltx,
                       TransactionMetaFrame& meta, MutableTxResultPtr txResult,
                       Hash const& sorobanBasePrngSeed = Hash{}) const = 0;

    virtual bool preParallelApply(Application& app, AbstractLedgerTxn& ltx,
                                  TransactionMetaFrame& meta,
                                  MutableTxResultPtr resPayload,
                                  bool chargeFee) const = 0;

    virtual ParallelOpReturnVal parallelApply(
        ThreadEntryMap const& entryMap, // Must not be shared between threads!,
        Config const& config, SorobanNetworkConfig const& sorobanConfig,
        CxxLedgerInfo const& ledgerInfo, MutableTxResultPtr resPayload,
        SorobanMetrics& sorobanMetrics, Hash const& sorobanBasePrngSeed,
        TransactionMetaFrame& meta, uint32_t ledgerSeq,
        uint32_t ledgerVersion) const = 0;

    virtual MutableTxResultPtr
    checkValid(Application& app, AbstractLedgerTxn& ltxOuter,
               SequenceNumber current, uint64_t lowerBoundCloseTimeOffset,
               uint64_t upperBoundCloseTimeOffset) const = 0;
    virtual bool
    checkSorobanResourceAndSetError(Application& app, uint32_t ledgerVersion,
                                    MutableTxResultPtr txResult) const = 0;

    virtual MutableTxResultPtr createSuccessResult() const = 0;

    virtual MutableTxResultPtr
    createSuccessResultWithFeeCharged(LedgerHeader const& header,
                                      std::optional<int64_t> baseFee,
                                      bool applying) const = 0;

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
    virtual Resource getResources(bool useByteLimitInClassic) const = 0;

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

    virtual void processPostApply(Application& app, AbstractLedgerTxn& ltx,
                                  TransactionMetaFrame& meta,
                                  MutableTxResultPtr txResult) const = 0;

    virtual std::shared_ptr<StellarMessage const> toStellarMessage() const = 0;

    virtual bool hasDexOperations() const = 0;

    virtual bool isSoroban() const = 0;
    virtual SorobanResources const& sorobanResources() const = 0;
    virtual int64 declaredSorobanResourceFee() const = 0;
    virtual bool XDRProvidesValidFee() const = 0;
};
}
