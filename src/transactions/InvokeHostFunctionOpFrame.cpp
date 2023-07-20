// Copyright 2022 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

// clang-format off
// This needs to be included first
#include "util/GlobalChecks.h"
#include "xdr/Stellar-ledger-entries.h"
#include <cstdint>
#include <json/json.h>
#include <medida/metrics_registry.h>
#include <xdrpp/types.h>
#ifdef ENABLE_NEXT_PROTOCOL_VERSION_UNSAFE_FOR_PRODUCTION
#include "rust/RustVecXdrMarshal.h"
#endif
// clang-format on

#include "ledger/LedgerTxnImpl.h"
#include "rust/CppShims.h"
#include "xdr/Stellar-transaction.h"
#include <stdexcept>
#include <xdrpp/xdrpp/printer.h>

#ifdef ENABLE_NEXT_PROTOCOL_VERSION_UNSAFE_FOR_PRODUCTION
#include "ledger/LedgerTxn.h"
#include "ledger/LedgerTxnEntry.h"
#include "rust/RustBridge.h"
#include "transactions/InvokeHostFunctionOpFrame.h"
#include <crypto/SHA.h>

namespace stellar
{
namespace
{
struct LedgerEntryRentState
{
    bool readOnly{};
    uint32_t oldExpirationLedger{};
    uint32_t newExpirationLedger{};
    uint32_t oldSize{};
    uint32_t newSize{};
};

bool
isCodeKey(LedgerKey const& lk)
{
    return lk.type() == CONTRACT_CODE;
}

template <typename T>
std::vector<uint8_t>
toVec(T const& t)
{
    return std::vector<uint8_t>(xdr::xdr_to_opaque(t));
}

template <typename T>
CxxBuf
toCxxBuf(T const& t)
{
    return CxxBuf{std::make_unique<std::vector<uint8_t>>(toVec(t))};
}

CxxLedgerInfo
getLedgerInfo(AbstractLedgerTxn& ltx, Config const& cfg,
              SorobanNetworkConfig const& sorobanConfig)
{
    CxxLedgerInfo info;
    auto const& hdr = ltx.loadHeader().current();
    info.base_reserve = hdr.baseReserve;
    info.protocol_version = hdr.ledgerVersion;
    info.sequence_number = hdr.ledgerSeq;
    info.timestamp = hdr.scpValue.closeTime;
    info.memory_limit = sorobanConfig.txMemoryLimit();
    info.min_persistent_entry_expiration =
        sorobanConfig.stateExpirationSettings().minPersistentEntryExpiration;
    info.min_temp_entry_expiration =
        sorobanConfig.stateExpirationSettings().minTempEntryExpiration;
    info.cpu_cost_params = toCxxBuf(sorobanConfig.cpuCostParams());
    info.mem_cost_params = toCxxBuf(sorobanConfig.memCostParams());
    // TODO: move network id to config to not recompute hash
    auto networkID = sha256(cfg.NETWORK_PASSPHRASE);
    for (auto c : networkID)
    {
        info.network_id.push_back(static_cast<unsigned char>(c));
    }
    return info;
}

bool
validateContractLedgerEntry(LedgerEntry const& le, size_t entrySize,
                            SorobanNetworkConfig const& config)
{
    releaseAssertOrThrow(!isSorobanEntry(le.data) ||
                         getLeType(le.data) == DATA_ENTRY);

    // check contract code size limit
    if (le.data.type() == CONTRACT_CODE &&
        config.maxContractSizeBytes() <
            le.data.contractCode().body.code().size())
    {
        return false;
    }
    // check contract data entry size limit
    if (le.data.type() == CONTRACT_DATA &&
        config.maxContractDataEntrySizeBytes() < entrySize)
    {
        return false;
    }
    return true;
}

} // namespace

InvokeHostFunctionOpFrame::InvokeHostFunctionOpFrame(Operation const& op,
                                                     OperationResult& res,
                                                     TransactionFrame& parentTx)
    : OperationFrame(op, res, parentTx)
    , mInvokeHostFunction(mOperation.body.invokeHostFunctionOp())
{
}

bool
InvokeHostFunctionOpFrame::isOpSupported(LedgerHeader const& header) const
{
    return header.ledgerVersion >= 20;
}

bool
InvokeHostFunctionOpFrame::doApply(AbstractLedgerTxn& ltx)
{
    throw std::runtime_error(
        "InvokeHostFunctionOpFrame::doApply needs Config and base PRNG seed");
}

void
InvokeHostFunctionOpFrame::maybePopulateDiagnosticEvents(
    Config const& cfg, InvokeHostFunctionOutput const& output)
{
    if (cfg.ENABLE_SOROBAN_DIAGNOSTIC_EVENTS)
    {
        xdr::xvector<DiagnosticEvent> diagnosticEvents;
        for (auto const& e : output.diagnostic_events)
        {
            DiagnosticEvent evt;
            xdr::xdr_from_opaque(e.data, evt);
            diagnosticEvents.emplace_back(evt);
        }
        mParentTx.pushDiagnosticEvents(std::move(diagnosticEvents));
    }
}

struct HostFunctionMetrics
{
    medida::MetricsRegistry& mMetrics;

    uint32 mReadEntry{0};
    uint32 mWriteEntry{0};

    uint32 mLedgerReadByte{0};
    uint32 mLedgerWriteByte{0};

    uint32 mReadKeyByte{0};
    uint32 mWriteKeyByte{0};

    uint32 mReadDataByte{0};
    uint32 mWriteDataByte{0};

    uint32 mReadCodeByte{0};
    uint32 mWriteCodeByte{0};

    uint32 mEmitEvent{0};
    uint32 mEmitEventByte{0};

    uint32 mCpuInsn{0};
    uint32 mMemByte{0};

    uint32 mMetadataSizeByte{0};

    bool mSuccess{false};

    HostFunctionMetrics(medida::MetricsRegistry& metrics) : mMetrics(metrics)
    {
    }

    void
    noteReadEntry(bool isCodeEntry, uint32 keySize, uint32 entrySize)
    {
        mReadEntry++;
        mReadKeyByte += keySize;
        mLedgerReadByte += keySize + entrySize;
        if (isCodeEntry)
        {
            mReadCodeByte += keySize + entrySize;
        }
        else
        {
            mReadDataByte += keySize + entrySize;
        }
    }

    void
    noteWriteEntry(bool isCodeEntry, uint32 keySize, uint32 entrySize)
    {
        mWriteEntry++;
        mWriteKeyByte += keySize;
        mLedgerWriteByte += keySize + entrySize;
        if (isCodeEntry)
        {
            mWriteCodeByte += keySize + entrySize;
        }
        else
        {
            mWriteDataByte += keySize + entrySize;
        }
    }

    ~HostFunctionMetrics()
    {
        mMetrics.NewMeter({"soroban", "host-fn-op", "read-entry"}, "entry")
            .Mark(mReadEntry);
        mMetrics.NewMeter({"soroban", "host-fn-op", "write-entry"}, "entry")
            .Mark(mWriteEntry);

        mMetrics.NewMeter({"soroban", "host-fn-op", "read-key-byte"}, "byte")
            .Mark(mReadKeyByte);
        mMetrics.NewMeter({"soroban", "host-fn-op", "write-key-byte"}, "byte")
            .Mark(mWriteKeyByte);

        mMetrics.NewMeter({"soroban", "host-fn-op", "read-ledger-byte"}, "byte")
            .Mark(mLedgerReadByte);
        mMetrics.NewMeter({"soroban", "host-fn-op", "read-data-byte"}, "byte")
            .Mark(mReadDataByte);
        mMetrics.NewMeter({"soroban", "host-fn-op", "read-code-byte"}, "byte")
            .Mark(mReadCodeByte);

        mMetrics
            .NewMeter({"soroban", "host-fn-op", "write-ledger-byte"}, "byte")
            .Mark(mLedgerWriteByte);
        mMetrics.NewMeter({"soroban", "host-fn-op", "write-data-byte"}, "byte")
            .Mark(mWriteDataByte);
        mMetrics.NewMeter({"soroban", "host-fn-op", "write-code-byte"}, "byte")
            .Mark(mWriteCodeByte);

        mMetrics.NewMeter({"soroban", "host-fn-op", "emit-event"}, "event")
            .Mark(mEmitEvent);
        mMetrics.NewMeter({"soroban", "host-fn-op", "emit-event-byte"}, "byte")
            .Mark(mEmitEventByte);

        mMetrics
            .NewMeter({"soroban", "host-fn-op", "metadata-size-byte"}, "byte")
            .Mark(mMetadataSizeByte);

        mMetrics.NewMeter({"soroban", "host-fn-op", "cpu-insn"}, "insn")
            .Mark(mCpuInsn);
        mMetrics.NewMeter({"soroban", "host-fn-op", "mem-byte"}, "byte")
            .Mark(mMemByte);

        if (mSuccess)
        {
            mMetrics.NewMeter({"soroban", "host-fn-op", "success"}, "call")
                .Mark();
        }
        else
        {
            mMetrics.NewMeter({"soroban", "host-fn-op", "failure"}, "call")
                .Mark();
        }
    }
    medida::TimerContext
    getExecTimer()
    {
        return mMetrics.NewTimer({"soroban", "host-fn-op", "exec"}).TimeScope();
    }
};

bool
InvokeHostFunctionOpFrame::doApply(Application& app, AbstractLedgerTxn& ltx,
                                   Hash const& sorobanBasePrngSeed)
{
    Config const& cfg = app.getConfig();
    HostFunctionMetrics metrics(app.getMetrics());
    auto const& sorobanConfig =
        app.getLedgerManager().getSorobanNetworkConfig(ltx);

    // Get the entries for the footprint
    rust::Vec<CxxBuf> ledgerEntryCxxBufs;
    UnorderedMap<LedgerKey, LedgerEntryRentState> entryRentChanges;

    auto const& resources = mParentTx.sorobanResources();
    auto const& footprint = resources.footprint;
    auto footprintLength =
        footprint.readOnly.size() + footprint.readWrite.size();

    uint32_t ledgerSeq = ltx.loadHeader().current().ledgerSeq;
    uint32_t autobumpLedgerCount =
        sorobanConfig.stateExpirationSettings().autoBumpLedgers;

    ledgerEntryCxxBufs.reserve(footprintLength);

    auto addReads = [&ledgerEntryCxxBufs, &ltx, &metrics, &entryRentChanges,
                     &resources, autobumpLedgerCount,
                     this](auto const& keys, bool readOnly) -> bool {
        for (auto const& lk : keys)
        {
            uint32 keySize = static_cast<uint32>(xdr::xdr_size(lk));
            uint32 entrySize = 0u;
            auto& entryRentChange = entryRentChanges[lk];
            // The invariant is that the footprint is unique, so we can't
            // accidentally override RW entry with RO flag.
            entryRentChange.readOnly = readOnly;
            // Load without record for readOnly to avoid writing them later
            auto ltxe = ltx.loadWithoutRecord(lk, /*loadExpiredEntry=*/false);
            if (ltxe)
            {
                auto const& le = ltxe.current();
                auto buf = toCxxBuf(le);
                entrySize = static_cast<uint32>(buf.data->size());
                ledgerEntryCxxBufs.emplace_back(std::move(buf));
                if (isSorobanEntry(le.data))
                {
                    uint32_t const totalReadSize = keySize + entrySize;
                    entryRentChange.oldSize = totalReadSize;
                    entryRentChange.newSize = totalReadSize;

                    entryRentChange.oldExpirationLedger =
                        getExpirationLedger(le);
                    entryRentChange.newExpirationLedger =
                        entryRentChange.oldExpirationLedger;
                    if (autobumpLedgerCount > 0 && autoBumpEnabled(le))
                    {
                        // Add the autobump ledgers on top of the old
                        // expiration. Since expiration is inclusive, the rent
                        // must be already payed for `oldExpirationLedger`.
                        entryRentChange.newExpirationLedger +=
                            autobumpLedgerCount;
                    }
                }
            }
            else if (!isTemporaryEntry(lk) &&
                     ltx.loadWithoutRecord(lk, /*loadExpiredEntry=*/true))
            {
                // Cannot access an expired entry
                this->innerResult().code(INVOKE_HOST_FUNCTION_ENTRY_EXPIRED);
                return false;
            }
            metrics.noteReadEntry(isCodeKey(lk), keySize, entrySize);

            if (resources.readBytes < metrics.mLedgerReadByte)
            {
                this->innerResult().code(
                    INVOKE_HOST_FUNCTION_RESOURCE_LIMIT_EXCEEDED);
                return false;
            }
        }
        return true;
    };

    if (!addReads(footprint.readWrite, false))
    {
        // Error code set in addReads
        return false;
    }
    // Metadata includes the ledger entry changes which we
    // approximate as the size of the RW entries that we read
    // plus size of the RW entries that we write back to the ledger.
    metrics.mMetadataSizeByte += metrics.mLedgerReadByte;
    if (resources.extendedMetaDataSizeBytes < metrics.mMetadataSizeByte)
    {
        innerResult().code(INVOKE_HOST_FUNCTION_RESOURCE_LIMIT_EXCEEDED);
        return false;
    }

    if (!addReads(footprint.readOnly, true))
    {
        // Error code set in addReads
        return false;
    }

    CxxBuf hostFnCxxBuf = toCxxBuf(mInvokeHostFunction.hostFunction);
    rust::Vec<CxxBuf> authEntryCxxBufs;
    authEntryCxxBufs.reserve(mInvokeHostFunction.auth.size());
    for (auto const& authEntry : mInvokeHostFunction.auth)
    {
        authEntryCxxBufs.push_back(toCxxBuf(authEntry));
    }

    InvokeHostFunctionOutput out{};
    try
    {
        auto timeScope = metrics.getExecTimer();
        CxxBuf basePrngSeedBuf;
        basePrngSeedBuf.data = std::make_unique<std::vector<uint8_t>>();
        basePrngSeedBuf.data->assign(sorobanBasePrngSeed.begin(),
                                     sorobanBasePrngSeed.end());

        out = rust_bridge::invoke_host_function(
            cfg.CURRENT_LEDGER_PROTOCOL_VERSION,
            cfg.ENABLE_SOROBAN_DIAGNOSTIC_EVENTS, hostFnCxxBuf,
            toCxxBuf(resources), toCxxBuf(getSourceID()), authEntryCxxBufs,
            getLedgerInfo(ltx, cfg, sorobanConfig), ledgerEntryCxxBufs,
            basePrngSeedBuf);

        if (out.success)
        {
            metrics.mSuccess = true;
        }
        else
        {
            maybePopulateDiagnosticEvents(cfg, out);
        }
    }
    catch (std::exception& e)
    {
        CLOG_DEBUG(Tx, "Exception caught while invoking host fn: {}", e.what());
    }

    metrics.mCpuInsn = static_cast<uint32>(out.cpu_insns);
    metrics.mMemByte = static_cast<uint32>(out.mem_bytes);
    if (!metrics.mSuccess)
    {
        if (resources.instructions < out.cpu_insns ||
            sorobanConfig.txMemoryLimit() < out.mem_bytes)
        {
            innerResult().code(INVOKE_HOST_FUNCTION_RESOURCE_LIMIT_EXCEEDED);
        }
        else
        {
            innerResult().code(INVOKE_HOST_FUNCTION_TRAPPED);
        }
        return false;
    }

    // Create or update every entry returned.
    UnorderedSet<LedgerKey> remainingRWKeys;
    for (auto const& buf : out.modified_ledger_entries)
    {
        LedgerEntry le;
        xdr::xdr_from_opaque(buf.data, le);
        if (!validateContractLedgerEntry(le, buf.data.size(), sorobanConfig))
        {
            innerResult().code(INVOKE_HOST_FUNCTION_RESOURCE_LIMIT_EXCEEDED);
            return false;
        }

        auto lk = LedgerEntryKey(le);
        remainingRWKeys.insert(lk);

        uint32 keySize = static_cast<uint32>(xdr::xdr_size(lk));
        uint32 entrySize = static_cast<uint32>(buf.data.size());
        metrics.noteWriteEntry(isCodeKey(lk), keySize, entrySize);
        if (resources.writeBytes < metrics.mLedgerWriteByte)
        {
            innerResult().code(INVOKE_HOST_FUNCTION_RESOURCE_LIMIT_EXCEEDED);
            return false;
        }

        // Temp entries can be overwritten even when expired
        auto ltxe = ltx.load(lk, /*loadExpiredEntry=*/true);
        if (ltxe)
        {
            ltxe.current() = le;
        }
        else
        {
            ltx.create(le);
        }

        if (isSorobanDataEntry(lk))
        {
            auto entryIt = entryRentChanges.find(lk);
            releaseAssertOrThrow(entryIt != entryRentChanges.end());
            entryIt->second.newSize =
                keySize + static_cast<uint32>(buf.data.size());
            entryIt->second.newExpirationLedger = std::max(
                entryIt->second.newExpirationLedger, getExpirationLedger(le));
        }
    }

    metrics.mMetadataSizeByte += metrics.mLedgerWriteByte;
    if (resources.extendedMetaDataSizeBytes < metrics.mMetadataSizeByte)
    {
        innerResult().code(INVOKE_HOST_FUNCTION_RESOURCE_LIMIT_EXCEEDED);
        return false;
    }

    // Erase every entry not returned.
    // NB: The entries that haven't been touched are passed through
    // from host, so this should never result in removing an entry
    // that hasn't been removed by host explicitly.
    for (auto const& lk : footprint.readWrite)
    {
        if (remainingRWKeys.find(lk) == remainingRWKeys.end())
        {
            auto ltxe = ltx.load(lk);
            if (ltxe)
            {
                ltx.erase(lk);
            }
        }
    }

    // Apply expiration bumps.
    for (auto const& bump : out.expiration_bumps)
    {
        LedgerKey lk;
        xdr::xdr_from_opaque(bump.ledger_key.data, lk);
        releaseAssertOrThrow(isSorobanDataEntry(lk));
        auto entryIt = entryRentChanges.find(lk);
        releaseAssertOrThrow(entryIt != entryRentChanges.end());
        entryIt->second.newExpirationLedger =
            std::max(bump.min_expiration, entryIt->second.newExpirationLedger);
    }

    uint32_t maxExpirationLedger =
        ledgerSeq + sorobanConfig.stateExpirationSettings().maxEntryExpiration -
        1;
    uint32_t minPersistentExpirationLedger =
        ledgerSeq +
        sorobanConfig.stateExpirationSettings().minPersistentEntryExpiration -
        1;
    uint32_t minTempExpirationLedger =
        ledgerSeq +
        sorobanConfig.stateExpirationSettings().minTempEntryExpiration - 1;

    rust::Vec<CxxLedgerEntryRentChange> rustEntryRentChanges;
    rustEntryRentChanges.reserve(remainingRWKeys.size() +
                                 resources.footprint.readOnly.size());
    // Perform actual expiration bumps and compute fee computation inputs.
    for (auto& [lk, entryChange] : entryRentChanges)
    {
        // Skip deleted RW entries.
        if (!entryChange.readOnly &&
            remainingRWKeys.find(lk) == remainingRWKeys.end())
        {
            continue;
        }
        bool isTemporary = isTemporaryEntry(lk);
        // Enforce minimum expiration for the new entries.
        if (entryChange.oldSize == 0)
        {
            if (isTemporary)
            {
                entryChange.newExpirationLedger = std::min(
                    entryChange.newExpirationLedger, minTempExpirationLedger);
            }
            else
            {
                entryChange.newExpirationLedger =
                    std::min(entryChange.newExpirationLedger,
                             minPersistentExpirationLedger);
            }
        }
        entryChange.newExpirationLedger =
            std::min(entryChange.newExpirationLedger, maxExpirationLedger);
        // If the entry didn't grow and wasn't bumped, then there is no reason
        // to charge any fees for it.
        if (entryChange.oldExpirationLedger ==
                entryChange.newExpirationLedger &&
            entryChange.oldSize >= entryChange.newSize)
        {
            continue;
        }
        if (entryChange.oldExpirationLedger < entryChange.newExpirationLedger)
        {

            // Bumped read-only entries should only use the expiration extension
            // entries, so just charge for the extension entry change (roughly
            // 2x the key size + small constant that we give for free for
            // simplicity).
            if (entryChange.readOnly)
            {
                metrics.mMetadataSizeByte +=
                    2 * static_cast<uint32>(xdr::xdr_size(lk));
            }
            auto ltxe = ltx.load(lk);
            releaseAssertOrThrow(ltxe);

            // TODO: this should use expiration extension for RO entries.
            setExpirationLedger(ltxe.current(),
                                entryChange.newExpirationLedger);
        }

        rustEntryRentChanges.emplace_back();
        auto& rustChange = rustEntryRentChanges.back();
        rustChange.is_persistent = !isTemporary;
        rustChange.old_size_bytes = entryChange.oldSize;
        rustChange.new_size_bytes = entryChange.newSize;
        rustChange.old_expiration_ledger = entryChange.oldExpirationLedger;
        rustChange.new_expiration_ledger = entryChange.newExpirationLedger;
    }
    if (resources.extendedMetaDataSizeBytes < metrics.mMetadataSizeByte)
    {
        innerResult().code(INVOKE_HOST_FUNCTION_RESOURCE_LIMIT_EXCEEDED);
        return false;
    }

    // Append events to the enclosing TransactionFrame, where
    // they'll be picked up and transferred to the TxMeta.
    InvokeHostFunctionSuccessPreImage success;
    for (auto const& buf : out.contract_events)
    {
        metrics.mEmitEvent++;
        metrics.mEmitEventByte += static_cast<uint32>(buf.data.size());
        metrics.mMetadataSizeByte += static_cast<uint32>(buf.data.size());
        if (resources.extendedMetaDataSizeBytes < metrics.mMetadataSizeByte)
        {
            innerResult().code(INVOKE_HOST_FUNCTION_RESOURCE_LIMIT_EXCEEDED);
            return false;
        }
        ContractEvent evt;
        xdr::xdr_from_opaque(buf.data, evt);
        success.events.emplace_back(evt);
    }

    maybePopulateDiagnosticEvents(cfg, out);
    // This may throw, but only in case of the Core version misconfiguration.
    int64_t rentFee = rust_bridge::compute_rent_fee(
        cfg.CURRENT_LEDGER_PROTOCOL_VERSION,
        ltx.loadHeader().current().ledgerVersion, rustEntryRentChanges,
        sorobanConfig.rustBridgeRentFeeConfiguration(), ledgerSeq);
    mParentTx.consumeRefundableSorobanResources(metrics.mMetadataSizeByte,
                                                rentFee);

    xdr::xdr_from_opaque(out.result_value.data, success.returnValue);
    innerResult().code(INVOKE_HOST_FUNCTION_SUCCESS);
    innerResult().success() = xdrSha256(success);

    mParentTx.pushContractEvents(std::move(success.events));
    mParentTx.setReturnValue(std::move(success.returnValue));
    return true;
}

bool
InvokeHostFunctionOpFrame::doCheckValid(SorobanNetworkConfig const& config,
                                        uint32_t ledgerVersion)
{
    // check wasm size if uploading contract
    auto const& hostFn = mInvokeHostFunction.hostFunction;
    if (hostFn.type() == HOST_FUNCTION_TYPE_UPLOAD_CONTRACT_WASM &&
        hostFn.wasm().size() > config.maxContractSizeBytes())
    {
        return false;
    }
    if (hostFn.type() == HOST_FUNCTION_TYPE_CREATE_CONTRACT)
    {
        auto const& preimage = hostFn.createContract().contractIDPreimage;
        if (preimage.type() == CONTRACT_ID_PREIMAGE_FROM_ASSET &&
            !isAssetValid(preimage.fromAsset(), ledgerVersion))
        {
            return false;
        }
    }
    return true;
}

bool
InvokeHostFunctionOpFrame::doCheckValid(uint32_t ledgerVersion)
{
    throw std::runtime_error(
        "InvokeHostFunctionOpFrame::doCheckValid needs Config");
}

void
InvokeHostFunctionOpFrame::insertLedgerKeysToPrefetch(
    UnorderedSet<LedgerKey>& keys) const
{
}

bool
InvokeHostFunctionOpFrame::isSoroban() const
{
    return true;
}
}
#endif // ENABLE_NEXT_PROTOCOL_VERSION_UNSAFE_FOR_PRODUCTION
