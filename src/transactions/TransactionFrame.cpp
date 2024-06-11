// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "util/asio.h"
#include "TransactionFrame.h"
#include "OperationFrame.h"
#include "crypto/Hex.h"
#include "crypto/SHA.h"
#include "crypto/SignerKey.h"
#include "crypto/SignerKeyUtils.h"
#include "database/Database.h"
#include "database/DatabaseUtils.h"
#include "herder/TxSetFrame.h"
#include "invariant/InvariantDoesNotHold.h"
#include "invariant/InvariantManager.h"
#include "ledger/LedgerHeaderUtils.h"
#include "ledger/LedgerTxn.h"
#include "ledger/LedgerTxnEntry.h"
#include "ledger/LedgerTxnHeader.h"
#include "ledger/SorobanMetrics.h"
#include "main/Application.h"
#include "transactions/MutableTransactionResult.h"
#include "transactions/SignatureChecker.h"
#include "transactions/SignatureUtils.h"
#include "transactions/SponsorshipUtils.h"
#include "transactions/TransactionBridge.h"
#include "transactions/TransactionMetaFrame.h"
#include "transactions/TransactionUtils.h"
#include "util/Decoder.h"
#include "util/GlobalChecks.h"
#include "util/Logging.h"
#include "util/ProtocolVersion.h"
#include "util/XDROperators.h"
#include "util/XDRStream.h"
#include "xdr/Stellar-contract.h"
#include "xdr/Stellar-ledger.h"
#include "xdrpp/marshal.h"
#include "xdrpp/printer.h"
#include <Tracy.hpp>
#include <iterator>
#include <string>

#include "medida/meter.h"
#include "medida/metrics_registry.h"

#include <algorithm>
#include <numeric>
#include <xdrpp/types.h>

namespace stellar
{
namespace
{
// Limit to the maximum resource fee allowed for transaction,
// roughly 112 million lumens.
int64_t const MAX_RESOURCE_FEE = 1LL << 50;
} // namespace

using namespace std;
using namespace stellar::txbridge;

namespace
{
bool
hasDexOperationsInternal(
    std::vector<std::shared_ptr<OperationFrame>> const& ops)
{
    for (auto const& op : ops)
    {
        if (op->isDexOperation())
        {
            return true;
        }
    }
    return false;
}

bool
isSorobanInternal(std::vector<std::shared_ptr<OperationFrame>> const& ops)
{
    return !ops.empty() && ops[0]->isSoroban();
}

bool
validateSorobanOpsConsistencyInternal(
    std::vector<std::shared_ptr<OperationFrame>> const& ops)
{
    bool hasSorobanOp = isSorobanInternal(ops);
    for (auto const& op : ops)
    {
        bool isSorobanOp = op->isSoroban();
        // Mixing Soroban ops with non-Soroban ops is not allowed.
        if (isSorobanOp != hasSorobanOp)
        {
            return false;
        }
    }
    // Only one operation is allowed per Soroban transaction.
    if (hasSorobanOp && ops.size() != 1)
    {
        return false;
    }
    return true;
}
}

TransactionFrame::TransactionFrame(Hash const& networkID,
                                   TransactionEnvelope const& envelope)
    : mEnvelope(envelope), mNetworkID(networkID)
{
    // We need information from the underlying OperationFrame objects, but we
    // don't want to store mutable state, so use a throw away ResultPayload.
    auto dummyPayload = createResultPayload();
    mHasDexOperations = hasDexOperationsInternal(dummyPayload->getOpFrames());
    mIsSoroban = isSorobanInternal(dummyPayload->getOpFrames());
    mHasValidSorobanOpsConsistency =
        validateSorobanOpsConsistencyInternal(dummyPayload->getOpFrames());
}

Hash const&
TransactionFrame::getFullHash() const
{
    ZoneScoped;
    if (isZero(mFullHash))
    {
        mFullHash = xdrSha256(mEnvelope);
    }
    return (mFullHash);
}

Hash const&
TransactionFrame::getContentsHash() const
{
    ZoneScoped;
#ifdef _DEBUG
    // force recompute
    Hash oldHash;
    std::swap(mContentsHash, oldHash);
#endif

    if (isZero(mContentsHash))
    {
        if (mEnvelope.type() == ENVELOPE_TYPE_TX_V0)
        {
            mContentsHash = sha256(xdr::xdr_to_opaque(
                mNetworkID, ENVELOPE_TYPE_TX, 0, mEnvelope.v0().tx));
        }
        else
        {
            mContentsHash = sha256(xdr::xdr_to_opaque(
                mNetworkID, ENVELOPE_TYPE_TX, mEnvelope.v1().tx));
        }
    }
#ifdef _DEBUG
    releaseAssert(isZero(oldHash) || (oldHash == mContentsHash));
#endif
    return (mContentsHash);
}

TransactionEnvelope const&
TransactionFrame::getEnvelope() const
{
    return mEnvelope;
}

#ifdef BUILD_TESTS
TransactionEnvelope&
TransactionFrame::getMutableEnvelope() const
{
    return mEnvelope;
}

void
TransactionFrame::clearCached() const
{
    Hash zero;
    mContentsHash = zero;
    mFullHash = zero;
}
#endif

SequenceNumber
TransactionFrame::getSeqNum() const
{
    return mEnvelope.type() == ENVELOPE_TYPE_TX_V0 ? mEnvelope.v0().tx.seqNum
                                                   : mEnvelope.v1().tx.seqNum;
}

AccountID
TransactionFrame::getFeeSourceID() const
{
    return getSourceID();
}

AccountID
TransactionFrame::getSourceID() const
{
    if (mEnvelope.type() == ENVELOPE_TYPE_TX_V0)
    {
        AccountID res;
        res.ed25519() = mEnvelope.v0().tx.sourceAccountEd25519;
        return res;
    }
    return toAccountID(mEnvelope.v1().tx.sourceAccount);
}

uint32_t
TransactionFrame::getNumOperations() const
{
    return mEnvelope.type() == ENVELOPE_TYPE_TX_V0
               ? static_cast<uint32_t>(mEnvelope.v0().tx.operations.size())
               : static_cast<uint32_t>(mEnvelope.v1().tx.operations.size());
}

Resource
TransactionFrame::getResources(bool useByteLimitInClassic) const
{
    auto txSize = static_cast<int64_t>(this->getSize());
    if (isSoroban())
    {
        auto r = sorobanResources();
        int64_t const opCount = 1;

        // When doing fee calculation, the rust host will include readWrite
        // entries in the read related fees. However, this resource calculation
        // is used for constructing TX sets before invoking the host, so we need
        // to sum readOnly size and readWrite size for correct resource limits
        // here.
        return Resource({opCount, r.instructions, txSize, r.readBytes,
                         r.writeBytes,
                         static_cast<int64_t>(r.footprint.readOnly.size() +
                                              r.footprint.readWrite.size()),
                         static_cast<int64_t>(r.footprint.readWrite.size())});
    }
    else if (useByteLimitInClassic)
    {
        return Resource({getNumOperations(), txSize});
    }
    else
    {
        return Resource(getNumOperations());
    }
}

std::vector<Operation> const&
TransactionFrame::getRawOperations() const
{
    return mEnvelope.type() == ENVELOPE_TYPE_TX_V0
               ? mEnvelope.v0().tx.operations
               : mEnvelope.v1().tx.operations;
}

int64_t
TransactionFrame::getFullFee() const
{
    return mEnvelope.type() == ENVELOPE_TYPE_TX_V0 ? mEnvelope.v0().tx.fee
                                                   : mEnvelope.v1().tx.fee;
}

int64_t
TransactionFrame::getInclusionFee() const
{
    if (isSoroban())
    {
        if (declaredSorobanResourceFee() < 0)
        {
            throw std::runtime_error(
                "TransactionFrame::getInclusionFee: negative resource fee");
        }
        return getFullFee() - declaredSorobanResourceFee();
    }

    return getFullFee();
}

int64_t
TransactionFrame::getFee(LedgerHeader const& header,
                         std::optional<int64_t> baseFee, bool applying) const
{
    if (!baseFee)
    {
        return getFullFee();
    }
    if (protocolVersionStartsFrom(header.ledgerVersion,
                                  ProtocolVersion::V_11) ||
        !applying)
    {
        int64_t adjustedFee =
            *baseFee * std::max<int64_t>(1, getNumOperations());
        int64_t maybeResourceFee =
            isSoroban() ? declaredSorobanResourceFee() : 0;

        if (applying)
        {
            return maybeResourceFee +
                   std::min<int64_t>(getInclusionFee(), adjustedFee);
        }
        else
        {
            return maybeResourceFee + adjustedFee;
        }
    }
    else
    {
        return getFullFee();
    }
}

bool
TransactionFrame::checkSignature(SignatureChecker& signatureChecker,
                                 LedgerTxnEntry const& account,
                                 int32_t neededWeight) const
{
    ZoneScoped;
    auto& acc = account.current().data.account();
    std::vector<Signer> signers;
    if (acc.thresholds[0])
    {
        auto signerKey = KeyUtils::convertKey<SignerKey>(acc.accountID);
        signers.push_back(Signer(signerKey, acc.thresholds[0]));
    }
    signers.insert(signers.end(), acc.signers.begin(), acc.signers.end());

    return signatureChecker.checkSignature(signers, neededWeight);
}

bool
TransactionFrame::checkSignatureNoAccount(SignatureChecker& signatureChecker,
                                          AccountID const& accountID) const
{
    ZoneScoped;
    std::vector<Signer> signers;
    auto signerKey = KeyUtils::convertKey<SignerKey>(accountID);
    signers.push_back(Signer(signerKey, 1));
    return signatureChecker.checkSignature(signers, 0);
}

bool
TransactionFrame::checkExtraSigners(SignatureChecker& signatureChecker) const
{
    ZoneScoped;
    if (extraSignersExist())
    {
        auto const& extraSigners = mEnvelope.v1().tx.cond.v2().extraSigners;
        std::vector<Signer> signers;

        std::transform(extraSigners.begin(), extraSigners.end(),
                       std::back_inserter(signers),
                       [](SignerKey const& k) { return Signer(k, 1); });

        // Sanity check for the int32 cast below
        static_assert(decltype(PreconditionsV2::extraSigners)::max_size() <=
                      INT32_MAX);

        // We want to verify that there is a signature for each extraSigner, so
        // we assign a weight of 1 to each key, and set the neededWeight to the
        // number of extraSigners
        return signatureChecker.checkSignature(
            signers, static_cast<int32_t>(signers.size()));
    }
    return true;
}

LedgerTxnEntry
TransactionFrame::loadSourceAccount(AbstractLedgerTxn& ltx,
                                    LedgerTxnHeader const& header) const
{
    ZoneScoped;
    auto res = loadAccount(ltx, header, getSourceID());
    if (protocolVersionIsBefore(header.current().ledgerVersion,
                                ProtocolVersion::V_8))
    {
        // this is buggy caching that existed in old versions of the protocol
        if (res)
        {
            auto newest = ltx.getNewestVersion(LedgerEntryKey(res.current()));
            mCachedAccountPreProtocol8 = newest;
        }
        else
        {
            mCachedAccountPreProtocol8.reset();
        }
    }
    return res;
}

LedgerTxnEntry
TransactionFrame::loadAccount(AbstractLedgerTxn& ltx,
                              LedgerTxnHeader const& header,
                              AccountID const& accountID) const
{
    ZoneScoped;
    if (protocolVersionIsBefore(header.current().ledgerVersion,
                                ProtocolVersion::V_8) &&
        mCachedAccountPreProtocol8 &&
        mCachedAccountPreProtocol8->ledgerEntry().data.account().accountID ==
            accountID)
    {
        // this is buggy caching that existed in old versions of the protocol
        auto res = stellar::loadAccount(ltx, accountID);
        if (res)
        {
            res.currentGeneralized() = *mCachedAccountPreProtocol8;
        }
        else
        {
            res = ltx.create(*mCachedAccountPreProtocol8);
        }

        auto newest = ltx.getNewestVersion(LedgerEntryKey(res.current()));
        mCachedAccountPreProtocol8 = newest;
        return res;
    }
    else
    {
        return stellar::loadAccount(ltx, accountID);
    }
}

bool
TransactionFrame::hasDexOperations() const
{
    return mHasDexOperations;
}

bool
TransactionFrame::isSoroban() const
{
    return mIsSoroban;
}

SorobanResources const&
TransactionFrame::sorobanResources() const
{
    releaseAssertOrThrow(isSoroban());
    return mEnvelope.v1().tx.ext.sorobanData().resources;
}

TransactionResultPayloadPtr
TransactionFrame::createResultPayloadWithFeeCharged(
    LedgerHeader const& header, std::optional<int64_t> baseFee,
    bool applying) const
{
    // feeCharged is updated accordingly to represent the cost of the
    // transaction regardless of the failure modes.
    auto feeCharged = getFee(header, baseFee, applying);
    return TransactionResultPayloadPtr(
        new MutableTransactionResult(*this, feeCharged));
}

TransactionResultPayloadPtr
TransactionFrame::createResultPayload() const
{
    return TransactionResultPayloadPtr(new MutableTransactionResult(*this, 0));
}

std::optional<TimeBounds const> const
TransactionFrame::getTimeBounds() const
{
    if (mEnvelope.type() == ENVELOPE_TYPE_TX_V0)
    {
        return mEnvelope.v0().tx.timeBounds ? std::optional<TimeBounds const>(
                                                  *mEnvelope.v0().tx.timeBounds)
                                            : std::optional<TimeBounds const>();
    }
    else
    {
        auto const& cond = mEnvelope.v1().tx.cond;
        switch (cond.type())
        {
        case PRECOND_NONE:
        {
            return std::optional<TimeBounds const>();
        }
        case PRECOND_TIME:
        {
            return std::optional<TimeBounds const>(cond.timeBounds());
        }
        case PRECOND_V2:
        {
            return cond.v2().timeBounds
                       ? std::optional<TimeBounds const>(*cond.v2().timeBounds)
                       : std::optional<TimeBounds const>();
        }
        default:
            throw std::runtime_error("unknown condition type");
        }
    }
}

std::optional<LedgerBounds const> const
TransactionFrame::getLedgerBounds() const
{
    if (mEnvelope.type() == ENVELOPE_TYPE_TX)
    {
        auto const& cond = mEnvelope.v1().tx.cond;
        if (cond.type() == PRECOND_V2 && cond.v2().ledgerBounds)
        {
            return std::optional<LedgerBounds const>(*cond.v2().ledgerBounds);
        }
    }

    return std::optional<LedgerBounds const>();
}

Duration
TransactionFrame::getMinSeqAge() const
{
    if (mEnvelope.type() == ENVELOPE_TYPE_TX)
    {
        auto& cond = mEnvelope.v1().tx.cond;
        return cond.type() == PRECOND_V2 ? cond.v2().minSeqAge : 0;
    }

    return 0;
}

uint32
TransactionFrame::getMinSeqLedgerGap() const
{
    if (mEnvelope.type() == ENVELOPE_TYPE_TX)
    {
        auto& cond = mEnvelope.v1().tx.cond;
        return cond.type() == PRECOND_V2 ? cond.v2().minSeqLedgerGap : 0;
    }

    return 0;
}

std::optional<SequenceNumber const> const
TransactionFrame::getMinSeqNum() const
{
    if (mEnvelope.type() == ENVELOPE_TYPE_TX)
    {
        auto& cond = mEnvelope.v1().tx.cond;
        if (cond.type() == PRECOND_V2 && cond.v2().minSeqNum)
        {
            return std::optional<SequenceNumber const>(*cond.v2().minSeqNum);
        }
    }

    return std::optional<SequenceNumber const>();
}

bool
TransactionFrame::extraSignersExist() const
{
    return mEnvelope.type() == ENVELOPE_TYPE_TX &&
           mEnvelope.v1().tx.cond.type() == PRECOND_V2 &&
           !mEnvelope.v1().tx.cond.v2().extraSigners.empty();
}

bool
TransactionFrame::validateSorobanOpsConsistency() const
{
    return mHasValidSorobanOpsConsistency;
}

bool
TransactionFrame::validateSorobanResources(
    SorobanNetworkConfig const& config, Config const& appConfig,
    uint32_t protocolVersion, MutableTransactionResultBase& txResult) const
{
    auto const& resources = sorobanResources();
    auto const& readEntries = resources.footprint.readOnly;
    auto const& writeEntries = resources.footprint.readWrite;
    if (resources.instructions > config.txMaxInstructions())
    {
        txResult.pushValidationTimeDiagnosticError(
            appConfig, SCE_BUDGET, SCEC_EXCEEDED_LIMIT,
            "transaction instructions resources exceed network config limit",
            {makeU64SCVal(resources.instructions),
             makeU64SCVal(config.txMaxInstructions())});
        return false;
    }
    if (resources.readBytes > config.txMaxReadBytes())
    {
        txResult.pushValidationTimeDiagnosticError(
            appConfig, SCE_STORAGE, SCEC_EXCEEDED_LIMIT,
            "transaction byte-read resources exceed network config limit",
            {makeU64SCVal(resources.readBytes),
             makeU64SCVal(config.txMaxReadBytes())});
        return false;
    }
    if (resources.writeBytes > config.txMaxWriteBytes())
    {
        txResult.pushValidationTimeDiagnosticError(
            appConfig, SCE_STORAGE, SCEC_EXCEEDED_LIMIT,
            "transaction byte-write resources exceed network config limit",
            {makeU64SCVal(resources.writeBytes),
             makeU64SCVal(config.txMaxWriteBytes())});
        return false;
    }
    if (readEntries.size() + writeEntries.size() >
        config.txMaxReadLedgerEntries())
    {
        txResult.pushValidationTimeDiagnosticError(
            appConfig, SCE_STORAGE, SCEC_EXCEEDED_LIMIT,
            "transaction entry-read resources exceed network config limit",
            {makeU64SCVal(readEntries.size() + writeEntries.size()),
             makeU64SCVal(config.txMaxReadLedgerEntries())});
        return false;
    }
    if (writeEntries.size() > config.txMaxWriteLedgerEntries())
    {
        txResult.pushValidationTimeDiagnosticError(
            appConfig, SCE_STORAGE, SCEC_EXCEEDED_LIMIT,
            "transaction entry-write resources exceed network config limit",
            {makeU64SCVal(writeEntries.size()),
             makeU64SCVal(config.txMaxWriteLedgerEntries())});
        return false;
    }
    auto footprintKeyIsValid = [&](LedgerKey const& key) -> bool {
        switch (key.type())
        {
        case ACCOUNT:
        case CONTRACT_DATA:
        case CONTRACT_CODE:
            break;
        case TRUSTLINE:
        {
            auto const& tl = key.trustLine();
            if (!isAssetValid(tl.asset, protocolVersion) ||
                (tl.asset.type() == ASSET_TYPE_NATIVE) ||
                isIssuer(tl.accountID, tl.asset))
            {
                txResult.pushValidationTimeDiagnosticError(
                    appConfig, SCE_STORAGE, SCEC_INVALID_INPUT,
                    "transaction footprint contains invalid trustline asset");
                return false;
            }
            break;
        }
        case OFFER:
        case DATA:
        case CLAIMABLE_BALANCE:
        case LIQUIDITY_POOL:
        case CONFIG_SETTING:
        case TTL:
            txResult.pushValidationTimeDiagnosticError(
                appConfig, SCE_STORAGE, SCEC_UNEXPECTED_TYPE,
                "transaction footprint contains unsupported ledger key type",
                {makeU64SCVal(key.type())});
            return false;
        default:
            throw std::runtime_error("unknown ledger key type");
        }

        if (xdr::xdr_size(key) > config.maxContractDataKeySizeBytes())
        {
            txResult.pushValidationTimeDiagnosticError(
                appConfig, SCE_STORAGE, SCEC_EXCEEDED_LIMIT,
                "transaction footprint key exceeds network config limit",
                {makeU64SCVal(xdr::xdr_size(key)),
                 makeU64SCVal(config.maxContractDataKeySizeBytes())});
            return false;
        }

        return true;
    };
    for (auto const& lk : readEntries)
    {
        if (!footprintKeyIsValid(lk))
        {
            return false;
        }
    }
    for (auto const& lk : writeEntries)
    {
        if (!footprintKeyIsValid(lk))
        {
            return false;
        }
    }
    auto txSize = this->getSize();
    if (txSize > config.txMaxSizeBytes())
    {
        txResult.pushSimpleDiagnosticError(
            appConfig, SCE_BUDGET, SCEC_EXCEEDED_LIMIT,
            "total transaction size exceeds network config limit",
            {makeU64SCVal(txSize), makeU64SCVal(config.txMaxSizeBytes())});
        return false;
    }
    return true;
}

int64_t
TransactionFrame::refundSorobanFee(AbstractLedgerTxn& ltxOuter,
                                   AccountID const& feeSource,
                                   MutableTransactionResultBase& txResult) const
{
    ZoneScoped;
    auto const feeRefund = txResult.getSorobanFeeRefund();
    if (feeRefund == 0)
    {
        return 0;
    }

    LedgerTxn ltx(ltxOuter);
    auto header = ltx.loadHeader();
    // The fee source could be from a Fee-bump, so it needs to be forwarded here
    // instead of using TransactionFrame's getFeeSource() method
    auto feeSourceAccount = loadAccount(ltx, header, feeSource);
    if (!feeSourceAccount)
    {
        // Account was merged (shouldn't be possible)
        return 0;
    }

    if (!addBalance(header, feeSourceAccount, feeRefund))
    {
        // Liabilities in the way of the refund, just skip.
        return 0;
    }

    txResult.getResult().feeCharged -= feeRefund;
    header.current().feePool -= feeRefund;
    ltx.commit();

    return feeRefund;
}

void
TransactionFrame::updateSorobanMetrics(Application& app) const
{
    releaseAssertOrThrow(isSoroban());
    SorobanMetrics& metrics = app.getLedgerManager().getSorobanMetrics();
    auto txSize = static_cast<int64_t>(this->getSize());
    auto r = sorobanResources();
    // update the tx metrics
    metrics.mTxSizeByte.Update(txSize);
    // accumulate the ledger-wide metrics, which will get emitted at the ledger
    // close
    metrics.accumulateLedgerTxCount(1);
    metrics.accumulateLedgerCpuInsn(r.instructions);
    metrics.accumulateLedgerTxsSizeByte(txSize);
    metrics.accumulateLedgerReadEntry(static_cast<int64_t>(
        r.footprint.readOnly.size() + r.footprint.readWrite.size()));
    metrics.accumulateLedgerReadByte(r.readBytes);
    metrics.accumulateLedgerWriteEntry(
        static_cast<int64_t>(r.footprint.readWrite.size()));
    metrics.accumulateLedgerWriteByte(r.writeBytes);
}

FeePair
TransactionFrame::computeSorobanResourceFee(
    uint32_t protocolVersion, SorobanResources const& txResources,
    uint32_t txSize, uint32_t eventsSize,
    SorobanNetworkConfig const& sorobanConfig, Config const& cfg)
{
    ZoneScoped;
    releaseAssertOrThrow(
        protocolVersionStartsFrom(protocolVersion, SOROBAN_PROTOCOL_VERSION));
    CxxTransactionResources cxxResources{};
    cxxResources.instructions = txResources.instructions;

    cxxResources.read_entries =
        static_cast<uint32>(txResources.footprint.readOnly.size());
    cxxResources.write_entries =
        static_cast<uint32>(txResources.footprint.readWrite.size());

    cxxResources.read_bytes = txResources.readBytes;
    cxxResources.write_bytes = txResources.writeBytes;

    cxxResources.transaction_size_bytes = txSize;
    cxxResources.contract_events_size_bytes = eventsSize;

    // This may throw, but only in case of the Core version misconfiguration.
    return rust_bridge::compute_transaction_resource_fee(
        cfg.CURRENT_LEDGER_PROTOCOL_VERSION, protocolVersion, cxxResources,
        sorobanConfig.rustBridgeFeeConfiguration());
}

int64
TransactionFrame::declaredSorobanResourceFee() const
{
    releaseAssertOrThrow(isSoroban());
    return mEnvelope.v1().tx.ext.sorobanData().resourceFee;
}

FeePair
TransactionFrame::computePreApplySorobanResourceFee(
    uint32_t protocolVersion, SorobanNetworkConfig const& sorobanConfig,
    Config const& cfg) const
{
    ZoneScoped;
    releaseAssertOrThrow(isSoroban());
    // We always use the declared resource value for the resource fee
    // computation. The refunds are performed as a separate operation that
    // doesn't involve modifying any transaction fees.
    return computeSorobanResourceFee(
        protocolVersion, sorobanResources(),
        static_cast<uint32>(
            getResources(false).getVal(Resource::Type::TX_BYTE_SIZE)),
        0, sorobanConfig, cfg);
}

bool
TransactionFrame::isTooEarly(LedgerTxnHeader const& header,
                             uint64_t lowerBoundCloseTimeOffset) const
{
    auto const tb = getTimeBounds();
    if (tb)
    {
        uint64 closeTime = header.current().scpValue.closeTime;
        if (tb->minTime &&
            (tb->minTime > (closeTime + lowerBoundCloseTimeOffset)))
        {
            return true;
        }
    }

    if (protocolVersionStartsFrom(header.current().ledgerVersion,
                                  ProtocolVersion::V_19))
    {
        auto const lb = getLedgerBounds();
        return lb && lb->minLedger > header.current().ledgerSeq;
    }

    return false;
}

bool
TransactionFrame::isTooLate(LedgerTxnHeader const& header,
                            uint64_t upperBoundCloseTimeOffset) const
{
    auto const tb = getTimeBounds();
    if (tb)
    {
        // Prior to consensus, we can pass in an upper bound estimate on when we
        // expect the ledger to close so we don't accept transactions that will
        // expire by the time they are applied
        uint64 closeTime = header.current().scpValue.closeTime;
        if (tb->maxTime &&
            (tb->maxTime < (closeTime + upperBoundCloseTimeOffset)))
        {
            return true;
        }
    }

    if (protocolVersionStartsFrom(header.current().ledgerVersion,
                                  ProtocolVersion::V_19))
    {
        auto const lb = getLedgerBounds();
        return lb && lb->maxLedger != 0 &&
               lb->maxLedger <= header.current().ledgerSeq;
    }
    return false;
}

bool
TransactionFrame::isTooEarlyForAccount(LedgerTxnHeader const& header,
                                       LedgerTxnEntry const& sourceAccount,
                                       uint64_t lowerBoundCloseTimeOffset) const
{
    if (protocolVersionIsBefore(header.current().ledgerVersion,
                                ProtocolVersion::V_19))
    {
        return false;
    }

    auto accountEntry = [&]() -> AccountEntry const& {
        return sourceAccount.current().data.account();
    };

    auto accSeqTime = hasAccountEntryExtV3(accountEntry())
                          ? getAccountEntryExtensionV3(accountEntry()).seqTime
                          : 0;
    auto minSeqAge = getMinSeqAge();

    auto lowerBoundCloseTime =
        header.current().scpValue.closeTime + lowerBoundCloseTimeOffset;
    if (minSeqAge > lowerBoundCloseTime ||
        lowerBoundCloseTime - minSeqAge < accSeqTime)
    {
        return true;
    }

    auto accSeqLedger =
        hasAccountEntryExtV3(accountEntry())
            ? getAccountEntryExtensionV3(accountEntry()).seqLedger
            : 0;
    auto minSeqLedgerGap = getMinSeqLedgerGap();

    auto ledgerSeq = header.current().ledgerSeq;
    if (minSeqLedgerGap > ledgerSeq ||
        ledgerSeq - minSeqLedgerGap < accSeqLedger)
    {
        return true;
    }

    return false;
}

bool
TransactionFrame::commonValidPreSeqNum(
    Application& app, AbstractLedgerTxn& ltx, bool chargeFee,
    uint64_t lowerBoundCloseTimeOffset, uint64_t upperBoundCloseTimeOffset,
    std::optional<FeePair> sorobanResourceFee,
    TransactionResultPayloadPtr txResult) const
{
    ZoneScoped;
    releaseAssertOrThrow(txResult);
    // this function does validations that are independent of the account state
    //    (stay true regardless of other side effects)

    uint32_t ledgerVersion = ltx.loadHeader().current().ledgerVersion;
    if ((protocolVersionIsBefore(ledgerVersion, ProtocolVersion::V_13) &&
         (mEnvelope.type() == ENVELOPE_TYPE_TX ||
          hasMuxedAccount(mEnvelope))) ||
        (protocolVersionStartsFrom(ledgerVersion, ProtocolVersion::V_13) &&
         mEnvelope.type() == ENVELOPE_TYPE_TX_V0))
    {
        txResult->setResultCode(txNOT_SUPPORTED);
        return false;
    }

    if (protocolVersionIsBefore(ledgerVersion, ProtocolVersion::V_19) &&
        mEnvelope.type() == ENVELOPE_TYPE_TX &&
        mEnvelope.v1().tx.cond.type() == PRECOND_V2)
    {
        txResult->setResultCode(txNOT_SUPPORTED);
        return false;
    }

    if (extraSignersExist())
    {
        auto const& extraSigners = mEnvelope.v1().tx.cond.v2().extraSigners;

        static_assert(decltype(PreconditionsV2::extraSigners)::max_size() == 2);
        if (extraSigners.size() == 2 && extraSigners[0] == extraSigners[1])
        {
            txResult->setResultCode(txMALFORMED);
            return false;
        }

        for (auto const& signer : extraSigners)
        {
            if (signer.type() == SIGNER_KEY_TYPE_ED25519_SIGNED_PAYLOAD &&
                signer.ed25519SignedPayload().payload.empty())
            {
                txResult->setResultCode(txMALFORMED);
                return false;
            }
        }
    }

    if (getNumOperations() == 0)
    {
        txResult->setResultCode(txMISSING_OPERATION);
        return false;
    }

    if (!validateSorobanOpsConsistency())
    {
        txResult->setResultCode(txMALFORMED);
        return false;
    }
    if (isSoroban())
    {
        if (protocolVersionIsBefore(ledgerVersion, SOROBAN_PROTOCOL_VERSION))
        {
            txResult->setResultCode(txMALFORMED);
            return false;
        }

        if (!checkSorobanResourceAndSetError(app, ledgerVersion, txResult))
        {
            return false;
        }

        auto const& sorobanData = mEnvelope.v1().tx.ext.sorobanData();
        if (sorobanData.resourceFee > getFullFee())
        {
            txResult->pushValidationTimeDiagnosticError(
                app.getConfig(), SCE_STORAGE, SCEC_EXCEEDED_LIMIT,
                "transaction `sorobanData.resourceFee` is higher than the "
                "full transaction fee",
                {makeU64SCVal(sorobanData.resourceFee),
                 makeU64SCVal(getFullFee())});

            txResult->setResultCode(txSOROBAN_INVALID);
            return false;
        }
        releaseAssertOrThrow(sorobanResourceFee);
        if (sorobanResourceFee->refundable_fee >
            INT64_MAX - sorobanResourceFee->non_refundable_fee)
        {
            txResult->pushValidationTimeDiagnosticError(
                app.getConfig(), SCE_STORAGE, SCEC_INVALID_INPUT,
                "transaction resource fees cannot be added",
                {makeU64SCVal(sorobanResourceFee->refundable_fee),
                 makeU64SCVal(sorobanResourceFee->non_refundable_fee)});

            txResult->setResultCode(txSOROBAN_INVALID);
            return false;
        }
        auto const resourceFees = sorobanResourceFee->refundable_fee +
                                  sorobanResourceFee->non_refundable_fee;
        if (sorobanData.resourceFee < resourceFees)
        {
            txResult->pushValidationTimeDiagnosticError(
                app.getConfig(), SCE_STORAGE, SCEC_EXCEEDED_LIMIT,
                "transaction `sorobanData.resourceFee` is lower than the "
                "actual Soroban resource fee",
                {makeU64SCVal(sorobanData.resourceFee),
                 makeU64SCVal(resourceFees)});

            txResult->setResultCode(txSOROBAN_INVALID);
            return false;
        }

        // check for duplicates
        UnorderedSet<LedgerKey> set;
        auto checkDuplicates =
            [&](xdr::xvector<stellar::LedgerKey> const& keys) -> bool {
            for (auto const& lk : keys)
            {
                if (!set.emplace(lk).second)
                {
                    txResult->pushValidationTimeDiagnosticError(
                        app.getConfig(), SCE_STORAGE, SCEC_INVALID_INPUT,
                        "Found duplicate key in the Soroban footprint; every "
                        "key across read-only and read-write footprints has to "
                        "be unique.",
                        {});

                    txResult->setResultCode(txSOROBAN_INVALID);
                    return false;
                }
            }
            return true;
        };

        if (!checkDuplicates(sorobanData.resources.footprint.readOnly) ||
            !checkDuplicates(sorobanData.resources.footprint.readWrite))
        {
            return false;
        }
    }
    else
    {
        if (protocolVersionStartsFrom(ledgerVersion, ProtocolVersion::V_21))
        {
            if (mEnvelope.type() == ENVELOPE_TYPE_TX &&
                mEnvelope.v1().tx.ext.v() != 0)
            {
                txResult->setResultCode(txMALFORMED);
                return false;
            }
        }
    }

    auto header = ltx.loadHeader();
    if (isTooEarly(header, lowerBoundCloseTimeOffset))
    {
        txResult->setResultCode(txTOO_EARLY);
        return false;
    }
    if (isTooLate(header, upperBoundCloseTimeOffset))
    {
        txResult->setResultCode(txTOO_LATE);
        return false;
    }

    if (chargeFee &&
        getInclusionFee() < getMinInclusionFee(*this, header.current()))
    {
        txResult->setResultCode(txINSUFFICIENT_FEE);
        return false;
    }
    if (!chargeFee && getInclusionFee() < 0)
    {
        txResult->setResultCode(txINSUFFICIENT_FEE);
        return false;
    }

    if (!loadSourceAccount(ltx, header))
    {
        txResult->setResultCode(txNO_ACCOUNT);
        return false;
    }

    return true;
}

void
TransactionFrame::processSeqNum(AbstractLedgerTxn& ltx) const
{
    ZoneScoped;
    auto header = ltx.loadHeader();
    if (protocolVersionStartsFrom(header.current().ledgerVersion,
                                  ProtocolVersion::V_10))
    {
        auto sourceAccount = loadSourceAccount(ltx, header);
        if (sourceAccount.current().data.account().seqNum > getSeqNum())
        {
            throw std::runtime_error("unexpected sequence number");
        }
        sourceAccount.current().data.account().seqNum = getSeqNum();

        maybeUpdateAccountOnLedgerSeqUpdate(header, sourceAccount);
    }
}

bool
TransactionFrame::processSignatures(
    ValidationType cv, SignatureChecker& signatureChecker,
    AbstractLedgerTxn& ltxOuter, MutableTransactionResultBase& txResult) const
{
    ZoneScoped;
    bool maybeValid = (cv == ValidationType::kMaybeValid);
    uint32_t ledgerVersion = ltxOuter.loadHeader().current().ledgerVersion;
    if (protocolVersionIsBefore(ledgerVersion, ProtocolVersion::V_10))
    {
        return maybeValid;
    }

    // check if we need to fast fail and use the original error code
    if (protocolVersionStartsFrom(ledgerVersion, ProtocolVersion::V_13) &&
        !maybeValid)
    {
        removeOneTimeSignerFromAllSourceAccounts(ltxOuter, txResult);
        return false;
    }
    // older versions of the protocol only fast fail in a subset of cases
    if (protocolVersionIsBefore(ledgerVersion, ProtocolVersion::V_13) &&
        cv < ValidationType::kInvalidPostAuth)
    {
        return false;
    }

    bool allOpsValid = true;
    {
        // scope here to avoid potential side effects of loading source accounts
        LedgerTxn ltx(ltxOuter);
        for (auto op : txResult.getOpFrames())
        {
            if (!op->checkSignature(signatureChecker, ltx, txResult, false))
            {
                allOpsValid = false;
            }
        }
    }

    removeOneTimeSignerFromAllSourceAccounts(ltxOuter, txResult);

    if (!allOpsValid)
    {
        // Changing "code" normally causes the XDR structure to be destructed,
        // then a different XDR structure is constructed. However, txFAILED and
        // txSUCCESS have the same underlying field number so this does not
        // occur.
        txResult.setResultCode(txFAILED);
        return false;
    }

    if (!signatureChecker.checkAllSignaturesUsed())
    {
        txResult.setResultCode(txBAD_AUTH_EXTRA);
        return false;
    }

    return maybeValid;
}

bool
TransactionFrame::isBadSeq(LedgerTxnHeader const& header, int64_t seqNum) const
{
    if (getSeqNum() == getStartingSequenceNumber(header))
    {
        return true;
    }

    // If seqNum == INT64_MAX, seqNum >= getSeqNum() is guaranteed to be true
    // because SequenceNumber is int64, so isBadSeq will always return true in
    // that case.
    if (protocolVersionStartsFrom(header.current().ledgerVersion,
                                  ProtocolVersion::V_19))
    {
        // Check if we need to relax sequence number checking
        auto minSeqNum = getMinSeqNum();
        if (minSeqNum)
        {
            return seqNum < *minSeqNum || seqNum >= getSeqNum();
        }
    }

    // If we get here, we need to do the strict seqnum check
    return seqNum == INT64_MAX || seqNum + 1 != getSeqNum();
}

TransactionFrame::ValidationType
TransactionFrame::commonValid(Application& app,
                              SignatureChecker& signatureChecker,
                              AbstractLedgerTxn& ltxOuter,
                              SequenceNumber current, bool applying,
                              bool chargeFee,
                              uint64_t lowerBoundCloseTimeOffset,
                              uint64_t upperBoundCloseTimeOffset,
                              std::optional<FeePair> sorobanResourceFee,
                              TransactionResultPayloadPtr txResult) const
{
    ZoneScoped;
    releaseAssertOrThrow(txResult);
    LedgerTxn ltx(ltxOuter);
    ValidationType res = ValidationType::kInvalid;

    if (applying &&
        (lowerBoundCloseTimeOffset != 0 || upperBoundCloseTimeOffset != 0))
    {
        throw std::logic_error(
            "Applying transaction with non-current closeTime");
    }

    if (!commonValidPreSeqNum(app, ltx, chargeFee, lowerBoundCloseTimeOffset,
                              upperBoundCloseTimeOffset, sorobanResourceFee,
                              txResult))
    {
        return res;
    }

    auto header = ltx.loadHeader();
    auto sourceAccount = loadSourceAccount(ltx, header);

    // in older versions, the account's sequence number is updated when taking
    // fees
    if (protocolVersionStartsFrom(header.current().ledgerVersion,
                                  ProtocolVersion::V_10) ||
        !applying)
    {
        if (current == 0)
        {
            current = sourceAccount.current().data.account().seqNum;
        }
        if (isBadSeq(header, current))
        {
            txResult->setResultCode(txBAD_SEQ);
            return res;
        }
    }

    res = ValidationType::kInvalidUpdateSeqNum;

    if (isTooEarlyForAccount(header, sourceAccount, lowerBoundCloseTimeOffset))
    {
        txResult->setResultCode(txBAD_MIN_SEQ_AGE_OR_GAP);
        return res;
    }

    if (!checkSignature(
            signatureChecker, sourceAccount,
            sourceAccount.current().data.account().thresholds[THRESHOLD_LOW]))
    {
        txResult->setResultCode(txBAD_AUTH);
        return res;
    }

    if (protocolVersionStartsFrom(header.current().ledgerVersion,
                                  ProtocolVersion::V_19) &&
        !checkExtraSigners(signatureChecker))
    {
        txResult->setResultCode(txBAD_AUTH);
        return res;
    }

    res = ValidationType::kInvalidPostAuth;

    // if we are in applying mode fee was already deduced from signing account
    // balance, if not, we need to check if after that deduction this account
    // will still have minimum balance
    uint32_t feeToPay =
        (applying && protocolVersionStartsFrom(header.current().ledgerVersion,
                                               ProtocolVersion::V_9))
            ? 0
            : static_cast<uint32_t>(getFullFee());
    // don't let the account go below the reserve after accounting for
    // liabilities
    if (chargeFee && getAvailableBalance(header, sourceAccount) < feeToPay)
    {
        txResult->setResultCode(txINSUFFICIENT_BALANCE);
        return res;
    }

    return ValidationType::kMaybeValid;
}

TransactionResultPayloadPtr
TransactionFrame::processFeeSeqNum(AbstractLedgerTxn& ltx,
                                   std::optional<int64_t> baseFee) const
{
    ZoneScoped;
    mCachedAccountPreProtocol8.reset();

    auto header = ltx.loadHeader();
    auto txResult =
        createResultPayloadWithFeeCharged(header.current(), baseFee, true);
    releaseAssert(txResult);

    auto sourceAccount = loadSourceAccount(ltx, header);
    if (!sourceAccount)
    {
        throw std::runtime_error("Unexpected database state");
    }

    auto& acc = sourceAccount.current().data.account();

    int64_t& fee = txResult->getResult().feeCharged;
    if (fee > 0)
    {
        fee = std::min(acc.balance, fee);
        // Note: TransactionUtil addBalance checks that reserve plus liabilities
        // are respected. In this case, we allow it to fall below that since it
        // will be caught later in commonValid.
        stellar::addBalance(acc.balance, -fee);
        header.current().feePool += fee;
    }
    // in v10 we update sequence numbers during apply
    if (protocolVersionIsBefore(header.current().ledgerVersion,
                                ProtocolVersion::V_10))
    {
        if (acc.seqNum + 1 != getSeqNum())
        {
            // this should not happen as the transaction set is sanitized for
            // sequence numbers
            throw std::runtime_error("Unexpected account state");
        }
        acc.seqNum = getSeqNum();
    }

    return txResult;
}

bool
TransactionFrame::XDRProvidesValidFee() const
{
    if (isSoroban())
    {
        if (mEnvelope.type() != ENVELOPE_TYPE_TX ||
            mEnvelope.v1().tx.ext.v() != 1)
        {
            return false;
        }
        int64_t resourceFee = declaredSorobanResourceFee();
        if (resourceFee < 0 || resourceFee > MAX_RESOURCE_FEE)
        {
            return false;
        }
    }
    return true;
}

// TODO: Remove txResult
void
TransactionFrame::removeOneTimeSignerFromAllSourceAccounts(
    AbstractLedgerTxn& ltx, MutableTransactionResultBase& txResult) const
{
    auto ledgerVersion = ltx.loadHeader().current().ledgerVersion;
    if (ledgerVersion == 7)
    {
        return;
    }

    UnorderedSet<AccountID> accounts{getSourceID()};
    for (auto& op : txResult.getOpFrames())
    {
        accounts.emplace(op->getSourceID());
    }

    auto signerKey = SignerKeyUtils::preAuthTxKey(*this);
    for (auto const& accountID : accounts)
    {
        removeAccountSigner(ltx, accountID, signerKey);
    }
}

void
TransactionFrame::removeAccountSigner(AbstractLedgerTxn& ltxOuter,
                                      AccountID const& accountID,
                                      SignerKey const& signerKey) const
{
    ZoneScoped;
    LedgerTxn ltx(ltxOuter);

    auto account = stellar::loadAccount(ltx, accountID);
    if (!account)
    {
        return; // probably account was removed due to merge operation
    }

    auto header = ltx.loadHeader();
    auto& signers = account.current().data.account().signers;
    auto findRes = findSignerByKey(signers.begin(), signers.end(), signerKey);
    if (findRes.second)
    {
        removeSignerWithPossibleSponsorship(ltx, header, findRes.first,
                                            account);
        ltx.commit();
    }
}

std::pair<bool, TransactionResultPayloadPtr>
TransactionFrame::checkValidWithOptionallyChargedFee(
    Application& app, AbstractLedgerTxn& ltxOuter, SequenceNumber current,
    bool chargeFee, uint64_t lowerBoundCloseTimeOffset,
    uint64_t upperBoundCloseTimeOffset) const
{
    ZoneScoped;
    mCachedAccountPreProtocol8.reset();

    if (!XDRProvidesValidFee())
    {
        auto txResult = createResultPayload();
        txResult->setResultCode(txMALFORMED);
        return {false, txResult};
    }

    LedgerTxn ltx(ltxOuter);
    int64_t minBaseFee = ltx.loadHeader().current().baseFee;
    if (!chargeFee)
    {
        minBaseFee = 0;
    }

    auto txResult = createResultPayloadWithFeeCharged(
        ltx.loadHeader().current(), minBaseFee, false);
    releaseAssert(txResult);

    SignatureChecker signatureChecker{ltx.loadHeader().current().ledgerVersion,
                                      getContentsHash(),
                                      getSignatures(mEnvelope)};
    std::optional<FeePair> sorobanResourceFee;
    if (protocolVersionStartsFrom(ltx.loadHeader().current().ledgerVersion,
                                  SOROBAN_PROTOCOL_VERSION) &&
        isSoroban())
    {
        sorobanResourceFee = computePreApplySorobanResourceFee(
            ltx.loadHeader().current().ledgerVersion,
            app.getLedgerManager().getSorobanNetworkConfig(), app.getConfig());
    }
    bool res = commonValid(app, signatureChecker, ltx, current, false,
                           chargeFee, lowerBoundCloseTimeOffset,
                           upperBoundCloseTimeOffset, sorobanResourceFee,
                           txResult) == ValidationType::kMaybeValid;
    if (res)
    {
        for (auto op : txResult->getOpFrames())
        {
            if (!op->checkValid(app, signatureChecker, ltx, false, *txResult))
            {
                // it's OK to just fast fail here and not try to call
                // checkValid on all operations as the resulting object
                // is only used by applications
                txResult->setResultCode(txFAILED);
                return {false, txResult};
            }
        }

        if (!signatureChecker.checkAllSignaturesUsed())
        {
            res = false;
            txResult->setResultCode(txBAD_AUTH_EXTRA);
        }
    }
    return {res, txResult};
}

std::pair<bool, TransactionResultPayloadPtr>
TransactionFrame::checkValid(Application& app, AbstractLedgerTxn& ltxOuter,
                             SequenceNumber current,
                             uint64_t lowerBoundCloseTimeOffset,
                             uint64_t upperBoundCloseTimeOffset) const
{
    return checkValidWithOptionallyChargedFee(app, ltxOuter, current, true,
                                              lowerBoundCloseTimeOffset,
                                              upperBoundCloseTimeOffset);
}

bool
TransactionFrame::checkSorobanResourceAndSetError(
    Application& app, uint32_t ledgerVersion,
    TransactionResultPayloadPtr txResult) const
{
    releaseAssertOrThrow(txResult);
    auto const& sorobanConfig =
        app.getLedgerManager().getSorobanNetworkConfig();
    if (!validateSorobanResources(sorobanConfig, app.getConfig(), ledgerVersion,
                                  *txResult))
    {
        txResult->setResultCode(txSOROBAN_INVALID);
        return false;
    }
    return true;
}

void
TransactionFrame::insertKeysForFeeProcessing(
    UnorderedSet<LedgerKey>& keys) const
{
    keys.emplace(accountKey(getSourceID()));
}

void
TransactionFrame::insertKeysForTxApply(UnorderedSet<LedgerKey>& keys) const
{
    // We need information from the underlying OperationFrame objects, but we
    // don't want to store mutable state, so use a throw away ResultPayload.
    auto dummyPayload = createResultPayload();
    for (auto const& op : dummyPayload->getOpFrames())
    {
        if (!(getSourceID() == op->getSourceID()))
        {
            keys.emplace(accountKey(op->getSourceID()));
        }
        op->insertLedgerKeysToPrefetch(keys);
    }
}

bool
TransactionFrame::apply(Application& app, AbstractLedgerTxn& ltx,
                        TransactionResultPayloadPtr txResult,
                        Hash const& sorobanBasePrngSeed) const
{
    TransactionMetaFrame tm(ltx.loadHeader().current().ledgerVersion);
    return apply(app, ltx, tm, txResult, sorobanBasePrngSeed);
}

bool
TransactionFrame::applyOperations(SignatureChecker& signatureChecker,
                                  Application& app, AbstractLedgerTxn& ltx,
                                  TransactionMetaFrame& outerMeta,
                                  MutableTransactionResultBase& txResult,
                                  Hash const& sorobanBasePrngSeed) const
{
    ZoneScoped;
    auto& internalErrorCounter = app.getMetrics().NewCounter(
        {"ledger", "transaction", "internal-error"});
    bool reportInternalErrOnException = true;
    try
    {
        bool success = true;

        xdr::xvector<OperationMeta> operationMetas;
        operationMetas.reserve(getNumOperations());

        // shield outer scope of any side effects with LedgerTxn
        LedgerTxn ltxTx(ltx);
        uint32_t ledgerVersion = ltxTx.loadHeader().current().ledgerVersion;
        // We do not want to increase the internal-error metric count for
        // older ledger versions. The minimum ledger version for which we
        // start internal-error counting is defined in the app config.
        reportInternalErrOnException =
            ledgerVersion >=
            app.getConfig().LEDGER_PROTOCOL_MIN_VERSION_INTERNAL_ERROR_REPORT;
        auto& opTimer =
            app.getMetrics().NewTimer({"ledger", "operation", "apply"});

        uint64_t opNum{0};
        for (auto op : txResult.getOpFrames())
        {
            auto time = opTimer.TimeScope();
            LedgerTxn ltxOp(ltxTx);

            Hash subSeed = sorobanBasePrngSeed;
            // If op can use the seed, we need to compute a sub-seed for it.
            if (op->isSoroban())
            {
                SHA256 subSeedSha;
                subSeedSha.add(sorobanBasePrngSeed);
                subSeedSha.add(xdr::xdr_to_opaque(opNum));
                subSeed = subSeedSha.finish();
            }
            ++opNum;

            bool txRes =
                op->apply(app, signatureChecker, ltxOp, subSeed, txResult);

            if (!txRes)
            {
                success = false;
            }
            if (success)
            {
                app.getInvariantManager().checkOnOperationApply(
                    op->getOperation(), op->getResult(), ltxOp.getDelta());

                // The operation meta will be empty if the transaction
                // doesn't succeed so we may as well not do any work in that
                // case
                operationMetas.emplace_back(ltxOp.getChanges());
            }

            if (txRes ||
                protocolVersionIsBefore(ledgerVersion, ProtocolVersion::V_14))
            {
                ltxOp.commit();
            }
        }

        if (success)
        {
            LedgerEntryChanges changesAfter;

            if (protocolVersionIsBefore(ledgerVersion, ProtocolVersion::V_10))
            {
                if (!signatureChecker.checkAllSignaturesUsed())
                {
                    txResult.setResultCode(txBAD_AUTH_EXTRA);

                    // this should never happen: malformed transaction
                    // should not be accepted by nodes
                    return false;
                }

                // if an error occurred, it is responsibility of account's
                // owner to remove that signer
                LedgerTxn ltxAfter(ltxTx);
                removeOneTimeSignerFromAllSourceAccounts(ltxAfter, txResult);
                changesAfter = ltxAfter.getChanges();
                ltxAfter.commit();
            }
            else if (protocolVersionStartsFrom(ledgerVersion,
                                               ProtocolVersion::V_14) &&
                     ltxTx.hasSponsorshipEntry())
            {
                txResult.setResultCode(txBAD_SPONSORSHIP);
                return false;
            }

            ltxTx.commit();
            // commit -> propagate the meta to the outer scope
            outerMeta.pushOperationMetas(std::move(operationMetas));
            outerMeta.pushTxChangesAfter(std::move(changesAfter));

            if (protocolVersionStartsFrom(ledgerVersion,
                                          SOROBAN_PROTOCOL_VERSION) &&
                isSoroban())
            {
                txResult.publishSuccessDiagnosticsToMeta(outerMeta,
                                                         app.getConfig());
            }
        }
        else
        {
            // Changing "code" normally causes the XDR structure to be
            // destructed, then a different XDR structure is constructed.
            // However, txFAILED and txSUCCESS have the same underlying field
            // number so this does not occur.
            txResult.setResultCode(txFAILED);
            if (protocolVersionStartsFrom(ledgerVersion,
                                          SOROBAN_PROTOCOL_VERSION) &&
                isSoroban())
            {
                // If transaction fails, we don't charge for any
                // refundable resources.
                auto preApplyFee = computePreApplySorobanResourceFee(
                    ledgerVersion,
                    app.getLedgerManager().getSorobanNetworkConfig(),
                    app.getConfig());

                txResult.setSorobanFeeRefund(declaredSorobanResourceFee() -
                                             preApplyFee.non_refundable_fee);

                txResult.publishFailureDiagnosticsToMeta(outerMeta,
                                                         app.getConfig());
            }
        }
        return success;
    }
    catch (InvariantDoesNotHold& e)
    {
        printErrorAndAbort("Invariant failure while applying operations: ",
                           e.what());
    }
    catch (std::bad_alloc& e)
    {
        printErrorAndAbort("Exception while applying operations: ", e.what());
    }
    catch (std::exception& e)
    {
        if (reportInternalErrOnException)
        {
            CLOG_ERROR(Tx, "Exception while applying operations ({}, {}): {}",
                       xdr_to_string(getFullHash(), "fullHash"),
                       xdr_to_string(getContentsHash(), "contentsHash"),
                       e.what());
        }
        else
        {
            CLOG_INFO(Tx,
                      "Exception occurred on outdated protocol version "
                      "while applying operations ({}, {}): {}",
                      xdr_to_string(getFullHash(), "fullHash"),
                      xdr_to_string(getContentsHash(), "contentsHash"),
                      e.what());
        }
    }
    catch (...)
    {
        if (reportInternalErrOnException)
        {
            CLOG_ERROR(Tx,
                       "Unknown exception while applying operations ({}, {})",
                       xdr_to_string(getFullHash(), "fullHash"),
                       xdr_to_string(getContentsHash(), "contentsHash"));
        }
        else
        {
            CLOG_INFO(Tx,
                      "Unknown exception on outdated protocol version "
                      "while applying operations ({}, {})",
                      xdr_to_string(getFullHash(), "fullHash"),
                      xdr_to_string(getContentsHash(), "contentsHash"));
        }
    }
    if (app.getConfig().HALT_ON_INTERNAL_TRANSACTION_ERROR)
    {
        printErrorAndAbort("Encountered an exception while applying "
                           "operations, see logs for details.");
    }
    // This is only reachable if an exception is thrown
    txResult.setResultCode(txINTERNAL_ERROR);

    // We only increase the internal-error metric count if the ledger is a
    // newer version.
    if (reportInternalErrOnException)
    {
        internalErrorCounter.inc();
    }

    // operations and txChangesAfter should already be empty at this point
    outerMeta.clearOperationMetas();
    outerMeta.clearTxChangesAfter();
    return false;
}

bool
TransactionFrame::apply(Application& app, AbstractLedgerTxn& ltx,
                        TransactionMetaFrame& meta,
                        TransactionResultPayloadPtr txResult, bool chargeFee,
                        Hash const& sorobanBasePrngSeed) const
{
    ZoneScoped;
    try
    {
        mCachedAccountPreProtocol8.reset();
        uint32_t ledgerVersion = ltx.loadHeader().current().ledgerVersion;
        SignatureChecker signatureChecker{ledgerVersion, getContentsHash(),
                                          getSignatures(mEnvelope)};

        //  when applying, a failure during tx validation means that
        //  we'll skip trying to apply operations but we'll still
        //  process the sequence number if needed
        std::optional<FeePair> sorobanResourceFee;
        if (protocolVersionStartsFrom(ledgerVersion,
                                      SOROBAN_PROTOCOL_VERSION) &&
            isSoroban())
        {
            sorobanResourceFee = computePreApplySorobanResourceFee(
                ledgerVersion, app.getLedgerManager().getSorobanNetworkConfig(),
                app.getConfig());

            txResult->setSorobanConsumedNonRefundableFee(
                sorobanResourceFee->non_refundable_fee);
            txResult->setSorobanFeeRefund(
                declaredSorobanResourceFee() -
                sorobanResourceFee->non_refundable_fee);
        }
        LedgerTxn ltxTx(ltx);
        auto cv = commonValid(app, signatureChecker, ltxTx, 0, true, chargeFee,
                              0, 0, sorobanResourceFee, txResult);
        if (cv >= ValidationType::kInvalidUpdateSeqNum)
        {
            processSeqNum(ltxTx);
        }

        bool signaturesValid =
            processSignatures(cv, signatureChecker, ltxTx, *txResult);

        meta.pushTxChangesBefore(ltxTx.getChanges());
        ltxTx.commit();

        bool ok = signaturesValid && cv == ValidationType::kMaybeValid;
        try
        {
            // This should only throw if the logging during exception
            // handling for applyOperations throws. In that case, we may not
            // have the correct TransactionResult so we must crash.
            if (ok)
            {
                if (isSoroban())
                {
                    updateSorobanMetrics(app);
                }

                ok = applyOperations(signatureChecker, app, ltx, meta,
                                     *txResult, sorobanBasePrngSeed);
            }
            return ok;
        }
        catch (std::exception& e)
        {
            printErrorAndAbort("Exception while applying operations: ",
                               e.what());
        }
        catch (...)
        {
            printErrorAndAbort("Unknown exception while applying operations");
        }
    }
    catch (std::exception& e)
    {
        printErrorAndAbort("Exception after processing fees but before "
                           "processing sequence number: ",
                           e.what());
    }
    catch (...)
    {
        printErrorAndAbort("Unknown exception after processing fees but before "
                           "processing sequence number");
    }
}

bool
TransactionFrame::apply(Application& app, AbstractLedgerTxn& ltx,
                        TransactionMetaFrame& meta,
                        TransactionResultPayloadPtr txResult,
                        Hash const& sorobanBasePrngSeed) const
{
    return apply(app, ltx, meta, txResult, true, sorobanBasePrngSeed);
}

void
TransactionFrame::processPostApply(Application& app,
                                   AbstractLedgerTxn& ltxOuter,
                                   TransactionMetaFrame& meta,
                                   TransactionResultPayloadPtr txResult) const
{
    releaseAssertOrThrow(txResult);
    processRefund(app, ltxOuter, meta, getSourceID(), *txResult);
}

// This is a TransactionFrame specific function that should only be used by
// FeeBumpTransactionFrame to forward a different account for the refund.
int64_t
TransactionFrame::processRefund(Application& app, AbstractLedgerTxn& ltxOuter,
                                TransactionMetaFrame& meta,
                                AccountID const& feeSource,
                                MutableTransactionResultBase& txResult) const
{
    ZoneScoped;

    if (!isSoroban())
    {
        return 0;
    }
    // Process Soroban resource fee refund (this is independent of the
    // transaction success).
    LedgerTxn ltx(ltxOuter);
    int64_t refund = refundSorobanFee(ltx, feeSource, txResult);
    meta.pushTxChangesAfter(ltx.getChanges());
    ltx.commit();

    return refund;
}

std::shared_ptr<StellarMessage const>
TransactionFrame::toStellarMessage() const
{
    auto msg = std::make_shared<StellarMessage>();
    msg->type(TRANSACTION);
    msg->transaction() = mEnvelope;
    return msg;
}

uint32_t
TransactionFrame::getSize() const
{
    ZoneScoped;
    return static_cast<uint32_t>(xdr::xdr_size(mEnvelope));
}
} // namespace stellar
