// Copyright 2021 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/ClawbackClaimableBalanceOpFrame.h"
#include "ledger/LedgerTxn.h"
#include "transactions/SponsorshipUtils.h"
#include "transactions/TransactionUtils.h"
#include <Tracy.hpp>

namespace stellar
{

ClawbackClaimableBalanceOpFrame::ClawbackClaimableBalanceOpFrame(
    Operation const& op, OperationResult& res, TransactionFrame& parentTx)
    : OperationFrame(op, res, parentTx)
    , mClawbackClaimableBalance(mOperation.body.clawbackClaimableBalanceOp())
{
}

bool
ClawbackClaimableBalanceOpFrame::isVersionSupported(
    uint32_t protocolVersion) const
{
    return protocolVersion >= 16;
}

bool
ClawbackClaimableBalanceOpFrame::doApply(AbstractLedgerTxn& ltx)
{
    ZoneNamedN(applyZone, "ClawbackClaimableBalanceOp apply", true);

    auto claimableBalanceLtxEntry =
        stellar::loadClaimableBalance(ltx, mClawbackClaimableBalance.balanceID);
    if (!claimableBalanceLtxEntry)
    {
        innerResult().code(CLAWBACK_CLAIMABLE_BALANCE_DOES_NOT_EXIST);
        return false;
    }

    auto const& claimableBalance =
        claimableBalanceLtxEntry.current().data.claimableBalance();

    if (claimableBalance.asset.type() == ASSET_TYPE_NATIVE)
    {
        innerResult().code(CLAWBACK_CLAIMABLE_BALANCE_NOT_ISSUER);
        return false;
    }

    switch (claimableBalance.asset.type())
    {
    case ASSET_TYPE_NATIVE:
        innerResult().code(CLAWBACK_CLAIMABLE_BALANCE_NOT_ISSUER);
        return false;

    case ASSET_TYPE_CREDIT_ALPHANUM4:
        if (!(claimableBalance.asset.alphaNum4().issuer == getSourceID()))
        {
            innerResult().code(CLAWBACK_CLAIMABLE_BALANCE_NOT_ISSUER);
            return false;
        }
        break;
    case ASSET_TYPE_CREDIT_ALPHANUM12:
        if (!(claimableBalance.asset.alphaNum12().issuer == getSourceID()))
        {
            innerResult().code(CLAWBACK_CLAIMABLE_BALANCE_NOT_ISSUER);
            return false;
        }
        break;
    default:
        throw std::runtime_error(
            "ClawbackClaimableBalanceOpFrame - Unknown asset type");
    }

    if (!isClawbackEnabledOnClaimableBalance(
            claimableBalanceLtxEntry.current()))
    {
        innerResult().code(CLAWBACK_CLAIMABLE_BALANCE_NOT_CLAWBACK_ENABLED);
        return false;
    }

    auto header = ltx.loadHeader();
    auto sourceAccount = loadSourceAccount(ltx, header);
    removeEntryWithPossibleSponsorship(
        ltx, header, claimableBalanceLtxEntry.current(), sourceAccount);

    claimableBalanceLtxEntry.erase();

    innerResult().code(CLAWBACK_CLAIMABLE_BALANCE_SUCCESS);
    return true;
}

bool
ClawbackClaimableBalanceOpFrame::doCheckValid(uint32_t ledgerVersion)
{
    return true;
}

void
ClawbackClaimableBalanceOpFrame::insertLedgerKeysToPrefetch(
    UnorderedSet<LedgerKey>& keys) const
{
    keys.emplace(claimableBalanceKey(mClawbackClaimableBalance.balanceID));
}
}
