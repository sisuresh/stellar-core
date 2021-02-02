// Copyright 2021 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/SetTrustLineFlagsOpFrame.h"
#include "ledger/LedgerTxn.h"
#include "ledger/LedgerTxnEntry.h"
#include "ledger/LedgerTxnHeader.h"
#include "main/Application.h"
#include "transactions/SponsorshipUtils.h"
#include "transactions/TransactionUtils.h"
#include "util/XDROperators.h"
#include <Tracy.hpp>

namespace stellar
{

SetTrustLineFlagsOpFrame::SetTrustLineFlagsOpFrame(Operation const& op,
                                                   OperationResult& res,
                                                   TransactionFrame& parentTx)
    : OperationFrame(op, res, parentTx)
    , mSetTrustLineFlags(mOperation.body.setTrustLineFlagsOp())
{
}

ThresholdLevel
SetTrustLineFlagsOpFrame::getThresholdLevel() const
{
    return ThresholdLevel::LOW;
}

bool
SetTrustLineFlagsOpFrame::isVersionSupported(uint32_t protocolVersion) const
{
    return protocolVersion >= 16;
}

bool
SetTrustLineFlagsOpFrame::doApply(AbstractLedgerTxn& ltx)
{
    ZoneNamedN(applyZone, "SetTrustLineFlagsOp apply", true);

    {
        LedgerTxn ltxSource(ltx); // ltxSource will be rolled back
        auto header = ltxSource.loadHeader();
        auto sourceAccountEntry = loadSourceAccount(ltxSource, header);
        auto const& sourceAccount = sourceAccountEntry.current().data.account();

        const uint32_t authFlags =
            AUTHORIZED_FLAG | AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG;

        bool authNotRevocable = !(sourceAccount.flags & AUTH_REVOCABLE_FLAG);

        // This condition covers all three revoke scenarios (AUTHORIZED_FLAG ->
        // AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG (You need to clear
        // AUTHORIZED_FLAG here, if not, then the auth flags will be invalid and
        // the op will return SET_TRUST_LINE_FLAGS_INVALID_STATE),
        // AUTHORIZED_FLAG -> 0, or AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG ->
        // 0).
        // If setFlags and clearFlags overlap, the op would've failed during
        // validation
        bool clearingAnyAuth =
            mSetTrustLineFlags.clearFlags &&
            (*mSetTrustLineFlags.clearFlags & authFlags) != 0;
        bool settingAuthorized =
            mSetTrustLineFlags.setFlags &&
            (*mSetTrustLineFlags.setFlags & AUTHORIZED_FLAG) != 0;
        if (authNotRevocable && clearingAnyAuth && !settingAuthorized)
        {
            innerResult().code(SET_TRUST_LINE_FLAGS_CANT_REVOKE);
            return false;
        }
    }

    auto key =
        trustlineKey(mSetTrustLineFlags.trustor, mSetTrustLineFlags.asset);
    bool shouldRemoveOffers = false;
    uint32_t expectedFlagValue = 0;
    {
        // trustline is loaded in this inner scope because it can be loaded
        // again in removeOffersByAccountAndAsset
        auto trust = ltx.load(key);
        if (!trust)
        {
            innerResult().code(SET_TRUST_LINE_FLAGS_NO_TRUST_LINE);
            return false;
        }

        expectedFlagValue = trust.current().data.trustLine().flags;
        if (mSetTrustLineFlags.clearFlags)
        {
            expectedFlagValue &= ~*mSetTrustLineFlags.clearFlags;
        }

        if (mSetTrustLineFlags.setFlags)
        {
            expectedFlagValue |= *mSetTrustLineFlags.setFlags;
        }

        shouldRemoveOffers =
            isAuthorizedToMaintainLiabilities(trust) && expectedFlagValue == 0;
    }

    if (!trustLineFlagAuthIsValid(expectedFlagValue))
    {
        innerResult().code(SET_TRUST_LINE_FLAGS_INVALID_STATE);
        return false;
    }

    if (shouldRemoveOffers)
    {
        auto header = ltx.loadHeader();
        removeOffersByAccountAndAsset(ltx, header, mSetTrustLineFlags.trustor,
                                      mSetTrustLineFlags.asset);
    }

    auto trust = ltx.load(key);
    trust.current().data.trustLine().flags = expectedFlagValue;
    innerResult().code(SET_TRUST_LINE_FLAGS_SUCCESS);
    return true;
}

bool
SetTrustLineFlagsOpFrame::doCheckValid(uint32_t ledgerVersion)
{
    if (mSetTrustLineFlags.asset.type() == ASSET_TYPE_NATIVE)
    {
        innerResult().code(SET_TRUST_LINE_FLAGS_MALFORMED);
        return false;
    }

    if (!isAssetValid(mSetTrustLineFlags.asset))
    {
        innerResult().code(SET_TRUST_LINE_FLAGS_MALFORMED);
        return false;
    }

    if (!(getSourceID() == getIssuer(mSetTrustLineFlags.asset)))
    {
        innerResult().code(SET_TRUST_LINE_FLAGS_MALFORMED);
        return false;
    }

    if (mSetTrustLineFlags.trustor == getSourceID())
    {
        innerResult().code(SET_TRUST_LINE_FLAGS_MALFORMED);
        return false;
    }

    if (mSetTrustLineFlags.setFlags && mSetTrustLineFlags.clearFlags)
    {
        if ((*mSetTrustLineFlags.setFlags & *mSetTrustLineFlags.clearFlags) !=
            0)
        {
            innerResult().code(SET_TRUST_LINE_FLAGS_MALFORMED);
            return false;
        }
    }

    if (mSetTrustLineFlags.setFlags)
    {
        // setFlags has the same restrictions as the trustline flags, so we can
        // use trustLineFlagIsValid here
        if (!trustLineFlagIsValid(*mSetTrustLineFlags.setFlags,
                                  ledgerVersion) ||
            (*mSetTrustLineFlags.setFlags & TRUSTLINE_CLAWBACK_ENABLED_FLAG) !=
                0)
        {
            innerResult().code(SET_TRUST_LINE_FLAGS_MALFORMED);
            return false;
        }
    }

    if (mSetTrustLineFlags.clearFlags &&
        !trustLineFlagMaskCheckIsValid(*mSetTrustLineFlags.clearFlags,
                                       ledgerVersion))
    {
        innerResult().code(SET_TRUST_LINE_FLAGS_MALFORMED);
        return false;
    }

    return true;
}

void
SetTrustLineFlagsOpFrame::insertLedgerKeysToPrefetch(
    UnorderedSet<LedgerKey>& keys) const
{
    if (mSetTrustLineFlags.asset.type() != ASSET_TYPE_NATIVE)
    {
        keys.emplace(
            trustlineKey(mSetTrustLineFlags.trustor, mSetTrustLineFlags.asset));
    }
}
}
