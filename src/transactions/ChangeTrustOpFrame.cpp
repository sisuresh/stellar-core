// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "ChangeTrustOpFrame.h"
#include "database/Database.h"
#include "ledger/LedgerManager.h"
#include "ledger/LedgerTxn.h"
#include "ledger/LedgerTxnEntry.h"
#include "ledger/LedgerTxnHeader.h"
#include "ledger/TrustLineWrapper.h"
#include "main/Application.h"
#include "transactions/SponsorshipUtils.h"
#include "transactions/TransactionUtils.h"
#include <Tracy.hpp>

namespace stellar
{

static void
decrementPoolUseCount(AbstractLedgerTxn& ltx, Asset const& asset,
                      AccountID const& accountID)
{
    if (!isIssuer(accountID, asset) && asset.type() != ASSET_TYPE_NATIVE)
    {
        auto assetTrustLine = ltx.load(trustlineKey(accountID, asset));
        if (!assetTrustLine)
        {
            throw std::runtime_error("asset trustline is missing");
        }

        auto& ext = getTrustLineEntryExtensionV2(
            assetTrustLine.current().data.trustLine());
        if (--ext.liquidityPoolUseCount < 0)
        {
            throw std::runtime_error("cannot decrement liquidityPoolUseCount");
        }
    }
}

static void
managePool(AbstractLedgerTxn& ltx,
           LiquidityPoolConstantProductParameters const& cpParams,
           PoolID const& poolID)
{
    auto poolLtxEntry = loadLiquidityPool(ltx, poolID);
    if (poolLtxEntry)
    {
        ++poolLtxEntry.current()
              .data.liquidityPool()
              .body.constantProduct()
              .poolSharesTrustLineCount;
    }
    else
    {
        LedgerEntry liquidityPoolEntry;
        liquidityPoolEntry.data.type(LIQUIDITY_POOL);
        auto& lp = liquidityPoolEntry.data.liquidityPool();
        lp.liquidityPoolID = poolID;

        auto& cp = lp.body.constantProduct();
        cp.reserveA = 0;
        cp.reserveB = 0;
        cp.totalPoolShares = 0;
        cp.poolSharesTrustLineCount = 1;
        cp.params = cpParams;

        ltx.create(liquidityPoolEntry);
    }
}

bool
ChangeTrustOpFrame::manageAssetTrustlineForPool(AbstractLedgerTxn& ltx,
                                                Asset const& asset)
{
    if (!isIssuer(getSourceID(), asset) && asset.type() != ASSET_TYPE_NATIVE)
    {
        auto assetTrustLine = ltx.load(trustlineKey(getSourceID(), asset));
        if (!assetTrustLine)
        {
            innerResult().code(CHANGE_TRUST_TRUST_LINE_MISSING);
            return false;
        }

        if (!isAuthorizedToMaintainLiabilities(assetTrustLine))
        {
            innerResult().code(CHANGE_TRUST_NOT_AUTH_MAINTAIN_LIABILITIES);
            return false;
        }

        ++prepareTrustLineEntryExtensionV2(
              assetTrustLine.current().data.trustLine())
              .liquidityPoolUseCount;
    }

    return true;
}

ChangeTrustOpFrame::ChangeTrustOpFrame(Operation const& op,
                                       OperationResult& res,
                                       TransactionFrame& parentTx)
    : OperationFrame(op, res, parentTx)
    , mChangeTrust(mOperation.body.changeTrustOp())
{
}

bool
ChangeTrustOpFrame::doApply(AbstractLedgerTxn& ltx)
{
    ZoneNamedN(applyZone, "ChangeTrustOp apply", true);
    auto header = ltx.loadHeader();

    if (mChangeTrust.line.type() == ASSET_TYPE_NATIVE)
    {
        throw std::runtime_error("native asset is not valid in ChangeTrustOp");
    }

    if (header.current().ledgerVersion > 2)
    {
        // Note: No longer checking if issuer exists here, because if
        //     issuerID == getSourceID()
        // and issuer does not exist then this operation would have already
        // failed with opNO_ACCOUNT.
        if (isIssuer(getSourceID(), mChangeTrust.line))
        {
            // since version 3 it is not allowed to use CHANGE_TRUST on self
            innerResult().code(CHANGE_TRUST_SELF_NOT_ALLOWED);
            return false;
        }
    }
    else if (isIssuer(getSourceID(), mChangeTrust.line))
    {
        if (mChangeTrust.limit < INT64_MAX)
        {
            innerResult().code(CHANGE_TRUST_INVALID_LIMIT);
            return false;
        }
        else if (!stellar::loadAccountWithoutRecord(ltx, getSourceID()))
        {
            innerResult().code(CHANGE_TRUST_NO_ISSUER);
            return false;
        }
        return true;
    }

    auto const key = trustlineKey(
        getSourceID(), changeTrustAssetToTrustLineAsset(mChangeTrust.line));

    auto trustLine = ltx.load(key);
    if (trustLine)
    { // we are modifying an old trustline
        if (mChangeTrust.limit < getMinimumLimit(header, trustLine))
        {
            // Can't drop the limit below the balance you are holding with them
            innerResult().code(CHANGE_TRUST_INVALID_LIMIT);
            return false;
        }

        if (mChangeTrust.limit == 0)
        {
            // use a lambda so we don't hold a reference to the TrustLineEntry
            auto tlEntry = [&]() -> TrustLineEntry const& {
                return trustLine.current().data.trustLine();
            };

            if (tlEntry().asset.type() == ASSET_TYPE_POOL_SHARE)
            {
                auto const& cp =
                    mChangeTrust.line.liquidityPool().constantProduct();
                decrementPoolUseCount(ltx, cp.assetA, getSourceID());
                decrementPoolUseCount(ltx, cp.assetB, getSourceID());

                auto const& poolID = key.trustLine().asset.liquidityPoolID();
                auto poolLtxEntry = loadLiquidityPool(ltx, poolID);
                if (!poolLtxEntry)
                {
                    throw std::runtime_error("liquidity pool is missing");
                }

                auto poolTLCount = --poolLtxEntry.current()
                                         .data.liquidityPool()
                                         .body.constantProduct()
                                         .poolSharesTrustLineCount;
                if (poolTLCount == 0)
                {
                    poolLtxEntry.erase();
                }
            }
            else if (hasTrustLineEntryExtV2(tlEntry()) &&
                     tlEntry().ext.v1().ext.v2().liquidityPoolUseCount != 0)
            {
                innerResult().code(CHANGE_TRUST_CANNOT_DELETE);
                return false;
            }

            // line gets deleted
            auto sourceAccount = loadSourceAccount(ltx, header);
            removeEntryWithPossibleSponsorship(ltx, header, trustLine.current(),
                                               sourceAccount);
            trustLine.erase();
        }
        else
        {
            if (mChangeTrust.line.type() != ASSET_TYPE_POOL_SHARE &&
                !stellar::loadAccountWithoutRecord(
                    ltx, getIssuer(mChangeTrust.line)))
            {
                innerResult().code(CHANGE_TRUST_NO_ISSUER);
                return false;
            }
            trustLine.current().data.trustLine().limit = mChangeTrust.limit;
        }
        innerResult().code(CHANGE_TRUST_SUCCESS);
        return true;
    }
    else
    { // new trust line
        if (mChangeTrust.limit == 0)
        {
            innerResult().code(CHANGE_TRUST_INVALID_LIMIT);
            return false;
        }

        LedgerEntry trustLineEntry;
        trustLineEntry.data.type(TRUSTLINE);
        auto& tl = trustLineEntry.data.trustLine();
        tl.accountID = getSourceID();
        tl.asset = key.trustLine().asset;
        tl.limit = mChangeTrust.limit;
        tl.balance = 0;

        if (mChangeTrust.line.type() != ASSET_TYPE_POOL_SHARE)
        {
            auto issuer = stellar::loadAccountWithoutRecord(
                ltx, getIssuer(mChangeTrust.line));
            if (!issuer)
            {
                innerResult().code(CHANGE_TRUST_NO_ISSUER);
                return false;
            }
            if (!isAuthRequired(issuer))
            {
                tl.flags = AUTHORIZED_FLAG;
            }
            if (isClawbackEnabledOnAccount(issuer))
            {
                tl.flags |= TRUSTLINE_CLAWBACK_ENABLED_FLAG;
            }
        }
        else
        {
            auto const& cp =
                mChangeTrust.line.liquidityPool().constantProduct();
            if (!manageAssetTrustlineForPool(ltx, cp.assetA) ||
                !manageAssetTrustlineForPool(ltx, cp.assetB))
            {
                return false;
            }

            managePool(ltx, cp, key.trustLine().asset.liquidityPoolID());
        }

        auto sourceAccount = loadSourceAccount(ltx, header);
        switch (createEntryWithPossibleSponsorship(ltx, header, trustLineEntry,
                                                   sourceAccount))
        {
        case SponsorshipResult::SUCCESS:
            break;
        case SponsorshipResult::LOW_RESERVE:
            innerResult().code(CHANGE_TRUST_LOW_RESERVE);
            return false;
        case SponsorshipResult::TOO_MANY_SUBENTRIES:
            mResult.code(opTOO_MANY_SUBENTRIES);
            return false;
        case SponsorshipResult::TOO_MANY_SPONSORING:
            mResult.code(opTOO_MANY_SPONSORING);
            return false;
        case SponsorshipResult::TOO_MANY_SPONSORED:
            // This is impossible right now because there is a limit on sub
            // entries, fall through and throw
        default:
            throw std::runtime_error("Unexpected result from "
                                     "createEntryWithPossibleSponsorship");
        }
        ltx.create(trustLineEntry);

        innerResult().code(CHANGE_TRUST_SUCCESS);
        return true;
    }
}

bool
ChangeTrustOpFrame::doCheckValid(uint32_t ledgerVersion)
{
    if (mChangeTrust.limit < 0)
    {
        innerResult().code(CHANGE_TRUST_MALFORMED);
        return false;
    }

    if (!isChangeTrustAssetValid(mChangeTrust.line, ledgerVersion))
    {
        innerResult().code(CHANGE_TRUST_MALFORMED);
        return false;
    }
    if (ledgerVersion > 9)
    {
        if (mChangeTrust.line.type() == ASSET_TYPE_NATIVE)
        {
            innerResult().code(CHANGE_TRUST_MALFORMED);
            return false;
        }
    }

    if (ledgerVersion > 15 && isIssuer(getSourceID(), mChangeTrust.line))
    {
        innerResult().code(CHANGE_TRUST_MALFORMED);
        return false;
    }
    return true;
}
}
