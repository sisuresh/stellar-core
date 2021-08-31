// Copyright 2021 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "ledger/LedgerTxn.h"
#include "lib/catch.hpp"
#include "main/Application.h"
#include "test/TestAccount.h"
#include "test/TestExceptions.h"
#include "test/TestMarket.h"
#include "test/TestUtils.h"
#include "test/TxTests.h"
#include "test/test.h"
#include "transactions/TransactionUtils.h"
#include "transactions/test/SponsorshipTestUtils.h"
#include "util/Timer.h"

using namespace stellar;
using namespace stellar::txtest;

static ClaimableBalanceID
getRevokeBalanceID(TestAccount& testAccount, Asset const& asset,
                   PoolID const& poolID, uint32_t opNum,
                   EnvelopeType envelopeType = ENVELOPE_TYPE_POOL_REVOKE_OP_ID)
{
    HashIDPreimage hashPreimage;
    hashPreimage.type(envelopeType);

    if (envelopeType == ENVELOPE_TYPE_POOL_REVOKE_OP_ID)
    {
        hashPreimage.revokeID().sourceAccount = testAccount;
        hashPreimage.revokeID().seqNum = testAccount.getLastSequenceNumber();
        hashPreimage.revokeID().opNum = opNum;
        hashPreimage.revokeID().liquidityPoolID = poolID;
        hashPreimage.revokeID().asset = asset;
    }
    else
    {
        hashPreimage.operationID().sourceAccount = testAccount;
        hashPreimage.operationID().seqNum = testAccount.getLastSequenceNumber();
        hashPreimage.operationID().opNum = opNum;
    }

    ClaimableBalanceID balanceID;
    balanceID.type(CLAIMABLE_BALANCE_ID_TYPE_V0);
    balanceID.v0() = xdrSha256(hashPreimage);

    return balanceID;
}

TEST_CASE("set trustline flags", "[tx][settrustlineflags]")
{
    auto const& cfg = getTestConfig();

    VirtualClock clock;
    auto app = createTestApplication(clock, cfg);

    const int64_t trustLineLimit = INT64_MAX;
    const int64_t trustLineStartingBalance = 20000;

    auto const minBalance4 = app->getLedgerManager().getLastMinBalance(4);

    // set up world
    auto root = TestAccount::createRoot(*app);
    auto gateway = root.create("gw", minBalance4);
    auto a1 = root.create("A1", minBalance4 + 10000);
    auto a2 = root.create("A2", minBalance4);

    auto idr = makeAsset(gateway, "IDR");
    auto native = makeNativeAsset();

    gateway.setOptions(setFlags(AUTH_REVOCABLE_FLAG));

    // gateway is not auth required, so trustline will be authorized
    a1.changeTrust(idr, trustLineLimit);

    SetTrustLineFlagsArguments emptyFlag;

    SECTION("not supported before version 17")
    {
        for_versions_to(16, *app, [&] {
            REQUIRE_THROWS_AS(gateway.setTrustLineFlags(idr, a1, emptyFlag),
                              ex_opNOT_SUPPORTED);
        });
    }

    for_versions_from(17, *app, [&] {
        // this lambda is used to verify offers are not pulled in non-revoke
        // scenarios
        auto market = TestMarket{*app};
        auto setFlagAndCheckOffer =
            [&](Asset const& asset, TestAccount& trustor,
                txtest::SetTrustLineFlagsArguments const& arguments,
                bool addOffer = true) {
                if (addOffer)
                {
                    auto offer = market.requireChangesWithOffer({}, [&] {
                        return market.addOffer(trustor,
                                               {native, asset, Price{1, 1}, 1});
                    });
                }

                // no offer should be deleted
                market.requireChanges({}, [&] {
                    gateway.setTrustLineFlags(asset, trustor, arguments);
                });
            };

        SECTION("small test")
        {
            gateway.pay(a1, idr, 5);
            a1.pay(gateway, idr, 1);

            auto flags =
                setTrustLineFlags(AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG) |
                clearTrustLineFlags(AUTHORIZED_FLAG);

            setFlagAndCheckOffer(idr, a1, flags);

            REQUIRE_THROWS_AS(a1.pay(gateway, idr, trustLineStartingBalance),
                              ex_PAYMENT_SRC_NOT_AUTHORIZED);
        }

        SECTION("empty flags")
        {
            // verify that the setTrustLineFlags call is a noop
            auto flag = a1.getTrustlineFlags(idr);
            setFlagAndCheckOffer(idr, a1, emptyFlag);
            REQUIRE(flag == a1.getTrustlineFlags(idr));
        }

        SECTION("clear clawback")
        {
            gateway.setOptions(setFlags(AUTH_CLAWBACK_ENABLED_FLAG));
            a2.changeTrust(idr, trustLineLimit);
            gateway.pay(a2, idr, 100);

            gateway.clawback(a2, idr, 25);

            // clear the clawback flag and then try to clawback
            setFlagAndCheckOffer(
                idr, a2, clearTrustLineFlags(TRUSTLINE_CLAWBACK_ENABLED_FLAG));
            REQUIRE_THROWS_AS(gateway.clawback(a2, idr, 25),
                              ex_CLAWBACK_NOT_CLAWBACK_ENABLED);
        }

        SECTION("upgrade auth when not revocable")
        {
            SECTION("authorized -> authorized to maintain liabilities -> "
                    "authorized - with offers")
            {
                // authorized -> authorized to maintain liabilities
                auto maintainLiabilitiesflags =
                    setTrustLineFlags(AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG) |
                    clearTrustLineFlags(AUTHORIZED_FLAG);

                setFlagAndCheckOffer(idr, a1, maintainLiabilitiesflags);

                gateway.setOptions(clearFlags(AUTH_REVOCABLE_FLAG));

                // authorized to maintain liabilities -> authorized
                auto authorizedFlags =
                    setTrustLineFlags(AUTHORIZED_FLAG) |
                    clearTrustLineFlags(
                        AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG);
                setFlagAndCheckOffer(idr, a1, authorizedFlags, false);
            }

            SECTION("0 -> authorized")
            {
                gateway.denyTrust(idr, a1, TrustFlagOp::SET_TRUST_LINE_FLAGS);
                gateway.setOptions(clearFlags(AUTH_REVOCABLE_FLAG));

                gateway.setTrustLineFlags(idr, a1,
                                          setTrustLineFlags(AUTHORIZED_FLAG));
            }

            SECTION("0 -> authorized to maintain liabilities")
            {
                gateway.denyTrust(idr, a1, TrustFlagOp::SET_TRUST_LINE_FLAGS);
                gateway.setOptions(clearFlags(AUTH_REVOCABLE_FLAG));

                gateway.setTrustLineFlags(
                    idr, a1,
                    setTrustLineFlags(AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG));
            }
        }

        SECTION("errors")
        {
            SECTION("invalid state")
            {
                gateway.setOptions(setFlags(AUTH_REQUIRED_FLAG));
                a2.changeTrust(idr, trustLineLimit);

                SECTION("set maintain liabilities when authorized")
                {
                    gateway.setTrustLineFlags(
                        idr, a2, setTrustLineFlags(AUTHORIZED_FLAG));
                    REQUIRE_THROWS_AS(
                        gateway.setTrustLineFlags(
                            idr, a2,
                            setTrustLineFlags(
                                AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG)),
                        ex_SET_TRUST_LINE_FLAGS_INVALID_STATE);
                }
                SECTION("set authorized when maintain liabilities")
                {
                    gateway.setTrustLineFlags(
                        idr, a2,
                        setTrustLineFlags(
                            AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG));
                    REQUIRE_THROWS_AS(
                        gateway.setTrustLineFlags(
                            idr, a2, setTrustLineFlags(AUTHORIZED_FLAG)),
                        ex_SET_TRUST_LINE_FLAGS_INVALID_STATE);
                }
            }

            SECTION("can't revoke")
            {
                // AllowTrustTests.cpp covers most cases.

                SECTION("authorized -> 0")
                {
                    gateway.setOptions(clearFlags(AUTH_REVOCABLE_FLAG));

                    REQUIRE_THROWS_AS(
                        gateway.setTrustLineFlags(
                            idr, a1, clearTrustLineFlags(AUTHORIZED_FLAG)),
                        ex_SET_TRUST_LINE_FLAGS_CANT_REVOKE);
                }
                SECTION("authorized to maintain liabilities -> 0")
                {
                    gateway.allowMaintainLiabilities(
                        idr, a1, TrustFlagOp::SET_TRUST_LINE_FLAGS);
                    gateway.setOptions(clearFlags(AUTH_REVOCABLE_FLAG));

                    REQUIRE_THROWS_AS(
                        gateway.setTrustLineFlags(
                            idr, a1,
                            clearTrustLineFlags(
                                AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG)),
                        ex_SET_TRUST_LINE_FLAGS_CANT_REVOKE);
                }
            }

            SECTION("no trust")
            {
                REQUIRE_THROWS_AS(gateway.setTrustLineFlags(idr, a2, emptyFlag),
                                  ex_SET_TRUST_LINE_FLAGS_NO_TRUST_LINE);
            }

            SECTION("malformed")
            {
                // invalid auth flags
                REQUIRE_THROWS_AS(
                    gateway.setTrustLineFlags(
                        idr, a1, setTrustLineFlags(TRUSTLINE_AUTH_FLAGS)),
                    ex_SET_TRUST_LINE_FLAGS_MALFORMED);

                // can't set clawback
                REQUIRE_THROWS_AS(
                    gateway.setTrustLineFlags(
                        idr, a1,
                        setTrustLineFlags(TRUSTLINE_CLAWBACK_ENABLED_FLAG)),
                    ex_SET_TRUST_LINE_FLAGS_MALFORMED);

                // can't use native asset
                REQUIRE_THROWS_AS(
                    gateway.setTrustLineFlags(native, a1, emptyFlag),
                    ex_SET_TRUST_LINE_FLAGS_MALFORMED);

                // invalid asset
                auto invalidAssets = testutil::getInvalidAssets(gateway);
                for (auto const& asset : invalidAssets)
                {
                    REQUIRE_THROWS_AS(
                        gateway.setTrustLineFlags(asset, a1, emptyFlag),
                        ex_SET_TRUST_LINE_FLAGS_MALFORMED);
                }

                {
                    // set and clear flags can't overlap
                    auto setFlag = setTrustLineFlags(AUTHORIZED_FLAG);
                    auto clearFlag = clearTrustLineFlags(TRUSTLINE_AUTH_FLAGS);
                    REQUIRE_THROWS_AS(
                        gateway.setTrustLineFlags(idr, a1, setFlag | clearFlag),
                        ex_SET_TRUST_LINE_FLAGS_MALFORMED);

                    REQUIRE_THROWS_AS(
                        gateway.setTrustLineFlags(
                            idr, a1,
                            setFlag | clearTrustLineFlags(AUTHORIZED_FLAG)),
                        ex_SET_TRUST_LINE_FLAGS_MALFORMED);
                }

                // can't clear or set unsupported flags
                REQUIRE_THROWS_AS(
                    gateway.setTrustLineFlags(
                        idr, a1,
                        setTrustLineFlags(MASK_TRUSTLINE_FLAGS_V17 + 1)),
                    ex_SET_TRUST_LINE_FLAGS_MALFORMED);
                REQUIRE_THROWS_AS(
                    gateway.setTrustLineFlags(
                        idr, a1,
                        clearTrustLineFlags(MASK_TRUSTLINE_FLAGS_V17 + 1)),
                    ex_SET_TRUST_LINE_FLAGS_MALFORMED);

                // can't operate on self
                REQUIRE_THROWS_AS(
                    gateway.setTrustLineFlags(idr, gateway, emptyFlag),
                    ex_SET_TRUST_LINE_FLAGS_MALFORMED);

                // source account is not issuer
                REQUIRE_THROWS_AS(a1.setTrustLineFlags(idr, gateway, emptyFlag),
                                  ex_SET_TRUST_LINE_FLAGS_MALFORMED);
            }
        }
    });
}

TEST_CASE("revoke from pool",
          "[tx][settrustlineflags][allowtrust][liquiditypool]")
{
    VirtualClock clock;
    auto app = createTestApplication(clock, getTestConfig());

    // set up world
    auto root = TestAccount::createRoot(*app);

    auto& lm = app->getLedgerManager();
    auto txFee = lm.getLastTxFee();

    auto minBal = [&](int32_t n) { return lm.getLastMinBalance(n); };

    auto acc1 = root.create("acc1", minBal(10));
    auto native = makeNativeAsset();
    auto cur1 = makeAsset(root, "CUR1");
    auto cur2 = makeAsset(root, "CUR2");

    root.setOptions(setFlags(AUTH_REVOCABLE_FLAG));

    auto share12 =
        makeChangeTrustAssetPoolShare(cur1, cur2, LIQUIDITY_POOL_FEE_V18);
    auto pool12 = xdrSha256(share12.liquidityPool());

    auto shareNative1 =
        makeChangeTrustAssetPoolShare(native, cur1, LIQUIDITY_POOL_FEE_V18);
    auto poolNative1 = xdrSha256(shareNative1.liquidityPool());

    for_versions_from(18, *app, [&] {
        auto revokeTest = [&](TrustFlagOp flagOp) {
            auto bothNonNative = [&](bool testClawback) {
                auto share12 = makeChangeTrustAssetPoolShare(
                    cur1, cur2, LIQUIDITY_POOL_FEE_V18);
                auto pool12 = xdrSha256(share12.liquidityPool());

                if (testClawback)
                {
                    root.setOptions(setFlags(AUTH_CLAWBACK_ENABLED_FLAG));
                }

                acc1.changeTrust(cur1, 200);
                acc1.changeTrust(cur2, 50);
                root.pay(acc1, cur1, 200);
                root.pay(acc1, cur2, 50);

                acc1.changeTrust(share12, 100);

                acc1.liquidityPoolDeposit(pool12, 200, 50, Price{4, 1},
                                          Price{4, 1});

                checkLiquidityPool(*app, pool12, 200, 50, 100, 1);

                SECTION("pool is deleted")
                {
                    root.denyTrust(cur1, acc1, flagOp);

                    // get balance id here so the right root seqnum is used
                    auto cur1BalanceID =
                        getRevokeBalanceID(root, cur1, pool12, 0);
                    auto cur2BalanceID =
                        getRevokeBalanceID(root, cur2, pool12, 0);

                    auto cur1WrongEnvelopeTypeBalanceID = getRevokeBalanceID(
                        root, cur1, pool12, 0, ENVELOPE_TYPE_OP_ID);

                    root.allowTrust(cur1, acc1);

                    REQUIRE(!acc1.hasTrustLine(
                        changeTrustAssetToTrustLineAsset(share12)));

                    // Pool should be deleted since the last pool share
                    // trustline was deleted
                    {
                        LedgerTxn ltx(app->getLedgerTxnRoot());
                        REQUIRE(!loadLiquidityPool(ltx, pool12));
                    }

                    auto cur1Tl = acc1.loadTrustLine(cur1);
                    REQUIRE(getTrustLineEntryExtensionV2(cur1Tl)
                                .liquidityPoolUseCount == 0);

                    auto cur2Tl = acc1.loadTrustLine(cur2);
                    REQUIRE(getTrustLineEntryExtensionV2(cur2Tl)
                                .liquidityPoolUseCount == 0);

                    REQUIRE(acc1.getTrustlineBalance(cur1) == 0);
                    if (testClawback)
                    {
                        root.clawbackClaimableBalance(cur1BalanceID);
                    }
                    else
                    {
                        // try the incorrect balance ID's
                        REQUIRE_THROWS_AS(
                            acc1.claimClaimableBalance(
                                cur1WrongEnvelopeTypeBalanceID),
                            ex_CLAIM_CLAIMABLE_BALANCE_DOES_NOT_EXIST);

                        REQUIRE_THROWS_AS(
                            root.clawbackClaimableBalance(cur1BalanceID),
                            ex_CLAWBACK_CLAIMABLE_BALANCE_NOT_CLAWBACK_ENABLED);

                        acc1.claimClaimableBalance(cur1BalanceID);
                        REQUIRE(acc1.getTrustlineBalance(cur1) == 200);
                    }

                    REQUIRE(acc1.getTrustlineBalance(cur2) == 0);
                    if (testClawback)
                    {
                        root.clawbackClaimableBalance(cur2BalanceID);
                    }
                    else
                    {
                        REQUIRE_THROWS_AS(
                            root.clawbackClaimableBalance(cur2BalanceID),
                            ex_CLAWBACK_CLAIMABLE_BALANCE_NOT_CLAWBACK_ENABLED);

                        acc1.claimClaimableBalance(cur2BalanceID);
                        REQUIRE(acc1.getTrustlineBalance(cur2) == 50);
                    }
                }

                SECTION("pool still exists")
                {
                    auto acc2 = root.create("acc2", minBal(10));
                    acc2.changeTrust(cur1, 200);
                    acc2.changeTrust(cur2, 50);
                    root.pay(acc2, cur1, 200);
                    root.pay(acc2, cur2, 50);
                    acc2.changeTrust(share12, 100);

                    acc2.liquidityPoolDeposit(pool12, 200, 50, Price{4, 1},
                                              Price{4, 1});

                    REQUIRE(acc1.getTrustlineBalance(cur1) == 0);
                    REQUIRE(acc1.getTrustlineBalance(cur2) == 0);
                    REQUIRE(acc1.getTrustlineBalance(pool12) == 100);
                    checkLiquidityPool(*app, pool12, 400, 100, 200, 2);

                    root.denyTrust(cur1, acc1, flagOp);

                    // get balance id here so the right root seqnum is used
                    auto cur1BalanceID =
                        getRevokeBalanceID(root, cur1, pool12, 0);
                    auto cur2BalanceID =
                        getRevokeBalanceID(root, cur2, pool12, 0);

                    root.allowTrust(cur1, acc1);

                    REQUIRE(!acc1.hasTrustLine(
                        changeTrustAssetToTrustLineAsset(share12)));

                    checkLiquidityPool(*app, pool12, 200, 50, 100, 1);

                    auto cur1Tl = acc1.loadTrustLine(cur1);
                    REQUIRE(getTrustLineEntryExtensionV2(cur1Tl)
                                .liquidityPoolUseCount == 0);

                    auto cur2Tl = acc1.loadTrustLine(cur2);
                    REQUIRE(getTrustLineEntryExtensionV2(cur2Tl)
                                .liquidityPoolUseCount == 0);

                    REQUIRE(acc1.getTrustlineBalance(cur1) == 0);
                    if (testClawback)
                    {
                        root.clawbackClaimableBalance(cur1BalanceID);
                    }
                    else
                    {
                        REQUIRE_THROWS_AS(
                            root.clawbackClaimableBalance(cur1BalanceID),
                            ex_CLAWBACK_CLAIMABLE_BALANCE_NOT_CLAWBACK_ENABLED);

                        acc1.claimClaimableBalance(cur1BalanceID);
                        REQUIRE(acc1.getTrustlineBalance(cur1) == 200);
                    }

                    REQUIRE(acc1.getTrustlineBalance(cur2) == 0);
                    if (testClawback)
                    {
                        root.clawbackClaimableBalance(cur2BalanceID);
                    }
                    else
                    {
                        REQUIRE_THROWS_AS(
                            root.clawbackClaimableBalance(cur2BalanceID),
                            ex_CLAWBACK_CLAIMABLE_BALANCE_NOT_CLAWBACK_ENABLED);

                        acc1.claimClaimableBalance(cur2BalanceID);
                        REQUIRE(acc1.getTrustlineBalance(cur2) == 50);
                    }
                }
            };

            SECTION("both non-native claim")
            {
                bothNonNative(false);
            }

            SECTION("both non-native clawback")
            {
                bothNonNative(true);
            }

            SECTION("one non-native")
            {
                acc1.changeTrust(cur1, 200);
                root.pay(acc1, cur1, 200);

                acc1.changeTrust(shareNative1, 100);

                acc1.liquidityPoolDeposit(poolNative1, 50, 200, Price{1, 4},
                                          Price{1, 4});

                checkLiquidityPool(*app, poolNative1, 50, 200, 100, 1);

                SECTION("pool is deleted")
                {
                    root.denyTrust(cur1, acc1, flagOp);

                    // get balance id here so the right root seqnum is used
                    auto cur1BalanceID =
                        getRevokeBalanceID(root, cur1, poolNative1, 0);
                    auto nativeBalanceID =
                        getRevokeBalanceID(root, native, poolNative1, 0);

                    root.allowTrust(cur1, acc1);

                    REQUIRE(!acc1.hasTrustLine(
                        changeTrustAssetToTrustLineAsset(shareNative1)));

                    // Pool should be deleted since the last pool share
                    // trustline was deleted
                    {
                        LedgerTxn ltx(app->getLedgerTxnRoot());
                        REQUIRE(!loadLiquidityPool(ltx, poolNative1));
                    }

                    auto cur1Tl = acc1.loadTrustLine(cur1);
                    REQUIRE(getTrustLineEntryExtensionV2(cur1Tl)
                                .liquidityPoolUseCount == 0);

                    REQUIRE(acc1.getTrustlineBalance(cur1) == 0);
                    acc1.claimClaimableBalance(cur1BalanceID);
                    REQUIRE(acc1.getTrustlineBalance(cur1) == 200);

                    int64_t balance = acc1.getBalance();
                    acc1.claimClaimableBalance(nativeBalanceID);
                    REQUIRE(acc1.getBalance() == balance - 100 + 50);
                }

                SECTION("pool still exists")
                {
                    auto acc2 = root.create("acc2", minBal(10));
                    acc2.changeTrust(cur1, 200);
                    root.pay(acc2, cur1, 200);
                    acc2.changeTrust(shareNative1, 100);

                    acc2.liquidityPoolDeposit(poolNative1, 50, 200, Price{1, 4},
                                              Price{1, 4});

                    REQUIRE(acc1.getTrustlineBalance(cur1) == 0);
                    REQUIRE(acc1.getTrustlineBalance(poolNative1) == 100);
                    checkLiquidityPool(*app, poolNative1, 100, 400, 200, 2);

                    root.denyTrust(cur1, acc1, flagOp);

                    // get balance id here so the right root seqnum is used
                    auto cur1BalanceID =
                        getRevokeBalanceID(root, cur1, poolNative1, 0);
                    auto nativeBalanceID =
                        getRevokeBalanceID(root, native, poolNative1, 0);

                    root.allowTrust(cur1, acc1);

                    REQUIRE(!acc1.hasTrustLine(
                        changeTrustAssetToTrustLineAsset(shareNative1)));

                    checkLiquidityPool(*app, poolNative1, 50, 200, 100, 1);

                    auto cur1Tl = acc1.loadTrustLine(cur1);
                    REQUIRE(getTrustLineEntryExtensionV2(cur1Tl)
                                .liquidityPoolUseCount == 0);

                    REQUIRE(acc1.getTrustlineBalance(cur1) == 0);
                    acc1.claimClaimableBalance(cur1BalanceID);
                    REQUIRE(acc1.getTrustlineBalance(cur1) == 200);

                    int64_t balance = acc1.getBalance();
                    acc1.claimClaimableBalance(nativeBalanceID);
                    REQUIRE(acc1.getBalance() == balance - 100 + 50);
                }
            }

            SECTION("validate op num and tx account is used in hash")
            {
                acc1.changeTrust(cur1, 200);
                root.pay(acc1, cur1, 200);

                acc1.changeTrust(shareNative1, 100);

                acc1.liquidityPoolDeposit(poolNative1, 50, 200, Price{1, 4},
                                          Price{1, 4});

                checkLiquidityPool(*app, poolNative1, 50, 200, 100, 1);

                // Allow acc1 to submit an op from root
                auto sk1 = makeSigner(acc1, 100);
                root.setOptions(setSigner(sk1));

                auto revokeOp =
                    flagOp == TrustFlagOp::ALLOW_TRUST
                        ? root.op(allowTrust(acc1, cur1, 0))
                        : root.op(setTrustLineFlags(
                              acc1, cur1,
                              clearTrustLineFlags(AUTHORIZED_FLAG)));
                applyCheck(acc1.tx({root.op(payment(acc1, 1)), revokeOp}),
                           *app);

                // acc1 is the txSource account, which is what should be used
                // for the balanceID
                auto rootNativeBalanceID =
                    getRevokeBalanceID(root, native, poolNative1, 1);
                auto acc1NativeWrongIndexBalanceID =
                    getRevokeBalanceID(acc1, native, poolNative1, 0);
                auto acc1Cur2BalanceID =
                    getRevokeBalanceID(acc1, cur2, poolNative1, 1);
                auto acc1NativeBalanceID =
                    getRevokeBalanceID(acc1, native, poolNative1, 1);

                REQUIRE_THROWS_AS(
                    acc1.claimClaimableBalance(rootNativeBalanceID),
                    ex_CLAIM_CLAIMABLE_BALANCE_DOES_NOT_EXIST);
                REQUIRE_THROWS_AS(
                    acc1.claimClaimableBalance(acc1NativeWrongIndexBalanceID),
                    ex_CLAIM_CLAIMABLE_BALANCE_DOES_NOT_EXIST);
                REQUIRE_THROWS_AS(acc1.claimClaimableBalance(acc1Cur2BalanceID),
                                  ex_CLAIM_CLAIMABLE_BALANCE_DOES_NOT_EXIST);

                acc1.claimClaimableBalance(acc1NativeBalanceID);
            }

            SECTION("claimable balance created for issuer")
            {
                auto acc1Usd = makeAsset(acc1, "USD1");
                auto share1Usd = makeChangeTrustAssetPoolShare(
                    cur1, acc1Usd, LIQUIDITY_POOL_FEE_V18);
                auto pool1Usd = xdrSha256(share1Usd.liquidityPool());

                acc1.changeTrust(cur1, 10);
                root.pay(acc1, cur1, 10);

                acc1.changeTrust(share1Usd, 10);

                acc1.liquidityPoolDeposit(pool1Usd, 10, 10, Price{1, 1},
                                          Price{1, 1});

                checkLiquidityPool(*app, pool1Usd, 10, 10, 10, 1);

                root.denyTrust(cur1, acc1, flagOp);

                auto cur1BalanceID =
                    getRevokeBalanceID(root, cur1, pool1Usd, 0);
                auto usdBalanceID =
                    getRevokeBalanceID(root, acc1Usd, pool1Usd, 0);

                root.allowTrust(cur1, acc1);

                REQUIRE(!acc1.hasTrustLine(
                    changeTrustAssetToTrustLineAsset(share1Usd)));

                // Pool should be deleted since the last pool share
                // trustline was deleted
                {
                    LedgerTxn ltx(app->getLedgerTxnRoot());
                    REQUIRE(!loadLiquidityPool(ltx, pool1Usd));
                }

                auto cur1Tl = acc1.loadTrustLine(cur1);
                REQUIRE(getTrustLineEntryExtensionV2(cur1Tl)
                            .liquidityPoolUseCount == 0);

                REQUIRE(acc1.getTrustlineBalance(cur1) == 0);
                acc1.claimClaimableBalance(cur1BalanceID);
                REQUIRE(acc1.getTrustlineBalance(cur1) == 10);

                // A claimable balance was not created for usd because acc1 is
                // the issuer
                REQUIRE_THROWS_AS(acc1.claimClaimableBalance(usdBalanceID),
                                  ex_CLAIM_CLAIMABLE_BALANCE_DOES_NOT_EXIST);
            }

            SECTION("revoke from 0 balance pool share trustline")
            {
                root.setOptions(setFlags(AUTH_REVOCABLE_FLAG));
                acc1.changeTrust(cur1, 1);
                acc1.changeTrust(cur2, 1);
                acc1.changeTrust(share12, 1);

                checkLiquidityPool(*app, pool12, 0, 0, 0, 1);

                root.denyTrust(cur1, acc1, flagOp);

                // no claimable balance should've been created
                auto cur1BalanceID = getRevokeBalanceID(root, cur1, pool12, 0);
                auto cur2BalanceID = getRevokeBalanceID(root, cur2, pool12, 0);

                REQUIRE_THROWS_AS(acc1.claimClaimableBalance(cur1BalanceID),
                                  ex_CLAIM_CLAIMABLE_BALANCE_DOES_NOT_EXIST);

                REQUIRE_THROWS_AS(acc1.claimClaimableBalance(cur1BalanceID),
                                  ex_CLAIM_CLAIMABLE_BALANCE_DOES_NOT_EXIST);

                REQUIRE(!acc1.hasTrustLine(
                    changeTrustAssetToTrustLineAsset(share12)));

                auto cur1Tl = acc1.loadTrustLine(cur1);
                REQUIRE(getTrustLineEntryExtensionV2(cur1Tl)
                            .liquidityPoolUseCount == 0);

                auto cur2Tl = acc1.loadTrustLine(cur2);
                REQUIRE(getTrustLineEntryExtensionV2(cur2Tl)
                            .liquidityPoolUseCount == 0);

                {
                    LedgerTxn ltx(app->getLedgerTxnRoot());
                    REQUIRE(!loadLiquidityPool(ltx, pool12));

                    // make sure this account isn't sponsoring any claimable
                    // balances
                    auto account = loadAccount(ltx, acc1.getPublicKey(), true);
                    REQUIRE(!hasAccountEntryExtV2(
                        account.current().data.account()));
                }
            }

            SECTION("revoke from multiple pools")
            {
                auto setupAndDeposit = [&](TestAccount& acc,
                                           Asset const& assetA,
                                           Asset const& assetB) {
                    acc.changeTrust(assetA, 100);
                    acc.changeTrust(assetB, 100);
                    root.pay(acc, assetA, 10);
                    root.pay(acc, assetB, 10);

                    auto psAsset = makeChangeTrustAssetPoolShare(
                        assetA, assetB, LIQUIDITY_POOL_FEE_V18);

                    acc.changeTrust(psAsset, 100);

                    auto poolID = xdrSha256(psAsset.liquidityPool());
                    acc.liquidityPoolDeposit(poolID, 10, 10, Price{1, 1},
                                             Price{1, 1});
                };

                auto usd = makeAsset(root, "usd");
                auto btc = makeAsset(root, "btc");
                auto eur = makeAsset(root, "eur");

                auto shareBtcUsd = makeChangeTrustAssetPoolShare(
                    btc, usd, LIQUIDITY_POOL_FEE_V18);
                auto shareEurUsd = makeChangeTrustAssetPoolShare(
                    eur, usd, LIQUIDITY_POOL_FEE_V18);
                auto shareBtcEur = makeChangeTrustAssetPoolShare(
                    btc, eur, LIQUIDITY_POOL_FEE_V18);

                auto poolBtcUsd = xdrSha256(shareBtcUsd.liquidityPool());
                auto poolEurUsd = xdrSha256(shareEurUsd.liquidityPool());
                auto poolBtcEur = xdrSha256(shareBtcEur.liquidityPool());

                for (int i = 0; i < 3; ++i)
                {
                    auto acc =
                        root.create(fmt::format("account{}", i), minBal(10));
                    setupAndDeposit(acc, btc, usd);
                    setupAndDeposit(acc, eur, usd);
                    setupAndDeposit(acc, btc, eur);

                    // revoke last account
                    if (i == 2)
                    {
                        checkLiquidityPool(*app, poolBtcUsd, 30, 30, 30, 3);
                        checkLiquidityPool(*app, poolEurUsd, 30, 30, 30, 3);
                        checkLiquidityPool(*app, poolBtcEur, 30, 30, 30, 3);

                        root.denyTrust(btc, acc, flagOp);

                        auto btc1BalanceID =
                            getRevokeBalanceID(root, btc, poolBtcUsd, 0);
                        auto btc2BalanceID =
                            getRevokeBalanceID(root, btc, poolBtcEur, 0);
                        auto usdBalanceID =
                            getRevokeBalanceID(root, usd, poolBtcUsd, 0);
                        auto eurBalanceID =
                            getRevokeBalanceID(root, eur, poolBtcEur, 0);

                        root.allowTrust(btc, acc);

                        REQUIRE(acc.getTrustlineBalance(btc) == 0);
                        REQUIRE(acc.getTrustlineBalance(usd) == 0);
                        REQUIRE(acc.getTrustlineBalance(eur) == 0);

                        acc.claimClaimableBalance(btc1BalanceID);
                        acc.claimClaimableBalance(btc2BalanceID);
                        acc.claimClaimableBalance(usdBalanceID);
                        acc.claimClaimableBalance(eurBalanceID);

                        REQUIRE(acc.getTrustlineBalance(btc) == 20);
                        REQUIRE(acc.getTrustlineBalance(usd) == 10);
                        REQUIRE(acc.getTrustlineBalance(eur) == 10);

                        auto btcTl = acc.loadTrustLine(btc);
                        auto usdTl = acc.loadTrustLine(usd);
                        auto eurTl = acc.loadTrustLine(eur);

                        REQUIRE(getTrustLineEntryExtensionV2(btcTl)
                                    .liquidityPoolUseCount == 0);
                        REQUIRE(getTrustLineEntryExtensionV2(usdTl)
                                    .liquidityPoolUseCount == 1);
                        REQUIRE(getTrustLineEntryExtensionV2(eurTl)
                                    .liquidityPoolUseCount == 1);
                    }
                }

                checkLiquidityPool(*app, poolBtcUsd, 20, 20, 20, 2);
                checkLiquidityPool(*app, poolEurUsd, 30, 30, 30, 3);
                checkLiquidityPool(*app, poolBtcEur, 20, 20, 20, 2);
            }

            SECTION("sponsorships")
            {
                auto acc2 = root.create("acc2", lm.getLastMinBalance(3));
                auto acc3 = root.create("acc3", lm.getLastMinBalance(3));

                auto depositIntoPool = [&](bool poolShareTrustlineIsSponsored) {
                    acc1.changeTrust(cur1, 10);
                    acc1.changeTrust(cur2, 10);
                    root.pay(acc1, cur1, 10);
                    root.pay(acc1, cur2, 10);

                    if (poolShareTrustlineIsSponsored)
                    {
                        auto tx = transactionFrameFromOps(
                            app->getNetworkID(), acc3,
                            {acc3.op(beginSponsoringFutureReserves(acc1)),
                             acc1.op(changeTrust(share12, 10)),
                             acc1.op(endSponsoringFutureReserves())},
                            {acc1});

                        LedgerTxn ltx(app->getLedgerTxnRoot());
                        TransactionMeta txm(2);
                        REQUIRE(tx->checkValid(ltx, 0, 0, 0));
                        REQUIRE(tx->apply(*app, ltx, txm));
                        REQUIRE(tx->getResultCode() == txSUCCESS);
                        ltx.commit();

                        acc3.pay(root, acc3.getAvailableBalance() - txFee);
                    }
                    else
                    {
                        acc1.changeTrust(share12, 10);
                    }

                    acc1.liquidityPoolDeposit(pool12, 10, 10, Price{1, 1},
                                              Price{1, 1});

                    checkLiquidityPool(*app, pool12, 10, 10, 10, 1);

                    // get rid of rest of available native balance
                    acc1.pay(root, acc1.getAvailableBalance() - txFee);
                };

                auto claimAndValidatePoolCounters =
                    [&](TestAccount& txSourceAcc, uint32_t opIndex) {
                        auto cur1BalanceID = getRevokeBalanceID(
                            txSourceAcc, cur1, pool12, opIndex);
                        auto cur2BalanceID = getRevokeBalanceID(
                            txSourceAcc, cur2, pool12, opIndex);

                        // pay acc1 so it can pay the fee to claim the balances
                        root.pay(acc1, lm.getLastMinBalance(2));

                        root.allowTrust(cur1, acc1);
                        acc1.claimClaimableBalance(cur1BalanceID);
                        acc1.claimClaimableBalance(cur2BalanceID);

                        auto cur1Tl = acc1.loadTrustLine(cur1);
                        REQUIRE(getTrustLineEntryExtensionV2(cur1Tl)
                                    .liquidityPoolUseCount == 0);

                        auto cur2Tl = acc1.loadTrustLine(cur2);
                        REQUIRE(getTrustLineEntryExtensionV2(cur2Tl)
                                    .liquidityPoolUseCount == 0);

                        REQUIRE(!acc1.hasTrustLine(
                            changeTrustAssetToTrustLineAsset(share12)));

                        {
                            LedgerTxn ltx(app->getLedgerTxnRoot());
                            REQUIRE(!loadLiquidityPool(ltx, pool12));
                        }
                    };

                auto submitRevokeInSandwich = [&](TestAccount& sponsoringAcc,
                                                  TestAccount& sponsoredAcc,
                                                  bool success) {
                    auto op =
                        flagOp == TrustFlagOp::ALLOW_TRUST
                            ? allowTrust(acc1, cur1, 0)
                            : setTrustLineFlags(
                                  acc1, cur1,
                                  clearTrustLineFlags(TRUSTLINE_AUTH_FLAGS));

                    auto tx = transactionFrameFromOps(
                        app->getNetworkID(), sponsoringAcc,
                        {sponsoringAcc.op(
                             beginSponsoringFutureReserves(sponsoredAcc)),
                         root.op(op),
                         sponsoredAcc.op(endSponsoringFutureReserves())},
                        {sponsoredAcc, root});

                    {
                        LedgerTxn ltx(app->getLedgerTxnRoot());
                        TransactionMeta txm(2);
                        REQUIRE(tx->checkValid(ltx, 0, 0, 0));
                        REQUIRE(tx->apply(*app, ltx, txm) == success);
                        ltx.commit();
                    }

                    if (success)
                    {
                        REQUIRE(tx->getResultCode() == txSUCCESS);
                    }
                    else
                    {
                        REQUIRE(tx->getResultCode() == txFAILED);
                        if (flagOp == TrustFlagOp::ALLOW_TRUST)
                        {
                            REQUIRE(tx->getResult()
                                        .result.results()[1]
                                        .tr()
                                        .allowTrustResult()
                                        .code() == ALLOW_TRUST_LOW_RESERVE);
                        }
                        else
                        {
                            REQUIRE(tx->getResult()
                                        .result.results()[1]
                                        .tr()
                                        .setTrustLineFlagsResult()
                                        .code() ==
                                    SET_TRUST_LINE_FLAGS_LOW_RESERVE);
                        }
                    }
                };

                auto increaseReserve = [&]() {
                    // double the reserve
                    auto newReserve = lm.getLastReserve() * 2;
                    REQUIRE(
                        executeUpgrade(*app, makeBaseReserveUpgrade(newReserve))
                            .baseReserve == newReserve);
                };

                // same reserves
                SECTION("same reserve - no sandwich on revoke")
                {
                    depositIntoPool(false);

                    root.denyTrust(cur1, acc1, flagOp);
                    claimAndValidatePoolCounters(root, 0);
                }
                SECTION("same reserve - sponsored pool share trustline - no "
                        "sandwich on revoke")
                {
                    depositIntoPool(true);

                    root.denyTrust(cur1, acc1, flagOp);
                    claimAndValidatePoolCounters(root, 0);
                }
                SECTION("same reserve - sandwich on revoke - success")
                {
                    depositIntoPool(false);
                    submitRevokeInSandwich(acc2, acc1, true);
                    claimAndValidatePoolCounters(acc2, 1);
                }

                SECTION("same reserve - sandwich on revoke - fail")
                {
                    depositIntoPool(false);

                    // leave enough to pay for this tx and the sponsorship
                    // sandwich
                    acc2.pay(root, acc2.getAvailableBalance() - txFee * 4);
                    submitRevokeInSandwich(acc2, acc1, false);
                }
                SECTION("same reserve - sponsoring account is the sponsor of "
                        "the pool share trustline")
                {
                    depositIntoPool(true);

                    // acc3 is the sponsor of the pool share trustline
                    root.pay(acc3, lm.getLastMinBalance(1));
                    submitRevokeInSandwich(acc3, acc1, true);
                }

                // upgrade reserves
                SECTION("increase reserve - no sandwich on revoke - success")
                {
                    depositIntoPool(false);
                    increaseReserve();

                    root.denyTrust(cur1, acc1, flagOp);
                    claimAndValidatePoolCounters(root, 0);
                }
                SECTION("increase reserve - sandwich on revoke - success")
                {
                    depositIntoPool(false);
                    increaseReserve();

                    root.pay(acc2, lm.getLastMinBalance(1));
                    submitRevokeInSandwich(acc2, acc1, true);
                }
                SECTION("increase reserve - sandwich on revoke - fail")
                {
                    depositIntoPool(false);
                    increaseReserve();

                    submitRevokeInSandwich(acc2, acc1, false);
                }
                SECTION("increase reserve - sponsored pool share trustline - "
                        "sandwich on revoke - fail")
                {
                    depositIntoPool(true);
                    increaseReserve();

                    submitRevokeInSandwich(acc2, acc3, false);
                }
                SECTION("increase reserve - sponsoring account is the sponsor "
                        "of the pool share trustline")
                {
                    depositIntoPool(true);
                    increaseReserve();

                    // acc3 is the sponsor of the pool share trustline
                    root.pay(acc3, lm.getLastMinBalance(1));
                    submitRevokeInSandwich(acc3, acc1, true);
                }
            }
        };

        SECTION("revoke with set trustline flags")
        {
            revokeTest(TrustFlagOp::SET_TRUST_LINE_FLAGS);
        }
        SECTION("revoke with allow trust")
        {
            revokeTest(TrustFlagOp::ALLOW_TRUST);
        }
    });

    SECTION("too many sponsoring")
    {
        SECTION("one claimable balance")
        {
            acc1.changeTrust(cur1, 10);
            acc1.changeTrust(cur2, 10);
            root.pay(acc1, cur1, 10);
            root.pay(acc1, cur2, 10);

            // use two assets issued by acc1. No claimable balances will be
            // created for them on revoke
            auto eur = makeAsset(acc1, "eur");
            auto usd = makeAsset(acc1, "usd");

            auto share1Eur = makeChangeTrustAssetPoolShare(
                cur1, eur, LIQUIDITY_POOL_FEE_V18);
            auto pool1Eur = xdrSha256(share1Eur.liquidityPool());

            auto share2Usd = makeChangeTrustAssetPoolShare(
                cur2, usd, LIQUIDITY_POOL_FEE_V18);
            auto pool2Usd = xdrSha256(share2Usd.liquidityPool());

            acc1.changeTrust(share1Eur, 10);
            acc1.changeTrust(share2Usd, 10);
            acc1.liquidityPoolDeposit(pool1Eur, 10, 10, Price{1, 1},
                                      Price{1, 1});
            acc1.liquidityPoolDeposit(pool2Usd, 10, 10, Price{1, 1},
                                      Price{1, 1});

            SECTION("allow trust")
            {
                tooManySponsoring(*app, acc1,
                                  root.op(allowTrust(acc1, cur1, 0)),
                                  root.op(allowTrust(acc1, cur2, 0)), 1);
            }
            SECTION("set trustline flags")
            {
                tooManySponsoring(
                    *app, acc1,
                    root.op(setTrustLineFlags(
                        acc1, cur1, clearTrustLineFlags(AUTHORIZED_FLAG))),
                    root.op(setTrustLineFlags(
                        acc1, cur2, clearTrustLineFlags(AUTHORIZED_FLAG))),
                    1);
            }
        }

        SECTION("two claimable balances")
        {
            auto cur3 = makeAsset(root, "CUR3");

            auto share23 = makeChangeTrustAssetPoolShare(
                cur2, cur3, LIQUIDITY_POOL_FEE_V18);
            auto pool23 = xdrSha256(share23.liquidityPool());

            acc1.changeTrust(cur1, 10);
            acc1.changeTrust(cur2, 20);
            acc1.changeTrust(cur3, 10);
            root.pay(acc1, cur1, 10);
            root.pay(acc1, cur2, 20);
            root.pay(acc1, cur3, 10);

            acc1.changeTrust(share12, 10);
            acc1.changeTrust(share23, 10);

            acc1.liquidityPoolDeposit(pool12, 10, 10, Price{1, 1}, Price{1, 1});
            acc1.liquidityPoolDeposit(pool23, 10, 10, Price{1, 1}, Price{1, 1});

            SECTION("allow trust")
            {
                tooManySponsoring(*app, acc1,
                                  root.op(allowTrust(acc1, cur1, 0)),
                                  root.op(allowTrust(acc1, cur2, 0)), 2);
            }
            SECTION("set trustline flags")
            {
                tooManySponsoring(
                    *app, acc1,
                    root.op(setTrustLineFlags(
                        acc1, cur1, clearTrustLineFlags(AUTHORIZED_FLAG))),
                    root.op(setTrustLineFlags(
                        acc1, cur2, clearTrustLineFlags(AUTHORIZED_FLAG))),
                    2);
            }
        }
    }
}