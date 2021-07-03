// Copyright 2016 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "ledger/LedgerTxn.h"
#include "ledger/LedgerTxnEntry.h"
#include "ledger/LedgerTxnHeader.h"
#include "ledger/TrustLineWrapper.h"
#include "lib/catch.hpp"
#include "lib/json/json.h"
#include "main/Application.h"
#include "test/TestAccount.h"
#include "test/TestExceptions.h"
#include "test/TestMarket.h"
#include "test/TestUtils.h"
#include "test/TxTests.h"
#include "test/test.h"
#include "transactions/TransactionUtils.h"
#include "transactions/test/SponsorshipTestUtils.h"

using namespace stellar;
using namespace stellar::txtest;

TEST_CASE("change trust", "[tx][changetrust]")
{
    Config const& cfg = getTestConfig();

    VirtualClock clock;
    auto app = createTestApplication(clock, cfg);

    app->start();

    // set up world
    auto root = TestAccount::createRoot(*app);
    auto const minBalance2 = app->getLedgerManager().getLastMinBalance(2);
    auto gateway = root.create("gw", minBalance2);
    Asset idr = makeAsset(gateway, "IDR");

    SECTION("basic tests")
    {
        for_all_versions(*app, [&] {
            // create a trustline with a limit of 0
            REQUIRE_THROWS_AS(root.changeTrust(idr, 0),
                              ex_CHANGE_TRUST_INVALID_LIMIT);

            // create a trustline with a limit of 100
            root.changeTrust(idr, 100);

            // fill it to 90
            gateway.pay(root, idr, 90);

            // can't lower the limit below balance
            REQUIRE_THROWS_AS(root.changeTrust(idr, 89),
                              ex_CHANGE_TRUST_INVALID_LIMIT);

            // can't delete if there is a balance
            REQUIRE_THROWS_AS(root.changeTrust(idr, 0),
                              ex_CHANGE_TRUST_INVALID_LIMIT);

            // lower the limit at the balance
            root.changeTrust(idr, 90);

            // clear the balance
            root.pay(gateway, idr, 90);

            // delete the trust line
            root.changeTrust(idr, 0);
            REQUIRE(!root.hasTrustLine(idr));
        });
    }
    SECTION("issuer does not exist")
    {
        SECTION("new trust line")
        {
            for_all_versions(*app, [&] {
                Asset usd = makeAsset(getAccount("non-existing"), "IDR");
                REQUIRE_THROWS_AS(root.changeTrust(usd, 100),
                                  ex_CHANGE_TRUST_NO_ISSUER);
            });
        }
        SECTION("edit existing")
        {
            closeLedgerOn(*app, 2, 1, 1, 2016);
            for_all_versions(*app, [&] {
                root.changeTrust(idr, 100);
                // Merge gateway back into root (the trustline still exists)
                gateway.merge(root);

                REQUIRE_THROWS_AS(root.changeTrust(idr, 99),
                                  ex_CHANGE_TRUST_NO_ISSUER);
                {
                    LedgerTxn ltx(app->getLedgerTxnRoot());
                    REQUIRE(!stellar::loadAccount(ltx, gateway));
                }
            });
        }
    }
    SECTION("trusting self")
    {
        auto validateTrustLineIsConst = [&]() {
            LedgerTxn ltx(app->getLedgerTxnRoot());
            auto trustLine =
                stellar::loadTrustLine(ltx, gateway.getPublicKey(), idr);
            REQUIRE(trustLine);
            REQUIRE(trustLine.getBalance() == INT64_MAX);
        };

        auto loadAccount = [&](PublicKey const& k) {
            LedgerTxn ltx(app->getLedgerTxnRoot());
            auto le = stellar::loadAccount(ltx, k).current();
            return le.data.account();
        };

        auto validatePayingSelf = [&]() {
            auto gatewayAccountBefore = loadAccount(gateway);
            gateway.pay(gateway, idr, 50);
            validateTrustLineIsConst();
            auto gatewayAccountAfter = loadAccount(gateway);
            REQUIRE(gatewayAccountAfter.balance ==
                    (gatewayAccountBefore.balance -
                     app->getLedgerManager().getLastTxFee()));
        };

        validateTrustLineIsConst();

        for_versions_to(2, *app, [&] {
            // create a trustline with a limit of INT64_MAX - 1 will fail
            REQUIRE_THROWS_AS(gateway.changeTrust(idr, INT64_MAX - 1),
                              ex_CHANGE_TRUST_INVALID_LIMIT);
            validateTrustLineIsConst();

            // create a trustline with a limit of INT64_MAX
            gateway.changeTrust(idr, INT64_MAX);
            validateTrustLineIsConst();

            validatePayingSelf();

            // lower the limit will fail, because it is still INT64_MAX
            REQUIRE_THROWS_AS(gateway.changeTrust(idr, 50),
                              ex_CHANGE_TRUST_INVALID_LIMIT);
            validateTrustLineIsConst();

            // delete the trust line will fail
            REQUIRE_THROWS_AS(gateway.changeTrust(idr, 0),
                              ex_CHANGE_TRUST_INVALID_LIMIT);
            validateTrustLineIsConst();
        });

        for_versions(3, 15, *app, [&] {
            REQUIRE_THROWS_AS(gateway.changeTrust(idr, INT64_MAX - 1),
                              ex_CHANGE_TRUST_SELF_NOT_ALLOWED);
            validateTrustLineIsConst();

            REQUIRE_THROWS_AS(gateway.changeTrust(idr, INT64_MAX),
                              ex_CHANGE_TRUST_SELF_NOT_ALLOWED);
            validateTrustLineIsConst();

            validatePayingSelf();

            // lower the limit will fail, because it is still INT64_MAX
            REQUIRE_THROWS_AS(gateway.changeTrust(idr, 50),
                              ex_CHANGE_TRUST_SELF_NOT_ALLOWED);
            validateTrustLineIsConst();

            // delete the trust line will fail
            REQUIRE_THROWS_AS(gateway.changeTrust(idr, 0),
                              ex_CHANGE_TRUST_SELF_NOT_ALLOWED);
            validateTrustLineIsConst();
        });

        for_versions_from(16, *app, [&] {
            REQUIRE_THROWS_AS(gateway.changeTrust(idr, 50),
                              ex_CHANGE_TRUST_MALFORMED);
            validateTrustLineIsConst();

            validatePayingSelf();
        });
    }

    SECTION("trustline on native asset")
    {
        const auto nativeAsset = makeNativeAsset();
        for_versions_to(9, *app, [&] {
            REQUIRE_THROWS_AS(gateway.changeTrust(nativeAsset, INT64_MAX - 1),
                              ex_txINTERNAL_ERROR);
        });

        for_versions_from(10, *app, [&] {
            REQUIRE_THROWS_AS(gateway.changeTrust(nativeAsset, INT64_MAX - 1),
                              ex_CHANGE_TRUST_MALFORMED);
        });
    }

    SECTION("create trust line with native selling liabilities")
    {
        auto const minBal2 = app->getLedgerManager().getLastMinBalance(2);
        auto txfee = app->getLedgerManager().getLastTxFee();
        auto const native = makeNativeAsset();
        auto acc1 = root.create("acc1", minBal2 + 2 * txfee + 500 - 1);
        TestMarket market(*app);

        auto cur1 = acc1.asset("CUR1");
        market.requireChangesWithOffer({}, [&] {
            return market.addOffer(acc1, {native, cur1, Price{1, 1}, 500});
        });

        for_versions_to(9, *app, [&] { acc1.changeTrust(idr, 1000); });
        for_versions_from(10, *app, [&] {
            REQUIRE_THROWS_AS(acc1.changeTrust(idr, 1000),
                              ex_CHANGE_TRUST_LOW_RESERVE);
            root.pay(acc1, txfee + 1);
            acc1.changeTrust(idr, 1000);
        });
    }

    SECTION("create trust line with native buying liabilities")
    {
        auto const minBal2 = app->getLedgerManager().getLastMinBalance(2);
        auto txfee = app->getLedgerManager().getLastTxFee();
        auto const native = makeNativeAsset();
        auto acc1 = root.create("acc1", minBal2 + 2 * txfee + 500 - 1);
        TestMarket market(*app);

        auto cur1 = acc1.asset("CUR1");
        market.requireChangesWithOffer({}, [&] {
            return market.addOffer(acc1, {cur1, native, Price{1, 1}, 500});
        });

        for_all_versions(*app, [&] { acc1.changeTrust(idr, 1000); });
    }

    SECTION("cannot reduce limit below buying liabilities or delete")
    {
        for_versions_from(10, *app, [&] {
            auto txfee = app->getLedgerManager().getLastTxFee();
            auto const native = makeNativeAsset();
            auto acc1 = root.create("acc1", minBalance2 + 10 * txfee);
            TestMarket market(*app);

            acc1.changeTrust(idr, 1000);
            market.requireChangesWithOffer({}, [&] {
                return market.addOffer(acc1, {native, idr, Price{1, 1}, 500});
            });
            acc1.changeTrust(idr, 500);
            REQUIRE_THROWS_AS(acc1.changeTrust(idr, 499),
                              ex_CHANGE_TRUST_INVALID_LIMIT);
            REQUIRE_THROWS_AS(acc1.changeTrust(idr, 0),
                              ex_CHANGE_TRUST_INVALID_LIMIT);
        });
    }

    SECTION("sponsorship")
    {
        auto const minBalance0 = app->getLedgerManager().getLastMinBalance(0);
        auto const minBalance1 = app->getLedgerManager().getLastMinBalance(1);
        auto acc1 = root.create("a1", minBalance1 - 1);
        auto acc2 = root.create("a2", minBalance0);
        createSponsoredEntryButSponsorHasInsufficientBalance(
            *app, acc1, acc2, changeTrust(idr, 1000),
            [](OperationResult const& opRes) {
                return opRes.tr().changeTrustResult().code() ==
                       CHANGE_TRUST_LOW_RESERVE;
            });

        createModifyAndRemoveSponsoredEntry(
            *app, acc2, changeTrust(idr, 1000), changeTrust(idr, 999),
            changeTrust(idr, 1001), changeTrust(idr, 0),
            trustlineKey(acc2, idr));
    }

    SECTION("pool trustline")
    {
        auto getNumSubEntries = [&](AccountID const& accountID) {
            LedgerTxn ltx(app->getLedgerTxnRoot());
            auto acc = stellar::loadAccount(ltx, accountID);
            return acc.current().data.account().numSubEntries;
        };

        auto poolShareTest = [&](Asset const& assetA, Asset const& assetB) {
            // trustlines will be deauthorized initially
            gateway.setOptions(setFlags(AUTH_REQUIRED_FLAG));

            auto poolShareAsset =
                makePoolShareAsset(assetA, assetB, LIQUIDITY_POOL_FEE_V18);

            // root is missing trustline(s)
            REQUIRE_THROWS_AS(root.changeTrust(poolShareAsset, 10),
                              ex_CHANGE_TRUST_TRUST_LINE_MISSING);

            if (assetA.type() != ASSET_TYPE_NATIVE)
            {
                root.changeTrust(assetA, 10);

                REQUIRE_THROWS_AS(
                    root.changeTrust(poolShareAsset, 10),
                    ex_CHANGE_TRUST_NOT_AUTH_MAINTAIN_LIABILITIES);
                gateway.allowMaintainLiabilities(assetA, root);
            }

            if (assetB.type() != ASSET_TYPE_NATIVE)
            {
                // root is still missing assetB trustline
                REQUIRE_THROWS_AS(root.changeTrust(poolShareAsset, 10),
                                  ex_CHANGE_TRUST_TRUST_LINE_MISSING);
                root.changeTrust(assetB, 10);

                // assetB trustline is not authorized to maintain liabilities
                REQUIRE_THROWS_AS(
                    root.changeTrust(poolShareAsset, 10),
                    ex_CHANGE_TRUST_NOT_AUTH_MAINTAIN_LIABILITIES);
                gateway.allowMaintainLiabilities(assetB, root);
            }

            // this should create a LiquidityPoolEntry, and modify
            // liquidityPoolUseCount on the asset trustlines
            auto prePoolNumSubEntries = getNumSubEntries(root);
            root.changeTrust(poolShareAsset, 10);

            REQUIRE(getNumSubEntries(root) - prePoolNumSubEntries == 2);

            auto poolShareTlAsset =
                changeTrustAssetToTrustLineAsset(poolShareAsset);

            // pool share trustline shouldn't have any flags set
            REQUIRE(root.loadTrustLine(poolShareTlAsset).flags == 0);

            if (assetA.type() != ASSET_TYPE_NATIVE)
            {
                auto assetATl = root.loadTrustLine(assetA);
                REQUIRE(getTrustLineEntryExtensionV2(assetATl)
                            .liquidityPoolUseCount == 1);
            }

            if (assetB.type() != ASSET_TYPE_NATIVE)
            {
                auto assetBTl = root.loadTrustLine(assetB);
                REQUIRE(getTrustLineEntryExtensionV2(assetBTl)
                            .liquidityPoolUseCount == 1);
            }

            {
                LedgerTxn ltx(app->getLedgerTxnRoot());
                auto pool =
                    loadLiquidityPool(ltx, poolShareTlAsset.liquidityPoolID());
                REQUIRE(pool);
                REQUIRE(pool.current()
                            .data.liquidityPool()
                            .body.constantProduct()
                            .poolSharesTrustLineCount == 1);
            }

            // can't delete asset trustlines while they are used in a pool
            if (assetA.type() != ASSET_TYPE_NATIVE)
            {
                REQUIRE_THROWS_AS(root.changeTrust(assetA, 0),
                                  ex_CHANGE_TRUST_CANNOT_DELETE);
            }
            if (assetB.type() != ASSET_TYPE_NATIVE)
            {
                REQUIRE_THROWS_AS(root.changeTrust(assetB, 0),
                                  ex_CHANGE_TRUST_CANNOT_DELETE);
            }

            // delete the pool sharetrust line
            root.changeTrust(poolShareAsset, 0);
            REQUIRE(!root.hasTrustLine(poolShareTlAsset));

            REQUIRE(getNumSubEntries(root) == prePoolNumSubEntries);

            if (assetA.type() != ASSET_TYPE_NATIVE)
            {
                auto assetATl = root.loadTrustLine(assetA);
                REQUIRE(getTrustLineEntryExtensionV2(assetATl)
                            .liquidityPoolUseCount == 0);
            }
            if (assetB.type() != ASSET_TYPE_NATIVE)
            {
                auto assetBTl = root.loadTrustLine(assetB);
                REQUIRE(getTrustLineEntryExtensionV2(assetBTl)
                            .liquidityPoolUseCount == 0);
            }

            {
                LedgerTxn ltx(app->getLedgerTxnRoot());
                auto pool =
                    loadLiquidityPool(ltx, poolShareTlAsset.liquidityPoolID());
                REQUIRE(!pool);
            }

            // now the asset trustlines can be deleted
            if (assetA.type() != ASSET_TYPE_NATIVE)
            {
                root.changeTrust(assetA, 0);
            }
            if (assetB.type() != ASSET_TYPE_NATIVE)
            {
                root.changeTrust(assetB, 0);
            }
        };

        auto usd = makeAsset(gateway, "USD");
        auto idrUsd = makePoolShareAsset(idr, usd, LIQUIDITY_POOL_FEE_V18);

        // Liquidity pools not supported pre V18
        for_versions_to(17, *app, [&] {
            REQUIRE_THROWS_AS(root.changeTrust(idrUsd, 10),
                              ex_CHANGE_TRUST_MALFORMED);
        });

        for_versions_from(18, *app, [&] {
            // CHANGE_TRUST_MALFORMED tests
            auto invalidFee =
                makePoolShareAsset(idr, usd, LIQUIDITY_POOL_FEE_V18 - 1);
            REQUIRE_THROWS_AS(root.changeTrust(invalidFee, 10),
                              ex_CHANGE_TRUST_MALFORMED);
            auto invalidOrder =
                makePoolShareAsset(usd, idr, LIQUIDITY_POOL_FEE_V18);
            REQUIRE_THROWS_AS(root.changeTrust(invalidOrder, 10),
                              ex_CHANGE_TRUST_MALFORMED);

            auto invalidOrderNative = makePoolShareAsset(
                idr, makeNativeAsset(), LIQUIDITY_POOL_FEE_V18);
            REQUIRE_THROWS_AS(root.changeTrust(invalidOrderNative, 10),
                              ex_CHANGE_TRUST_MALFORMED);

            auto sameAssets =
                makePoolShareAsset(idr, idr, LIQUIDITY_POOL_FEE_V18);
            REQUIRE_THROWS_AS(root.changeTrust(sameAssets, 10),
                              ex_CHANGE_TRUST_MALFORMED);

            // create a pool trustline with a limit of 0
            REQUIRE_THROWS_AS(root.changeTrust(idrUsd, 0),
                              ex_CHANGE_TRUST_INVALID_LIMIT);

            SECTION("pool with two non-native assets")
            {
                poolShareTest(idr, usd);
            }
            SECTION("pool with two alphanum12 assets")
            {
                poolShareTest(makeAssetAlphanum12(gateway, "IDR12"),
                              makeAssetAlphanum12(gateway, "USD12"));
            }
            SECTION("pool with a native asset")
            {
                poolShareTest(makeNativeAsset(), idr);
            }
        });
    }
}
