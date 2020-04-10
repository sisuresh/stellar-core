// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "catchup/simulation/MaintainLiabilitiesTestWork.h"
#include "catchup/simulation/GenerateBucketsWithNewAccountWork.h"
#include "crypto/SignerKey.h"
#include "ledger/LedgerManagerImpl.h"
#include "ledger/TrustLineWrapper.h"
#include "transactions/SignatureUtils.h"
#include "transactions/TransactionBridge.h"
#include "transactions/TransactionUtils.h"
#include "util/SimulationUtils.h"

// todo:remove
#include "xdrpp/printer.h"

namespace stellar
{
MaintainLiabilitiesTestWork::MaintainLiabilitiesTestWork(
    Application& app, TmpDir const& downloadDir, LedgerRange const& range,
    std::string const& networkPassphrase, uint32_t desiredOperations,
    uint32_t multiplier, bool verifyResults)
    : ApplyTransactionsWork(app, downloadDir, range, networkPassphrase,
                            desiredOperations, multiplier, verifyResults)
    , mTestAccount(SecretKey::fromSeed(
          GenerateBucketsWithNewAccountWork::TEST_ACCOUNT_SEED))
    , mTestAccountSeqNum(1)
{
    auto const& lcl = mApp.getLedgerManager().getLastClosedLedgerHeader();
    // upgrade to protocol 13 is in onReset
    if (lcl.header.ledgerVersion != 12 ||
        Config::CURRENT_LEDGER_PROTOCOL_VERSION != 13)
    {
        throw std::runtime_error("Invalid ledger version: can only force "
                                 "upgrade from 12 to 13");
    }
}

void
logAndThrow(std::string const& msg)
{
    CLOG(ERROR, "History") << msg;
    throw std::runtime_error(msg);
}

void
MaintainLiabilitiesTestWork::getSecretKeyMap(
    AbstractLedgerTxn& ltx, TransactionEnvelope& env, int n,
    std::unordered_map<PublicKey, SecretKey>& newKeyMap)
{
    AccountID acc;
    switch (env.type())
    {
    case ENVELOPE_TYPE_TX_V0:
        acc.type(PUBLIC_KEY_TYPE_ED25519);
        acc.ed25519() = env.v0().tx.sourceAccountEd25519;
        break;
    case ENVELOPE_TYPE_TX:
        acc = env.v1().tx.sourceAccount;
        break;
    case ENVELOPE_TYPE_TX_FEE_BUMP:
        // Note: just pull key from inner tx for now
        assert(env.feeBump().tx.innerTx.type() == ENVELOPE_TYPE_TX);
        acc = env.feeBump().tx.innerTx.v1().tx.sourceAccount;
        break;
    default:
        throw std::runtime_error("Unknown envelope type");
    }

    getKeysFromAccount(acc, ltx, n, newKeyMap);
    getKeysFromOps(env, n, newKeyMap);
}

void
MaintainLiabilitiesTestWork::getKeysFromOps(
    TransactionEnvelope& env, uint32_t n,
    std::unordered_map<PublicKey, SecretKey>& newKeyMap)
{
    auto& ops = txbridge::getOperations(env);

    auto getKeyFromAccount = [&](AccountID acc) {
        auto secret = SimulationUtils::updateAccountID(acc, n);
        newKeyMap[acc] = secret;
    };

    auto getKeyFromIssuer = [&](Asset& asset) {
        switch (asset.type())
        {
        case ASSET_TYPE_CREDIT_ALPHANUM4:
            getKeyFromAccount(asset.alphaNum4().issuer);
            break;
        case ASSET_TYPE_CREDIT_ALPHANUM12:
            getKeyFromAccount(asset.alphaNum12().issuer);
            break;
        case ASSET_TYPE_NATIVE:
            // nothing to do for native assets
            break;
        default:
            abort();
        }
    };

    for (auto& op : ops)
    {
        if (op.sourceAccount)
        {
            getKeyFromAccount(*op.sourceAccount);
        }

        switch (op.body.type())
        {
        case CREATE_ACCOUNT:
            getKeyFromAccount(op.body.createAccountOp().destination);
            break;
        case PAYMENT:
            getKeyFromAccount(op.body.paymentOp().destination);
            getKeyFromIssuer(op.body.paymentOp().asset);
            break;
        case PATH_PAYMENT_STRICT_RECEIVE:
            getKeyFromIssuer(op.body.pathPaymentStrictReceiveOp().sendAsset);
            getKeyFromIssuer(op.body.pathPaymentStrictReceiveOp().destAsset);
            getKeyFromAccount(op.body.pathPaymentStrictReceiveOp().destination);
            for (auto& asset : op.body.pathPaymentStrictReceiveOp().path)
            {
                getKeyFromIssuer(asset);
            }
            break;
        case PATH_PAYMENT_STRICT_SEND:
            getKeyFromIssuer(op.body.pathPaymentStrictSendOp().sendAsset);
            getKeyFromIssuer(op.body.pathPaymentStrictSendOp().destAsset);
            getKeyFromAccount(op.body.pathPaymentStrictSendOp().destination);
            for (auto& asset : op.body.pathPaymentStrictSendOp().path)
            {
                getKeyFromIssuer(asset);
            }
            break;
        case MANAGE_SELL_OFFER:
            getKeyFromIssuer(op.body.manageSellOfferOp().selling);
            getKeyFromIssuer(op.body.manageSellOfferOp().buying);
            break;
        case CREATE_PASSIVE_SELL_OFFER:
            getKeyFromIssuer(op.body.createPassiveSellOfferOp().selling);
            getKeyFromIssuer(op.body.createPassiveSellOfferOp().buying);
            break;
        case SET_OPTIONS:
            if (op.body.setOptionsOp().signer)
            {
                Signer& signer = *op.body.setOptionsOp().signer;
                if (signer.key.type() == SIGNER_KEY_TYPE_ED25519)
                {
                    auto pubKey = KeyUtils::convertKey<PublicKey>(signer.key);
                    getKeyFromAccount(pubKey);
                }
            }
            break;
        case CHANGE_TRUST:
            getKeyFromIssuer(op.body.changeTrustOp().line);
            break;
        case ALLOW_TRUST:
            getKeyFromAccount(op.body.allowTrustOp().trustor);
            break;
        case ACCOUNT_MERGE:
            getKeyFromAccount(op.body.destination());
            break;
        case MANAGE_BUY_OFFER:
            getKeyFromIssuer(op.body.manageBuyOfferOp().selling);
            getKeyFromIssuer(op.body.manageBuyOfferOp().buying);
            break;
        case INFLATION:
        case MANAGE_DATA:
        case BUMP_SEQUENCE:
            break;
        default:
            abort();
        }
    }
}

void
MaintainLiabilitiesTestWork::getKeysFromAccount(
    AccountID const& acc, AbstractLedgerTxn& ltx, uint32_t n,
    std::unordered_map<PublicKey, SecretKey>& newKeyMap)
{
    auto newSecret = SimulationUtils::getNewSecret(acc, n);
    newKeyMap[newSecret.getPublicKey()] = newSecret;

    auto account = stellar::loadAccount(ltx, acc);
    if (!account)
    {
        return;
    }

    for (auto const& signer : account.current().data.account().signers)
    {
        if (signer.key.type() == SIGNER_KEY_TYPE_ED25519)
        {
            auto pubKey = KeyUtils::convertKey<PublicKey>(signer.key);
            auto signerSecret = SimulationUtils::getNewSecret(pubKey, n);
            newKeyMap[signerSecret.getPublicKey()] = signerSecret;
        }
    }
}

bool
signForIssuerAccount(SecretKey const& issuerKey, AbstractLedgerTxn& ltx,
                     xdr::xvector<DecoratedSignature, 20>& sigs,
                     Hash const& txHash,
                     std::unordered_map<PublicKey, SecretKey> const& newKeyMap,
                     bool setOptionsFound)
{
    auto issuerAccount =
        stellar::loadAccountWithoutRecord(ltx, issuerKey.getPublicKey());

    // shouldn't happen
    if (!issuerAccount)
    {
        logAndThrow("signForAllowTrust: issuer account missing");
    }

    auto& acc = issuerAccount.current().data.account();

    // Our operations will have allow trust, and maybe a set options
    int32_t neededThreshold = setOptionsFound ? acc.thresholds[THRESHOLD_MED]
                                              : acc.thresholds[THRESHOLD_LOW];
    neededThreshold -= acc.thresholds[THRESHOLD_MASTER_WEIGHT];

    // add source account key
    sigs.push_back(SignatureUtils::sign(issuerKey, txHash));
    if (neededThreshold <= 0)
    {
        return true;
    }

    // source account key isn't enough. so go through all signers
    for (auto& signer : acc.signers)
    {
        if (signer.key.type() != SIGNER_KEY_TYPE_ED25519)
        {
            continue;
        }

        auto it = newKeyMap.find(KeyUtils::convertKey<PublicKey>(signer.key));
        if (it == newKeyMap.end())
        {
            logAndThrow("signForAllowTrust: signer secret missing");
        }

        neededThreshold -= signer.weight;
        sigs.push_back(SignatureUtils::sign(it->second, txHash));
    }

    return neededThreshold <= 0;
}

void
MaintainLiabilitiesTestWork::processSimulatedTx(
    AbstractLedgerTxn& ltx, TransactionResultPair const& txResPair,
    TransactionEnvelope& originalEnv, TransactionEnvelope& simulatedEnv,
    std::vector<TransactionEnvelope>& transactions,
    std::vector<TransactionResultPair>& results, uint32_t n)
{
    // any value above 1 would be incorrect here (multiplier should be 2). this
    // can be simplified
    if (n == 1)
    {
        std::unordered_map<PublicKey, SecretKey> newKeyMap;
        getSecretKeyMap(ltx, originalEnv, n, newKeyMap);
        // save previous Tx
        std::vector<TransactionEnvelope> savedTxs;

        // since we need AUTH_REQUIRED_FLAG and AUTH_REVOCABLE_FLAG, we need to
        // save the flag state before we modify it so we can set it back at the
        // end of the sandwich
        std::unordered_map<PublicKey, uint32_t> issuerToAccountFlags;

        auto addTx = [&](TransactionEnvelope& newEnv,
                         SecretKey const& issuerKey, bool saveTx) {
            auto& ops = newEnv.v1().tx.operations;

            if (ops.size() == 0)
            {
                return;
            }

            //// make sure this isn't just a set options
            bool allowTrustFound = false;
            bool setOptionsFound = false;
            for (size_t i = 0; i < ops.size(); i++)
            {
                if (ops[i].body.type() == ALLOW_TRUST)
                {
                    allowTrustFound = true;
                }
                if (ops[i].body.type() == SET_OPTIONS)
                {
                    setOptionsFound = true;
                }
            }

            if (!allowTrustFound)
            {
                return;
            }

            // Todo: This might be unnecessary. Can we just set this to a high
            // value?
            newEnv.v1().tx.fee = static_cast<uint32_t>(
                (ops.size() * mApp.getLedgerManager().getLastTxFee()) &
                UINT32_MAX);

            newEnv.v1().tx.seqNum = mTestAccountSeqNum++;

            auto txFrame = TransactionFrameBase::makeTransactionFromWire(
                mApp.getNetworkID(), newEnv);
            auto txHash = txFrame->getContentsHash();

            // sign with source account and issuer
            auto& sigs = txbridge::getSignaturesInner(newEnv);
            sigs.clear();
            sigs.push_back(SignatureUtils::sign(mTestAccount, txHash));

            if (!signForIssuerAccount(issuerKey, ltx, sigs, txHash, newKeyMap,
                                      setOptionsFound))
            {
                // unable to meet threshold... todo:this is a problem if
                // trustlines are in auth to maintain liabilities
                --mTestAccountSeqNum;
                return;
            }

            TransactionResultPair txResPair;
            txResPair.result.result.code(txSUCCESS);
            txResPair.result.result.results().resize(
                static_cast<uint32_t>(ops.size()));
            txResPair.transactionHash = txHash;

            for (size_t i = 0; i < ops.size(); i++)
            {
                auto& opResult = txResPair.result.result.results()[i];
                opResult.code(opINNER);
                opResult.tr().type(ops[i].body.type());
            }

            // we always expect allow trust to succeed
            results.emplace_back(txResPair);
            transactions.emplace_back(newEnv);

            if (saveTx)
            {
                savedTxs.emplace_back(newEnv);
            }

            newEnv.v1().tx.operations.clear();
        };

        auto const& issuerToTrustorToAsset =
            getUsedIssuerAssets(simulatedEnv, ltx);
        for (auto const& issuerToTrustorToAssetKvp : issuerToTrustorToAsset)
        {
            TransactionEnvelope authTxEnv(ENVELOPE_TYPE_TX);
            authTxEnv.v1().tx.sourceAccount = mTestAccount.getPublicKey();

            auto const& issuer = issuerToTrustorToAssetKvp.first;

            //// get current flags so we can set them back at the end of the
            /// sandwich
            uint32_t currentFlags;
            {
                auto issuerAccount =
                    stellar::loadAccountWithoutRecord(ltx, issuer);
                if (!issuerAccount)
                {
                    CLOG(ERROR, "History") << "Can't load issuer account";
                    // we could recreate the issuer
                    continue;
                }

                currentFlags = issuerAccount.current().data.account().flags;
            }

            issuerToAccountFlags[issuer] = currentFlags;

            ////AUTH_REQUIRED_FLAG and AUTH_REVOCABLE_FLAG are required to
            /// change trsutline flags
            auto toSet = static_cast<uint32_t>(AUTH_REQUIRED_FLAG) |
                         static_cast<uint32_t>(AUTH_REVOCABLE_FLAG);

            Operation op1;
            op1.body.type(SET_OPTIONS);
            op1.sourceAccount.activate() = issuer;
            op1.body.setOptionsOp().setFlags.activate() = toSet;

            authTxEnv.v1().tx.operations.emplace_back(op1);

            //// get issuer secret key
            auto it = newKeyMap.find(issuer);
            if (it == newKeyMap.end())
            {
                logAndThrow("Issuer key is missing from map");
            }
            auto const& issuerSecret = it->second;

            for (auto const& trustorToAsset : issuerToTrustorToAssetKvp.second)
            {
                auto& trustorAccount = trustorToAsset.first;

                auto& assets = trustorToAsset.second;
                for (auto& asset : assets)
                {
                    if (asset.type() == ASSET_TYPE_NATIVE)
                    {
                        continue;
                    }

                    auto trustLine =
                        loadTrustLineWithoutRecord(ltx, trustorAccount, asset);
                    if (!trustLine ||
                        !trustLine.isAuthorizedToMaintainLiabilities())
                    {
                        continue;
                    }

                    // We only have this restriction because the bottom of
                    // the sandwich is reconstructed from the top, with the
                    // set options flipped to the bottom of the operations
                    // instead of the beginning. Multiple transactions would
                    // make the current implementation a little more
                    // complicated, so we can fix it if we hit this
                    // scenario. (I don't think we'll actually see this
                    // though)
                    if (authTxEnv.v1().tx.operations.size() == MAX_OPS_PER_TX)
                    {
                        logAndThrow("We Only expect one transaction per "
                                    "issuer at the moment");
                        // addTx(authTxEnv, issuerSecret, true);
                    }

                    // authorize
                    Operation op2;
                    op2.sourceAccount.activate() = issuer;
                    op2.body.type(ALLOW_TRUST);
                    op2.body.allowTrustOp().trustor = trustorAccount;
                    op2.body.allowTrustOp().asset.type(asset.type());
                    op2.body.allowTrustOp().authorize = AUTHORIZED_FLAG;

                    if (op2.body.allowTrustOp().asset.type() ==
                        ASSET_TYPE_CREDIT_ALPHANUM4)
                    {
                        op2.body.allowTrustOp().asset.assetCode4() =
                            asset.alphaNum4().assetCode;
                    }
                    else
                    {
                        op2.body.allowTrustOp().asset.assetCode12() =
                            asset.alphaNum12().assetCode;
                    }

                    authTxEnv.v1().tx.operations.emplace_back(op2);
                }
            }

            addTx(authTxEnv, issuerSecret, true);
        }

        // middle of sandwich
        results.emplace_back(txResPair);
        transactions.emplace_back(simulatedEnv);

        // bottom of sandwich
        for (auto& savedTxEnv : savedTxs)
        {
            auto postTxEnv = savedTxEnv;
            auto& postTxOps = postTxEnv.v1().tx.operations;

            if (postTxOps.empty())
            {
                logAndThrow("postTxOps is empty!");
            }

            if (!postTxOps.begin()->sourceAccount)
            {
                logAndThrow("issuer account should be set!");
            }

            PublicKey issuer = *(postTxOps.begin()->sourceAccount);

            int setOptionsInd = -1;
            for (size_t i = 0; i < postTxOps.size(); ++i)
            {
                auto& op = postTxOps[i];
                auto opType = op.body.type();
                if (opType == ALLOW_TRUST)
                {
                    op.body.allowTrustOp().authorize =
                        AUTHORIZED_TO_MAINTAIN_LIABILITIES_FLAG;
                }
                else if (opType == SET_OPTIONS)
                {
                    auto flagIt = issuerToAccountFlags.find(issuer);
                    if (flagIt == issuerToAccountFlags.end())
                    {
                        logAndThrow("issuer account flag is missing");
                    }

                    auto originalFlags = flagIt->second;
                    uint32_t flagsToClear = 0;
                    if (!(originalFlags & AUTH_REQUIRED_FLAG))
                    {
                        flagsToClear |= AUTH_REQUIRED_FLAG;
                    }

                    if (!(originalFlags & AUTH_REVOCABLE_FLAG))
                    {
                        flagsToClear |= AUTH_REVOCABLE_FLAG;
                    }

                    op.body.setOptionsOp().clearFlags.activate() = flagsToClear;
                    op.body.setOptionsOp().setFlags = nullptr;

                    setOptionsInd = i;
                }
                else
                {
                    abort();
                }
            }

            // setOptions is called first in the top of the sandwich, but it
            // needs to be called last on the bottom since we need to set
            // flags to maintain liabilities
            if (setOptionsInd != -1)
            {
                auto setOptionsOp = postTxOps[setOptionsInd];
                postTxOps.erase(postTxOps.begin() + setOptionsInd);
                postTxOps.emplace_back(setOptionsOp);
            }

            auto it2 = newKeyMap.find(issuer);
            if (it2 == newKeyMap.end())
            {
                // this shouldn't happen here
                logAndThrow("Issuer key is missing from map!");
            }

            addTx(postTxEnv, it2->second, false);
        }
    }
    else
    {
        results.emplace_back(txResPair);
        transactions.emplace_back(simulatedEnv);
    }
}

void
MaintainLiabilitiesTestWork::onReset()
{
    auto& lm = mApp.getLedgerManager();
    auto const& lclHeader = lm.getLastClosedLedgerHeader();
    auto const& header = lclHeader.header;

    LedgerUpgrade upgrade(LEDGER_UPGRADE_VERSION);
    upgrade.newLedgerVersion() = Config::CURRENT_LEDGER_PROTOCOL_VERSION;
    auto opaqueUpgrade = xdr::xdr_to_opaque(upgrade);

    StellarValue sv;
    sv.upgrades.emplace_back(opaqueUpgrade.begin(), opaqueUpgrade.end());

    TransactionSet txSetXDR;
    txSetXDR.previousLedgerHash = lclHeader.hash;
    auto txSet = std::make_shared<TxSetFrame>(mNetworkID, txSetXDR);

    sv.txSetHash = txSet->getContentsHash();
    sv.closeTime = header.scpValue.closeTime + 1;

    LedgerCloseData closeData(header.ledgerSeq + 1, txSet, sv);
    lm.closeLedger(closeData);

    ApplyTransactionsWork::onReset();
}

std::map<PublicKey, std::map<PublicKey, std::set<Asset>>>
MaintainLiabilitiesTestWork::getUsedIssuerAssets(TransactionEnvelope& env,
                                                 AbstractLedgerTxn& ltx) const
{
    // issuer -> trustor -> Asset
    std::map<PublicKey, std::map<PublicKey, std::set<Asset>>> assetsAndAccounts;

    auto txSourceAccount = txbridge::getTxSourceAccountInner(env);
    auto& ops = txbridge::getOperations(env);
    for (auto& op : ops)
    {
        auto opSourceAccount =
            op.sourceAccount ? *op.sourceAccount : txSourceAccount;

        auto recordIssuerAndTrustor = [&](Asset const& asset,
                                          AccountID const& trustor) {
            AccountID issuer;
            switch (asset.type())
            {
            case ASSET_TYPE_CREDIT_ALPHANUM4:
                issuer = asset.alphaNum4().issuer;
                break;
            case ASSET_TYPE_CREDIT_ALPHANUM12:
                issuer = asset.alphaNum12().issuer;
                break;
            case ASSET_TYPE_NATIVE:
                return;
            default:
                abort();
            }

            // can't use allow trust on self
            if (issuer == trustor)
            {
                return;
            }

            assetsAndAccounts[issuer][trustor].emplace(asset);
        };

        switch (op.body.type())
        {
        case PAYMENT:
            recordIssuerAndTrustor(op.body.paymentOp().asset,
                                   op.body.paymentOp().destination);
            recordIssuerAndTrustor(op.body.paymentOp().asset, opSourceAccount);

            break;
        case PATH_PAYMENT_STRICT_RECEIVE:
            recordIssuerAndTrustor(
                op.body.pathPaymentStrictReceiveOp().destAsset,
                op.body.pathPaymentStrictReceiveOp().destination);
            recordIssuerAndTrustor(
                op.body.pathPaymentStrictReceiveOp().sendAsset,
                opSourceAccount);

            // todo:Is it alright to ignore the trustlines along the path? Path
            // only specifies the asset path
            break;
        case PATH_PAYMENT_STRICT_SEND:
            recordIssuerAndTrustor(
                op.body.pathPaymentStrictSendOp().destAsset,
                op.body.pathPaymentStrictSendOp().destination);
            recordIssuerAndTrustor(op.body.pathPaymentStrictSendOp().sendAsset,
                                   opSourceAccount);
            break;
        case MANAGE_SELL_OFFER:
            recordIssuerAndTrustor(op.body.manageSellOfferOp().selling,
                                   opSourceAccount);
            recordIssuerAndTrustor(op.body.manageSellOfferOp().buying,
                                   opSourceAccount);
            break;
        case CREATE_PASSIVE_SELL_OFFER:
            recordIssuerAndTrustor(op.body.createPassiveSellOfferOp().selling,
                                   opSourceAccount);
            recordIssuerAndTrustor(op.body.createPassiveSellOfferOp().buying,
                                   opSourceAccount);
            break;
        case MANAGE_BUY_OFFER:
            recordIssuerAndTrustor(op.body.manageBuyOfferOp().selling,
                                   opSourceAccount);
            recordIssuerAndTrustor(op.body.manageBuyOfferOp().buying,
                                   opSourceAccount);
            break;
        case CREATE_ACCOUNT:
        case SET_OPTIONS:
        case CHANGE_TRUST:
        case ALLOW_TRUST:
        case ACCOUNT_MERGE:
        case INFLATION:
        case MANAGE_DATA:
        case BUMP_SEQUENCE:
            break;
        default:
            abort();
        }
    }

    return assetsAndAccounts;
}
}