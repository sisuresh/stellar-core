// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "SimulationUtils.h"
#include "crypto/SHA.h"
#include "crypto/SignerKey.h"
#include "transactions/TransactionUtils.h"

namespace stellar
{
namespace SimulationUtils
{
SecretKey
getNewSecret(AccountID const& key, uint32_t n)
{
    return SecretKey::fromSeed(
        sha256(KeyUtils::toStrKey(key) + std::to_string(n)));
}

int64_t
getNewOfferID(int64_t offerId, uint32_t n)
{
    if (n == 0 || offerId == 0)
    {
        return offerId;
    }

    // For the purpose of the simulation, assume offer id is capped at INT32_MAX
    if (offerId > INT32_MAX)
    {
        throw std::runtime_error(
            "Invalid offer id, simulation will not work correctly");
    }

    uint64 offset = static_cast<uint64_t>(n) << 32u;
    uint64_t newId = offset | static_cast<uint64_t>(offerId);

    return newId;
}

int64_t
getNewOfferID(OperationResult const& result, uint32_t n)
{
    if (result.code() != opINNER)
    {
        // offerID should not matter here, as the op failed anyway
        return 0;
    }

    int64_t offerID;
    switch (result.tr().type())
    {
    case MANAGE_SELL_OFFER:
        offerID =
            result.tr().manageSellOfferResult().success().offer.offer().offerID;
        break;
    case MANAGE_BUY_OFFER:
        offerID =
            result.tr().manageBuyOfferResult().success().offer.offer().offerID;
        break;
    case CREATE_PASSIVE_SELL_OFFER:
        offerID = result.tr()
                      .createPassiveSellOfferResult()
                      .success()
                      .offer.offer()
                      .offerID;
        break;
    default:
        throw std::runtime_error(
            "Invalid OperationResult: must be manage offer");
    }
    return getNewOfferID(offerID, n);
}

SignerKey
getNewEd25519Signer(Signer const& signer, uint32_t n)
{
    assert(signer.key.type() == SIGNER_KEY_TYPE_ED25519);
    auto pubKey = KeyUtils::convertKey<PublicKey>(signer.key);
    auto newPubKey = getNewSecret(pubKey, n).getPublicKey();
    return KeyUtils::convertKey<SignerKey>(newPubKey);
}

SecretKey
updateAccountID(AccountID& acc, uint32_t n)
{
    auto key = getNewSecret(acc, n);
    acc = key.getPublicKey();
    return key;
}

void
generateLiveEntries(std::vector<LedgerEntry>& entries,
                    std::vector<LedgerEntry> const& oldEntries, uint32_t count)
{
    for (auto const& le : oldEntries)
    {
        LedgerEntry newEntry = le;

        switch (le.data.type())
        {
        case ACCOUNT:
            updateAccountID(newEntry.data.account().accountID, count);
            newEntry.data.account().signers.clear();
            for (auto const& signer : le.data.account().signers)
            {
                if (signer.key.type() == SIGNER_KEY_TYPE_ED25519)
                {
                    newEntry.data.account().signers.emplace_back(
                        getNewEd25519Signer(signer, count), signer.weight);
                }
            }
            normalizeSigners(newEntry.data.account());
            break;
        case TRUSTLINE:
            updateAccountID(newEntry.data.trustLine().accountID, count);
            replaceIssuer(newEntry.data.trustLine().asset, count);
            break;
        case DATA:
            updateAccountID(newEntry.data.data().accountID, count);
            break;
        case OFFER:
            updateAccountID(newEntry.data.offer().sellerID, count);
            newEntry.data.offer().offerID =
                getNewOfferID(le.data.offer().offerID, count);
            replaceIssuer(newEntry.data.offer().buying, count);
            replaceIssuer(newEntry.data.offer().selling, count);
            break;
        default:
            abort();
        }
        entries.emplace_back(newEntry);
    }
}

void
generateDeadEntries(std::vector<LedgerKey>& dead,
                    std::vector<LedgerKey> const& oldKeys, uint32_t count)
{
    for (auto const& lk : oldKeys)
    {
        LedgerKey newKey = lk;

        switch (lk.type())
        {
        case ACCOUNT:
            updateAccountID(newKey.account().accountID, count);
            break;
        case TRUSTLINE:
            updateAccountID(newKey.trustLine().accountID, count);
            replaceIssuer(newKey.trustLine().asset, count);
            break;
        case DATA:
            updateAccountID(newKey.data().accountID, count);
            break;
        case OFFER:
            updateAccountID(newKey.offer().sellerID, count);
            newKey.offer().offerID = getNewOfferID(lk.offer().offerID, count);
            break;
        default:
            abort();
        }
        dead.emplace_back(newKey);
    }
}

void
replaceIssuer(Asset& asset, uint32_t n)
{
    switch (asset.type())
    {
    case ASSET_TYPE_CREDIT_ALPHANUM4:
        updateAccountID(asset.alphaNum4().issuer, n);
        break;
    case ASSET_TYPE_CREDIT_ALPHANUM12:
        updateAccountID(asset.alphaNum12().issuer, n);
        break;
    case ASSET_TYPE_NATIVE:
        // nothing to do for native assets
        break;
    default:
        abort();
    }
}

void
updateOperation(Operation& op, uint32_t n)
{
    // Update sourceAccount, if present
    if (op.sourceAccount)
    {
        auto opKey = SimulationUtils::getNewSecret(*op.sourceAccount, n);
        op.sourceAccount.activate() = opKey.getPublicKey();
    }

    switch (op.body.type())
    {
    case CREATE_ACCOUNT:
        updateAccountID(op.body.createAccountOp().destination, n);
        break;
    case PAYMENT:
        updateAccountID(op.body.paymentOp().destination, n);
        replaceIssuer(op.body.paymentOp().asset, n);
        break;
    case PATH_PAYMENT_STRICT_RECEIVE:
        replaceIssuer(op.body.pathPaymentStrictReceiveOp().sendAsset, n);
        replaceIssuer(op.body.pathPaymentStrictReceiveOp().destAsset, n);
        updateAccountID(op.body.pathPaymentStrictReceiveOp().destination, n);
        for (auto& asset : op.body.pathPaymentStrictReceiveOp().path)
        {
            replaceIssuer(asset, n);
        }
        break;
    case PATH_PAYMENT_STRICT_SEND:
        replaceIssuer(op.body.pathPaymentStrictSendOp().sendAsset, n);
        replaceIssuer(op.body.pathPaymentStrictSendOp().destAsset, n);
        updateAccountID(op.body.pathPaymentStrictSendOp().destination, n);
        for (auto& asset : op.body.pathPaymentStrictSendOp().path)
        {
            replaceIssuer(asset, n);
        }
        break;
    case MANAGE_SELL_OFFER:
        op.body.manageSellOfferOp().offerID =
            getNewOfferID(op.body.manageSellOfferOp().offerID, n);
        replaceIssuer(op.body.manageSellOfferOp().selling, n);
        replaceIssuer(op.body.manageSellOfferOp().buying, n);
        break;
    case CREATE_PASSIVE_SELL_OFFER:
        replaceIssuer(op.body.createPassiveSellOfferOp().selling, n);
        replaceIssuer(op.body.createPassiveSellOfferOp().buying, n);
        break;
    case SET_OPTIONS:
        if (op.body.setOptionsOp().signer)
        {
            Signer& signer = *op.body.setOptionsOp().signer;
            if (signer.key.type() == SIGNER_KEY_TYPE_ED25519)
            {
                signer = Signer{getNewEd25519Signer(signer, n), signer.weight};
            }
        }
        break;
    case CHANGE_TRUST:
        replaceIssuer(op.body.changeTrustOp().line, n);
        break;
    case ALLOW_TRUST:
        updateAccountID(op.body.allowTrustOp().trustor, n);
        break;
    case ACCOUNT_MERGE:
        updateAccountID(op.body.destination(), n);
        break;
    case MANAGE_BUY_OFFER:
        op.body.manageBuyOfferOp().offerID =
            getNewOfferID(op.body.manageBuyOfferOp().offerID, n);
        replaceIssuer(op.body.manageBuyOfferOp().selling, n);
        replaceIssuer(op.body.manageBuyOfferOp().buying, n);
        break;
    case INFLATION:
    case MANAGE_DATA:
    case BUMP_SEQUENCE:
        // Note: don't care about inflation
        break;
    default:
        abort();
    }
}
}
}