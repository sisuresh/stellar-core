// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "transactions/SponsorshipUtils.h"
#include "ledger/LedgerTxn.h"
#include "ledger/LedgerTxnEntry.h"
#include "ledger/LedgerTxnHeader.h"
#include "overlay/StellarXDR.h"
#include "transactions/TransactionUtils.h"
#include "util/types.h"

using namespace stellar;

static SponsorshipResult
canEstablishSponsorshipHelper(LedgerHeader const& lh,
                              LedgerEntry const& sponsoringAcc,
                              LedgerEntry const* sponsoredAcc, uint32_t mult)
{
    int64_t reserve = (int64_t)mult * (int64_t)lh.baseReserve;
    if (getAvailableBalance(lh, sponsoringAcc) < reserve)
    {
        return SponsorshipResult::LOW_RESERVE;
    }

    if (getNumSponsoring(sponsoringAcc) > UINT32_MAX - mult)
    {
        return SponsorshipResult::TOO_MANY_SPONSORING;
    }
    if (sponsoredAcc && getNumSponsored(*sponsoredAcc) > UINT32_MAX - mult)
    {
        return SponsorshipResult::TOO_MANY_SPONSORED;
    }

    return SponsorshipResult::SUCCESS;
}

static SponsorshipResult
canRemoveSponsorshipHelper(LedgerHeader const& lh,
                           LedgerEntry const& sponsoringAcc,
                           LedgerEntry const* sponsoredAcc, uint32_t mult)
{
    if (getNumSponsoring(sponsoringAcc) < mult)
    {
        throw std::runtime_error("insufficient numSponsoring");
    }
    if (sponsoredAcc && getNumSponsored(*sponsoredAcc) < mult)
    {
        throw std::runtime_error("insufficient numSponsored");
    }

    int64_t reserve = (int64_t)mult * (int64_t)lh.baseReserve;
    if (sponsoredAcc && getAvailableBalance(lh, *sponsoredAcc) < reserve)
    {
        return SponsorshipResult::LOW_RESERVE;
    }

    return SponsorshipResult::SUCCESS;
}

static SponsorshipResult
canTransferSponsorshipHelper(LedgerHeader const& lh,
                             LedgerEntry const& oldSponsoringAcc,
                             LedgerEntry const& newSponsoringAcc, uint32_t mult)
{
    auto removeRes =
        canRemoveSponsorshipHelper(lh, oldSponsoringAcc, nullptr, mult);
    if (removeRes != SponsorshipResult::SUCCESS)
    {
        return removeRes;
    }

    return canEstablishSponsorshipHelper(lh, newSponsoringAcc, nullptr, mult);
}

namespace stellar
{

////////////////////////////////////////////////////////////////////////////////
//
// Sponsorship "getters"
//
////////////////////////////////////////////////////////////////////////////////
uint32_t
getNumSponsored(LedgerEntry const& le)
{
    auto const& ae = le.data.account();
    if (ae.ext.v() == 1 && ae.ext.v1().ext.v() == 2)
    {
        return ae.ext.v1().ext.v2().numSponsored;
    }
    return 0;
}

uint32_t
getNumSponsoring(LedgerEntry const& le)
{
    auto const& ae = le.data.account();
    if (ae.ext.v() == 1 && ae.ext.v1().ext.v() == 2)
    {
        return ae.ext.v1().ext.v2().numSponsoring;
    }
    return 0;
}

IsSignerSponsoredResult
isSignerSponsored(std::vector<Signer>::const_iterator const& signerIt,
                  LedgerEntry const& le)
{
    auto const& ae = le.data.account();
    if (signerIt == ae.signers.end())
    {
        return IsSignerSponsoredResult::DOES_NOT_EXIST;
    }

    if (ae.ext.v() == 1 && ae.ext.v1().ext.v() == 2)
    {
        auto const& extV2 = ae.ext.v1().ext.v2();
        size_t n = signerIt - ae.signers.begin();
        if (extV2.signerSponsoringIDs.at(n))
        {
            return IsSignerSponsoredResult::SPONSORED;
        }
    }
    return IsSignerSponsoredResult::NOT_SPONSORED;
}

////////////////////////////////////////////////////////////////////////////////
//
// Utility functions to check if you can establish/remove/transfer sponsorships
//
////////////////////////////////////////////////////////////////////////////////
static uint32_t
computeMultiplier(LedgerEntryType let)
{
    // TODO(jonjove): Handle ClaimableBalance
    if (let == ACCOUNT)
    {
        return 2;
    }
    return 1;
}

SponsorshipResult
canEstablishEntrySponsorship(LedgerHeader const& lh, LedgerEntry const& le,
                             LedgerEntry const& sponsoringAcc,
                             LedgerEntry const* sponsoredAcc)
{
    if (lh.ledgerVersion < 14)
    {
        throw std::runtime_error("sponsorship before version 14");
    }
    if (le.ext.v() == 1 && le.ext.v1().sponsoringID)
    {
        throw std::runtime_error("sponsoring sponsored entry");
    }

    uint32_t mult = computeMultiplier(le.data.type());
    return canEstablishSponsorshipHelper(lh, sponsoringAcc, sponsoredAcc, mult);
}

SponsorshipResult
canRemoveEntrySponsorship(LedgerHeader const& lh, LedgerEntry const& le,
                          LedgerEntry const& sponsoringAcc,
                          LedgerEntry const* sponsoredAcc)
{
    if (lh.ledgerVersion < 14)
    {
        throw std::runtime_error("sponsorship before version 14");
    }
    if (le.ext.v() == 0 || !le.ext.v1().sponsoringID)
    {
        throw std::runtime_error("removing sponsorship on unsponsored entry");
    }

    uint32_t mult = computeMultiplier(le.data.type());
    return canRemoveSponsorshipHelper(lh, sponsoringAcc, sponsoredAcc, mult);
}

SponsorshipResult
canTransferEntrySponsorship(LedgerHeader const& lh, LedgerEntry const& le,
                            LedgerEntry const& oldSponsoringAcc,
                            LedgerEntry const& newSponsoringAcc)
{
    if (lh.ledgerVersion < 14)
    {
        throw std::runtime_error("sponsorship before version 14");
    }
    if (le.ext.v() == 0 || !le.ext.v1().sponsoringID)
    {
        throw std::runtime_error(
            "transferring sponsorship on unsponsored entry");
    }

    uint32_t mult = computeMultiplier(le.data.type());
    return canTransferSponsorshipHelper(lh, oldSponsoringAcc, newSponsoringAcc,
                                        mult);
}

SponsorshipResult
canEstablishSignerSponsorship(
    LedgerHeader const& lh, std::vector<Signer>::const_iterator const& signerIt,
    LedgerEntry const& sponsoringAcc, LedgerEntry const& sponsoredAcc)
{
    if (lh.ledgerVersion < 14)
    {
        throw std::runtime_error("sponsorship before version 14");
    }
    if (isSignerSponsored(signerIt, sponsoredAcc) !=
        IsSignerSponsoredResult::NOT_SPONSORED)
    {
        throw std::runtime_error("bad signer sponsorship");
    }

    return canEstablishSponsorshipHelper(lh, sponsoringAcc, &sponsoredAcc, 1);
}

SponsorshipResult
canRemoveSignerSponsorship(LedgerHeader const& lh,
                           std::vector<Signer>::const_iterator const& signerIt,
                           LedgerEntry const& sponsoringAcc,
                           LedgerEntry const& sponsoredAcc)
{
    if (lh.ledgerVersion < 14)
    {
        throw std::runtime_error("sponsorship before version 14");
    }
    if (isSignerSponsored(signerIt, sponsoredAcc) !=
        IsSignerSponsoredResult::SPONSORED)
    {
        throw std::runtime_error("bad signer sponsorship");
    }

    return canRemoveSponsorshipHelper(lh, sponsoringAcc, &sponsoredAcc, 1);
}

SponsorshipResult
canTransferSignerSponsorship(
    LedgerHeader const& lh, std::vector<Signer>::const_iterator const& signerIt,
    LedgerEntry const& oldSponsoringAcc, LedgerEntry const& newSponsoringAcc,
    LedgerEntry const& sponsoredAcc)
{
    if (lh.ledgerVersion < 14)
    {
        throw std::runtime_error("sponsorship before version 14");
    }
    if (isSignerSponsored(signerIt, sponsoredAcc) !=
        IsSignerSponsoredResult::SPONSORED)
    {
        throw std::runtime_error("bad signer sponsorship");
    }

    return canTransferSponsorshipHelper(lh, oldSponsoringAcc, newSponsoringAcc,
                                        1);
}

////////////////////////////////////////////////////////////////////////////////
//
// Utility functions to establish/remove/transfer sponsorships
//
////////////////////////////////////////////////////////////////////////////////
void
establishEntrySponsorship(LedgerEntry& le, LedgerEntry& sponsoringAcc,
                          LedgerEntry* sponsoredAcc)
{
    uint32_t mult = computeMultiplier(le.data.type());
    prepareLedgerEntryExtensionV1(le).sponsoringID.activate() =
        sponsoringAcc.data.account().accountID;
    prepareAccountEntryExtensionV2(sponsoringAcc.data.account())
        .numSponsoring += mult;
    if (sponsoredAcc)
    {
        prepareAccountEntryExtensionV2(sponsoredAcc->data.account())
            .numSponsored += mult;
    }
}

void
removeEntrySponsorship(LedgerEntry& le, LedgerEntry& sponsoringAcc,
                       LedgerEntry* sponsoredAcc)
{
    uint32_t mult = computeMultiplier(le.data.type());
    prepareLedgerEntryExtensionV1(le).sponsoringID.reset();
    prepareAccountEntryExtensionV2(sponsoringAcc.data.account())
        .numSponsoring -= mult;
    if (sponsoredAcc)
    {
        prepareAccountEntryExtensionV2(sponsoredAcc->data.account())
            .numSponsored -= mult;
    }
}

void
transferEntrySponsorship(LedgerEntry& le, LedgerEntry& oldSponsoringAcc,
                         LedgerEntry& newSponsoringAcc)
{
    uint32_t mult = computeMultiplier(le.data.type());
    prepareLedgerEntryExtensionV1(le).sponsoringID.activate() =
        newSponsoringAcc.data.account().accountID;
    prepareAccountEntryExtensionV2(newSponsoringAcc.data.account())
        .numSponsoring += mult;
    prepareAccountEntryExtensionV2(oldSponsoringAcc.data.account())
        .numSponsoring -= mult;
}

void
establishSignerSponsorship(std::vector<Signer>::const_iterator const& signerIt,
                           LedgerEntry& sponsoringAcc,
                           LedgerEntry& sponsoredAcc)
{
    size_t n = signerIt - sponsoredAcc.data.account().signers.begin();

    auto& extV2 = prepareAccountEntryExtensionV2(sponsoredAcc.data.account());
    extV2.signerSponsoringIDs.at(n).activate() =
        sponsoringAcc.data.account().accountID;
    ++extV2.numSponsored;
    ++prepareAccountEntryExtensionV2(sponsoringAcc.data.account())
          .numSponsoring;
}

void
removeSignerSponsorship(std::vector<Signer>::const_iterator const& signerIt,
                        LedgerEntry& sponsoringAcc, LedgerEntry& sponsoredAcc)
{
    size_t n = signerIt - sponsoredAcc.data.account().signers.begin();

    auto& extV2 = prepareAccountEntryExtensionV2(sponsoredAcc.data.account());
    extV2.signerSponsoringIDs.at(n).reset();
    --extV2.numSponsored;
    --prepareAccountEntryExtensionV2(sponsoringAcc.data.account())
          .numSponsoring;
}

void
transferSignerSponsorship(std::vector<Signer>::const_iterator const& signerIt,
                          LedgerEntry& oldSponsoringAcc,
                          LedgerEntry& newSponsoringAcc,
                          LedgerEntry& sponsoredAcc)
{
    size_t n = signerIt - sponsoredAcc.data.account().signers.begin();

    auto& extV2 = prepareAccountEntryExtensionV2(sponsoredAcc.data.account());
    extV2.signerSponsoringIDs.at(n).activate() =
        newSponsoringAcc.data.account().accountID;
    ++prepareAccountEntryExtensionV2(newSponsoringAcc.data.account())
          .numSponsoring;
    --prepareAccountEntryExtensionV2(oldSponsoringAcc.data.account())
          .numSponsoring;
}

////////////////////////////////////////////////////////////////////////////////
//
// Utility functions to check if you can create/remove an entry with or without
// sponsorship
//
////////////////////////////////////////////////////////////////////////////////
SponsorshipResult
canCreateEntryWithoutSponsorship(LedgerHeader const& lh, LedgerEntry const& le,
                                 LedgerEntry const& acc)
{
    if (le.data.type() != ACCOUNT)
    {
        if (lh.ledgerVersion >= FIRST_PROTOCOL_SUPPORTING_OPERATION_LIMITS &&
            acc.data.account().numSubEntries >= ACCOUNT_SUBENTRY_LIMIT)
        {
            return SponsorshipResult::TOO_MANY_SUBENTRIES;
        }

        uint32_t mult = computeMultiplier(le.data.type());
        if (lh.ledgerVersion < 9)
        {
            // This is needed to handle the overflow in getMinBalance which was
            // corrected in protocol version 9
            auto accCopy = acc;
            accCopy.data.account().numSubEntries += mult;
            if (getAvailableBalance(lh, accCopy) < 0)
            {
                return SponsorshipResult::LOW_RESERVE;
            }
        }
        else
        {
            int64_t reserve = (int64_t)mult * (int64_t)lh.baseReserve;
            if (getAvailableBalance(lh, acc) < reserve)
            {
                return SponsorshipResult::LOW_RESERVE;
            }
        }
    }
    else
    {
        if (le.data.account().balance < getMinBalance(lh, acc.data.account()))
        {
            return SponsorshipResult::LOW_RESERVE;
        }
    }

    return SponsorshipResult::SUCCESS;
}

SponsorshipResult
canCreateEntryWithSponsorship(LedgerHeader const& lh, LedgerEntry const& le,
                              LedgerEntry const& sponsoringAcc,
                              LedgerEntry const* sponsoredAcc)
{
    if (lh.ledgerVersion < 14)
    {
        throw std::runtime_error("sponsorship before version 14");
    }

    // TODO(jonjove): Handle ClaimableBalance
    if (sponsoredAcc && le.data.type() != ACCOUNT)
    {
        auto const& acc = sponsoredAcc->data.account();
        if (acc.numSubEntries >= ACCOUNT_SUBENTRY_LIMIT)
        {
            return SponsorshipResult::TOO_MANY_SUBENTRIES;
        }
    }

    return canEstablishEntrySponsorship(lh, le, sponsoringAcc, sponsoredAcc);
}

void
canRemoveEntryWithoutSponsorship(LedgerHeader const& lh, LedgerEntry const& le,
                                 LedgerEntry const& acc)
{
    if (le.data.type() != ACCOUNT)
    {
        uint32_t mult = computeMultiplier(le.data.type());
        if (acc.data.account().numSubEntries < mult)
        {
            throw std::runtime_error("invalid account state");
        }
    }
}

void
canRemoveEntryWithSponsorship(LedgerHeader const& lh, LedgerEntry const& le,
                              LedgerEntry const& sponsoringAcc,
                              LedgerEntry const* sponsoredAcc)
{
    if (lh.ledgerVersion < 14)
    {
        throw std::runtime_error("sponsorship before version 14");
    }

    uint32_t mult = computeMultiplier(le.data.type());
    if (getNumSponsoring(sponsoringAcc) < mult)
    {
        throw std::runtime_error("invalid sponsoring account state");
    }

    if (sponsoredAcc && (sponsoredAcc->data.account().numSubEntries < mult ||
                         getNumSponsored(*sponsoredAcc) < mult))
    {
        throw std::runtime_error("invalid sponsored account state");
    }
}

SponsorshipResult
canCreateSignerWithoutSponsorship(LedgerHeader const& lh,
                                  LedgerEntry const& acc)
{
    if (lh.ledgerVersion >= FIRST_PROTOCOL_SUPPORTING_OPERATION_LIMITS &&
        acc.data.account().numSubEntries >= ACCOUNT_SUBENTRY_LIMIT)
    {
        return SponsorshipResult::TOO_MANY_SUBENTRIES;
    }

    // TODO(jonjove): Compare against getMinBalance
    if (getAvailableBalance(lh, acc) < lh.baseReserve)
    {
        return SponsorshipResult::LOW_RESERVE;
    }

    return SponsorshipResult::SUCCESS;
}

SponsorshipResult
canCreateSignerWithSponsorship(
    LedgerHeader const& lh, std::vector<Signer>::const_iterator const& signerIt,
    LedgerEntry const& sponsoringAcc, LedgerEntry const& sponsoredAcc)
{
    if (lh.ledgerVersion < 14)
    {
        throw std::runtime_error("sponsorship before version 14");
    }

    auto const& acc = sponsoredAcc.data.account();
    if (acc.numSubEntries >= ACCOUNT_SUBENTRY_LIMIT)
    {
        return SponsorshipResult::TOO_MANY_SUBENTRIES;
    }

    return canEstablishSignerSponsorship(lh, signerIt, sponsoringAcc,
                                         sponsoredAcc);
}

// TODO(jonjove): Use signerIt or remove it
void
canRemoveSignerWithoutSponsorship(
    LedgerHeader const& lh, std::vector<Signer>::const_iterator const& signerIt,
    LedgerEntry const& acc)
{
    if (acc.data.account().numSubEntries < 1)
    {
        throw std::runtime_error("invalid account state");
    }
}

// TODO(jonjove): Use signerIt or remove it
void
canRemoveSignerWithSponsorship(
    LedgerHeader const& lh, std::vector<Signer>::const_iterator const& signerIt,
    LedgerEntry const& sponsoringAcc, LedgerEntry const& sponsoredAcc)
{
    if (lh.ledgerVersion < 14)
    {
        throw std::runtime_error("sponsorship before version 14");
    }

    if (getNumSponsoring(sponsoringAcc) < 1)
    {
        throw std::runtime_error("invalid sponsoring account state");
    }

    if (sponsoredAcc.data.account().numSubEntries < 1 ||
        getNumSponsored(sponsoredAcc) < 1)
    {
        throw std::runtime_error("invalid sponsored account state");
    }
}

////////////////////////////////////////////////////////////////////////////////
//
// Utility functions to create/remove with/without sponsorship
//
////////////////////////////////////////////////////////////////////////////////
void
createEntryWithoutSponsorship(LedgerEntry& le, LedgerEntry& acc)
{
    // TODO(jonjove): Handle ClaimableBalance
    if (le.data.type() != ACCOUNT)
    {
        ++acc.data.account().numSubEntries;
    }
}

void
createEntryWithSponsorship(LedgerEntry& le, LedgerEntry& sponsoringAcc,
                           LedgerEntry* sponsoredAcc)
{
    if (sponsoredAcc)
    {
        createEntryWithoutSponsorship(le, *sponsoredAcc);
    }
    establishEntrySponsorship(le, sponsoringAcc, sponsoredAcc);
}

void
removeEntryWithoutSponsorship(LedgerEntry& le, LedgerEntry& acc)
{
    if (le.data.type() != ACCOUNT)
    {
        --acc.data.account().numSubEntries;
    }
}

void
removeEntryWithSponsorship(LedgerEntry& le, LedgerEntry& sponsoringAcc,
                           LedgerEntry* sponsoredAcc)
{
    if (sponsoredAcc)
    {
        removeEntryWithoutSponsorship(le, *sponsoredAcc);
    }
    removeEntrySponsorship(le, sponsoringAcc, sponsoredAcc);
}

void
createSignerWithoutSponsorship(
    std::vector<Signer>::const_iterator const& signerIt, LedgerEntry& acc)
{
    auto& ae = acc.data.account();
    ++ae.numSubEntries;
    if (ae.ext.v() == 1 && ae.ext.v1().ext.v() == 2)
    {
        size_t n = signerIt - ae.signers.begin();
        auto& extV2 = ae.ext.v1().ext.v2();
        extV2.signerSponsoringIDs.insert(extV2.signerSponsoringIDs.begin() + n,
                                         {});
    }
}

void
createSignerWithSponsorship(std::vector<Signer>::const_iterator const& signerIt,
                            LedgerEntry& sponsoringAcc,
                            LedgerEntry& sponsoredAcc)
{
    createSignerWithoutSponsorship(signerIt, sponsoredAcc);
    establishSignerSponsorship(signerIt, sponsoringAcc, sponsoredAcc);
}

void
removeSignerWithoutSponsorship(
    std::vector<Signer>::const_iterator const& signerIt, LedgerEntry& acc)
{
    auto& ae = acc.data.account();
    --ae.numSubEntries;
    if (ae.ext.v() == 1 && ae.ext.v1().ext.v() == 2)
    {
        size_t n = signerIt - ae.signers.begin();
        auto& extV2 = ae.ext.v1().ext.v2();
        extV2.signerSponsoringIDs.erase(extV2.signerSponsoringIDs.begin() + n);
    }
    ae.signers.erase(signerIt);
}

void
removeSignerWithSponsorship(std::vector<Signer>::const_iterator const& signerIt,
                            LedgerEntry& sponsoringAcc,
                            LedgerEntry& sponsoredAcc)
{
    removeSignerSponsorship(signerIt, sponsoringAcc, sponsoredAcc);
    removeSignerWithoutSponsorship(signerIt, sponsoredAcc);
}

////////////////////////////////////////////////////////////////////////////////
//
// Utility functions to handle possible sponsorship
//
////////////////////////////////////////////////////////////////////////////////
SponsorshipResult
createEntryWithPossibleSponsorship(AbstractLedgerTxn& ltx,
                                   LedgerTxnHeader const& header,
                                   LedgerEntry& le, LedgerTxnEntry& acc)
{
    SponsorshipResult res;

    LedgerEntry* sponsored = &le;
    if (le.data.type() != ACCOUNT)
    {
        sponsored = &acc.current();
    }

    auto sponsorship =
        loadSponsorship(ltx, sponsored->data.account().accountID);
    if (sponsorship)
    {
        auto const& se = sponsorship.currentGeneralized().sponsorshipEntry();
        auto sponsoringAcc = loadAccount(ltx, se.sponsoringID);

        res = canCreateEntryWithSponsorship(header.current(), le,
                                            sponsoringAcc.current(), sponsored);
        if (res == SponsorshipResult::SUCCESS)
        {
            createEntryWithSponsorship(le, sponsoringAcc.current(), sponsored);
        }
    }
    else
    {
        res =
            canCreateEntryWithoutSponsorship(header.current(), le, *sponsored);
        if (res == SponsorshipResult::SUCCESS)
        {
            createEntryWithoutSponsorship(le, *sponsored);
        }
    }

    return res;
}

void
removeEntryWithPossibleSponsorship(AbstractLedgerTxn& ltx,
                                   LedgerTxnHeader const& header,
                                   LedgerEntry& le, LedgerTxnEntry& acc)
{
    if (le.ext.v() == 1 && le.ext.v1().sponsoringID)
    {
        auto sponsoringAcc = loadAccount(ltx, *le.ext.v1().sponsoringID);

        canRemoveEntryWithSponsorship(header.current(), le,
                                      sponsoringAcc.current(), &acc.current());
        removeEntryWithSponsorship(le, sponsoringAcc.current(), &acc.current());
    }
    else
    {
        canRemoveEntryWithoutSponsorship(header.current(), le, acc.current());
        removeEntryWithoutSponsorship(le, acc.current());
    }
}

SponsorshipResult
createSignerWithPossibleSponsorship(
    AbstractLedgerTxn& ltx, LedgerTxnHeader const& header,
    std::vector<Signer>::const_iterator const& signerIt, LedgerTxnEntry& acc)
{
    SponsorshipResult res;
    auto sponsorship =
        loadSponsorship(ltx, acc.current().data.account().accountID);
    if (sponsorship)
    {
        auto const& se = sponsorship.currentGeneralized().sponsorshipEntry();
        auto sponsoringAcc = loadAccount(ltx, se.sponsoringID);

        res = canCreateSignerWithSponsorship(
            header.current(), signerIt, sponsoringAcc.current(), acc.current());
        if (res == SponsorshipResult::SUCCESS)
        {
            createSignerWithSponsorship(signerIt, sponsoringAcc.current(),
                                        acc.current());
        }
    }
    else
    {
        res =
            canCreateSignerWithoutSponsorship(header.current(), acc.current());
        if (res == SponsorshipResult::SUCCESS)
        {
            createSignerWithoutSponsorship(signerIt, acc.current());
        }
    }

    return res;
}

void
removeSignerWithPossibleSponsorship(
    AbstractLedgerTxn& ltx, LedgerTxnHeader const& header,
    std::vector<Signer>::const_iterator const& signerIt, LedgerTxnEntry& acc)
{
    AccountID const* sponsoringID = nullptr;
    auto const& ae = acc.current().data.account();
    if (ae.ext.v() == 1 && ae.ext.v1().ext.v() == 2)
    {
        size_t n = signerIt - ae.signers.begin();
        auto const& extV2 = ae.ext.v1().ext.v2();
        sponsoringID = extV2.signerSponsoringIDs.at(n).get();
    }

    if (sponsoringID)
    {
        auto sponsoringAcc = loadAccount(ltx, *sponsoringID);

        canRemoveSignerWithSponsorship(header.current(), signerIt,
                                       sponsoringAcc.current(), acc.current());
        removeSignerWithSponsorship(signerIt, sponsoringAcc.current(),
                                    acc.current());
    }
    else
    {
        canRemoveSignerWithoutSponsorship(header.current(), signerIt,
                                          acc.current());
        removeSignerWithoutSponsorship(signerIt, acc.current());
    }
}
}
