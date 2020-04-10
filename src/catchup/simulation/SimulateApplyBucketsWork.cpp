// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "SimulateApplyBucketsWork.h"
#include "catchup/ApplyBucketsWork.h"
#include "catchup/simulation/GenerateBucketsWork.h"
#include "historywork/DownloadBucketsWork.h"
#include "historywork/GetHistoryArchiveStateWork.h"
#include "ledger/LedgerManager.h"
#include "ledger/LedgerTxn.h"

namespace stellar
{
SimulateApplyBucketsWork::SimulateApplyBucketsWork(
    Application& app, uint32_t multiplier, uint32_t ledger,
    TmpDir const& tmpDir, std::shared_ptr<HistoryArchiveState> has)
    : Work(app, "simulate-buckets", BasicWork::RETRY_NEVER)
    , mApp(app)
    , mHAS(has)
    , mTmpDir(tmpDir)
    , mMultiplier(multiplier)
    , mLedger(ledger)
{
}

BasicWork::State
SimulateApplyBucketsWork::doWork()
{
    if (mApplyBuckets)
    {
        return mApplyBuckets->getState();
    }

    auto done = mDownloadGenerateBuckets &&
                mDownloadGenerateBuckets->getState() == State::WORK_SUCCESS;
    if (done || mHAS)
    {
        // Step 3: Apply artificially created bucketlist
        auto const& has =
            mHAS ? *mHAS : mGenerateBucketsWork->getGeneratedHAS();
        mApplyBuckets = addWork<ApplyBucketsWork>(
            mGeneratedBuckets, has, Config::CURRENT_LEDGER_PROTOCOL_VERSION);
        return State::WORK_RUNNING;
    }
    else if (mDownloadGenerateBuckets)
    {
        return mDownloadGenerateBuckets->getState();
    }

    if (mGetState)
    {
        if (mGetState->getState() == State::WORK_SUCCESS)
        {
            auto has = mGetState->getHistoryArchiveState();
            auto bucketHashes = has.differingBuckets(
                mApp.getLedgerManager().getLastClosedLedgerHAS());

            auto downloadBuckets = std::make_shared<DownloadBucketsWork>(
                mApp, mCurrentBuckets, bucketHashes, mTmpDir);
            mGenerateBucketsWork = std::make_shared<GenerateBucketsWork>(
                mApp, mGeneratedBuckets, has, mMultiplier);

            std::vector<std::shared_ptr<BasicWork>> seq{downloadBuckets,
                                                        mGenerateBucketsWork};

            // Step 2: download current buckets, and generate the new bucketlist
            mDownloadGenerateBuckets =
                addWork<WorkSequence>("download-simulate-buckets", seq);
        }
        else
        {
            return mGetState->getState();
        }
    }

    // Step 1: retrieve current HAS
    mGetState = addWork<GetHistoryArchiveStateWork>(mLedger);
    return State::WORK_RUNNING;
}

void
SimulateApplyBucketsWork::doReset()
{
    mGetState.reset();
    mGenerateBucketsWork.reset();
    mDownloadGenerateBuckets.reset();
    mApplyBuckets.reset();
    mCurrentBuckets.clear();
    mGeneratedBuckets.clear();
}
}
