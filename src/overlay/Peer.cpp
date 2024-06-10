// Copyright 2014 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "overlay/Peer.h"

#include "BanManager.h"
#include "crypto/CryptoError.h"
#include "crypto/Hex.h"
#include "crypto/KeyUtils.h"
#include "crypto/Random.h"
#include "crypto/SHA.h"
#include "database/Database.h"
#include "herder/Herder.h"
#include "herder/TxSetFrame.h"
#include "ledger/LedgerManager.h"
#include "main/Application.h"
#include "main/Config.h"
#include "overlay/FlowControl.h"
#include "overlay/OverlayManager.h"
#include "overlay/OverlayMetrics.h"
#include "overlay/PeerAuth.h"
#include "overlay/PeerManager.h"
#include "overlay/SurveyManager.h"
#include "overlay/TxAdverts.h"
#include "util/GlobalChecks.h"
#include "util/Logging.h"
#include "util/ProtocolVersion.h"
#include "util/finally.h"

#include "herder/HerderUtils.h"
#include "medida/meter.h"
#include "medida/timer.h"
#include "xdrpp/marshal.h"
#include <fmt/format.h>

#include <Tracy.hpp>
#include <soci.h>
#include <time.h>

// LATER: need to add some way of docking peers that are misbehaving by sending
// you bad data

namespace stellar
{

using namespace std;
using namespace soci;

static constexpr VirtualClock::time_point PING_NOT_SENT =
    VirtualClock::time_point::min();

Peer::Peer(Application& app, PeerRole role)
    : mAppConnector(app)
    , mNetworkID(app.getNetworkID())
    , mFlowControl(std::make_shared<FlowControl>(mAppConnector))
    , mLastRead(app.getClock().now())
    , mLastWrite(app.getClock().now())
    , mEnqueueTimeOfLastWrite(app.getClock().now())
    , mRole(role)
    , mOverlayMetrics(app.getOverlayManager().getOverlayMetrics())
    , mPeerMetrics(app.getClock().now())
    , mRecurringTimer(app)
    , mState(role == WE_CALLED_REMOTE ? CONNECTING : CONNECTED)
    , mRemoteOverlayMinVersion(0)
    , mRemoteOverlayVersion(std::nullopt)
    , mCreationTime(app.getClock().now())
    , mDelayedExecutionTimer(app)
    , mTxAdverts(std::make_shared<TxAdverts>(app))
{
    mPingSentTime = PING_NOT_SENT;
    mLastPing = std::chrono::hours(24); // some default very high value
    auto bytes = randomBytes(mSendNonce.size());
    std::copy(bytes.begin(), bytes.end(), mSendNonce.begin());
}

Peer::MsgCapacityTracker::MsgCapacityTracker(std::weak_ptr<Peer> peer,
                                             StellarMessage const& msg)
    : mWeakPeer(peer), mMsg(msg)
{
    auto self = mWeakPeer.lock();
    if (!self)
    {
        throw std::runtime_error("Invalid peer");
    }
    self->beginMessageProcessing(mMsg);
}

Peer::MsgCapacityTracker::~MsgCapacityTracker()
{
    auto self = mWeakPeer.lock();
    if (self)
    {
        self->endMessageProcessing(mMsg);
    }
}

StellarMessage const&
Peer::MsgCapacityTracker::getMessage()
{
    return mMsg;
}

std::weak_ptr<Peer>
Peer::MsgCapacityTracker::getPeer()
{
    return mWeakPeer;
}

void
Peer::sendHello()
{
    ZoneScoped;
    CLOG_DEBUG(Overlay, "Peer::sendHello to {}", toString());
    StellarMessage msg;
    msg.type(HELLO);
    Hello& elo = msg.hello();
    elo.ledgerVersion = mAppConnector.getConfig().LEDGER_PROTOCOL_VERSION;
    elo.overlayMinVersion =
        mAppConnector.getConfig().OVERLAY_PROTOCOL_MIN_VERSION;
    elo.overlayVersion = mAppConnector.getConfig().OVERLAY_PROTOCOL_VERSION;
    elo.versionStr = mAppConnector.getConfig().VERSION_STR;
    elo.networkID = mNetworkID;
    elo.listeningPort = mAppConnector.getConfig().PEER_PORT;
    elo.peerID = mAppConnector.getConfig().NODE_SEED.getPublicKey();
    elo.cert = this->getAuthCert();
    elo.nonce = mSendNonce;

    auto msgPtr = std::make_shared<StellarMessage const>(msg);
    sendMessage(msgPtr);
}

void
Peer::beginMessageProcessing(StellarMessage const& msg)
{
    releaseAssert(mFlowControl);
    auto success = mFlowControl->beginMessageProcessing(msg);
    if (!success)
    {
        drop("unexpected flood message, peer at capacity",
             Peer::DropDirection::WE_DROPPED_REMOTE,
             Peer::DropMode::IGNORE_WRITE_QUEUE);
    }
}

void
Peer::endMessageProcessing(StellarMessage const& msg)
{
    if (shouldAbort())
    {
        return;
    }

    releaseAssert(threadIsMain());
    releaseAssert(mFlowControl);

    // We may release reading capacity, which gets taken by the background
    // thread immediately, so we can't assert `canRead` here
    auto res = mFlowControl->endMessageProcessing(msg);
    if (res.second)
    {
        sendSendMore(static_cast<uint32>(res.first),
                     static_cast<uint32>(*res.second));
    }
    else if (res.first > 0)
    {
        sendSendMore(static_cast<uint32>(res.first));
    }

    // Now that we've released some capacity, maybe schedule more reads
    if (mFlowControl->stopThrottling())
    {
        scheduleRead();
    }
}

AuthCert
Peer::getAuthCert()
{
    return mAppConnector.getOverlayManager().getPeerAuth().getAuthCert();
}

std::chrono::seconds
Peer::getIOTimeout() const
{
    if (isAuthenticated())
    {
        // Normally willing to wait 30s to hear anything
        // from an authenticated peer.
        return std::chrono::seconds(mAppConnector.getConfig().PEER_TIMEOUT);
    }
    else
    {
        // We give peers much less timing leeway while
        // performing handshake.
        return std::chrono::seconds(
            mAppConnector.getConfig().PEER_AUTHENTICATION_TIMEOUT);
    }
}

void
Peer::receivedBytes(size_t byteCount, bool gotFullMessage)
{
    mLastRead = mAppConnector.now();
    if (gotFullMessage)
    {
        mOverlayMetrics.mMessageRead.Mark();
        ++mPeerMetrics.mMessageRead;
    }
    mOverlayMetrics.mByteRead.Mark(byteCount);
    mPeerMetrics.mByteRead += byteCount;
}

void
Peer::startRecurrentTimer()
{
    constexpr std::chrono::seconds RECURRENT_TIMER_PERIOD(5);

    if (shouldAbort())
    {
        return;
    }

    pingPeer();

    auto self = shared_from_this();
    mRecurringTimer.expires_from_now(RECURRENT_TIMER_PERIOD);
    mRecurringTimer.async_wait([self](asio::error_code const& error) {
        self->recurrentTimerExpired(error);
    });
}

void
Peer::initialize(PeerBareAddress const& address)
{
    mAddress = address;
    startRecurrentTimer();
}

void
Peer::recurrentTimerExpired(asio::error_code const& error)
{
    if (!error)
    {
        auto now = mAppConnector.now();
        auto timeout = getIOTimeout();
        auto stragglerTimeout = std::chrono::seconds(
            mAppConnector.getConfig().PEER_STRAGGLER_TIMEOUT);
        if (((now - mLastRead) >= timeout) && ((now - mLastWrite) >= timeout))
        {
            mOverlayMetrics.mTimeoutIdle.Mark();
            drop("idle timeout", Peer::DropDirection::WE_DROPPED_REMOTE,
                 Peer::DropMode::IGNORE_WRITE_QUEUE);
        }
        else if (mFlowControl && mFlowControl->getOutboundCapacityTimestamp() &&
                 (now - *(mFlowControl->getOutboundCapacityTimestamp())) >=
                     Peer::PEER_SEND_MODE_IDLE_TIMEOUT)
        {
            drop("idle timeout (no new flood requests)",
                 Peer::DropDirection::WE_DROPPED_REMOTE,
                 Peer::DropMode::IGNORE_WRITE_QUEUE);
        }
        else if (((now - mEnqueueTimeOfLastWrite) >= stragglerTimeout))
        {
            mOverlayMetrics.mTimeoutStraggler.Mark();
            drop("straggling (cannot keep up)",
                 Peer::DropDirection::WE_DROPPED_REMOTE,
                 Peer::DropMode::IGNORE_WRITE_QUEUE);
        }
        else
        {
            startRecurrentTimer();
        }
    }
}

void
Peer::startExecutionDelayedTimer(
    VirtualClock::duration d, std::function<void()> const& onSuccess,
    std::function<void(asio::error_code)> const& onFailure)
{
    mDelayedExecutionTimer.expires_from_now(d);
    mDelayedExecutionTimer.async_wait(onSuccess, onFailure);
}

Json::Value
Peer::getJsonInfo(bool compact) const
{
    Json::Value res;
    res["address"] = mAddress.toString();
    res["elapsed"] = (int)getLifeTime().count();
    res["latency"] = (int)getPing().count();
    res["ver"] = getRemoteVersion();
    if (getRemoteOverlayVersion())
    {
        res["olver"] = getRemoteOverlayVersion().value();
    }
    if (mFlowControl)
    {
        res["flow_control"] = mFlowControl->getFlowControlJsonInfo(compact);
    }
    if (!compact)
    {
        res["pull_mode"]["advert_delay"] = static_cast<Json::UInt64>(
            mPeerMetrics.mAdvertQueueDelay.GetSnapshot().get75thPercentile());
        res["pull_mode"]["pull_latency"] = static_cast<Json::UInt64>(
            mPeerMetrics.mPullLatency.GetSnapshot().get75thPercentile());
        res["pull_mode"]["demand_timeouts"] =
            static_cast<Json::UInt64>(mPeerMetrics.mDemandTimeouts);
        res["message_read"] =
            static_cast<Json::UInt64>(mPeerMetrics.mMessageRead);
        res["message_write"] =
            static_cast<Json::UInt64>(mPeerMetrics.mMessageWrite);
        res["byte_read"] = static_cast<Json::UInt64>(mPeerMetrics.mByteRead);
        res["byte_write"] = static_cast<Json::UInt64>(mPeerMetrics.mByteWrite);

        res["async_read"] = static_cast<Json::UInt64>(mPeerMetrics.mAsyncRead);
        res["async_write"] =
            static_cast<Json::UInt64>(mPeerMetrics.mAsyncWrite);

        res["message_drop"] =
            static_cast<Json::UInt64>(mPeerMetrics.mMessageDrop);

        res["message_delay_in_write_queue_p75"] = static_cast<Json::UInt64>(
            mPeerMetrics.mMessageDelayInWriteQueueTimer.GetSnapshot()
                .get75thPercentile());
        res["message_delay_in_async_write_p75"] = static_cast<Json::UInt64>(
            mPeerMetrics.mMessageDelayInAsyncWriteTimer.GetSnapshot()
                .get75thPercentile());

        res["unique_flood_message_recv"] =
            static_cast<Json::UInt64>(mPeerMetrics.mUniqueFloodMessageRecv);
        res["duplicate_flood_message_recv"] =
            static_cast<Json::UInt64>(mPeerMetrics.mDuplicateFloodMessageRecv);
        res["unique_fetch_message_recv"] =
            static_cast<Json::UInt64>(mPeerMetrics.mUniqueFetchMessageRecv);
        res["duplicate_fetch_message_recv"] =
            static_cast<Json::UInt64>(mPeerMetrics.mDuplicateFetchMessageRecv);
    }

    return res;
}

void
Peer::sendAuth()
{
    ZoneScoped;
    StellarMessage msg;
    msg.type(AUTH);
    if (mAppConnector.getConfig().ENABLE_FLOW_CONTROL_BYTES)
    {
        msg.auth().flags = AUTH_MSG_FLAG_FLOW_CONTROL_BYTES_REQUESTED;
    }
    auto msgPtr = std::make_shared<StellarMessage const>(msg);
    sendMessage(msgPtr);
}

std::string const&
Peer::toString()
{
    return mAddress.toString();
}

void
Peer::shutdown()
{
    if (mShuttingDown)
    {
        return;
    }
    mShuttingDown = true;
    mRecurringTimer.cancel();
    mDelayedExecutionTimer.cancel();
    if (mTxAdverts)
    {
        mTxAdverts->shutdown();
    }
}

void
Peer::clearBelow(uint32_t seq)
{
    if (mTxAdverts)
    {
        mTxAdverts->clearBelow(seq);
    }
}

void
Peer::connectHandler(asio::error_code const& error)
{
    if (error)
    {
        drop("unable to connect: " + error.message(),
             Peer::DropDirection::WE_DROPPED_REMOTE,
             Peer::DropMode::IGNORE_WRITE_QUEUE);
    }
    else
    {
        CLOG_DEBUG(Overlay, "Async connect to {}", toString());
        connected();
        setState(CONNECTED);
        sendHello();
    }
}

void
Peer::sendDontHave(MessageType type, uint256 const& itemID)
{
    ZoneScoped;
    StellarMessage msg;
    msg.type(DONT_HAVE);
    msg.dontHave().reqHash = itemID;
    msg.dontHave().type = type;
    auto msgPtr = std::make_shared<StellarMessage const>(msg);
    sendMessage(msgPtr);
}

void
Peer::sendSCPQuorumSet(SCPQuorumSetPtr qSet)
{
    ZoneScoped;
    StellarMessage msg;
    msg.type(SCP_QUORUMSET);
    msg.qSet() = *qSet;
    auto msgPtr = std::make_shared<StellarMessage const>(msg);
    sendMessage(msgPtr);
}

void
Peer::sendGetTxSet(uint256 const& setID)
{
    ZoneScoped;
    StellarMessage newMsg;
    newMsg.type(GET_TX_SET);
    newMsg.txSetHash() = setID;

    auto msgPtr = std::make_shared<StellarMessage const>(newMsg);
    sendMessage(msgPtr);
}

void
Peer::sendGetQuorumSet(uint256 const& setID)
{
    ZoneScoped;
    StellarMessage newMsg;
    newMsg.type(GET_SCP_QUORUMSET);
    newMsg.qSetHash() = setID;

    auto msgPtr = std::make_shared<StellarMessage const>(newMsg);
    sendMessage(msgPtr);
}

void
Peer::sendGetPeers()
{
    ZoneScoped;
    StellarMessage newMsg;
    newMsg.type(GET_PEERS);
    auto msgPtr = std::make_shared<StellarMessage const>(newMsg);
    sendMessage(msgPtr);
}

void
Peer::sendGetScpState(uint32 ledgerSeq)
{
    ZoneScoped;
    StellarMessage newMsg;
    newMsg.type(GET_SCP_STATE);
    newMsg.getSCPLedgerSeq() = ledgerSeq;
    auto msgPtr = std::make_shared<StellarMessage const>(newMsg);
    sendMessage(msgPtr);
}

void
Peer::sendPeers()
{
    ZoneScoped;
    StellarMessage newMsg;
    newMsg.type(PEERS);
    uint32 maxPeerCount = std::min<uint32>(50, newMsg.peers().max_size());

    // send top peers we know about
    auto peers =
        mAppConnector.getOverlayManager().getPeerManager().getPeersToSend(
            maxPeerCount, mAddress);
    releaseAssert(peers.size() <= maxPeerCount);

    if (!peers.empty())
    {
        newMsg.peers().reserve(peers.size());
        for (auto const& address : peers)
        {
            newMsg.peers().push_back(toXdr(address));
        }
        auto msgPtr = std::make_shared<StellarMessage const>(newMsg);
        sendMessage(msgPtr);
    }
}

void
Peer::sendError(ErrorCode error, std::string const& message)
{
    ZoneScoped;
    StellarMessage m;
    m.type(ERROR_MSG);
    m.error().code = error;
    m.error().msg = message;
    auto msgPtr = std::make_shared<StellarMessage const>(m);
    sendMessage(msgPtr);
}

void
Peer::sendErrorAndDrop(ErrorCode error, std::string const& message,
                       DropMode dropMode)
{
    ZoneScoped;
    sendError(error, message);
    drop(message, DropDirection::WE_DROPPED_REMOTE, dropMode);
}

void
Peer::sendSendMore(uint32_t numMessages)
{
    ZoneScoped;
    releaseAssert(threadIsMain());

    auto m = std::make_shared<StellarMessage>();
    m->type(SEND_MORE);
    m->sendMoreMessage().numMessages = numMessages;
    sendMessage(m);
}

void
Peer::sendSendMore(uint32_t numMessages, uint32_t numBytes)
{
    ZoneScoped;
    releaseAssert(threadIsMain());

    auto m = std::make_shared<StellarMessage>();
    m->type(SEND_MORE_EXTENDED);
    m->sendMoreExtendedMessage().numMessages = numMessages;
    m->sendMoreExtendedMessage().numBytes = numBytes;
    sendMessage(m);
}

std::string
Peer::msgSummary(StellarMessage const& msg)
{
    switch (msg.type())
    {
    case ERROR_MSG:
        return "ERROR";
    case HELLO:
        return "HELLO";
    case AUTH:
        return "AUTH";
    case DONT_HAVE:
        return fmt::format(FMT_STRING("DONTHAVE {}:{}"), msg.dontHave().type,
                           hexAbbrev(msg.dontHave().reqHash));
    case GET_PEERS:
        return "GETPEERS";
    case PEERS:
        return fmt::format(FMT_STRING("PEERS {:d}"), msg.peers().size());

    case GET_TX_SET:
        return fmt::format(FMT_STRING("GETTXSET {}"),
                           hexAbbrev(msg.txSetHash()));
    case TX_SET:
    case GENERALIZED_TX_SET:
        return "TXSET";

    case TRANSACTION:
        return "TRANSACTION";

    case GET_SCP_QUORUMSET:
        return fmt::format(FMT_STRING("GET_SCP_QSET {}"),
                           hexAbbrev(msg.qSetHash()));
    case SCP_QUORUMSET:
        return "SCP_QSET";
    case SCP_MESSAGE:
    {
        std::string t;
        switch (msg.envelope().statement.pledges.type())
        {
        case SCP_ST_PREPARE:
            t = "SCP::PREPARE";
            break;
        case SCP_ST_CONFIRM:
            t = "SCP::CONFIRM";
            break;
        case SCP_ST_EXTERNALIZE:
            t = "SCP::EXTERNALIZE";
            break;
        case SCP_ST_NOMINATE:
            t = "SCP::NOMINATE";
            break;
        default:
            t = "unknown";
        }
        return fmt::format(FMT_STRING("{} ({})"), t,
                           mAppConnector.getConfig().toShortString(
                               msg.envelope().statement.nodeID));
    }
    case GET_SCP_STATE:
        return fmt::format(FMT_STRING("GET_SCP_STATE {:d}"),
                           msg.getSCPLedgerSeq());

    case SURVEY_REQUEST:
    case SURVEY_RESPONSE:
        return SurveyManager::getMsgSummary(msg);
    case SEND_MORE:
        return "SENDMORE";
    case SEND_MORE_EXTENDED:
        return "SENDMORE_EXTENDED";
    case FLOOD_ADVERT:
        return "FLODADVERT";
    case FLOOD_DEMAND:
        return "FLOODDEMAND";
    }
    return "UNKNOWN";
}

void
Peer::sendMessage(std::shared_ptr<StellarMessage const> msg, bool log)
{
    ZoneScoped;
    CLOG_TRACE(Overlay, "send: {} to : {}", msgSummary(*msg),
               mAppConnector.getConfig().toShortString(mPeerID));

    switch (msg->type())
    {
    case ERROR_MSG:
        mOverlayMetrics.mSendErrorMeter.Mark();
        break;
    case HELLO:
        mOverlayMetrics.mSendHelloMeter.Mark();
        break;
    case AUTH:
        mOverlayMetrics.mSendAuthMeter.Mark();
        break;
    case DONT_HAVE:
        mOverlayMetrics.mSendDontHaveMeter.Mark();
        break;
    case GET_PEERS:
        mOverlayMetrics.mSendGetPeersMeter.Mark();
        break;
    case PEERS:
        mOverlayMetrics.mSendPeersMeter.Mark();
        break;
    case GET_TX_SET:
        mOverlayMetrics.mSendGetTxSetMeter.Mark();
        break;
    case TX_SET:
    case GENERALIZED_TX_SET:
        mOverlayMetrics.mSendTxSetMeter.Mark();
        break;
    case TRANSACTION:
        mOverlayMetrics.mSendTransactionMeter.Mark();
        break;
    case GET_SCP_QUORUMSET:
        mOverlayMetrics.mSendGetSCPQuorumSetMeter.Mark();
        break;
    case SCP_QUORUMSET:
        mOverlayMetrics.mSendSCPQuorumSetMeter.Mark();
        break;
    case SCP_MESSAGE:
        mOverlayMetrics.mSendSCPMessageSetMeter.Mark();
        break;
    case GET_SCP_STATE:
        mOverlayMetrics.mSendGetSCPStateMeter.Mark();
        break;
    case SURVEY_REQUEST:
        mOverlayMetrics.mSendSurveyRequestMeter.Mark();
        break;
    case SURVEY_RESPONSE:
        mOverlayMetrics.mSendSurveyResponseMeter.Mark();
        break;
    case SEND_MORE:
    case SEND_MORE_EXTENDED:
        mOverlayMetrics.mSendSendMoreMeter.Mark();
        break;
    case FLOOD_ADVERT:
        mOverlayMetrics.mSendFloodAdvertMeter.Mark();
        break;
    case FLOOD_DEMAND:
        mOverlayMetrics.mSendFloodDemandMeter.Mark();
        break;
    };

    releaseAssert(mFlowControl);
    if (!mFlowControl->maybeSendMessage(msg))
    {
        // Outgoing message is not flow-controlled, send it directly
        sendAuthenticatedMessage(msg);
    }
}

void
Peer::sendAuthenticatedMessage(std::shared_ptr<StellarMessage const> msg)
{
    AuthenticatedMessage amsg;
    amsg.v0().message = *msg;
    if (msg->type() != HELLO && msg->type() != ERROR_MSG)
    {
        ZoneNamedN(hmacZone, "message HMAC", true);
        amsg.v0().sequence = mSendMacSeq;
        amsg.v0().mac =
            hmacSha256(mSendMacKey, xdr::xdr_to_opaque(mSendMacSeq, *msg));
        ++mSendMacSeq;
    }
    xdr::msg_ptr xdrBytes;
    {
        ZoneNamedN(xdrZone, "XDR serialize", true);
        xdrBytes = xdr::xdr_to_msg(amsg);
    }
    this->sendMessage(std::move(xdrBytes));
}

bool
Peer::isConnected() const
{
    return mState != CONNECTING && mState != CLOSING;
}

bool
Peer::isAuthenticated() const
{
    return mState == GOT_AUTH;
}

std::chrono::seconds
Peer::getLifeTime() const
{
    return std::chrono::duration_cast<std::chrono::seconds>(
        mAppConnector.now() - mCreationTime);
}

bool
Peer::shouldAbort() const
{
    return mState == CLOSING || mAppConnector.overlayShuttingDown();
}

void
Peer::recvAuthenticatedMessage(AuthenticatedMessage&& msg)
{
    ZoneScoped;
    if (shouldAbort())
    {
        return;
    }

    if (mState >= GOT_HELLO && msg.v0().message.type() != ERROR_MSG)
    {
        if (msg.v0().sequence != mRecvMacSeq)
        {
            ++mRecvMacSeq;
            sendErrorAndDrop(ERR_AUTH, "unexpected auth sequence",
                             DropMode::IGNORE_WRITE_QUEUE);
            return;
        }

        if (!hmacSha256Verify(
                msg.v0().mac, mRecvMacKey,
                xdr::xdr_to_opaque(msg.v0().sequence, msg.v0().message)))
        {
            ++mRecvMacSeq;
            sendErrorAndDrop(ERR_AUTH, "unexpected MAC",
                             DropMode::IGNORE_WRITE_QUEUE);
            return;
        }
        ++mRecvMacSeq;
    }
    recvMessage(msg.v0().message);
}

void
Peer::recvMessage(StellarMessage const& stellarMsg)
{
    ZoneScoped;
    if (shouldAbort())
    {
        return;
    }

    char const* cat = nullptr;
    Scheduler::ActionType type = Scheduler::ActionType::NORMAL_ACTION;
    auto msgType = stellarMsg.type();
    bool ignoreIfOutOfSync = false;
    switch (msgType)
    {
    // group messages used during handshake, process those synchronously
    case HELLO:
    case AUTH:
        Peer::recvRawMessage(stellarMsg);
        return;
    // control messages
    case GET_PEERS:
    case PEERS:
    case ERROR_MSG:
    case SEND_MORE:
    case SEND_MORE_EXTENDED:
        cat = "CTRL";
        break;
    // high volume flooding
    case TRANSACTION:
    case FLOOD_ADVERT:
    case FLOOD_DEMAND:
    {
        cat = "TX";
        type = Scheduler::ActionType::DROPPABLE_ACTION;
        ignoreIfOutOfSync = true;
        break;
    }

    // consensus, inbound
    case GET_TX_SET:
    case GET_SCP_QUORUMSET:
    case GET_SCP_STATE:
        cat = "SCPQ";
        type = Scheduler::ActionType::DROPPABLE_ACTION;
        break;

    // consensus, self
    case DONT_HAVE:
    case TX_SET:
    case GENERALIZED_TX_SET:
    case SCP_QUORUMSET:
    case SCP_MESSAGE:
        cat = "SCP";
        break;

    default:
        cat = "MISC";
    }

    std::weak_ptr<Peer> weak = shared_from_this();
    auto msgTracker = std::make_shared<MsgCapacityTracker>(weak, stellarMsg);

    if (!mAppConnector.getLedgerManager().isSynced() && ignoreIfOutOfSync)
    {
        // For transactions, exit early during the state rebuild, as we
        // can't properly verify them
        return;
    }

    mAppConnector.postOnMainThread(
        [msgTracker, cat, port = mAppConnector.getConfig().PEER_PORT]() {
            auto self = msgTracker->getPeer().lock();
            if (!self)
            {
                CLOG_TRACE(Overlay, "Error RecvMessage T:{} cat:{}",
                           msgTracker->getMessage().type(), cat);
                return;
            }

            try
            {
                self->recvRawMessage(msgTracker->getMessage());
            }
            catch (CryptoError const& e)
            {
                std::string err = fmt::format(
                    FMT_STRING("Error RecvMessage T:{} cat:{} {} @{:d}"),
                    msgTracker->getMessage().type(), cat, self->toString(),
                    port);
                CLOG_ERROR(Overlay, "Dropping connection with {}: {}", err,
                           e.what());
                self->drop("Bad crypto request",
                           Peer::DropDirection::WE_DROPPED_REMOTE,
                           Peer::DropMode::IGNORE_WRITE_QUEUE);
            }
        },
        fmt::format(FMT_STRING("{} recvMessage"), cat), type);
}

void
Peer::recvSendMore(StellarMessage const& msg)
{
    releaseAssert(mFlowControl);
    mFlowControl->maybeReleaseCapacityAndTriggerSend(msg);
}

void
Peer::recvRawMessage(StellarMessage const& stellarMsg)
{
    ZoneScoped;
    releaseAssert(threadIsMain());

    auto peerStr = toString();
    ZoneText(peerStr.c_str(), peerStr.size());

    if (shouldAbort())
    {
        return;
    }

    if (!isAuthenticated() && (stellarMsg.type() != HELLO) &&
        (stellarMsg.type() != AUTH) && (stellarMsg.type() != ERROR_MSG))
    {
        drop(fmt::format(FMT_STRING("received {} before completed handshake"),
                         stellarMsg.type()),
             Peer::DropDirection::WE_DROPPED_REMOTE,
             Peer::DropMode::IGNORE_WRITE_QUEUE);
        return;
    }

    releaseAssert(isAuthenticated() || stellarMsg.type() == HELLO ||
                  stellarMsg.type() == AUTH || stellarMsg.type() == ERROR_MSG);
    mAppConnector.getOverlayManager().recordMessageMetric(stellarMsg,
                                                          shared_from_this());

    switch (stellarMsg.type())
    {
    case ERROR_MSG:
    {
        auto t = mOverlayMetrics.mRecvErrorTimer.TimeScope();
        recvError(stellarMsg);
    }
    break;

    case HELLO:
    {
        auto t = mOverlayMetrics.mRecvHelloTimer.TimeScope();
        this->recvHello(stellarMsg.hello());
    }
    break;

    case AUTH:
    {
        auto t = mOverlayMetrics.mRecvAuthTimer.TimeScope();
        this->recvAuth(stellarMsg);
    }
    break;

    case DONT_HAVE:
    {
        auto t = mOverlayMetrics.mRecvDontHaveTimer.TimeScope();
        recvDontHave(stellarMsg);
    }
    break;

    case GET_PEERS:
    {
        auto t = mOverlayMetrics.mRecvGetPeersTimer.TimeScope();
        recvGetPeers(stellarMsg);
    }
    break;

    case PEERS:
    {
        auto t = mOverlayMetrics.mRecvPeersTimer.TimeScope();
        recvPeers(stellarMsg);
    }
    break;

    case SURVEY_REQUEST:
    {
        auto t = mOverlayMetrics.mRecvSurveyRequestTimer.TimeScope();
        recvSurveyRequestMessage(stellarMsg);
    }
    break;

    case SURVEY_RESPONSE:
    {
        auto t = mOverlayMetrics.mRecvSurveyResponseTimer.TimeScope();
        recvSurveyResponseMessage(stellarMsg);
    }
    break;

    case GET_TX_SET:
    {
        auto t = mOverlayMetrics.mRecvGetTxSetTimer.TimeScope();
        recvGetTxSet(stellarMsg);
    }
    break;

    case TX_SET:
    {
        auto t = mOverlayMetrics.mRecvTxSetTimer.TimeScope();
        recvTxSet(stellarMsg);
    }
    break;

    case GENERALIZED_TX_SET:
    {
        auto t = mOverlayMetrics.mRecvTxSetTimer.TimeScope();
        recvGeneralizedTxSet(stellarMsg);
    }
    break;

    case TRANSACTION:
    {
        auto t = mOverlayMetrics.mRecvTransactionTimer.TimeScope();
        recvTransaction(stellarMsg);
    }
    break;

    case GET_SCP_QUORUMSET:
    {
        auto t = mOverlayMetrics.mRecvGetSCPQuorumSetTimer.TimeScope();
        recvGetSCPQuorumSet(stellarMsg);
    }
    break;

    case SCP_QUORUMSET:
    {
        auto t = mOverlayMetrics.mRecvSCPQuorumSetTimer.TimeScope();
        recvSCPQuorumSet(stellarMsg);
    }
    break;

    case SCP_MESSAGE:
    {
        auto t = mOverlayMetrics.mRecvSCPMessageTimer.TimeScope();
        recvSCPMessage(stellarMsg);
    }
    break;

    case GET_SCP_STATE:
    {
        auto t = mOverlayMetrics.mRecvGetSCPStateTimer.TimeScope();
        recvGetSCPState(stellarMsg);
    }
    break;
    case SEND_MORE:
    case SEND_MORE_EXTENDED:
    {
        std::string errorMsg;
        releaseAssert(mFlowControl);
        if (!mFlowControl->isSendMoreValid(stellarMsg, errorMsg))
        {
            drop(errorMsg, Peer::DropDirection::WE_DROPPED_REMOTE,
                 Peer::DropMode::IGNORE_WRITE_QUEUE);
            return;
        }
        auto t = mOverlayMetrics.mRecvSendMoreTimer.TimeScope();
        recvSendMore(stellarMsg);
    }
    break;

    case FLOOD_ADVERT:
    {
        auto t = mOverlayMetrics.mRecvFloodAdvertTimer.TimeScope();
        recvFloodAdvert(stellarMsg);
    }
    break;

    case FLOOD_DEMAND:
    {
        auto t = mOverlayMetrics.mRecvFloodDemandTimer.TimeScope();
        recvFloodDemand(stellarMsg);
    }
    }
}

void
Peer::recvDontHave(StellarMessage const& msg)
{
    ZoneScoped;
    maybeProcessPingResponse(msg.dontHave().reqHash);

    mAppConnector.getHerder().peerDoesntHave(
        msg.dontHave().type, msg.dontHave().reqHash, shared_from_this());
}

void
Peer::recvGetTxSet(StellarMessage const& msg)
{
    ZoneScoped;
    auto self = shared_from_this();
    if (auto txSet = mAppConnector.getHerder().getTxSet(msg.txSetHash()))
    {
        auto newMsg = std::make_shared<StellarMessage>();
        if (txSet->isGeneralizedTxSet())
        {
            newMsg->type(GENERALIZED_TX_SET);
            txSet->toXDR(newMsg->generalizedTxSet());
        }
        else
        {
            newMsg->type(TX_SET);
            txSet->toXDR(newMsg->txSet());
        }

        self->sendMessage(newMsg);
    }
    else
    {
        // Technically we don't exactly know what is the kind of the tx set
        // missing, however both TX_SET and GENERALIZED_TX_SET get the same
        // treatment when missing, so it should be ok to maybe send the
        // incorrect version during the upgrade.
        auto messageType =
            protocolVersionIsBefore(mAppConnector.getLedgerManager()
                                        .getLastClosedLedgerHeader()
                                        .header.ledgerVersion,
                                    SOROBAN_PROTOCOL_VERSION)
                ? TX_SET
                : GENERALIZED_TX_SET;
        sendDontHave(messageType, msg.txSetHash());
    }
}

void
Peer::recvTxSet(StellarMessage const& msg)
{
    ZoneScoped;
    auto frame = TxSetXDRFrame::makeFromWire(msg.txSet());
    mAppConnector.getHerder().recvTxSet(frame->getContentsHash(), frame);
}

void
Peer::recvGeneralizedTxSet(StellarMessage const& msg)
{
    ZoneScoped;
    auto frame = TxSetXDRFrame::makeFromWire(msg.generalizedTxSet());
    mAppConnector.getHerder().recvTxSet(frame->getContentsHash(), frame);
}

void
Peer::recvTransaction(StellarMessage const& msg)
{
    ZoneScoped;
    mAppConnector.getOverlayManager().recvTransaction(msg, shared_from_this());
}

Hash
Peer::pingIDfromTimePoint(VirtualClock::time_point const& tp)
{
    auto sh = shortHash::xdrComputeHash(
        xdr::xdr_to_opaque(uint64_t(tp.time_since_epoch().count())));
    Hash res;
    releaseAssert(res.size() >= sizeof(sh));
    std::memcpy(res.data(), &sh, sizeof(sh));
    return res;
}

void
Peer::pingPeer()
{
    if (isAuthenticated() && mPingSentTime == PING_NOT_SENT)
    {
        mPingSentTime = mAppConnector.now();
        auto h = pingIDfromTimePoint(mPingSentTime);
        sendGetQuorumSet(h);
    }
}

void
Peer::maybeProcessPingResponse(Hash const& id)
{
    if (mPingSentTime != PING_NOT_SENT)
    {
        auto h = pingIDfromTimePoint(mPingSentTime);
        if (h == id)
        {
            mLastPing = std::chrono::duration_cast<std::chrono::milliseconds>(
                mAppConnector.now() - mPingSentTime);
            mPingSentTime = PING_NOT_SENT;
            CLOG_DEBUG(Overlay, "Latency {}: {} ms", toString(),
                       mLastPing.count());
            mOverlayMetrics.mConnectionLatencyTimer.Update(mLastPing);
        }
    }
}

std::chrono::milliseconds
Peer::getPing() const
{
    return mLastPing;
}

bool
Peer::canRead() const
{
    releaseAssert(mFlowControl);
    return mFlowControl->canRead();
}

void
Peer::recvGetSCPQuorumSet(StellarMessage const& msg)
{
    ZoneScoped;

    SCPQuorumSetPtr qset = mAppConnector.getHerder().getQSet(msg.qSetHash());

    if (qset)
    {
        sendSCPQuorumSet(qset);
    }
    else
    {
        CLOG_TRACE(Overlay, "No quorum set: {}", hexAbbrev(msg.qSetHash()));
        sendDontHave(SCP_QUORUMSET, msg.qSetHash());
        // do we want to ask other people for it?
    }
}
void
Peer::recvSCPQuorumSet(StellarMessage const& msg)
{
    ZoneScoped;
    Hash hash = xdrSha256(msg.qSet());
    maybeProcessPingResponse(hash);
    mAppConnector.getHerder().recvSCPQuorumSet(hash, msg.qSet());
}

void
Peer::recvSCPMessage(StellarMessage const& msg)
{
    ZoneScoped;
    SCPEnvelope const& envelope = msg.envelope();

    auto type = msg.envelope().statement.pledges.type();
    auto t = (type == SCP_ST_PREPARE
                  ? mOverlayMetrics.mRecvSCPPrepareTimer.TimeScope()
                  : (type == SCP_ST_CONFIRM
                         ? mOverlayMetrics.mRecvSCPConfirmTimer.TimeScope()
                         : (type == SCP_ST_EXTERNALIZE
                                ? mOverlayMetrics.mRecvSCPExternalizeTimer
                                      .TimeScope()
                                : (mOverlayMetrics.mRecvSCPNominateTimer
                                       .TimeScope()))));
    std::string codeStr;
    switch (type)
    {
    case SCP_ST_PREPARE:
        codeStr = "PREPARE";
        break;
    case SCP_ST_CONFIRM:
        codeStr = "CONFIRM";
        break;
    case SCP_ST_EXTERNALIZE:
        codeStr = "EXTERNALIZE";
        break;
    case SCP_ST_NOMINATE:
    default:
        codeStr = "NOMINATE";
        break;
    }
    ZoneText(codeStr.c_str(), codeStr.size());

    // add it to the floodmap so that this peer gets credit for it
    Hash msgID;
    mAppConnector.getOverlayManager().recvFloodedMsgID(msg, shared_from_this(),
                                                       msgID);

    auto res = mAppConnector.getHerder().recvSCPEnvelope(envelope);
    if (res == Herder::ENVELOPE_STATUS_DISCARDED)
    {
        // the message was discarded, remove it from the floodmap as well
        mAppConnector.getOverlayManager().forgetFloodedMsg(msgID);
    }
}

void
Peer::recvGetSCPState(StellarMessage const& msg)
{
    ZoneScoped;
    uint32 seq = msg.getSCPLedgerSeq();
    mAppConnector.getHerder().sendSCPStateToPeer(seq, shared_from_this());
}

void
Peer::recvError(StellarMessage const& msg)
{
    ZoneScoped;
    std::string codeStr = "UNKNOWN";
    switch (msg.error().code)
    {
    case ERR_MISC:
        codeStr = "ERR_MISC";
        break;
    case ERR_DATA:
        codeStr = "ERR_DATA";
        break;
    case ERR_CONF:
        codeStr = "ERR_CONF";
        break;
    case ERR_AUTH:
        codeStr = "ERR_AUTH";
        break;
    case ERR_LOAD:
        codeStr = "ERR_LOAD";
        break;
    default:
        break;
    }

    std::string msgStr;
    msgStr.reserve(msg.error().msg.size());
    std::transform(msg.error().msg.begin(), msg.error().msg.end(),
                   std::back_inserter(msgStr),
                   [](char c) { return (isalnum(c) || c == ' ') ? c : '*'; });

    drop(fmt::format(FMT_STRING("{} ({})"), codeStr, msgStr),
         Peer::DropDirection::REMOTE_DROPPED_US,
         Peer::DropMode::IGNORE_WRITE_QUEUE);
}

void
Peer::updatePeerRecordAfterEcho()
{
    releaseAssert(!getAddress().isEmpty());

    PeerType type;
    if (mAppConnector.getOverlayManager().isPreferred(this))
    {
        type = PeerType::PREFERRED;
    }
    else if (mRole == WE_CALLED_REMOTE)
    {
        type = PeerType::OUTBOUND;
    }
    else
    {
        type = PeerType::INBOUND;
    }
    // Now that we've done authentication, we know whether this peer is
    // preferred or not
    mAppConnector.getOverlayManager().getPeerManager().update(
        getAddress(), type,
        /* preferredTypeKnown */ true);
}

void
Peer::updatePeerRecordAfterAuthentication()
{
    releaseAssert(!getAddress().isEmpty());

    if (mRole == WE_CALLED_REMOTE)
    {
        mAppConnector.getOverlayManager().getPeerManager().update(
            getAddress(), PeerManager::BackOffUpdate::RESET);
    }

    CLOG_DEBUG(Overlay, "successful handshake with {}@{}",
               mAppConnector.getConfig().toShortString(mPeerID),
               getAddress().toString());
}

void
Peer::recvHello(Hello const& elo)
{
    ZoneScoped;
    if (mState >= GOT_HELLO)
    {
        drop("received unexpected HELLO",
             Peer::DropDirection::WE_DROPPED_REMOTE,
             Peer::DropMode::IGNORE_WRITE_QUEUE);
        return;
    }

    auto& peerAuth = mAppConnector.getOverlayManager().getPeerAuth();
    if (!peerAuth.verifyRemoteAuthCert(elo.peerID, elo.cert))
    {
        drop("failed to verify auth cert",
             Peer::DropDirection::WE_DROPPED_REMOTE,
             Peer::DropMode::IGNORE_WRITE_QUEUE);
        return;
    }

    if (mAppConnector.getBanManager().isBanned(elo.peerID))
    {
        drop("node is banned", Peer::DropDirection::WE_DROPPED_REMOTE,
             Peer::DropMode::IGNORE_WRITE_QUEUE);
        return;
    }

    mRemoteOverlayMinVersion = elo.overlayMinVersion;
    mRemoteOverlayVersion = elo.overlayVersion;
    mRemoteVersion = elo.versionStr;
    mPeerID = elo.peerID;
    mRecvNonce = elo.nonce;
    mSendMacSeq = 0;
    mRecvMacSeq = 0;
    mSendMacKey = peerAuth.getSendingMacKey(elo.cert.pubkey, mSendNonce,
                                            mRecvNonce, mRole);
    mRecvMacKey = peerAuth.getReceivingMacKey(elo.cert.pubkey, mSendNonce,
                                              mRecvNonce, mRole);

    setState(GOT_HELLO);

    // mAddress is set in TCPPeer::initiate and TCPPeer::accept. It should
    // contain valid IP (but not necessarily port yet)
    auto ip = mAddress.getIP();
    if (ip.empty())
    {
        drop("failed to determine remote address",
             Peer::DropDirection::WE_DROPPED_REMOTE,
             Peer::DropMode::IGNORE_WRITE_QUEUE);
        return;
    }
    mAddress =
        PeerBareAddress{ip, static_cast<unsigned short>(elo.listeningPort)};

    CLOG_DEBUG(Overlay, "recvHello from {}", toString());

    auto dropMode = Peer::DropMode::IGNORE_WRITE_QUEUE;
    if (mRole == REMOTE_CALLED_US)
    {
        // Send a HELLO back, even if it's going to be followed
        // immediately by ERROR, because ERROR is an authenticated
        // message type and the caller won't decode it right if
        // still waiting for an unauthenticated HELLO.
        sendHello();
        dropMode = Peer::DropMode::FLUSH_WRITE_QUEUE;
    }

    bool rejectBasedOnLedgerVersion = shouldDropPeerPredicate(
        shared_from_this(), mAppConnector.getLedgerManager()
                                .getLastClosedLedgerHeader()
                                .header.ledgerVersion);
    if (mRemoteOverlayMinVersion > mRemoteOverlayVersion.value() ||
        mRemoteOverlayVersion.value() <
            mAppConnector.getConfig().OVERLAY_PROTOCOL_MIN_VERSION ||
        mRemoteOverlayMinVersion >
            mAppConnector.getConfig().OVERLAY_PROTOCOL_VERSION ||
        rejectBasedOnLedgerVersion)
    {
        CLOG_DEBUG(Overlay, "Protocol = [{},{}] expected: [{},{}]",
                   mRemoteOverlayMinVersion, mRemoteOverlayVersion.value(),
                   mAppConnector.getConfig().OVERLAY_PROTOCOL_MIN_VERSION,
                   mAppConnector.getConfig().OVERLAY_PROTOCOL_VERSION);
        sendErrorAndDrop(ERR_CONF, "wrong protocol version", dropMode);
        return;
    }

    if (elo.peerID == mAppConnector.getConfig().NODE_SEED.getPublicKey())
    {
        sendErrorAndDrop(ERR_CONF, "connecting to self", dropMode);
        return;
    }

    if (elo.networkID != mNetworkID)
    {
        CLOG_WARNING(Overlay, "Connection from peer with different NetworkID");
        CLOG_WARNING(Overlay, "Check your configuration file settings: "
                              "KNOWN_PEERS and PREFERRED_PEERS for peers "
                              "that are from other networks.");
        CLOG_DEBUG(Overlay, "NetworkID = {} expected: {}",
                   hexAbbrev(elo.networkID), hexAbbrev(mNetworkID));
        sendErrorAndDrop(ERR_CONF, "wrong network passphrase", dropMode);
        return;
    }

    if (elo.listeningPort <= 0 || elo.listeningPort > UINT16_MAX || ip.empty())
    {
        sendErrorAndDrop(ERR_CONF, "bad address",
                         Peer::DropMode::IGNORE_WRITE_QUEUE);
        return;
    }

    updatePeerRecordAfterEcho();

    auto const& authenticated =
        mAppConnector.getOverlayManager().getAuthenticatedPeers();
    auto authenticatedIt = authenticated.find(mPeerID);
    // no need to self-check here as this one cannot be in authenticated yet
    if (authenticatedIt != std::end(authenticated))
    {
        if (&(authenticatedIt->second->mPeerID) != &mPeerID)
        {
            sendErrorAndDrop(
                ERR_CONF,
                "already-connected peer: " +
                    mAppConnector.getConfig().toShortString(mPeerID),
                dropMode);
            return;
        }
    }

    for (auto const& p : mAppConnector.getOverlayManager().getPendingPeers())
    {
        if (&(p->mPeerID) == &mPeerID)
        {
            continue;
        }
        if (p->getPeerID() == mPeerID)
        {
            sendErrorAndDrop(
                ERR_CONF,
                "already-connected peer: " +
                    mAppConnector.getConfig().toShortString(mPeerID),
                dropMode);
            return;
        }
    }

    if (mRole == WE_CALLED_REMOTE)
    {
        sendAuth();
    }
}

void
Peer::setState(PeerState newState)
{
    mState = newState;
}

void
Peer::recvAuth(StellarMessage const& msg)
{
    ZoneScoped;

    if (mState != GOT_HELLO)
    {
        sendErrorAndDrop(ERR_MISC, "out-of-order AUTH message",
                         DropMode::IGNORE_WRITE_QUEUE);
        return;
    }

    if (isAuthenticated())
    {
        sendErrorAndDrop(ERR_MISC, "out-of-order AUTH message",
                         DropMode::IGNORE_WRITE_QUEUE);
        return;
    }

    setState(GOT_AUTH);

    if (mRole == REMOTE_CALLED_US)
    {
        sendAuth();
        sendPeers();
    }

    updatePeerRecordAfterAuthentication();

    auto self = shared_from_this();
    if (!mAppConnector.getOverlayManager().acceptAuthenticatedPeer(self))
    {
        sendErrorAndDrop(ERR_LOAD, "peer rejected",
                         Peer::DropMode::FLUSH_WRITE_QUEUE);
        return;
    }

    // Subtle: after successful auth, must send sendMore message first to
    // tell the other peer about the local node's reading capacity.
    auto weakSelf = std::weak_ptr<Peer>(self);
    auto sendCb = [weakSelf](std::shared_ptr<StellarMessage const> msg) {
        auto self = weakSelf.lock();
        if (self)
        {
            self->sendAuthenticatedMessage(msg);
        }
    };

    bool enableBytes =
        (mAppConnector.getConfig().OVERLAY_PROTOCOL_VERSION >=
             Peer::FIRST_VERSION_SUPPORTING_FLOW_CONTROL_IN_BYTES &&
         getRemoteOverlayVersion() >=
             Peer::FIRST_VERSION_SUPPORTING_FLOW_CONTROL_IN_BYTES);
    bool bothWantBytes =
        enableBytes &&
        msg.auth().flags == AUTH_MSG_FLAG_FLOW_CONTROL_BYTES_REQUESTED &&
        mAppConnector.getConfig().ENABLE_FLOW_CONTROL_BYTES;

    std::optional<uint32_t> fcBytes =
        bothWantBytes
            ? std::optional<uint32_t>(mAppConnector.getOverlayManager()
                                          .getFlowControlBytesConfig()
                                          .mTotal)
            : std::nullopt;
    mFlowControl->start(mPeerID, sendCb, fcBytes);
    if (fcBytes)
    {
        sendSendMore(mAppConnector.getConfig().PEER_FLOOD_READING_CAPACITY,
                     *fcBytes);
    }
    else
    {
        sendSendMore(mAppConnector.getConfig().PEER_FLOOD_READING_CAPACITY);
    }

    mTxAdverts->start([weakSelf](std::shared_ptr<StellarMessage const> msg) {
        auto self = weakSelf.lock();
        if (self)
        {
            self->sendMessage(msg);
        }
    });

    // Ask for SCP data _after_ the flow control message
    auto low = mAppConnector.getHerder().getMinLedgerSeqToAskPeers();
    sendGetScpState(low);
}

void
Peer::recvGetPeers(StellarMessage const& msg)
{
    ZoneScoped;
    sendPeers();
}

void
Peer::recvPeers(StellarMessage const& msg)
{
    ZoneScoped;
    for (auto const& peer : msg.peers())
    {
        if (peer.port == 0 || peer.port > UINT16_MAX)
        {
            CLOG_DEBUG(Overlay, "ignoring received peer with bad port {}",
                       peer.port);
            continue;
        }
        if (peer.ip.type() == IPv6)
        {
            CLOG_DEBUG(Overlay,
                       "ignoring received IPv6 address (not yet supported)");
            continue;
        }

        releaseAssert(peer.ip.type() == IPv4);
        auto address = PeerBareAddress{peer};

        if (address.isPrivate())
        {
            CLOG_DEBUG(Overlay, "ignoring received private address {}",
                       address.toString());
        }
        else if (address ==
                 PeerBareAddress{getAddress().getIP(),
                                 mAppConnector.getConfig().PEER_PORT})
        {
            CLOG_DEBUG(Overlay, "ignoring received self-address {}",
                       address.toString());
        }
        else if (address.isLocalhost() &&
                 !mAppConnector.getConfig().ALLOW_LOCALHOST_FOR_TESTING)
        {
            CLOG_DEBUG(Overlay, "ignoring received localhost");
        }
        else
        {
            // don't use peer.numFailures here as we may have better luck
            // (and we don't want to poison our failure count)
            mAppConnector.getOverlayManager().getPeerManager().ensureExists(
                address);
        }
    }
}

void
Peer::recvSurveyRequestMessage(StellarMessage const& msg)
{
    ZoneScoped;
    mAppConnector.getOverlayManager().getSurveyManager().relayOrProcessRequest(
        msg, shared_from_this());
}

void
Peer::recvSurveyResponseMessage(StellarMessage const& msg)
{
    ZoneScoped;
    mAppConnector.getOverlayManager().getSurveyManager().relayOrProcessResponse(
        msg, shared_from_this());
}

void
Peer::recvFloodAdvert(StellarMessage const& msg)
{
    releaseAssert(mTxAdverts);
    auto seq = mAppConnector.getHerder().trackingConsensusLedgerIndex();
    mTxAdverts->queueIncomingAdvert(msg.floodAdvert().txHashes, seq);
}

void
Peer::recvFloodDemand(StellarMessage const& msg)
{
    // Pass the demand to OverlayManager for processing
    mAppConnector.getOverlayManager().recvTxDemand(msg.floodDemand(),
                                                   shared_from_this());
}

Peer::PeerMetrics::PeerMetrics(VirtualClock::time_point connectedTime)
    : mMessageRead(0)
    , mMessageWrite(0)
    , mByteRead(0)
    , mByteWrite(0)
    , mAsyncRead(0)
    , mAsyncWrite(0)
    , mMessageDrop(0)
    , mMessageDelayInWriteQueueTimer(medida::Timer(PEER_METRICS_DURATION_UNIT,
                                                   PEER_METRICS_RATE_UNIT,
                                                   PEER_METRICS_WINDOW_SIZE))
    , mMessageDelayInAsyncWriteTimer(medida::Timer(PEER_METRICS_DURATION_UNIT,
                                                   PEER_METRICS_RATE_UNIT,
                                                   PEER_METRICS_WINDOW_SIZE))
    , mAdvertQueueDelay(medida::Timer(PEER_METRICS_DURATION_UNIT,
                                      PEER_METRICS_RATE_UNIT,
                                      PEER_METRICS_WINDOW_SIZE))
    , mPullLatency(medida::Timer(PEER_METRICS_DURATION_UNIT,
                                 PEER_METRICS_RATE_UNIT,
                                 PEER_METRICS_WINDOW_SIZE))
    , mDemandTimeouts(0)
    , mUniqueFloodBytesRecv(0)
    , mDuplicateFloodBytesRecv(0)
    , mUniqueFetchBytesRecv(0)
    , mDuplicateFetchBytesRecv(0)
    , mUniqueFloodMessageRecv(0)
    , mDuplicateFloodMessageRecv(0)
    , mUniqueFetchMessageRecv(0)
    , mDuplicateFetchMessageRecv(0)
    , mTxHashReceived(0)
    , mConnectedTime(connectedTime)
    , mMessagesFulfilled(0)
    , mBannedMessageUnfulfilled(0)
    , mUnknownMessageUnfulfilled(0)
{
}

void
Peer::sendTxDemand(TxDemandVector&& demands)
{
    if (demands.size() > 0)
    {
        auto msg = std::make_shared<StellarMessage>();
        msg->type(FLOOD_DEMAND);
        msg->floodDemand().txHashes = std::move(demands);
        mOverlayMetrics.mMessagesDemanded.Mark(
            msg->floodDemand().txHashes.size());
        std::weak_ptr<Peer> weak = shared_from_this();
        mAppConnector.postOnMainThread(
            [weak, msg = std::move(msg)]() {
                auto strong = weak.lock();
                if (strong)
                {
                    strong->sendMessage(msg);
                }
            },
            "sendTxDemand");
        ++mPeerMetrics.mTxDemandSent;
    }
}

void
Peer::handleMaxTxSizeIncrease(uint32_t increase)
{
    if (increase > 0)
    {
        mFlowControl->handleTxSizeIncrease(increase);
        // Send an additional SEND_MORE to let the other peer know we have more
        // capacity available (and possibly unblock it)
        sendSendMore(0, increase);
    }
}

bool
Peer::sendAdvert(Hash const& hash)
{
    if (!mTxAdverts)
    {
        throw std::runtime_error("Pull mode is not set");
    }

    // No-op if peer already knows about the hash
    if (mTxAdverts->seenAdvert(hash))
    {
        return false;
    }

    // Otherwise, queue up an advert to broadcast to peer
    mTxAdverts->queueOutgoingAdvert(hash);
    return true;
}

void
Peer::retryAdvert(std::list<Hash>& hashes)
{
    if (!mTxAdverts)
    {
        throw std::runtime_error("Pull mode is not set");
    }
    mTxAdverts->retryIncomingAdvert(hashes);
}

bool
Peer::hasAdvert()
{
    if (!mTxAdverts)
    {
        throw std::runtime_error("Pull mode is not set");
    }
    return mTxAdverts->size() > 0;
}

std::pair<Hash, std::optional<VirtualClock::time_point>>
Peer::popAdvert()
{
    if (!mTxAdverts)
    {
        throw std::runtime_error("Pull mode is not set");
    }

    return mTxAdverts->popIncomingAdvert();
}

}
