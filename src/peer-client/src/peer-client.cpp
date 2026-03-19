#include <catl/peer-client/peer-client.h>

namespace catl::peer_client {

PeerClient::PeerClient(
    std::shared_ptr<PeerConnection> connection,
    asio::io_context& io_context)
    : connection_(std::move(connection))
    , io_context_(io_context)
{
}

PeerClient::~PeerClient()
{
    cancel_all();
}

// ---------------------------------------------------------------
// Ledger headers
// ---------------------------------------------------------------

void
PeerClient::get_ledger_header(
    uint32_t /*ledger_seq*/,
    Callback<LedgerHeaderResult> callback,
    RequestOptions /*opts*/)
{
    // TODO: build TMGetLedger(liBASE, ledgerSeq), send, track with cookie
    callback(Error::BadRequest, {});
}

void
PeerClient::get_ledger_header(
    Hash256 const& /*ledger_hash*/,
    Callback<LedgerHeaderResult> callback,
    RequestOptions /*opts*/)
{
    // TODO: build TMGetLedger(liBASE, ledgerHash), send, track with cookie
    callback(Error::BadRequest, {});
}

// ---------------------------------------------------------------
// Proof paths
// ---------------------------------------------------------------

void
PeerClient::get_tx_proof_path(
    Hash256 const& /*ledger_hash*/,
    Hash256 const& /*key*/,
    Callback<ProofPathResult> callback,
    RequestOptions /*opts*/)
{
    if (!has_ledger_replay())
    {
        callback(Error::FeatureDisabled, {});
        return;
    }
    // TODO: build TMProofPathRequest(lmTRANASCTION), send, track
    callback(Error::BadRequest, {});
}

void
PeerClient::get_state_proof_path(
    Hash256 const& /*ledger_hash*/,
    Hash256 const& /*key*/,
    Callback<ProofPathResult> callback,
    RequestOptions /*opts*/)
{
    if (!has_ledger_replay())
    {
        callback(Error::FeatureDisabled, {});
        return;
    }
    // TODO: build TMProofPathRequest(lmACCOUNT_STATE), send, track
    callback(Error::BadRequest, {});
}

// ---------------------------------------------------------------
// SHAMap node fetching
// ---------------------------------------------------------------

void
PeerClient::get_tx_tree_nodes(
    Hash256 const& /*ledger_hash*/,
    std::vector<SHAMapNodeID> const& /*node_ids*/,
    Callback<LedgerNodesResult> callback,
    RequestOptions /*opts*/)
{
    // TODO: build TMGetLedger(liTX_NODE, nodeIDs), send, track with cookie
    callback(Error::BadRequest, {});
}

void
PeerClient::get_state_tree_nodes(
    Hash256 const& /*ledger_hash*/,
    std::vector<SHAMapNodeID> const& /*node_ids*/,
    Callback<LedgerNodesResult> callback,
    RequestOptions /*opts*/)
{
    // TODO: build TMGetLedger(liAS_NODE, nodeIDs), send, track with cookie
    callback(Error::BadRequest, {});
}

void
PeerClient::get_txset_nodes(
    Hash256 const& /*txset_hash*/,
    std::vector<SHAMapNodeID> const& /*node_ids*/,
    Callback<LedgerNodesResult> callback,
    RequestOptions /*opts*/)
{
    // TODO: build TMGetLedger(liTS_CANDIDATE, nodeIDs), send, track with cookie
    callback(Error::BadRequest, {});
}

// ---------------------------------------------------------------
// Replay delta
// ---------------------------------------------------------------

void
PeerClient::get_replay_delta(
    Hash256 const& /*ledger_hash*/,
    Callback<ReplayDeltaResult> callback,
    RequestOptions /*opts*/)
{
    if (!has_ledger_replay())
    {
        callback(Error::FeatureDisabled, {});
        return;
    }
    // TODO: build TMReplayDeltaRequest, send, track by hash
    callback(Error::BadRequest, {});
}

// ---------------------------------------------------------------
// Ping
// ---------------------------------------------------------------

void
PeerClient::ping(Callback<PingResult> callback, RequestOptions /*opts*/)
{
    // TODO: build TMPing(ptPING, seq), send, track by seq
    callback(Error::BadRequest, {});
}

// ---------------------------------------------------------------
// Unsolicited messages
// ---------------------------------------------------------------

void
PeerClient::set_unsolicited_handler(UnsolicitedHandler handler)
{
    unsolicited_handler_ = std::move(handler);
}

// ---------------------------------------------------------------
// Connection state
// ---------------------------------------------------------------

bool
PeerClient::is_connected() const
{
    // TODO: delegate to connection_
    return false;
}

bool
PeerClient::has_ledger_replay() const
{
    // TODO: check connection's negotiated features
    return false;
}

void
PeerClient::cancel_all()
{
    std::lock_guard lock(mutex_);
    for (auto& [_, req] : pending_headers_)
        req->cancel();
    for (auto& [_, req] : pending_nodes_)
        req->cancel();
    for (auto& [_, req] : pending_proof_paths_)
        req->cancel();
    for (auto& [_, req] : pending_deltas_)
        req->cancel();
    for (auto& [_, req] : pending_pings_)
        req->cancel();
    pending_headers_.clear();
    pending_nodes_.clear();
    pending_proof_paths_.clear();
    pending_deltas_.clear();
    pending_pings_.clear();
}

size_t
PeerClient::pending_count() const
{
    std::lock_guard lock(mutex_);
    return pending_headers_.size() + pending_nodes_.size() +
        pending_proof_paths_.size() + pending_deltas_.size() +
        pending_pings_.size();
}

PeerConnection&
PeerClient::raw_connection()
{
    return *connection_;
}

// ---------------------------------------------------------------
// Packet dispatch
// ---------------------------------------------------------------

void
PeerClient::on_packet(uint16_t type, std::vector<uint8_t> const& data)
{
    switch (type)
    {
        case 32:  // ledger_data
            dispatch_ledger_data(data);
            break;
        case 58:  // proof_path_response
            dispatch_proof_path_response(data);
            break;
        case 60:  // replay_delta_response
            dispatch_replay_delta_response(data);
            break;
        case 3:  // ping (pong)
            dispatch_pong(data);
            break;
        default:
            if (unsolicited_handler_)
                unsolicited_handler_(type, data);
            break;
    }
}

void
PeerClient::dispatch_ledger_data(std::vector<uint8_t> const& /*data*/)
{
    // TODO: deserialize TMLedgerData, match by requestCookie
}

void
PeerClient::dispatch_proof_path_response(std::vector<uint8_t> const& /*data*/)
{
    // TODO: deserialize TMProofPathResponse, match by {key, ledgerHash, type}
}

void
PeerClient::dispatch_replay_delta_response(
    std::vector<uint8_t> const& /*data*/)
{
    // TODO: deserialize TMReplayDeltaResponse, match by ledgerHash
}

void
PeerClient::dispatch_pong(std::vector<uint8_t> const& /*data*/)
{
    // TODO: deserialize TMPing, check ptPONG, match by seq
}

// ---------------------------------------------------------------
// Timer management
// ---------------------------------------------------------------

void
PeerClient::start_timeout(
    PendingRequestBase& req,
    std::chrono::seconds timeout)
{
    req.timer = std::make_shared<asio::steady_timer>(io_context_, timeout);
    req.timer->async_wait([weak = weak_from_this(), id = req.id](
                              boost::system::error_code ec) {
        if (ec)
            return;  // Timer cancelled
        if (auto self = weak.lock())
        {
            // TODO: find request by id across all maps, call timeout()
            (void)id;
        }
    });
}

// ---------------------------------------------------------------
// PendingRequest cancel/timeout
// ---------------------------------------------------------------

template <typename ResultT>
void
PeerClient::PendingRequest<ResultT>::cancel()
{
    if (timer)
        timer->cancel();
    if (callback)
        callback(Error::Cancelled, {});
}

template <typename ResultT>
void
PeerClient::PendingRequest<ResultT>::timeout()
{
    if (timer)
        timer->cancel();
    if (callback)
        callback(Error::Timeout, {});
}

// ---------------------------------------------------------------
// ProofPathKey comparison
// ---------------------------------------------------------------

bool
PeerClient::ProofPathKey::operator<(ProofPathKey const& other) const
{
    if (ledger_hash != other.ledger_hash)
        return ledger_hash < other.ledger_hash;
    if (key != other.key)
        return key < other.key;
    return type < other.type;
}

// Explicit template instantiations
template struct PeerClient::PendingRequest<LedgerHeaderResult>;
template struct PeerClient::PendingRequest<LedgerNodesResult>;
template struct PeerClient::PendingRequest<ProofPathResult>;
template struct PeerClient::PendingRequest<ReplayDeltaResult>;
template struct PeerClient::PendingRequest<PingResult>;

}  // namespace catl::peer_client
