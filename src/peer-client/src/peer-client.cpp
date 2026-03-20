#include "ripple.pb.h"
#include <catl/peer-client/peer-client.h>

namespace catl::peer_client {

LogPartition PeerClient::log_("peer-client", LogLevel::INFO);

// ═══════════════════════════════════════════════════════════════════════
// Result types
// ═══════════════════════════════════════════════════════════════════════

// ─── LedgerHeaderResult ─────────────────────────────────────────────

LedgerHeaderResult::LedgerHeaderResult(
    std::shared_ptr<protocol::TMLedgerData> msg)
    : msg_(std::move(msg))
{
}

uint32_t
LedgerHeaderResult::seq() const
{
    return msg_ ? msg_->ledgerseq() : 0;
}

Key
LedgerHeaderResult::ledger_hash() const
{
    if (msg_ && msg_->ledgerhash().size() == 32)
        return Key(
            reinterpret_cast<const uint8_t*>(msg_->ledgerhash().data()));
    return Key(Hash256::zero().data());
}

std::span<const uint8_t>
LedgerHeaderResult::header_data() const
{
    if (!msg_ || msg_->nodes_size() < 1)
        return {};
    auto const& d = msg_->nodes(0).nodedata();
    return {reinterpret_cast<const uint8_t*>(d.data()), d.size()};
}

catl::common::LedgerInfoView
LedgerHeaderResult::header() const
{
    auto data = header_data();
    return catl::common::LedgerInfoView(data.data(), data.size());
}

WireNodeView
LedgerHeaderResult::state_root_node() const
{
    if (!msg_ || msg_->nodes_size() < 2)
        return WireNodeView({});
    auto const& d = msg_->nodes(1).nodedata();
    return WireNodeView(
        {reinterpret_cast<const uint8_t*>(d.data()), d.size()});
}

WireNodeView
LedgerHeaderResult::tx_root_node() const
{
    if (!msg_ || msg_->nodes_size() < 3)
        return WireNodeView({});
    auto const& d = msg_->nodes(2).nodedata();
    return WireNodeView(
        {reinterpret_cast<const uint8_t*>(d.data()), d.size()});
}

bool
LedgerHeaderResult::has_state_root() const
{
    return msg_ && msg_->nodes_size() >= 2;
}

bool
LedgerHeaderResult::has_tx_root() const
{
    return msg_ && msg_->nodes_size() >= 3;
}

int
LedgerHeaderResult::node_count() const
{
    return msg_ ? msg_->nodes_size() : 0;
}

// ─── ProofPathResult ────────────────────────────────────────────────

ProofPathResult::ProofPathResult(
    std::shared_ptr<protocol::TMProofPathResponse> msg)
    : msg_(std::move(msg))
{
}

Key
ProofPathResult::key() const
{
    if (msg_ && msg_->key().size() == 32)
        return Key(reinterpret_cast<const uint8_t*>(msg_->key().data()));
    return Key(Hash256::zero().data());
}

Key
ProofPathResult::ledger_hash() const
{
    if (msg_ && msg_->ledgerhash().size() == 32)
        return Key(
            reinterpret_cast<const uint8_t*>(msg_->ledgerhash().data()));
    return Key(Hash256::zero().data());
}

std::span<const uint8_t>
ProofPathResult::header_data() const
{
    if (!msg_ || !msg_->has_ledgerheader())
        return {};
    auto const& h = msg_->ledgerheader();
    return {reinterpret_cast<const uint8_t*>(h.data()), h.size()};
}

catl::common::LedgerInfoView
ProofPathResult::header() const
{
    auto data = header_data();
    return catl::common::LedgerInfoView(data.data(), data.size());
}

int
ProofPathResult::path_length() const
{
    return msg_ ? msg_->path_size() : 0;
}

WireNodeView
ProofPathResult::path_node(int index) const
{
    if (!msg_ || index < 0 || index >= msg_->path_size())
        return WireNodeView({});
    auto const& p = msg_->path(index);
    return WireNodeView(
        {reinterpret_cast<const uint8_t*>(p.data()), p.size()});
}

WireNodeView
ProofPathResult::leaf() const
{
    return path_node(0);
}

std::span<const uint8_t>
ProofPathResult::leaf_data() const
{
    if (!msg_ || msg_->path_size() < 1)
        return {};
    auto const& p = msg_->path(0);
    if (p.size() < 1)
        return {};
    // Strip the wire type byte at the end
    return {reinterpret_cast<const uint8_t*>(p.data()), p.size() - 1};
}

// ═══════════════════════════════════════════════════════════════════════
// PeerClient
// ═══════════════════════════════════════════════════════════════════════

// ─── Construction ───────────────────────────────────────────────────

PeerClient::PeerClient(asio::io_context& io_context)
    : io_context_(io_context)
{
}

PeerClient::~PeerClient()
{
    cancel_all();
}

std::shared_ptr<PeerClient>
PeerClient::connect(
    asio::io_context& io_context,
    std::string const& host,
    uint16_t port,
    uint32_t network_id,
    ReadyCallback on_ready)
{
    auto client = std::shared_ptr<PeerClient>(new PeerClient(io_context));
    client->do_connect(host, port, network_id, std::move(on_ready));
    return client;
}

// ─── Connection lifecycle ───────────────────────────────────────────

void
PeerClient::do_connect(
    std::string const& host,
    uint16_t port,
    uint32_t network_id,
    ReadyCallback on_ready)
{
    state_ = State::Connecting;

    ssl_context_ =
        std::make_unique<asio::ssl::context>(asio::ssl::context::tlsv12);
    ssl_context_->set_options(
        asio::ssl::context::default_workarounds |
        asio::ssl::context::no_sslv2 | asio::ssl::context::no_sslv3 |
        asio::ssl::context::single_dh_use);
    ssl_context_->set_verify_mode(asio::ssl::verify_none);
    SSL_CTX_set_ecdh_auto(ssl_context_->native_handle(), 1);

    peer_config config;
    config.host = host;
    config.port = port;
    config.network_id = network_id;

    connection_ =
        std::make_shared<peer_connection>(io_context_, *ssl_context_, config);

    PLOGI(log_, "Connecting to ", host, ":", port, "...");

    auto self = shared_from_this();
    connection_->async_connect(
        [self, on_ready = std::move(on_ready)](
            boost::system::error_code ec) mutable {
            if (ec)
            {
                PLOGE(PeerClient::log_, "Connection failed: ", ec.message());
                self->state_ = State::Failed;
                return;
            }
            self->on_connected(std::move(on_ready));
        });
}

void
PeerClient::on_connected(ReadyCallback on_ready)
{
    state_ = State::Connected;
    PLOGI(log_, "Connected to ", connection_->remote_endpoint());

    auto self = shared_from_this();
    ready_callback_ = std::move(on_ready);

    connection_->start_read(
        [self](
            packet_header const& header,
            std::vector<uint8_t> const& payload) {
            self->on_packet(header, payload);
        });

    send_monitoring_status();
}

void
PeerClient::send_monitoring_status()
{
    state_ = State::ExchangingStatus;

    protocol::TMStatusChange status;
    status.set_newstatus(protocol::nsMONITORING);

    std::vector<uint8_t> data(status.ByteSizeLong());
    status.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::status_change, data, [](boost::system::error_code) {});

    PLOGI(log_, "Sent monitoring status, waiting for peer...");
}

void
PeerClient::handle_status_change(
    std::vector<uint8_t> const& payload,
    ReadyCallback on_ready)
{
    protocol::TMStatusChange status;
    if (!status.ParseFromArray(payload.data(), payload.size()))
        return;

    if (status.has_ledgerseq())
    {
        peer_ledger_seq_ = status.ledgerseq();
        PLOGI(log_, "Peer at ledger ", peer_ledger_seq_.load());
    }

    // Mirror status back
    connection_->async_send_packet(
        packet_type::status_change, payload, [](boost::system::error_code) {});

    if (state_ == State::ExchangingStatus)
    {
        become_ready(std::move(on_ready));
    }
}

void
PeerClient::become_ready(ReadyCallback on_ready)
{
    state_ = State::Ready;
    PLOGI(log_, "Ready (peer ledger: ", peer_ledger_seq_.load(), ")");

    flush_queue();

    if (on_ready)
    {
        on_ready(peer_ledger_seq_);
    }
}

// ─── Packet dispatch ────────────────────────────────────────────────

void
PeerClient::on_packet(
    packet_header const& header,
    std::vector<uint8_t> const& payload)
{
    auto type = static_cast<packet_type>(header.type);

    switch (type)
    {
        case packet_type::ping:
            handle_ping(payload);
            return;
        case packet_type::status_change:
            handle_status_change(payload, std::move(ready_callback_));
            return;
        case packet_type::ledger_data:
            dispatch_ledger_data(payload);
            return;
        case packet_type::proof_path_response:
            dispatch_proof_path_response(payload);
            return;
        default:
            if (unsolicited_handler_)
                unsolicited_handler_(header.type, payload);
            return;
    }
}

void
PeerClient::handle_ping(std::vector<uint8_t> const& payload)
{
    protocol::TMPing msg;
    if (!msg.ParseFromArray(payload.data(), payload.size()))
        return;

    if (msg.type() == protocol::TMPing_pingType_ptPING)
    {
        // Auto-reply
        protocol::TMPing pong;
        pong.set_type(protocol::TMPing_pingType_ptPONG);
        if (msg.has_seq())
            pong.set_seq(msg.seq());

        std::vector<uint8_t> data(pong.ByteSizeLong());
        pong.SerializeToArray(data.data(), data.size());
        connection_->async_send_packet(
            packet_type::ping, data, [](boost::system::error_code) {});
    }
    else if (msg.type() == protocol::TMPing_pingType_ptPONG)
    {
        if (msg.has_seq())
        {
            auto it = pending_pings_.find(msg.seq());
            if (it != pending_pings_.end())
            {
                auto cb = std::move(it->second);
                pending_pings_.erase(it);

                PingResult result;
                result.seq = msg.seq();
                result.received_at = std::chrono::steady_clock::now();
                // sent_at would need to be stored — TODO
                cb(Error::Success, std::move(result));
            }
        }
    }
}

// ─── Ledger header ──────────────────────────────────────────────────

void
PeerClient::get_ledger_header(
    uint32_t ledger_seq,
    Callback<LedgerHeaderResult> callback,
    RequestOptions /*opts*/)
{
    if (queue_if_not_ready([this, ledger_seq, cb = std::move(callback)]() mutable {
            get_ledger_header(ledger_seq, std::move(cb));
        }))
        return;

    auto cookie = allocate_cookie();

    protocol::TMGetLedger request;
    request.set_itype(protocol::liBASE);
    request.set_ledgerseq(ledger_seq);
    request.set_requestcookie(cookie);

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    pending_headers_[cookie] = std::move(callback);

    connection_->async_send_packet(
        packet_type::get_ledger, data, [](boost::system::error_code ec) {
            if (ec)
                PLOGE(PeerClient::log_, "Failed to send TMGetLedger: ", ec.message());
        });
}

void
PeerClient::get_ledger_header(
    Hash256 const& ledger_hash,
    Callback<LedgerHeaderResult> callback,
    RequestOptions /*opts*/)
{
    if (queue_if_not_ready(
            [this, ledger_hash, cb = std::move(callback)]() mutable {
                get_ledger_header(ledger_hash, std::move(cb));
            }))
        return;

    auto cookie = allocate_cookie();

    protocol::TMGetLedger request;
    request.set_itype(protocol::liBASE);
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_requestcookie(cookie);

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    pending_headers_[cookie] = std::move(callback);

    connection_->async_send_packet(
        packet_type::get_ledger, data, [](boost::system::error_code) {});
}

void
PeerClient::dispatch_ledger_data(std::vector<uint8_t> const& payload)
{
    auto msg = std::make_shared<protocol::TMLedgerData>();
    if (!msg->ParseFromArray(payload.data(), payload.size()))
        return;

    // Match by cookie
    Callback<LedgerHeaderResult> callback;
    if (msg->has_requestcookie())
    {
        auto it = pending_headers_.find(msg->requestcookie());
        if (it != pending_headers_.end())
        {
            callback = std::move(it->second);
            pending_headers_.erase(it);
        }
    }

    if (!callback)
        return;

    if (msg->has_error())
    {
        auto err = msg->error() == protocol::reNO_LEDGER ? Error::NoLedger
            : msg->error() == protocol::reNO_NODE        ? Error::NoNode
                                                          : Error::BadRequest;
        callback(err, LedgerHeaderResult{});
        return;
    }

    callback(Error::Success, LedgerHeaderResult{std::move(msg)});
}

// ─── Proof paths ────────────────────────────────────────────────────

void
PeerClient::get_tx_proof_path(
    Hash256 const& ledger_hash,
    Hash256 const& key,
    Callback<ProofPathResult> callback,
    RequestOptions /*opts*/)
{
    if (queue_if_not_ready(
            [this, ledger_hash, key, cb = std::move(callback)]() mutable {
                get_tx_proof_path(ledger_hash, key, std::move(cb));
            }))
        return;

    protocol::TMProofPathRequest request;
    request.set_key(key.data(), 32);
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_type(protocol::lmTRANASCTION);

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    ProofPathKey ppk{ledger_hash, key, protocol::lmTRANASCTION};
    pending_proof_paths_[ppk] = std::move(callback);

    connection_->async_send_packet(
        packet_type::proof_path_req, data, [](boost::system::error_code) {});
}

void
PeerClient::get_state_proof_path(
    Hash256 const& ledger_hash,
    Hash256 const& key,
    Callback<ProofPathResult> callback,
    RequestOptions /*opts*/)
{
    if (queue_if_not_ready(
            [this, ledger_hash, key, cb = std::move(callback)]() mutable {
                get_state_proof_path(ledger_hash, key, std::move(cb));
            }))
        return;

    protocol::TMProofPathRequest request;
    request.set_key(key.data(), 32);
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_type(protocol::lmACCOUNT_STATE);

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    ProofPathKey ppk{ledger_hash, key, protocol::lmACCOUNT_STATE};
    pending_proof_paths_[ppk] = std::move(callback);

    connection_->async_send_packet(
        packet_type::proof_path_req, data, [](boost::system::error_code) {});
}

void
PeerClient::dispatch_proof_path_response(std::vector<uint8_t> const& payload)
{
    auto msg = std::make_shared<protocol::TMProofPathResponse>();
    if (!msg->ParseFromArray(payload.data(), payload.size()))
        return;

    if (msg->key().size() != 32 || msg->ledgerhash().size() != 32)
        return;

    Hash256 key(reinterpret_cast<const uint8_t*>(msg->key().data()));
    Hash256 lh(reinterpret_cast<const uint8_t*>(msg->ledgerhash().data()));
    ProofPathKey ppk{lh, key, msg->type()};

    auto it = pending_proof_paths_.find(ppk);
    if (it == pending_proof_paths_.end())
        return;

    auto callback = std::move(it->second);
    pending_proof_paths_.erase(it);

    if (msg->has_error())
    {
        auto err = msg->error() == protocol::reNO_LEDGER ? Error::NoLedger
            : msg->error() == protocol::reNO_NODE        ? Error::NoNode
                                                          : Error::BadRequest;
        callback(err, ProofPathResult{});
        return;
    }

    callback(Error::Success, ProofPathResult{std::move(msg)});
}

// ─── Ping ───────────────────────────────────────────────────────────

void
PeerClient::ping(Callback<PingResult> callback, RequestOptions /*opts*/)
{
    if (queue_if_not_ready([this, cb = std::move(callback)]() mutable {
            ping(std::move(cb));
        }))
        return;

    auto seq = static_cast<uint32_t>(next_seq_.fetch_add(1));

    protocol::TMPing msg;
    msg.set_type(protocol::TMPing_pingType_ptPING);
    msg.set_seq(seq);

    std::vector<uint8_t> data(msg.ByteSizeLong());
    msg.SerializeToArray(data.data(), data.size());

    pending_pings_[seq] = std::move(callback);

    connection_->async_send_packet(
        packet_type::ping, data, [](boost::system::error_code) {});
}

// ─── Request queue ──────────────────────────────────────────────────

bool
PeerClient::queue_if_not_ready(DeferredRequest fn)
{
    if (state_ == State::Ready)
        return false;
    pending_queue_.push_back(std::move(fn));
    return true;
}

void
PeerClient::flush_queue()
{
    while (!pending_queue_.empty())
    {
        auto fn = std::move(pending_queue_.front());
        pending_queue_.pop_front();
        fn();
    }
}

// ─── Misc ───────────────────────────────────────────────────────────

void
PeerClient::cancel_all()
{
    for (auto& [_, cb] : pending_headers_)
        cb(Error::Cancelled, LedgerHeaderResult{});
    for (auto& [_, cb] : pending_pings_)
        cb(Error::Cancelled, PingResult{});
    for (auto& [_, cb] : pending_proof_paths_)
        cb(Error::Cancelled, ProofPathResult{});
    pending_headers_.clear();
    pending_pings_.clear();
    pending_proof_paths_.clear();
    pending_queue_.clear();
}

size_t
PeerClient::pending_count() const
{
    return pending_headers_.size() + pending_pings_.size() +
        pending_proof_paths_.size() + pending_queue_.size();
}

bool
PeerClient::ProofPathKey::operator<(ProofPathKey const& other) const
{
    if (ledger_hash != other.ledger_hash)
        return ledger_hash < other.ledger_hash;
    if (key != other.key)
        return key < other.key;
    return type < other.type;
}

}  // namespace catl::peer_client
