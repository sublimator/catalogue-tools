#include "ripple.pb.h"
#include <catl/peer-client/peer-client.h>

#include <catl/crypto/sha512-half-hasher.h>

namespace catl::peer_client {

LogPartition PeerClient::log_("peer-client", LogLevel::INHERIT);

// ═══════════════════════════════════════════════════════════════════════
// Canonical key computation
// ═══════════════════════════════════════════════════════════════════════

static Hash256
hash_fields(void const* data, size_t size)
{
    crypto::Sha512HalfHasher h;
    h.update(static_cast<uint8_t const*>(data), size);
    return h.finalize();
}

Hash256
PeerClient::ledger_key(uint32_t seq, int type)
{
    struct
    {
        uint32_t seq;
        int type;
    } fields{seq, type};
    return hash_fields(&fields, sizeof(fields));
}

Hash256
PeerClient::ledger_key(Hash256 const& hash, int type)
{
    struct
    {
        uint8_t hash[32];
        int type;
    } fields;
    std::memcpy(fields.hash, hash.data(), 32);
    fields.type = type;
    return hash_fields(&fields, sizeof(fields));
}

Hash256
PeerClient::nodes_key(
    Hash256 const& ledger_hash,
    int type,
    std::vector<SHAMapNodeID> const& node_ids)
{
    crypto::Sha512HalfHasher h;
    h.update(ledger_hash.data(), 32);
    h.update(reinterpret_cast<uint8_t const*>(&type), sizeof(type));
    for (auto const& nid : node_ids)
    {
        auto wire = nid.to_wire();
        h.update(reinterpret_cast<uint8_t const*>(wire.data()), wire.size());
    }
    return h.finalize();
}

Hash256
PeerClient::ledger_key_from_response(uint32_t seq, int type)
{
    return ledger_key(seq, type);
}

Hash256
PeerClient::proof_path_key(
    Hash256 const& ledger_hash,
    Hash256 const& key,
    int type)
{
    struct
    {
        uint8_t lh[32];
        uint8_t k[32];
        int type;
    } fields;
    std::memcpy(fields.lh, ledger_hash.data(), 32);
    std::memcpy(fields.k, key.data(), 32);
    fields.type = type;
    return hash_fields(&fields, sizeof(fields));
}

Hash256
PeerClient::ping_key(uint32_t seq)
{
    return hash_fields(&seq, sizeof(seq));
}

// ═══════════════════════════════════════════════════════════════════════
// Pending request management
// ═══════════════════════════════════════════════════════════════════════

template <typename T>
bool
PeerClient::register_callback(
    PendingMap<T>& map,
    Hash256 const& key,
    Callback<T> callback,
    RequestOptions const& opts,
    bool dedupe)
{
    auto it = map.find(key);

    // Existing entry — piggyback if deduping
    if (it != map.end() && dedupe)
    {
        PLOGD(
            log_,
            "register_callback: piggyback key=",
            key.hex().substr(0, 16),
            " existing_callbacks=",
            it->second.callbacks.size());
        auto timer =
            std::make_unique<asio::steady_timer>(strand_, opts.timeout);
        auto* tp = timer.get();
        it->second.callbacks.push_back({std::move(callback), std::move(timer)});
        auto idx = it->second.callbacks.size() - 1;

        auto self = shared_from_this();
        tp->async_wait([self, &map, key, idx](boost::system::error_code ec) {
            if (ec)
                return;
            PLOGW(
                PeerClient::log_,
                "TIMEOUT (piggyback) key=",
                key.hex().substr(0, 16),
                " idx=",
                idx);
            auto it2 = map.find(key);
            if (it2 == map.end())
                return;
            auto& cbs = it2->second.callbacks;
            if (idx < cbs.size() && cbs[idx].callback)
            {
                auto cb = std::move(cbs[idx].callback);
                cbs[idx].callback = nullptr;
                cb(Error::Timeout, T{});
            }
            bool all_done = true;
            for (auto& c : cbs)
                if (c.callback)
                    all_done = false;
            if (all_done)
                map.erase(it2);
        });

        return false;
    }

    // New entry
    PLOGD(
        log_,
        "register_callback: NEW key=",
        key.hex().substr(0, 16),
        " timeout=",
        opts.timeout.count(),
        "s",
        " callback_valid=",
        static_cast<bool>(callback));
    PendingEntry<T> entry;
    auto timer =
        std::make_unique<asio::steady_timer>(strand_, opts.timeout);
    auto* tp = timer.get();
    entry.callbacks.push_back({std::move(callback), std::move(timer)});
    PLOGT(
        log_,
        "  after push_back: callback_valid=",
        static_cast<bool>(entry.callbacks[0].callback));
    entry.network_request_sent = true;
    map[key] = std::move(entry);
    PLOGD(
        log_,
        "  stored in map: callback_valid=",
        static_cast<bool>(map[key].callbacks[0].callback),
        " map_size=",
        map.size());

    auto self = shared_from_this();
    tp->async_wait([self, &map, key](boost::system::error_code ec) {
        if (ec)
        {
            PLOGD(
                PeerClient::log_,
                "timer cancelled key=",
                key.hex().substr(0, 16));
            return;
        }
        PLOGW(
            PeerClient::log_,
            "TIMEOUT key=",
            key.hex().substr(0, 16),
            " map_size=",
            map.size());
        auto it2 = map.find(key);
        if (it2 == map.end())
        {
            PLOGW(PeerClient::log_, "  key already gone from map");
            return;
        }
        auto& cbs = it2->second.callbacks;
        PLOGW(
            PeerClient::log_,
            "  callbacks=",
            cbs.size(),
            " cb[0]_valid=",
            (!cbs.empty() ? static_cast<bool>(cbs[0].callback) : false));
        if (!cbs.empty() && cbs[0].callback)
        {
            auto cb = std::move(cbs[0].callback);
            cbs[0].callback = nullptr;
            cb(Error::Timeout, T{});
        }
        bool all_done = true;
        for (auto& c : cbs)
            if (c.callback)
                all_done = false;
        if (all_done)
            map.erase(it2);
    });

    return true;
}

template <typename T>
void
PeerClient::resolve(PendingMap<T>& map, Hash256 const& key, Error err, T result)
{
    auto it = map.find(key);
    if (it == map.end())
    {
        PLOGW(
            log_,
            "resolve: key=",
            key.hex().substr(0, 16),
            " NOT FOUND (map_size=",
            map.size(),
            ")");
        return;
    }

    auto num_callbacks = it->second.callbacks.size();
    PLOGD(
        log_,
        "resolve: key=",
        key.hex().substr(0, 16),
        " err=",
        static_cast<int>(err),
        " callbacks=",
        num_callbacks);

    // Log validity of each callback before moving
    for (size_t i = 0; i < it->second.callbacks.size(); ++i)
        PLOGD(
            log_,
            "  pre-move: callback[",
            i,
            "] valid=",
            static_cast<bool>(it->second.callbacks[i].callback));

    auto shared_result = std::make_shared<T>(std::move(result));

    auto entry = std::move(it->second);
    map.erase(it);

    for (size_t i = 0; i < entry.callbacks.size(); ++i)
    {
        auto& pc = entry.callbacks[i];
        if (pc.timer)
            pc.timer->cancel();
        if (pc.callback)
        {
            PLOGD(log_, "  invoking callback ", i);
            try
            {
                pc.callback(err, *shared_result);
                PLOGD(log_, "  callback ", i, " returned");
            }
            catch (std::exception const& e)
            {
                PLOGE(log_, "  callback ", i, " THREW: ", e.what());
            }
            catch (...)
            {
                PLOGE(log_, "  callback ", i, " THREW unknown exception");
            }
        }
        else
        {
            PLOGE(log_, "  callback ", i, " is NULL — should never happen");
        }
    }
}

// Explicit instantiations
template bool
PeerClient::register_callback(
    PendingMap<LedgerHeaderResult>&,
    Hash256 const&,
    Callback<LedgerHeaderResult>,
    RequestOptions const&,
    bool);
template bool
PeerClient::register_callback(
    PendingMap<LedgerNodesResult>&,
    Hash256 const&,
    Callback<LedgerNodesResult>,
    RequestOptions const&,
    bool);
template bool
PeerClient::register_callback(
    PendingMap<PingResult>&,
    Hash256 const&,
    Callback<PingResult>,
    RequestOptions const&,
    bool);
template bool
PeerClient::register_callback(
    PendingMap<ProofPathResult>&,
    Hash256 const&,
    Callback<ProofPathResult>,
    RequestOptions const&,
    bool);
template void
PeerClient::resolve(
    PendingMap<LedgerHeaderResult>&,
    Hash256 const&,
    Error,
    LedgerHeaderResult);
template void
PeerClient::resolve(
    PendingMap<LedgerNodesResult>&,
    Hash256 const&,
    Error,
    LedgerNodesResult);
template void
PeerClient::resolve(PendingMap<PingResult>&, Hash256 const&, Error, PingResult);
template void
PeerClient::resolve(
    PendingMap<ProofPathResult>&,
    Hash256 const&,
    Error,
    ProofPathResult);

// ═══════════════════════════════════════════════════════════════════════
// Result types
// ═══════════════════════════════════════════════════════════════════════

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
        return Key(reinterpret_cast<const uint8_t*>(msg_->ledgerhash().data()));
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
    auto d = header_data();
    return catl::common::LedgerInfoView(d.data(), d.size());
}

WireNodeView
LedgerHeaderResult::state_root_node() const
{
    if (!msg_ || msg_->nodes_size() < 2)
        return WireNodeView({});
    auto const& d = msg_->nodes(1).nodedata();
    return WireNodeView({reinterpret_cast<const uint8_t*>(d.data()), d.size()});
}

WireNodeView
LedgerHeaderResult::tx_root_node() const
{
    if (!msg_ || msg_->nodes_size() < 3)
        return WireNodeView({});
    auto const& d = msg_->nodes(2).nodedata();
    return WireNodeView({reinterpret_cast<const uint8_t*>(d.data()), d.size()});
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

// ─── LedgerNodesResult ──────────────────────────────────────────────

LedgerNodesResult::LedgerNodesResult(
    std::shared_ptr<protocol::TMLedgerData> msg)
    : msg_(std::move(msg))
{
}

uint32_t
LedgerNodesResult::seq() const
{
    return msg_ ? msg_->ledgerseq() : 0;
}

Key
LedgerNodesResult::ledger_hash() const
{
    if (msg_ && msg_->ledgerhash().size() == 32)
        return Key(reinterpret_cast<const uint8_t*>(msg_->ledgerhash().data()));
    return Key(Hash256::zero().data());
}

int
LedgerNodesResult::node_count() const
{
    return msg_ ? msg_->nodes_size() : 0;
}

std::span<const uint8_t>
LedgerNodesResult::node_data(int index) const
{
    if (!msg_ || index < 0 || index >= msg_->nodes_size())
        return {};
    auto const& d = msg_->nodes(index).nodedata();
    return {reinterpret_cast<const uint8_t*>(d.data()), d.size()};
}

std::span<const uint8_t>
LedgerNodesResult::node_id(int index) const
{
    if (!msg_ || index < 0 || index >= msg_->nodes_size())
        return {};
    if (!msg_->nodes(index).has_nodeid())
        return {};
    auto const& d = msg_->nodes(index).nodeid();
    return {reinterpret_cast<const uint8_t*>(d.data()), d.size()};
}

WireNodeView
LedgerNodesResult::node_view(int index) const
{
    return WireNodeView(node_data(index));
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
        return Key(reinterpret_cast<const uint8_t*>(msg_->ledgerhash().data()));
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
    auto d = header_data();
    return catl::common::LedgerInfoView(d.data(), d.size());
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
    return WireNodeView({reinterpret_cast<const uint8_t*>(p.data()), p.size()});
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
    return {reinterpret_cast<const uint8_t*>(p.data()), p.size() - 1};
}

// ═══════════════════════════════════════════════════════════════════════
// PeerClient — lifecycle
// ═══════════════════════════════════════════════════════════════════════

PeerClient::PeerClient(asio::io_context& io_context)
    : io_context_(io_context), strand_(asio::make_strand(io_context))
{
}

PeerClient::~PeerClient()
{
    PLOGD(log_, "~PeerClient");
    cancel_all();
}

std::shared_ptr<PeerClient>
PeerClient::connect(
    asio::io_context& io_context,
    std::string const& host,
    uint16_t port,
    ConnectOptions opts)
{
    auto client = std::shared_ptr<PeerClient>(new PeerClient(io_context));
    // Install handlers BEFORE the read loop starts — no cross-strand race.
    {
        std::lock_guard lock(client->handler_mutex_);
        client->tracker_ = std::move(opts.tracker);
        client->unsolicited_handler_ = std::move(opts.unsolicited_handler);
    }
    client->on_disconnect_ = std::move(opts.on_disconnect);
    client->do_connect(
        host,
        port,
        opts.network_id,
        std::move(opts.on_ready),
        std::move(opts.on_complete));
    return client;
}

std::shared_ptr<PeerClient>
PeerClient::connect(
    asio::io_context& io_context,
    std::string const& host,
    uint16_t port,
    uint32_t network_id,
    ReadyCallback on_ready,
    ConnectCompletionCallback on_complete)
{
    ConnectOptions opts;
    opts.network_id = network_id;
    opts.on_ready = std::move(on_ready);
    opts.on_complete = std::move(on_complete);
    return connect(io_context, host, port, std::move(opts));
}

void
PeerClient::set_unsolicited_handler(UnsolicitedHandler handler)
{
    std::lock_guard lock(handler_mutex_);
    unsolicited_handler_ = std::move(handler);
}

void
PeerClient::set_tracker(std::shared_ptr<EndpointTracker> tracker)
{
    std::lock_guard lock(handler_mutex_);
    tracker_ = std::move(tracker);
}

void
PeerClient::set_node_response_handler(NodeResponseHandler handler)
{
    std::lock_guard lock(handler_mutex_);
    node_response_handler_ = std::move(handler);
}

void
PeerClient::disconnect()
{
    if (connection_)
        connection_->close();
}

void
PeerClient::send_get_nodes(
    Hash256 const& ledger_hash,
    int type,
    std::vector<SHAMapNodeID> const& node_ids,
    std::function<void(boost::system::error_code)> on_error)
{
    if (state_ != State::Ready)
    {
        PLOGD(log_, "send_get_nodes: not ready, dropping");
        if (on_error)
            on_error(boost::asio::error::connection_aborted);
        return;
    }

    protocol::TMGetLedger request;
    request.set_itype(static_cast<protocol::TMLedgerInfoType>(type));
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_querytype(protocol::qtINDIRECT);
    request.set_querydepth(0);
    for (auto const& nid : node_ids)
        request.add_nodeids(nid.to_wire());

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    PLOGD(
        log_,
        "send_get_nodes: type=",
        type,
        " nodeids=",
        node_ids.size(),
        " ledger=",
        ledger_hash.hex().substr(0, 16));

    connection_->async_send_packet(
        packet_type::get_ledger,
        data,
        [on_error = std::move(on_error)](boost::system::error_code ec) {
            if (ec)
            {
                PLOGE(
                    PeerClient::log_,
                    "send_get_nodes failed: ",
                    ec.message());
                if (on_error)
                    on_error(ec);
            }
        });
}

void
PeerClient::do_connect(
    std::string const& host,
    uint16_t port,
    uint32_t network_id,
    ReadyCallback on_ready,
    ConnectCompletionCallback on_complete)
{
    state_ = State::Connecting;
    endpoint_str_ = host + ":" + std::to_string(port);
    connect_completion_callback_ = std::move(on_complete);

    ssl_context_ =
        std::make_unique<asio::ssl::context>(asio::ssl::context::tlsv12);
    ssl_context_->set_options(
        asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
        asio::ssl::context::no_sslv3 | asio::ssl::context::single_dh_use);
    ssl_context_->set_verify_mode(asio::ssl::verify_none);
    SSL_CTX_set_ecdh_auto(ssl_context_->native_handle(), 1);

    peer_config config;
    config.host = host;
    config.port = port;
    config.network_id = network_id;

    connection_ =
        std::make_shared<peer_connection>(strand_, *ssl_context_, config);

    PLOGI(log_, "Connecting to ", host, ":", port, "...");

    auto self = shared_from_this();
    // Disconnect handler fires on the strand (socket is strand-aware).
    // Notifies both PeerClient (state update) and any registered observer
    // (PeerSet removal).
    connection_->set_disconnect_handler([self](boost::system::error_code ec) {
        // Always notify the observer (PeerSet needs to know)
        if (self->on_disconnect_)
        {
            self->on_disconnect_();
        }

        if (self->state_ == State::Ready || self->state_ == State::Failed)
        {
            return;
        }

        if (ec == boost::asio::error::invalid_argument ||
            ec == boost::asio::error::operation_aborted)
        {
            PLOGD(
                PeerClient::log_,
                "[",
                self->endpoint_str_,
                "] Connection lost before ready: ",
                ec.message());
        }
        else
        {
            PLOGE(
                PeerClient::log_,
                "[",
                self->endpoint_str_,
                "] Connection lost before ready: ",
                ec.message());
        }
        self->state_ = State::Failed;
        self->complete_connect(ec);
    });

    // Connect callback fires on the strand (socket is strand-aware)
    connection_->async_connect([self, on_ready = std::move(on_ready)](
                                   boost::system::error_code ec) mutable {
        if (ec)
        {
            // Connection failures are routine (503 redirect, timeout,
            // refused). Only unexpected errors are worth logging loudly.
            PLOGD(
                PeerClient::log_,
                "[",
                self->endpoint_str_,
                "] Connection failed: ",
                ec.message());
            self->state_ = State::Failed;
            self->complete_connect(ec);
            return;
        }
        self->on_connected(std::move(on_ready));
    });
}

void
PeerClient::complete_connect(boost::system::error_code ec)
{
    if (!connect_completion_callback_)
    {
        return;
    }

    auto cb = std::move(connect_completion_callback_);
    cb(ec, peer_ledger_seq_.load());
}

void
PeerClient::on_connected(ReadyCallback on_ready)
{
    state_ = State::Connected;
    PLOGI(log_, "Connected to ", connection_->remote_endpoint());

    auto self = shared_from_this();
    ready_callback_ = std::move(on_ready);

    // Packet handler fires on the strand (socket is strand-aware).
    // No manual dispatch needed.
    connection_->start_read(
        [self](
            packet_header const& header, std::vector<uint8_t> const& payload) {
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
PeerClient::handle_status_change(std::vector<uint8_t> const& payload)
{
    protocol::TMStatusChange status;
    if (!status.ParseFromArray(payload.data(), payload.size()))
    {
        PLOGE(log_, "handle_status_change: parse failed");
        return;
    }

    if (status.has_ledgerseq())
    {
        peer_ledger_seq_ = status.ledgerseq();
        PLOGT(
            log_, "[", endpoint_str_, "] at ledger ", peer_ledger_seq_.load());
    }

    if (status.has_firstseq() && status.has_lastseq())
    {
        auto old_first = peer_first_seq_.load();
        auto old_last = peer_last_seq_.load();
        peer_first_seq_.store(status.firstseq());
        peer_last_seq_.store(status.lastseq());
        if (peer_last_seq_.load() < peer_first_seq_.load() ||
            peer_first_seq_.load() == 0 || peer_last_seq_.load() == 0)
        {
            peer_first_seq_ = 0;
            peer_last_seq_ = 0;
        }
        else if (old_first != peer_first_seq_ || old_last != peer_last_seq_)
        {
            // Only log when range actually changes
            if (old_first == 0)
            {
                PLOGI(
                    log_,
                    "[",
                    endpoint_str_,
                    "] range: ",
                    peer_first_seq_,
                    " - ",
                    peer_last_seq_);
            }
            else
            {
                PLOGD(
                    log_,
                    "[",
                    endpoint_str_,
                    "] range: ",
                    peer_first_seq_,
                    " - ",
                    peer_last_seq_);
            }
        }

        // Feed the shared tracker if one is set
        std::shared_ptr<EndpointTracker> tracker;
        {
            std::lock_guard lock(handler_mutex_);
            tracker = tracker_;
        }
        if (tracker && !endpoint_str_.empty())
        {
            tracker->update(
                endpoint_str_,
                {.first_seq = peer_first_seq_,
                 .last_seq = peer_last_seq_,
                 .current_seq = peer_ledger_seq_});
        }
    }

    // Mirror status back
    connection_->async_send_packet(
        packet_type::status_change, payload, [](boost::system::error_code) {});

    if (state_ == State::ExchangingStatus)
    {
        become_ready();
    }
}

void
PeerClient::handle_endpoints(std::vector<uint8_t> const& payload)
{
    std::shared_ptr<EndpointTracker> tracker;
    {
        std::lock_guard lock(handler_mutex_);
        tracker = tracker_;
    }
    if (!tracker)
        return;

    protocol::TMEndpoints msg;
    if (!msg.ParseFromArray(payload.data(), payload.size()))
    {
        PLOGD(log_, "handle_endpoints: parse failed");
        return;
    }

    int added = 0;
    for (auto const& ep : msg.endpoints_v2())
    {
        if (ep.hops() > 1)
            continue;

        auto const& addr = ep.endpoint();

        // Skip useless wildcard bind addresses
        if (addr.find("[::]:") == 0 || addr.find("0.0.0.0:") == 0)
            continue;

        // Must be parseable
        std::string host;
        uint16_t port = 0;
        if (!EndpointTracker::parse_endpoint(addr, host, port))
            continue;

        tracker->add_discovered(addr);
        PLOGD(
            log_,
            "[",
            endpoint_str_,
            "] TMEndpoints: ",
            addr,
            " (hops=",
            ep.hops(),
            ")");
        added++;
    }

    if (added > 0)
    {
        PLOGI(
            log_,
            "[",
            endpoint_str_,
            "] discovered ",
            added,
            " peer endpoints");
    }
}

void
PeerClient::become_ready()
{
    state_ = State::Ready;
    PLOGI(log_, "Ready (peer ledger: ", peer_ledger_seq_.load(), ")");

    flush_queue();

    if (ready_callback_)
    {
        PLOGD(log_, "become_ready: invoking ready_callback_");
        auto cb = std::move(ready_callback_);
        cb(peer_ledger_seq_);
        PLOGD(log_, "become_ready: ready_callback_ returned");
    }

    complete_connect({});
}

// ═══════════════════════════════════════════════════════════════════════
// Packet dispatch
// ═══════════════════════════════════════════════════════════════════════

void
PeerClient::on_packet(
    packet_header const& header,
    std::vector<uint8_t> const& payload)
{
    auto type = static_cast<packet_type>(header.type);
    PLOGT(
        log_,
        "on_packet type=",
        header.type,
        " size=",
        payload.size(),
        " compressed=",
        header.compressed);
    switch (type)
    {
        case packet_type::ping:
            handle_ping(payload);
            return;
        case packet_type::status_change:
            handle_status_change(payload);
            return;
        case packet_type::endpoints:
            handle_endpoints(payload);
            return;
        case packet_type::ledger_data:
            dispatch_ledger_data(payload);
            return;
        case packet_type::proof_path_response:
            dispatch_proof_path_response(payload);
            return;
        default:
            PLOGT(log_, "  unhandled packet type=", header.type);
            UnsolicitedHandler handler;
            {
                std::lock_guard lock(handler_mutex_);
                handler = unsolicited_handler_;
            }
            if (handler)
                handler(header.type, payload);
            return;
    }
}

void
PeerClient::handle_ping(std::vector<uint8_t> const& payload)
{
    protocol::TMPing msg;
    if (!msg.ParseFromArray(payload.data(), payload.size()))
    {
        PLOGE(log_, "handle_ping: parse failed");
        return;
    }

    if (msg.type() == protocol::TMPing_pingType_ptPING)
    {
        PLOGD(log_, "handle_ping: PING received, replying PONG");
        protocol::TMPing pong;
        pong.set_type(protocol::TMPing_pingType_ptPONG);
        if (msg.has_seq())
            pong.set_seq(msg.seq());
        std::vector<uint8_t> data(pong.ByteSizeLong());
        pong.SerializeToArray(data.data(), data.size());
        connection_->async_send_packet(
            packet_type::ping, data, [](boost::system::error_code) {});
    }
    else if (msg.type() == protocol::TMPing_pingType_ptPONG && msg.has_seq())
    {
        PLOGD(log_, "handle_ping: PONG received seq=", msg.seq());
        auto key = ping_key(msg.seq());
        PingResult result;
        result.seq = msg.seq();
        result.received_at = std::chrono::steady_clock::now();
        resolve(pending_pings_, key, Error::Success, std::move(result));
    }
}

void
PeerClient::dispatch_ledger_data(std::vector<uint8_t> const& payload)
{
    auto msg = std::make_shared<protocol::TMLedgerData>();
    if (!msg->ParseFromArray(payload.data(), payload.size()))
    {
        PLOGE(
            log_,
            "dispatch_ledger_data: parse failed (",
            payload.size(),
            " bytes)");
        return;
    }

    PLOGD(
        log_,
        "dispatch_ledger_data: type=",
        msg->type(),
        " seq=",
        msg->ledgerseq(),
        " nodes=",
        msg->nodes_size(),
        " has_error=",
        msg->has_error(),
        " has_cookie=",
        msg->has_requestcookie());

    // Feed node responses to the NodeCache interceptor FIRST.
    // This handles responses from send_get_nodes() which bypass pending_nodes.
    if (msg->type() != protocol::liBASE && !msg->has_error())
    {
        NodeResponseHandler handler;
        {
            std::lock_guard lock(handler_mutex_);
            handler = node_response_handler_;
        }
        if (handler)
        {
            handler(msg);
        }
    }

    if (msg->ledgerhash().size() == 32)
    {
        Hash256 lh(reinterpret_cast<const uint8_t*>(msg->ledgerhash().data()));
        PLOGD(log_, "  ledger_hash=", lh.hex().substr(0, 16));
    }

    auto seq_key = ledger_key(msg->ledgerseq(), msg->type());
    Hash256 hash_key_val;
    bool have_hash_key = false;
    if (msg->ledgerhash().size() == 32)
    {
        Hash256 lh(reinterpret_cast<const uint8_t*>(msg->ledgerhash().data()));
        hash_key_val = ledger_key(lh, msg->type());
        have_hash_key = true;
    }

    PLOGD(
        log_,
        "  seq_key=",
        seq_key.hex().substr(0, 16),
        " in_headers=",
        pending_headers_.count(seq_key),
        " in_nodes=",
        pending_nodes_.count(seq_key));
    if (have_hash_key)
        PLOGD(
            log_,
            "  hash_key=",
            hash_key_val.hex().substr(0, 16),
            " in_headers=",
            pending_headers_.count(hash_key_val),
            " in_nodes=",
            pending_nodes_.count(hash_key_val));

    PLOGD(
        log_,
        "  pending: headers=",
        pending_headers_.size(),
        " nodes=",
        pending_nodes_.size());

    // Dump all pending keys
    for (auto const& [k, e] : pending_headers_)
        PLOGT(
            log_,
            "    hdr key=",
            k.hex().substr(0, 16),
            " cbs=",
            e.callbacks.size(),
            " cb0_valid=",
            (!e.callbacks.empty() ? static_cast<bool>(e.callbacks[0].callback)
                                  : false));
    for (auto const& [k, e] : pending_nodes_)
        PLOGT(
            log_,
            "    node key=",
            k.hex().substr(0, 16),
            " cbs=",
            e.callbacks.size(),
            " cb0_valid=",
            (!e.callbacks.empty() ? static_cast<bool>(e.callbacks[0].callback)
                                  : false));

    // For headers: direct key match (seq_key or hash_key)
    auto find_header_key = [&]() -> Hash256 const* {
        if (pending_headers_.count(seq_key))
            return &seq_key;
        if (have_hash_key && pending_headers_.count(hash_key_val))
            return &hash_key_val;
        return nullptr;
    };

    //@@start find-node-key
    // For nodes: match response to the correct pending entry using the
    // per-node nodeids in the response. Each response nodeid must be a
    // descendant-or-equal of one of the entry's requested nodeids.
    // This handles: exact matches, speculative depth (leaf terminates
    // early → fewer nodes returned), and single-child chain traversal
    // (queryDepth=0 still follows bc==1 chains → extra descendants).
    //
    // Fallback to match_key scan for error responses (no nodes).
    auto is_descendant_or_equal =
        [](SHAMapNodeID const& child, SHAMapNodeID const& ancestor) -> bool {
        if (child.depth < ancestor.depth)
            return false;
        if (child.depth == ancestor.depth)
            return child.id == ancestor.id;
        // Check that the first ancestor.depth nibbles match
        for (uint8_t d = 0; d < ancestor.depth; ++d)
        {
            int byte_idx = d / 2;
            uint8_t c_nibble = (d % 2 == 0)
                ? (child.id.data()[byte_idx] >> 4) & 0xF
                : child.id.data()[byte_idx] & 0xF;
            uint8_t a_nibble = (d % 2 == 0)
                ? (ancestor.id.data()[byte_idx] >> 4) & 0xF
                : ancestor.id.data()[byte_idx] & 0xF;
            if (c_nibble != a_nibble)
                return false;
        }
        return true;
    };

    auto find_node_key = [&]() -> Hash256 const* {
        auto response_match = have_hash_key ? hash_key_val : seq_key;

        // Extract response nodeids
        std::vector<SHAMapNodeID> response_nids;
        for (int i = 0; i < msg->nodes_size(); ++i)
        {
            if (!msg->nodes(i).has_nodeid() ||
                msg->nodes(i).nodeid().size() != 33)
                continue;
            auto const& nid_bytes = msg->nodes(i).nodeid();
            SHAMapNodeID nid;
            std::memcpy(nid.id.data(), nid_bytes.data(), 32);
            nid.depth = static_cast<uint8_t>(nid_bytes[32]);
            response_nids.push_back(nid);
        }

        // If we have response nodeids, find the pending entry whose
        // requested_nodeids are ancestors of ALL response nodeids
        if (!response_nids.empty())
        {
            Hash256 const* best = nullptr;
            for (auto const& [k, entry] : pending_nodes_)
            {
                if (entry.match_key != response_match)
                    continue;
                if (entry.requested_nodeids.empty())
                    continue;

                // Every response nodeid must descend from at least one
                // requested nodeid in this entry
                bool all_match = true;
                for (auto const& resp_nid : response_nids)
                {
                    bool found = false;
                    for (auto const& req_nid : entry.requested_nodeids)
                    {
                        if (is_descendant_or_equal(resp_nid, req_nid))
                        {
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                    {
                        all_match = false;
                        break;
                    }
                }
                if (all_match)
                {
                    best = &k;
                    break;
                }
            }
            if (best)
                return best;
        }

        // Fallback: scan by match_key only (error responses, no nodeids,
        // or no descendant match found)
        for (auto const& [k, entry] : pending_nodes_)
        {
            if (entry.match_key == response_match)
                return &k;
        }
        return nullptr;
    };
    //@@end find-node-key

    if (msg->has_error())
    {
        auto err = msg->error() == protocol::reNO_LEDGER ? Error::NoLedger
            : msg->error() == protocol::reNO_NODE        ? Error::NoNode
                                                         : Error::BadRequest;
        PLOGW(log_, "  error response: code=", msg->error());
        if (auto k = find_header_key())
        {
            PLOGD(
                log_,
                "  resolving error → pending_headers key=",
                k->hex().substr(0, 16));
            resolve(pending_headers_, *k, err, LedgerHeaderResult{});
        }
        else if (auto k = find_node_key())
        {
            PLOGD(
                log_,
                "  resolving error → pending_nodes key=",
                k->hex().substr(0, 16));
            resolve(pending_nodes_, *k, err, LedgerNodesResult{});
        }
        else
        {
            PLOGW(log_, "  error response with no matching pending request");
        }
        return;
    }

    if (msg->type() == protocol::liBASE)
    {
        if (auto k = find_header_key())
        {
            PLOGD(
                log_,
                "  resolving liBASE → headers key=",
                k->hex().substr(0, 16));
            resolve(
                pending_headers_,
                *k,
                Error::Success,
                LedgerHeaderResult{std::move(msg)});
        }
        else
        {
            PLOGW(log_, "  liBASE response but no matching pending_headers");
        }
    }
    else
    {
        if (auto k = find_node_key())
        {
            PLOGD(
                log_,
                "  resolving type=",
                msg->type(),
                " → nodes key=",
                k->hex().substr(0, 16));
            resolve(
                pending_nodes_,
                *k,
                Error::Success,
                LedgerNodesResult{std::move(msg)});
        }
        else
        {
            PLOGW(
                log_,
                "  type=",
                msg->type(),
                " response but no matching pending_nodes");
        }
    }
}

void
PeerClient::dispatch_proof_path_response(std::vector<uint8_t> const& payload)
{
    auto msg = std::make_shared<protocol::TMProofPathResponse>();
    if (!msg->ParseFromArray(payload.data(), payload.size()))
    {
        PLOGE(log_, "dispatch_proof_path_response: parse failed");
        return;
    }

    if (msg->key().size() != 32 || msg->ledgerhash().size() != 32)
    {
        PLOGE(log_, "dispatch_proof_path_response: invalid key/hash size");
        return;
    }

    Hash256 k(reinterpret_cast<const uint8_t*>(msg->key().data()));
    Hash256 lh(reinterpret_cast<const uint8_t*>(msg->ledgerhash().data()));
    auto key = proof_path_key(lh, k, msg->type());

    PLOGD(
        log_,
        "dispatch_proof_path_response: key=",
        key.hex().substr(0, 16),
        " has_error=",
        msg->has_error(),
        " path_size=",
        msg->path_size());

    if (msg->has_error())
    {
        auto err = msg->error() == protocol::reNO_LEDGER ? Error::NoLedger
            : msg->error() == protocol::reNO_NODE        ? Error::NoNode
                                                         : Error::BadRequest;
        resolve(pending_proof_paths_, key, err, ProofPathResult{});
        return;
    }

    resolve(
        pending_proof_paths_,
        key,
        Error::Success,
        ProofPathResult{std::move(msg)});
}

// ═══════════════════════════════════════════════════════════════════════
// Request methods
// ═══════════════════════════════════════════════════════════════════════

void
PeerClient::get_ledger_header(
    uint32_t ledger_seq,
    Callback<LedgerHeaderResult> callback,
    RequestOptions opts)
{
    if (state_ != State::Ready)
    {
        PLOGD(
            log_,
            "get_ledger_header(seq=",
            ledger_seq,
            "): not ready, queuing");
        queue_if_not_ready(
            [this, ledger_seq, cb = std::move(callback), opts]() mutable {
                get_ledger_header(ledger_seq, std::move(cb), opts);
            });
        return;
    }

    auto key = ledger_key(ledger_seq, protocol::liBASE);
    PLOGD(
        log_,
        "get_ledger_header(seq=",
        ledger_seq,
        ") key=",
        key.hex().substr(0, 16));

    if (!register_callback(
            pending_headers_, key, std::move(callback), opts, opts.dedupe))
    {
        PLOGD(log_, "  deduped (piggybacking)");
        return;
    }

    PLOGD(
        log_,
        "  sending TMGetLedger liBASE seq=",
        ledger_seq,
        " pending_headers=",
        pending_headers_.size());
    protocol::TMGetLedger request;
    request.set_itype(protocol::liBASE);
    request.set_ledgerseq(ledger_seq);

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::get_ledger, data, [](boost::system::error_code ec) {
            if (ec)
                PLOGE(
                    PeerClient::log_,
                    "send TMGetLedger failed: ",
                    ec.message());
        });
}

void
PeerClient::get_ledger_header(
    Hash256 const& ledger_hash,
    Callback<LedgerHeaderResult> callback,
    RequestOptions opts)
{
    if (state_ != State::Ready)
    {
        PLOGD(log_, "get_ledger_header(hash): not ready, queuing");
        queue_if_not_ready(
            [this, ledger_hash, cb = std::move(callback), opts]() mutable {
                get_ledger_header(ledger_hash, std::move(cb), opts);
            });
        return;
    }

    auto key = ledger_key(ledger_hash, protocol::liBASE);
    PLOGD(
        log_,
        "get_ledger_header(hash=",
        ledger_hash.hex().substr(0, 16),
        ") key=",
        key.hex().substr(0, 16));

    if (!register_callback(
            pending_headers_, key, std::move(callback), opts, opts.dedupe))
        return;

    PLOGD(log_, "  sending TMGetLedger liBASE by hash");
    protocol::TMGetLedger request;
    request.set_itype(protocol::liBASE);
    request.set_ledgerhash(ledger_hash.data(), 32);

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::get_ledger, data, [](boost::system::error_code) {});
}

void
PeerClient::get_state_nodes(
    Hash256 const& ledger_hash,
    std::vector<SHAMapNodeID> const& node_ids,
    Callback<LedgerNodesResult> callback,
    RequestOptions opts)
{
    if (state_ != State::Ready)
    {
        PLOGD(log_, "get_state_nodes: not ready, queuing");
        queue_if_not_ready([this,
                            ledger_hash,
                            node_ids,
                            cb = std::move(callback),
                            opts]() mutable {
            get_state_nodes(ledger_hash, node_ids, std::move(cb), opts);
        });
        return;
    }

    auto key = nodes_key(ledger_hash, protocol::liAS_NODE, node_ids);
    auto match = ledger_key(ledger_hash, protocol::liAS_NODE);
    PLOGD(
        log_,
        "get_state_nodes: hash=",
        ledger_hash.hex().substr(0, 16),
        " node_ids=",
        node_ids.size(),
        " key=",
        key.hex().substr(0, 16));

    if (!register_callback(
            pending_nodes_, key, std::move(callback), opts, opts.dedupe))
    {
        PLOGD(log_, "  deduped (piggybacking)");
        return;
    }
    pending_nodes_[key].match_key = match;
    pending_nodes_[key].requested_nodeids = node_ids;

    PLOGD(
        log_,
        "  sending TMGetLedger liAS_NODE, pending_nodes=",
        pending_nodes_.size());
    protocol::TMGetLedger request;
    request.set_itype(protocol::liAS_NODE);
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_querytype(protocol::qtINDIRECT);
    request.set_querydepth(0);  // exact node only, no fan-out
    for (auto const& nid : node_ids)
        request.add_nodeids(nid.to_wire());

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::get_ledger, data, [](boost::system::error_code ec) {
            if (ec)
                PLOGE(
                    PeerClient::log_,
                    "send get_state_nodes failed: ",
                    ec.message());
        });
}

void
PeerClient::get_tx_nodes(
    Hash256 const& ledger_hash,
    std::vector<SHAMapNodeID> const& node_ids,
    Callback<LedgerNodesResult> callback,
    RequestOptions opts)
{
    if (state_ != State::Ready)
    {
        PLOGD(log_, "get_tx_nodes: not ready, queuing");
        queue_if_not_ready([this,
                            ledger_hash,
                            node_ids,
                            cb = std::move(callback),
                            opts]() mutable {
            get_tx_nodes(ledger_hash, node_ids, std::move(cb), opts);
        });
        return;
    }

    auto key = nodes_key(ledger_hash, protocol::liTX_NODE, node_ids);
    auto match = ledger_key(ledger_hash, protocol::liTX_NODE);
    PLOGD(
        log_,
        "get_tx_nodes: hash=",
        ledger_hash.hex().substr(0, 16),
        " node_ids=",
        node_ids.size(),
        " key=",
        key.hex().substr(0, 16));

    if (!register_callback(
            pending_nodes_, key, std::move(callback), opts, opts.dedupe))
    {
        PLOGD(log_, "  deduped (piggybacking)");
        return;
    }
    pending_nodes_[key].match_key = match;
    pending_nodes_[key].requested_nodeids = node_ids;

    PLOGD(
        log_,
        "  sending TMGetLedger liTX_NODE, pending_nodes=",
        pending_nodes_.size());
    protocol::TMGetLedger request;
    request.set_itype(protocol::liTX_NODE);
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_querytype(protocol::qtINDIRECT);
    request.set_querydepth(0);  // exact node only, no fan-out
    for (auto const& nid : node_ids)
        request.add_nodeids(nid.to_wire());

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::get_ledger, data, [](boost::system::error_code ec) {
            if (ec)
                PLOGE(
                    PeerClient::log_,
                    "send get_tx_nodes failed: ",
                    ec.message());
        });
}

void
PeerClient::get_tx_proof_path(
    Hash256 const& ledger_hash,
    Hash256 const& key,
    Callback<ProofPathResult> callback,
    RequestOptions opts)
{
    if (state_ != State::Ready)
    {
        PLOGD(log_, "get_tx_proof_path: not ready, queuing");
        queue_if_not_ready(
            [this, ledger_hash, key, cb = std::move(callback), opts]() mutable {
                get_tx_proof_path(ledger_hash, key, std::move(cb), opts);
            });
        return;
    }

    auto ckey = proof_path_key(ledger_hash, key, protocol::lmTRANASCTION);
    PLOGD(
        log_,
        "get_tx_proof_path: ledger=",
        ledger_hash.hex().substr(0, 16),
        " key=",
        Hash256(key.data()).hex().substr(0, 16),
        " ckey=",
        ckey.hex().substr(0, 16));

    if (!register_callback(
            pending_proof_paths_, ckey, std::move(callback), opts, opts.dedupe))
    {
        PLOGD(log_, "  deduped (piggybacking)");
        return;
    }

    PLOGD(log_, "  sending TMProofPathRequest lmTRANASCTION");
    protocol::TMProofPathRequest request;
    request.set_key(key.data(), 32);
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_type(protocol::lmTRANASCTION);

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::proof_path_req, data, [](boost::system::error_code ec) {
            if (ec)
                PLOGE(
                    PeerClient::log_,
                    "send get_tx_proof_path failed: ",
                    ec.message());
        });
}

void
PeerClient::get_state_proof_path(
    Hash256 const& ledger_hash,
    Hash256 const& key,
    Callback<ProofPathResult> callback,
    RequestOptions opts)
{
    if (state_ != State::Ready)
    {
        PLOGD(log_, "get_state_proof_path: not ready, queuing");
        queue_if_not_ready(
            [this, ledger_hash, key, cb = std::move(callback), opts]() mutable {
                get_state_proof_path(ledger_hash, key, std::move(cb), opts);
            });
        return;
    }

    auto ckey = proof_path_key(ledger_hash, key, protocol::lmACCOUNT_STATE);
    PLOGD(
        log_,
        "get_state_proof_path: ledger=",
        ledger_hash.hex().substr(0, 16),
        " key=",
        Hash256(key.data()).hex().substr(0, 16),
        " ckey=",
        ckey.hex().substr(0, 16));

    if (!register_callback(
            pending_proof_paths_, ckey, std::move(callback), opts, opts.dedupe))
    {
        PLOGD(log_, "  deduped (piggybacking)");
        return;
    }

    PLOGD(log_, "  sending TMProofPathRequest lmACCOUNT_STATE");
    protocol::TMProofPathRequest request;
    request.set_key(key.data(), 32);
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_type(protocol::lmACCOUNT_STATE);

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::proof_path_req, data, [](boost::system::error_code ec) {
            if (ec)
                PLOGE(
                    PeerClient::log_,
                    "send get_state_proof_path failed: ",
                    ec.message());
        });
}

void
PeerClient::ping(Callback<PingResult> callback, RequestOptions opts)
{
    if (state_ != State::Ready)
    {
        PLOGD(log_, "ping: not ready, queuing");
        queue_if_not_ready([this, cb = std::move(callback), opts]() mutable {
            ping(std::move(cb), opts);
        });
        return;
    }

    auto seq = static_cast<uint32_t>(next_seq_.fetch_add(1));
    auto key = ping_key(seq);
    PLOGD(log_, "ping: seq=", seq, " key=", key.hex().substr(0, 16));

    register_callback(pending_pings_, key, std::move(callback), opts, false);

    protocol::TMPing msg;
    msg.set_type(protocol::TMPing_pingType_ptPING);
    msg.set_seq(seq);

    std::vector<uint8_t> data(msg.ByteSizeLong());
    msg.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::ping, data, [](boost::system::error_code) {});
}

// ═══════════════════════════════════════════════════════════════════════
// Queue & misc
// ═══════════════════════════════════════════════════════════════════════

bool
PeerClient::queue_if_not_ready(DeferredRequest fn)
{
    if (state_ == State::Ready)
        return false;
    PLOGD(
        log_,
        "queue_if_not_ready: queuing (state=",
        static_cast<int>(state_.load()),
        " queue_size=",
        pending_queue_.size() + 1,
        ")");
    pending_queue_.push_back(std::move(fn));
    return true;
}

void
PeerClient::flush_queue()
{
    if (!pending_queue_.empty())
        PLOGD(
            log_,
            "flush_queue: flushing ",
            pending_queue_.size(),
            " queued requests");
    while (!pending_queue_.empty())
    {
        auto fn = std::move(pending_queue_.front());
        pending_queue_.pop_front();
        fn();
    }
}

void
PeerClient::cancel_all()
{
    PLOGD(
        log_,
        "cancel_all: headers=",
        pending_headers_.size(),
        " nodes=",
        pending_nodes_.size(),
        " pings=",
        pending_pings_.size(),
        " proofs=",
        pending_proof_paths_.size(),
        " queue=",
        pending_queue_.size());
    auto cancel = [](auto& map) {
        for (auto& [_, entry] : map)
            for (auto& pc : entry.callbacks)
            {
                if (pc.timer)
                    pc.timer->cancel();
            }
        map.clear();
    };
    cancel(pending_headers_);
    cancel(pending_nodes_);
    cancel(pending_pings_);
    cancel(pending_proof_paths_);
    pending_queue_.clear();
}

size_t
PeerClient::pending_count() const
{
    size_t n = pending_queue_.size();
    for (auto const& [_, e] : pending_headers_)
        n += e.callbacks.size();
    for (auto const& [_, e] : pending_nodes_)
        n += e.callbacks.size();
    for (auto const& [_, e] : pending_pings_)
        n += e.callbacks.size();
    for (auto const& [_, e] : pending_proof_paths_)
        n += e.callbacks.size();
    return n;
}

}  // namespace catl::peer_client
