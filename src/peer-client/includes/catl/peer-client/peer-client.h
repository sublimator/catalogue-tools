#pragma once

#include "types.h"

#include <catl/core/logger.h>
#include <catl/core/types.h>
#include <catl/peer/peer-connection.h>
#include <catl/peer/types.h>

#include <atomic>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <chrono>
#include <deque>
#include <functional>
#include <map>
#include <memory>

namespace catl::peer_client {

namespace asio = boost::asio;
using catl::peer::packet_header;
using catl::peer::packet_type;
using catl::peer::peer_config;
using catl::peer::peer_connection;

//------------------------------------------------------------------------------
// Callback type — Layer 1 (core)
//------------------------------------------------------------------------------

template <typename T>
using Callback = std::function<void(Error, T)>;

/// Called when the client becomes ready (status exchange complete)
using ReadyCallback = std::function<void(uint32_t peer_ledger_seq)>;

/// Called for messages PeerClient doesn't handle internally
using UnsolicitedHandler = std::function<
    void(uint16_t type, std::vector<uint8_t> const& data)>;

//------------------------------------------------------------------------------
// Connection state
//------------------------------------------------------------------------------

enum class State {
    Disconnected,
    Connecting,
    Connected,       // TLS + HTTP upgrade done, sending status
    ExchangingStatus,// Sent our status, waiting for peer's
    Ready,           // Status exchanged, can send requests
    Failed,
};

//------------------------------------------------------------------------------
// PeerClient
//------------------------------------------------------------------------------

class PeerClient : public std::enable_shared_from_this<PeerClient>
{
public:
    /// Connect to a peer. Returns immediately. Calls on_ready when status
    /// exchange completes and requests can be made. Any get_* calls before
    /// ready are queued and flushed automatically.
    static std::shared_ptr<PeerClient>
    connect(
        asio::io_context& io_context,
        std::string const& host,
        uint16_t port,
        uint32_t network_id = 0,
        ReadyCallback on_ready = nullptr);

    ~PeerClient();

    // ---------------------------------------------------------------
    // Requests — safe to call before ready (will be queued)
    // ---------------------------------------------------------------

    void
    get_ledger_header(
        uint32_t ledger_seq,
        Callback<LedgerHeaderResult> callback,
        RequestOptions opts = {});

    void
    get_ledger_header(
        Hash256 const& ledger_hash,
        Callback<LedgerHeaderResult> callback,
        RequestOptions opts = {});

    void
    get_tx_proof_path(
        Hash256 const& ledger_hash,
        Hash256 const& key,
        Callback<ProofPathResult> callback,
        RequestOptions opts = {});

    void
    get_state_proof_path(
        Hash256 const& ledger_hash,
        Hash256 const& key,
        Callback<ProofPathResult> callback,
        RequestOptions opts = {});

    void
    ping(Callback<PingResult> callback, RequestOptions opts = {});

    // ---------------------------------------------------------------
    // State
    // ---------------------------------------------------------------

    State
    state() const
    {
        return state_;
    }

    bool
    is_ready() const
    {
        return state_ == State::Ready;
    }

    /// Peer's current ledger seq (known after status exchange)
    uint32_t
    peer_ledger_seq() const
    {
        return peer_ledger_seq_;
    }

    void
    set_unsolicited_handler(UnsolicitedHandler handler)
    {
        unsolicited_handler_ = std::move(handler);
    }

    void
    cancel_all();

    size_t
    pending_count() const;

    peer_connection&
    raw_connection()
    {
        return *connection_;
    }

private:
    /// Pairs a callback with its timeout timer.
    template <typename T>
    struct PendingRequest
    {
        Callback<T> callback;
        std::unique_ptr<asio::steady_timer> timer;
    };


    PeerClient(asio::io_context& io_context);

    // ---------------------------------------------------------------
    // Connection lifecycle
    // ---------------------------------------------------------------

    void
    do_connect(
        std::string const& host,
        uint16_t port,
        uint32_t network_id,
        ReadyCallback on_ready);

    void
    on_connected(ReadyCallback on_ready);

    void
    send_monitoring_status();

    void
    handle_status_change(std::vector<uint8_t> const& payload, ReadyCallback on_ready);

    void
    become_ready(ReadyCallback on_ready);

    // ---------------------------------------------------------------
    // Packet dispatch
    // ---------------------------------------------------------------

    void
    on_packet(
        packet_header const& header,
        std::vector<uint8_t> const& data);

    void
    handle_ping(std::vector<uint8_t> const& payload);

    void
    dispatch_ledger_data(std::vector<uint8_t> const& data);

    void
    dispatch_proof_path_response(std::vector<uint8_t> const& data);

    // ---------------------------------------------------------------
    // Request queue (pre-ready)
    // ---------------------------------------------------------------

    using DeferredRequest = std::function<void()>;
    std::deque<DeferredRequest> pending_queue_;

    void
    flush_queue();

    /// If not ready, queue the callable and return true. Otherwise return false.
    bool
    queue_if_not_ready(DeferredRequest fn);

    // ---------------------------------------------------------------
    // Sequence allocator
    // ---------------------------------------------------------------

    std::atomic<uint64_t> next_seq_{1};

    uint32_t
    allocate_cookie()
    {
        return static_cast<uint32_t>(next_seq_.fetch_add(1) & 0xFFFFFFFF);
    }

    // ---------------------------------------------------------------
    // Pending request tracking
    // ---------------------------------------------------------------

    /// Extract a pending request, cancel its timer, return the callback.
    template <typename K, typename T>
    static Callback<T>
    take_pending(std::map<K, PendingRequest<T>>& map, K const& key)
    {
        auto it = map.find(key);
        if (it == map.end())
            return nullptr;
        auto cb = std::move(it->second.callback);
        if (it->second.timer)
            it->second.timer->cancel();
        map.erase(it);
        return cb;
    }

    /// Start a timeout timer for a pending request.
    /// When it fires, removes the entry from the map and calls callback(Timeout).
    template <typename K, typename T>
    void
    start_timer(
        std::map<K, PendingRequest<T>>& map,
        K const& key,
        std::chrono::seconds timeout);

    struct ProofPathKey
    {
        Hash256 ledger_hash;
        Hash256 key;
        int type;
        bool operator<(ProofPathKey const& other) const;
    };

    std::map<uint32_t, PendingRequest<LedgerHeaderResult>> pending_headers_;
    std::map<uint32_t, PendingRequest<PingResult>> pending_pings_;
    std::map<ProofPathKey, PendingRequest<ProofPathResult>> pending_proof_paths_;

    // No mutex — all operations must be on the io_context thread.
    // Use asio::post(io_context_, ...) if calling from another thread.

    // ---------------------------------------------------------------
    // Members
    // ---------------------------------------------------------------

    asio::io_context& io_context_;
    std::unique_ptr<asio::ssl::context> ssl_context_;
    std::shared_ptr<peer_connection> connection_;
    UnsolicitedHandler unsolicited_handler_;
    ReadyCallback ready_callback_;

    std::atomic<State> state_{State::Disconnected};
    std::atomic<uint32_t> peer_ledger_seq_{0};

    static LogPartition log_;
};

}  // namespace catl::peer_client
