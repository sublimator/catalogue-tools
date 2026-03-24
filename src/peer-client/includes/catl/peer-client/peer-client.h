#pragma once

#include "connection.h"
#include "endpoint-tracker.h"
#include "types.h"

#include <catl/core/logger.h>
#include <catl/core/types.h>

#include <atomic>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <chrono>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <string>

namespace catl::peer_client {

//------------------------------------------------------------------------------
// Callback type — Layer 1 (core)
//------------------------------------------------------------------------------

template <typename T>
using Callback = std::function<void(Error, T)>;

/// Called when the client becomes ready (status exchange complete)
using ReadyCallback = std::function<void(uint32_t peer_ledger_seq)>;

/// Called exactly once when initial setup either becomes ready or fails.
using ConnectCompletionCallback =
    std::function<void(boost::system::error_code, uint32_t peer_ledger_seq)>;

/// Called for messages PeerClient doesn't handle internally
using UnsolicitedHandler =
    std::function<void(uint16_t type, std::vector<uint8_t> const& data)>;

//------------------------------------------------------------------------------
// Connection state
//------------------------------------------------------------------------------

enum class State {
    Disconnected,
    Connecting,
    Connected,         // TLS + HTTP upgrade done, sending status
    ExchangingStatus,  // Sent our status, waiting for peer's
    Ready,             // Status exchanged, can send requests
    Failed,
};

//------------------------------------------------------------------------------
// PeerClient
//------------------------------------------------------------------------------

/**
 * High-level request/response API for XRPL peer protocol.
 *
 * ## Connection Lifecycle
 *
 * Use PeerClient::connect() to create. It handles SSL, TLS handshake,
 * HTTP upgrade, and the status exchange dance (send nsMONITORING, wait
 * for peer's status, mirror it back). Requests made before the status
 * exchange completes are queued and flushed automatically.
 * Peer pings are auto-replied.
 *
 * ## Request/Response Correlation
 *
 * The XRPL peer protocol has NO general request/response correlation ID.
 * requestCookie on TMGetLedger is for relay routing — NOT correlation.
 * TMPing.seq is the only field that is truly echoed for matching.
 *
 * We correlate by computing a canonical Hash256 key from response fields:
 *   - TMLedgerData (liBASE):        hash(ledgerSeq, type)
 *   - TMLedgerData (liAS/TX_NODE):  hash(ledgerHash, type)
 *   - TMProofPathResponse:          hash(key, ledgerHash, type)
 *   - TMPing (ptPONG):              hash(seq)
 *
 * ## Deduplication
 *
 * By default (RequestOptions::dedupe=true), if a request is made for a
 * canonical key that already has an in-flight request, the callback is
 * appended to the existing entry. No new network request is sent. When
 * the response arrives, ALL callbacks for that key are called with the
 * same result. Set dedupe=false to force a new network request.
 *
 * ## Timeouts
 *
 * Each callback has its own asio::steady_timer. When it fires, that
 * callback is called with Error::Timeout and removed. Other callbacks
 * for the same key remain active. If all callbacks timeout, the entry
 * is removed (response will arrive but nobody cares).
 *
 * ## Result Types
 *
 * Result types (LedgerHeaderResult, ProofPathResult, etc.) own a
 * shared_ptr to the protobuf message. Accessors return zero-copy views
 * (Key, WireNodeView, LedgerInfoView) pointing into protobuf storage.
 * raw() exposes the protobuf for escape hatch.
 *
 * ## Threading
 *
 * Single io_context thread. No mutex. All get_* calls and callbacks
 * run on the io_context. Use asio::post(io_context, ...) if calling
 * from another thread.
 */
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
        ReadyCallback on_ready = nullptr,
        ConnectCompletionCallback on_complete = nullptr);

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

    // ---------------------------------------------------------------
    // SHAMap node fetching (TMGetLedger with nodeIDs)
    // ---------------------------------------------------------------

    /// Fetch specific nodes from a ledger's account state tree.
    void
    get_state_nodes(
        Hash256 const& ledger_hash,
        std::vector<SHAMapNodeID> const& node_ids,
        Callback<LedgerNodesResult> callback,
        RequestOptions opts = {});

    /// Fetch specific nodes from a ledger's transaction tree.
    void
    get_tx_nodes(
        Hash256 const& ledger_hash,
        std::vector<SHAMapNodeID> const& node_ids,
        Callback<LedgerNodesResult> callback,
        RequestOptions opts = {});

    // ---------------------------------------------------------------
    // Ping
    // ---------------------------------------------------------------

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

    /// Set a shared endpoint tracker. TMStatusChange updates will
    /// be fed into it automatically.
    void
    set_tracker(std::shared_ptr<EndpointTracker> tracker)
    {
        tracker_ = std::move(tracker);
    }

    /// Peer's advertised ledger range (from TMStatusChange).
    uint32_t
    peer_first_seq() const
    {
        return peer_first_seq_;
    }

    uint32_t
    peer_last_seq() const
    {
        return peer_last_seq_;
    }

    void
    cancel_all();

    size_t
    pending_count() const;

    std::string const&
    endpoint() const
    {
        return endpoint_str_;
    }

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
        ReadyCallback on_ready,
        ConnectCompletionCallback on_complete);

    void
    on_connected(ReadyCallback on_ready);

    void
    complete_connect(boost::system::error_code ec);

    void
    send_monitoring_status();

    void
    handle_status_change(std::vector<uint8_t> const& payload);

    void
    handle_endpoints(std::vector<uint8_t> const& payload);

    void
    become_ready();

    // ---------------------------------------------------------------
    // Packet dispatch
    // ---------------------------------------------------------------

    void
    on_packet(packet_header const& header, std::vector<uint8_t> const& data);

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

    /// If not ready, queue the callable and return true. Otherwise return
    /// false.
    bool
    queue_if_not_ready(DeferredRequest fn);

    // ---------------------------------------------------------------
    // Sequence allocator (for ping seq)
    // ---------------------------------------------------------------

    std::atomic<uint64_t> next_seq_{1};

    // ---------------------------------------------------------------
    // Pending request tracking
    //
    // Canonical Hash256 keys computed from response fields.
    // Multiple callbacks per key (fan-out / dedupe).
    // Per-callback timeouts.
    // ---------------------------------------------------------------

    template <typename T>
    struct PendingCallback
    {
        Callback<T> callback;
        std::unique_ptr<asio::steady_timer> timer;
    };

    template <typename T>
    struct PendingEntry
    {
        std::vector<PendingCallback<T>> callbacks;
        bool network_request_sent =
            false;          // first callback sends, rest piggyback
        Hash256 match_key;  // secondary key for response matching (e.g.
                            // ledger_key for node requests)
    };

    template <typename T>
    using PendingMap = std::map<Hash256, PendingEntry<T>>;

    /// Compute canonical key for TMGetLedger/TMLedgerData correlation.
    /// For by-seq (liBASE): hash(seq, type). For by-hash: hash(ledgerHash,
    /// type).
    static Hash256
    ledger_key(uint32_t seq, int type);
    static Hash256
    ledger_key(Hash256 const& hash, int type);

    /// Compute canonical key for TMProofPathResponse correlation.
    static Hash256
    proof_path_key(Hash256 const& ledger_hash, Hash256 const& key, int type);

    /// Compute canonical key for node requests — includes node_ids so that
    /// different node-set requests for the same (ledgerHash, type) don't
    /// collide. Response dispatch matches by scanning pending_nodes_ with
    /// (ledgerHash, type).
    static Hash256
    nodes_key(
        Hash256 const& ledger_hash,
        int type,
        std::vector<SHAMapNodeID> const& node_ids);

    /// Compute canonical key for TMPing correlation.
    static Hash256
    ping_key(uint32_t seq);

    /// Compute canonical key from a received TMLedgerData response.
    static Hash256
    ledger_key_from_response(uint32_t seq, int type);

    /// Register a callback for a canonical key. Starts its timeout timer.
    /// Returns true if this is the first callback (caller should send the
    /// request).
    template <typename T>
    bool
    register_callback(
        PendingMap<T>& map,
        Hash256 const& key,
        Callback<T> callback,
        RequestOptions const& opts,
        bool dedupe);

    /// Resolve all callbacks for a canonical key with a result.
    template <typename T>
    void
    resolve(PendingMap<T>& map, Hash256 const& key, Error err, T result);

    PendingMap<LedgerHeaderResult> pending_headers_;
    PendingMap<LedgerNodesResult> pending_nodes_;
    PendingMap<PingResult> pending_pings_;
    PendingMap<ProofPathResult> pending_proof_paths_;

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
    ConnectCompletionCallback connect_completion_callback_;

    std::atomic<State> state_{State::Disconnected};
    std::atomic<uint32_t> peer_ledger_seq_{0};
    uint32_t peer_first_seq_{0};
    uint32_t peer_last_seq_{0};
    std::string endpoint_str_;  // "host:port" for tracker

    /// Optional shared tracker — fed from TMStatusChange.
    std::shared_ptr<EndpointTracker> tracker_;

    static LogPartition log_;
};

}  // namespace catl::peer_client
