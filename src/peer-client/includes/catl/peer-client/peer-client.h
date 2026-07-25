#pragma once

#include "connection.h"
#include "endpoint-tracker.h"
#include "peer-session.h"
#include "types.h"

#include <catl/core/logger.h>
#include <catl/core/types.h>

#include <array>
#include <atomic>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <chrono>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace catl::peer_client {

/// Called when the client becomes ready (status exchange complete)
using ReadyCallback = std::function<void(uint32_t peer_ledger_seq)>;

/// Called exactly once when initial setup either becomes ready or fails.
using ConnectCompletionCallback =
    std::function<void(boost::system::error_code, uint32_t peer_ledger_seq)>;

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
 * same result. Set dedupe=false to request an independent network request.
 * If the same canonical key is already live on this connection, the new
 * request is refused with DuplicateRequest: the wire protocol cannot
 * unambiguously correlate two overlapping responses for that key.
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
 * Request submission is thread-safe: every public request enters the
 * PeerClient strand before reading or mutating request state. Completion
 * callbacks run on that strand. Callers never need to enter it themselves.
 */
class PeerClientTestAccess;

class PeerClient final : public PeerSession,
                         public std::enable_shared_from_this<PeerClient>
{
public:
    /// Fires exactly once on the client strand with the immutable terminal
    /// fact for this connection instance. The observer must not assume the
    /// session is still registered anywhere when this runs.
    using DisconnectCallback = std::function<void(PeerTerminalReason const&)>;

    /// Pre-connect configuration. Set these before the read loop starts
    /// to avoid cross-strand races.
    struct ConnectOptions
    {
        uint32_t network_id{};
        std::uint64_t connection_generation{};
        ReadyCallback on_ready{};
        ConnectCompletionCallback on_complete{};
        std::shared_ptr<EndpointTracker> tracker{};
        UnsolicitedHandler unsolicited_handler{};
        DisconnectCallback on_disconnect{};
    };

    /// Connect to a peer. Returns immediately. All handlers are installed
    /// before the read loop starts — no cross-strand races.
    static std::shared_ptr<PeerClient>
    connect(
        asio::io_context& io_context,
        std::string const& host,
        uint16_t port,
        ConnectOptions opts);

    /// Legacy overload for backward compat.
    static std::shared_ptr<PeerClient>
    connect(
        asio::io_context& io_context,
        std::string const& host,
        uint16_t port,
        uint32_t network_id,
        ReadyCallback on_ready,
        ConnectCompletionCallback on_complete);

    ~PeerClient() override;

    // ---------------------------------------------------------------
    // Requests — safe to call before ready (will be queued)
    // ---------------------------------------------------------------

    void
    get_ledger_header(
        uint32_t ledger_seq,
        Callback<LedgerHeaderResult> callback,
        RequestOptions opts = {}) override;

    void
    get_ledger_header(
        Hash256 const& ledger_hash,
        Callback<LedgerHeaderResult> callback,
        RequestOptions opts = {}) override;

    void
    get_tx_proof_path(
        Hash256 const& ledger_hash,
        Hash256 const& key,
        Callback<ProofPathResult> callback,
        RequestOptions opts = {}) override;

    void
    get_state_proof_path(
        Hash256 const& ledger_hash,
        Hash256 const& key,
        Callback<ProofPathResult> callback,
        RequestOptions opts = {}) override;

    // ---------------------------------------------------------------
    // SHAMap node fetching (TMGetLedger with nodeIDs)
    // ---------------------------------------------------------------

    /// Fetch specific nodes from a ledger's account state tree.
    void
    get_state_nodes(
        Hash256 const& ledger_hash,
        std::vector<SHAMapNodeID> const& node_ids,
        Callback<LedgerNodesResult> callback,
        RequestOptions opts = {}) override;

    /// Fetch specific nodes from a ledger's transaction tree.
    void
    get_tx_nodes(
        Hash256 const& ledger_hash,
        std::vector<SHAMapNodeID> const& node_ids,
        Callback<LedgerNodesResult> callback,
        RequestOptions opts = {}) override;

    // ---------------------------------------------------------------
    // Ping
    // ---------------------------------------------------------------

    void
    ping(Callback<PingResult> callback, RequestOptions opts = {}) override;

    // ---------------------------------------------------------------
    // State
    // ---------------------------------------------------------------

    State
    state() const
    {
        return state_;
    }

    bool
    is_ready() const override
    {
        return state_ == State::Ready;
    }

    /// Peer's current ledger seq (known after status exchange)
    uint32_t
    peer_ledger_seq() const override
    {
        return peer_ledger_seq_;
    }

    void
    set_unsolicited_handler(UnsolicitedHandler handler) override;

    /// Set a shared endpoint tracker. TMStatusChange updates will
    /// be fed into it automatically.
    void
    set_tracker(std::shared_ptr<EndpointTracker> tracker);

    /// Peer's advertised ledger range (from TMStatusChange).
    uint32_t
    peer_first_seq() const override
    {
        return peer_first_seq_;
    }

    uint32_t
    peer_last_seq() const override
    {
        return peer_last_seq_;
    }

    std::map<std::string, std::string>
    peer_headers() const override
    {
        if (!connection_)
            return {};
        return connection_->upgrade_headers();
    }

    /// Set a handler that receives every node-type TMLedgerData response
    /// (liAS_NODE, liTX_NODE) before the pending_nodes dispatch.
    void
    set_node_response_handler(NodeResponseHandler handler) override;

    /// Send a raw TMGetLedger for specific node IDs without registering
    /// in pending_nodes. The response will be delivered via the
    /// node_response_handler (if set) and then through normal dispatch.
    /// Thread-safe: submission first enters the PeerClient/connection strand.
    /// Optional on_error callback fires if the async write fails (e.g.
    /// broken pipe) — use this to cancel signal timers immediately
    /// instead of waiting for the full fetch timeout.
    void
    send_get_nodes(
        Hash256 const& ledger_hash,
        int type,  // liTX_NODE or liAS_NODE
        std::vector<SHAMapNodeID> const& node_ids,
        std::function<void(boost::system::error_code)> on_error =
            nullptr) override;

    bool
    submit_node_batch(
        NodeBatchCommand command,
        NodeBatchWriteHandler on_receipt) override;

    bool
    acknowledge_node_batch_receipt(NodeBatchReceiptAck ack) override;

    void
    disconnect() override;

    size_t
    pending_count() const override;

    std::string const&
    endpoint() const override
    {
        return endpoint_str_;
    }

    peer_connection&
    raw_connection()
    {
        return *connection_;
    }

    /// The PeerClient's strand. Callers can use this to hop to the
    /// strand before calling request methods.
    asio::strand<asio::io_context::executor_type>&
    strand() override
    {
        return strand_;
    }

private:
    friend class PeerClientTestAccess;

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
    send_initial_probe_ping();

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
    // Owner-strand request implementations
    // ---------------------------------------------------------------

    void
    disconnect_on_strand();

    void
    send_get_nodes_on_strand(
        Hash256 ledger_hash,
        int type,
        std::vector<SHAMapNodeID> node_ids,
        std::function<void(boost::system::error_code)> on_error);

    bool
    submit_node_batch_on_strand(
        NodeBatchCommand command,
        NodeBatchWriteHandler on_receipt);

    void
    complete_node_batch_write(
        SenderGenerationId sender_generation,
        TransportAttemptId attempt_id,
        boost::system::error_code ec,
        NodeBatchWriteOutcome terminal_override =
            NodeBatchWriteOutcome::written) noexcept;

    void
    acknowledge_node_batch_receipt_on_strand(
        NodeBatchReceiptAck ack) noexcept;

    void
    get_ledger_header_on_strand(
        uint32_t ledger_seq,
        Callback<LedgerHeaderResult> callback,
        RequestOptions opts);

    void
    get_ledger_header_on_strand(
        Hash256 ledger_hash,
        Callback<LedgerHeaderResult> callback,
        RequestOptions opts);

    void
    get_state_nodes_on_strand(
        Hash256 ledger_hash,
        std::vector<SHAMapNodeID> node_ids,
        Callback<LedgerNodesResult> callback,
        RequestOptions opts);

    void
    get_tx_nodes_on_strand(
        Hash256 ledger_hash,
        std::vector<SHAMapNodeID> node_ids,
        Callback<LedgerNodesResult> callback,
        RequestOptions opts);

    void
    get_tx_proof_path_on_strand(
        Hash256 ledger_hash,
        Hash256 key,
        Callback<ProofPathResult> callback,
        RequestOptions opts);

    void
    get_state_proof_path_on_strand(
        Hash256 ledger_hash,
        Hash256 key,
        Callback<ProofPathResult> callback,
        RequestOptions opts);

    void
    ping_on_strand(Callback<PingResult> callback, RequestOptions opts);

    // ---------------------------------------------------------------
    // Request queue (pre-ready)
    // ---------------------------------------------------------------

    using DeferredRequest = std::function<void()>;
    std::deque<DeferredRequest> pending_queue_;
    std::size_t active_deferred_handoffs_ = 0;

    void
    flush_queue();

    void
    defer_request(DeferredRequest fn);

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
        std::uint64_t incarnation = 0;
        Callback<T> callback;
        std::unique_ptr<asio::steady_timer> timer;
    };

    template <typename T>
    struct PendingEntry
    {
        std::uint64_t incarnation = 0;
        std::vector<PendingCallback<T>> callbacks;
        bool network_request_sent =
            false;          // first callback sends, rest piggyback
        Hash256 match_key;  // secondary key for response matching (e.g.
                            // ledger_key for node requests)
        std::vector<SHAMapNodeID>
            requested_nodeids;  // nodeids sent in the request — used to
                                // match responses when multiple requests
                                // share the same (ledgerHash, type)
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

    void
    complete_all_pending(Error error);

    void
    refresh_pending_count_projection() noexcept;

    PendingMap<LedgerHeaderResult> pending_headers_;
    PendingMap<LedgerNodesResult> pending_nodes_;
    PendingMap<PingResult> pending_pings_;
    PendingMap<ProofPathResult> pending_proof_paths_;

    struct NodeBatchWriteRecord
    {
        explicit NodeBatchWriteRecord(
            NodeBatchCommand command,
            NodeBatchWriteHandler on_receipt) noexcept
            : attempt_id(command.attempt_id())
            , sender_generation(command.sender_generation())
            , payload_digest(command.payload_digest())
            , pending_command(std::move(command))
        {
            waiters[0] = std::move(on_receipt);
            waiter_count = 1;
        }

        NodeBatchWriteRecord(NodeBatchWriteRecord const&) = delete;
        NodeBatchWriteRecord&
        operator=(NodeBatchWriteRecord const&) = delete;
        NodeBatchWriteRecord(NodeBatchWriteRecord&&) noexcept = default;
        NodeBatchWriteRecord&
        operator=(NodeBatchWriteRecord&&) noexcept = default;

        TransportAttemptId attempt_id;
        SenderGenerationId sender_generation;
        Hash256 payload_digest;
        std::optional<NodeBatchCommand> pending_command;
        std::array<NodeBatchWriteHandler, kMaxNodeBatchReceiptWaiters> waiters;
        std::size_t waiter_count = 0;
        std::optional<NodeBatchWriteReceipt> terminal_receipt;
    };
    static_assert(
        std::is_nothrow_move_constructible_v<NodeBatchWriteRecord>);
    static_assert(
        std::is_nothrow_move_assignable_v<NodeBatchWriteRecord>);
    std::vector<NodeBatchWriteRecord> node_batch_write_records_;
    std::atomic<std::size_t> node_batch_ingress_projections_{0};
    std::atomic<std::size_t> node_batch_work_projection_{0};
    std::uint64_t next_node_batch_receipt_sequence_ = 0;
    std::uint64_t reserved_node_batch_receipt_sequences_ = 0;
    std::uint64_t next_pending_incarnation_{1};
    std::uint64_t next_callback_incarnation_{1};
    std::atomic<std::size_t> pending_count_projection_{0};

    // Request state assumes strand affinity. Late-bound handler pointers
    // are separately mutex-protected so callers do not reintroduce races
    // by updating them after async work has started.

    // ---------------------------------------------------------------
    // Members
    // ---------------------------------------------------------------

    asio::io_context& io_context_;
    asio::strand<asio::io_context::executor_type> strand_;
    std::unique_ptr<asio::ssl::context> ssl_context_;
    std::shared_ptr<peer_connection> connection_;
    mutable std::mutex handler_mutex_;
    UnsolicitedHandler unsolicited_handler_;
    ReadyCallback ready_callback_;
    ConnectCompletionCallback connect_completion_callback_;

    std::atomic<State> state_{State::Disconnected};
    std::atomic<uint32_t> peer_ledger_seq_{0};
    std::atomic<uint32_t> peer_first_seq_{0};
    std::atomic<uint32_t> peer_last_seq_{0};
    std::string net_label_;
    std::string endpoint_str_;  // "host:port" for tracker
    std::uint64_t receiver_instance_id_ = 0;
    std::uint64_t connection_generation_ = 0;

    /// Optional shared tracker — fed from TMStatusChange.
    std::shared_ptr<EndpointTracker> tracker_;

    DisconnectCallback on_disconnect_;
    NodeResponseHandler node_response_handler_;

    static LogPartition log_;
};

}  // namespace catl::peer_client
