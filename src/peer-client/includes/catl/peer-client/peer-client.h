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
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/ssl.hpp>
#include <chrono>
#include <deque>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
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

namespace detail {

enum class ConnectAttemptTerminalKind : std::uint8_t {
    ready,
    peer_failure,
    owner_disconnect,
    deadline,
    caller_cancelled,
    infrastructure_failure,
};

struct ConnectAttemptTerminal
{
    ConnectAttemptTerminal(
        ConnectAttemptTerminalKind kind,
        boost::system::error_code error = {},
        std::uint32_t peer_seq = 0,
        int deadline_seconds = 0,
        std::exception_ptr failure = {})
        : kind(kind)
        , error(error)
        , peer_seq(peer_seq)
        , deadline_seconds(deadline_seconds)
        , failure(std::move(failure))
    {
    }

    ConnectAttemptTerminalKind kind;
    boost::system::error_code error;
    uint32_t peer_seq = 0;
    int deadline_seconds = 0;
    std::exception_ptr failure;
};

enum class ConnectAttemptPublication : std::uint8_t {
    immediate,
    deferred,
};

struct ConnectAttemptCommit
{
    bool event_committed = false;
    bool terminal_committed = false;
};

struct ConnectAttemptSnapshot
{
    std::optional<ConnectAttemptTerminal> initial_terminal;
    PeerLogicalTerminalCause session_terminal_cause =
        PeerLogicalTerminalCause::none;
};

/// One synchronization home for the initial attempt result and the pre-ready
/// PeerClient terminal cause. The deadline and PeerClient owner transitions
/// compete under the same mutex; the capacity-one channel retains the winner
/// when publication precedes receive initiation.
class ConnectAttemptState
{
public:
    using Signal = boost::asio::experimental::concurrent_channel<void(
        boost::system::error_code)>;

    explicit ConnectAttemptState(boost::asio::any_io_executor executor)
        : signal_(std::move(executor), 1)
    {
    }

    template <typename Commit>
        requires std::is_nothrow_invocable_v<Commit>
    //@@authority transition=prv.T0186 status=provisional scope=function
    // bind=catl::peer_client::detail::ConnectAttemptState::try_commit_initial@40046a19bde7ce9b
    // owner=unresolved owner_hypothesis=connect_attempt
    // commit=connect_attempt_mutex touches=prv.S0059
    // mutation_class=terminal_receipt
    // label=commit_first_connect_attempt_terminal_result
    ConnectAttemptCommit
    try_commit_initial(
        ConnectAttemptTerminal terminal,
        Commit&& owner_commit,
        ConnectAttemptPublication publication =
            ConnectAttemptPublication::immediate) noexcept
    {
        bool publish_now = false;
        {
            std::lock_guard lock(mutex_);
            if (terminal_)
                return {};
            terminal_ = terminal;
            std::forward<Commit>(owner_commit)();
            terminal_publication_pending_ = true;
            publish_now = publication == ConnectAttemptPublication::immediate &&
                !hold_terminal_publication_for_test_;
        }
        if (publish_now)
            publish_terminal();
        return {.event_committed = true, .terminal_committed = true};
    }

    template <typename Commit>
        requires std::is_nothrow_invocable_v<Commit>
    ConnectAttemptCommit
    try_commit_peer_terminal(
        PeerLogicalTerminalCause cause,
        ConnectAttemptTerminal terminal,
        Commit&& owner_commit) noexcept
    {
        bool terminal_committed = false;
        bool publish_now = false;
        {
            std::lock_guard lock(mutex_);
            if (terminal_cause_ != PeerLogicalTerminalCause::none)
                return {};
            terminal_cause_ = cause;
            if (!terminal_)
            {
                terminal_ = terminal;
                terminal_committed = true;
                terminal_publication_pending_ = true;
                publish_now = !hold_terminal_publication_for_test_;
            }
            std::forward<Commit>(owner_commit)();
        }
        if (publish_now)
            publish_terminal();
        return {
            .event_committed = true, .terminal_committed = terminal_committed};
    }

    std::optional<ConnectAttemptTerminal>
    snapshot() const
    {
        std::lock_guard lock(mutex_);
        return terminal_;
    }

    /// Returns both retained facts under one lock. This is an observation, not
    /// a transfer of session authority to PeerSet.
    ConnectAttemptSnapshot
    snapshot_for_handoff() const
    {
        std::lock_guard lock(mutex_);
        return {terminal_, terminal_cause_};
    }

    PeerLogicalTerminalCause
    terminal_cause() const
    {
        std::lock_guard lock(mutex_);
        return terminal_cause_;
    }

    boost::asio::awaitable<boost::system::error_code>
    wait_terminal()
    {
        boost::system::error_code ec;
        co_await signal_.async_receive(
            boost::asio::redirect_error(boost::asio::use_awaitable, ec));
        co_return ec;
    }

    bool
    hold_terminal_publication_for_test()
    {
        std::lock_guard lock(mutex_);
        if (terminal_)
            return false;
        hold_terminal_publication_for_test_ = true;
        return true;
    }

    bool
    release_terminal_publication_for_test() noexcept
    {
        {
            std::lock_guard lock(mutex_);
            if (!hold_terminal_publication_for_test_ ||
                !terminal_publication_pending_)
                return false;
            hold_terminal_publication_for_test_ = false;
        }
        return publish_terminal();
    }

    bool
    publish_terminal() noexcept
    {
        {
            std::lock_guard lock(mutex_);
            if (!terminal_publication_pending_ || terminal_published_ ||
                hold_terminal_publication_for_test_)
            {
                return false;
            }
            terminal_publication_pending_ = false;
            terminal_published_ = true;
        }
        try
        {
            if (!signal_.try_send(boost::system::error_code{}))
                std::terminate();
        }
        catch (...)
        {
            std::terminate();
        }
        return true;
    }

private:
    mutable std::mutex mutex_;
    //@@authority state=prv.S0059 status=provisional scope=field
    // bind=catl::peer_client::detail::ConnectAttemptState::terminal_@3a9e1ad6fc4a0214
    // owner=unresolved owner_hypothesis=connect_attempt
    // observed_home=connect_attempt
    // label=first_wins_initial_connect_attempt_terminal_result
    std::optional<ConnectAttemptTerminal> terminal_;
    //@@authority state=prv.S0058 status=provisional scope=field
    // bind=catl::peer_client::detail::ConnectAttemptState::terminal_cause_@00cad104243cf62c
    // owner=unresolved owner_hypothesis=connect_attempt
    // observed_home=connect_attempt
    // label=retained_first_peer_logical_terminal_cause
    PeerLogicalTerminalCause terminal_cause_ = PeerLogicalTerminalCause::none;
    Signal signal_;
    bool hold_terminal_publication_for_test_ = false;
    bool terminal_publication_pending_ = false;
    bool terminal_published_ = false;
};

}  // namespace detail

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

    PeerLogicalTerminalCause
    terminal_cause() const
    {
        return initial_connect_attempt_->terminal_cause();
    }

    /// Internal typed result home consumed by the coroutine bridge. Public
    /// callbacks remain legacy two-argument observers.
    std::shared_ptr<detail::ConnectAttemptState> const&
    initial_connect_attempt() const noexcept
    {
        return initial_connect_attempt_;
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
    AdvertisedLedgerRange
    advertised_ledger_range() const override
    {
        return AdvertisedLedgerRange::from_packed(
            advertised_ledger_range_.load(std::memory_order_acquire));
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

    /// Set a sink which reports acceptance and failure independently.
    void
    set_node_response_observer(NodeResponseObserver observer) override;

    /// Send a raw TMGetLedger for specific node IDs without registering
    /// in pending_nodes. The response will be delivered via the
    /// node_response_handler (if set) and then through normal dispatch.
    /// Thread-safe: submission first enters the PeerClient/connection strand.
    /// Optional on_write callback fires exactly once: with an error if the
    /// owner strand refuses the request or the async write fails, and with
    /// success only after the async write completes.
    void
    send_get_nodes(
        Hash256 const& ledger_hash,
        uint32_t ledger_seq,
        int type,  // liTX_NODE or liAS_NODE
        std::vector<SHAMapNodeID> const& node_ids,
        std::function<void(boost::system::error_code)> on_write =
            nullptr) override;

    bool
    submit_node_batch(
        NodeBatchCommand command,
        NodeBatchWriteHandler on_receipt) override;

    bool
    acknowledge_node_batch_receipt(NodeBatchReceiptAck ack) override;

    void
    disconnect() override;

    boost::asio::awaitable<void>
    co_disconnect() override;

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

    AdvertisedLedgerRange
    publish_advertised_ledger_range(AdvertisedLedgerRange range) noexcept;

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

    std::optional<State>
    commit_failed_on_strand(
        boost::system::error_code ec =
            boost::asio::error::connection_reset) noexcept;

    void
    send_get_nodes_on_strand(
        Hash256 ledger_hash,
        uint32_t ledger_seq,
        int type,
        std::vector<SHAMapNodeID> node_ids,
        std::function<void(boost::system::error_code)> on_write);

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
    acknowledge_node_batch_receipt_on_strand(NodeBatchReceiptAck ack) noexcept;

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
        //@@authority state=prv.S0065 status=provisional scope=field
        // bind=catl::peer_client::PeerClient::PendingEntry::network_request_initiated@6512b1a82fc8c797
        // owner=unresolved owner_hypothesis=peer_client
        // observed_home=peer_client
        // label=per_pending_attempt_send_initiation_fact
        // True only after async send initiation returns successfully.
        bool network_request_initiated = false;
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

    template <typename T>
    void
    mark_network_request_initiated(
        PendingMap<T>& map,
        Hash256 const& key,
        std::uint64_t incarnation) noexcept;

    template <typename T>
    void
    complete_network_send(
        PendingMap<T>& map,
        Hash256 const& key,
        std::uint64_t incarnation,
        boost::system::error_code error);

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
    static_assert(std::is_nothrow_move_constructible_v<NodeBatchWriteRecord>);
    static_assert(std::is_nothrow_move_assignable_v<NodeBatchWriteRecord>);
    std::vector<NodeBatchWriteRecord> node_batch_write_records_;
    std::atomic<std::size_t> node_batch_ingress_projections_{0};
    std::atomic<std::size_t> node_batch_work_projection_{0};
    std::uint64_t next_node_batch_receipt_sequence_ = 0;
    std::uint64_t reserved_node_batch_receipt_sequences_ = 0;
    std::uint64_t next_pending_incarnation_{1};
    std::uint64_t next_callback_incarnation_{1};
    std::atomic<std::size_t> pending_count_projection_{0};
    std::function<void()> before_ledger_attempt_consume_for_test_;

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
    //@@authority state=prv.S0061 status=provisional scope=field
    // bind=catl::peer_client::PeerClient::ready_callback_@14ce61062bcf8d7c
    // owner=unresolved owner_hypothesis=peer_client
    // observed_home=peer_client
    // label=one_shot_ready_observer
    ReadyCallback ready_callback_;
    //@@authority state=prv.S0060 status=provisional scope=field
    // bind=catl::peer_client::PeerClient::connect_completion_callback_@0916e06aa7c57741
    // owner=unresolved owner_hypothesis=peer_client observed_home=peer_client
    // label=one_shot_initial_connect_completion_export_capability
    ConnectCompletionCallback connect_completion_callback_;

    //@@authority state=prv.S0049 status=provisional scope=field
    // bind=catl::peer_client::PeerClient::state_@9f0ac0751dded7b7
    // owner=unresolved owner_hypothesis=peer_client
    // observed_home=peer_client label=peer_client_logical_lifecycle_phase
    std::atomic<State> state_{State::Disconnected};
    std::shared_ptr<detail::ConnectAttemptState> initial_connect_attempt_;
    std::atomic<uint32_t> peer_ledger_seq_{0};
    //@@authority state=prv.S0021 status=provisional scope=field
    // bind=catl::peer_client::PeerClient::advertised_ledger_range_@f4cbfc77b30d3841
    // owner=unresolved owner_hypothesis=peer_client
    // observed_home=peer_client
    // label=coherent_advertised_ledger_range_snapshot
    std::atomic<std::uint64_t> advertised_ledger_range_{0};
    std::string net_label_;
    std::string endpoint_str_;  // "host:port" for tracker
    std::uint64_t receiver_instance_id_ = 0;
    std::uint64_t connection_generation_ = 0;

    /// Optional shared tracker — fed from TMStatusChange.
    std::shared_ptr<EndpointTracker> tracker_;

    DisconnectCallback on_disconnect_;
    //@@authority state=prv.S0007 status=provisional scope=field
    // bind=catl::peer_client::PeerClient::node_response_observer_@4ed65a4019fccdea
    // owner=unresolved owner_hypothesis=peer_client observed_home=peer_client
    // label=singleton_uncorrelated_node_response_observer
    NodeResponseObserver node_response_observer_;

    static LogPartition log_;
};

}  // namespace catl::peer_client
