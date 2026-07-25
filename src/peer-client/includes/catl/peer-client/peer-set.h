#pragma once

// PeerSet — manages multiple peer connections with ledger range awareness.
//
// Wraps EndpointTracker (knows ranges) with live PeerClient connections.
// bootstrap() and try_undiscovered() launch background connection attempts,
// while wait_for_any_peer()/wait_for_peer() wait for usable results.
//
// PeerSet-owned state is serialized through its strand. Detached connect
// coroutines keep PeerSet alive by capturing shared_ptr<PeerSet>.

#include "endpoint-tracker.h"
#include "node-transport.h"
#include "peer-crawl-client.h"
#include "peer-endpoint-cache.h"
#include "peer-generation-pin.h"
#include "peer-session.h"

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <chrono>
#include <cstddef>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

class PeerSetTestAccess;  // forward decl for test friend

namespace catl::peer_client {

/// Well-known bootstrap peers by network ID.
struct BootstrapPeer
{
    std::string host;
    uint16_t port;
};

/// Get bootstrap peers for a network.
std::vector<BootstrapPeer> const&
get_bootstrap_peers(uint32_t network_id);

struct PeerSetOptions
{
    uint32_t network_id = 0;
    std::string endpoint_cache_path;
    std::size_t cached_endpoint_limit = 64;
    std::size_t max_in_flight_connects = 8;
    std::size_t max_in_flight_crawls = 4;
    std::size_t max_connected_peers = 20;  // hub peer cap
    std::chrono::seconds retry_backoff{5};

    /// Separate pool for archival peers (wide ledger range). These don't
    /// count against max_connected_peers — they have their own budget.
    std::size_t max_archival_peers = 5;

    /// A peer is considered "archival" if its ledger range span exceeds
    /// this threshold. Default 1M ledgers (~40 days of history).
    uint32_t archival_range_threshold = 1'000'000;

    /// After this duration without finding a peer whose advertised range
    /// covers the target ledger, fall back to any ready peer. Many
    /// full-history nodes report narrow ranges but can still serve old
    /// data. 0ms = disable fallback. Default 1000ms.
    std::chrono::milliseconds peer_fallback{1000};

    /// Maximum new endpoints one ledger-specific wait may probe. Existing
    /// connected peers are handled separately by the node/header fan-out.
    std::size_t max_targeted_connect_attempts = 3;
};

class PeerSet : public std::enable_shared_from_this<PeerSet>
{
    friend class ::PeerSetTestAccess;

public:
    static std::shared_ptr<PeerSet>
    create(boost::asio::io_context& io, PeerSetOptions const& options = {})
    {
        return std::shared_ptr<PeerSet>(new PeerSet(io, options));
    }

    static std::shared_ptr<PeerSet>
    create(boost::asio::io_context& io, uint32_t network_id)
    {
        PeerSetOptions options;
        options.network_id = network_id;
        return create(io, options);
    }

    /// Wire observer callbacks (must be called after create).
    /// Uses shared_from_this — unsafe in constructor.
    void
    start();

    /// The shared tracker — peers feed it via TMStatusChange.
    std::shared_ptr<EndpointTracker>
    tracker()
    {
        return tracker_;
    }

    /// Connect to a peer and add it to the set (awaitable).
    boost::asio::awaitable<PeerGenerationPin>
    add(std::string const& host, uint16_t port);

    /// Start connecting to all bootstrap peers in parallel.
    /// Returns immediately — connections complete in background.
    void
    bootstrap();

    /// Start connecting to all undiscovered tracker endpoints in parallel.
    /// Deduplicates against in-flight and completed connections.
    void
    try_undiscovered();

    /// Set the current target ledger so queued peers are ranked toward the
    /// most promising archival candidates first.
    void
    prioritize_ledger(uint32_t ledger_seq);

    /// Seal one coordinator intent onto the exact admitted peer generation the
    /// supplied pin share names, on this PeerSet's owner strand.
    ///
    /// PeerSet is the sole minter of `PeerConnectionGenerationId` and
    /// `NodeBatchCommand`; this is that authority's only entry point. The seal
    /// commits only when the pin still names the CURRENT admitted
    /// `(session, connection_generation)` at its endpoint and that session is
    /// still ready. A stale, replaced, or foreign pin yields `std::nullopt` —
    /// it never silently retargets the intent at whatever peer is admitted now.
    ///
    /// `pin_share` is an explicit `share()`, so validation cannot be performed
    /// against a handle the caller has since dropped, and the caller's outbox
    /// keeps owning the retention.
    ///
    /// Completion is delivered on `completion_executor`, never on PeerSet's
    /// strand. Containment is therefore structural: a throwing consumer cannot
    /// unwind a PeerSet owner-strand handler, and cannot re-enter PeerSet while
    /// its own transition is still on the stack. That is why this takes an
    /// executor rather than wrapping an arbitrary callback in try/catch.
    ///
    /// Returns false when the bind projection could not be queued at all, so a
    /// caller can distinguish "accepted, await completion" from "nothing
    /// happened" instead of waiting forever on a silent failure.
    [[nodiscard]] bool
    seal_node_batch_command(
        NodeBatchIntent intent,
        PeerGenerationPin pin_share,
        boost::asio::any_io_executor completion_executor,
        std::function<void(std::optional<NodeBatchCommand>)> on_sealed);

    /// Synchronous check: any connected peer with this ledger?
    /// Returns nullptr if none found.
    PeerGenerationPin
    peer_for(uint32_t ledger_seq);

    PeerGenerationPin
    peer_for(
        uint32_t ledger_seq,
        std::unordered_set<std::string> const& excluded);

    /// Wait until any ready peer is available.
    /// Returns nullptr on timeout.
    boost::asio::awaitable<PeerGenerationPin>
    wait_for_any_peer(int timeout_secs = 15);

    boost::asio::awaitable<PeerGenerationPin>
    wait_for_any_peer(
        int timeout_secs,
        std::unordered_set<std::string> const& excluded);

    /// Wait until a peer with the given ledger is available.
    /// Returns nullptr on timeout.
    boost::asio::awaitable<PeerGenerationPin>
    wait_for_peer(
        uint32_t ledger_seq,
        int timeout_secs = 300,
        std::shared_ptr<std::atomic<bool>> cancel = nullptr);

    boost::asio::awaitable<PeerGenerationPin>
    wait_for_peer(
        uint32_t ledger_seq,
        int timeout_secs,
        std::unordered_set<std::string> const& excluded,
        std::shared_ptr<std::atomic<bool>> cancel = nullptr);

    /// Get any ready peer.
    PeerGenerationPin
    any_peer();

    PeerGenerationPin
    any_peer(std::unordered_set<std::string> const& excluded);

    struct ConnectionStats
    {
        size_t connected = 0;
        uint64_t total_connects = 0;
        uint64_t total_disconnects = 0;
    };

    /// Awaitable connection stats — runs the read on the strand.
    boost::asio::awaitable<ConnectionStats>
    co_connection_stats();

    /// Awaitable peer count — runs the read on the strand.
    boost::asio::awaitable<size_t>
    co_size();

    struct SnapshotEntry
    {
        std::string endpoint;
        bool connected = false;
        bool ready = false;
        bool in_flight = false;
        bool queued_connect = false;
        bool crawl_in_flight = false;
        bool queued_crawl = false;
        bool crawled = false;
        uint32_t first_seq = 0;
        uint32_t last_seq = 0;
        uint32_t current_seq = 0;
        std::int64_t last_seen_at = 0;
        std::int64_t last_success_at = 0;
        std::int64_t last_failure_at = 0;
        std::uint64_t success_count = 0;
        std::uint64_t failure_count = 0;
        std::uint64_t ledger_failure_count = 0;
        std::size_t recent_failed_ledgers = 0;
        std::uint64_t selection_count = 0;
        std::uint64_t last_selected_ticket = 0;
        std::map<std::string, std::string> peer_headers;
    };

    struct Snapshot
    {
        struct FailureBucket
        {
            std::string kind;
            std::size_t count = 0;
        };

        struct FailureEndpoint
        {
            std::string endpoint;
            std::size_t count = 0;
        };

        size_t known_endpoints = 0;
        size_t tracked_endpoints = 0;
        size_t connected_peers = 0;
        size_t ready_peers = 0;
        size_t hub_peers = 0;
        size_t archival_peers = 0;
        size_t in_flight_connects = 0;
        size_t queued_connects = 0;
        size_t crawl_in_flight = 0;
        size_t queued_crawls = 0;
        uint64_t total_connects = 0;
        uint64_t total_disconnects = 0;
        size_t crawled_size = 0;
        size_t failed_at_size = 0;
        std::vector<uint32_t> wanted_ledgers;
        std::vector<FailureBucket> recent_failures;
        std::vector<FailureEndpoint> top_failing_endpoints;
        std::vector<SnapshotEntry> peers;
    };

    boost::asio::awaitable<Snapshot>
    co_snapshot();

    void
    set_unsolicited_handler(UnsolicitedHandler handler);

    /// Try to connect to an endpoint. Returns nullptr on failure.
    /// Redirect IPs from 503 are fed into the tracker + tried immediately.
    boost::asio::awaitable<PeerGenerationPin>
    try_connect(std::string const& host, uint16_t port);

    /// Number of live (ready) peer connections (all types).
    size_t
    connected_count() const;

    /// Get up to N peers for a ledger, for fan-out requests.
    /// Synchronous version — must be called on strand.
    std::vector<PeerGenerationPin>
    select_peers_for(
        uint32_t ledger_seq,
        int max_count,
        std::unordered_set<std::string> const& excluded);

    /// Awaitable version — hops to strand internally.
    boost::asio::awaitable<std::vector<PeerGenerationPin>>
    co_select_peers_for(
        uint32_t ledger_seq,
        int max_count,
        std::unordered_set<std::string> excluded);

    /// A receipt for every generation-pin release command posted before this
    /// call. This does not assert that unrelated pins are absent; it proves
    /// only that PeerSet has consumed the causally preceding releases.
    boost::asio::awaitable<void>
    co_generation_pin_release_barrier();

    /// Record that a peer failed to serve a specific ledger.
    /// All peer selection methods will exclude this peer for this
    /// ledger until the TTL expires (default 60s).
    void
    note_ledger_failure(std::string const& endpoint, uint32_t ledger_seq);

    /// Number of connected archival peers (range > archival_range_threshold).
    size_t
    archival_peer_count() const;

    /// Number of connected non-archival (hub) peers.
    size_t
    hub_peer_count() const;

private:
    struct TargetedConnectAuthorization;
    using TargetedConnectAuthorizationPtr =
        std::shared_ptr<TargetedConnectAuthorization>;

    struct TargetedAdmissionLease
    {
        enum class State : uint8_t { available, consumed, revoked };

        explicit TargetedAdmissionLease(
            TargetedConnectAuthorizationPtr authorization_)
            : authorization(std::move(authorization_))
        {
        }

        bool
        is_available() const noexcept
        {
            return state.load(std::memory_order_acquire) == State::available;
        }

        bool
        try_consume() noexcept
        {
            auto expected = State::available;
            return state.compare_exchange_strong(
                expected,
                State::consumed,
                std::memory_order_acq_rel,
                std::memory_order_acquire);
        }

        void
        revoke() noexcept
        {
            auto expected = State::available;
            state.compare_exchange_strong(
                expected,
                State::revoked,
                std::memory_order_acq_rel,
                std::memory_order_acquire);
        }

        TargetedConnectAuthorizationPtr authorization;
        std::atomic<State> state{State::available};
    };
    using TargetedAdmissionLeasePtr = std::shared_ptr<TargetedAdmissionLease>;

    struct TargetedConnectAuthorization
        : std::enable_shared_from_this<TargetedConnectAuthorization>
    {
        TargetedConnectAuthorization(
            uint32_t ledger_seq_,
            std::unordered_set<std::string> excluded_)
            : ledger_seq(ledger_seq_), excluded(std::move(excluded_))
        {
        }

        bool
        is_active() const noexcept
        {
            return accepting.load(std::memory_order_acquire);
        }

        TargetedAdmissionLeasePtr
        claim();

        void
        close(bool revoke_claimed) noexcept;

        void
        invalidate() noexcept
        {
            close(true);
        }

        uint32_t ledger_seq;
        std::unordered_set<std::string> excluded;
        std::atomic<bool> accepting{true};
        std::mutex leases_mutex;
        std::vector<std::weak_ptr<TargetedAdmissionLease>> leases;
    };

    struct RotationTarget
    {
        uint32_t ledger_seq = 0;
        std::unordered_set<std::string> excluded;
        bool candidate_covers = false;
        std::string candidate_key;
        uint32_t candidate_first_seq = 0;
        uint32_t candidate_last_seq = 0;
        TargetedAdmissionLeasePtr lease;
    };

    boost::asio::awaitable<PeerSessionPtr>
    try_connect_on_strand(
        std::string host,
        uint16_t port,
        std::vector<TargetedAdmissionLeasePtr> admission_leases);

    boost::asio::awaitable<PeerSessionPtr>
    wait_for_any_peer_on_strand(
        int timeout_secs,
        std::unordered_set<std::string> excluded);

    boost::asio::awaitable<PeerSessionPtr>
    wait_for_peer_on_strand(
        uint32_t ledger_seq,
        int timeout_secs,
        std::unordered_set<std::string> excluded,
        std::shared_ptr<std::atomic<bool>> cancel);

    //@@authority transition=peer_set.mint_generation_pin owner=peer_set
    // commit=strand
    PeerGenerationPin
    make_peer_pin(PeerSessionPtr const& expected_session);

    /// The single delivery point for a seal result. Marked //@@callback-funnel
    /// in the implementation: consumer code runs on its own executor, so
    /// containment and ordering are properties of one function.
    void
    deliver_seal_result(
        boost::asio::any_io_executor completion_executor,
        std::function<void(std::optional<NodeBatchCommand>)> on_sealed,
        std::optional<NodeBatchCommand> sealed);

    /// Owner-strand half of seal_node_batch_command. Takes the intent BY VALUE
    /// so the capability can be constructed with a move: a copy here would
    /// allocate on the owner strand after every guard had already passed, which
    /// is precisely the throwing commit section the prepare/commit rule forbids.
    std::optional<NodeBatchCommand>
    seal_node_batch_command_on_strand(
        NodeBatchIntent intent,
        PeerGenerationPin const& pin);

    //@@authority transition=peer_set.release_generation_pin owner=peer_set
    // commit=strand
    void
    release_peer_pin(
        std::uint64_t pin_id,
        std::uint64_t connection_generation) noexcept;

    void
    prune_expired_peer_pins() const noexcept;

    bool
    generation_has_active_pin(std::uint64_t connection_generation) const;

    /// Remove a peer by PeerSet's own decision (eviction, tests). For
    /// deaths reported by the connection itself, use consume_peer_terminal.
    //@@authority transition=peer_set.remove_peer owner=peer_set commit=strand
    void
    remove_peer(std::string const& key, bool forget_endpoint = true);

    /// Consume one connection instance's terminal record, posted from the
    /// client strand's export commit. The consumer has authority only when
    /// the live entry IS that exact generation: record the admitted peer's
    /// failure once, erase the row, count the disconnect once. Everything
    /// else — absent, evicted, duplicate, never-admitted, or mismatched —
    /// is a pure no-op: the row itself is the dedup state (first match
    /// erases it; later copies find no matching authority), and
    /// pre-admission connect failures remain owned by the existing
    /// note_connect_failure path. Terminal delivery is NOT ordered by
    /// generation, so no scalar mark may gate consumption.
    //@@authority transition=peer_set.consume_peer_terminal owner=peer_set
    // commit=strand
    void
    consume_peer_terminal(
        std::string const& key,
        std::uint64_t generation,
        PeerTerminalReason reason);

    /// Check if we're at the hub connection cap.
    bool
    at_connection_cap() const;

    /// Check if we're at the archival peer cap.
    bool
    at_archival_cap() const;

    /// Check if a known endpoint is archival (from crawl stats).
    bool
    is_archival_endpoint(std::string const& key) const;

    /// Make room in the candidate's own pool without crossing either cap.
    bool
    make_room_for_peer(
        std::string const& candidate_key,
        PeerSessionPtr const& candidate,
        std::vector<TargetedAdmissionLeasePtr> const& admission_leases);

    /// Try to evict an idle hub that is unsuitable for the target ledger.
    bool
    evict_hub_for(RotationTarget const& target);

    /// Try to rotate an idle archival peer that failed or cannot cover the
    /// target ledger.
    bool
    evict_archival_for(RotationTarget const& target);

    bool
    peer_is_archival(PeerSessionPtr const& peer) const;

    bool
    peer_failed_for_ledger(
        std::string const& endpoint,
        uint32_t ledger_seq,
        std::chrono::steady_clock::time_point now) const;

    bool
    has_usable_covering_peer(
        uint32_t ledger_seq,
        std::unordered_set<std::string> const& excluded) const;

    bool
    would_drop_wanted_coverage(
        std::string const& victim_key,
        PeerSessionPtr const& victim,
        RotationTarget const& target,
        std::chrono::steady_clock::time_point now) const;

    std::optional<RotationTarget>
    rotation_target_for(
        std::string const& candidate_key,
        PeerSessionPtr const& candidate,
        std::vector<TargetedAdmissionLeasePtr> const& admission_leases) const;

    void
    acquire_wanted_ledger(
        uint32_t ledger_seq,
        TargetedConnectAuthorizationPtr const& authorization = nullptr);

    void
    release_wanted_ledger(
        uint32_t ledger_seq,
        TargetedConnectAuthorizationPtr const& authorization = nullptr);

    void
    finish_targeted_authorization(
        TargetedConnectAuthorizationPtr const& authorization,
        bool revoke_claimed);
    PeerSet(boost::asio::io_context& io, PeerSetOptions const& options);

    /// Schedule a background connect attempt (deduped, fire-and-forget).
    void
    start_connect(std::string const& host, uint16_t port);

    void
    start_connect(std::string const& endpoint);

    void
    queue_connect(std::string const& endpoint);

    void
    queue_crawl(std::string const& endpoint);

    void
    pump_connects();

    void
    pump_crawls();

    void
    sort_pending_connects();

    void
    sort_pending_crawls();

    void
    load_cached_endpoints();

    void
    configure_tracker_persistence();

    void
    start_tracked_endpoints();

    std::vector<std::string>
    try_candidates_for(
        TargetedConnectAuthorizationPtr const& authorization,
        std::unordered_set<std::string> const& selection_excluded,
        std::size_t connect_budget);

    /// Remove stale endpoint_stats_ entries to bound memory.
    /// Keeps entries that are connected, in-flight, queued, or recently seen.
    /// Also prunes expired failed_ledgers sub-entries.
    void
    prune_endpoint_stats();

    /// Bound long-lived discovery state (tracker, crawl history, backoff map).
    void
    prune_discovery_state();

    void
    update_endpoint_stats(PeerEndpointCache::Entry const& entry);

    void
    note_discovered(
        std::string const& endpoint,
        DiscoverySource source = DiscoverySource::Unknown);

    void
    note_status(std::string const& endpoint, PeerStatus const& status);

    void
    note_connect_success(std::string const& endpoint, PeerStatus const& status);

    void
    note_peer_headers(
        std::string const& endpoint,
        std::map<std::string, std::string> const& headers);

    void
    note_connect_failure(std::string const& endpoint, std::string detail = {});

    void
    note_crawl_failure(std::string const& endpoint, std::string detail);

    std::chrono::steady_clock::duration
    retry_backoff_for(std::string const& endpoint) const;

    void
    record_failure_event(
        std::string const& endpoint,
        std::string const& kind,
        std::chrono::steady_clock::time_point now);

    void
    prune_recent_failure_events(std::chrono::steady_clock::time_point now);

    std::shared_ptr<boost::asio::steady_timer>
    attach_wait_signal();

    void
    notify_waiters();

    PeerSessionPtr
    choose_any_peer(std::unordered_set<std::string> const& excluded);

    PeerSessionPtr
    choose_peer_for(
        uint32_t ledger_seq,
        std::unordered_set<std::string> const& excluded);

    void
    note_peer_selected(std::string const& key);

    std::optional<PeerStatus>
    choose_crawl_status(std::vector<CrawlLedgerRange> const& ranges) const;

    bool
    endpoint_has_range(std::string const& endpoint) const;

    bool
    endpoint_covers_preferred_ledger(std::string const& endpoint) const;

    bool
    should_connect_endpoint(std::string const& endpoint) const;

    bool
    candidate_better(std::string const& lhs, std::string const& rhs) const;

    Snapshot
    snapshot_unsafe() const;

    static std::int64_t
    now_unix();

    static std::string
    make_key(std::string const& host, uint16_t port)
    {
        return host + ":" + std::to_string(port);
    }

    boost::asio::io_context& io_;
    // TODO: mutable only for ASSERT_ON_STRAND() in const methods — revisit
    mutable boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    PeerSetOptions options_;
    uint32_t network_id_;
    std::string net_label_;
    std::shared_ptr<EndpointTracker> tracker_;
    std::shared_ptr<PeerEndpointCache> endpoint_cache_;
    struct EndpointStats
    {
        PeerStatus status;
        std::int64_t last_seen_at = 0;
        std::int64_t last_success_at = 0;
        std::int64_t last_failure_at = 0;
        std::uint64_t success_count = 0;
        std::uint64_t failure_count = 0;
        std::uint64_t ledger_failure_count = 0;
        std::chrono::steady_clock::time_point last_ledger_failure_at{};
        std::uint64_t selection_count = 0;
        std::uint64_t last_selected_ticket = 0;
        bool seen_crawl = false;
        bool seen_endpoints = false;
        bool seen_redirect = false;
        bool connected_ok = false;
        std::chrono::steady_clock::time_point crawl_private_until{};

        // Ledger seqs this peer has failed to serve. Entries expire
        // after failed_ledger_ttl_secs (default 60s) so peers get
        // fresh chances as they recover or cache data.
        struct LedgerFailure
        {
            std::chrono::steady_clock::time_point at;
        };
        std::unordered_map<uint32_t, LedgerFailure> failed_ledgers;
    };
    std::unordered_map<std::string, EndpointStats> endpoint_stats_;
    std::uint64_t next_selection_ticket_ = 0;
    /// One admitted connection instance: the session plus the generation
    /// minted for it at candidate creation. The generation is PeerSet-owned
    /// instance identity — endpoint strings alone cannot distinguish an old
    /// instance's delayed death from the instance currently admitted.
    //@@authority state=peer_set.connections v4=S16 owner=peer_set
    struct ConnectionEntry
    {
        PeerSessionPtr session;
        std::uint64_t generation = 0;
    };
    std::map<std::string, ConnectionEntry> connections_;
    /// Strand-confined mint for connection-instance identity. Non-reused.
    std::uint64_t next_connection_generation_ = 0;
    struct ActivePeerPin
    {
        std::string endpoint;
        std::uint64_t connection_generation = 0;
        std::weak_ptr<void> live_handle;
    };
    //@@authority state=peer_set.active_generation_pins owner=peer_set
    mutable std::map<std::uint64_t, ActivePeerPin> active_peer_pins_;
    std::uint64_t next_peer_pin_id_ = 0;
    uint64_t total_connects_ = 0;
    uint64_t total_disconnects_ = 0;
    std::map<std::string, std::chrono::steady_clock::time_point> failed_at_;
    struct FailureEvent
    {
        std::chrono::steady_clock::time_point at;
        std::string endpoint;
        std::string kind;
    };
    std::deque<FailureEvent> recent_failures_;
    std::set<std::string> in_flight_;  // currently connecting
    std::set<std::string> queued_;
    // Request-scoped authority for queued ledger-specific replacement probes.
    std::map<std::string, std::vector<TargetedConnectAuthorizationPtr>>
        targeted_connects_;
    std::vector<std::string> pending_connects_;
    std::set<std::string> crawl_in_flight_;
    std::set<std::string> crawl_queued_;
    std::map<std::string, std::chrono::steady_clock::time_point> crawled_;
    std::vector<std::string> pending_crawls_;
    // Active target ledgers from in-flight wait_for_peer calls.
    // Used by candidate admission and same-pool rotation to prioritize peers
    // that can serve any active target. Counts preserve concurrent waiters.
    std::map<uint32_t, std::size_t> wanted_ledgers_;
    std::vector<TargetedConnectAuthorizationPtr> active_target_authorizations_;
    // Persistent one-shot priorities registered through prioritize_ledger().
    std::set<uint32_t> prioritized_ledgers_;
    std::shared_ptr<boost::asio::steady_timer> wait_signal_;
    mutable std::mutex unsolicited_handler_mutex_;
    UnsolicitedHandler unsolicited_handler_;
};

}  // namespace catl::peer_client
