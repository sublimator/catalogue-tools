#pragma once

// PeerSet — manages multiple peer connections with ledger range awareness.
//
// Wraps EndpointTracker (knows ranges) with live PeerClient connections.
// bootstrap() and try_undiscovered() launch background connection attempts,
// while wait_for_any_peer()/wait_for_covering_peer() wait for usable results.
//
// PeerSet-owned state is serialized through its strand. Detached connect
// coroutines keep PeerSet alive by capturing shared_ptr<PeerSet>.

#include "endpoint-tracker.h"
#include "history-demand.h"
#include "history-search.h"
#include "node-transport.h"
#include "peer-crawl-client.h"
#include "peer-endpoint-cache.h"
#include "peer-generation-pin.h"
#include "peer-lifecycle.h"
#include "peer-session.h"
#include "peer-use.h"

#include <array>
#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <chrono>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stop_token>
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

    /// Maximum new endpoints one ledger-specific wait may probe. Existing
    /// connected peers are handled separately by the node/header fan-out.
    std::size_t max_targeted_connect_attempts = 3;

    /// Non-dialling Cut-3 parent limits. Child acquisitions borrow these
    /// counters; they never receive private copies.
    std::size_t max_active_ledger_demands = 512;
    std::size_t max_acquisitions_per_demand = 64;
    std::size_t max_history_promotions_per_demand = 1;
    std::size_t max_history_blind_uses_per_demand = 1;

    /// Retain bounded /crawl claims for diagnostics. This side table has no
    /// path into discovery, ranking, dial admission, or persistent storage.
    bool capture_crawl_observations = true;

    /// Cut-4 process-local physical and rate bounds. Admitted capacity remains
    /// the configured hub plus archival limits above; transient and reserve
    /// are independent parts of the one physical census.
    std::size_t max_transient_peer_generations = 64;
    std::size_t physical_drain_reserve = 16;
    std::size_t max_global_history_dials = 32;
    std::size_t max_global_history_blind_uses = 8;
    std::chrono::milliseconds global_history_token_refill{250};
    std::size_t max_history_attachments_per_generation = 64;
    std::chrono::milliseconds history_connect_timeout{10'000};
};

/// Read-only diagnostic projection of the currently connected peer set.
///
/// The ranges are the merged union advertised by ready peers at snapshot time.
/// This value owns no selection authority and is never retained by PeerSet.
struct PeerHistoryCoverage
{
    bool observed = false;
    std::size_t connected_peers = 0;
    std::size_t ready_peers = 0;
    std::size_t advertising_peers = 0;
    std::vector<AdvertisedLedgerRange> advertised_ranges;
};

/// Compact human-readable form suitable for status and terminal diagnostics.
std::string
describe_peer_history_coverage(PeerHistoryCoverage const& coverage);

/** Result of one fresh owner-side replan and attempted exact grant spend. */
struct HistorySearchExecution
{
    std::optional<HistorySearchPlan> plan;
    std::optional<HistoryGrantSpendReceipt> spend;
    std::optional<HistoryAuthorityTarget> created_generation;
    PeerUse peer_use;

    bool
    committed() const noexcept
    {
        return spend.has_value();
    }
};

/** Terminal or exact-use result of one continuously revised participant. */
struct HistorySearchDriveResult
{
    HistoryInterestRef interest;
    PeerUse peer_use;
    std::optional<HistoryAcquisitionTerminal> local_terminal;
    std::optional<SharedHistoryFetchTerminal> shared_terminal;
    std::optional<HistorySearchPlan> last_plan;
    std::size_t replans = 0;
    std::size_t grants = 0;
    std::size_t commits = 0;

    bool
    acquired() const noexcept
    {
        return static_cast<bool>(peer_use);
    }
};

class PeerSet : public std::enable_shared_from_this<PeerSet>
{
    friend class ::PeerSetTestAccess;
    friend class SharedHistoryFetch;

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

    /// Seal every PeerSet admission root, let predecessor-held generation
    /// pins drain, close each admitted session on its own strand, and wait for
    /// all PeerSet-owned connect/crawl servants to finish.
    ///
    /// The caller must stop and drain predecessor services first. Completion
    /// proves PeerSet quiescence only; it does not prove NodeCache or
    /// coordinator quiescence.
    boost::asio::awaitable<void>
    co_stop();

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

    /// Queue one configured endpoint through the PeerSet-owned servant census.
    /// Unlike a detached caller awaiting try_connect(), this operation is part
    /// of co_stop()'s exhaustive connect drain.
    void
    start_connect(std::string const& host, uint16_t port);

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

    /// Seal against one already-installed exact ledger-node use.
    [[nodiscard]] bool
    seal_node_batch_command_on_use(
        NodeBatchIntent intent,
        PeerUseCompletion use,
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

    /// Wait only for a ready peer whose advertised range covers the ledger.
    boost::asio::awaitable<PeerGenerationPin>
    wait_for_covering_peer(
        uint32_t ledger_seq,
        int timeout_secs = 300,
        std::shared_ptr<std::atomic<bool>> cancel = nullptr);

    boost::asio::awaitable<PeerGenerationPin>
    wait_for_covering_peer(
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

    /// Snapshot current ready-peer history coverage on the PeerSet strand.
    boost::asio::awaitable<PeerHistoryCoverage>
    co_history_coverage();

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
        bool targeted_connect = false;
        bool range_is_persistent_hint = false;
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
        std::vector<uint32_t> targeted_ledgers;
        std::map<std::string, std::string> peer_headers;
    };

    struct Snapshot
    {
        struct ReportedCrawlObservation
        {
            std::string reporter_endpoint;
            std::string subject_endpoint;
            std::optional<std::string> reported_subject_key;
            std::vector<CrawlLedgerRange> intervals;
            std::int64_t observed_at = 0;
            std::uint64_t crawl_epoch = 0;
            bool truncated = false;
            bool reported_key_conflict = false;
        };

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
        size_t targeted_connects = 0;
        size_t cached_endpoint_limit = 0;
        size_t max_targeted_connect_attempts = 0;
        std::string endpoint_cache_state;
        std::string endpoint_cache_error;
        uint64_t total_connects = 0;
        uint64_t total_disconnects = 0;
        size_t crawled_size = 0;
        size_t crawl_observation_batches = 0;
        size_t crawl_observation_count = 0;
        std::uint64_t crawl_truncated_peer_rows = 0;
        std::uint64_t crawl_truncated_interval_rows = 0;
        std::uint64_t crawl_truncated_public_keys = 0;
        std::uint64_t crawl_invalid_public_keys = 0;
        std::uint64_t crawl_malformed_interval_tokens = 0;
        std::uint64_t crawl_truncated_observations = 0;
        std::uint64_t crawl_observation_capture_failures = 0;
        size_t failed_at_size = 0;
        std::vector<uint32_t> wanted_ledgers;
        std::vector<FailureBucket> recent_failures;
        std::vector<FailureEndpoint> top_failing_endpoints;
        std::vector<ReportedCrawlObservation> reported_crawl_observations;
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

    /// Strict fan-out selection. Every returned peer explicitly advertises the
    /// target ledger; unknown and non-covering ranges are excluded.
    std::vector<PeerGenerationPin>
    select_covering_peers_for(
        uint32_t ledger_seq,
        int max_count,
        std::unordered_set<std::string> const& excluded);

    boost::asio::awaitable<std::vector<PeerGenerationPin>>
    co_select_covering_peers_for(
        uint32_t ledger_seq,
        int max_count,
        std::unordered_set<std::string> excluded);

    /// Revalidate one inert generation reference and install exactly one
    /// request-scoped use before the caller may touch the pooled session.
    boost::asio::awaitable<PeerUse>
    co_acquire_peer_use(PeerGenerationRef generation, PeerUseRequest request);

    /// Atomically get or create the sole parent for (request scope, ledger).
    /// The returned value is inert; Cut 3 records cannot create network work.
    boost::asio::awaitable<LedgerDemandRef>
    co_get_or_create_ledger_demand(
        std::shared_ptr<catl::core::RequestScope> request_scope,
        LedgerDemandOptions options);

    /// Mint one child identity under an existing live parent. The child owns
    /// exclusions and request context only; every finite counter remains on
    /// the parent row.
    boost::asio::awaitable<HistoryAcquisitionRef>
    co_open_history_acquisition(
        LedgerDemandRef demand,
        HistoryAcquisitionOptions options);

    /// Add one exact endpoint to a live child's bounded retry exclusions.
    boost::asio::awaitable<bool>
    co_exclude_history_endpoint(
        HistoryAcquisitionRef acquisition, std::string endpoint);

    /// Acknowledged child terminal transition. First terminal wins.
    boost::asio::awaitable<void>
    co_finish_history_acquisition(
        HistoryAcquisitionRef acquisition,
        HistoryAcquisitionTerminal terminal);

    /// Owner-strand diagnostic snapshot; no scheduling capability escapes.
    boost::asio::awaitable<std::optional<LedgerDemandSnapshot>>
    co_ledger_demand_snapshot(LedgerDemandRef demand);

    /// Build one authority-free Cut-4 pool projection for an exact child.
    /// Supplying the current interest adds its complete control revision for
    /// final grant binding; omitting it is sufficient for proposal planning.
    boost::asio::awaitable<HistorySearchSnapshot>
    co_history_search_snapshot(
        HistoryAcquisitionRef acquisition,
        std::optional<HistoryInterestRef> interest = std::nullopt);

    /// Replan from current canonical state and, for a ready generation action,
    /// atomically spend the participant/request authority while installing its
    /// exact PeerUse. Creation and attachment plans remain inert until their
    /// owner rows are implemented by the following Cut-4 slice.
    boost::asio::awaitable<HistorySearchExecution>
    co_execute_history_grant(
        std::shared_ptr<SharedHistoryFetch> fetch,
        HistoryAuthorityGrantRef grant,
        PeerUseKind kind);

    /// Continuously revise one exact shared-fetch participant until it owns a
    /// request-scoped use or reaches a typed local/shared terminal. Every wait
    /// is tied to a pool revision signal or the aggregate exact wake/deadline.
    boost::asio::awaitable<HistorySearchDriveResult>
    co_drive_history_interest(
        std::shared_ptr<SharedHistoryFetch> fetch,
        HistoryAcquisitionRef acquisition,
        HistoryInterestRef interest,
        PeerUseKind kind);

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
    struct PreparedPeerUse;
    struct LedgerDemandRecord;
    struct HistoryAcquisitionRecord;
    struct PeerGeneration;

    enum class Lifecycle : std::uint8_t { running, stopping, stopped };

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
            std::unordered_set<std::string> excluded_,
            std::chrono::steady_clock::time_point deadline_ =
                std::chrono::steady_clock::now() + std::chrono::seconds(60))
            : ledger_seq(ledger_seq_)
            , excluded(std::move(excluded_))
            , deadline(deadline_)
        {
        }

        bool
        is_active() const noexcept
        {
            return is_active_at(std::chrono::steady_clock::now());
        }

        bool
        is_active_at(std::chrono::steady_clock::time_point now) const noexcept
        {
            return accepting.load(std::memory_order_acquire) && now < deadline;
        }

        TargetedAdmissionLeasePtr
        claim();

        TargetedAdmissionLeasePtr
        claim_at(std::chrono::steady_clock::time_point now);

        void
        close(bool revoke_claimed) noexcept;

        void
        invalidate() noexcept
        {
            close(true);
        }

        uint32_t ledger_seq;
        std::unordered_set<std::string> excluded;
        std::chrono::steady_clock::time_point deadline;
        std::atomic<bool> accepting{true};
        std::mutex leases_mutex;
        std::vector<std::weak_ptr<TargetedAdmissionLease>> leases;
    };

    static constexpr std::size_t kMaxQueuedTargetAuthorizations = 64;
    static constexpr std::size_t kMaxHistoryRedirectSuccessors = 64;
    static constexpr auto kOrdinaryConnectDeadline = std::chrono::seconds(60);

    struct QueuedConnectAuthority
    {
        std::uint64_t generation = 0;
        std::chrono::steady_clock::time_point physical_deadline{};
        bool history_demand_authorized = false;
        bool ordinary_discovery_authorized = false;
        std::chrono::steady_clock::time_point ordinary_deadline{};
        std::vector<TargetedConnectAuthorizationPtr> target_authorizations;
    };

    struct ClaimedConnectAuthority
    {
        std::uint64_t generation = 0;
        std::chrono::steady_clock::time_point physical_deadline{};
        std::optional<std::chrono::steady_clock::time_point> absolute_deadline;
        std::vector<TargetedAdmissionLeasePtr> admission_leases;
    };

    static ClaimedConnectAuthority
    claim_queued_connect_authority(
        QueuedConnectAuthority authority,
        std::chrono::steady_clock::time_point now);

    struct RotationTarget
    {
        uint32_t ledger_seq = 0;
        std::unordered_set<std::string> excluded;
        std::string candidate_key;
        uint32_t candidate_first_seq = 0;
        uint32_t candidate_last_seq = 0;
        TargetedAdmissionLeasePtr lease;
    };

    boost::asio::awaitable<PeerSessionPtr>
    try_connect_on_strand(
        std::string host,
        uint16_t port,
        std::uint64_t reserved_generation,
        std::vector<TargetedAdmissionLeasePtr> admission_leases,
        std::optional<std::chrono::steady_clock::time_point> absolute_deadline =
            std::nullopt);

    boost::asio::awaitable<PeerSessionPtr>
    wait_for_any_peer_on_strand(
        int timeout_secs,
        std::unordered_set<std::string> excluded);

    boost::asio::awaitable<PeerSessionPtr>
    wait_for_covering_peer_on_strand(
        uint32_t ledger_seq,
        int timeout_secs,
        std::unordered_set<std::string> excluded,
        std::shared_ptr<std::atomic<bool>> cancel);

    //@@authority transition=peer_set.mint_generation_pin owner=peer_set
    // bind=catl::peer_client::PeerSet::make_peer_pin@04236598c8359c88
    // commit=strand
    PeerGenerationPin
    make_peer_pin(PeerSessionPtr const& expected_session);

    PeerGenerationRef
    make_generation_ref(PeerSessionPtr const& expected_session) const;

    PeerUse
    make_peer_use(
        PeerGenerationRef const& generation,
        PeerUseRequest const& request);

    std::unique_ptr<PreparedPeerUse>
    prepare_peer_use(
        PeerGenerationRef const& generation,
        PeerUseRequest const& request,
        bool require_coverage,
        bool allow_ready_transient = false) noexcept;

    PeerUse
    commit_prepared_peer_use(PreparedPeerUse& prepared) noexcept;

    void
    finish_peer_use(
        PeerUseTicket ticket,
        PeerUseOutcome outcome,
        detail::PeerUseProposalSource source) noexcept;

    void
    finalize_peer_use(PeerUseTicket ticket, PeerUseOutcome outcome) noexcept;

    void
    begin_forced_peer_use_finish(
        PeerUseTicket ticket,
        PeerUseOutcome outcome) noexcept;

    enum class GenerationDrainCause : std::uint8_t {
        peer_terminal,
        local_use,
        shutdown,
    };

    void
    mark_generation_peer_uses_finishing(
        std::uint64_t generation,
        GenerationDrainCause cause,
        PeerUseOutcome outcome,
        std::optional<PeerUseTicket> trigger = std::nullopt) noexcept;

    void
    finish_generation_peer_uses_after_drain(
        std::uint64_t generation,
        PeerUseOutcome fallback) noexcept;

    void
    fail_generation_peer_uses_without_drain(std::uint64_t generation) noexcept;

    void
    complete_generation_drain_servant(
        std::uint64_t generation,
        PeerSessionPtr const& session,
        std::exception_ptr failure) noexcept;

    void
    expire_peer_use(PeerUseTicket ticket) noexcept;

    void
    cancel_all_peer_uses_for_shutdown() noexcept;

    LedgerDemandRef
    get_or_create_ledger_demand_on_strand(
        std::shared_ptr<catl::core::RequestScope> const& request_scope,
        LedgerDemandOptions options);

    HistoryAcquisitionRef
    open_history_acquisition_on_strand(
        LedgerDemandRef const& demand,
        HistoryAcquisitionOptions options);

    bool
    exclude_history_endpoint_on_strand(
        HistoryAcquisitionRef const& acquisition, std::string endpoint);

    void
    finish_history_acquisition_on_strand(
        HistoryAcquisitionRef const& acquisition,
        HistoryAcquisitionTerminal terminal) noexcept;

    void
    release_history_acquisition_attachments(AcquisitionId acquisition) noexcept;

    void
    release_history_acquisition_origins(AcquisitionId acquisition) noexcept;

    void
    release_generation_history_attachments(
        PeerGeneration& generation) noexcept;

    void
    release_generation_history_origin(
        PeerGeneration& generation,
        HistoryWorkReceipt receipt) noexcept;

    void
    on_history_generation_watchdog(
        std::string const& endpoint,
        std::uint64_t generation) noexcept;

    std::optional<std::pair<std::size_t, std::size_t>>
    capture_history_redirect_successors(
        std::string const& endpoint,
        std::uint64_t generation,
        std::uint16_t port,
        std::set<std::string> const& successors) noexcept;

    void
    terminalize_ledger_demand(
        LedgerDemandId demand,
        LedgerDemandTerminal terminal) noexcept;

    void
    on_request_scope_terminal(
        LedgerDemandId demand,
        catl::core::RequestScopeId request_scope,
        catl::core::RequestScopeTerminal terminal) noexcept;

    void
    on_ledger_demand_deadline(LedgerDemandId demand) noexcept;

    void
    prune_ledger_demand_tombstones() noexcept;

    HistorySearchSnapshot
    history_search_snapshot_on_strand(
        HistoryAcquisitionRef const& acquisition,
        std::optional<HistoryInterestRef> const& interest);

    HistorySearchSnapshot
    project_history_search_snapshot(
        HistoryAcquisitionRecord const& acquisition,
        LedgerDemandRecord& demand,
        HistoryControlRevision control);

    HistorySearchExecution
    execute_history_grant_on_strand(
        std::shared_ptr<SharedHistoryFetch> const& fetch,
        HistoryAuthorityGrantRef const& grant,
        PeerUseKind kind);

    boost::asio::awaitable<HistorySearchDriveResult>
    drive_history_interest_on_strand(
        std::shared_ptr<SharedHistoryFetch> fetch,
        HistoryAcquisitionRef acquisition,
        HistoryInterestRef interest,
        PeerUseKind kind);

    void
    refresh_global_history_tokens(
        std::chrono::steady_clock::time_point now) noexcept;

    void
    advance_history_pool_revision() noexcept;

    HistoryPhysicalCensus
    physical_census_on_strand() const noexcept;

    /// Default-inert initiation failpoint, settable only through the declared
    /// test seam. Proving that a failed projection returns false requires a
    /// deterministic throw inside the public function's try, immediately before
    /// asio::post. A stopped io_context is not a substitute: Asio may accept
    /// work that never runs, which tests liveness rather than initiation.
    bool fail_seal_projection_for_test_ = false;

    /// Deterministically discard owner-proposal posts without disabling the
    /// independent deadline servant. Cross-thread because proposals may come
    /// from any PeerSession callback executor.
    std::atomic<std::uint32_t> drop_peer_use_finish_posts_for_test_{0};
    std::uint32_t fail_generation_drain_launches_for_test_ = 0;
    std::uint32_t fail_generation_drain_servants_after_start_for_test_ = 0;
    std::uint32_t fail_generation_drain_retry_waits_for_test_ = 0;

    /// Drop the seal COMPLETION post, reproducing the one failure this class
    /// deliberately does not recover from: the capability is sealed and then
    /// the completion cannot be queued, so the caller is never told anything.
    ///
    /// Distinct from fail_seal_projection_for_test_, which fails initiation and
    /// IS reported through the bool return. This one is unreportable by
    /// construction — that is the whole point, and it is why the coordinator
    /// needs a deadline. A coordinator-side simulation could not stand in for
    /// it: the coordinator's own callback would still run, so its accounting
    /// would still settle, and a real loss leaves the projection permanently
    /// unobserved.
    ///
    /// A COUNT, not a flag, and one-shot by default in tests. A sticky flag has
    /// to be disarmed mid-flight, and with a zero bind deadline the retry races
    /// the timer, so "drop exactly the first completion" is not expressible by
    /// stepping — the first attempt to write that control burned the whole
    /// retry budget with the flag still armed.
    std::uint32_t drop_seal_completions_for_test_ = 0;
    std::uint32_t fail_connect_servant_launches_for_test_ = 0;
    std::uint32_t fail_crawl_servant_launches_for_test_ = 0;
    std::uint32_t fail_crawl_completion_setup_for_test_ = 0;
    std::function<boost::asio::awaitable<void>(
        std::shared_ptr<PeerClient> const&)>
        after_connect_ready_before_admission_for_test_;
    std::function<void()> connect_failure_classification_hook_for_test_;
    std::function<void()> connect_completion_diagnostic_hook_for_test_;
    std::function<void()> crawl_completion_diagnostic_hook_for_test_;
    std::function<void()> before_crawl_fallback_stop_check_for_test_;
    std::function<void()> before_crawl_https_fallback_for_test_;
    std::function<void()> after_crawl_ingress_seal_for_test_;
    std::function<void()> before_crawl_observation_build_for_test_;
    std::function<void()> before_crawl_observation_commit_for_test_;
    std::function<void(std::string const&, std::function<void()>)>
        defer_crawl_servant_receipt_for_test_;
    /// Pause after one owner-strand generation-pin barrier has consumed its
    /// causal prefix but before that barrier receipt returns to its caller.
    /// Production leaves this empty.
    std::function<void()>
        before_generation_pin_release_barrier_return_for_test_;
    /// Pause after the aggregate stopped fact is committed and joined waiters
    /// are notified, but before co_stop returns its receipt. Production leaves
    /// this empty.
    std::function<void()> before_stop_receipt_return_for_test_;

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
    /// is precisely the throwing commit section the prepare/commit rule
    /// forbids.
    std::optional<NodeBatchCommand>
    seal_node_batch_command_on_strand(
        NodeBatchIntent intent,
        PeerGenerationPin const& pin);

    std::optional<NodeBatchCommand>
    seal_node_batch_command_on_use_on_strand(
        NodeBatchIntent intent,
        PeerUseCompletion const& use);

    //@@authority transition=peer_set.release_generation_pin owner=peer_set
    // bind=catl::peer_client::PeerSet::release_peer_pin@ce1f535e08044301
    // commit=strand
    void
    release_peer_pin(
        std::uint64_t pin_id,
        std::uint64_t connection_generation) noexcept;

    void
    prune_expired_peer_pins() const noexcept;

    bool
    generation_has_active_pin(std::uint64_t connection_generation) const;

    void
    consume_lifecycle_event(PeerLifecycleEvent event);

    void
    install_generation_session(
        std::string const& key,
        std::uint64_t generation,
        PeerSessionPtr const& session) noexcept;

    std::uint64_t
    reserve_connection_generation(
        std::string const& key,
        std::chrono::steady_clock::time_point physical_deadline);

    [[nodiscard]] bool
    replaceable_generation_tombstone(PeerGeneration const& generation) const
        noexcept;

    void
    abandon_unstarted_generation(
        std::string const& key,
        std::uint64_t generation) noexcept;

    void
    begin_generation_close(
        std::string const& key,
        std::uint64_t generation) noexcept;

    PeerStatusSnapshot const*
    ready_status_for(std::string const& key, std::uint64_t generation)
        const noexcept;

    PeerStatusSnapshot const*
    ready_status_for(PeerSessionPtr const& session) const noexcept;

    [[nodiscard]] bool
    start_generation_drain(
        std::string const& key,
        std::uint64_t generation,
        GenerationDrainCause cause = GenerationDrainCause::peer_terminal,
        PeerUseOutcome trigger_outcome = PeerUseOutcome::transport_failure,
        std::optional<PeerUseTicket> trigger = std::nullopt) noexcept;

    /// Remove a peer by PeerSet's own decision (eviction, tests). For
    /// deaths reported by the connection itself, use consume_peer_terminal.
    //@@authority transition=peer_set.remove_peer owner=peer_set commit=strand
    // bind=catl::peer_client::PeerSet::remove_peer@9c8561b27d00c927
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
    // bind=catl::peer_client::PeerSet::consume_peer_terminal@8bfce9e29581687d
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
    start_connect(std::string const& endpoint);

    void
    queue_connect(std::string const& endpoint);

    bool
    queue_targeted_connect(
        std::string const& endpoint,
        TargetedConnectAuthorizationPtr const& authorization);

    void
    queue_crawl(std::string const& endpoint);

    void
    pump_connects();

    void
    pump_crawls();

    void
    begin_stop_on_strand();

    void
    notify_shutdown_progress() noexcept;

    struct CrawlServantOutcome
    {
        std::size_t raw = 0;
        std::size_t accepted = 0;
        std::size_t ranged = 0;
        std::size_t filtered = 0;
        std::size_t truncated_peer_rows = 0;
        std::size_t truncated_interval_rows = 0;
        std::size_t truncated_public_keys = 0;
        bool used_tls = false;
    };

    bool
    capture_crawl_observations(
        std::string const& reporter,
        CrawlResponse const& response) noexcept;

    void
    apply_crawl_response(
        std::string const& reporter,
        CrawlResponse const& response,
        CrawlServantOutcome& outcome);

    enum class SuccessorPump : std::uint8_t { run, defer };

    void
    complete_connect_servant(
        std::string const& key,
        std::exception_ptr failure,
        SuccessorPump successor = SuccessorPump::run) noexcept;

    boost::asio::awaitable<void>
    finish_connect_servant(
        std::string key,
        std::exception_ptr failure,
        SuccessorPump successor = SuccessorPump::run);

    void
    install_connect_servant_session(
        std::string const& key,
        std::uint64_t generation,
        PeerSessionPtr const& session) noexcept;

    void
    classify_unadmitted_connect_terminal(
        std::string const& key,
        std::uint64_t generation,
        std::shared_ptr<PeerClient> const& client);

    void
    complete_crawl_servant(
        std::string const& key,
        CrawlServantOutcome const& outcome,
        std::exception_ptr failure,
        SuccessorPump successor = SuccessorPump::run) noexcept;

    bool
    shutdown_drained_on_strand() const noexcept;

    void
    sort_pending_connects();

    void
    sort_pending_crawls();

    void
    load_cached_endpoints();

    void
    restore_persistent_failure(
        std::string const& endpoint,
        PeerEndpointCache::Entry const& entry,
        std::chrono::steady_clock::time_point steady_now,
        std::int64_t unix_now);

    void
    configure_tracker_persistence();

    void
    start_tracked_endpoints();

    std::vector<std::string>
    load_cached_candidates_for(uint32_t ledger_seq);

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
    notify_waiters() noexcept;

    void
    wake_waiters_without_revision() noexcept;

    void
    post_history_fetch_wake() noexcept;

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
        auto normalized =
            EndpointTracker::normalize_endpoint_identity(host, port);
        return normalized ? std::move(*normalized) : std::string{};
    }

    boost::asio::io_context& io_;
    // TODO: mutable only for ASSERT_ON_STRAND() in const methods — revisit
    mutable boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    PeerSetOptions options_;
    uint32_t network_id_;
    std::string net_label_;
    std::shared_ptr<EndpointTracker> tracker_;
    std::shared_ptr<PeerEndpointCache> endpoint_cache_;
    /// Immediate cross-thread admission gate. The strand-owned lifecycle below
    /// remains the sole authority for settlement and the stop receipt.
    //@@authority state=prv.S0034 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::shutdown_ingress_sealed_@c68580f2aee79d2b
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=cross_thread_peer_ingress_shutdown_seal
    std::atomic<bool> shutdown_ingress_sealed_{false};
    // clang-format off
    //@@authority state=prv.S0062 status=provisional scope=field bind=catl::peer_client::PeerSet::crawl_stop_source_@53c130b70c160a7e
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=cross_thread_retained_crawl_stop_fact
    std::stop_source crawl_stop_source_;
    // clang-format on
    //@@authority state=prv.S0035 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::lifecycle_@189b670b4df58ed4
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=peer_set_admission_and_drain_lifecycle
    Lifecycle lifecycle_ = Lifecycle::running;
    //@@authority state=prv.S0036 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::shutdown_waiters_@945bb664bfc34337
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=joined_peer_set_stop_waiter_registry
    std::vector<std::weak_ptr<boost::asio::steady_timer>> shutdown_waiters_;
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
        bool range_is_current_live = false;
        bool range_is_persistent_hint = false;
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
    //@@authority state=prv.S0008 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::endpoint_stats_@9bdf35aba9a4d0ef
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=per_peer_ledger_failure_and_selection_evidence
    std::unordered_map<std::string, EndpointStats> endpoint_stats_;
    std::uint64_t next_selection_ticket_ = 0;
    /// One admitted connection instance: the session plus the generation
    /// minted for it at candidate creation. The generation is PeerSet-owned
    /// instance identity — endpoint strings alone cannot distinguish an old
    /// instance's delayed death from the instance currently admitted.
    struct ConnectionEntry
    {
        PeerSessionPtr session;
        std::uint64_t generation = 0;
    };
    //@@authority state=peer_set.connections v4=S16 owner=peer_set
    // bind=catl::peer_client::PeerSet::connections_@cd8f1261656861a3
    std::map<std::string, ConnectionEntry> connections_;
    /// Strand-confined mint for connection-instance identity. Non-reused.
    std::uint64_t next_connection_generation_ = 0;
    struct HistoryGenerationAttachment
    {
        HistoryControlRevision control;
        std::shared_ptr<SharedHistoryFetch> fetch;
        HistoryAuthorityGrantRef grant;
        std::chrono::steady_clock::time_point deadline{};
    };
    struct HistoryGenerationOrigin
    {
        HistoryControlRevision creation_control;
        HistorySourceLane source = HistorySourceLane::crawl_only;
        HistoryControlRevision retainer_control;
        std::shared_ptr<SharedHistoryFetch> retainer_fetch;
        HistoryAuthorityGrantRef retainer_grant;
        bool close_requested = false;
    };
    struct PeerGeneration
    {
        PeerGeneration(
            std::string endpoint,
            std::uint64_t generation,
            std::chrono::steady_clock::time_point physical_deadline =
                std::chrono::steady_clock::time_point::max())
            : lifecycle(std::move(endpoint), generation)
            , physical_deadline(physical_deadline)
        {
        }

        PeerGenerationLifecycle lifecycle;
        PeerSessionPtr session;
        std::set<std::uint64_t> active_use_ids;
        //@@authority state=prv.S0119 status=provisional scope=field bind=catl::peer_client::PeerSet::PeerGeneration::history_origin@aaa91bca6043e702
        // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
        // label=immutable_exact_history_demand_generation_origin
        std::optional<HistoryGenerationOrigin> history_origin;
        //@@authority state=prv.S0116 status=provisional scope=field bind=catl::peer_client::PeerSet::PeerGeneration::history_attachments@2a3b072ed8f9d424
        // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
        // label=bounded_generation_to_history_acquisition_attachments
        std::map<AcquisitionId, HistoryGenerationAttachment>
            history_attachments;
        //@@authority state=prv.S0118 status=provisional scope=field bind=catl::peer_client::PeerSet::PeerGeneration::physical_deadline@f2cf88320d67fe0c
        // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
        // label=immutable_physical_generation_completion_deadline
        std::chrono::steady_clock::time_point physical_deadline{};
        //@@authority state=prv.S0121 status=provisional scope=field bind=catl::peer_client::PeerSet::PeerGeneration::history_watchdog@4b83b94a02576e68
        // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
        // label=exact_demand_generation_physical_watchdog
        std::shared_ptr<boost::asio::steady_timer> history_watchdog;
    };
    //@@authority state=prv.S0088 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::peer_generations_@221008d6ea60dbb8
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=canonical_live_peer_generation_rows
    std::map<std::string, PeerGeneration> peer_generations_;
    struct DrainingGeneration
    {
        std::string endpoint;
        PeerSessionPtr session;
        std::shared_ptr<boost::asio::steady_timer> retry_timer;
        std::size_t attempts = 0;
        bool receipt_failed = false;
    };
    //@@authority state=prv.S0089 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::draining_generations_@03c0b4d86b3916ae
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=terminal_peer_generations_retained_until_disconnect_receipt
    std::map<std::uint64_t, DrainingGeneration> draining_generations_;
    struct ActivePeerUse
    {
        std::string endpoint;
        std::uint64_t connection_generation = 0;
        PeerUseRequest request;
        std::shared_ptr<detail::PeerUseControl> control;
        std::shared_ptr<boost::asio::steady_timer> deadline;
        std::shared_ptr<SharedHistoryFetch> history_fetch;
        std::optional<HistoryAuthorityGrantRef> history_grant;
        //@@authority state=prv.S0098 status=provisional scope=field
        // bind=catl::peer_client::PeerSet::ActivePeerUse::forced_outcome@242169be28e3d328
        // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
        // label=sticky_owner_selected_peer_use_forced_outcome
        std::optional<PeerUseOutcome> forced_outcome;
        //@@authority state=prv.S0097 status=provisional scope=field
        // bind=catl::peer_client::PeerSet::ActivePeerUse::finishing@df82a9d54c08c275
        // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
        // label=peer_use_waiting_for_exact_generation_drain_receipt
        bool finishing = false;
    };
    struct PreparedPeerUse
    {
        std::uint64_t use_id = 0;
        std::string endpoint;
        std::uint64_t generation = 0;
        std::shared_ptr<detail::PeerUseControl> control;
        std::shared_ptr<detail::PeerUseState> state;
        std::shared_ptr<boost::asio::steady_timer> deadline;
        std::map<std::uint64_t, ActivePeerUse>::node_type active_node;
        std::set<std::uint64_t>::node_type generation_use_node;
        bool committed = false;

        PreparedPeerUse() = default;
        PreparedPeerUse(PreparedPeerUse const&) = delete;
        PreparedPeerUse&
        operator=(PreparedPeerUse const&) = delete;
        ~PreparedPeerUse() noexcept;
    };
    //@@authority state=prv.S0090 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::active_peer_uses_@640d1f613cb1b089
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=authoritative_exact_generation_active_peer_uses
    std::map<std::uint64_t, ActivePeerUse> active_peer_uses_;
    //@@authority state=prv.S0091 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::next_peer_use_id_@ce26f1fbcb41802d
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=monotonic_nonreused_peer_use_identity_mint
    std::uint64_t next_peer_use_id_ = 0;
    //@@authority state=prv.S0092 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::peer_use_outcomes_@9e34f82e34c11789
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=classified_terminal_peer_use_outcomes
    std::array<std::uint64_t, static_cast<std::size_t>(PeerUseOutcome::count)>
        peer_use_outcomes_{};

    struct LedgerDemandRecord
    {
        LedgerDemandId id;
        catl::core::RequestScopeId request_scope_id;
        std::weak_ptr<catl::core::RequestScope> request_scope;
        LedgerDemandOptions options;
        std::chrono::steady_clock::time_point absolute_deadline{};
        std::optional<LedgerDemandTerminal> terminal;
        std::set<AcquisitionId> acquisitions;
        catl::core::RequestScope::Subscription terminal_subscription;
        std::shared_ptr<boost::asio::steady_timer> deadline_timer;
        struct BudgetToken
        {
            std::uint64_t id = 0;
            std::uint64_t revision = 1;
            bool spent = false;
            std::optional<std::chrono::steady_clock::time_point> refill_at;
        };
        std::vector<BudgetToken> dial_tokens;
        std::vector<BudgetToken> promotion_tokens;
        std::vector<BudgetToken> blind_tokens;
        std::array<std::size_t, kHistorySourceLaneCount> source_attempts{};
        std::size_t crawl_only_creations = 0;
    };

    struct HistoryAcquisitionRecord
    {
        AcquisitionId id;
        LedgerDemandId demand;
        HistorySelectionContext context;
        std::unordered_set<std::string> excluded_endpoints;
        //@@authority state=prv.S0117 status=provisional scope=field bind=catl::peer_client::PeerSet::HistoryAcquisitionRecord::attached_generations@85328cc34b5fb2f7
        // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
        // label=history_acquisition_to_exact_generation_reverse_attachments
        std::set<std::uint64_t> attached_generations;
        //@@authority state=prv.S0120 status=provisional scope=field bind=catl::peer_client::PeerSet::HistoryAcquisitionRecord::retained_generations@a467ad72c4cf7525
        // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
        // label=history_acquisition_to_exact_retained_generation_reverse_index
        std::set<std::uint64_t> retained_generations;
        //@@authority state=prv.S0122 status=provisional scope=field bind=catl::peer_client::PeerSet::HistoryAcquisitionRecord::redirect_successors@c810dfde6dc3fc66
        // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
        // label=bounded_acquisition_scoped_redirect_successor_values
        std::set<std::string> redirect_successors;
        std::optional<HistoryAcquisitionTerminal> terminal;
    };

    //@@authority state=prv.S0104 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::ledger_demands_@7971f2c3286d07e3
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=one_parent_history_demand_per_request_scope_and_ledger
    std::map<LedgerDemandId, LedgerDemandRecord> ledger_demands_;
    std::map<std::pair<std::uint64_t, std::uint32_t>, LedgerDemandId>
        ledger_demand_keys_;
    //@@authority state=prv.S0105 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::history_acquisitions_@4d5dccf4147931e6
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=budgetless_history_acquisition_children
    std::map<AcquisitionId, HistoryAcquisitionRecord> history_acquisitions_;
    std::uint64_t next_ledger_demand_id_ = 1;
    std::uint64_t next_history_acquisition_id_ = 1;
    std::uint64_t next_history_budget_token_id_ = 1;
    std::vector<LedgerDemandRecord::BudgetToken> global_history_dial_tokens_;
    std::vector<LedgerDemandRecord::BudgetToken> global_history_blind_tokens_;
    //@@authority state=prv.S0115 status=provisional scope=field bind=catl::peer_client::PeerSet::history_pool_revision_@43682a6dd9503a79
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=monotonic_history_pool_control_revision
    std::uint64_t history_pool_revision_ = 1;
    //@@authority state=prv.S0093 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::peer_set_instance_id_@564c32da7db0f1b9
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=process_unique_peer_set_instance_identity
    std::uint64_t peer_set_instance_id_ = 0;
    struct ActivePeerPin
    {
        std::string endpoint;
        std::uint64_t connection_generation = 0;
        std::weak_ptr<void> live_handle;
    };
    //@@authority state=peer_set.active_generation_pins owner=peer_set
    // bind=catl::peer_client::PeerSet::active_peer_pins_@c4866b09e05fda6d
    mutable std::map<std::uint64_t, ActivePeerPin> active_peer_pins_;
    std::uint64_t next_peer_pin_id_ = 0;
    uint64_t total_connects_ = 0;
    uint64_t total_disconnects_ = 0;
    struct EndpointRetryGate
    {
        std::chrono::steady_clock::time_point failed_at;
        std::uint64_t failure_count = 0;
    };
    //@@authority state=prv.S0076 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::retry_gates_@97a0e0015b5350d2
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=bounded_endpoint_retry_age_and_magnitude_gate
    std::map<std::string, EndpointRetryGate> retry_gates_;
    struct FailureEvent
    {
        std::chrono::steady_clock::time_point at;
        std::string endpoint;
        std::string kind;
    };
    std::deque<FailureEvent> recent_failures_;
    struct ConnectServant
    {
        /// Installed before the connect helper first suspends. The row remains
        /// authoritative until the servant consumes the exact physical drain
        /// receipt; requesting disconnect never retires it.
        PeerSessionPtr session;
        /// Exact candidate identity installed with session. A terminal from a
        /// predecessor generation cannot spend this row's classification.
        std::uint64_t generation = 0;
        /// Set by the PeerSet owner immediately before requesting disconnect.
        /// This proves cancellation intent for this exact row. The immutable
        /// terminal cause exported by PeerClient still decides whether peer
        /// failure evidence must be retained.
        bool stop_cancellation_requested = false;
        /// Consumed before recording/suppressing the post-Ready terminal cause
        /// of a candidate which never reached connections_.
        bool terminal_classified = false;
        /// Immutable diagnostic projection of the target authorizations which
        /// caused or joined this physical servant. These values grant no pool
        /// rotation or completion authority.
        std::vector<uint32_t> targeted_ledgers;
    };
    //@@authority state=prv.S0037 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::in_flight_@8017d2247e9adce2
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=owned_background_connect_servant_census_cancel_and_terminal_handoff
    std::map<std::string, ConnectServant> in_flight_;  // currently connecting
    std::set<std::string> queued_;
    // Provenance for every queued connection: ordinary discovery authority,
    // bounded target-request authorities, and their immutable deadlines.
    //@@authority state=prv.S0009 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::targeted_connects_@2feca8aa2d8a41ab
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=queued_ordinary_and_history_target_connection_authorities
    std::map<std::string, QueuedConnectAuthority> targeted_connects_;
    std::vector<std::string> pending_connects_;
    struct CrawlServant
    {
        /// Installed in the authoritative row before co_spawn can expose the
        /// servant. Stop may request cancellation through this exact signal,
        /// but only the co_spawn completion funnel may retire the row.
        std::shared_ptr<boost::asio::cancellation_signal> cancellation;
        /// Sticky owner-issued intent survives nested coroutine cancellation
        /// state and prevents a cancelled HTTP attempt from minting HTTPS work.
        std::stop_token stop;
        /// The transport interrupt is separate from the retained stop fact.
        bool cancellation_emitted = false;
        /// Allocated before admission and shared with the completion token.
        /// Every post-admission terminal path consumes this exact capability
        /// before the row can retire.
        std::shared_ptr<std::atomic_bool> receipt_consumed;
    };
    //@@authority state=prv.S0038 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::crawl_in_flight_@5fdc9078863857e4
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=owned_background_crawl_servant_census_and_terminal_authorities
    std::map<std::string, CrawlServant> crawl_in_flight_;
    std::set<std::string> crawl_queued_;
    std::map<std::string, std::chrono::steady_clock::time_point> crawled_;
    std::vector<std::string> pending_crawls_;
    struct CrawlObservation
    {
        std::string subject_endpoint;
        std::optional<std::string> reported_subject_key;
        std::vector<CrawlLedgerRange> intervals;
        bool truncated = false;
        bool reported_key_conflict = false;
    };
    struct CrawlObservationBatch
    {
        std::chrono::steady_clock::time_point observed_at;
        std::int64_t observed_at_unix = 0;
        std::uint64_t crawl_epoch = 0;
        std::vector<CrawlObservation> observations;
    };
    // Diagnostic-only reporter claims. No selector or connection-admission
    // function reads this table.
    //@@authority state=prv.S0081 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::crawl_observation_batches_@b76408793188153d
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=bounded_ephemeral_untrusted_crawl_observation_batches
    std::map<std::string, CrawlObservationBatch> crawl_observation_batches_;
    std::uint64_t next_crawl_observation_epoch_ = 0;
    std::uint64_t crawl_truncated_peer_rows_ = 0;
    std::uint64_t crawl_truncated_interval_rows_ = 0;
    std::uint64_t crawl_truncated_public_keys_ = 0;
    std::uint64_t crawl_invalid_public_keys_ = 0;
    std::uint64_t crawl_malformed_interval_tokens_ = 0;
    std::uint64_t crawl_truncated_observations_ = 0;
    std::uint64_t crawl_observation_capture_failures_ = 0;
    std::set<std::string> configured_endpoints_;
    // Active target ledgers from in-flight wait_for_covering_peer calls.
    // Used by candidate admission and same-pool rotation to prioritize peers
    // that can serve any active target. Counts preserve concurrent waiters.
    //@@authority state=prv.S0010 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::wanted_ledgers_@e19f417f60e84044
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=reference_counted_active_history_targets
    std::map<uint32_t, std::size_t> wanted_ledgers_;
    //@@authority state=prv.S0011 status=provisional scope=field
    // bind=catl::peer_client::PeerSet::active_target_authorizations_@c1962ff0c49ed8a4
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_set
    // label=live_history_target_authorization_rows
    std::vector<TargetedConnectAuthorizationPtr> active_target_authorizations_;
    std::string endpoint_cache_error_;
    // Persistent one-shot priorities registered through prioritize_ledger().
    std::set<uint32_t> prioritized_ledgers_;
    std::shared_ptr<boost::asio::steady_timer> wait_signal_;
    mutable std::mutex unsolicited_handler_mutex_;
    UnsolicitedHandler unsolicited_handler_;
};

}  // namespace catl::peer_client
