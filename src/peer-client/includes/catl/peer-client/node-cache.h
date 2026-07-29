#pragma once

// NodeCache — content-addressed in-memory cache for SHAMap wire nodes.
//
// Stores raw wire bytes keyed by node hash. Provides walk_to() which
// walks a SHAMap from root hash to target key, fetching on cache miss
// via the peer-session request layer. Cross-ledger structural sharing is
// automatic: two
// ledgers that share an inner node (same hash) share the cache entry.
//
// Usage:
//   auto cache = NodeCache::create(io);
//   auto walk = co_await cache->walk_to(
//       root_hash, target_key, ledger_hash, liTX_NODE, peers);
//   // walk.found(), walk.leaf(), walk.placeholders()

#include "peer-client-coro.h"
#include "peer-set.h"
#include "wire-node-view.h"

#include <catl/core/logger.h>
#include <catl/core/types.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>
#include <vector>

class NodeCacheTestAccess;  // forward decl for test friend

namespace catl::peer_client {

namespace asio = boost::asio;

class LedgerFetchCoordinator;

/**
 * A latched one-shot signal whose timer operations are confined to one strand.
 *
 * Boost permits concurrent waits, but a shared basic_waitable_timer object is
 * otherwise unsafe. NodeCache signals are intentionally shared by unrelated
 * request executors, so every wait initiation and every expiry/cancel mutation
 * crosses this owner strand. Completion is retained as state: a waiter that
 * arrives after complete() observes operation_aborted immediately, and a later
 * expiry update cannot resurrect the signal.
 */
class SerializedTimerSignal
    : public std::enable_shared_from_this<SerializedTimerSignal>
{
public:
    using TimePoint = asio::steady_timer::time_point;
    using Duration = asio::steady_timer::duration;

    static std::shared_ptr<SerializedTimerSignal>
    create(asio::io_context& io, TimePoint expiry)
    {
        return std::shared_ptr<SerializedTimerSignal>(
            new SerializedTimerSignal(io, expiry));
    }

    static std::shared_ptr<SerializedTimerSignal>
    create(asio::io_context& io, Duration duration)
    {
        return create(io, std::chrono::steady_clock::now() + duration);
    }

    asio::awaitable<boost::system::error_code>
    wait()
    {
        auto self = shared_from_this();
        co_return co_await asio::co_spawn(
            strand_,
            [self]() -> asio::awaitable<boost::system::error_code> {
                if (self->completed_)
                    co_return self->completion_ec_;

                boost::system::error_code ec;
                co_await self->timer_.async_wait(
                    asio::redirect_error(asio::use_awaitable, ec));

                if (self->completed_)
                    co_return self->completion_ec_;

                // Cancellation of this particular wait (for example, when a
                // progress signal wins a parallel_group) is not completion of
                // the shared one-shot signal.
                if (ec == asio::error::operation_aborted)
                    co_return ec;

                if (!self->completed_)
                {
                    self->completed_ = true;
                    self->completion_ec_ = ec;
                    std::lock_guard lock(self->projection_mutex_);
                    self->projected_completed_ = true;
                }
                co_return ec;
            },
            asio::use_awaitable);
    }

    asio::io_context::executor_type
    base_executor() const noexcept
    {
        return strand_.get_inner_executor();
    }

    TimePoint
    expiry() const
    {
        std::lock_guard lock(projection_mutex_);
        return projected_completed_ ? TimePoint::min() : projected_expiry_;
    }

    void
    expires_at(TimePoint expiry) noexcept
    {
        try
        {
            auto self = shared_from_this();
            asio::post(strand_, [self, expiry]() {
                if (self->completed_)
                    return;
                try
                {
                    self->timer_.expires_at(expiry);
                    std::lock_guard lock(self->projection_mutex_);
                    self->projected_expiry_ = expiry;
                }
                catch (...)
                {
                }
            });
        }
        catch (...)
        {
        }
    }

    void
    expires_after(Duration duration) noexcept
    {
        expires_at(std::chrono::steady_clock::now() + duration);
    }

    void
    complete() noexcept
    {
        try
        {
            auto self = shared_from_this();
            asio::post(strand_, [self]() {
                if (self->completed_)
                    return;
                self->completed_ = true;
                self->completion_ec_ = asio::error::operation_aborted;
                {
                    std::lock_guard lock(self->projection_mutex_);
                    self->projected_completed_ = true;
                }
                try
                {
                    self->timer_.cancel();
                }
                catch (...)
                {
                }
            });
        }
        catch (...)
        {
        }
    }

private:
    SerializedTimerSignal(asio::io_context& io, TimePoint expiry)
        : strand_(asio::make_strand(io))
        , timer_(strand_, expiry)
        , projected_expiry_(expiry)
    {
    }

    asio::strand<asio::io_context::executor_type> strand_;
    asio::steady_timer timer_;
    bool completed_ = false;
    boost::system::error_code completion_ec_;
    mutable std::mutex projection_mutex_;
    TimePoint projected_expiry_;
    bool projected_completed_ = false;
};

// ─────────────────────────────────────────────────────────────────────
// Result types
// ─────────────────────────────────────────────────────────────────────

/// A node on the walk path with its tree position.
struct PathNode
{
    SHAMapNodeID nodeid;  // position in tree
    Hash256 hash;         // content hash
    std::shared_ptr<std::vector<uint8_t> const>
        wire;  // shared with cache — zero-copy
};

/// Sibling branch at a depth — becomes a placeholder in the abbreviated tree.
struct PlaceholderNode
{
    SHAMapNodeID nodeid;
    Hash256 hash;
};

struct WalkFound
{
    SHAMapNodeID leaf_nid;
    std::vector<uint8_t> leaf_data;
};

struct WalkBranchAbsent
{
    PathNode parent;
    std::uint8_t branch = 0;
};

struct WalkNoSuitablePeer
{
    std::uint32_t ledger_seq = 0;
};

struct WalkTransportExhausted
{
    std::uint32_t ledger_seq = 0;
    std::uint16_t depth = 0;
    std::uint16_t retries = 0;
    std::vector<std::string> attempted_peers;
};

struct WalkCancelled
{
};

struct WalkDepthExhausted
{
    std::uint16_t depth = 0;
};

struct WalkInternalFailure
{
};

/// Closed terminal disposition for one bounded tree walk.
///
/// A local selection refusal cannot carry a peer-response outcome. A found
/// result owns its leaf; no leaf fields exist on any failure alternative.
using WalkTerminal = std::variant<
    WalkInternalFailure,
    WalkFound,
    WalkBranchAbsent,
    WalkNoSuitablePeer,
    WalkTransportExhausted,
    WalkCancelled,
    WalkDepthExhausted>;

/// Result of walk_to: the resolved path plus one exact terminal disposition.
class WalkResult
{
public:
    /// Required by Asio's completion adapter for its exception path. The only
    /// publicly mintable value is an inert failure with no authority-bearing
    /// evidence.
    WalkResult() = default;

    std::vector<PathNode> const&
    path() const noexcept
    {
        return path_;
    }

    std::vector<PlaceholderNode> const&
    placeholders() const noexcept
    {
        return placeholders_;
    }

    WalkTerminal const&
    terminal() const noexcept
    {
        return terminal_;
    }

    bool
    found() const noexcept
    {
        return std::holds_alternative<WalkFound>(terminal_);
    }

    WalkFound const&
    leaf() const
    {
        return std::get<WalkFound>(terminal_);
    }

private:
    friend class NodeCache;

    WalkResult(
        std::vector<PathNode> path,
        std::vector<PlaceholderNode> placeholders,
        WalkTerminal terminal)
        : path_(std::move(path))
        , placeholders_(std::move(placeholders))
        , terminal_(std::move(terminal))
    {
    }

    std::vector<PathNode> path_;
    std::vector<PlaceholderNode> placeholders_;

    //@@authority state=prv.S0063 status=provisional scope=field
    // bind=catl::peer_client::WalkResult::terminal_@ce74c248ea8dde2c
    // owner=unresolved owner_hypothesis=node_cache
    // observed_home=walk_result
    // label=closed_node_walk_terminal_disposition
    WalkTerminal terminal_ = WalkInternalFailure{};
};

// ─────────────────────────────────────────────────────────────────────
// NodeCache
// ─────────────────────────────────────────────────────────────────────

class NodeCache : public std::enable_shared_from_this<NodeCache>
{
    friend class ::NodeCacheTestAccess;

public:
    struct Options
    {
        size_t max_entries = 65536;
        std::chrono::milliseconds fetch_timeout{1500};
        // Managed transport has a separate pre-publication phase. It must not
        // consume the response budget while the coordinator binds and writes.
        std::chrono::milliseconds managed_admission_timeout{4000};
        int max_walk_peer_retries = 3;
        int fetch_stale_multiplier = 2;
        int peer_refresh_timeout_secs = 5;
        std::chrono::milliseconds header_timeout_floor{6000};
    };

    static std::shared_ptr<NodeCache>
    create(
        asio::io_context& io,
        Options opts,
        std::shared_ptr<LedgerFetchCoordinator> node_fetch_coordinator = {});

    /// Walk a SHAMap from root hash to target key, fetching on miss.
    ///
    /// @param root_hash   Root of the tree (account_hash or tx_hash from
    /// header)
    /// @param target_key  The key to find (tx hash or SLE key)
    /// @param ledger_hash Which ledger to request from peers on miss
    /// @param ledger_seq  Ledger sequence (for peer selection by range)
    /// @param tree_type   liTX_NODE (1) or liAS_NODE (2)
    /// @param peers       Peer pool for fetching
    /// @param peer        Preferred peer (may be null — will use peers)
    /// @param cancel      Cancel token — checked between depths to stop early
    asio::awaitable<WalkResult>
    walk_to(
        Hash256 root_hash,
        Hash256 target_key,
        Hash256 ledger_hash,
        uint32_t ledger_seq,
        int tree_type,
        std::shared_ptr<PeerSet> peers,
        PeerGenerationPin peer,
        std::shared_ptr<std::atomic<bool>> cancel = nullptr);

    /// Unmanaged compatibility overload for callers whose session does not
    /// come from PeerSet (principally direct/test sessions). PeerSet-selected
    /// production work must use the generation-pin overload above.
    asio::awaitable<WalkResult>
    walk_to(
        Hash256 root_hash,
        Hash256 target_key,
        Hash256 ledger_hash,
        uint32_t ledger_seq,
        int tree_type,
        std::shared_ptr<PeerSet> peers,
        PeerSessionPtr peer = nullptr,
        std::shared_ptr<std::atomic<bool>> cancel = nullptr);

    /// Insert a node into the cache by hash. Returns true if newly inserted.
    bool
    insert(Hash256 const& hash, std::vector<uint8_t> data);

    /// Insert a node and wake any waiters for this hash.
    void
    insert_and_notify(Hash256 const& hash, std::vector<uint8_t> data);

    /// Check if a hash is in the cache (present, not in-flight).
    bool
    has(Hash256 const& hash) const;

    /// Get the wire data for a hash. Returns nullptr if not cached/present.
    std::shared_ptr<std::vector<uint8_t> const>
    get(Hash256 const& hash) const;

    /// Legacy compatibility: put by hash.
    void
    put(Hash256 const& hash, std::span<const uint8_t> data)
    {
        insert(hash, std::vector<uint8_t>(data.begin(), data.end()));
    }

    /// Cache stats.
    struct Stats
    {
        size_t entries = 0;
        size_t resident_entries = 0;
        size_t header_entries = 0;
        size_t max_entries = 0;
        size_t hits = 0;
        size_t misses = 0;
        size_t fetches = 0;
        size_t fetch_errors = 0;
        bool managed_transport_selected = false;
        size_t raw_fetches_projected = 0;
        size_t raw_writes_confirmed = 0;
        size_t raw_not_written = 0;
        size_t managed_fetches_reserved = 0;
        size_t managed_fetches_refused = 0;
        size_t managed_writes_confirmed = 0;
        size_t managed_not_written = 0;
        size_t managed_indeterminate = 0;
        size_t hash_mismatches = 0;
        size_t waiter_wakeups = 0;
    };
    Stats
    stats() const;

    /// Fetch a ledger header, with cache + in-flight dedup.
    /// First requester fans out to up to 3 peers; first response wins.
    /// Subsequent callers join the shared wait.
    asio::awaitable<LedgerHeaderResult>
    get_header(
        uint32_t ledger_seq,
        std::shared_ptr<PeerSet> peers,
        PeerGenerationPin peer);

    /// Unmanaged compatibility overload; see walk_to().
    asio::awaitable<LedgerHeaderResult>
    get_header(
        uint32_t ledger_seq,
        std::shared_ptr<PeerSet> peers,
        PeerSessionPtr peer = nullptr);

    /// Number of present entries in the cache.
    size_t
    size() const;

    /// Total bytes of wire data stored.
    size_t
    bytes() const;

    /// Synchronously invalidate node-store entries and wake their waiters.
    /// This is not a shutdown/drain receipt.
    void
    clear_nodes();

    /// Permanently seal node admission and wait until every NodeCache-owned
    /// deadline servant has ended and every NodeCache-local generation-pin
    /// share has been destroyed. The driver then crosses every relevant
    /// PeerSet strand; a later service-level coordinator fence owns final
    /// release of pin states that still have non-NodeCache aliases.
    ///
    /// Precondition: the request-admission owner has stopped and every request
    /// frame capable of entering NodeCache has completed. Overlapping and later
    /// calls join the same terminal boundary.
    asio::awaitable<void>
    co_stop_nodes();

    /// Invalidate every pre-boundary header entry and wait until its detached
    /// drivers have ended and PeerSet has consumed their pin releases.
    /// Overlapping clears join one boundary. New header admissions are refused
    /// until that boundary is acknowledged.
    asio::awaitable<void>
    co_clear_headers();

#ifdef XPRV_TESTING
    void
    set_before_node_stop_driver_launch_for_test(std::function<void()> hook)
    {
        before_node_stop_driver_launch_for_test_ = std::move(hook);
    }

    void
    set_before_header_clear_driver_launch_for_test(std::function<void()> hook)
    {
        before_header_clear_driver_launch_for_test_ = std::move(hook);
    }
#endif

private:
    NodeCache(
        asio::io_context& io,
        Options opts,
        std::shared_ptr<LedgerFetchCoordinator> node_fetch_coordinator);

    static constexpr std::size_t kProgressCapacity = 16;
    static constexpr uint16_t kNoDepth = 0xFFFF;

    enum class ProgressKind : uint8_t {
        retry_same_peer,
        retry_covered_peer,
        switch_peer,
        acquired_peer,
        give_up,
        searching_covering_peer,
        no_covering_peer,
    };

    struct ProgressEvent
    {
        uint64_t seq = 0;
        ProgressKind kind = ProgressKind::retry_same_peer;
        uint32_t ledger_seq = 0;
        uint16_t depth = kNoDepth;
        uint8_t attempt = 0;
        uint8_t max_attempts = 0;
        std::array<char, 64> peer{};
    };

    struct ProgressCursor
    {
        uint64_t next_seq = 1;
    };

    template <std::size_t Capacity>
    struct ProgressJournal
    {
        uint64_t next_seq = 1;
        std::array<ProgressEvent, Capacity> slots{};

        ProgressCursor
        subscribe() const noexcept
        {
            return ProgressCursor{next_seq};
        }

        //@@authority transition=prv.T0116 status=provisional scope=function
        // bind=catl::peer_client::NodeCache::ProgressJournal::publish@215045b93c64e480
        // owner=unresolved owner_hypothesis=node_cache commit=owner_lock
        // touches=prv.S0004 mutation_class=running_guarded
        // label=publish_bounded_progress_journal_event
        void
        publish(ProgressEvent event) noexcept
        {
            event.seq = next_seq;
            slots[(next_seq - 1) % Capacity] = event;
            ++next_seq;
        }

        std::vector<ProgressEvent>
        replay(ProgressCursor& cursor) const
        {
            auto oldest = next_seq > Capacity ? next_seq - Capacity : 1;
            if (cursor.next_seq < oldest)
                cursor.next_seq = oldest;

            std::vector<ProgressEvent> out;
            auto end = next_seq;
            out.reserve(static_cast<std::size_t>(end - cursor.next_seq));
            for (auto seq = cursor.next_seq; seq < end; ++seq)
            {
                auto const& event = slots[(seq - 1) % Capacity];
                if (event.seq == seq)
                    out.push_back(event);
            }
            cursor.next_seq = end;
            return out;
        }
    };

    struct ProgressState
    {
        ProgressJournal<kProgressCapacity> journal;
        std::shared_ptr<SerializedTimerSignal> signal;
    };

    struct NodeFetchRound;

    struct WaitRegistration
    {
        std::shared_ptr<SerializedTimerSignal> done_signal;
        std::shared_ptr<SerializedTimerSignal> progress_signal;
        std::shared_ptr<NodeFetchRound> round;
        ProgressCursor cursor;
    };

    struct NodeFetchRound
    {
        enum class Phase : std::uint8_t {
            admission,
            response,
            retired,
        };
        enum class DispatchState : std::uint8_t {
            submitting,
            reserved,
            written,
            not_written,
            indeterminate,
        };
        struct DispatchSlot
        {
            std::string endpoint;
            DispatchState state = DispatchState::submitting;
        };

        Phase phase = Phase::response;
        //@@authority state=prv.S0041 status=provisional scope=field
        // bind=catl::peer_client::NodeCache::NodeFetchRound::deadline_servants@f5a990307921c287
        // owner=unresolved owner_hypothesis=node_cache
        // observed_home=node_cache
        // label=exact_deadline_servant_identities_for_node_operation
        std::map<std::uint64_t, std::shared_ptr<SerializedTimerSignal>>
            deadline_servants;
        std::shared_ptr<SerializedTimerSignal> servants_done_signal;
        std::uint64_t operation_id = 0;
        // NodeCache fans out to one primary plus at most two additional peers.
        // A fixed table makes the callback/return handshake bounded by shape.
        std::array<DispatchSlot, 3> dispatches;
        std::size_t dispatch_count = 0;
        bool submissions_closed = false;
        // Admission time and waiter progress are different facts. Only the
        // deadline servant observes this timer; Entry::signal is a phase latch
        // completed when admission commits to response or retirement.
        std::shared_ptr<SerializedTimerSignal> admission_deadline;
        //@@authority state=prv.S0020 status=provisional scope=field
        // bind=catl::peer_client::NodeCache::NodeFetchRound::admission_expired@f1eac8a90ba7ad6a
        // owner=unresolved owner_hypothesis=node_cache
        // observed_home=node_cache
        // label=managed_round_admission_boundary_elapsed
        bool admission_expired = false;
        bool response_possible = false;
    };

    struct NodeStopState
    {
        enum class Phase : std::uint8_t { running, succeeded, failed };

        std::shared_ptr<SerializedTimerSignal> completion_signal;
        std::exception_ptr failure;
        Phase phase = Phase::running;
    };

    // ── Cache entry ─────────────────────────────────────────────
    //
    // Single shared signal per entry. When data arrives via
    // insert_and_notify, the signal is cancelled — all waiters wake.
    //
    // Key invariant: entries are NEVER erased on timeout/error.
    // A timed-out caller returns nullptr, but the entry stays alive
    // so that late-arriving peer responses still populate it.
    // Next caller detects stale via round_opened_at, re-sends, and
    // creates a fresh signal.
    //
    // Entries are only removed by LRU eviction (which skips
    // in-flight entries with non-null signal).
    struct Entry
    {
        std::shared_ptr<std::vector<uint8_t> const> wire_data;
        //@@authority state=prv.S0002 status=provisional scope=field
        // bind=catl::peer_client::NodeCache::Entry::signal@e24590d1aa2b06af
        // owner=unresolved owner_hypothesis=node_cache
        // observed_home=node_cache
        // label=shared_node_fetch_round_completion_signal
        std::shared_ptr<SerializedTimerSignal>
            signal;  // non-null while in-flight
        // Stable identity and bounded evidence for one exact physical round.
        // The phase signal may change at managed publication, but the round
        // identity does not. Waiters retain this object so evidence remains
        // attributable even if a later caller opens a replacement round.
        //@@authority state=prv.S0018 status=provisional scope=field
        // bind=catl::peer_client::NodeCache::Entry::round@c6fa8c63a891c776
        // owner=unresolved owner_hypothesis=node_cache
        // observed_home=node_cache
        // label=exact_node_fetch_round_phase_dispatch_and_evidence
        std::shared_ptr<NodeFetchRound> round;
        // Physical generation pins for the transport round represented by
        // signal. Shared waiters do not own these; the coalesced entry does.
        //@@authority state=prv.S0003 status=provisional scope=field
        // bind=catl::peer_client::NodeCache::Entry::peer_pins@dde6c4bc6b2e73b7
        // owner=unresolved owner_hypothesis=node_cache
        // observed_home=node_cache
        // label=peer_generation_pins_retained_by_shared_round
        std::vector<PeerGenerationPin> peer_pins;
        //@@authority state=prv.S0004 status=provisional scope=field
        // bind=catl::peer_client::NodeCache::Entry::progress@701253230d8be0f3
        // owner=unresolved owner_hypothesis=node_cache
        // observed_home=node_cache
        // label=shared_round_progress_and_rider_cursor_home
        ProgressState progress;
        bool present = false;

        // Admission time of the physical round represented by signal.
        // Publishing the signal and this timestamp is one mutex commit, so a
        // second caller cannot immediately replace a freshly admitted round
        // using its predecessor's stale timestamp.
        //@@authority state=prv.S0005 status=provisional scope=field
        // bind=catl::peer_client::NodeCache::Entry::round_opened_at@43339160535e957d
        // owner=unresolved owner_hypothesis=node_cache
        // observed_home=node_cache label=exact_round_admission_time
        std::chrono::steady_clock::time_point round_opened_at{};

        // Hash-level evidence that at least one raw TMGetLedger write completed
        // successfully. Projecting work onto PeerClient's strand is
        // deliberately weaker and does not update this value. Unlike
        // round_opened_at, confirmed write evidence may outlive exact round
        // retirement because a late content-addressed response remains useful.
        // It justifies waiting without a currently usable peer.
        //@@authority state=prv.S0006 status=provisional scope=field
        // bind=catl::peer_client::NodeCache::Entry::last_fetch_at@c630d439bcf3126a
        // owner=unresolved owner_hypothesis=node_cache
        // observed_home=node_cache
        // label=confirmed_raw_write_evidence_outliving_exact_round
        std::chrono::steady_clock::time_point last_fetch_at{};
    };

    struct HeaderRound
    {
        std::shared_ptr<SerializedTimerSignal> signal;
        std::vector<PeerGenerationPin> peer_pins;
        std::mutex peer_pins_mutex;
        std::atomic<int> outstanding{0};
        std::atomic<bool> dispatch_complete{false};
        std::atomic<bool> completed{false};
        bool closed = false;

        bool
        try_begin_send(PeerGenerationPin pin)
        {
            std::lock_guard lock(peer_pins_mutex);
            if (closed)
                return false;
            if (pin)
                peer_pins.push_back(std::move(pin));
            outstanding.fetch_add(1, std::memory_order_relaxed);
            return true;
        }

        void
        close_and_release_peer_pins() noexcept
        {
            std::vector<PeerGenerationPin> released;
            {
                std::lock_guard lock(peer_pins_mutex);
                closed = true;
                released.swap(peer_pins);
            }
        }
    };

    struct HeaderFetchState
    {
        enum class Phase : std::uint8_t {
            running,
            succeeded,
            failed,
            cleared,
        };

        // Overall terminal signal is distinct from per-round timeout signals.
        std::shared_ptr<SerializedTimerSignal> completion_signal;
        std::shared_ptr<SerializedTimerSignal> deadline_signal;
        std::shared_ptr<SerializedTimerSignal> driver_done_signal;
        std::shared_ptr<SerializedTimerSignal> deadline_done_signal;
        std::shared_ptr<PeerSet> peers;
        // Receipt routing is distinct from selection authority. This contains
        // the actual owner of the initial managed pin plus any PeerSet that
        // can mint later fan-out pins.
        std::vector<std::shared_ptr<PeerSet>> release_owners;
        std::shared_ptr<std::atomic<bool>> cancel =
            std::make_shared<std::atomic<bool>>(false);

        // Exactly one of these owns the initial session borrow.
        PeerGenerationPin initial_pin;
        PeerSessionPtr unmanaged_owner;

        std::unordered_set<std::string> tried_peers;
        std::vector<std::string> round_peers;
        int total_peers_tried = 0;
        std::uint8_t round_number = 0;
        std::shared_ptr<HeaderRound> active_round;
        std::uint64_t operation_id = 0;
        std::uint64_t epoch = 0;
        bool driver_frame_done = false;
        bool deadline_frame_done = false;
        bool retirement_started = false;
        Phase phase = Phase::running;
    };

    struct HeaderClearState
    {
        enum class Phase : std::uint8_t { running, succeeded, failed };

        std::shared_ptr<SerializedTimerSignal> completion_signal;
        std::exception_ptr failure;
        Phase phase = Phase::running;
    };

    // Ledger header cache: keyed by seq
    struct HeaderEntry
    {
        LedgerHeaderResult result;
        // One authoritative shared operation. Its pointer identity fences
        // callbacks, retries, deadline, and clear from later same-sequence
        // work.
        //@@authority state=node_cache.header_fetch_slots owner=node_cache
        // bind=catl::peer_client::NodeCache::HeaderEntry::fetch@94463d77f3fa7446
        std::shared_ptr<HeaderFetchState> fetch;
        ProgressState progress;
        bool present = false;
    };

    /// Ensure a node is in the cache. Fetches from peer on miss.
    /// Peer must be pre-acquired by walk_to — no strand hop here.
    /// Returns shared wire data, or nullptr on timeout/error.
    asio::awaitable<std::shared_ptr<std::vector<uint8_t> const>>
    ensure_present(
        Hash256 expected_hash,
        Hash256 ledger_hash,
        int tree_type,
        SHAMapNodeID position,
        Hash256 const& target_key,
        int speculative_depth,
        PeerSession* peer,
        PeerGenerationPin peer_pin,
        std::shared_ptr<PeerSet> peers = nullptr,
        uint32_t ledger_seq = 0,
        std::shared_ptr<std::atomic<bool>> cancel = nullptr,
        std::unordered_set<std::string>* attempted_peers = nullptr,
        std::unordered_set<std::string> const* excluded_peers = nullptr,
        bool* initiated_fetch = nullptr,
        std::unordered_set<std::string>* written_peers = nullptr);

    /// Send a TMGetLedger request for a node (+ speculative deeper nodes).
    /// Non-coroutine: just builds and sends the request, no waiting.
    /// The response arrives asynchronously via on_node_response →
    /// insert_and_notify which wakes all waiters.
    enum class NodeFetchSubmission : std::uint8_t {
        raw_projected,
        managed_reserved,
        not_submitted,
    };

    enum class ManagedSubmissionOutcome : std::uint8_t {
        written,
        not_written,
        indeterminate,
    };

    NodeFetchSubmission
    send_fetch(
        Hash256 expected_hash,
        Hash256 ledger_hash,
        int tree_type,
        SHAMapNodeID position,
        Hash256 const& target_key,
        int speculative_depth,
        uint32_t ledger_seq,
        PeerSession& peer,
        PeerGenerationPin peer_pin,
        std::shared_ptr<NodeFetchRound> round);

    std::optional<std::size_t>
    register_managed_submission(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round,
        std::string_view endpoint);

    void
    commit_managed_submission(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round,
        std::size_t slot) noexcept;

    void
    rollback_managed_submission(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round,
        std::size_t slot) noexcept;

    void
    close_managed_submissions(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round) noexcept;

    void
    settle_managed_submission(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round,
        std::size_t slot,
        ManagedSubmissionOutcome outcome) noexcept;

    void
    reconcile_managed_round(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round) noexcept;

    bool
    retain_node_fanout_pin(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round,
        PeerGenerationPin pin) noexcept;

    bool
    handle_node_waiter_timeout(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round,
        std::shared_ptr<SerializedTimerSignal> const& waited_signal,
        WaitRegistration& wait) noexcept;

    std::optional<std::size_t>
    register_raw_submission(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round,
        std::string_view endpoint);

    void
    settle_raw_submission(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round,
        std::size_t slot,
        boost::system::error_code ec) noexcept;

    void
    close_raw_submissions(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round) noexcept;

    void
    reconcile_raw_round(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round) noexcept;

    /// Send a header request to a single peer (fire-and-forget).
    /// On success: populate header_cache_ entry and wake all callers.
    /// On failure: decrement the round counter and wake only that round's
    /// owner after dispatch has completed.
    void
    send_header_fetch(
        uint32_t ledger_seq,
        PeerSession& peer,
        std::shared_ptr<HeaderFetchState> fetch,
        std::shared_ptr<HeaderRound> round,
        std::shared_ptr<PeerSet> peers);

    asio::awaitable<void>
    drive_header_fetch(
        uint32_t ledger_seq,
        std::shared_ptr<HeaderFetchState> fetch);

    asio::awaitable<void>
    drive_header_clear(std::shared_ptr<HeaderClearState> clear);

    void
    on_header_servant_done(
        std::shared_ptr<HeaderFetchState> const& fetch,
        bool driver) noexcept;

    asio::awaitable<void>
    retire_header_fetch(std::shared_ptr<HeaderFetchState> fetch);

    asio::awaitable<LedgerHeaderResult>
    wait_for_header_fetch(
        uint32_t ledger_seq,
        std::shared_ptr<HeaderFetchState> fetch);

    void
    finish_header_fetch(
        uint32_t ledger_seq,
        std::shared_ptr<HeaderFetchState> const& fetch,
        std::optional<LedgerHeaderResult> result = std::nullopt) noexcept;

    /// Compute the content hash of an inner wire node.
    /// Expands compressed format to canonical 16×32 layout, then
    /// SHA512Half(MINprefix + 16 child hashes).
    static Hash256
    compute_inner_hash(std::span<const uint8_t> wire_data);

    /// Install our response handler on a peer.
    /// Safe to call repeatedly; PeerClient just overwrites the handler.
    void
    ensure_response_handler(PeerSession& peer);

    /// Give a physical node-fetch round a deadline owner independent of all
    /// subscriber coroutine frames. Missing responses therefore release
    /// generation pins even when every waiter has cancelled.
    bool
    arm_node_round_deadline(
        Hash256 hash,
        std::shared_ptr<NodeFetchRound> round,
        std::shared_ptr<SerializedTimerSignal> signal) noexcept;

    void
    expire_node_fetch_round(
        Hash256 const& hash,
        std::shared_ptr<NodeFetchRound> const& round,
        std::shared_ptr<SerializedTimerSignal> const& signal) noexcept;

    void
    retire_node_fetch_round(
        Hash256 const& hash,
        std::shared_ptr<SerializedTimerSignal> const& signal) noexcept;

    void
    remember_node_pin_owner_barrier_locked(PeerGenerationPin const& pin);

    std::shared_ptr<SerializedTimerSignal>
    maybe_complete_node_operation_locked(
        std::shared_ptr<NodeFetchRound> const& round) noexcept;

    std::shared_ptr<SerializedTimerSignal>
    terminalize_node_operation_locked(
        std::shared_ptr<NodeFetchRound> const& round) noexcept;

    void
    on_node_deadline_servant_done(
        std::shared_ptr<NodeFetchRound> const& round,
        std::uint64_t servant_id) noexcept;

    void
    publish_node_stop_failure_locked(
        std::shared_ptr<NodeStopState> const& stop,
        std::exception_ptr failure) noexcept;

    bool
    node_stop_terminal_predicate_locked(
        std::shared_ptr<NodeStopState> const& stop,
        std::vector<std::shared_ptr<NodeFetchRound>> const& operations,
        bool local_pins_consumed,
        bool owner_barriers_consumed) const noexcept;

    asio::awaitable<void>
    drive_node_stop(std::shared_ptr<NodeStopState> stop);

    asio::awaitable<WalkResult>
    walk_to_impl(
        Hash256 root_hash,
        Hash256 target_key,
        Hash256 ledger_hash,
        uint32_t ledger_seq,
        int tree_type,
        std::shared_ptr<PeerSet> peers,
        PeerSession* peer,
        PeerGenerationPin peer_pin,
        PeerSessionPtr unmanaged_owner,
        std::shared_ptr<std::atomic<bool>> cancel);

    asio::awaitable<LedgerHeaderResult>
    get_header_impl(
        uint32_t ledger_seq,
        std::shared_ptr<PeerSet> peers,
        PeerGenerationPin peer_pin,
        PeerSessionPtr unmanaged_owner);

    /// Called by the response handler for every TMLedgerData node response.
    /// Reports acceptance and processing failure independently. An earlier
    /// node in a multi-node response may commit before a later node fails.
    NodeResponseObservation
    on_node_response(std::shared_ptr<protocol::TMLedgerData> const& msg);

    /// Observed form used only by the response sink. Public callers keep the
    /// predecessor's void insert-and-notify contract.
    bool
    insert_and_notify_observed(Hash256 const& hash, std::vector<uint8_t> data);

    /// Evict LRU entries when over capacity.
    void
    evict_if_needed();

    void
    evict_headers_if_needed();

    std::shared_ptr<SerializedTimerSignal>
    make_progress_signal();

    static std::string
    format_progress(ProgressEvent const& event);

    WaitRegistration
    attach_waiter_locked(Entry& entry);

    WaitRegistration
    attach_waiter_locked(HeaderEntry& entry);

    void
    refresh_waiter_locked(Entry& entry, WaitRegistration& wait);

    void
    refresh_waiter_locked(HeaderEntry& entry, WaitRegistration& wait);

    std::shared_ptr<SerializedTimerSignal>
    publish_progress_locked(
        Entry& entry,
        ProgressKind kind,
        uint32_t ledger_seq,
        uint16_t depth,
        uint8_t attempt,
        uint8_t max_attempts,
        std::string_view peer = {});

    std::shared_ptr<SerializedTimerSignal>
    publish_progress_locked(
        HeaderEntry& entry,
        ProgressKind kind,
        uint32_t ledger_seq,
        uint16_t depth,
        uint8_t attempt,
        uint8_t max_attempts,
        std::string_view peer = {});

    void
    publish_node_progress(
        Hash256 const& hash,
        ProgressKind kind,
        uint32_t ledger_seq,
        uint16_t depth,
        uint8_t attempt,
        uint8_t max_attempts,
        std::string_view peer = {});

    void
    publish_header_progress(
        uint32_t ledger_seq,
        ProgressKind kind,
        uint8_t attempt,
        uint8_t max_attempts,
        std::string_view peer = {});

    asio::io_context& io_;
    asio::strand<asio::io_context::executor_type> header_state_strand_;
    size_t max_entries_;
    std::chrono::milliseconds fetch_timeout_;
    std::chrono::milliseconds managed_admission_timeout_;
    int max_walk_peer_retries_;
    int fetch_stale_multiplier_;
    int peer_refresh_timeout_secs_;
    std::chrono::milliseconds header_timeout_floor_;

    // Immutable transport selection. An engaged weak capability means managed
    // mode: missing pins and expired lifecycle owners fail closed. Disengaged
    // means the explicitly selected raw compatibility mode.
    //@@authority state=prv.S0014 status=provisional scope=field
    // bind=catl::peer_client::NodeCache::node_fetch_coordinator_@73ff97420ec99324
    // owner=unresolved owner_hypothesis=node_cache observed_home=node_cache
    // label=non_owning_managed_node_transport_selection
    std::optional<std::weak_ptr<LedgerFetchCoordinator>>
        node_fetch_coordinator_;

    mutable std::mutex mutex_;
    enum class NodeLifecycle : std::uint8_t { running, stopping, stopped };
    //@@authority state=prv.S0039 status=provisional scope=field
    // bind=catl::peer_client::NodeCache::node_lifecycle_@25163ae6b3471c09
    // owner=unresolved owner_hypothesis=node_cache observed_home=node_cache
    // label=node_admission_and_terminal_lifecycle
    NodeLifecycle node_lifecycle_ = NodeLifecycle::running;
    // The node map is not an operation census: a row may disappear while its
    // deadline servant is still unwinding. This registry owns each exact round
    // until it is terminal and its exact servant set is empty.
    //@@authority state=prv.S0040 status=provisional scope=field
    // bind=catl::peer_client::NodeCache::active_node_operations_@872bc98a5381389e
    // owner=unresolved owner_hypothesis=node_cache observed_home=node_cache
    // label=active_node_operations_independent_of_cache_rows
    std::map<std::uint64_t, std::shared_ptr<NodeFetchRound>>
        active_node_operations_;
    std::uint64_t next_node_operation_id_ = 0;
    std::uint64_t next_node_deadline_servant_id_ = 0;
    // Destroying a NodeCache-local pin share need not emit a release command:
    // another component may retain an alias of the same shared pin state.
    // Retaining each distinct PeerSet here lets node stop cross every relevant
    // owner strand after destroying its local shares. The later service-level
    // coordinator fence owns final shared-pin release.
    //@@authority state=prv.S0043 status=provisional scope=field
    // bind=catl::peer_client::NodeCache::node_pin_owner_barriers_@dae05b46d99a0a78
    // owner=unresolved owner_hypothesis=node_cache observed_home=node_cache
    // label=peer_set_strands_relevant_to_node_pin_destruction
    std::vector<std::shared_ptr<PeerSet>> node_pin_owner_barriers_;
    //@@authority state=prv.S0042 status=provisional scope=field
    // bind=catl::peer_client::NodeCache::node_stop_state_@b492410fc860e0cf
    // owner=unresolved owner_hypothesis=node_cache observed_home=node_cache
    // label=shared_terminal_node_stop_receipt
    std::shared_ptr<NodeStopState> node_stop_state_;
    //@@authority state=prv.S0001 status=provisional scope=field
    // bind=catl::peer_client::NodeCache::store_@6b31857b371de564
    // owner=unresolved owner_hypothesis=mixed observed_home=node_cache
    // legacy_container=store_ label=mixed_cached_and_coalesced_node_rows
    std::map<Hash256, Entry> store_;
    std::map<uint32_t, HeaderEntry> header_cache_;
    // Immutable off-strand projection. The container itself remains owned by
    // header_state_strand_; observability never reads it across that boundary.
    std::atomic<size_t> header_entries_projection_{0};
    // Cache terminality and servant terminality are separate transitions.
    // This registry owns every operation until both coroutine frames are gone
    // and PeerSet has consumed their generation-pin releases.
    //@@authority state=node_cache.active_header_fetches owner=node_cache
    // bind=catl::peer_client::NodeCache::active_header_fetches_@64a4050eddf54810
    std::map<std::uint64_t, std::shared_ptr<HeaderFetchState>>
        active_header_fetches_;
    std::uint64_t next_header_fetch_id_ = 0;
    // Owner-strand lifecycle state. A non-null barrier means one clear
    // boundary has committed and is still draining predecessor authority.
    //@@authority state=node_cache.header_clear_boundary owner=node_cache
    // bind=catl::peer_client::NodeCache::header_clear_state_@a98153bde92847e1
    std::shared_ptr<HeaderClearState> header_clear_state_;
    std::uint64_t header_epoch_ = 0;

    // LRU tracking: front = most recently accessed (mutable for const get())
    // Includes present nodes and non-present placeholders so failed fetches
    // cannot grow store_ without bound.
    using LruList = std::list<Hash256>;
    mutable LruList lru_;
    mutable std::map<Hash256, LruList::iterator> lru_map_;

    void
    touch_lru(Hash256 const& hash) const;

    using HeaderLruList = std::list<uint32_t>;
    mutable HeaderLruList header_lru_;
    mutable std::map<uint32_t, HeaderLruList::iterator> header_lru_map_;
    size_t max_header_entries_ = 0;

    void
    touch_header_lru(uint32_t ledger_seq) const;

    void
    refresh_header_entries_projection_locked() noexcept;

    // Deterministic interleaving seams. Production instances leave these
    // empty; tests use them to stop precisely at the authority boundaries.
    std::function<void()> before_node_send_commit_for_test_;
    std::function<void()> after_node_waiter_attached_for_test_;
    std::function<void()> before_node_fanout_for_test_;
    std::function<void()> after_managed_slot_registered_for_test_;
    std::function<void()> after_managed_ingress_reconciled_for_test_;
    std::function<void()> before_node_deadline_launch_for_test_;
    std::function<void()> node_deadline_servant_hook_for_test_;
    std::function<void()> after_node_deadline_wait_for_test_;
    std::function<void()> after_node_deadline_commit_for_test_;
    std::function<void()> node_round_retired_hook_for_test_;
    std::function<void()> node_stop_sealed_hook_for_test_;
    std::function<void()> before_node_response_prepare_for_test_;
    std::function<void()> before_node_response_commit_for_test_;
    std::function<void()> after_node_response_commit_for_test_;
    std::function<void()> before_node_primary_pin_commit_for_test_;
    std::function<void()> before_node_waiter_attach_commit_for_test_;
    std::function<void()> before_node_stop_driver_launch_for_test_;
    std::function<void()> node_stop_driver_hook_for_test_;
    std::function<void()> after_node_local_pins_destroyed_for_test_;
    std::function<void()> before_header_clear_driver_launch_for_test_;

    // Stats — atomic for lock-free concurrent access
    mutable std::atomic<size_t> hits_{0};
    mutable std::atomic<size_t> misses_{0};
    std::atomic<size_t> fetches_{0};
    std::atomic<size_t> fetch_errors_{0};
    std::atomic<size_t> raw_fetches_projected_{0};
    std::atomic<size_t> raw_writes_confirmed_{0};
    std::atomic<size_t> raw_not_written_{0};
    std::atomic<size_t> managed_fetches_reserved_{0};
    std::atomic<size_t> managed_fetches_refused_{0};
    std::atomic<size_t> managed_writes_confirmed_{0};
    std::atomic<size_t> managed_not_written_{0};
    std::atomic<size_t> managed_indeterminate_{0};
    std::atomic<size_t> hash_mismatches_{0};
    std::atomic<size_t> waiter_wakeups_{0};

    static LogPartition log_;
};

}  // namespace catl::peer_client
