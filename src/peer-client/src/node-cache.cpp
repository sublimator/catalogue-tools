#include <catl/peer-client/node-cache.h>

#include "ripple.pb.h"

#include <catl/core/context-executor.h>
#include <catl/core/log-macros.h>
#include <catl/core/request-context.h>
#include <catl/crypto/sha512-half-hasher.h>
#include <catl/peer-client/ledger-fetch-coordinator.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>  // for reset_cancellation_state
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <cstring>
#include <limits>
#include <string_view>
#include <unordered_set>

namespace catl::peer_client {

LogPartition NodeCache::log_("node-cache", LogLevel::INHERIT);

namespace {

void
complete_timer_noexcept(
    std::shared_ptr<SerializedTimerSignal> const& timer) noexcept
{
    if (!timer)
        return;
    timer->complete();
}

template <typename Round>
void
complete_header_round(std::shared_ptr<Round> const& round) noexcept
{
    if (round && !round->completed.exchange(true, std::memory_order_acq_rel))
    {
        round->close_and_release_peer_pins();
        complete_timer_noexcept(round->signal);
    }
}

asio::awaitable<std::pair<int, boost::system::error_code>>
wait_for_signal_or_progress(
    std::shared_ptr<SerializedTimerSignal> done_signal,
    std::shared_ptr<SerializedTimerSignal> progress_signal)
{
    auto ex = co_await asio::this_coro::executor;
    auto [order, ex0, ec0, ex1, ec1] =
        co_await asio::experimental::make_parallel_group(
            asio::co_spawn(
                ex,
                [done_signal]() -> asio::awaitable<boost::system::error_code> {
                    co_return co_await done_signal->wait();
                },
                asio::deferred),
            asio::co_spawn(
                ex,
                [progress_signal]()
                    -> asio::awaitable<boost::system::error_code> {
                    co_return co_await progress_signal->wait();
                },
                asio::deferred))
            .async_wait(
                asio::experimental::wait_for_one(), asio::use_awaitable);

    if (order[0] == 0)
        co_return std::pair{0, ec0};
    co_return std::pair{1, ec1};
}

asio::awaitable<std::pair<int, boost::system::error_code>>
wait_for_signal_or_progress_or_cancel(
    std::shared_ptr<SerializedTimerSignal> done_signal,
    std::shared_ptr<SerializedTimerSignal> progress_signal,
    std::shared_ptr<std::atomic<bool>> cancel)
{
    if (!cancel)
        co_return co_await wait_for_signal_or_progress(
            std::move(done_signal), std::move(progress_signal));

    static constexpr auto kCancelPollInterval = std::chrono::milliseconds(25);

    auto ex = co_await asio::this_coro::executor;
    while (!cancel->load(std::memory_order_relaxed))
    {
        auto [order, ex0, ec0, ex1, ec1, ex2, ec2] =
            co_await asio::experimental::make_parallel_group(
                asio::co_spawn(
                    ex,
                    [done_signal]()
                        -> asio::awaitable<boost::system::error_code> {
                        co_return co_await done_signal->wait();
                    },
                    asio::deferred),
                asio::co_spawn(
                    ex,
                    [progress_signal]()
                        -> asio::awaitable<boost::system::error_code> {
                        co_return co_await progress_signal->wait();
                    },
                    asio::deferred),
                asio::co_spawn(
                    ex,
                    [ex]() -> asio::awaitable<boost::system::error_code> {
                        asio::steady_timer timer(ex, kCancelPollInterval);
                        boost::system::error_code ec;
                        co_await timer.async_wait(
                            asio::redirect_error(asio::use_awaitable, ec));
                        co_return ec;
                    },
                    asio::deferred))
                .async_wait(
                    asio::experimental::wait_for_one(), asio::use_awaitable);

        if (order[0] == 0)
            co_return std::pair{0, ec0};
        if (order[0] == 1)
            co_return std::pair{1, ec1};
    }

    co_return std::pair{2, boost::system::error_code{}};
}

}  // namespace

// ═══════════════════════════════════════════════════════════════════════
// Lifecycle
// ═══════════════════════════════════════════════════════════════════════

NodeCache::NodeCache(
    asio::io_context& io,
    Options opts,
    std::shared_ptr<LedgerFetchCoordinator> node_fetch_coordinator)
    : io_(io)
    , header_state_strand_(asio::make_strand(io))
    , max_entries_(opts.max_entries)
    , fetch_timeout_(opts.fetch_timeout)
    , managed_admission_timeout_(opts.managed_admission_timeout)
    , max_walk_peer_retries_(opts.max_walk_peer_retries)
    , fetch_stale_multiplier_(opts.fetch_stale_multiplier)
    , peer_refresh_timeout_secs_(opts.peer_refresh_timeout_secs)
    , header_timeout_floor_(opts.header_timeout_floor)
    , max_header_entries_(std::max<std::size_t>(
          128,
          std::min<std::size_t>(
              4096,
              std::max<std::size_t>(1, max_entries_ / 16))))
{
    if (node_fetch_coordinator)
        node_fetch_coordinator_.emplace(node_fetch_coordinator);
}

std::shared_ptr<NodeCache>
NodeCache::create(
    asio::io_context& io,
    Options opts,
    std::shared_ptr<LedgerFetchCoordinator> node_fetch_coordinator)
{
    return std::shared_ptr<NodeCache>(
        new NodeCache(io, std::move(opts), std::move(node_fetch_coordinator)));
}

std::shared_ptr<SerializedTimerSignal>
NodeCache::make_progress_signal()
{
    return SerializedTimerSignal::create(
        io_, asio::steady_timer::time_point::max());
}

std::string
NodeCache::format_progress(ProgressEvent const& event)
{
    auto peer = std::string_view(event.peer.data());
    auto prefix = "[shared] ";

    switch (event.kind)
    {
        case ProgressKind::retry_same_peer:
            if (event.depth != kNoDepth)
            {
                return prefix +
                    ("timeout depth=" + std::to_string(event.depth) +
                     " — retrying same peer " + std::string(peer) + " (" +
                     std::to_string(event.attempt) + "/" +
                     std::to_string(event.max_attempts) + ")");
            }
            return prefix +
                ("header seq=" + std::to_string(event.ledger_seq) +
                 " — retrying same peer " + std::string(peer) + " (" +
                 std::to_string(event.attempt) + "/" +
                 std::to_string(event.max_attempts) + ")");

        case ProgressKind::retry_covered_peer:
            return prefix +
                ("timeout depth=" + std::to_string(event.depth) +
                 " — retrying previously covered peer " + std::string(peer) +
                 " (" + std::to_string(event.attempt) + "/" +
                 std::to_string(event.max_attempts) + ")");

        case ProgressKind::switch_peer:
            if (event.depth != kNoDepth)
            {
                return prefix +
                    ("timeout depth=" + std::to_string(event.depth) +
                     " — switching peer (" + std::to_string(event.attempt) +
                     "/" + std::to_string(event.max_attempts) + ")");
            }
            return prefix +
                ("header seq=" + std::to_string(event.ledger_seq) +
                 " — switching peer (" + std::to_string(event.attempt) + "/" +
                 std::to_string(event.max_attempts) + ")");

        case ProgressKind::acquired_peer:
            if (event.depth != kNoDepth)
            {
                return prefix + ("acquired peer " + std::string(peer));
            }
            return prefix +
                ("header seq=" + std::to_string(event.ledger_seq) +
                 " — acquired peer " + std::string(peer));

        case ProgressKind::give_up:
            if (event.depth != kNoDepth)
            {
                return prefix +
                    ("giving up at depth=" + std::to_string(event.depth) +
                     " after " + std::to_string(event.attempt) + " retries");
            }
            return prefix +
                ("header seq=" + std::to_string(event.ledger_seq) +
                 " — giving up after " + std::to_string(event.attempt) +
                 " retries");

        case ProgressKind::no_peer_available:
            return prefix +
                ("no peer available for ledger " +
                 std::to_string(event.ledger_seq));
    }

    return std::string(prefix) + "unknown progress";
}

NodeCache::WaitRegistration
NodeCache::attach_waiter_locked(Entry& entry)
{
    if (!entry.progress.signal)
        entry.progress.signal = make_progress_signal();

    return WaitRegistration{
        .done_signal = entry.signal,
        .progress_signal = entry.progress.signal,
        .round = entry.round,
        .cursor = entry.progress.journal.subscribe()};
}

NodeCache::WaitRegistration
NodeCache::attach_waiter_locked(HeaderEntry& entry)
{
    if (!entry.progress.signal)
        entry.progress.signal = make_progress_signal();

    return WaitRegistration{
        .done_signal = entry.fetch ? entry.fetch->completion_signal : nullptr,
        .progress_signal = entry.progress.signal,
        .round = nullptr,
        .cursor = entry.progress.journal.subscribe()};
}

void
NodeCache::refresh_waiter_locked(Entry& entry, WaitRegistration& wait)
{
    // Admission->response is a phase change inside one exact round. A
    // replacement round belongs to a later caller and must not silently absorb
    // this waiter's retry budget.
    wait.done_signal = entry.round == wait.round ? entry.signal : nullptr;
    if (!entry.progress.signal)
        entry.progress.signal = make_progress_signal();
    wait.progress_signal = entry.progress.signal;
}

void
NodeCache::refresh_waiter_locked(HeaderEntry& entry, WaitRegistration& wait)
{
    wait.done_signal = entry.fetch ? entry.fetch->completion_signal : nullptr;
    if (!entry.progress.signal)
        entry.progress.signal = make_progress_signal();
    wait.progress_signal = entry.progress.signal;
}

std::shared_ptr<SerializedTimerSignal>
NodeCache::publish_progress_locked(
    Entry& entry,
    ProgressKind kind,
    uint32_t ledger_seq,
    uint16_t depth,
    uint8_t attempt,
    uint8_t max_attempts,
    std::string_view peer)
{
    ProgressEvent event;
    event.kind = kind;
    event.ledger_seq = ledger_seq;
    event.depth = depth;
    event.attempt = attempt;
    event.max_attempts = max_attempts;

    auto peer_len = std::min(peer.size(), event.peer.size() - 1);
    std::memcpy(event.peer.data(), peer.data(), peer_len);
    event.peer[peer_len] = '\0';

    auto wake = entry.progress.signal;
    entry.progress.journal.publish(event);
    entry.progress.signal = make_progress_signal();
    return wake;
}

std::shared_ptr<SerializedTimerSignal>
NodeCache::publish_progress_locked(
    HeaderEntry& entry,
    ProgressKind kind,
    uint32_t ledger_seq,
    uint16_t depth,
    uint8_t attempt,
    uint8_t max_attempts,
    std::string_view peer)
{
    ProgressEvent event;
    event.kind = kind;
    event.ledger_seq = ledger_seq;
    event.depth = depth;
    event.attempt = attempt;
    event.max_attempts = max_attempts;

    auto peer_len = std::min(peer.size(), event.peer.size() - 1);
    std::memcpy(event.peer.data(), peer.data(), peer_len);
    event.peer[peer_len] = '\0';

    auto wake = entry.progress.signal;
    entry.progress.journal.publish(event);
    entry.progress.signal = make_progress_signal();
    return wake;
}

void
NodeCache::publish_node_progress(
    Hash256 const& hash,
    ProgressKind kind,
    uint32_t ledger_seq,
    uint16_t depth,
    uint8_t attempt,
    uint8_t max_attempts,
    std::string_view peer)
{
    std::shared_ptr<SerializedTimerSignal> wake;
    {
        std::lock_guard lock(mutex_);
        auto it = store_.find(hash);
        if (it != store_.end() && !it->second.present)
        {
            wake = publish_progress_locked(
                it->second,
                kind,
                ledger_seq,
                depth,
                attempt,
                max_attempts,
                peer);
        }
    }

    if (wake)
    {
        waiter_wakeups_++;
        wake->complete();
    }
}

void
NodeCache::publish_header_progress(
    uint32_t ledger_seq,
    ProgressKind kind,
    uint8_t attempt,
    uint8_t max_attempts,
    std::string_view peer)
{
    std::shared_ptr<SerializedTimerSignal> wake;
    {
        std::lock_guard lock(mutex_);
        auto it = header_cache_.find(ledger_seq);
        if (it != header_cache_.end() && !it->second.present)
        {
            wake = publish_progress_locked(
                it->second,
                kind,
                ledger_seq,
                kNoDepth,
                attempt,
                max_attempts,
                peer);
        }
    }

    if (wake)
    {
        waiter_wakeups_++;
        wake->complete();
    }
}

// ═══════════════════════════════════════════════════════════════════════
// walk_to — the main API
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<WalkResult>
NodeCache::walk_to(
    Hash256 root_hash,
    Hash256 target_key,
    Hash256 ledger_hash,
    uint32_t ledger_seq,
    int tree_type,
    std::shared_ptr<PeerSet> peers,
    PeerGenerationPin peer,
    std::shared_ptr<std::atomic<bool>> cancel)
{
    if (peer)
    {
        auto const owner = peer.release_owner();
        if (!owner)
            throw PeerClientException(
                Error::Cancelled,
                "managed peer pin release owner no longer exists");
        if (peers && owner != peers)
            throw PeerClientException(
                Error::BadRequest,
                "managed peer pin does not belong to the supplied PeerSet");
    }
    auto* session = peer.get();
    co_return co_await walk_to_impl(
        root_hash,
        target_key,
        ledger_hash,
        ledger_seq,
        tree_type,
        std::move(peers),
        session,
        std::move(peer),
        nullptr,
        std::move(cancel));
}

asio::awaitable<WalkResult>
NodeCache::walk_to(
    Hash256 root_hash,
    Hash256 target_key,
    Hash256 ledger_hash,
    uint32_t ledger_seq,
    int tree_type,
    std::shared_ptr<PeerSet> peers,
    PeerSessionPtr peer,
    std::shared_ptr<std::atomic<bool>> cancel)
{
    auto* session = peer.get();
    co_return co_await walk_to_impl(
        root_hash,
        target_key,
        ledger_hash,
        ledger_seq,
        tree_type,
        std::move(peers),
        session,
        {},
        std::move(peer),
        std::move(cancel));
}

//@@authority transition=prv.T0009 status=provisional scope=function
// bind=catl::peer_client::walk_to_impl@21a821917bd52914
// owner=unresolved owner_hypothesis=node_cache commit=unresolved
// touches=prv.S0001 label=legacy_walk_owns_retry_and_peer_rotation
asio::awaitable<WalkResult>
NodeCache::walk_to_impl(
    Hash256 root_hash,
    Hash256 target_key,
    Hash256 ledger_hash,
    uint32_t ledger_seq,
    int tree_type,
    std::shared_ptr<PeerSet> peers,
    PeerSession* peer,
    PeerGenerationPin peer_pin,
    PeerSessionPtr unmanaged_owner,
    std::shared_ptr<std::atomic<bool>> cancel)
{
    // The unmanaged owner is intentionally retained by this frame. Managed
    // production sessions are retained by peer_pin instead.
    (void)unmanaged_owner;
    // ── CANCELLATION BOUNDARY ──
    // Shield all inner co_awaits (ensure_present, signal timers)
    // from the || operator's enable_total_cancellation. Without this,
    // operation_aborted propagates into shared NodeCache signal timers,
    // causing retry loops and orphaned entries.
    // Manual cancel token checked between depths for fast exit.
    co_await asio::this_coro::reset_cancellation_state(
        asio::disable_cancellation());

    WalkResult result;
    Hash256 cursor = root_hash;
    auto pos = SHAMapNodeID::root();

    // Speculative depth: request multiple depths in one TMGetLedger.
    // Halves each round: 8 → 4 → 2 → 1 (same strategy as old TreeWalker).
    int spec_depth = (tree_type == 2) ? 8 : 4;  // state=8, tx=4

    // ── Acquire peer ONCE up front (single strand hop) ──
    // This eliminates per-depth strand contention that was causing laggards
    // with 300+ concurrent proves. On failure mid-walk, we retry with a
    // different peer (max retries per walk).
    auto peer_covers = [](PeerSession const* p, uint32_t seq) {
        if (!p || !p->is_ready())
            return false;
        auto first = p->peer_first_seq();
        auto last = p->peer_last_seq();
        return first != 0 && last != 0 && seq >= first && seq <= last;
    };

    if (!peer_covers(peer, ledger_seq))
    {
        if (peers)
        {
            PLOGD(
                log_,
                "walk_to: peer ",
                (peer ? peer->endpoint() : "<none>"),
                " doesn't cover ledger ",
                ledger_seq,
                " — acquiring one");
            //@@authority transition=prv.T0010 status=provisional
            // bind=wait_for_peer@db2cede63330d449
            // scope=statement
            // owner=unresolved owner_hypothesis=peer_set commit=strand
            // calls=wait_for_peer
            // label=request_history_covered_peer_generation_for_walk
            peer_pin = co_await peers->wait_for_peer(
                ledger_seq, 10, cancel); //@@authority-target ids=prv.T0010
            peer = peer_pin.get();
        }
        if (!peer || !peer->is_ready())
        {
            PLOGW(log_, "walk_to: no peer available for ledger ", ledger_seq);
            catl::core::emit_status(
                "no peer available for ledger " + std::to_string(ledger_seq));
            publish_node_progress(
                cursor, ProgressKind::no_peer_available, ledger_seq, 0, 0, 0);
            //@@authority transition=prv.T0037 status=provisional
            // bind=co_return_statement@e868903f6c11abd4
            // scope=statement
            // owner=unresolved owner_hypothesis=ledger_data_control
            // commit=walk_coroutine
            // label=terminate_walk_when_history_covered_peer_search_exhausts
            co_return result; //@@authority-target ids=prv.T0037
        }
    }

    PLOGI(
        log_,
        "walk_to: root=",
        root_hash.hex().substr(0, 16),
        " key=",
        target_key.hex().substr(0, 16),
        " ledger=",
        ledger_hash.hex().substr(0, 16),
        " type=",
        tree_type,
        " spec=",
        spec_depth,
        " peer=",
        peer->endpoint());

    int total_retries = 0;
    int max_retries = max_walk_peer_retries_;
    bool waited_for_new_peer = false;
    std::unordered_set<std::string> failed_peers;

    for (int depth = 0; depth < 64; ++depth)
    {
        // Cancel checkpoint — fast atomic load, no strand hop
        if (cancel && cancel->load(std::memory_order_relaxed))
        {
            PLOGD(log_, "  depth=", depth, " CANCELLED before fetch");
            co_return result;
        }

        std::unordered_set<std::string> attempted_peers;
        std::unordered_set<std::string> written_peers;
        bool initiated_fetch = false;
        auto wire = co_await ensure_present(
            cursor,
            ledger_hash,
            tree_type,
            pos,
            target_key,
            spec_depth,
            peer,
            peer_pin.share(),
            peers,
            ledger_seq,
            cancel,
            &attempted_peers,
            &failed_peers,
            &initiated_fetch,
            &written_peers);

        // Halve speculation after each fetch round
        if (spec_depth > 1)
            spec_depth = std::max(1, spec_depth / 2);

        if (!wire)
        {
            // Check cancel token (client disconnect). Bail immediately —
            // entry stays alive for late responses / other callers.
            if (cancel && cancel->load(std::memory_order_relaxed))
            {
                PLOGD(log_, "  depth=", depth, " CANCELLED — stopping walk");
                co_return result;
            }

            // A timed-out fan-out round failed on every endpoint it used.
            // Carry that complete set into the next selection so each bounded
            // retry reaches a distinct connected peer.
            if (initiated_fetch)
            {
                for (auto const& endpoint : attempted_peers)
                    failed_peers.insert(endpoint);
                for (auto const& endpoint : written_peers)
                {
                    if (peers)
                        //@@authority transition=prv.T0029 status=provisional
                        // bind=note_ledger_failure@ad3bac49c609254b
                        // scope=statement
                        // owner=unresolved owner_hypothesis=peer_set
                        // commit=strand calls=note_ledger_failure
                        // label=project_round_failure_into_peer_history_evidence
                        peers->note_ledger_failure(
                            endpoint,
                            ledger_seq); //@@authority-target ids=prv.T0029
                }
            }

            if (total_retries < max_retries && peers)
            {
                // The completed Entry round has released its retained share.
                // Drop this frame's operation pin before asking PeerSet to
                // rotate/admit: at max_connected_peers=1, retaining the failed
                // generation here would make replacement impossible.
                peer_pin.reset();
                peer = nullptr;

                //@@authority transition=prv.T0030 status=provisional
                // bind=co_select_peers_for@f2b23bb4d7236db5
                // scope=statement
                // owner=unresolved owner_hypothesis=peer_set commit=strand
                // calls=co_select_peers_for
                // label=select_distinct_history_covered_retry_generation
                bool retrying_covered_peer =
                    false; //@@authority-target ids=prv.T0030
                auto alternatives = co_await peers->co_select_peers_for(
                    ledger_seq, 1, failed_peers);
                if (alternatives.empty() && !waited_for_new_peer)
                {
                    waited_for_new_peer = true;
                    //@@authority transition=prv.T0031 status=provisional
                    // bind=wait_for_peer@c0d523ca288cc259
                    // scope=statement
                    // owner=unresolved owner_hypothesis=peer_set commit=strand
                    // calls=wait_for_peer
                    // label=trigger_bounded_history_targeted_replacement_search
                    auto new_peer = co_await peers->wait_for_peer(
                        ledger_seq,
                        peer_refresh_timeout_secs_,
                        failed_peers,
                        cancel); //@@authority-target ids=prv.T0031
                    if (new_peer && new_peer->is_ready())
                        alternatives.push_back(std::move(new_peer));
                }
                if (alternatives.empty())
                {
                    // The distinct set is exhausted. Reacquire through
                    // PeerSet instead of retaining an old pin across
                    // replacement search. PeerSet's ledger-failure evidence
                    // still filters confirmed written timeouts; this fallback
                    // is therefore available only for unblamed/unknown
                    // publication or a recovered selection.
                    alternatives =
                        co_await peers->co_select_peers_for(ledger_seq, 1, {});
                    retrying_covered_peer = !alternatives.empty();
                }
                if (!alternatives.empty())
                {
                    peer_pin = std::move(alternatives.front());
                    peer = peer_pin.get();
                    ++total_retries;
                    PLOGW(
                        log_,
                        "  depth=",
                        depth,
                        " MISS hash=",
                        cursor.hex().substr(0, 16),
                        retrying_covered_peer
                            ? " — retrying previously covered peer ("
                            : " — trying different peer (",
                        total_retries,
                        "/",
                        max_retries,
                        "), excluded=",
                        failed_peers.size());
                    catl::core::emit_status(
                        "timeout depth=" + std::to_string(depth) +
                        (retrying_covered_peer
                             ? " — retrying previously covered peer "
                             : " — switching peer ") +
                        peer->endpoint() + " (" +
                        std::to_string(total_retries) + "/" +
                        std::to_string(max_retries) + ")");
                    publish_node_progress(
                        cursor,
                        retrying_covered_peer ? ProgressKind::retry_covered_peer
                                              : ProgressKind::switch_peer,
                        ledger_seq,
                        static_cast<uint16_t>(depth),
                        static_cast<uint8_t>(total_retries),
                        static_cast<uint8_t>(max_retries));
                    catl::core::emit_status(
                        "acquired peer " + peer->endpoint());
                    publish_node_progress(
                        cursor,
                        ProgressKind::acquired_peer,
                        ledger_seq,
                        static_cast<uint16_t>(depth),
                        static_cast<uint8_t>(total_retries),
                        static_cast<uint8_t>(max_retries),
                        peer->endpoint());
                    --depth;  // retry this depth
                    continue;
                }
            }

            // A small pool may have been covered completely by the first
            // fan-out. Preserve a bounded resend for packet loss after there
            // are no distinct or newly connected candidates left.
            if (total_retries < max_retries && !peers && peer &&
                peer->is_ready())
            {
                ++total_retries;
                PLOGW(
                    log_,
                    "  depth=",
                    depth,
                    " MISS hash=",
                    cursor.hex().substr(0, 16),
                    " — retrying peer after fan-out (",
                    total_retries,
                    "/",
                    max_retries,
                    ")");
                catl::core::emit_status(
                    "timeout depth=" + std::to_string(depth) +
                    " — retrying peer " + peer->endpoint() + " (" +
                    std::to_string(total_retries) + "/" +
                    std::to_string(max_retries) + ")");
                publish_node_progress(
                    cursor,
                    ProgressKind::retry_same_peer,
                    ledger_seq,
                    static_cast<uint16_t>(depth),
                    static_cast<uint8_t>(total_retries),
                    static_cast<uint8_t>(max_retries),
                    peer->endpoint());
                --depth;
                continue;
            }

            PLOGW(
                log_,
                "  depth=",
                depth,
                " MISS hash=",
                cursor.hex().substr(0, 16),
                " — giving up");
            catl::core::emit_status(
                "node fetch failed at depth=" + std::to_string(depth) +
                " after " + std::to_string(total_retries) + " retries");
            publish_node_progress(
                cursor,
                ProgressKind::give_up,
                ledger_seq,
                static_cast<uint16_t>(depth),
                static_cast<uint8_t>(total_retries),
                static_cast<uint8_t>(max_retries));
            //@@authority transition=prv.T0038 status=provisional
            // bind=co_return_statement@e868903f6c11abd4
            // scope=statement
            // owner=unresolved owner_hypothesis=ledger_data_control
            // commit=walk_coroutine
            // label=terminate_walk_after_bounded_transport_retry_exhaustion
            co_return result; //@@authority-target ids=prv.T0038
        }

        WireNodeView node(*wire);

        if (node.is_leaf())
        {
            result.found = true;
            result.leaf_nid = pos;
            auto leaf = node.leaf();
            auto data = leaf.data();
            result.leaf_data.assign(data.begin(), data.end());
            result.path.push_back({pos, cursor, wire});

            PLOGI(
                log_,
                "  depth=",
                depth,
                " LEAF hash=",
                cursor.hex().substr(0, 16),
                " data=",
                data.size(),
                "B");
            co_return result;
        }

        // Inner node — record it on the path (zero-copy via shared_ptr)
        result.path.push_back({pos, cursor, wire});

        // Collect sibling placeholders at this depth
        auto inner = node.inner();
        int byte_idx = depth / 2;
        int target_nibble = (depth % 2 == 0)
            ? (target_key.data()[byte_idx] >> 4) & 0xF
            : target_key.data()[byte_idx] & 0xF;

        inner.for_each_child([&](int branch, Key child_hash) {
            if (branch != target_nibble)
            {
                result.placeholders.push_back(
                    {pos.child(branch), child_hash.to_hash()});
            }
        });

        auto next = inner.child_hash(target_nibble);
        auto next_hash = next.to_hash();
        if (next_hash == Hash256::zero())
        {
            PLOGD(
                log_, "  depth=", depth, " DEAD END at nibble ", target_nibble);
            co_return result;  // found=false, branch is empty
        }

        PLOGD(
            log_,
            "  depth=",
            depth,
            " INNER hash=",
            cursor.hex().substr(0, 16),
            " → nibble=",
            target_nibble,
            " next=",
            next_hash.hex().substr(0, 16),
            " placeholders=",
            result.placeholders.size());

        cursor = next_hash;
        pos = pos.child(target_nibble);
    }

    PLOGW(log_, "  walk_to: reached max depth 64 without finding leaf");
    co_return result;
}

// ═══════════════════════════════════════════════════════════════════════
// ensure_present — cache check + fetch on miss
//
// Single shared signal per entry. All waiters co_await the same timer.
// When data arrives (insert_and_notify), signal is cancelled — all
// waiters wake. On timeout, callers return nullptr but the entry stays
// alive so late-arriving peer responses still get cached.
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<std::shared_ptr<std::vector<uint8_t>>>
NodeCache::ensure_present(
    Hash256 expected_hash,
    Hash256 ledger_hash,
    int tree_type,
    SHAMapNodeID position,
    Hash256 const& target_key,
    int speculative_depth,
    PeerSession* peer,
    PeerGenerationPin peer_pin,
    std::shared_ptr<PeerSet> peers,
    uint32_t ledger_seq,
    std::shared_ptr<std::atomic<bool>> cancel,
    std::unordered_set<std::string>* attempted_peers,
    std::unordered_set<std::string> const* excluded_peers,
    bool* initiated_fetch,
    std::unordered_set<std::string>* written_peers)
{
    if (initiated_fetch)
        *initiated_fetch = false;
    if (cancel && cancel->load(std::memory_order_relaxed))
        co_return nullptr;

    enum class Action { hit, return_null, wait, send_and_wait };
    Action action;
    std::shared_ptr<std::vector<uint8_t>> hit_data;
    std::shared_ptr<SerializedTimerSignal> send_signal;
    std::shared_ptr<SerializedTimerSignal> deadline_signal;
    std::shared_ptr<NodeFetchRound> deadline_round;
    std::shared_ptr<SerializedTimerSignal> superseded_signal;
    std::shared_ptr<NodeFetchRound> send_round;
    bool const managed_transport = node_fetch_coordinator_.has_value();

    auto const open_round = [&](Entry& entry, bool send) {
        if (entry.round)
            entry.round->phase = NodeFetchRound::Phase::retired;
        auto round = std::make_shared<NodeFetchRound>();
        round->phase = managed_transport && send
            ? NodeFetchRound::Phase::admission
            : NodeFetchRound::Phase::response;
        if (!send)
            round->response_possible = true;
        auto signal = round->phase == NodeFetchRound::Phase::admission
            ? make_progress_signal()
            : SerializedTimerSignal::create(io_, fetch_timeout_);
        if (round->phase == NodeFetchRound::Phase::admission)
        {
            round->admission_deadline =
                SerializedTimerSignal::create(io_, managed_admission_timeout_);
        }
        entry.round = round;
        entry.signal = signal;
        entry.round_opened_at = std::chrono::steady_clock::now();
        return std::pair{std::move(round), std::move(signal)};
    };
    auto const project_round_evidence =
        [&](std::shared_ptr<NodeFetchRound> const& round) {
            if (!round)
                return;
            bool accepted = false;
            std::lock_guard lock(mutex_);
            for (std::size_t i = 0; i < round->dispatch_count; ++i)
            {
                auto const& dispatch = round->dispatches[i];
                if (dispatch.state == NodeFetchRound::DispatchState::submitting)
                {
                    continue;
                }
                accepted = true;
                if (attempted_peers)
                    attempted_peers->insert(dispatch.endpoint);
                if (written_peers &&
                    dispatch.state == NodeFetchRound::DispatchState::written)
                {
                    written_peers->insert(dispatch.endpoint);
                }
            }
            if (initiated_fetch && accepted)
                *initiated_fetch = true;
        };

    {
        std::lock_guard lock(mutex_);
        //@@authority transition=prv.T0011 status=provisional scope=statement
        // bind=declaration@c92e96dd35b173c2
        // owner=unresolved owner_hypothesis=node_cache
        // commit=mutex touches=prv.S0001
        // label=admit_or_join_hash_keyed_shared_node_row
        auto [it, inserted] = store_.try_emplace(
            expected_hash); //@@authority-target ids=prv.T0011
        touch_lru(expected_hash);

        if (!inserted && it->second.present)
        {
            // HIT — already cached
            hits_++;
            touch_lru(expected_hash);
            hit_data = it->second.wire_data;
            action = Action::hit;
            PLOGD(
                log_,
                "  ensure: HIT hash=",
                expected_hash.hex().substr(0, 16),
                " (",
                hit_data->size(),
                "B)");
        }
        else if (!inserted)
        {
            // IN-FLIGHT — entry exists but data hasn't arrived yet.
            // Stale detection: if the last fetch was sent more than
            // N*timeout ago, the original sender likely timed out and
            // the peer probably dropped the request. Re-send with a
            // fresh signal.
            auto now = std::chrono::steady_clock::now();
            bool signal_missing = !it->second.signal;
            bool signal_expired =
                it->second.signal && it->second.signal->expiry() <= now;
            bool has_prior_dispatch = it->second.last_fetch_at !=
                std::chrono::steady_clock::time_point{};
            bool has_open_round = it->second.round_opened_at !=
                std::chrono::steady_clock::time_point{};
            auto const phase_timeout = it->second.round &&
                    it->second.round->phase == NodeFetchRound::Phase::admission
                ? managed_admission_timeout_
                : fetch_timeout_;
            auto stale_threshold = phase_timeout * fetch_stale_multiplier_;
            bool is_stale = has_open_round &&
                now - it->second.round_opened_at > stale_threshold;
            bool const admission_owner_active = it->second.round &&
                it->second.round->phase == NodeFetchRound::Phase::admission;

            if (!admission_owner_active &&
                (signal_missing || signal_expired || is_stale) && peer &&
                peer->is_ready())
            {
                // There is no active shared signal or the fetch is stale, so
                // refresh the wait handle and re-send with the current peer.
                it->second.peer_pins.clear();
                superseded_signal = it->second.signal;
                auto [round, signal] = open_round(it->second, true);
                send_round = std::move(round);
                send_signal = signal;
                deadline_round = send_round;
                deadline_signal = send_round->admission_deadline
                    ? send_round->admission_deadline
                    : signal;
                action = Action::send_and_wait;
                PLOGD(
                    log_,
                    "  ensure: ",
                    signal_missing ? "RESET"
                                   : (signal_expired ? "EXPIRED" : "STALE"),
                    " in-flight hash=",
                    expected_hash.hex().substr(0, 16),
                    " — re-sending");
            }
            else if (
                !admission_owner_active && (signal_missing || signal_expired) &&
                has_prior_dispatch)
            {
                // A prior fetch was sent, but there is no active timer left to
                // wait on. Create a fresh shared signal so another caller can
                // still benefit from a late response.
                auto [round, signal] = open_round(it->second, false);
                deadline_round = std::move(round);
                deadline_signal = std::move(signal);
                action = Action::wait;
                PLOGD(
                    log_,
                    "  ensure: ",
                    signal_missing ? "RESET" : "EXPIRED",
                    " (no peer) hash=",
                    expected_hash.hex().substr(0, 16),
                    " — fresh signal, waiting");
            }
            else if (signal_missing)
            {
                // No active timer and no evidence a fetch was ever sent.
                // There is nothing to wait for until a caller supplies a peer.
                action = Action::return_null;
                PLOGD(
                    log_,
                    "  ensure: RESET (no prior fetch) hash=",
                    expected_hash.hex().substr(0, 16),
                    " — no peer, returning miss");
            }
            else
            {
                action = Action::wait;
                PLOGD(
                    log_,
                    "  ensure: IN-FLIGHT hash=",
                    expected_hash.hex().substr(0, 16),
                    " — waiting on signal");
            }
        }
        else
        {
            // MISS — new entry, create signal and fetch
            misses_++;
            //@@authority transition=prv.T0012 status=provisional
            // bind=declaration@19ae7a56972085f7
            // scope=statement
            // owner=unresolved owner_hypothesis=node_cache
            // commit=mutex touches=prv.S0002
            // label=install_new_shared_round_signal
            auto [round, signal] = open_round(
                it->second, true); //@@authority-target ids=prv.T0012
            //@@authority transition=prv.T0013 status=provisional
            // bind=expression_statement@618c0b43a69e9a74
            // scope=statement
            // owner=unresolved owner_hypothesis=node_cache
            // commit=mutex touches=prv.S0005
            // label=publish_exact_round_admission_time
            send_round = std::move(round); //@@authority-target ids=prv.T0013
            send_signal = signal;
            deadline_round = send_round;
            deadline_signal = send_round->admission_deadline
                ? send_round->admission_deadline
                : signal;
            action = Action::send_and_wait;
            evict_if_needed();
            PLOGD(
                log_,
                "  ensure: MISS hash=",
                expected_hash.hex().substr(0, 16),
                " depth=",
                static_cast<int>(position.depth),
                " — fetching");
        }
    }  // mutex released

    if (superseded_signal)
        complete_timer_noexcept(superseded_signal);

    auto abandon_exact_send_round = [&]() {
        retire_node_fetch_round(expected_hash, send_signal);
    };

    if (action == Action::send_and_wait && before_node_send_commit_for_test_)
    {
        try
        {
            before_node_send_commit_for_test_();
        }
        catch (...)
        {
            abandon_exact_send_round();
            throw;
        }
    }

    if (deadline_signal &&
        !arm_node_round_deadline(
            expected_hash, deadline_round, deadline_signal))
    {
        retire_node_fetch_round(expected_hash, deadline_signal);
        co_return nullptr;
    }

    if (action == Action::hit)
        co_return hit_data;
    if (action == Action::return_null)
        co_return nullptr;
    if (action != Action::send_and_wait && cancel &&
        cancel->load(std::memory_order_relaxed))
    {
        co_return nullptr;
    }

    if (action == Action::wait)
    {
        catl::core::emit_status(
            "[shared] joining in-flight fetch hash=" +
            expected_hash.hex().substr(0, 16));
    }

    // Send fetch request if needed (non-blocking, just queues the packet)
    if (action == Action::send_and_wait)
    {
        bool transport_owns_round = false;
        auto const retire_unowned_round = [&] {
            // No transport owns this round. Leaving the signal and generation
            // pins retained would advertise a phantom in-flight request and
            // make every rider wait for a response that cannot exist.
            //@@authority transition=prv.T0046 status=provisional
            // bind=retire_node_fetch_round@b0fa6c1352fe3159
            // scope=statement
            // owner=unresolved owner_hypothesis=node_cache
            // commit=mutex calls=retire_node_fetch_round
            // touches=prv.S0002
            // label=retire_shared_round_when_no_transport_accepts_dispatch
            retire_node_fetch_round(
                expected_hash, send_signal); //@@authority-target ids=prv.T0046
        };

        if (!peer || !peer->is_ready())
        {
            PLOGW(
                log_,
                "  ensure: peer not ready for hash=",
                expected_hash.hex().substr(0, 16));
            fetch_errors_++;
            abandon_exact_send_round();
            co_return nullptr;
        }

        try
        {
            std::lock_guard lock(mutex_);
            auto it = store_.find(expected_hash);
            if (it == store_.end() || it->second.present ||
                it->second.signal != send_signal ||
                it->second.round != send_round)
            {
                co_return it != store_.end() && it->second.present
                    ? it->second.wire_data
                    : nullptr;
            }
            //@@authority transition=prv.T0001 status=provisional
            // bind=share@7fe56c43ace668b4
            // scope=statement owner=unresolved
            // owner_hypothesis=node_cache commit=mutex
            // touches=prv.S0003 calls=share
            // label=conditionally_retain_primary_peer_pin
            if (peer_pin)
                it->second.peer_pins.push_back(
                    peer_pin.share()); //@@authority-target ids=prv.T0001
            // The admission timestamp was committed with this exact signal.
            // This commit adds the primary transport pin without reopening
            // stale-round selection.
        }
        catch (...)
        {
            abandon_exact_send_round();
            throw;
        }

        try
        {
            //@@authority transition=prv.T0002 status=provisional
            // bind=send_fetch@20ce0e296674258d
            // scope=statement owner=unresolved
            // owner_hypothesis=node_cache commit=unresolved
            // calls=send_fetch
            // label=submit_primary_fetch_after_optional_pin_step
            auto const submission = send_fetch(
                expected_hash,
                ledger_hash,
                tree_type,
                position,
                target_key,
                speculative_depth,
                *peer,
                peer_pin ? peer_pin.share() : PeerGenerationPin{},
                send_round); //@@authority-target ids=prv.T0002
            transport_owns_round =
                submission != NodeFetchSubmission::managed_refused;
            if (submission == NodeFetchSubmission::raw_queued)
            {
                if (initiated_fetch)
                    *initiated_fetch = true;
                if (attempted_peers)
                    attempted_peers->insert(peer->endpoint());
                // Content-addressed late responses remain useful even if this
                // exact round was concurrently retired. Coordinator ingress
                // is not publication evidence, so only raw submission records
                // this legacy peer-attempt fact.
                std::lock_guard lock(mutex_);
                auto const it = store_.find(expected_hash);
                if (it != store_.end() && !it->second.present)
                {
                    auto& last_fetch = it->second.last_fetch_at;
                    auto const now = std::chrono::steady_clock::now();
                    //@@authority transition=prv.T0032 status=provisional
                    // bind=expression_statement@1dd391cdc59969e6
                    // scope=statement
                    // owner=unresolved owner_hypothesis=object_store
                    // commit=mutex touches=prv.S0006
                    // label=record_raw_content_addressed_dispatch
                    last_fetch = now; //@@authority-target ids=prv.T0032
                }
            }
        }
        catch (...)
        {
            abandon_exact_send_round();
            throw;
        }
        if (cancel && cancel->load(std::memory_order_relaxed))
        {
            if (managed_transport)
                close_managed_submissions(expected_hash, send_round);
            if (!transport_owns_round)
                retire_unowned_round();
            co_return nullptr;
        }

        try
        {
            // Fan-out: also send to additional peers from PeerSet.
            // First response wins via insert_and_notify (content-addressed).
            // Duplicate responses are harmless (already-present early return).
            if (peers && ledger_seq > 0)
            {
                if (before_node_fanout_for_test_)
                    before_node_fanout_for_test_();
                std::unordered_set<std::string> exclude_set = excluded_peers
                    ? *excluded_peers
                    : std::unordered_set<std::string>{};
                exclude_set.insert(peer->endpoint());
                //@@authority transition=prv.T0033 status=provisional
                // bind=co_select_peers_for@3529bb9ceb7113ae
                // scope=statement
                // owner=unresolved owner_hypothesis=peer_set commit=strand
                // calls=co_select_peers_for
                // label=select_history_covered_fanout_generations
                auto fanout_pins = co_await peers->co_select_peers_for(
                    ledger_seq,
                    2,
                    std::move(exclude_set)); //@@authority-target ids=prv.T0033

                for (auto& extra : fanout_pins)
                {
                    if (extra && extra->is_ready())
                    {
                        if (!retain_node_fanout_pin(
                                expected_hash, send_round, extra.share()))
                            break;
                        PLOGD(
                            log_,
                            "  ensure: fan-out to ",
                            extra->endpoint(),
                            " for hash=",
                            expected_hash.hex().substr(0, 16));
                        auto const submission = send_fetch(
                            expected_hash,
                            ledger_hash,
                            tree_type,
                            position,
                            target_key,
                            speculative_depth,
                            *extra,
                            extra.share(),
                            send_round);
                        if (submission != NodeFetchSubmission::managed_refused)
                            transport_owns_round = true;
                        if (submission == NodeFetchSubmission::raw_queued)
                        {
                            if (initiated_fetch)
                                *initiated_fetch = true;
                            if (attempted_peers)
                                attempted_peers->insert(extra->endpoint());
                            std::lock_guard lock(mutex_);
                            auto const it = store_.find(expected_hash);
                            if (it != store_.end() && !it->second.present)
                                it->second.last_fetch_at =
                                    std::chrono::steady_clock::now();
                        }
                        extra.reset();
                    }
                }
            }
        }
        catch (...)
        {
            if (managed_transport)
                close_managed_submissions(expected_hash, send_round);
            throw;
        }

        if (managed_transport)
            close_managed_submissions(expected_hash, send_round);

        if (!transport_owns_round)
        {
            retire_unowned_round();
            co_return nullptr;
        }

        if (cancel && cancel->load(std::memory_order_relaxed))
            co_return nullptr;
    }

    if (cancel && cancel->load(std::memory_order_relaxed))
        co_return nullptr;

    WaitRegistration wait;
    {
        std::lock_guard lock(mutex_);
        auto it = store_.find(expected_hash);
        if (it == store_.end())
            co_return nullptr;
        if (it->second.present)
        {
            touch_lru(expected_hash);
            co_return it->second.wire_data;
        }
        //@@authority transition=prv.T0014 status=provisional scope=statement
        // bind=attach_waiter_locked@92d86de861198d83
        // owner=unresolved owner_hypothesis=node_cache
        // commit=mutex touches=prv.S0004 calls=attach_waiter_locked
        // label=attach_request_rider_to_shared_round_progress
        wait = attach_waiter_locked(
            it->second); //@@authority-target ids=prv.T0014
    }

    if (after_node_waiter_attached_for_test_)
        after_node_waiter_attached_for_test_();

    if (!wait.done_signal)
    {
        project_round_evidence(wait.round);
        co_return nullptr;
    }

    for (;;)
    {
        auto const waited_signal = wait.done_signal;
        if (!waited_signal)
            co_return nullptr;
        auto [wake_kind, ec] = co_await wait_for_signal_or_progress_or_cancel(
            waited_signal, wait.progress_signal, cancel);

        std::vector<ProgressEvent> progress_events;
        {
            std::lock_guard lock(mutex_);
            auto it = store_.find(expected_hash);
            if (it == store_.end())
                co_return nullptr;

            if (it->second.present)
            {
                if (ec == asio::error::operation_aborted)
                    waiter_wakeups_++;
                touch_lru(expected_hash);
                co_return it->second.wire_data;
            }

            progress_events = it->second.progress.journal.replay(wait.cursor);
            refresh_waiter_locked(it->second, wait);
        }

        for (auto const& event : progress_events)
            catl::core::emit_status(format_progress(event));

        if (wake_kind == 2)
        {
            PLOGD(
                log_,
                "  ensure: CANCELLED hash=",
                expected_hash.hex().substr(0, 16));
            co_return nullptr;
        }

        if (wake_kind == 1)
        {
            if (!wait.done_signal)
                co_return nullptr;
            continue;
        }

        if (wait.done_signal && wait.done_signal != waited_signal)
        {
            // Managed publication committed a phase transition. The exact
            // round is unchanged; only its admission signal was replaced by a
            // fresh response-deadline signal.
            continue;
        }

        // Timed out and data still isn't present.
        // The entry stays alive so a late peer response can still
        // populate the cache for future callers.
        //@@authority transition=prv.T0015 status=provisional scope=statement
        // bind=handle_node_waiter_timeout@c8fb2eab1d0e7c03
        // owner=unresolved owner_hypothesis=node_cache
        // commit=mutex calls=handle_node_waiter_timeout
        // touches=prv.S0002 label=timeout_retires_exact_shared_round
        if (handle_node_waiter_timeout(
                expected_hash, wait.round, waited_signal, wait))
        {
            continue;
        } //@@authority-target ids=prv.T0015
        project_round_evidence(wait.round);
        if (ec != asio::error::operation_aborted)
        {
            PLOGD(
                log_,
                "  ensure: TIMEOUT hash=",
                expected_hash.hex().substr(0, 16));
            fetch_errors_++;
        }
        co_return nullptr;
    }
}

//@@authority transition=prv.T0034 status=provisional scope=function
// bind=catl::peer_client::retain_node_fanout_pin@986e8fd18b9f00c0
// owner=unresolved owner_hypothesis=node_cache commit=mutex
// touches=prv.S0003,prv.S0018
// label=retain_fanout_against_stable_round_identity
bool
NodeCache::retain_node_fanout_pin(
    Hash256 const& hash,
    std::shared_ptr<NodeFetchRound> const& round,
    PeerGenerationPin pin) noexcept
{
    try
    {
        std::lock_guard lock(mutex_);
        auto const it = store_.find(hash);
        if (!round || it == store_.end() || it->second.present ||
            !it->second.signal || it->second.round != round ||
            round->phase == NodeFetchRound::Phase::retired ||
            round->submissions_closed)
        {
            return false;
        }
        if (pin)
            it->second.peer_pins.push_back(std::move(pin));
        return true;
    }
    catch (...)
    {
        return false;
    }
}

//@@authority transition=prv.T0056 status=provisional scope=function
// bind=catl::peer_client::handle_node_waiter_timeout@c4790b17bbde32e5
// owner=unresolved owner_hypothesis=node_cache commit=mutex
// touches=prv.S0002,prv.S0018
// label=waiter_defers_admission_timeout_to_exact_round_owner
bool
NodeCache::handle_node_waiter_timeout(
    Hash256 const& hash,
    std::shared_ptr<NodeFetchRound> const& round,
    std::shared_ptr<SerializedTimerSignal> const& waited_signal,
    WaitRegistration& wait) noexcept
{
    bool retired = false;
    try
    {
        std::lock_guard lock(mutex_);
        auto const it = store_.find(hash);
        if (!round || it == store_.end() || it->second.present ||
            it->second.round != round || !it->second.signal ||
            round->phase == NodeFetchRound::Phase::retired)
        {
            return it != store_.end() && it->second.present;
        }

        if (it->second.signal != waited_signal)
        {
            refresh_waiter_locked(it->second, wait);
            return true;
        }

        if (round->phase == NodeFetchRound::Phase::admission)
        {
            // The waiter observes the admission timeout but owns no lifecycle
            // transition. The independent deadline servant will close
            // admission, classify only accepted slots, and either start the
            // response window or retire after the return-side handshake.
            return false;
        }

        it->second.signal.reset();
        it->second.peer_pins.clear();
        round->phase = NodeFetchRound::Phase::retired;
        retired = true;
    }
    catch (...)
    {
        return false;
    }

    if (retired)
    {
        complete_timer_noexcept(waited_signal);
        if (node_round_retired_hook_for_test_)
        {
            try
            {
                node_round_retired_hook_for_test_();
            }
            catch (...)
            {
            }
        }
    }
    return false;
}

// ═══════════════════════════════════════════════════════════════════════
// send_fetch — fire-and-forget TMGetLedger request
//
// Sends the request to the peer. The caller records successful submission.
// Does NOT wait for a response — that's handled by the shared signal in
// ensure_present. The response arrives asynchronously via
// on_node_response → insert_and_notify → signal cancelled.
// ═══════════════════════════════════════════════════════════════════════

//@@authority transition=prv.T0047 status=provisional scope=function
// bind=catl::peer_client::register_managed_submission@4e9db16b5b96c56f
// owner=unresolved owner_hypothesis=node_cache commit=mutex
// touches=prv.S0018 label=preinstall_managed_submission_handshake_slot
std::optional<std::size_t>
NodeCache::register_managed_submission(
    Hash256 const& hash,
    std::shared_ptr<NodeFetchRound> const& round,
    std::string_view endpoint)
{
    std::lock_guard lock(mutex_);
    auto const it = store_.find(hash);
    if (it == store_.end() || it->second.present || it->second.round != round ||
        !it->second.signal || round->phase == NodeFetchRound::Phase::retired ||
        round->submissions_closed ||
        round->dispatch_count == round->dispatches.size())
    {
        return std::nullopt;
    }

    auto const slot = round->dispatch_count++;
    round->dispatches[slot].endpoint = endpoint;
    round->dispatches[slot].state = NodeFetchRound::DispatchState::submitting;
    return slot;
}

//@@authority transition=prv.T0048 status=provisional scope=function
// bind=catl::peer_client::commit_managed_submission@8bb95378c9b77f97
// owner=unresolved owner_hypothesis=node_cache commit=mutex
// touches=prv.S0018 label=reconcile_reserved_return_with_early_outcome
void
NodeCache::commit_managed_submission(
    Hash256 const& hash,
    std::shared_ptr<NodeFetchRound> const& round,
    std::size_t slot) noexcept
{
    try
    {
        std::lock_guard lock(mutex_);
        if (!round || slot >= round->dispatch_count)
            return;
        auto& dispatch = round->dispatches[slot];
        if (dispatch.state == NodeFetchRound::DispatchState::submitting)
        {
            if (round->admission_expired)
            {
                dispatch.state = NodeFetchRound::DispatchState::indeterminate;
                round->response_possible = true;
                managed_indeterminate_.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                dispatch.state = NodeFetchRound::DispatchState::reserved;
            }
        }
        // An outcome may legally beat the synchronous reserved return on
        // another io_context thread. Its terminal state is already the
        // stronger fact and must not be overwritten.
    }
    catch (...)
    {
    }
    reconcile_managed_round(hash, round);
}

//@@authority transition=prv.T0049 status=provisional scope=function
// bind=catl::peer_client::rollback_managed_submission@73d9616462591ca5
// owner=unresolved owner_hypothesis=node_cache commit=mutex
// touches=prv.S0018 label=rollback_synchronously_refused_submission_slot
void
NodeCache::rollback_managed_submission(
    Hash256 const& hash,
    std::shared_ptr<NodeFetchRound> const& round,
    std::size_t slot) noexcept
{
    try
    {
        std::lock_guard lock(mutex_);
        if (!round || slot >= round->dispatch_count)
            return;

        auto& dispatch = round->dispatches[slot];
        if (dispatch.state != NodeFetchRound::DispatchState::submitting)
        {
            PLOGE(
                log_,
                "managed node submission produced an outcome after its "
                "synchronous ingress refusal");
            return;
        }

        // Submissions are made serially by one ensure_present frame, so a
        // synchronously refused provisional slot is necessarily the tail.
        if (slot + 1 != round->dispatch_count)
        {
            PLOGE(
                log_, "managed node submission rollback was not the tail slot");
            return;
        }
        dispatch = {};
        --round->dispatch_count;
    }
    catch (...)
    {
    }
    reconcile_managed_round(hash, round);
}

//@@authority transition=prv.T0050 status=provisional scope=function
// bind=catl::peer_client::close_managed_submissions@4c81761bc7dd9922
// owner=unresolved owner_hypothesis=node_cache commit=mutex
// touches=prv.S0018 label=seal_fanout_set_before_all_not_written_terminal
void
NodeCache::close_managed_submissions(
    Hash256 const& hash,
    std::shared_ptr<NodeFetchRound> const& round) noexcept
{
    try
    {
        std::lock_guard lock(mutex_);
        if (!round)
            return;
        round->submissions_closed = true;
    }
    catch (...)
    {
    }
    reconcile_managed_round(hash, round);
}

//@@authority transition=prv.T0051 status=provisional scope=function
// bind=catl::peer_client::settle_managed_submission@2d057973641c1f27
// owner=unresolved owner_hypothesis=node_cache commit=mutex
// touches=prv.S0018 label=consume_managed_publication_outcome_exactly_once
void
NodeCache::settle_managed_submission(
    Hash256 const& hash,
    std::shared_ptr<NodeFetchRound> const& round,
    std::size_t slot,
    ManagedSubmissionOutcome outcome) noexcept
{
    try
    {
        std::lock_guard lock(mutex_);
        if (!round || slot >= round->dispatch_count)
            return;

        auto& dispatch = round->dispatches[slot];
        switch (dispatch.state)
        {
            case NodeFetchRound::DispatchState::written:
            case NodeFetchRound::DispatchState::not_written:
            case NodeFetchRound::DispatchState::indeterminate:
                return;  // exact one-shot already consumed
            case NodeFetchRound::DispatchState::submitting:
            case NodeFetchRound::DispatchState::reserved:
                break;
        }

        switch (outcome)
        {
            case ManagedSubmissionOutcome::written:
                dispatch.state = NodeFetchRound::DispatchState::written;
                round->response_possible = true;
                managed_writes_confirmed_.fetch_add(
                    1, std::memory_order_relaxed);
                break;
            case ManagedSubmissionOutcome::not_written:
                dispatch.state = NodeFetchRound::DispatchState::not_written;
                managed_not_written_.fetch_add(1, std::memory_order_relaxed);
                break;
            case ManagedSubmissionOutcome::indeterminate:
                dispatch.state = NodeFetchRound::DispatchState::indeterminate;
                round->response_possible = true;
                managed_indeterminate_.fetch_add(1, std::memory_order_relaxed);
                break;
        }

        auto const it = store_.find(hash);
        if (outcome == ManagedSubmissionOutcome::written &&
            it != store_.end() && !it->second.present &&
            it->second.round == round)
        {
            it->second.last_fetch_at = std::chrono::steady_clock::now();
        }
    }
    catch (...)
    {
    }

    reconcile_managed_round(hash, round);
}

//@@authority transition=prv.T0055 status=provisional scope=function
// bind=catl::peer_client::reconcile_managed_round@09790c04f27df0f3
// owner=unresolved owner_hypothesis=node_cache commit=mutex
// touches=prv.S0018,prv.S0020
// label=reconcile_closed_managed_round_into_response_or_retirement
void
NodeCache::reconcile_managed_round(
    Hash256 const& hash,
    std::shared_ptr<NodeFetchRound> const& round) noexcept
{
    std::shared_ptr<SerializedTimerSignal> observed_signal;
    std::shared_ptr<SerializedTimerSignal> retire_signal;
    bool prepare_response = false;

    try
    {
        std::lock_guard lock(mutex_);
        auto const it = store_.find(hash);
        if (!round || it == store_.end() || it->second.present ||
            it->second.round != round || !it->second.signal ||
            round->phase != NodeFetchRound::Phase::admission ||
            !round->submissions_closed)
        {
            return;
        }

        for (std::size_t i = 0; i < round->dispatch_count; ++i)
        {
            if (round->dispatches[i].state ==
                NodeFetchRound::DispatchState::submitting)
            {
                return;
            }
        }

        observed_signal = it->second.signal;
        if (round->response_possible)
        {
            prepare_response = true;
        }
        else
        {
            bool const no_dispatches = round->dispatch_count == 0;
            bool all_not_written = !no_dispatches;
            for (std::size_t i = 0; i < round->dispatch_count; ++i)
            {
                if (round->dispatches[i].state !=
                    NodeFetchRound::DispatchState::not_written)
                {
                    all_not_written = false;
                    break;
                }
            }
            if (no_dispatches || all_not_written)
                retire_signal = observed_signal;
        }
    }
    catch (...)
    {
        return;
    }

    if (retire_signal)
    {
        retire_node_fetch_round(hash, retire_signal);
        return;
    }
    if (!prepare_response)
        return;

    std::shared_ptr<SerializedTimerSignal> response_signal;
    try
    {
        response_signal = SerializedTimerSignal::create(io_, fetch_timeout_);
    }
    catch (...)
    {
        retire_node_fetch_round(hash, observed_signal);
        return;
    }

    std::shared_ptr<SerializedTimerSignal> old_signal;
    std::shared_ptr<SerializedTimerSignal> admission_deadline;
    try
    {
        std::lock_guard lock(mutex_);
        auto const it = store_.find(hash);
        if (!round || it == store_.end() || it->second.present ||
            it->second.round != round || it->second.signal != observed_signal ||
            round->phase != NodeFetchRound::Phase::admission ||
            !round->submissions_closed || !round->response_possible)
        {
            return;
        }
        for (std::size_t i = 0; i < round->dispatch_count; ++i)
        {
            if (round->dispatches[i].state ==
                NodeFetchRound::DispatchState::submitting)
            {
                return;
            }
        }

        old_signal = it->second.signal;
        it->second.signal = response_signal;
        it->second.round_opened_at = std::chrono::steady_clock::now();
        round->phase = NodeFetchRound::Phase::response;
        admission_deadline = std::move(round->admission_deadline);
    }
    catch (...)
    {
        return;
    }

    complete_timer_noexcept(old_signal);
    complete_timer_noexcept(admission_deadline);
    if (!arm_node_round_deadline(hash, round, response_signal))
        retire_node_fetch_round(hash, response_signal);
}

void
NodeCache::record_raw_submission(
    Hash256 const& hash,
    std::shared_ptr<NodeFetchRound> const& round,
    std::string_view endpoint) noexcept
{
    try
    {
        std::lock_guard lock(mutex_);
        auto const it = store_.find(hash);
        if (!round || it == store_.end() || it->second.present ||
            it->second.round != round || !it->second.signal ||
            round->dispatch_count == round->dispatches.size())
        {
            return;
        }
        auto& slot = round->dispatches[round->dispatch_count++];
        slot.endpoint = endpoint;
        slot.state = NodeFetchRound::DispatchState::written;
        round->response_possible = true;
        it->second.last_fetch_at = std::chrono::steady_clock::now();
    }
    catch (...)
    {
    }
}

NodeCache::NodeFetchSubmission
NodeCache::send_fetch(
    Hash256 expected_hash,
    Hash256 ledger_hash,
    int tree_type,
    SHAMapNodeID position,
    Hash256 const& target_key,
    int speculative_depth,
    PeerSession& peer,
    PeerGenerationPin peer_pin,
    std::shared_ptr<NodeFetchRound> round)
{
    fetches_++;

    // Ensure the response handler is installed on this peer
    //@@authority transition=prv.T0016 status=provisional scope=statement
    // bind=ensure_response_handler@3eb6121101418a73
    // owner=unresolved owner_hypothesis=node_cache
    // commit=unresolved calls=ensure_response_handler
    // label=install_shared_response_route_on_selected_peer
    ensure_response_handler(peer); //@@authority-target ids=prv.T0016

    // Build nodeid list: primary + speculative deeper positions
    std::vector<SHAMapNodeID> node_ids = {position};

    if (speculative_depth > 1)
    {
        auto spec = position;
        for (int d = 1; d < speculative_depth; ++d)
        {
            uint8_t next_depth = spec.depth + 1;
            if (next_depth >= 64)
                break;

            int byte_idx = spec.depth / 2;
            int nibble = (spec.depth % 2 == 0)
                ? (target_key.data()[byte_idx] >> 4) & 0xF
                : target_key.data()[byte_idx] & 0xF;

            spec = spec.child(nibble);
            node_ids.push_back(spec);
        }
    }

    PLOGD(
        log_,
        "  send_fetch: hash=",
        expected_hash.hex().substr(0, 16),
        " depth=",
        static_cast<int>(position.depth),
        " nodeids=",
        node_ids.size(),
        " peer=",
        peer.endpoint());

    auto const refuse_managed = [&](char const* reason) {
        managed_fetches_refused_++;
        //@@authority transition=prv.T0043 status=provisional scope=statement
        // bind=expression_statement@0f736171e08fa72e
        // owner=unresolved owner_hypothesis=node_cache
        // commit=node_cache_observation
        // label=report_managed_ingress_refusal_without_raw_fallback
        PLOGW(
            log_,
            "  managed node-fetch ingress refused: ",
            reason,
            " peer=",
            peer.endpoint(),
            " hash=",
            expected_hash.hex().substr(
                0, 16)); //@@authority-target ids=prv.T0043
        return NodeFetchSubmission::managed_refused;
    };

    if (node_fetch_coordinator_)
    {
        if (!peer_pin)
            return refuse_managed("unpinned_peer");

        auto coordinator = node_fetch_coordinator_->lock();
        if (!coordinator)
            return refuse_managed("coordinator_expired");

        std::optional<NodeTree> tree;
        switch (tree_type)
        {
            case static_cast<int>(NodeTree::transaction):
                tree = NodeTree::transaction;
                break;
            case static_cast<int>(NodeTree::account_state):
                tree = NodeTree::account_state;
                break;
            default:
                return refuse_managed("invalid_tree_type");
        }

        //@@authority transition=prv.T0042 status=provisional scope=statement
        // bind=submit_node_batch_on_pin@8ee303c673d32fec
        // owner=unresolved owner_hypothesis=ledger_fetch_coordinator
        // commit=atomic calls=submit_node_batch_on_pin
        // label=route_existing_managed_generation_through_coordinator_ingress
        auto endpoint = peer.endpoint(); //@@authority-target ids=prv.T0042
        auto const slot = round
            ? register_managed_submission(expected_hash, round, endpoint)
            : std::optional<std::size_t>{};
        if (round && !slot)
        {
            return refuse_managed("stale_round");
        }
        if (round && after_managed_slot_registered_for_test_)
            after_managed_slot_registered_for_test_();

        auto weak = weak_from_this();
        std::weak_ptr<NodeFetchRound> weak_round = round;
        auto const ingress = coordinator->submit_node_batch_on_pin(
            ledger_hash,
            *tree,
            std::move(node_ids),
            std::move(peer_pin),
            [weak, expected_hash, weak_round, slot](
                LedgerFetchCoordinator::SubmissionOutcome outcome) {
                auto self = weak.lock();
                auto round = weak_round.lock();
                if (!self || !round || !slot)
                    return;
                ManagedSubmissionOutcome projected;
                switch (outcome)
                {
                    case LedgerFetchCoordinator::SubmissionOutcome::written:
                        projected = ManagedSubmissionOutcome::written;
                        break;
                    case LedgerFetchCoordinator::SubmissionOutcome::not_written:
                        projected = ManagedSubmissionOutcome::not_written;
                        break;
                    case LedgerFetchCoordinator::SubmissionOutcome::
                        indeterminate:
                        projected = ManagedSubmissionOutcome::indeterminate;
                        break;
                }
                self->settle_managed_submission(
                    expected_hash, round, *slot, projected);
            });
        if (ingress == LedgerFetchCoordinator::Ingress::reserved)
        {
            if (round)
                commit_managed_submission(expected_hash, round, *slot);
            if (round && after_managed_ingress_reconciled_for_test_)
                after_managed_ingress_reconciled_for_test_();
            managed_fetches_reserved_++;
            return NodeFetchSubmission::managed_reserved;
        }

        if (round)
            rollback_managed_submission(expected_hash, round, *slot);
        if (round && after_managed_ingress_reconciled_for_test_)
            after_managed_ingress_reconciled_for_test_();

        char const* reason = "unknown";
        switch (ingress)
        {
            case LedgerFetchCoordinator::Ingress::reserved:
                break;
            case LedgerFetchCoordinator::Ingress::ingress_sealed:
                reason = "ingress_sealed";
                break;
            case LedgerFetchCoordinator::Ingress::work_budget_exhausted:
                reason = "work_budget_exhausted";
                break;
            case LedgerFetchCoordinator::Ingress::node_count_invalid:
                reason = "node_count_invalid";
                break;
            case LedgerFetchCoordinator::Ingress::unpinned_peer:
                reason = "unpinned_peer";
                break;
            case LedgerFetchCoordinator::Ingress::projection_failed:
                reason = "projection_failed";
                break;
        }
        return refuse_managed(reason);
    }

    // Explicit raw compatibility mode: the NodeCache was constructed without a
    // coordinator capability. Managed mode never reaches this branch merely
    // because its pin or lifecycle owner is missing.
    raw_fetches_queued_++;
    auto endpoint = peer.endpoint();
    if (round)
        record_raw_submission(expected_hash, round, endpoint);
    //@@authority channel=prv.C0001 status=provisional scope=statement
    // bind=send_get_nodes@b9d7d090869dcd1b
    // endpoints=node_cache,peer_client
    // channel_semantics=legacy_uncorrelated calls=send_get_nodes
    // label=legacy_node_request_response_channel_without_local_correlation
    //@@authority transition=prv.T0003 status=provisional scope=statement
    // bind=send_get_nodes@b9d7d090869dcd1b
    // owner=unresolved owner_hypothesis=peer_client commit=strand
    // calls=send_get_nodes
    // label=queue_raw_compatibility_node_fetch_without_receipt
    peer.send_get_nodes(
        ledger_hash,
        tree_type,
        node_ids,
        [endpoint = std::move(endpoint)](boost::system::error_code ec) {
            PLOGD(
                log_,
                "  send_fetch write failed for ",
                endpoint,
                ": ",
                ec.message());
        }); //@@authority-target ids=prv.C0001,prv.T0003
    return NodeFetchSubmission::raw_queued;
}

// ═══════════════════════════════════════════════════════════════════════
// Response handler — feeds TMLedgerData directly into the cache
// ═══════════════════════════════════════════════════════════════════════

//@@authority transition=prv.T0017 status=provisional scope=function
// bind=catl::peer_client::ensure_response_handler@2046ece6aff84f9c
// owner=unresolved owner_hypothesis=peer_client commit=unresolved
// label=overwrite_peer_node_response_handler_for_shared_store
void
NodeCache::ensure_response_handler(PeerSession& peer)
{
    auto weak = weak_from_this();
    peer.set_node_response_handler(
        [weak](std::shared_ptr<protocol::TMLedgerData> const& msg) {
            if (auto self = weak.lock())
                self->on_node_response(msg);
        });

    PLOGD(log_, "  installed response handler on peer ", peer.endpoint());
}

//@@authority transition=prv.T0039 status=provisional scope=function
// bind=catl::peer_client::arm_node_round_deadline@a426f45bba4fe4e5
// owner=unresolved owner_hypothesis=node_cache
// commit=io_context_projection
// touches=prv.S0002 label=own_exact_round_deadline_servant
bool
NodeCache::arm_node_round_deadline(
    Hash256 hash,
    std::shared_ptr<NodeFetchRound> round,
    std::shared_ptr<SerializedTimerSignal> signal) noexcept
{
    try
    {
        auto weak = weak_from_this();
        asio::co_spawn(
            io_,
            [weak, hash, round, signal]() -> asio::awaitable<void> {
                try
                {
                    // Keep servant startup observably after successful co_spawn
                    // initiation. Tests can now fault the runtime body rather
                    // than merely the caller-side launch.
                    auto ex = co_await asio::this_coro::executor;
                    co_await asio::post(ex, asio::use_awaitable);
                    if (auto self = weak.lock();
                        self && self->node_deadline_servant_hook_for_test_)
                    {
                        self->node_deadline_servant_hook_for_test_();
                    }

                    auto ec = co_await signal->wait();
                    if (ec == asio::error::operation_aborted)
                        co_return;
                    if (auto self = weak.lock();
                        self && self->after_node_deadline_wait_for_test_)
                    {
                        self->after_node_deadline_wait_for_test_();
                    }
                    if (auto self = weak.lock())
                    {
                        self->expire_node_fetch_round(hash, round, signal);
                        if (self->after_node_deadline_commit_for_test_)
                            self->after_node_deadline_commit_for_test_();
                    }
                }
                catch (...)
                {
                    // A servant-body failure still consumes the exact
                    // lifecycle obligation it was created to own.
                    if (auto self = weak.lock())
                        self->retire_node_fetch_round(hash, signal);
                }
            },
            asio::detached);
        return true;
    }
    catch (std::exception const& e)
    {
        PLOGE(log_, "failed to arm node-round deadline: ", e.what());
    }
    catch (...)
    {
        PLOGE(log_, "failed to arm node-round deadline");
    }
    return false;
}

//@@authority transition=prv.T0052 status=provisional scope=function
// bind=catl::peer_client::expire_node_fetch_round@06c62132472cdf10
// owner=unresolved owner_hypothesis=node_cache commit=mutex
// touches=prv.S0018 label=classify_lost_publication_as_indeterminate
void
NodeCache::expire_node_fetch_round(
    Hash256 const& hash,
    std::shared_ptr<NodeFetchRound> const& round,
    std::shared_ptr<SerializedTimerSignal> const& signal) noexcept
{
    bool reconcile_admission = false;
    bool retire_response = false;

    try
    {
        std::lock_guard lock(mutex_);
        auto const it = store_.find(hash);
        bool const exact_deadline = round &&
            (round->phase == NodeFetchRound::Phase::admission
                 ? round->admission_deadline == signal
                 : it != store_.end() && it->second.signal == signal);
        if (!round || it == store_.end() || it->second.present ||
            it->second.round != round || !exact_deadline ||
            round->phase == NodeFetchRound::Phase::retired)
        {
            return;
        }

        if (round->phase == NodeFetchRound::Phase::admission)
        {
            // Expiry closes admission. A submitting slot is still only a
            // provisional return/callback handshake cell: its synchronous
            // return must decide reserved versus refused. Only already
            // reserved unresolved slots can conservatively become unknown.
            round->admission_expired = true;
            round->submissions_closed = true;
            round->admission_deadline.reset();
            for (std::size_t i = 0; i < round->dispatch_count; ++i)
            {
                auto& dispatch = round->dispatches[i];
                if (dispatch.state == NodeFetchRound::DispatchState::reserved)
                {
                    dispatch.state =
                        NodeFetchRound::DispatchState::indeterminate;
                    round->response_possible = true;
                    managed_indeterminate_.fetch_add(
                        1, std::memory_order_relaxed);
                }
            }
            reconcile_admission = true;
        }
        else
        {
            retire_response = true;
        }
    }
    catch (...)
    {
        return;
    }

    if (reconcile_admission)
        reconcile_managed_round(hash, round);
    else if (retire_response)
        retire_node_fetch_round(hash, signal);
}

//@@authority transition=prv.T0018 status=provisional scope=function
// bind=catl::peer_client::retire_node_fetch_round@77dfd4cb94a3d65b
// owner=unresolved owner_hypothesis=node_cache commit=mutex
// touches=prv.S0002 label=retire_exact_node_fetch_round_and_release_pins
void
NodeCache::retire_node_fetch_round(
    Hash256 const& hash,
    std::shared_ptr<SerializedTimerSignal> const& signal) noexcept
{
    bool retired = false;
    std::shared_ptr<SerializedTimerSignal> phase_signal;
    std::shared_ptr<SerializedTimerSignal> admission_deadline;
    try
    {
        std::lock_guard lock(mutex_);
        auto const it = store_.find(hash);
        if (it != store_.end() && !it->second.present &&
            (it->second.signal == signal ||
             (it->second.round &&
              it->second.round->admission_deadline == signal)))
        {
            phase_signal = it->second.signal;
            if (it->second.round)
            {
                admission_deadline =
                    std::move(it->second.round->admission_deadline);
            }
            it->second.signal.reset();
            it->second.peer_pins.clear();
            if (it->second.round)
                it->second.round->phase = NodeFetchRound::Phase::retired;
            retired = true;
        }
    }
    catch (...)
    {
        return;
    }

    if (!retired)
        return;
    complete_timer_noexcept(phase_signal);
    if (admission_deadline != phase_signal)
        complete_timer_noexcept(admission_deadline);
    if (node_round_retired_hook_for_test_)
    {
        try
        {
            node_round_retired_hook_for_test_();
        }
        catch (...)
        {
            // Test observation must not weaken the noexcept retirement path.
        }
    }
}

//@@authority transition=prv.T0019 status=provisional scope=function
// bind=catl::peer_client::on_node_response@c096b9c840f0b353
// owner=unresolved owner_hypothesis=object_store commit=unresolved
// label=accept_uncorrelated_peer_node_response
void
NodeCache::on_node_response(std::shared_ptr<protocol::TMLedgerData> const& msg)
{
    PLOGD(
        log_,
        "  on_response: type=",
        msg->type(),
        " nodes=",
        msg->nodes_size());

    for (int i = 0; i < msg->nodes_size(); ++i)
    {
        auto const& node = msg->nodes(i);
        if (!node.has_nodeid() || node.nodeid().size() != 33)
            continue;

        auto const& wire = node.nodedata();
        if (wire.empty())
            continue;

        std::span<const uint8_t> wire_span(
            reinterpret_cast<const uint8_t*>(wire.data()), wire.size());

        WireNodeView view(wire_span);
        Hash256 computed_hash;

        if (view.is_inner())
        {
            computed_hash = compute_inner_hash(wire_span);
            PLOGD(
                log_,
                "    response node depth=",
                static_cast<int>(static_cast<uint8_t>(node.nodeid()[32])),
                " INNER hash=",
                computed_hash.hex().substr(0, 16),
                " children=",
                view.inner().child_count());
        }
        else
        {
            // Leaf: compute the content hash using the correct prefix.
            // Wire format: [item_data][wire_type_byte]
            // Leaf hash: SHA512Half(prefix + item_data + key)
            // The key is the LAST 32 bytes of item_data.
            auto leaf_data = view.leaf().data();  // excludes wire type byte
            if (leaf_data.size() < 32)
            {
                PLOGW(log_, "    response LEAF too small: ", leaf_data.size());
                continue;
            }

            // Determine prefix from message type
            // liTX_NODE (1) → SNDprefix (tx with metadata)
            // liAS_NODE (2) → MLNprefix (account state leaf)
            static constexpr uint8_t tx_prefix[] = {'S', 'N', 'D', 0x00};
            static constexpr uint8_t state_prefix[] = {'M', 'L', 'N', 0x00};
            auto const* prefix = (msg->type() == 1) ? tx_prefix : state_prefix;

            // item_data = everything except the last 32 bytes (the key)
            // key = last 32 bytes
            auto const* key_ptr = leaf_data.data() + leaf_data.size() - 32;
            size_t data_size = leaf_data.size() - 32;

            crypto::Sha512HalfHasher hasher;
            hasher.update(prefix, 4);
            hasher.update(leaf_data.data(), data_size);
            hasher.update(key_ptr, 32);
            computed_hash = hasher.finalize();

            PLOGD(
                log_,
                "    response LEAF depth=",
                static_cast<int>(static_cast<uint8_t>(node.nodeid()[32])),
                " hash=",
                computed_hash.hex().substr(0, 16),
                " size=",
                wire.size());
        }

        //@@authority transition=prv.T0040 status=provisional scope=statement
        // bind=insert_and_notify@0958f75fea8a50d8
        // owner=unresolved owner_hypothesis=object_store
        // commit=mutex_via_insert_and_notify calls=insert_and_notify
        // touches=prv.S0001
        // label=project_validated_uncorrelated_node_into_content_store
        insert_and_notify(
            computed_hash,
            std::vector<uint8_t>(
                wire_span.begin(),
                wire_span.end())); //@@authority-target ids=prv.T0040
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Hash computation
// ═══════════════════════════════════════════════════════════════════════

Hash256
NodeCache::compute_inner_hash(std::span<const uint8_t> wire_data)
{
    WireNodeView view(wire_data);
    auto inner = view.inner();

    crypto::Sha512HalfHasher hasher;
    // MINprefix = {'M', 'I', 'N', 0x00} — inner node hash prefix
    static constexpr uint8_t prefix[] = {'M', 'I', 'N', 0x00};
    hasher.update(prefix, 4);

    // Build canonical 16×32 layout from wire format
    for (int i = 0; i < 16; ++i)
    {
        auto child = inner.child_hash(i);
        hasher.update(child.data(), 32);
    }

    return hasher.finalize();
}

// ═══════════════════════════════════════════════════════════════════════
// Cache operations
// ═══════════════════════════════════════════════════════════════════════

bool
NodeCache::insert(Hash256 const& hash, std::vector<uint8_t> data)
{
    std::lock_guard lock(mutex_);
    auto [it, inserted] = store_.try_emplace(hash);
    if (inserted || !it->second.present)
    {
        it->second.wire_data =
            std::make_shared<std::vector<uint8_t>>(std::move(data));
        it->second.present = true;
        it->second.signal = nullptr;
        if (it->second.round)
            it->second.round->phase = NodeFetchRound::Phase::retired;
        it->second.round.reset();
        it->second.peer_pins.clear();
        it->second.progress.signal = nullptr;
        touch_lru(hash);
        evict_if_needed();

        PLOGD(
            log_,
            "  insert: hash=",
            hash.hex().substr(0, 16),
            " (",
            it->second.wire_data->size(),
            "B) store_size=",
            store_.size());
        return true;
    }
    return false;
}

//@@authority transition=prv.T0020 status=provisional scope=function
// bind=catl::peer_client::insert_and_notify@b837ca69c236e302
// owner=unresolved owner_hypothesis=object_store commit=mutex
// touches=prv.S0001 label=commit_content_addressed_response_and_wake_round
void
NodeCache::insert_and_notify(Hash256 const& hash, std::vector<uint8_t> data)
{
    std::shared_ptr<SerializedTimerSignal> signal;
    std::shared_ptr<SerializedTimerSignal> admission_deadline;

    {
        std::lock_guard lock(mutex_);
        auto [it, inserted] = store_.try_emplace(hash);

        if (!inserted && it->second.present)
            return;  // already populated — skip

        signal = it->second.signal;  // may be null if no waiters
        if (it->second.round)
        {
            admission_deadline =
                std::move(it->second.round->admission_deadline);
        }
        it->second.wire_data =
            std::make_shared<std::vector<uint8_t>>(std::move(data));
        it->second.present = true;
        it->second.signal = nullptr;
        if (it->second.round)
            it->second.round->phase = NodeFetchRound::Phase::retired;
        it->second.round.reset();
        it->second.peer_pins.clear();
        it->second.progress.signal = nullptr;
        touch_lru(hash);
        evict_if_needed();

        PLOGD(
            log_,
            "  insert+notify: hash=",
            hash.hex().substr(0, 16),
            " waiters=",
            (signal ? "yes" : "no"),
            " store_size=",
            store_.size());
    }

    // Wake waiters outside the lock
    if (signal)
    {
        waiter_wakeups_++;
        signal->complete();
    }
    if (admission_deadline != signal)
        complete_timer_noexcept(admission_deadline);
}

void
NodeCache::send_header_fetch(
    uint32_t ledger_seq,
    PeerSession& peer,
    std::shared_ptr<HeaderFetchState> fetch,
    std::shared_ptr<HeaderRound> round,
    std::shared_ptr<PeerSet> peers)
{
    fetches_++;

    PLOGD(
        log_,
        "  send_header_fetch: seq=",
        ledger_seq,
        " peer=",
        peer.endpoint());

    std::weak_ptr<NodeCache> weak_self = shared_from_this();
    RequestOptions opts;
    opts.dedupe = false;

    peer.get_ledger_header(
        ledger_seq,
        [weak_self,
         ledger_seq,
         fetch,
         round,
         peers,
         endpoint = peer.endpoint()](Error err, LedgerHeaderResult result) {
            auto self = weak_self.lock();
            if (!self)
                return;

            asio::post(
                self->header_state_strand_,
                [weak_self,
                 ledger_seq,
                 fetch,
                 round,
                 peers,
                 endpoint,
                 err,
                 result = std::move(result)]() mutable {
                    auto self = weak_self.lock();
                    if (!self)
                        return;

                    {
                        std::lock_guard lock(self->mutex_);
                        auto const it = self->header_cache_.find(ledger_seq);
                        if (it == self->header_cache_.end() ||
                            it->second.fetch != fetch ||
                            fetch->phase != HeaderFetchState::Phase::running ||
                            fetch->active_round != round)
                        {
                            return;
                        }
                    }

                    if (err != Error::Success)
                    {
                        PLOGD(
                            NodeCache::log_,
                            "  send_header_fetch: FAIL seq=",
                            ledger_seq,
                            " peer=",
                            endpoint,
                            " err=",
                            static_cast<int>(err));

                        // Record per-peer failure for future peer selection
                        if (peers)
                        {
                            peers->note_ledger_failure(endpoint, ledger_seq);
                        }

                        // If all fan-out peers have failed, wake this round's
                        // owner instead of waiting for its timeout.
                        if (round->outstanding.fetch_sub(1) == 1 &&
                            round->dispatch_complete.load(
                                std::memory_order_acquire))
                        {
                            complete_header_round(round);
                        }
                        return;
                    }

                    round->outstanding.fetch_sub(1);
                    self->finish_header_fetch(
                        ledger_seq, fetch, std::move(result));

                    PLOGI(
                        NodeCache::log_,
                        "  send_header_fetch: OK seq=",
                        ledger_seq,
                        " peer=",
                        endpoint);
                });
        },
        opts);
}

asio::awaitable<LedgerHeaderResult>
NodeCache::get_header(
    uint32_t ledger_seq,
    std::shared_ptr<PeerSet> peers,
    PeerGenerationPin peer)
{
    if (peer)
    {
        auto const owner = peer.release_owner();
        if (!owner)
            throw PeerClientException(
                Error::Cancelled,
                "managed peer pin release owner no longer exists");
        if (peers && owner != peers)
            throw PeerClientException(
                Error::BadRequest,
                "managed peer pin does not belong to the supplied PeerSet");
    }
    co_return co_await get_header_impl(
        ledger_seq, std::move(peers), std::move(peer), nullptr);
}

asio::awaitable<LedgerHeaderResult>
NodeCache::get_header(
    uint32_t ledger_seq,
    std::shared_ptr<PeerSet> peers,
    PeerSessionPtr peer)
{
    co_return co_await get_header_impl(
        ledger_seq, std::move(peers), {}, std::move(peer));
}

asio::awaitable<LedgerHeaderResult>
NodeCache::get_header_impl(
    uint32_t ledger_seq,
    std::shared_ptr<PeerSet> peers,
    PeerGenerationPin peer_pin,
    PeerSessionPtr unmanaged_owner)
{
    if (!header_state_strand_.running_in_this_thread())
    {
        auto self = shared_from_this();
        auto state_executor = catl::core::ContextExecutor(
            header_state_strand_, catl::core::retain_current_request());
        co_return co_await asio::co_spawn(
            std::move(state_executor),
            [self,
             ledger_seq,
             peers = std::move(peers),
             peer_pin = std::move(peer_pin),
             unmanaged_owner = std::move(unmanaged_owner)]() mutable
            -> asio::awaitable<LedgerHeaderResult> {
                co_return co_await self->get_header_impl(
                    ledger_seq,
                    std::move(peers),
                    std::move(peer_pin),
                    std::move(unmanaged_owner));
            },
            asio::use_awaitable);
    }

    // Clear is a closed admission boundary, not a best-effort map erase. A
    // caller that races it receives a typed refusal and may retry after the
    // acknowledged boundary; it cannot mint post-clear work inside the drain.
    if (header_clear_state_)
        throw PeerClientException(
            Error::Cancelled, "ledger-header store is clearing");

    // Single owner-strand transaction: check present, join the exact operation,
    // or install one new HeaderFetchState. The detached driver is an executor
    // servant; HeaderEntry::fetch remains the operation authority.
    std::shared_ptr<HeaderFetchState> fetch;
    LedgerHeaderResult hit_result;
    bool start_fetch = false;
    {
        std::lock_guard lock(mutex_);
        auto const existing = header_cache_.find(ledger_seq);
        if ((existing == header_cache_.end() ||
             (!existing->second.present && !existing->second.fetch)) &&
            next_header_fetch_id_ == std::numeric_limits<std::uint64_t>::max())
        {
            throw PeerClientException(
                Error::ResourceExhausted,
                "ledger-header operation identity exhausted");
        }

        auto [it, inserted] = header_cache_.try_emplace(ledger_seq);
        touch_header_lru(ledger_seq);

        if (!inserted && it->second.present)
        {
            hit_result = it->second.result;
        }
        else if (!inserted && it->second.fetch)
        {
            fetch = it->second.fetch;
        }
        else
        {
            auto const round_timeout =
                std::max(fetch_timeout_, header_timeout_floor_);
            fetch = std::make_shared<HeaderFetchState>();
            fetch->completion_signal = SerializedTimerSignal::create(
                io_, asio::steady_timer::time_point::max());
            fetch->deadline_signal = SerializedTimerSignal::create(
                io_, round_timeout * 3 + std::chrono::seconds(5));
            fetch->driver_done_signal = SerializedTimerSignal::create(
                io_, asio::steady_timer::time_point::max());
            fetch->deadline_done_signal = SerializedTimerSignal::create(
                io_, asio::steady_timer::time_point::max());
            fetch->peers = std::move(peers);
            if (auto owner = peer_pin.release_owner())
                fetch->release_owners.push_back(std::move(owner));
            if (fetch->peers &&
                std::find(
                    fetch->release_owners.begin(),
                    fetch->release_owners.end(),
                    fetch->peers) == fetch->release_owners.end())
            {
                fetch->release_owners.push_back(fetch->peers);
            }
            fetch->initial_pin = std::move(peer_pin);
            fetch->unmanaged_owner = std::move(unmanaged_owner);
            fetch->operation_id = next_header_fetch_id_ + 1;
            fetch->epoch = header_epoch_;
            //@@authority transition=node_cache.install_header_fetch
            // bind=catl::peer_client::get_header_impl@0251e110e5833a33
            // owner=node_cache commit=header-state-strand
            active_header_fetches_.emplace(fetch->operation_id, fetch);
            next_header_fetch_id_ = fetch->operation_id;
            it->second.present = false;
            it->second.fetch = fetch;
            start_fetch = true;
            evict_headers_if_needed();
            refresh_header_entries_projection_locked();
        }
    }

    // A caller that joins someone else's shared operation contributes no
    // transport authority. Release its unused preferred generation before
    // suspending so coalescing cannot become an accidental eviction veto.
    if (!start_fetch)
    {
        peer_pin.reset();
        unmanaged_owner.reset();
        peers.reset();
    }

    if (!fetch)
        co_return hit_result;

    if (start_fetch)
    {
        auto self = shared_from_this();
        auto weak = weak_from_this();
        try
        {
            asio::co_spawn(
                header_state_strand_,
                [weak, ledger_seq, fetch]() -> asio::awaitable<void> {
                    auto ec = co_await fetch->deadline_signal->wait();
                    if (ec != asio::error::operation_aborted)
                    {
                        if (auto self = weak.lock())
                            self->finish_header_fetch(ledger_seq, fetch);
                    }
                },
                [weak, ledger_seq, fetch](std::exception_ptr error) {
                    // co_spawn invokes this only after the coroutine frame and
                    // all of its locals have been destroyed.
                    if (error)
                    {
                        if (auto self = weak.lock())
                            self->finish_header_fetch(ledger_seq, fetch);
                    }
                    if (auto self = weak.lock())
                        self->on_header_servant_done(fetch, false);
                });
        }
        catch (...)
        {
            finish_header_fetch(ledger_seq, fetch);
            on_header_servant_done(fetch, false);
        }

        auto owner_executor = catl::core::ContextExecutor(
            header_state_strand_, catl::core::retain_current_request());
        try
        {
            asio::co_spawn(
                std::move(owner_executor),
                [self, ledger_seq, fetch]() -> asio::awaitable<void> {
                    co_await self->drive_header_fetch(ledger_seq, fetch);
                },
                [weak, ledger_seq, fetch](std::exception_ptr error) {
                    // This acknowledgement is deliberately the co_spawn
                    // completion, not a finally inside drive_header_fetch:
                    // selection vectors and their pins are gone first.
                    if (error)
                    {
                        if (auto self = weak.lock())
                            self->finish_header_fetch(ledger_seq, fetch);
                    }
                    if (auto self = weak.lock())
                        self->on_header_servant_done(fetch, true);
                });
        }
        catch (...)
        {
            finish_header_fetch(ledger_seq, fetch);
            on_header_servant_done(fetch, true);
        }
    }

    co_return co_await wait_for_header_fetch(ledger_seq, fetch);
}

//@@authority transition=node_cache.ack_header_servant owner=node_cache
// bind=catl::peer_client::on_header_servant_done@37133936dce1376e
// commit=header-state-strand
void
NodeCache::on_header_servant_done(
    std::shared_ptr<HeaderFetchState> const& fetch,
    bool driver) noexcept
{
    if (!header_state_strand_.running_in_this_thread())
    {
        try
        {
            asio::post(
                header_state_strand_,
                [self = shared_from_this(), fetch, driver]() {
                    self->on_header_servant_done(fetch, driver);
                });
        }
        catch (...)
        {
            // No state is published off-strand. The done signal still lets a
            // committed clear remain fail-closed rather than wait on a frame
            // that has already ended.
            auto const& signal = driver ? fetch->driver_done_signal
                                        : fetch->deadline_done_signal;
            complete_timer_noexcept(signal);
        }
        return;
    }

    if (driver)
    {
        fetch->driver_frame_done = true;
        complete_timer_noexcept(fetch->driver_done_signal);
    }
    else
    {
        fetch->deadline_frame_done = true;
        complete_timer_noexcept(fetch->deadline_done_signal);
    }

    if (!fetch->driver_frame_done || !fetch->deadline_frame_done ||
        fetch->retirement_started)
    {
        return;
    }

    fetch->retirement_started = true;
    try
    {
        auto weak = weak_from_this();
        asio::co_spawn(
            header_state_strand_,
            [self = shared_from_this(), fetch]() -> asio::awaitable<void> {
                co_await self->retire_header_fetch(fetch);
            },
            [weak, fetch](std::exception_ptr error) {
                if (error)
                {
                    if (auto self = weak.lock())
                    {
                        PLOGE(
                            NodeCache::log_,
                            "header servant retirement failed: operation=",
                            fetch->operation_id);
                    }
                }
            });
    }
    catch (...)
    {
        // The operation stays in the active registry. A later acknowledged
        // clear can still reap it; silently erasing would manufacture a false
        // drain receipt.
        fetch->retirement_started = false;
    }
}

//@@authority transition=node_cache.retire_header_fetch owner=node_cache
// bind=catl::peer_client::retire_header_fetch@700aded8d2f46ea3
// commit=header-state-strand
asio::awaitable<void>
NodeCache::retire_header_fetch(std::shared_ptr<HeaderFetchState> fetch)
{
    co_await asio::this_coro::reset_cancellation_state(
        asio::disable_cancellation());

    for (auto const& owner : fetch->release_owners)
        co_await owner->co_generation_pin_release_barrier();

    if (!header_state_strand_.running_in_this_thread())
        co_await asio::post(header_state_strand_, asio::use_awaitable);

    auto const it = active_header_fetches_.find(fetch->operation_id);
    if (it != active_header_fetches_.end() && it->second == fetch)
        active_header_fetches_.erase(it);
}

//@@authority transition=node_cache.publish_header_terminal owner=node_cache
// bind=catl::peer_client::finish_header_fetch@1a4eb72d634143bf
// commit=header-state-strand
void
NodeCache::finish_header_fetch(
    uint32_t ledger_seq,
    std::shared_ptr<HeaderFetchState> const& fetch,
    std::optional<LedgerHeaderResult> result) noexcept
{
    std::shared_ptr<HeaderRound> active_round;
    std::shared_ptr<SerializedTimerSignal> completion_signal;
    std::shared_ptr<SerializedTimerSignal> deadline_signal;
    std::shared_ptr<SerializedTimerSignal> progress_signal;
    PeerGenerationPin initial_pin;
    PeerSessionPtr unmanaged_owner;

    {
        std::lock_guard lock(mutex_);
        auto const it = header_cache_.find(ledger_seq);
        if (it == header_cache_.end() || it->second.fetch != fetch ||
            fetch->phase != HeaderFetchState::Phase::running)
        {
            return;
        }

        fetch->phase = result ? HeaderFetchState::Phase::succeeded
                              : HeaderFetchState::Phase::failed;
        fetch->cancel->store(true, std::memory_order_release);
        active_round = std::move(fetch->active_round);
        initial_pin = std::move(fetch->initial_pin);
        unmanaged_owner = std::move(fetch->unmanaged_owner);
        completion_signal = fetch->completion_signal;
        deadline_signal = fetch->deadline_signal;
        progress_signal = it->second.progress.signal;

        if (result)
        {
            it->second.result = std::move(*result);
            it->second.present = true;
            it->second.fetch.reset();
            it->second.progress.signal.reset();
        }
        else
        {
            if (auto const lru_it = header_lru_map_.find(ledger_seq);
                lru_it != header_lru_map_.end())
            {
                header_lru_.erase(lru_it->second);
                header_lru_map_.erase(lru_it);
            }
            header_cache_.erase(it);
            refresh_header_entries_projection_locked();
        }
    }

    complete_header_round(active_round);
    complete_timer_noexcept(deadline_signal);
    complete_timer_noexcept(progress_signal);
    if (completion_signal)
    {
        waiter_wakeups_++;
        complete_timer_noexcept(completion_signal);
    }
}

asio::awaitable<LedgerHeaderResult>
NodeCache::wait_for_header_fetch(
    uint32_t ledger_seq,
    std::shared_ptr<HeaderFetchState> fetch)
{
    WaitRegistration wait;
    {
        std::lock_guard lock(mutex_);
        if (fetch->phase == HeaderFetchState::Phase::cleared)
            throw PeerClientException(Error::Cancelled);
        auto const it = header_cache_.find(ledger_seq);
        if (it == header_cache_.end())
            throw PeerClientException(Error::Timeout);
        if (it->second.present)
            co_return it->second.result;
        if (it->second.fetch != fetch ||
            fetch->phase != HeaderFetchState::Phase::running)
        {
            throw PeerClientException(Error::Timeout);
        }
        wait = attach_waiter_locked(it->second);
    }

    if (!wait.done_signal)
        throw PeerClientException(Error::Timeout);

    for (;;)
    {
        auto [wake_kind, ec] = co_await wait_for_signal_or_progress(
            wait.done_signal, wait.progress_signal);
        (void)ec;

        std::vector<ProgressEvent> progress_events;
        {
            std::lock_guard lock(mutex_);
            if (fetch->phase == HeaderFetchState::Phase::cleared)
                throw PeerClientException(Error::Cancelled);
            auto const it = header_cache_.find(ledger_seq);
            if (it == header_cache_.end())
                throw PeerClientException(Error::Timeout);
            if (it->second.present)
                co_return it->second.result;
            if (it->second.fetch != fetch ||
                fetch->phase != HeaderFetchState::Phase::running)
            {
                throw PeerClientException(Error::Timeout);
            }

            progress_events = it->second.progress.journal.replay(wait.cursor);
            refresh_waiter_locked(it->second, wait);
        }

        auto const cancel_state = co_await asio::this_coro::cancellation_state;
        if (cancel_state.cancelled() != asio::cancellation_type::none)
            throw boost::system::system_error(asio::error::operation_aborted);

        for (auto const& event : progress_events)
            catl::core::emit_status(format_progress(event));

        // A progress pulse only refreshes the journal cursor. Cancellation of
        // this observer was handled above without touching HeaderFetchState;
        // shared terminality remains exclusively operation-owned.
        if (wake_kind == 1)
            continue;
    }
}

asio::awaitable<void>
NodeCache::drive_header_fetch(
    uint32_t ledger_seq,
    std::shared_ptr<HeaderFetchState> fetch)
{
    co_await asio::this_coro::reset_cancellation_state(
        asio::disable_cancellation());

    static constexpr int kHeaderFanOut = 3;
    static constexpr std::uint8_t kMaxRounds = 3;
    auto const header_timeout = std::max(fetch_timeout_, header_timeout_floor_);

    auto round_is_active =
        [this, ledger_seq, &fetch](std::shared_ptr<HeaderRound> const& round) {
            std::lock_guard lock(mutex_);
            auto const it = header_cache_.find(ledger_seq);
            return it != header_cache_.end() && !it->second.present &&
                it->second.fetch == fetch &&
                fetch->phase == HeaderFetchState::Phase::running &&
                fetch->active_round == round;
        };

    auto begin_round = [this, ledger_seq, &fetch](std::uint8_t round_number) {
        auto round = std::make_shared<HeaderRound>();
        round->signal = SerializedTimerSignal::create(
            io_, asio::steady_timer::time_point::max());

        std::lock_guard lock(mutex_);
        auto const it = header_cache_.find(ledger_seq);
        if (it == header_cache_.end() || it->second.present ||
            it->second.fetch != fetch ||
            fetch->phase != HeaderFetchState::Phase::running)
        {
            return std::shared_ptr<HeaderRound>{};
        }
        fetch->round_number = round_number;
        fetch->round_peers.clear();
        fetch->active_round = round;
        return round;
    };

    auto send_one = [this, ledger_seq, &fetch, &round_is_active](
                        PeerSession* session,
                        PeerGenerationPin pin,
                        std::shared_ptr<HeaderRound> const& round) {
        if (!session || !session->is_ready() || !round_is_active(round))
            return false;

        auto const endpoint = session->endpoint();
        if (!round->try_begin_send(std::move(pin)))
            return false;

        fetch->tried_peers.insert(endpoint);
        fetch->round_peers.push_back(endpoint);
        try
        {
            send_header_fetch(ledger_seq, *session, fetch, round, fetch->peers);
        }
        catch (...)
        {
            fetch_errors_++;
            if (fetch->peers)
                fetch->peers->note_ledger_failure(endpoint, ledger_seq);
            if (round->outstanding.fetch_sub(1, std::memory_order_acq_rel) ==
                    1 &&
                round->dispatch_complete.load(std::memory_order_acquire))
            {
                complete_header_round(round);
            }
        }
        return true;
    };

    auto dispatch_round =
        [this, ledger_seq, &fetch, &round_is_active, &send_one, header_timeout](
            std::shared_ptr<HeaderRound> const& round,
            PeerGenerationPin primary_pin,
            PeerSessionPtr primary_owner) -> asio::awaitable<int> {
        int sent = 0;

        PeerSession* primary =
            primary_pin ? primary_pin.get() : primary_owner.get();
        if (primary)
        {
            if (send_one(primary, std::move(primary_pin), round))
                ++sent;
        }
        // HeaderRound is the only managed authority allowed to survive the
        // synchronous send. Do not retain a duplicate generation pin (or an
        // unmanaged owner) across peer selection: clear() must be able to
        // drain the admitted round while this detached driver is suspended.
        primary_pin.reset();
        primary_owner.reset();

        if (fetch->peers && sent < kHeaderFanOut && round_is_active(round))
        {
            auto extras = co_await fetch->peers->co_select_peers_for(
                ledger_seq, kHeaderFanOut - sent, fetch->tried_peers);
            for (auto& extra : extras)
            {
                if (!round_is_active(round))
                    break;
                auto* session = extra.get();
                if (send_one(session, std::move(extra), round))
                    ++sent;
            }
        }

        // Only the first empty round waits for targeted admission. The
        // operation-owned cancellation token makes clear/deadline drain
        // this wait instead of leaving a detached driver behind.
        if (sent == 0 && fetch->peers && fetch->total_peers_tried == 0 &&
            round_is_active(round))
        {
            auto fallback = co_await fetch->peers->wait_for_peer(
                ledger_seq,
                std::max(0, peer_refresh_timeout_secs_),
                fetch->tried_peers,
                fetch->cancel);
            if (round_is_active(round))
            {
                auto* session = fallback.get();
                if (send_one(session, std::move(fallback), round))
                    ++sent;
            }
            fallback.reset();
        }

        if (sent > 0 && round_is_active(round))
            round->signal->expires_after(header_timeout);

        round->dispatch_complete.store(true, std::memory_order_release);
        if (round->outstanding.load(std::memory_order_acquire) == 0)
            complete_header_round(round);

        co_return sent;
    };

    try
    {
        for (std::uint8_t round_number = 0; round_number < kMaxRounds;
             ++round_number)
        {
            auto round = begin_round(round_number);
            if (!round)
                co_return;

            PeerGenerationPin primary_pin;
            PeerSessionPtr primary_owner;
            if (round_number == 0)
            {
                std::lock_guard lock(mutex_);
                auto const it = header_cache_.find(ledger_seq);
                if (it == header_cache_.end() || it->second.fetch != fetch ||
                    fetch->phase != HeaderFetchState::Phase::running)
                {
                    co_return;
                }
                primary_pin = fetch->initial_pin.share();
                primary_owner = fetch->unmanaged_owner;
            }

            auto const sent = co_await dispatch_round(
                round, std::move(primary_pin), std::move(primary_owner));
            fetch->total_peers_tried += sent;

            if (round_number == 0)
            {
                PeerGenerationPin initial_pin;
                {
                    std::lock_guard lock(mutex_);
                    auto const it = header_cache_.find(ledger_seq);
                    if (it != header_cache_.end() && it->second.fetch == fetch)
                    {
                        initial_pin = std::move(fetch->initial_pin);
                    }
                }
            }

            if (!round_is_active(round))
                co_return;

            if (sent == 0)
            {
                catl::core::emit_status(
                    "header seq=" + std::to_string(ledger_seq) +
                    " — no peers available");
                publish_header_progress(
                    ledger_seq,
                    ProgressKind::no_peer_available,
                    0,
                    static_cast<std::uint8_t>(kHeaderFanOut));
                finish_header_fetch(ledger_seq, fetch);
                co_return;
            }

            std::string peer_list;
            for (auto const& endpoint : fetch->round_peers)
            {
                if (!peer_list.empty())
                    peer_list += ", ";
                peer_list += endpoint;
            }
            catl::core::emit_status(
                (round_number == 0 ? "anchor header: seq="
                                   : "header retry: seq=") +
                std::to_string(ledger_seq) + " asking " + std::to_string(sent) +
                " peer(s): " + peer_list);

            WaitRegistration wait;
            {
                std::lock_guard lock(mutex_);
                auto const it = header_cache_.find(ledger_seq);
                if (it == header_cache_.end() || it->second.fetch != fetch ||
                    fetch->phase != HeaderFetchState::Phase::running)
                {
                    co_return;
                }
                wait = attach_waiter_locked(it->second);
                wait.done_signal = round->signal;
            }

            for (;;)
            {
                auto [wake_kind, ec] = co_await wait_for_signal_or_progress(
                    wait.done_signal, wait.progress_signal);
                (void)ec;

                std::vector<ProgressEvent> progress_events;
                {
                    std::lock_guard lock(mutex_);
                    auto const it = header_cache_.find(ledger_seq);
                    if (it == header_cache_.end() || it->second.present ||
                        it->second.fetch != fetch ||
                        fetch->phase != HeaderFetchState::Phase::running ||
                        fetch->active_round != round)
                    {
                        co_return;
                    }
                    progress_events =
                        it->second.progress.journal.replay(wait.cursor);
                    refresh_waiter_locked(it->second, wait);
                    wait.done_signal = round->signal;
                }

                for (auto const& event : progress_events)
                    catl::core::emit_status(format_progress(event));

                if (wake_kind == 1)
                    continue;
                break;
            }

            complete_header_round(round);
            if (fetch->peers)
            {
                for (auto const& endpoint : fetch->round_peers)
                    fetch->peers->note_ledger_failure(endpoint, ledger_seq);
            }

            if (round_number + 1 < kMaxRounds)
            {
                publish_header_progress(
                    ledger_seq,
                    ProgressKind::switch_peer,
                    static_cast<std::uint8_t>(round_number + 2),
                    kMaxRounds);
            }
        }

        catl::core::emit_status(
            "header seq=" + std::to_string(ledger_seq) + " — giving up after " +
            std::to_string(fetch->total_peers_tried) + " peers across " +
            std::to_string(kMaxRounds) + " round(s)");
        publish_header_progress(
            ledger_seq,
            ProgressKind::give_up,
            static_cast<std::uint8_t>(fetch->total_peers_tried),
            static_cast<std::uint8_t>(kHeaderFanOut * kMaxRounds));
        finish_header_fetch(ledger_seq, fetch);
    }
    catch (...)
    {
        finish_header_fetch(ledger_seq, fetch);
    }
}

bool
NodeCache::has(Hash256 const& hash) const
{
    std::lock_guard lock(mutex_);
    auto it = store_.find(hash);
    return it != store_.end() && it->second.present;
}

std::shared_ptr<std::vector<uint8_t>>
NodeCache::get(Hash256 const& hash) const
{
    std::lock_guard lock(mutex_);
    auto it = store_.find(hash);
    if (it != store_.end() && it->second.present)
    {
        hits_++;
        touch_lru(hash);
        return it->second.wire_data;
    }
    misses_++;
    return nullptr;
}

size_t
NodeCache::size() const
{
    std::lock_guard lock(mutex_);
    size_t count = 0;
    for (auto const& [_, e] : store_)
    {
        if (e.present)
            count++;
    }
    return count;
}

size_t
NodeCache::bytes() const
{
    std::lock_guard lock(mutex_);
    size_t total = 0;
    for (auto const& [_, e] : store_)
    {
        if (e.present && e.wire_data)
            total += e.wire_data->size();
    }
    return total;
}

void
NodeCache::clear_nodes()
{
    std::vector<std::shared_ptr<SerializedTimerSignal>> signals;
    {
        std::lock_guard lock(mutex_);
        signals.reserve(store_.size() * 2);
        for (auto const& [_, entry] : store_)
        {
            if (entry.signal)
                signals.push_back(entry.signal);
            if (entry.progress.signal)
                signals.push_back(entry.progress.signal);
        }

        store_.clear();
        lru_.clear();
        lru_map_.clear();
    }

    for (auto const& signal : signals)
        complete_timer_noexcept(signal);
}

asio::awaitable<void>
NodeCache::co_clear_headers()
{
    if (!header_state_strand_.running_in_this_thread())
    {
        auto self = shared_from_this();
        auto state_executor = catl::core::ContextExecutor(
            header_state_strand_, catl::core::retain_current_request());
        co_return co_await asio::co_spawn(
            std::move(state_executor),
            [self]() -> asio::awaitable<void> {
                co_await self->co_clear_headers();
            },
            asio::use_awaitable);
    }

    auto clear = header_clear_state_;
    if (!clear)
    {
        //@@authority transition=node_cache.open_header_clear owner=node_cache
        // bind=catl::peer_client::co_clear_headers@8c2db24d22b8a532
        // commit=header-state-strand
        clear = std::make_shared<HeaderClearState>();
        clear->completion_signal = SerializedTimerSignal::create(
            io_, asio::steady_timer::time_point::max());
        header_clear_state_ = clear;

        try
        {
            auto self = shared_from_this();
            asio::co_spawn(
                header_state_strand_,
                [self, clear]() -> asio::awaitable<void> {
                    co_await self->drive_header_clear(clear);
                },
                [self, clear](std::exception_ptr error) {
                    if (error)
                    {
                        clear->failure = error;
                        clear->phase = HeaderClearState::Phase::failed;
                        // Failure after the boundary committed stays closed.
                    }
                    else
                    {
                        clear->phase = HeaderClearState::Phase::succeeded;
                        if (self->header_clear_state_ == clear)
                            self->header_clear_state_.reset();
                    }
                    clear->completion_signal->complete();
                });
        }
        catch (...)
        {
            // Launch failed before drive_header_clear could commit the epoch or
            // touch an entry, so this is a genuine owner-local rollback.
            clear->failure = std::current_exception();
            clear->phase = HeaderClearState::Phase::failed;
            if (header_clear_state_ == clear)
                header_clear_state_.reset();
            clear->completion_signal->complete();
        }
    }

    for (;;)
    {
        (void)co_await clear->completion_signal->wait();
        auto const cancel_state = co_await asio::this_coro::cancellation_state;
        if (cancel_state.cancelled() != asio::cancellation_type::none)
            throw boost::system::system_error(asio::error::operation_aborted);
        if (clear->phase != HeaderClearState::Phase::running)
            break;
    }

    if (clear->failure)
        std::rethrow_exception(clear->failure);
}

asio::awaitable<void>
NodeCache::drive_header_clear(std::shared_ptr<HeaderClearState> clear)
{
    co_await asio::this_coro::reset_cancellation_state(
        asio::disable_cancellation());

    struct DrainItem
    {
        std::shared_ptr<HeaderFetchState> fetch;
        std::shared_ptr<HeaderRound> round;
        std::shared_ptr<SerializedTimerSignal> progress_signal;
        PeerGenerationPin initial_pin;
        PeerSessionPtr unmanaged_owner;
    };

    if (header_clear_state_ != clear)
        co_return;
    if (header_epoch_ == std::numeric_limits<std::uint64_t>::max())
        throw PeerClientException(
            Error::ResourceExhausted, "ledger-header epoch exhausted");

    std::vector<DrainItem> drains;
    drains.reserve(active_header_fetches_.size());
    for (auto const& [_, fetch] : active_header_fetches_)
        drains.push_back(DrainItem{.fetch = fetch});

    // The active-operation registry, not the cache map, is the exhaustive
    // predecessor census. Terminal cache entries may already have released
    // HeaderEntry::fetch while their servants are still unwinding.
    //@@authority transition=node_cache.commit_header_clear owner=node_cache
    // bind=catl::peer_client::drive_header_clear@8624e9f5db72a96d
    // commit=header-state-strand
    ++header_epoch_;
    {
        std::lock_guard lock(mutex_);
        for (auto& drain : drains)
        {
            auto& fetch = drain.fetch;
            if (fetch->phase == HeaderFetchState::Phase::running)
                fetch->phase = HeaderFetchState::Phase::cleared;
            fetch->cancel->store(true, std::memory_order_release);
            drain.round = std::move(fetch->active_round);
            drain.initial_pin = std::move(fetch->initial_pin);
            drain.unmanaged_owner = std::move(fetch->unmanaged_owner);
        }
        for (auto const& [_, entry] : header_cache_)
        {
            if (entry.progress.signal)
            {
                // Store one retained signal on its owning operation when
                // possible; otherwise complete it immediately below.
                auto const fetch = entry.fetch;
                if (fetch)
                {
                    auto const found = std::find_if(
                        drains.begin(),
                        drains.end(),
                        [&fetch](DrainItem const& drain) {
                            return drain.fetch == fetch;
                        });
                    if (found != drains.end())
                        found->progress_signal = entry.progress.signal;
                    else
                        complete_timer_noexcept(entry.progress.signal);
                }
                else
                {
                    complete_timer_noexcept(entry.progress.signal);
                }
            }
        }

        header_cache_.clear();
        header_lru_.clear();
        header_lru_map_.clear();
        refresh_header_entries_projection_locked();
    }

    // Commit precedes every wake. Crucially, the clear servant must not retain
    // moved pins while awaiting the frame acknowledgements.
    for (auto& drain : drains)
    {
        complete_header_round(drain.round);
        complete_timer_noexcept(drain.fetch->completion_signal);
        complete_timer_noexcept(drain.fetch->deadline_signal);
        complete_timer_noexcept(drain.progress_signal);
        drain.initial_pin.reset();
        drain.unmanaged_owner.reset();
        drain.round.reset();
    }

    for (auto const& drain : drains)
    {
        if (drain.fetch->driver_done_signal)
            (void)co_await drain.fetch->driver_done_signal->wait();
        if (drain.fetch->deadline_done_signal)
            (void)co_await drain.fetch->deadline_done_signal->wait();
    }

    // Every pin destructor has now emitted its release command. Receipt the
    // actual owner PeerSets after those commands, once each.
    std::vector<std::shared_ptr<PeerSet>> peer_sets;
    peer_sets.reserve(drains.size());
    for (auto const& drain : drains)
    {
        for (auto const& peers : drain.fetch->release_owners)
        {
            if (peers &&
                std::find(peer_sets.begin(), peer_sets.end(), peers) ==
                    peer_sets.end())
            {
                peer_sets.push_back(peers);
            }
        }
    }
    for (auto const& peers : peer_sets)
        co_await peers->co_generation_pin_release_barrier();

    if (!header_state_strand_.running_in_this_thread())
        co_await asio::post(header_state_strand_, asio::use_awaitable);

    //@@authority transition=node_cache.complete_header_clear_drain
    // bind=for_range_loop@f7048c9fe53a8fa9
    // owner=node_cache commit=header-state-strand scope=statement
    for (auto const& drain : drains)
    {
        auto const it = active_header_fetches_.find(drain.fetch->operation_id);
        if (it != active_header_fetches_.end() && it->second == drain.fetch)
        {
            active_header_fetches_.erase(it);
        }
    } //@@authority-target ids=node_cache.complete_header_clear_drain
}

NodeCache::Stats
NodeCache::stats() const
{
    std::lock_guard lock(mutex_);
    size_t resident_entries = 0;
    for (auto const& [_, e] : store_)
    {
        if (e.present)
            resident_entries++;
    }
    return {
        .entries = store_.size(),
        .resident_entries = resident_entries,
        .header_entries =
            header_entries_projection_.load(std::memory_order_acquire),
        .max_entries = max_entries_,
        .hits = hits_,
        .misses = misses_,
        .fetches = fetches_,
        .fetch_errors = fetch_errors_,
        .managed_transport_selected = node_fetch_coordinator_.has_value(),
        .raw_fetches_queued = raw_fetches_queued_,
        .managed_fetches_reserved = managed_fetches_reserved_,
        .managed_fetches_refused = managed_fetches_refused_,
        .managed_writes_confirmed = managed_writes_confirmed_,
        .managed_not_written = managed_not_written_,
        .managed_indeterminate = managed_indeterminate_,
        .hash_mismatches = hash_mismatches_,
        .waiter_wakeups = waiter_wakeups_,
    };
}

void
NodeCache::refresh_header_entries_projection_locked() noexcept
{
    header_entries_projection_.store(
        header_cache_.size(), std::memory_order_release);
}

// ═══════════════════════════════════════════════════════════════════════
// LRU
// ═══════════════════════════════════════════════════════════════════════

void
NodeCache::touch_lru(Hash256 const& hash) const
{
    // Must be called under lock
    auto it = lru_map_.find(hash);
    if (it != lru_map_.end())
    {
        lru_.splice(lru_.begin(), lru_, it->second);
    }
    else
    {
        lru_.push_front(hash);
        lru_map_[hash] = lru_.begin();
    }
}

void
NodeCache::touch_header_lru(uint32_t ledger_seq) const
{
    // Must be called under lock
    auto it = header_lru_map_.find(ledger_seq);
    if (it != header_lru_map_.end())
    {
        header_lru_.splice(header_lru_.begin(), header_lru_, it->second);
    }
    else
    {
        header_lru_.push_front(ledger_seq);
        header_lru_map_[ledger_seq] = header_lru_.begin();
    }
}

void
NodeCache::evict_if_needed()
{
    // Must be called under lock
    size_t attempts = 0;
    while (lru_.size() > max_entries_ && !lru_.empty())
    {
        // Guard against infinite loop if all entries are in-flight
        if (++attempts > lru_.size())
            break;

        // Copy hash before pop_back — the reference would dangle
        auto victim = lru_.back();
        auto it = store_.find(victim);
        if (it != store_.end())
        {
            // Don't evict in-flight entries (non-null signal means
            // someone sent a fetch and waiters may be listening)
            if (!it->second.present && it->second.signal)
            {
                // Move to front instead of evicting
                lru_.splice(lru_.begin(), lru_, std::prev(lru_.end()));
                continue;
            }
            PLOGD(log_, "  evict: hash=", victim.hex().substr(0, 16));
            store_.erase(it);
        }
        lru_map_.erase(victim);
        lru_.pop_back();
    }
}

void
NodeCache::evict_headers_if_needed()
{
    // Must be called under lock
    size_t attempts = 0;
    while (header_lru_.size() > max_header_entries_ && !header_lru_.empty())
    {
        if (++attempts > header_lru_.size())
            break;

        auto victim = header_lru_.back();
        auto it = header_cache_.find(victim);
        if (it != header_cache_.end())
        {
            if (!it->second.present && it->second.fetch)
            {
                header_lru_.splice(
                    header_lru_.begin(),
                    header_lru_,
                    std::prev(header_lru_.end()));
                continue;
            }
            PLOGD(log_, "  evict header: seq=", victim);
            header_cache_.erase(it);
            refresh_header_entries_projection_locked();
        }
        header_lru_map_.erase(victim);
        header_lru_.pop_back();
    }
}

}  // namespace catl::peer_client
