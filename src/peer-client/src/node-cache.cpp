#include <catl/peer-client/node-cache.h>

#include "ripple.pb.h"

#include <catl/core/log-macros.h>
#include <catl/core/request-context.h>
#include <catl/crypto/sha512-half-hasher.h>

#include <boost/asio/redirect_error.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/this_coro.hpp>  // for reset_cancellation_state
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <cstring>
#include <string_view>
#include <unordered_set>

namespace catl::peer_client {

LogPartition NodeCache::log_("node-cache", LogLevel::INHERIT);

namespace {

asio::awaitable<std::pair<int, boost::system::error_code>>
wait_for_signal_or_progress(
    std::shared_ptr<asio::steady_timer> done_signal,
    std::shared_ptr<asio::steady_timer> progress_signal)
{
    auto ex = co_await asio::this_coro::executor;
    auto [order, ex0, ec0, ex1, ec1] =
        co_await asio::experimental::make_parallel_group(
            asio::co_spawn(
                ex,
                [done_signal]() -> asio::awaitable<boost::system::error_code> {
                    boost::system::error_code ec;
                    co_await done_signal->async_wait(
                        asio::redirect_error(asio::use_awaitable, ec));
                    co_return ec;
                },
                asio::deferred),
            asio::co_spawn(
                ex,
                [progress_signal]()
                    -> asio::awaitable<boost::system::error_code> {
                    boost::system::error_code ec;
                    co_await progress_signal->async_wait(
                        asio::redirect_error(asio::use_awaitable, ec));
                    co_return ec;
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
    std::shared_ptr<asio::steady_timer> done_signal,
    std::shared_ptr<asio::steady_timer> progress_signal,
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
                    [done_signal]() -> asio::awaitable<boost::system::error_code> {
                        boost::system::error_code ec;
                        co_await done_signal->async_wait(
                            asio::redirect_error(asio::use_awaitable, ec));
                        co_return ec;
                    },
                    asio::deferred),
                asio::co_spawn(
                    ex,
                    [progress_signal]()
                        -> asio::awaitable<boost::system::error_code> {
                        boost::system::error_code ec;
                        co_await progress_signal->async_wait(
                            asio::redirect_error(asio::use_awaitable, ec));
                        co_return ec;
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

NodeCache::NodeCache(asio::io_context& io, Options opts)
    : io_(io)
    , max_entries_(opts.max_entries)
    , fetch_timeout_(opts.fetch_timeout)
    , max_walk_peer_retries_(opts.max_walk_peer_retries)
    , fetch_stale_multiplier_(opts.fetch_stale_multiplier)
    , max_header_entries_(
          std::max<std::size_t>(
              128,
              std::min<std::size_t>(
                  4096, std::max<std::size_t>(1, max_entries_ / 16))))
{
}

std::shared_ptr<NodeCache>
NodeCache::create(asio::io_context& io, Options opts)
{
    return std::shared_ptr<NodeCache>(new NodeCache(io, std::move(opts)));
}

std::shared_ptr<asio::steady_timer>
NodeCache::make_progress_signal()
{
    return std::make_shared<asio::steady_timer>(
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
                return prefix + ("timeout depth=" +
                    std::to_string(event.depth) + " — retrying same peer " +
                    std::string(peer) + " (" +
                    std::to_string(event.attempt) + "/" +
                    std::to_string(event.max_attempts) + ")");
            }
            return prefix + ("header seq=" + std::to_string(event.ledger_seq) +
                " — retrying same peer " + std::string(peer) + " (" +
                std::to_string(event.attempt) + "/" +
                std::to_string(event.max_attempts) + ")");

        case ProgressKind::switch_peer:
            if (event.depth != kNoDepth)
            {
                return prefix + ("timeout depth=" +
                    std::to_string(event.depth) + " — switching peer (" +
                    std::to_string(event.attempt) + "/" +
                    std::to_string(event.max_attempts) + ")");
            }
            return prefix + ("header seq=" + std::to_string(event.ledger_seq) +
                " — switching peer (" + std::to_string(event.attempt) + "/" +
                std::to_string(event.max_attempts) + ")");

        case ProgressKind::acquired_peer:
            if (event.depth != kNoDepth)
            {
                return prefix + ("acquired peer " + std::string(peer));
            }
            return prefix + ("header seq=" + std::to_string(event.ledger_seq) +
                " — acquired peer " + std::string(peer));

        case ProgressKind::give_up:
            if (event.depth != kNoDepth)
            {
                return prefix + ("giving up at depth=" +
                    std::to_string(event.depth) + " after " +
                    std::to_string(event.attempt) + " retries");
            }
            return prefix + ("header seq=" + std::to_string(event.ledger_seq) +
                " — giving up after " + std::to_string(event.attempt) +
                " retries");

        case ProgressKind::no_peer_available:
            return prefix + ("no peer available for ledger " +
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
        .cursor = entry.progress.journal.subscribe()};
}

NodeCache::WaitRegistration
NodeCache::attach_waiter_locked(HeaderEntry& entry)
{
    if (!entry.progress.signal)
        entry.progress.signal = make_progress_signal();

    return WaitRegistration{
        .done_signal = entry.signal,
        .progress_signal = entry.progress.signal,
        .cursor = entry.progress.journal.subscribe()};
}

void
NodeCache::refresh_waiter_locked(Entry& entry, WaitRegistration& wait)
{
    wait.done_signal = entry.signal;
    if (!entry.progress.signal)
        entry.progress.signal = make_progress_signal();
    wait.progress_signal = entry.progress.signal;
}

void
NodeCache::refresh_waiter_locked(HeaderEntry& entry, WaitRegistration& wait)
{
    wait.done_signal = entry.signal;
    if (!entry.progress.signal)
        entry.progress.signal = make_progress_signal();
    wait.progress_signal = entry.progress.signal;
}

std::shared_ptr<asio::steady_timer>
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

std::shared_ptr<asio::steady_timer>
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
    std::shared_ptr<asio::steady_timer> wake;
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
        wake->cancel();
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
    std::shared_ptr<asio::steady_timer> wake;
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
        wake->cancel();
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
    PeerSessionPtr peer,
    std::shared_ptr<std::atomic<bool>> cancel)
{
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
    auto peer_covers = [](PeerSessionPtr const& p, uint32_t seq) {
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
            peer = co_await peers->wait_for_peer(ledger_seq, 10, cancel);
        }
        if (!peer || !peer->is_ready())
        {
            PLOGW(log_, "walk_to: no peer available for ledger ", ledger_seq);
            catl::core::emit_status(
                "no peer available for ledger " + std::to_string(ledger_seq));
            publish_node_progress(
                cursor,
                ProgressKind::no_peer_available,
                ledger_seq,
                0,
                0,
                0);
            co_return result;
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
    bool same_peer_retried = false;
    std::unordered_set<std::string> failed_peers;

    for (int depth = 0; depth < 64; ++depth)
    {
        // Cancel checkpoint — fast atomic load, no strand hop
        if (cancel && cancel->load(std::memory_order_relaxed))
        {
            PLOGD(log_, "  depth=", depth, " CANCELLED before fetch");
            co_return result;
        }

        auto wire = co_await ensure_present(
            cursor, ledger_hash, tree_type, pos, target_key, spec_depth,
            peer, peers, ledger_seq, cancel);

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

            if (total_retries >= max_retries)
            {
                // Exhausted retries — fall through to give up
            }
            // First retry: same peer (packet may have been dropped,
            // peer might still respond to a re-send). The entry stays
            // in the cache, and ensure_present will re-send via the
            // stale detection path.
            else if (!same_peer_retried && peer && peer->is_ready())
            {
                same_peer_retried = true;
                ++total_retries;
                PLOGW(
                    log_,
                    "  depth=",
                    depth,
                    " MISS hash=",
                    cursor.hex().substr(0, 16),
                    " — retrying same peer (",
                    total_retries,
                    "/",
                    max_retries,
                    ")");
                catl::core::emit_status(
                    "timeout depth=" + std::to_string(depth) +
                    " — retrying same peer " + peer->endpoint() +
                    " (" + std::to_string(total_retries) + "/" +
                    std::to_string(max_retries) + ")");
                publish_node_progress(
                    cursor,
                    ProgressKind::retry_same_peer,
                    ledger_seq,
                    static_cast<uint16_t>(depth),
                    static_cast<uint8_t>(total_retries),
                    static_cast<uint8_t>(max_retries),
                    peer->endpoint());
                --depth;  // retry this depth
                continue;
            }
            // Subsequent retries: different peer
            else if (peers)
            {
                same_peer_retried = false;
                if (peer)
                {
                    failed_peers.insert(peer->endpoint());
                    // Report to PeerSet so ALL callers skip this
                    // peer for this ledger (shared knowledge).
                    if (peers)
                        peers->note_ledger_failure(
                            peer->endpoint(), ledger_seq);
                }

                ++total_retries;
                PLOGW(
                    log_,
                    "  depth=",
                    depth,
                    " MISS hash=",
                    cursor.hex().substr(0, 16),
                    " — trying different peer (",
                    total_retries,
                    "/",
                    max_retries,
                    "), excluded=",
                    failed_peers.size());
                catl::core::emit_status(
                    "timeout depth=" + std::to_string(depth) +
                    " — switching peer (" +
                    std::to_string(total_retries) + "/" +
                    std::to_string(max_retries) + ")");
                publish_node_progress(
                    cursor,
                    ProgressKind::switch_peer,
                    ledger_seq,
                    static_cast<uint16_t>(depth),
                    static_cast<uint8_t>(total_retries),
                    static_cast<uint8_t>(max_retries));
                auto new_peer =
                    co_await peers->wait_for_peer(
                        ledger_seq, 10, failed_peers, cancel);
                if (new_peer && new_peer->is_ready())
                {
                    catl::core::emit_status(
                        "acquired peer " + new_peer->endpoint());
                    publish_node_progress(
                        cursor,
                        ProgressKind::acquired_peer,
                        ledger_seq,
                        static_cast<uint16_t>(depth),
                        static_cast<uint8_t>(total_retries),
                        static_cast<uint8_t>(max_retries),
                        new_peer->endpoint());
                    peer = new_peer;
                    --depth;  // retry this depth
                    continue;
                }
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
                " after " + std::to_string(total_retries) +
                " retries, will retry walk");
            publish_node_progress(
                cursor,
                ProgressKind::give_up,
                ledger_seq,
                static_cast<uint16_t>(depth),
                static_cast<uint8_t>(total_retries),
                static_cast<uint8_t>(max_retries));
            co_return result;  // found=false, caller retries
        }

        // Reset same-peer retry flag on success
        same_peer_retried = false;

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
    PeerSessionPtr peer,
    std::shared_ptr<PeerSet> peers,
    uint32_t ledger_seq,
    std::shared_ptr<std::atomic<bool>> cancel)
{
    if (cancel && cancel->load(std::memory_order_relaxed))
        co_return nullptr;

    enum class Action { hit, return_null, wait, send_and_wait };
    Action action;
    std::shared_ptr<std::vector<uint8_t>> hit_data;

    {
        std::lock_guard lock(mutex_);
        auto [it, inserted] = store_.try_emplace(expected_hash);
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
            bool signal_expired = it->second.signal &&
                it->second.signal->expiry() <= now;
            bool has_prior_fetch =
                it->second.last_fetch_at != std::chrono::steady_clock::time_point{};
            auto stale_threshold =
                fetch_timeout_ * fetch_stale_multiplier_;
            bool is_stale =
                now - it->second.last_fetch_at > stale_threshold;

            if ((signal_missing || signal_expired || is_stale) && peer &&
                peer->is_ready())
            {
                // There is no active shared signal or the fetch is stale, so
                // refresh the wait handle and re-send with the current peer.
                it->second.signal = std::make_shared<asio::steady_timer>(
                    io_, fetch_timeout_);
                action = Action::send_and_wait;
                PLOGD(
                    log_,
                    "  ensure: ",
                    signal_missing ? "RESET" : (signal_expired ? "EXPIRED" : "STALE"),
                    " in-flight hash=",
                    expected_hash.hex().substr(0, 16),
                    " — re-sending");
            }
            else if ((signal_missing || signal_expired) && has_prior_fetch)
            {
                // A prior fetch was sent, but there is no active timer left to
                // wait on. Create a fresh shared signal so another caller can
                // still benefit from a late response.
                it->second.signal = std::make_shared<asio::steady_timer>(
                    io_, fetch_timeout_);
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
            it->second.signal = std::make_shared<asio::steady_timer>(
                io_, fetch_timeout_);
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

    if (action == Action::hit)
        co_return hit_data;
    if (action == Action::return_null)
        co_return nullptr;
    if (cancel && cancel->load(std::memory_order_relaxed))
        co_return nullptr;

    if (action == Action::wait)
    {
        catl::core::emit_status(
            "[shared] joining in-flight fetch hash=" +
            expected_hash.hex().substr(0, 16));
    }

    // Send fetch request if needed (non-blocking, just queues the packet)
    if (action == Action::send_and_wait)
    {
        if (!peer || !peer->is_ready())
        {
            PLOGW(
                log_,
                "  ensure: peer not ready for hash=",
                expected_hash.hex().substr(0, 16));
            fetch_errors_++;
            {
                std::lock_guard lock(mutex_);
                auto it = store_.find(expected_hash);
                if (it != store_.end() && !it->second.present)
                    it->second.signal = nullptr;
            }
            co_return nullptr;
        }

        send_fetch(
            expected_hash,
            ledger_hash,
            tree_type,
            position,
            target_key,
            speculative_depth,
            peer);

        if (cancel && cancel->load(std::memory_order_relaxed))
            co_return nullptr;

        // Fan-out: also send to additional peers from PeerSet.
        // First response wins via insert_and_notify (content-addressed).
        // Duplicate responses are harmless (already-present early return).
        if (peers && ledger_seq > 0)
        {
            std::unordered_set<std::string> exclude_set;
            exclude_set.insert(peer->endpoint());
            auto extras = co_await peers->co_select_peers_for(
                ledger_seq, 2, std::move(exclude_set));

            for (auto const& extra : extras)
            {
                if (extra && extra->is_ready())
                {
                    PLOGD(
                        log_,
                        "  ensure: fan-out to ",
                        extra->endpoint(),
                        " for hash=",
                        expected_hash.hex().substr(0, 16));
                    send_fetch(
                        expected_hash,
                        ledger_hash,
                        tree_type,
                        position,
                        target_key,
                        speculative_depth,
                        extra);
                }
            }
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
        wait = attach_waiter_locked(it->second);
    }

    if (!wait.done_signal)
        co_return nullptr;

    for (;;)
    {
        auto [wake_kind, ec] = co_await wait_for_signal_or_progress_or_cancel(
            wait.done_signal, wait.progress_signal, cancel);

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
            continue;

        // Timed out (or write failed) and data still isn't present.
        // The entry stays alive so a late peer response can still
        // populate the cache for future callers.
        {
            std::lock_guard lock(mutex_);
            auto it = store_.find(expected_hash);
            if (it != store_.end() && !it->second.present &&
                it->second.signal == wait.done_signal)
            {
                it->second.signal = nullptr;
            }
        }
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

// ═══════════════════════════════════════════════════════════════════════
// send_fetch — fire-and-forget TMGetLedger request
//
// Sends the request to the peer and records last_fetch_at. Does NOT
// wait for a response — that's handled by the shared signal in
// ensure_present. The response arrives asynchronously via
// on_node_response → insert_and_notify → signal cancelled.
// ═══════════════════════════════════════════════════════════════════════

void
NodeCache::send_fetch(
    Hash256 expected_hash,
    Hash256 ledger_hash,
    int tree_type,
    SHAMapNodeID position,
    Hash256 const& target_key,
    int speculative_depth,
    PeerSessionPtr peer)
{
    fetches_++;

    // Ensure the response handler is installed on this peer
    ensure_response_handler(peer);

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
        peer->endpoint());

    // Send raw TMGetLedger — no pending_nodes registration.
    // send_get_nodes just queues an async write, safe outside lock.
    // On write failure (broken pipe etc), cancel the signal immediately
    // so waiters don't sit around for the full timeout.
    std::weak_ptr<NodeCache> weak_self = shared_from_this();
    auto cancel_hash = expected_hash;
    peer->send_get_nodes(
        ledger_hash,
        tree_type,
        node_ids,
        [weak_self, cancel_hash](boost::system::error_code) {
            if (auto self = weak_self.lock())
            {
                std::shared_ptr<asio::steady_timer> sig;
                {
                    std::lock_guard lock(self->mutex_);
                    auto it = self->store_.find(cancel_hash);
                    if (it != self->store_.end() && !it->second.present)
                        sig = it->second.signal;
                }
                if (sig)
                    sig->cancel();
            }
        });

    // Record when this fetch was sent (for stale detection)
    {
        std::lock_guard lock(mutex_);
        auto it = store_.find(expected_hash);
        if (it != store_.end())
        {
            it->second.last_fetch_at = std::chrono::steady_clock::now();
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Response handler — feeds TMLedgerData directly into the cache
// ═══════════════════════════════════════════════════════════════════════

void
NodeCache::ensure_response_handler(PeerSessionPtr peer)
{
    auto self = shared_from_this();
    peer->set_node_response_handler(
        [self](std::shared_ptr<protocol::TMLedgerData> const& msg) {
            self->on_node_response(msg);
        });

    PLOGD(log_, "  installed response handler on peer ", peer->endpoint());
}

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

        insert_and_notify(
            computed_hash,
            std::vector<uint8_t>(wire_span.begin(), wire_span.end()));
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

void
NodeCache::insert_and_notify(Hash256 const& hash, std::vector<uint8_t> data)
{
    std::shared_ptr<asio::steady_timer> signal;

    {
        std::lock_guard lock(mutex_);
        auto [it, inserted] = store_.try_emplace(hash);

        if (!inserted && it->second.present)
            return;  // already populated — skip

        signal = it->second.signal;  // may be null if no waiters
        it->second.wire_data =
            std::make_shared<std::vector<uint8_t>>(std::move(data));
        it->second.present = true;
        it->second.signal = nullptr;
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
        signal->cancel();
    }
}

void
NodeCache::send_header_fetch(
    uint32_t ledger_seq,
    PeerSessionPtr peer,
    std::shared_ptr<std::atomic<int>> outstanding,
    std::shared_ptr<PeerSet> peers)
{
    fetches_++;

    PLOGD(
        log_,
        "  send_header_fetch: seq=",
        ledger_seq,
        " peer=",
        peer->endpoint());

    std::weak_ptr<NodeCache> weak_self = shared_from_this();
    RequestOptions opts;
    opts.dedupe = false;

    peer->get_ledger_header(
        ledger_seq,
        [weak_self, ledger_seq, outstanding, peers,
         endpoint = peer->endpoint()](Error err, LedgerHeaderResult result) {
            auto self = weak_self.lock();
            if (!self)
                return;

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

                // If all fan-out peers have failed, cancel the signal
                // so waiters wake immediately instead of blocking until
                // the full timeout.
                if (outstanding->fetch_sub(1) == 1)
                {
                    std::shared_ptr<asio::steady_timer> sig;
                    {
                        std::lock_guard lock(self->mutex_);
                        auto it = self->header_cache_.find(ledger_seq);
                        if (it != self->header_cache_.end() &&
                            !it->second.present)
                        {
                            sig = it->second.signal;
                        }
                    }
                    if (sig)
                    {
                        sig->cancel();
                    }
                }
                return;
            }

            outstanding->fetch_sub(1);

            std::shared_ptr<asio::steady_timer> sig;
            {
                std::lock_guard lock(self->mutex_);
                auto it = self->header_cache_.find(ledger_seq);
                if (it == self->header_cache_.end())
                    return;
                if (it->second.present)
                    return;  // another peer already won

                sig = it->second.signal;
                it->second.result = result;
                it->second.present = true;
                it->second.signal = nullptr;
                it->second.progress.signal = nullptr;
                self->touch_header_lru(ledger_seq);
                self->evict_headers_if_needed();
            }
            if (sig)
            {
                self->waiter_wakeups_++;
                sig->cancel();
            }

            PLOGI(
                NodeCache::log_,
                "  send_header_fetch: OK seq=",
                ledger_seq,
                " peer=",
                endpoint);
        },
        opts);
}

asio::awaitable<LedgerHeaderResult>
NodeCache::get_header(
    uint32_t ledger_seq,
    std::shared_ptr<PeerSet> peers,
    PeerSessionPtr peer)
{
    // Single lock: check present → check in-flight → create in-flight.
    enum class Action { hit, wait, fetch };
    Action action;
    LedgerHeaderResult hit_result;

    {
        std::lock_guard lock(mutex_);
        auto [it, inserted] = header_cache_.try_emplace(ledger_seq);
        touch_header_lru(ledger_seq);

        if (!inserted && it->second.present)
        {
            action = Action::hit;
            hit_result = it->second.result;
            PLOGD(log_, "  get_header: HIT seq=", ledger_seq);
        }
        else if (!inserted && it->second.signal)
        {
            action = Action::wait;
            PLOGD(log_, "  get_header: IN-FLIGHT seq=", ledger_seq);
        }
        else
        {
            // Create signal with max expiry — real timeout is set after
            // peer acquisition and dispatch (see Action::fetch below).
            it->second.signal = std::make_shared<asio::steady_timer>(
                io_, asio::steady_timer::time_point::max());
            it->second.present = false;
            action = Action::fetch;
            evict_headers_if_needed();
            PLOGD(log_, "  get_header: MISS seq=", ledger_seq, " — fetching");
        }
    }  // mutex released

    if (action == Action::hit)
        co_return hit_result;

    auto abandon_header_fetch =
        [&](std::shared_ptr<asio::steady_timer> const& expected_signal) {
            std::shared_ptr<asio::steady_timer> sig;
            {
                std::lock_guard lock(mutex_);
                auto it = header_cache_.find(ledger_seq);
                if (
                    it != header_cache_.end() && !it->second.present &&
                    (!expected_signal || it->second.signal == expected_signal))
                {
                    sig = it->second.signal;
                    header_cache_.erase(it);
                    if (auto lru_it = header_lru_map_.find(ledger_seq);
                        lru_it != header_lru_map_.end())
                    {
                        header_lru_.erase(lru_it->second);
                        header_lru_map_.erase(lru_it);
                    }
                }
            }
            if (sig)
                sig->cancel();
        };

    if (action == Action::wait)
    {
        catl::core::emit_status(
            "[shared] joining in-flight header fetch seq=" +
            std::to_string(ledger_seq));

        WaitRegistration wait;
        {
            std::lock_guard lock(mutex_);
            auto it = header_cache_.find(ledger_seq);
            if (it == header_cache_.end())
                throw PeerClientException(Error::Timeout);
            if (it->second.present)
                co_return it->second.result;
            wait = attach_waiter_locked(it->second);
            wait.round_seen = it->second.round;
        }

        if (!wait.done_signal)
            throw PeerClientException(Error::Timeout);

        for (;;)
        {
            auto [wake_kind, ec] = co_await wait_for_signal_or_progress(
                wait.done_signal, wait.progress_signal);

            std::vector<ProgressEvent> progress_events;
            uint8_t current_round = 0;
            bool retry_pending = false;
            {
                std::lock_guard lock(mutex_);
                auto it = header_cache_.find(ledger_seq);
                if (it == header_cache_.end())
                    throw PeerClientException(Error::Timeout);
                if (it->second.present)
                    co_return it->second.result;

                current_round = it->second.round;
                retry_pending = it->second.retry_pending;
                progress_events = it->second.progress.journal.replay(wait.cursor);
                refresh_waiter_locked(it->second, wait);
            }

            for (auto const& event : progress_events)
                catl::core::emit_status(format_progress(event));

            if (wake_kind == 1)
                continue;

            // Owner is dispatching next round — keep waiting
            if (retry_pending || current_round != wait.round_seen)
            {
                wait.round_seen = current_round;
                continue;
            }

            // Owner exhausted all retry rounds. Drop the stale
            // in-flight slot so the next caller starts fresh.
            abandon_header_fetch(wait.done_signal);
            throw PeerClientException(Error::Timeout);
        }
    }

    // action == Action::fetch
    //
    // Fan-out: send header request to up to 3 peers simultaneously.
    // First response wins via send_header_fetch callback.
    // Then wait on the shared signal.
    std::shared_ptr<asio::steady_timer> owned_signal;
    {
        std::lock_guard lock(mutex_);
        auto it = header_cache_.find(ledger_seq);
        if (it != header_cache_.end())
            owned_signal = it->second.signal;
    }

    struct HeaderFetchCleanup
    {
        decltype(abandon_header_fetch)& abandon;
        std::shared_ptr<asio::steady_timer> owned_signal;
        bool active = true;

        void
        dismiss()
        {
            active = false;
        }

        ~HeaderFetchCleanup()
        {
            if (active)
                abandon(owned_signal);
        }
    } cleanup{abandon_header_fetch, owned_signal};

    static constexpr int kHeaderFanOut = 3;
    static constexpr int kMaxRounds = 3;
    auto header_timeout = std::max(
        fetch_timeout_, std::chrono::milliseconds{6000});
    std::unordered_set<std::string> tried_peers;
    int total_peers_tried = 0;

    // ── Dispatch round ──
    // Returns {peers_sent, peer_names}. Subsequent rounds are
    // dispatched from the wait loop on timeout/all-failed.
    std::vector<std::string> round_peers;

    auto dispatch_round =
        [&](PeerSessionPtr primary,
            std::shared_ptr<std::atomic<int>> outstanding)
            -> asio::awaitable<int> {
        int sent = 0;
        round_peers.clear();

        if (primary && primary->is_ready())
        {
            tried_peers.insert(primary->endpoint());
            round_peers.push_back(primary->endpoint());
            outstanding->fetch_add(1);
            send_header_fetch(ledger_seq, primary, outstanding, peers);
            ++sent;
        }

        if (peers && sent < kHeaderFanOut)
        {
            auto extras = co_await peers->co_select_peers_for(
                ledger_seq, kHeaderFanOut - sent, tried_peers);
            for (auto const& extra : extras)
            {
                if (extra && extra->is_ready())
                {
                    tried_peers.insert(extra->endpoint());
                    round_peers.push_back(extra->endpoint());
                    outstanding->fetch_add(1);
                    send_header_fetch(
                        ledger_seq, extra, outstanding, peers);
                    ++sent;
                }
            }
        }

        // Fallback: wait for a peer only on the first round to keep
        // the total retry budget predictable (~18s max).
        if (sent == 0 && peers && total_peers_tried == 0)
        {
            auto fallback = co_await peers->wait_for_peer(
                ledger_seq, 5, tried_peers);
            if (fallback && fallback->is_ready())
            {
                tried_peers.insert(fallback->endpoint());
                round_peers.push_back(fallback->endpoint());
                outstanding->fetch_add(1);
                send_header_fetch(
                    ledger_seq, fallback, outstanding, peers);
                ++sent;
            }
        }

        co_return sent;
    };

    // Each round gets its own outstanding counter.
    // The signal is shared across all rounds (single timer object).
    auto outstanding = std::make_shared<std::atomic<int>>(0);
    int round_sent = co_await dispatch_round(peer, outstanding);
    total_peers_tried += round_sent;

    if (round_sent == 0)
    {
        catl::core::emit_status(
            "header seq=" + std::to_string(ledger_seq) +
            " — no peers available");
        publish_header_progress(
            ledger_seq,
            ProgressKind::no_peer_available,
            0,
            static_cast<uint8_t>(kHeaderFanOut));
        throw PeerClientException(Error::Timeout);
    }

    // Arm the signal now that requests are dispatched.
    {
        std::lock_guard lock(mutex_);
        auto it = header_cache_.find(ledger_seq);
        if (it != header_cache_.end() && it->second.present)
        {
            cleanup.dismiss();
            co_return it->second.result;
        }
        if (it != header_cache_.end() && it->second.signal &&
            outstanding->load() > 0)
        {
            it->second.signal->expires_after(header_timeout);
        }
    }

    {
        std::string peer_list;
        for (auto const& ep : round_peers)
        {
            if (!peer_list.empty())
                peer_list += ", ";
            peer_list += ep;
        }
        catl::core::emit_status(
            "anchor header: seq=" + std::to_string(ledger_seq) +
            " asking " + std::to_string(round_sent) +
            " peer(s): " + peer_list);
    }

    // ── Wait loop ──
    // Shared across owner and joiners. On round timeout, the owner
    // dispatches a new round and re-arms the same timer. Joiners
    // detect the round change via generation counter and keep waiting.
    WaitRegistration wait;
    {
        std::lock_guard lock(mutex_);
        auto it = header_cache_.find(ledger_seq);
        if (it == header_cache_.end())
        {
            throw PeerClientException(Error::Timeout);
        }
        if (it->second.present)
        {
            cleanup.dismiss();
            co_return it->second.result;
        }
        if (outstanding->load() == 0)
        {
            // All peers failed before we could attach — will retry
            // in the loop below via round advancement.
        }
        else
        {
            // Note: narrow race between outstanding check and
            // async_wait — safe because Asio's async_wait on an
            // already-cancelled timer returns operation_aborted.
        }
        wait = attach_waiter_locked(it->second);
        wait.round_seen = it->second.round;
    }

    if (!wait.done_signal)
    {
        throw PeerClientException(Error::Timeout);
    }

    for (;;)
    {
        auto [wake_kind, ec] = co_await wait_for_signal_or_progress(
            wait.done_signal, wait.progress_signal);

        std::vector<ProgressEvent> progress_events;
        uint8_t current_round = 0;
        bool is_present = false;
        {
            std::lock_guard lock(mutex_);
            auto it = header_cache_.find(ledger_seq);
            if (it == header_cache_.end())
            {
                throw PeerClientException(Error::Timeout);
            }
            if (it->second.present)
            {
                cleanup.dismiss();
                co_return it->second.result;
            }

            current_round = it->second.round;
            progress_events =
                it->second.progress.journal.replay(wait.cursor);
            refresh_waiter_locked(it->second, wait);
        }

        for (auto const& event : progress_events)
        {
            catl::core::emit_status(format_progress(event));
        }

        if (wake_kind == 1)
            continue;

        // Round changed by another path — keep waiting
        if (current_round != wait.round_seen)
        {
            wait.round_seen = current_round;
            continue;
        }

        // This round timed out / all peers failed.
        // Try dispatching a new round with fresh peers.
        // Increment round BEFORE the co_await so joiners waking
        // during dispatch see the new generation and keep waiting.
        if (current_round < kMaxRounds - 1)
        {
            {
                std::lock_guard lock(mutex_);
                auto it = header_cache_.find(ledger_seq);
                if (it != header_cache_.end() && !it->second.present)
                {
                    it->second.round++;
                    it->second.retry_pending = true;
                    // Re-arm timer before dispatch so joiners waking
                    // during the co_await see a live timer, not the
                    // expired one from the previous round.
                    if (it->second.signal)
                    {
                        it->second.signal->expires_after(header_timeout);
                    }
                }
            }

            outstanding = std::make_shared<std::atomic<int>>(0);
            round_sent = co_await dispatch_round(nullptr, outstanding);
            total_peers_tried += round_sent;

            std::lock_guard lock(mutex_);
            {
                auto it2 = header_cache_.find(ledger_seq);
                if (it2 != header_cache_.end())
                {
                    it2->second.retry_pending = false;
                }
            }
            auto it = header_cache_.find(ledger_seq);
            if (it == header_cache_.end())
            {
                throw PeerClientException(Error::Timeout);
            }
            if (it->second.present)
            {
                cleanup.dismiss();
                co_return it->second.result;
            }

            wait.round_seen = it->second.round;
            if (round_sent > 0)
            {
                if (outstanding->load() > 0)
                {
                    it->second.signal->cancel();
                    it->second.signal->expires_after(header_timeout);
                }
                catl::core::emit_status(
                    "header seq=" + std::to_string(ledger_seq) +
                    " — retry round " +
                    std::to_string(it->second.round + 1) + "/" +
                    std::to_string(kMaxRounds) + " asking " +
                    std::to_string(round_sent) + " peer(s): " +
                    [&]() {
                        std::string s;
                        for (auto const& ep : round_peers)
                        {
                            if (!s.empty())
                                s += ", ";
                            s += ep;
                        }
                        return s;
                    }());
                continue;
            }
            // No new peers found — fall through to give up
        }

        catl::core::emit_status(
            "header seq=" + std::to_string(ledger_seq) +
            " — giving up after " +
            std::to_string(total_peers_tried) + " peers across " +
            std::to_string(current_round + 1) + " round(s)");
        publish_header_progress(
            ledger_seq,
            ProgressKind::give_up,
            static_cast<uint8_t>(total_peers_tried),
            static_cast<uint8_t>(kHeaderFanOut * kMaxRounds));
        throw PeerClientException(Error::Timeout);
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
NodeCache::clear()
{
    std::lock_guard lock(mutex_);
    store_.clear();
    header_cache_.clear();
    lru_.clear();
    lru_map_.clear();
    header_lru_.clear();
    header_lru_map_.clear();
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
        .header_entries = header_cache_.size(),
        .max_entries = max_entries_,
        .hits = hits_,
        .misses = misses_,
        .fetches = fetches_,
        .fetch_errors = fetch_errors_,
        .hash_mismatches = hash_mismatches_,
        .waiter_wakeups = waiter_wakeups_,
    };
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
            if (!it->second.present && it->second.signal)
            {
                header_lru_.splice(
                    header_lru_.begin(),
                    header_lru_,
                    std::prev(header_lru_.end()));
                continue;
            }
            PLOGD(log_, "  evict header: seq=", victim);
            header_cache_.erase(it);
        }
        header_lru_map_.erase(victim);
        header_lru_.pop_back();
    }
}

}  // namespace catl::peer_client
