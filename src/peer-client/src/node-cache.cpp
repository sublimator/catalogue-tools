#include <catl/peer-client/node-cache.h>

#include "ripple.pb.h"

#include <catl/core/log-macros.h>
#include <catl/crypto/sha512-half-hasher.h>

#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>  // for reset_cancellation_state
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <cstring>
#include <unordered_set>

namespace catl::peer_client {

LogPartition NodeCache::log_("node-cache", LogLevel::INHERIT);

// ═══════════════════════════════════════════════════════════════════════
// Lifecycle
// ═══════════════════════════════════════════════════════════════════════

NodeCache::NodeCache(asio::io_context& io, Options opts)
    : io_(io)
    , max_entries_(opts.max_entries)
    , fetch_timeout_secs_(opts.fetch_timeout_secs)
    , max_walk_peer_retries_(opts.max_walk_peer_retries)
    , fetch_stale_multiplier_(opts.fetch_stale_multiplier)
{
}

std::shared_ptr<NodeCache>
NodeCache::create(asio::io_context& io, Options opts)
{
    return std::shared_ptr<NodeCache>(new NodeCache(io, std::move(opts)));
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
    std::shared_ptr<PeerClient> peer,
    std::shared_ptr<std::atomic<bool>> cancel)
{
    // ── CANCELLATION BOUNDARY ──
    // Shield all inner co_awaits (ensure_present, fetch_node, signal timers)
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
    // different peer (max 2 retries per walk).
    auto peer_covers = [](std::shared_ptr<PeerClient> const& p, uint32_t seq) {
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
            peer = co_await peers->wait_for_peer(ledger_seq, 10);
        }
        if (!peer || !peer->is_ready())
        {
            PLOGW(log_, "walk_to: no peer available for ledger ", ledger_seq);
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
            cursor, ledger_hash, tree_type, pos, target_key, spec_depth, peer);

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
            // in the cache as in-flight, and ensure_present will
            // re-send via the stale detection path.
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
                --depth;  // retry this depth
                continue;
            }
            // Subsequent retries: different peer
            else if (peers)
            {
                same_peer_retried = false;
                if (peer)
                    failed_peers.insert(peer->endpoint());

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
                auto new_peer =
                    co_await peers->wait_for_peer(ledger_seq, 10, failed_peers);
                if (new_peer && new_peer->is_ready())
                {
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
// ensure_present — per-caller waiter model
//
// Each caller creates their own asio::steady_timer with their own
// deadline. The cache entry holds weak_ptr<timer> to each waiter.
// When data arrives (insert_and_notify), all waiter timers are
// cancelled. When a caller times out, only THAT caller gives up —
// the entry stays alive so late-arriving peer responses still get
// cached. This fixes the "priming then instant" problem.
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<std::shared_ptr<std::vector<uint8_t>>>
NodeCache::ensure_present(
    Hash256 expected_hash,
    Hash256 ledger_hash,
    int tree_type,
    SHAMapNodeID position,
    Hash256 const& target_key,
    int speculative_depth,
    std::shared_ptr<PeerClient> peer)
{
    enum class Action { hit, wait, send_and_wait };
    Action action;
    std::shared_ptr<std::vector<uint8_t>> hit_data;

    // Each caller gets their own deadline timer. When data arrives,
    // insert_and_notify cancels it (async_wait returns operation_aborted).
    // When it expires naturally, the caller timed out.
    auto my_timer = std::make_shared<asio::steady_timer>(
        io_, std::chrono::seconds(fetch_timeout_secs_));

    {
        std::lock_guard lock(mutex_);
        auto [it, inserted] = store_.try_emplace(expected_hash);

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
            // Register our timer as a waiter. Prune expired ones lazily.
            std::erase_if(
                it->second.waiters, [](auto& wp) { return wp.expired(); });
            it->second.waiters.push_back(my_timer);

            // Stale detection: if the last fetch was sent more than
            // N*timeout ago, the original sender likely timed out and
            // the peer probably dropped the request. Re-send.
            auto now = std::chrono::steady_clock::now();
            auto stale_threshold = std::chrono::seconds(
                fetch_timeout_secs_ * fetch_stale_multiplier_);
            if (now - it->second.last_fetch_at > stale_threshold && peer &&
                peer->is_ready())
            {
                action = Action::send_and_wait;
                PLOGD(
                    log_,
                    "  ensure: STALE in-flight hash=",
                    expected_hash.hex().substr(0, 16),
                    " — re-sending");
            }
            else
            {
                action = Action::wait;
                PLOGD(
                    log_,
                    "  ensure: IN-FLIGHT hash=",
                    expected_hash.hex().substr(0, 16),
                    " — joining ",
                    it->second.waiters.size(),
                    " waiters");
            }
        }
        else
        {
            // MISS — new entry, we'll send the first fetch
            misses_++;
            it->second.waiters.push_back(my_timer);
            action = Action::send_and_wait;
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
            // Don't erase the entry — other waiters may have a peer.
            // Our timer will expire naturally and we'll return nullptr.
        }
        else
        {
            send_fetch(
                expected_hash,
                ledger_hash,
                tree_type,
                position,
                target_key,
                speculative_depth,
                peer);
        }
    }

    // ── Wait on our personal timer ───────────────────────────────
    //
    // Two outcomes:
    //   operation_aborted → insert_and_notify cancelled our timer → data
    //   arrived no error → timer expired naturally → we timed out
    //
    // In BOTH cases, we check the store. The data might have arrived
    // between our timer firing and us acquiring the lock.
    boost::system::error_code ec;
    co_await my_timer->async_wait(
        asio::redirect_error(asio::use_awaitable, ec));

    {
        std::lock_guard lock(mutex_);
        auto it = store_.find(expected_hash);
        if (it != store_.end() && it->second.present)
        {
            if (ec == asio::error::operation_aborted)
                waiter_wakeups_++;
            touch_lru(expected_hash);
            co_return it->second.wire_data;
        }
    }

    // Timed out and data still not present. Return nullptr but
    // CRUCIALLY do NOT erase the entry. The peer response may still
    // arrive and populate it via insert_and_notify — benefiting other
    // waiters and future requests. The entry will be cleaned up by
    // LRU eviction when it has no active waiters.
    if (ec != asio::error::operation_aborted)
    {
        PLOGD(
            log_, "  ensure: TIMEOUT hash=", expected_hash.hex().substr(0, 16));
        fetch_errors_++;
    }
    co_return nullptr;
}

// ═══════════════════════════════════════════════════════════════════════
// send_fetch — fire-and-forget TMGetLedger request
//
// Sends the request to the peer and records last_fetch_at. Does NOT
// wait for a response — that's handled by each caller's personal
// timer in ensure_present. The response arrives asynchronously via
// on_node_response → insert_and_notify → waiter timers cancelled.
// ═══════════════════════════════════════════════════════════════════════

void
NodeCache::send_fetch(
    Hash256 expected_hash,
    Hash256 ledger_hash,
    int tree_type,
    SHAMapNodeID position,
    Hash256 const& target_key,
    int speculative_depth,
    std::shared_ptr<PeerClient> peer)
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
    peer->send_get_nodes(ledger_hash, tree_type, node_ids);

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
NodeCache::ensure_response_handler(std::shared_ptr<PeerClient> peer)
{
    auto ep = peer->endpoint();
    {
        std::lock_guard lock(mutex_);
        if (handler_installed_peers_.count(ep))
            return;  // already installed
        handler_installed_peers_.insert(ep);
    }

    auto self = shared_from_this();
    peer->set_node_response_handler(
        [self](std::shared_ptr<protocol::TMLedgerData> const& msg) {
            self->on_node_response(msg);
        });

    PLOGD(log_, "  installed response handler on peer ", ep);
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
        it->second.waiters.clear();
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
    // Collect live waiter timers to cancel outside the lock.
    // Each waiter's async_wait will return operation_aborted,
    // prompting them to re-check the store and find the data.
    std::vector<std::shared_ptr<asio::steady_timer>> to_cancel;

    {
        std::lock_guard lock(mutex_);
        auto [it, inserted] = store_.try_emplace(hash);

        if (!inserted && it->second.present)
            return;  // already populated — skip

        // Move out the waiter list before populating
        auto pending = std::move(it->second.waiters);
        it->second.waiters.clear();

        // Populate the entry
        it->second.wire_data =
            std::make_shared<std::vector<uint8_t>>(std::move(data));
        it->second.present = true;
        touch_lru(hash);
        evict_if_needed();

        // Collect live timers (weak_ptr → shared_ptr)
        for (auto& wp : pending)
        {
            if (auto sp = wp.lock())
                to_cancel.push_back(sp);
        }

        PLOGD(
            log_,
            "  insert+notify: hash=",
            hash.hex().substr(0, 16),
            " waiters=",
            to_cancel.size(),
            " store_size=",
            store_.size());
    }

    // Wake all waiters outside the lock
    for (auto& timer : to_cancel)
    {
        waiter_wakeups_++;
        timer->cancel();
    }
}

asio::awaitable<LedgerHeaderResult>
NodeCache::get_header(
    uint32_t ledger_seq,
    std::shared_ptr<PeerSet> peers,
    std::shared_ptr<PeerClient> peer)
{
    // Single lock: check present → check in-flight → create in-flight.
    enum class Action { hit, wait, fetch };
    Action action;
    std::shared_ptr<asio::steady_timer> signal;
    LedgerHeaderResult hit_result;

    {
        std::lock_guard lock(mutex_);
        auto [it, inserted] = header_cache_.try_emplace(ledger_seq);

        if (!inserted && it->second.present)
        {
            action = Action::hit;
            hit_result = it->second.result;
            PLOGD(log_, "  get_header: HIT seq=", ledger_seq);
        }
        else if (!inserted && it->second.signal)
        {
            action = Action::wait;
            signal = it->second.signal;
            PLOGD(log_, "  get_header: IN-FLIGHT seq=", ledger_seq);
        }
        else
        {
            it->second.signal = std::make_shared<asio::steady_timer>(
                io_, std::chrono::seconds(fetch_timeout_secs_));
            it->second.present = false;
            action = Action::fetch;
            PLOGD(log_, "  get_header: MISS seq=", ledger_seq, " — fetching");
        }
    }  // mutex released

    if (action == Action::hit)
        co_return hit_result;

    if (action == Action::wait)
    {
        boost::system::error_code ec;
        co_await signal->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));

        std::lock_guard lock(mutex_);
        auto it = header_cache_.find(ledger_seq);
        if (it != header_cache_.end() && it->second.present)
            co_return it->second.result;

        throw PeerClientException(Error::Timeout);
    }

    // action == Action::fetch

    // Use the provided peer if available. Otherwise find one via
    // wait_for_peer (awaitable — hops to PeerSet strand internally,
    // safe to call from any executor).
    if (!peer || !peer->is_ready())
    {
        if (peers)
            peer = co_await peers->wait_for_peer(ledger_seq, 10);
    }

    if (!peer || !peer->is_ready())
    {
        PLOGW(log_, "  get_header: no peer for seq=", ledger_seq);
        // Don't erase — next caller will see non-present entry with
        // no signal (treated as a MISS) and retry the fetch.
        std::shared_ptr<asio::steady_timer> sig;
        {
            std::lock_guard lock(mutex_);
            auto it = header_cache_.find(ledger_seq);
            if (it != header_cache_.end())
            {
                sig = it->second.signal;
                it->second.signal = nullptr;
            }
        }
        if (sig)
            sig->cancel();
        throw PeerClientException(Error::Timeout);
    }

    try
    {
        auto result = co_await co_get_ledger_header(peer, ledger_seq);

        // Populate cache and wake waiters
        std::shared_ptr<asio::steady_timer> signal;
        {
            std::lock_guard lock(mutex_);
            auto& entry = header_cache_[ledger_seq];
            signal = entry.signal;
            entry.result = result;
            entry.present = true;
            entry.signal = nullptr;
        }

        if (signal)
            signal->cancel();

        PLOGD(
            log_,
            "  get_header: fetched seq=",
            ledger_seq,
            " hash=",
            result.ledger_hash256().hex().substr(0, 16));
        co_return result;
    }
    catch (...)
    {
        // Don't erase — clear signal so next caller retries the fetch.
        // Preserves the entry for in-flight waiters to discover failure.
        std::shared_ptr<asio::steady_timer> signal;
        {
            std::lock_guard lock(mutex_);
            auto it = header_cache_.find(ledger_seq);
            if (it != header_cache_.end())
            {
                signal = it->second.signal;
                it->second.signal = nullptr;
            }
        }
        if (signal)
            signal->cancel();
        throw;
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
    lru_.clear();
    lru_map_.clear();
}

NodeCache::Stats
NodeCache::stats() const
{
    std::lock_guard lock(mutex_);
    size_t entries = 0;
    for (auto const& [_, e] : store_)
    {
        if (e.present)
            entries++;
    }
    return {
        .entries = entries,
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
            // Don't evict in-flight entries with active waiters —
            // someone is still waiting for this data to arrive.
            if (!it->second.present)
            {
                bool has_active = false;
                for (auto& wp : it->second.waiters)
                {
                    if (!wp.expired())
                    {
                        has_active = true;
                        break;
                    }
                }
                if (has_active)
                {
                    lru_.splice(lru_.begin(), lru_, std::prev(lru_.end()));
                    continue;
                }
            }
            PLOGD(log_, "  evict: hash=", victim.hex().substr(0, 16));
            store_.erase(it);
        }
        lru_map_.erase(victim);
        lru_.pop_back();
    }
}

}  // namespace catl::peer_client
