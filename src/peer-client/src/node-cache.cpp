#include <catl/peer-client/node-cache.h>

#include "ripple.pb.h"

#include <catl/core/log-macros.h>
#include <catl/crypto/sha512-half-hasher.h>

#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <cstring>

namespace catl::peer_client {

LogPartition NodeCache::log_("node-cache", LogLevel::INHERIT);

// ═══════════════════════════════════════════════════════════════════════
// Lifecycle
// ═══════════════════════════════════════════════════════════════════════

NodeCache::NodeCache(asio::io_context& io, size_t max_entries)
    : io_(io), max_entries_(max_entries)
{
}

std::shared_ptr<NodeCache>
NodeCache::create(asio::io_context& io, size_t max_entries)
{
    return std::shared_ptr<NodeCache>(new NodeCache(io, max_entries));
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
    std::shared_ptr<PeerClient> peer)
{
    WalkResult result;
    Hash256 cursor = root_hash;
    auto pos = SHAMapNodeID::root();

    // Speculative depth: request multiple depths in one TMGetLedger.
    // Halves each round: 8 → 4 → 2 → 1 (same strategy as old TreeWalker).
    int spec_depth = (tree_type == 2) ? 8 : 4;  // state=8, tx=4

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
        spec_depth);

    for (int depth = 0; depth < 64; ++depth)
    {
        auto wire = co_await ensure_present(
            cursor,
            ledger_hash,
            ledger_seq,
            tree_type,
            pos,
            target_key,
            spec_depth,
            peers,
            peer);

        // Halve speculation after each fetch round
        if (spec_depth > 1)
            spec_depth = std::max(1, spec_depth / 2);

        if (!wire)
        {
            PLOGW(
                log_,
                "  depth=",
                depth,
                " MISS hash=",
                cursor.hex().substr(0, 16),
                " — timeout/error");
            co_return result;  // found=false, caller retries
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
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<std::shared_ptr<std::vector<uint8_t>>>
NodeCache::ensure_present(
    Hash256 expected_hash,
    Hash256 ledger_hash,
    uint32_t ledger_seq,
    int tree_type,
    SHAMapNodeID position,
    Hash256 const& target_key,
    int speculative_depth,
    std::shared_ptr<PeerSet> peers,
    std::shared_ptr<PeerClient> peer)
{
    // Single lock: check present → check in-flight → create in-flight.
    // Eliminates TOCTOU race where two coroutines both create in-flight.
    enum class Action { hit, wait, fetch };
    Action action;
    std::shared_ptr<asio::steady_timer> signal;
    std::shared_ptr<std::vector<uint8_t>> hit_data;

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
        else if (!inserted && it->second.signal)
        {
            // IN-FLIGHT — someone else is fetching, wait on their signal
            signal = it->second.signal;
            action = Action::wait;
            PLOGD(
                log_,
                "  ensure: IN-FLIGHT hash=",
                expected_hash.hex().substr(0, 16),
                " — waiting on signal");
        }
        else
        {
            // MISS — we create the in-flight entry (inserted or stale)
            misses_++;
            it->second.signal = std::make_shared<asio::steady_timer>(
                io_, std::chrono::seconds(30));
            it->second.present = false;
            action = Action::fetch;
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

    if (action == Action::wait)
    {
        boost::system::error_code ec;
        co_await signal->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));

        // Re-check after wake
        std::lock_guard lock(mutex_);
        auto it2 = store_.find(expected_hash);
        if (it2 != store_.end() && it2->second.present)
        {
            waiter_wakeups_++;
            co_return it2->second.wire_data;
        }
        PLOGD(
            log_,
            "  ensure: woke but entry gone/not-present hash=",
            expected_hash.hex().substr(0, 16));
        co_return nullptr;
    }

    // action == Action::fetch
    // Smart peer selection: use provided peer if it covers the ledger,
    // otherwise find one via wait_for_peer (awaitable, hops to strand).
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
                "  ensure: peer ",
                (peer ? peer->endpoint() : "<none>"),
                " doesn't cover ledger ",
                ledger_seq,
                " — waiting for one");
            // wait_for_peer is awaitable and hops to the PeerSet strand
            peer = co_await peers->wait_for_peer(ledger_seq, 10);
        }
    }

    if (!peer || !peer->is_ready())
    {
        PLOGW(
            log_,
            "  ensure: no peer covering ledger ",
            ledger_seq,
            " for hash=",
            expected_hash.hex().substr(0, 16));
        // Clean up in-flight entry
        {
            std::lock_guard lock(mutex_);
            store_.erase(expected_hash);
        }
        fetch_errors_++;
        co_return nullptr;
    }

    // Fetch — includes speculative deeper nodes along the key path
    bool ok = co_await fetch_node(
        expected_hash,
        ledger_hash,
        tree_type,
        position,
        target_key,
        speculative_depth,
        peer);

    if (ok)
    {
        std::lock_guard lock(mutex_);
        auto it = store_.find(expected_hash);
        if (it != store_.end() && it->second.present)
        {
            co_return it->second.wire_data;
        }
    }

    // Fetch failed — clean up
    {
        std::lock_guard lock(mutex_);
        store_.erase(expected_hash);
    }
    co_return nullptr;
}

// ═══════════════════════════════════════════════════════════════════════
// fetch_node — send raw TMGetLedger, wait on cache signal
//
// Bypasses PeerClient's pending_nodes dispatch entirely. The response
// arrives via dispatch_ledger_data → node_response_handler → our
// on_node_response callback → insert_and_notify → signal fires.
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<bool>
NodeCache::fetch_node(
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

            // Compute the nibble from target_key at current depth
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
        "  fetch: hash=",
        expected_hash.hex().substr(0, 16),
        " depth=",
        static_cast<int>(position.depth),
        " nodeids=",
        node_ids.size(),
        " peer=",
        peer->endpoint());

    // Send raw TMGetLedger — no pending_nodes registration
    peer->send_get_nodes(ledger_hash, tree_type, node_ids);

    // Wait on the cache entry's signal timer. The response handler
    // will compute the content hash and call insert_and_notify,
    // which cancels this timer.
    std::shared_ptr<asio::steady_timer> signal;
    {
        std::lock_guard lock(mutex_);
        auto it = store_.find(expected_hash);
        if (it != store_.end())
        {
            if (it->second.present)
                co_return true;  // already populated (race won)
            signal = it->second.signal;
        }
    }

    if (!signal)
    {
        PLOGW(
            log_,
            "  fetch: no signal for hash=",
            expected_hash.hex().substr(0, 16));
        co_return false;
    }

    boost::system::error_code ec;
    co_await signal->async_wait(asio::redirect_error(asio::use_awaitable, ec));

    // Check if the entry was populated
    {
        std::lock_guard lock(mutex_);
        auto it = store_.find(expected_hash);
        if (it != store_.end() && it->second.present)
        {
            co_return true;
        }
    }

    PLOGD(
        log_,
        "  fetch: signal fired but entry not present for hash=",
        expected_hash.hex().substr(0, 16));
    co_return false;
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
        it->second.signal = nullptr;
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
        {
            // Already present — skip
            return;
        }

        signal = it->second.signal;  // may be null if no waiters
        it->second.wire_data =
            std::make_shared<std::vector<uint8_t>>(std::move(data));
        it->second.present = true;
        it->second.signal = nullptr;
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

asio::awaitable<LedgerHeaderResult>
NodeCache::get_header(
    uint32_t ledger_seq,
    std::shared_ptr<PeerSet> peers,
    std::shared_ptr<PeerClient> peer)
{
    // Fast path: already cached
    {
        std::lock_guard lock(mutex_);
        auto it = header_cache_.find(ledger_seq);
        if (it != header_cache_.end() && it->second.present)
        {
            PLOGD(log_, "  get_header: HIT seq=", ledger_seq);
            co_return it->second.result;
        }
    }

    // Check if in-flight — wait on signal
    {
        std::shared_ptr<asio::steady_timer> signal;
        {
            std::lock_guard lock(mutex_);
            auto it = header_cache_.find(ledger_seq);
            if (it != header_cache_.end() && !it->second.present &&
                it->second.signal)
            {
                PLOGD(log_, "  get_header: IN-FLIGHT seq=", ledger_seq);
                signal = it->second.signal;
            }
        }

        if (signal)
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
    }

    // Miss — create in-flight entry and fetch
    {
        std::lock_guard lock(mutex_);
        auto& entry = header_cache_[ledger_seq];
        entry.signal =
            std::make_shared<asio::steady_timer>(io_, std::chrono::seconds(30));
        entry.present = false;
    }

    PLOGD(log_, "  get_header: MISS seq=", ledger_seq, " — fetching");

    // Use the provided peer. If none, try any ready peer (don't wait —
    // the caller's failover loop handles peer acquisition).
    if (!peer || !peer->is_ready())
    {
        if (peers)
            peer = peers->any_peer();
    }

    if (!peer || !peer->is_ready())
    {
        PLOGW(log_, "  get_header: no peer for seq=", ledger_seq);
        std::lock_guard lock(mutex_);
        header_cache_.erase(ledger_seq);
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
        // Clean up and rethrow
        std::shared_ptr<asio::steady_timer> signal;
        {
            std::lock_guard lock(mutex_);
            auto it = header_cache_.find(ledger_seq);
            if (it != header_cache_.end())
            {
                signal = it->second.signal;
                header_cache_.erase(it);
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

std::vector<uint8_t> const*
NodeCache::get(Hash256 const& hash) const
{
    std::lock_guard lock(mutex_);
    auto it = store_.find(hash);
    if (it != store_.end() && it->second.present)
    {
        hits_++;
        touch_lru(hash);
        return it->second.wire_data.get();
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
            // Don't evict in-flight entries
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

}  // namespace catl::peer_client
