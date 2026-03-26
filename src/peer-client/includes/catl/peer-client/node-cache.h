#pragma once

// NodeCache — content-addressed in-memory cache for SHAMap wire nodes.
//
// Stores raw wire bytes keyed by node hash. Provides walk_to() which
// walks a SHAMap from root hash to target key, fetching on cache miss
// via PeerClient. Cross-ledger structural sharing is automatic: two
// ledgers that share an inner node (same hash) share the cache entry.
//
// Usage:
//   auto cache = NodeCache::create(io);
//   auto walk = co_await cache->walk_to(
//       root_hash, target_key, ledger_hash, liTX_NODE, peers);
//   // walk.found, walk.leaf_nid, walk.leaf_data, walk.placeholders

#include "peer-client-coro.h"
#include "peer-set.h"
#include "wire-node-view.h"

#include <catl/core/logger.h>
#include <catl/core/types.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

namespace catl::peer_client {

namespace asio = boost::asio;

// ─────────────────────────────────────────────────────────────────────
// Result types
// ─────────────────────────────────────────────────────────────────────

/// A node on the walk path with its tree position.
struct PathNode
{
    SHAMapNodeID nodeid;  // position in tree
    Hash256 hash;         // content hash
    std::shared_ptr<std::vector<uint8_t>>
        wire;  // shared with cache — zero-copy
};

/// Sibling branch at a depth — becomes a placeholder in the abbreviated tree.
struct PlaceholderNode
{
    SHAMapNodeID nodeid;
    Hash256 hash;
};

/// Result of walk_to: the full path from root to leaf (or deepest resolved).
struct WalkResult
{
    bool found = false;

    /// Inner nodes along the path from root to leaf, in depth order.
    std::vector<PathNode> path;

    /// Sibling hashes at each depth — branches NOT on our target path.
    /// These become placeholders in the abbreviated tree.
    std::vector<PlaceholderNode> placeholders;

    /// The leaf position and data (only valid when found=true).
    SHAMapNodeID leaf_nid;
    std::vector<uint8_t> leaf_data;
};

// ─────────────────────────────────────────────────────────────────────
// NodeCache
// ─────────────────────────────────────────────────────────────────────

class NodeCache : public std::enable_shared_from_this<NodeCache>
{
public:
    struct Options
    {
        size_t max_entries = 65536;
        int fetch_timeout_secs = 5;
        int max_walk_peer_retries = 3;
        int fetch_stale_multiplier = 2;
    };

    static std::shared_ptr<NodeCache>
    create(asio::io_context& io, Options opts);

    /// Walk a SHAMap from root hash to target key, fetching on miss.
    ///
    /// @param root_hash   Root of the tree (account_hash or tx_hash from header)
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
        std::shared_ptr<PeerClient> peer = nullptr,
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
    std::shared_ptr<std::vector<uint8_t>>
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
        size_t max_entries = 0;
        size_t hits = 0;
        size_t misses = 0;
        size_t fetches = 0;
        size_t fetch_errors = 0;
        size_t hash_mismatches = 0;
        size_t waiter_wakeups = 0;
    };
    Stats
    stats() const;

    /// Fetch a ledger header, with cache + in-flight dedup.
    /// First requester fetches via co_get_ledger_header, others wait.
    asio::awaitable<LedgerHeaderResult>
    get_header(
        uint32_t ledger_seq,
        std::shared_ptr<PeerSet> peers,
        std::shared_ptr<PeerClient> peer = nullptr);

    /// Number of present entries in the cache.
    size_t
    size() const;

    /// Total bytes of wire data stored.
    size_t
    bytes() const;

    /// Clear all entries.
    void
    clear();

private:
    NodeCache(asio::io_context& io, Options opts);

    // ── Cache entry ─────────────────────────────────────────────
    //
    // Single shared signal per entry. When data arrives via
    // insert_and_notify, the signal is cancelled — all waiters wake.
    //
    // Key invariant: entries are NEVER erased on timeout/error.
    // A timed-out caller returns nullptr, but the entry stays alive
    // so that late-arriving peer responses still populate it.
    // Next caller detects stale via last_fetch_at, re-sends, and
    // creates a fresh signal.
    //
    // Entries are only removed by LRU eviction (which skips
    // in-flight entries with non-null signal).
    struct Entry
    {
        std::shared_ptr<std::vector<uint8_t>> wire_data;
        std::shared_ptr<asio::steady_timer> signal;  // non-null while in-flight
        bool present = false;

        // When the most recent TMGetLedger was sent for this hash.
        // Used for stale detection: if now - last_fetch_at >
        // fetch_timeout * stale_multiplier, a new caller will
        // re-send the request (original sender likely timed out).
        std::chrono::steady_clock::time_point last_fetch_at{};
    };

    /// Ensure a node is in the cache. Fetches from peer on miss.
    /// Peer must be pre-acquired by walk_to — no strand hop here.
    /// Returns shared wire data, or nullptr on timeout/error.
    asio::awaitable<std::shared_ptr<std::vector<uint8_t>>>
    ensure_present(
        Hash256 expected_hash,
        Hash256 ledger_hash,
        int tree_type,
        SHAMapNodeID position,
        Hash256 const& target_key,
        int speculative_depth,
        std::shared_ptr<PeerClient> peer);

    /// Send a TMGetLedger request for a node (+ speculative deeper nodes).
    /// Non-coroutine: just builds and sends the request, no waiting.
    /// The response arrives asynchronously via on_node_response →
    /// insert_and_notify which wakes all waiters.
    void
    send_fetch(
        Hash256 expected_hash,
        Hash256 ledger_hash,
        int tree_type,
        SHAMapNodeID position,
        Hash256 const& target_key,
        int speculative_depth,
        std::shared_ptr<PeerClient> peer);

    /// Compute the content hash of an inner wire node.
    /// Expands compressed format to canonical 16×32 layout, then
    /// SHA512Half(MINprefix + 16 child hashes).
    static Hash256
    compute_inner_hash(std::span<const uint8_t> wire_data);

    /// Install our response handler on a peer (idempotent per peer).
    void
    ensure_response_handler(std::shared_ptr<PeerClient> peer);

    /// Called by the response handler for every TMLedgerData node response.
    /// Computes content hash for each node, inserts into cache, wakes waiters.
    void
    on_node_response(std::shared_ptr<protocol::TMLedgerData> const& msg);

    /// Track which peers have our handler installed.
    std::set<std::string> handler_installed_peers_;

    /// Evict LRU entries when over capacity.
    void
    evict_if_needed();

    asio::io_context& io_;
    size_t max_entries_;
    int fetch_timeout_secs_;
    int max_walk_peer_retries_;
    int fetch_stale_multiplier_;

    mutable std::mutex mutex_;
    std::map<Hash256, Entry> store_;

    // Ledger header cache: keyed by seq
    struct HeaderEntry
    {
        LedgerHeaderResult result;
        std::shared_ptr<asio::steady_timer> signal;  // non-null while in-flight
        bool present = false;
    };
    std::map<uint32_t, HeaderEntry> header_cache_;

    // LRU tracking: front = most recently accessed (mutable for const get())
    using LruList = std::list<Hash256>;
    mutable LruList lru_;
    mutable std::map<Hash256, LruList::iterator> lru_map_;

    void
    touch_lru(Hash256 const& hash) const;

    // Stats — atomic for lock-free concurrent access
    mutable std::atomic<size_t> hits_{0};
    mutable std::atomic<size_t> misses_{0};
    std::atomic<size_t> fetches_{0};
    std::atomic<size_t> fetch_errors_{0};
    std::atomic<size_t> hash_mismatches_{0};
    std::atomic<size_t> waiter_wakeups_{0};

    static LogPartition log_;
};

}  // namespace catl::peer_client
