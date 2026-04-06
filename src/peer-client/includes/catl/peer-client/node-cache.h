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
//   // walk.found, walk.leaf_nid, walk.leaf_data, walk.placeholders

#include "peer-client-coro.h"
#include "peer-set.h"
#include "wire-node-view.h"

#include <catl/core/logger.h>
#include <catl/core/types.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

class NodeCacheTestAccess;  // forward decl for test friend

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
    friend class ::NodeCacheTestAccess;
public:
    struct Options
    {
        size_t max_entries = 65536;
        std::chrono::milliseconds fetch_timeout{1500};
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
        size_t resident_entries = 0;
        size_t header_entries = 0;
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
        PeerSessionPtr peer = nullptr);

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

    static constexpr std::size_t kProgressCapacity = 16;
    static constexpr uint16_t kNoDepth = 0xFFFF;

    enum class ProgressKind : uint8_t {
        retry_same_peer,
        switch_peer,
        acquired_peer,
        give_up,
        no_peer_available,
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
        std::shared_ptr<asio::steady_timer> signal;
    };

    struct WaitRegistration
    {
        std::shared_ptr<asio::steady_timer> done_signal;
        std::shared_ptr<asio::steady_timer> progress_signal;
        ProgressCursor cursor;
    };

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
        ProgressState progress;
        bool present = false;

        // When the most recent TMGetLedger was sent for this hash.
        // Used for stale detection: if now - last_fetch_at >
        // fetch_timeout * stale_multiplier, a new caller will
        // re-send the request (original sender likely timed out).
        std::chrono::steady_clock::time_point last_fetch_at{};
    };

    // Ledger header cache: keyed by seq
    struct HeaderEntry
    {
        LedgerHeaderResult result;
        std::shared_ptr<asio::steady_timer> signal;  // non-null while in-flight
        ProgressState progress;
        bool present = false;
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
        PeerSessionPtr peer,
        std::shared_ptr<PeerSet> peers = nullptr,
        uint32_t ledger_seq = 0,
        std::shared_ptr<std::atomic<bool>> cancel = nullptr);

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
        PeerSessionPtr peer);

    /// Compute the content hash of an inner wire node.
    /// Expands compressed format to canonical 16×32 layout, then
    /// SHA512Half(MINprefix + 16 child hashes).
    static Hash256
    compute_inner_hash(std::span<const uint8_t> wire_data);

    /// Install our response handler on a peer.
    /// Safe to call repeatedly; PeerClient just overwrites the handler.
    void
    ensure_response_handler(PeerSessionPtr peer);

    /// Called by the response handler for every TMLedgerData node response.
    /// Computes content hash for each node, inserts into cache, wakes waiters.
    void
    on_node_response(std::shared_ptr<protocol::TMLedgerData> const& msg);

    /// Evict LRU entries when over capacity.
    void
    evict_if_needed();

    void
    evict_headers_if_needed();

    std::shared_ptr<asio::steady_timer>
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

    std::shared_ptr<asio::steady_timer>
    publish_progress_locked(
        Entry& entry,
        ProgressKind kind,
        uint32_t ledger_seq,
        uint16_t depth,
        uint8_t attempt,
        uint8_t max_attempts,
        std::string_view peer = {});

    std::shared_ptr<asio::steady_timer>
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
    size_t max_entries_;
    std::chrono::milliseconds fetch_timeout_;
    int max_walk_peer_retries_;
    int fetch_stale_multiplier_;

    mutable std::mutex mutex_;
    std::map<Hash256, Entry> store_;
    std::map<uint32_t, HeaderEntry> header_cache_;

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
