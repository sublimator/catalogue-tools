#pragma once

// PeerSet — manages multiple peer connections with ledger range awareness.
//
// Wraps EndpointTracker (knows ranges) with live PeerClient connections.
// bootstrap() and try_undiscovered() launch background connection attempts,
// while wait_for_any_peer()/wait_for_peer() wait for usable results.
//
// The intended xprv usage is single-threaded on one io_context. Detached
// connect coroutines keep PeerSet alive by capturing shared_ptr<PeerSet>.

#include "endpoint-tracker.h"
#include "peer-crawl-client.h"
#include "peer-client.h"
#include "peer-endpoint-cache.h"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <chrono>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
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
    boost::asio::awaitable<std::shared_ptr<PeerClient>>
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

    /// Synchronous check: any connected peer with this ledger?
    /// Returns nullptr if none found.
    std::shared_ptr<PeerClient>
    peer_for(uint32_t ledger_seq) const;

    std::shared_ptr<PeerClient>
    peer_for(
        uint32_t ledger_seq,
        std::unordered_set<std::string> const& excluded) const;

    /// Wait until any ready peer is available.
    /// Returns nullptr on timeout.
    boost::asio::awaitable<std::shared_ptr<PeerClient>>
    wait_for_any_peer(int timeout_secs = 15);

    boost::asio::awaitable<std::shared_ptr<PeerClient>>
    wait_for_any_peer(
        int timeout_secs,
        std::unordered_set<std::string> const& excluded);

    /// Wait until a peer with the given ledger is available.
    /// Returns nullptr on timeout.
    boost::asio::awaitable<std::shared_ptr<PeerClient>>
    wait_for_peer(uint32_t ledger_seq, int timeout_secs = 300);

    boost::asio::awaitable<std::shared_ptr<PeerClient>>
    wait_for_peer(
        uint32_t ledger_seq,
        int timeout_secs,
        std::unordered_set<std::string> const& excluded);

    /// Get any ready peer.
    std::shared_ptr<PeerClient>
    any_peer() const;

    std::shared_ptr<PeerClient>
    any_peer(std::unordered_set<std::string> const& excluded) const;

    size_t
    size() const
    {
        return connections_.size();
    }

    /// Awaitable peer count — hops to strand for safe read.
    boost::asio::awaitable<size_t>
    co_size()
    {
        co_await boost::asio::post(strand_, boost::asio::use_awaitable);
        co_return connections_.size();
    }

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
        std::uint64_t selection_count = 0;
        std::uint64_t last_selected_ticket = 0;
    };

    struct Snapshot
    {
        size_t known_endpoints = 0;
        size_t tracked_endpoints = 0;
        size_t connected_peers = 0;
        size_t ready_peers = 0;
        size_t in_flight_connects = 0;
        size_t queued_connects = 0;
        size_t crawl_in_flight = 0;
        size_t queued_crawls = 0;
        std::vector<uint32_t> wanted_ledgers;
        std::vector<SnapshotEntry> peers;
    };

    boost::asio::awaitable<Snapshot>
    co_snapshot();

    void
    set_unsolicited_handler(UnsolicitedHandler handler);

    /// Try to connect to an endpoint. Returns nullptr on failure.
    /// Redirect IPs from 503 are fed into the tracker + tried immediately.
    boost::asio::awaitable<std::shared_ptr<PeerClient>>
    try_connect(std::string const& host, uint16_t port);

    /// Number of live (ready) peer connections (all types).
    size_t
    connected_count() const;

    /// Get up to N peers for a ledger, for fan-out requests.
    /// Synchronous version — must be called on strand.
    std::vector<std::shared_ptr<PeerClient>>
    select_peers_for(
        uint32_t ledger_seq,
        int max_count,
        std::unordered_set<std::string> const& excluded);

    /// Awaitable version — hops to strand internally.
    boost::asio::awaitable<std::vector<std::shared_ptr<PeerClient>>>
    co_select_peers_for(
        uint32_t ledger_seq,
        int max_count,
        std::unordered_set<std::string> excluded);

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
    /// Remove a dead peer from the connection map.
    void
    remove_peer(std::string const& key);

    /// Check if we're at the hub connection cap.
    bool
    at_connection_cap() const;

    /// Check if we're at the archival peer cap.
    bool
    at_archival_cap() const;

    /// Check if a known endpoint is archival (from crawl stats).
    bool
    is_archival_endpoint(std::string const& key) const;

    /// Try to evict an idle peer that doesn't cover the target ledger.
    /// Returns true if a peer was evicted (freeing a slot).
    bool
    evict_for(uint32_t target_ledger_seq);
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

    void
    try_candidates_for(uint32_t ledger_seq);

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
    note_discovered(std::string const& endpoint);

    void
    note_status(std::string const& endpoint, PeerStatus const& status);

    void
    note_connect_success(std::string const& endpoint, PeerStatus const& status);

    void
    note_connect_failure(std::string const& endpoint);

    std::shared_ptr<PeerClient>
    choose_any_peer(std::unordered_set<std::string> const& excluded);

    std::shared_ptr<PeerClient>
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
        std::uint64_t selection_count = 0;
        std::uint64_t last_selected_ticket = 0;

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
    std::map<std::string, std::shared_ptr<PeerClient>> connections_;
    std::map<std::string, std::chrono::steady_clock::time_point> failed_at_;
    std::set<std::string> in_flight_;  // currently connecting
    std::set<std::string> queued_;
    std::vector<std::string> pending_connects_;
    std::set<std::string> crawl_in_flight_;
    std::set<std::string> crawl_queued_;
    std::map<std::string, std::chrono::steady_clock::time_point> crawled_;
    std::vector<std::string> pending_crawls_;
    // Active target ledgers from in-flight wait_for_peer calls.
    // Used by should_connect_endpoint and evict_for to prioritize
    // candidates that cover any active target.
    std::set<uint32_t> wanted_ledgers_;
    mutable std::mutex unsolicited_handler_mutex_;
    UnsolicitedHandler unsolicited_handler_;
};

}  // namespace catl::peer_client
