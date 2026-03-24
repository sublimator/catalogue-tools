#pragma once

// PeerSet — manages multiple peer connections with ledger range awareness.
//
// Wraps EndpointTracker (knows ranges) with live PeerClient connections.
// bootstrap() and try_undiscovered() launch background connection attempts,
// while wait_for_any_peer()/wait_for_peer() wait for usable results.
//
// The intended xproof usage is single-threaded on one io_context. Detached
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
    std::size_t max_connected_peers = 20;  // hard cap on live connections
    std::chrono::seconds retry_backoff{5};
};

class PeerSet : public std::enable_shared_from_this<PeerSet>
{
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
    std::optional<std::shared_ptr<PeerClient>>
    peer_for(uint32_t ledger_seq) const;

    std::optional<std::shared_ptr<PeerClient>>
    peer_for(
        uint32_t ledger_seq,
        std::unordered_set<std::string> const& excluded) const;

    /// Wait until any ready peer is available.
    boost::asio::awaitable<std::optional<std::shared_ptr<PeerClient>>>
    wait_for_any_peer(int timeout_secs = 15);

    boost::asio::awaitable<std::optional<std::shared_ptr<PeerClient>>>
    wait_for_any_peer(
        int timeout_secs,
        std::unordered_set<std::string> const& excluded);

    /// Wait until a peer with the given ledger is available.
    boost::asio::awaitable<std::optional<std::shared_ptr<PeerClient>>>
    wait_for_peer(uint32_t ledger_seq, int timeout_secs = 300);

    boost::asio::awaitable<std::optional<std::shared_ptr<PeerClient>>>
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

    void
    set_unsolicited_handler(UnsolicitedHandler handler);

    /// Try to connect to an endpoint. Returns nullptr on failure.
    /// Redirect IPs from 503 are fed into the tracker + tried immediately.
    boost::asio::awaitable<std::shared_ptr<PeerClient>>
    try_connect(std::string const& host, uint16_t port);

    /// Number of live (ready) peer connections.
    size_t
    connected_count() const;

private:
    /// Remove a dead peer from the connection map.
    void
    remove_peer(std::string const& key);

    /// Check if we're at the connection cap.
    bool
    at_connection_cap() const;
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

    static std::int64_t
    now_unix();

    static std::string
    make_key(std::string const& host, uint16_t port)
    {
        return host + ":" + std::to_string(port);
    }

    boost::asio::io_context& io_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    PeerSetOptions options_;
    uint32_t network_id_;
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
    };
    std::unordered_map<std::string, EndpointStats> endpoint_stats_;
    std::map<std::string, std::shared_ptr<PeerClient>> connections_;
    std::map<std::string, std::chrono::steady_clock::time_point> failed_at_;
    std::set<std::string> in_flight_;  // currently connecting
    std::set<std::string> queued_;
    std::vector<std::string> pending_connects_;
    std::set<std::string> crawl_in_flight_;
    std::set<std::string> crawl_queued_;
    std::set<std::string> crawled_;
    std::vector<std::string> pending_crawls_;
    std::optional<uint32_t> preferred_ledger_seq_;
    mutable std::mutex unsolicited_handler_mutex_;
    UnsolicitedHandler unsolicited_handler_;
};

}  // namespace catl::peer_client
