#pragma once

#include "endpoint-tracker.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

struct sqlite3;

namespace catl::peer_client {

/// Persistent cache of peer endpoints discovered during bootstrap.
///
/// This is intentionally small, synchronous, and internally mutex-protected.
/// Immediate event-driven writes keep bootstrap state fresh across process
/// runs without introducing another asynchronous owner.
class PeerEndpointCache
{
public:
    struct Health
    {
        bool available = false;
        std::string error;
    };

    struct Entry
    {
        std::string endpoint;
        PeerStatus status;
        std::int64_t last_seen_at = 0;
        std::int64_t last_success_at = 0;
        std::int64_t last_failure_at = 0;
        std::uint64_t success_count = 0;
        std::uint64_t failure_count = 0;
        bool seen_crawl = false;      // appeared in /crawl response
        bool seen_endpoints = false;  // arrived via TMEndpoints message
        bool seen_redirect = false;   // arrived via HTTP 503 peer-ips redirect
        bool connected_ok = false;    // we successfully connected
    };

    static std::shared_ptr<PeerEndpointCache>
    open(std::string const& path);

    ~PeerEndpointCache();

    std::vector<Entry>
    load_bootstrap_candidates(uint32_t network_id, std::size_t limit) const;

    /// Load bounded persistent hints whose last observed range covered the
    /// requested ledger. These rows authorize search only; a live handshake
    /// must still revalidate coverage before the peer can be used.
    std::vector<Entry>
    load_covering_candidates(
        uint32_t network_id,
        uint32_t ledger_seq,
        std::size_t limit) const;

    std::size_t
    count_endpoints(uint32_t network_id) const;

    Health
    health() const;

    void
    remember_discovered(uint32_t network_id, std::string const& endpoint);

    void
    remember_status(
        uint32_t network_id,
        std::string const& endpoint,
        PeerStatus const& status);

    void
    remember_connect_success(
        uint32_t network_id,
        std::string const& endpoint,
        PeerStatus const& status);

    void
    remember_connect_failure(uint32_t network_id, std::string const& endpoint);

    /// Mark a peer as seen in a /crawl response.
    void
    remember_seen_crawl(uint32_t network_id, std::string const& endpoint);

    /// Mark a peer as seen in a TMEndpoints message.
    void
    remember_seen_endpoints(uint32_t network_id, std::string const& endpoint);

    /// Mark a peer as seen in an HTTP 503 peer-ips redirect.
    void
    remember_seen_redirect(uint32_t network_id, std::string const& endpoint);

    std::string const&
    path() const
    {
        return path_;
    }

private:
    explicit PeerEndpointCache(std::string path);

    void
    initialize_schema();

    void
    migrate_canonical_endpoints();

    [[nodiscard]] bool
    is_disabled() const;

    void
    throw_if_disabled() const;

    void
    disable();

    static std::int64_t
    now_unix();

    std::string path_;
    //@@authority state=prv.S0073 status=provisional scope=field
    // bind=catl::peer_client::PeerEndpointCache::db_@ea77b8c353c4656e
    // owner=unresolved owner_hypothesis=peer_endpoint_cache
    // observed_home=peer_endpoint_cache
    // label=persistent_peer_endpoint_range_search_hints
    sqlite3* db_ = nullptr;
    mutable std::mutex mutex_;
    //@@authority state=prv.S0075 status=provisional scope=field
    //bind=catl::peer_client::PeerEndpointCache::disabled_@ff731b30e8046e6b
    // owner=unresolved owner_hypothesis=peer_endpoint_cache
    // observed_home=peer_endpoint_cache
    // label=latched_peer_endpoint_cache_unavailable_fact
    bool disabled_ = false;
    std::string disabled_reason_;
};

}  // namespace catl::peer_client
