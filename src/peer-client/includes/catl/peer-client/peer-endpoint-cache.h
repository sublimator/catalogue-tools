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
/// This is intentionally small and synchronous. xproof drives peers from a
/// single io_context thread, so immediate event-driven writes are simple and
/// keep the bootstrap state fresh across process runs.
class PeerEndpointCache
{
public:
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
        bool connected_ok = false;    // we successfully connected
    };

    static std::shared_ptr<PeerEndpointCache>
    open(std::string const& path);

    ~PeerEndpointCache();

    std::vector<Entry>
    load_bootstrap_candidates(uint32_t network_id, std::size_t limit) const;

    std::size_t
    count_endpoints(uint32_t network_id) const;

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

    std::string const&
    path() const
    {
        return path_;
    }

private:
    explicit PeerEndpointCache(std::string path);

    void
    initialize_schema();

    [[nodiscard]] bool
    is_disabled() const;

    void
    disable();

    static std::int64_t
    now_unix();

    std::string path_;
    sqlite3* db_ = nullptr;
    mutable std::mutex mutex_;
    bool disabled_ = false;
};

}  // namespace catl::peer_client
