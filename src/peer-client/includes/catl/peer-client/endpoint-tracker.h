#pragma once

// EndpointTracker — tracks peer ledger ranges from TMStatusChange messages.
//
// Standalone, testable component. No network code. Feed it status updates
// from any source (TMStatusChange handler, manual config, etc.) and query
// which peers have a given ledger range.
//
// Usage:
//   EndpointTracker tracker;
//   tracker.update("s1.ripple.com:51235", {.first_seq = 32570, .last_seq =
//   103000000}); auto peer = tracker.best_peer_for(99000000);

#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace catl::peer_client {

struct PeerStatus
{
    uint32_t first_seq = 0;    // from TMStatusChange.firstSeq
    uint32_t last_seq = 0;     // from TMStatusChange.lastSeq
    uint32_t current_seq = 0;  // from TMStatusChange.ledgerSeq
    std::chrono::steady_clock::time_point last_seen;
};

class EndpointTracker
{
public:
    using DiscoveredObserver = std::function<void(std::string const& endpoint)>;
    using StatusObserver = std::function<
        void(std::string const& endpoint, PeerStatus const& status)>;

    /// Update a peer's status. Called from TMStatusChange handler.
    void
    update(std::string const& endpoint, PeerStatus status);

    /// Remove a peer (e.g. on disconnect).
    void
    remove(std::string const& endpoint);

    /// Find the best peer that has a given ledger sequence.
    /// Returns the endpoint string, or nullopt if none found.
    std::optional<std::string>
    best_peer_for(uint32_t ledger_seq) const;

    /// Get all peers that have a given ledger sequence.
    std::vector<std::string>
    peers_for(uint32_t ledger_seq) const;

    /// Check if any tracked peer has the given ledger.
    bool
    has_peer_for(uint32_t ledger_seq) const;

    /// Get a peer's status, if tracked.
    std::optional<PeerStatus>
    get_status(std::string const& endpoint) const;

    /// Number of tracked peers.
    size_t
    size() const;

    /// Add a discovered endpoint (from TMEndpoints gossip).
    /// Only records the address — range unknown until we connect.
    void
    add_discovered(std::string const& endpoint);

    /// Get all discovered endpoints that we haven't connected to yet
    /// (have no range info).
    std::vector<std::string>
    undiscovered() const;

    /// Get every known endpoint, including cached/discovered ones whose
    /// ledger range is still unknown.
    std::vector<std::string>
    all_endpoints() const;

    /// Clear all tracked peers.
    void
    clear();

    /// Parse an endpoint string into host + port.
    /// Handles IPv4 ("1.2.3.4:51235"), IPv6 ("[::ffff:1.2.3.4]:51235"),
    /// and hostnames ("host.com:51235").
    static bool
    parse_endpoint(
        std::string const& endpoint,
        std::string& host,
        uint16_t& port);

    void
    set_discovered_observer(DiscoveredObserver observer);

    void
    set_status_observer(StatusObserver observer);

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, PeerStatus> peers_;
    DiscoveredObserver discovered_observer_;
    StatusObserver status_observer_;
};

}  // namespace catl::peer_client
