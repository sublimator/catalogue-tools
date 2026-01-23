#pragma once

#include "catl/peer/manifest-tracker.h"
#include "catl/peer/monitor/peer-dashboard.h"
#include "catl/peer/monitor/types.h"
#include "catl/peer/peer-connection.h"
#include "catl/peer/shared-node-cache.h"
#include "catl/peer/txset-acquirer.h"
#include <catl/core/logger.h>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>

namespace catl::peer::monitor {

// Log partition for proposal tracking - set to INFO to see all, ERROR for
// conflicts only
inline LogPartition&
proposal_log()
{
    static LogPartition partition("PROPOSAL", LogLevel::INFO);
    return partition;
}

class packet_processor
{
public:
    packet_processor(monitor_config const& config);
    ~packet_processor();

    // Stop all active acquirers - call before destroying
    void
    stop();

    // Process incoming packet
    void
    process_packet(
        std::string const& peer_id,
        std::shared_ptr<peer_connection> connection,
        packet_header const& header,
        std::vector<std::uint8_t> const& payload);

    // Get statistics
    packet_counters const&
    get_stats() const
    {
        return counters_;
    }

    // Set custom packet handler for specific types
    using custom_handler =
        std::function<void(packet_type, std::vector<std::uint8_t> const&)>;
    void
    set_custom_handler(packet_type type, custom_handler handler);

    // Set shutdown callback
    using shutdown_callback = std::function<void()>;
    void
    set_shutdown_callback(shutdown_callback callback)
    {
        shutdown_callback_ = std::move(callback);
    }

    // Set dashboard for ledger/validation updates
    // Note: Per-peer stats are handled separately by monitor via events
    void
    set_dashboard(std::shared_ptr<PeerDashboard> dashboard)
    {
        dashboard_ = dashboard;
    }

private:
    void
    handle_ping(
        std::shared_ptr<peer_connection> connection,
        std::vector<std::uint8_t> const& payload);
    void
    handle_manifests(std::vector<std::uint8_t> const& payload);
    void
    handle_transaction(std::vector<std::uint8_t> const& payload);
    void
    handle_get_ledger(std::vector<std::uint8_t> const& payload);
    void
    handle_ledger_data(
        std::shared_ptr<peer_connection> connection,
        std::vector<std::uint8_t> const& payload);
    void
    handle_propose_ledger(
        std::string const& peer_id,
        std::shared_ptr<peer_connection> connection,
        std::vector<std::uint8_t> const& payload);
    void
    handle_status_change(std::vector<std::uint8_t> const& payload);
    void
    handle_validation(
        std::string const& peer_id,
        std::vector<std::uint8_t> const& payload);
    void
    handle_endpoints(std::vector<std::uint8_t> const& payload);
    void
    handle_get_objects(
        std::shared_ptr<peer_connection> connection,
        std::vector<std::uint8_t> const& payload);
    void
    update_stats(packet_type type, std::size_t bytes);
    bool
    should_display_packet(packet_type type) const;
    void
    display_stats() const;

    void
    print_hex(std::uint8_t const* data, std::size_t len) const;
    void
    print_sto(std::string const& st) const;
    std::string
    get_sto_json(std::string const& st) const;
    std::string
    format_bytes(double bytes) const;

private:
    monitor_config config_;
    packet_counters counters_;
    std::map<packet_type, custom_handler> custom_handlers_;
    shutdown_callback shutdown_callback_;
    std::shared_ptr<PeerDashboard> dashboard_;

    std::chrono::steady_clock::time_point start_time_;

    // Transaction set acquirers keyed by set hash (ONE acquirer per txset)
    // Use shared_ptr so callbacks cannot invalidate current stack frames while
    // we iterate (completion may erase from map).
    std::map<std::string, std::shared_ptr<TransactionSetAcquirer>>
        txset_acquirers_;

    // Manifest tracker for mapping ephemeral keys to master validator keys
    ManifestTracker manifest_tracker_;

    // Discovered peer endpoints (from mtENDPOINTS)
    std::set<std::string> available_endpoints_;

    // Proposal duplicate detection: (prev_hash, validator_key, propose_seq) ->
    // tx_set_hash If we see same key with different hash, that's a protocol
    // violation
    struct ProposalKey
    {
        std::string prev_hash;
        std::string validator_key;
        uint32_t propose_seq;
        bool
        operator<(ProposalKey const& o) const
        {
            if (prev_hash != o.prev_hash)
                return prev_hash < o.prev_hash;
            if (validator_key != o.validator_key)
                return validator_key < o.validator_key;
            return propose_seq < o.propose_seq;
        }
    };
    std::map<ProposalKey, std::string> seen_proposals_;  // key -> tx_set_hash

    // Active peer connections for parallel txset acquisition
    std::map<std::string, std::weak_ptr<peer_connection>>
        active_connections_;  // peer_id -> connection

    // Shared node cache for txset acquisition deduplication
    SharedNodeCache shared_node_cache_;

    // Get up to N different connections for txset acquisition pool
    std::vector<std::pair<std::string, std::shared_ptr<peer_connection>>>
    get_connections_for_acquisition(
        std::string const& exclude_peer_id,
        size_t max_count);
};

}  // namespace catl::peer::monitor
