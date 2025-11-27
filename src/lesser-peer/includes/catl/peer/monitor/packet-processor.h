#pragma once

#include "catl/peer/manifest-tracker.h"
#include "catl/peer/monitor/peer-dashboard.h"
#include "catl/peer/monitor/types.h"
#include "catl/peer/peer-connection.h"
#include "catl/peer/txset-acquirer.h"
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>

namespace catl::peer::monitor {

class packet_processor
{
public:
    packet_processor(monitor_config const& config);

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
    handle_ledger_data(std::vector<std::uint8_t> const& payload);
    void
    handle_propose_ledger(
        std::shared_ptr<peer_connection> connection,
        std::vector<std::uint8_t> const& payload);
    void
    handle_status_change(std::vector<std::uint8_t> const& payload);
    void
    handle_validation(std::vector<std::uint8_t> const& payload);
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

    // Transaction set acquirers keyed by set hash
    // Use shared_ptr so callbacks cannot invalidate current stack frames while
    // we iterate (completion may erase from map).
    std::map<std::string, std::shared_ptr<TransactionSetAcquirer>>
        txset_acquirers_;

    // Manifest tracker for mapping ephemeral keys to master validator keys
    ManifestTracker manifest_tracker_;

    // Discovered peer endpoints (from mtENDPOINTS)
    std::set<std::string> available_endpoints_;

    // Seen validations per ledger for deduplication
    // Key = ledger sequence, Value = set of validation hashes
    std::map<uint32_t, std::set<std::string>> validations_by_ledger_;
    static constexpr size_t MAX_LEDGERS_TRACKED = 5;
};

}  // namespace catl::peer::monitor
