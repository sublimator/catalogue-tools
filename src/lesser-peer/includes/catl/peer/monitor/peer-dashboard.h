#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace catl::peer::monitor {

class PeerDashboard
{
public:
    struct LedgerInfo
    {
        uint32_t sequence = 0;
        std::string hash;
        uint32_t validation_count = 0;
        std::chrono::steady_clock::time_point last_update;
    };

    struct Stats
    {
        // Peer identification
        std::string peer_id;  // Unique ID for multi-peer tracking

        // Connection info
        std::string peer_address;
        std::string peer_version;
        std::string network_id;
        std::string protocol_version;
        bool connected = false;

        // Packet counters by type
        std::map<std::string, uint64_t> packet_counts;
        std::map<std::string, uint64_t> packet_bytes;

        // Overall stats
        uint64_t total_packets = 0;
        uint64_t total_bytes = 0;
        double elapsed_seconds = 0;

        // Recent activity (last 60s)
        double packets_per_sec = 0;
        double bytes_per_sec = 0;

        // Connection state
        std::string connection_state = "Disconnected";
        std::chrono::steady_clock::time_point last_packet_time;

        // Ledger tracking
        LedgerInfo current_ledger;
        std::map<uint32_t, uint32_t> recent_ledgers;  // seq -> validation count
    };

    PeerDashboard();
    ~PeerDashboard();

    void
    start();
    void
    stop();

    // Single peer methods (for backward compatibility)
    void
    update_stats(const Stats& stats);
    Stats
    get_stats() const;

    // Multi-peer methods
    void
    update_peer_stats(const std::string& peer_id, const Stats& stats);
    void
    remove_peer(const std::string& peer_id);
    std::vector<Stats>
    get_all_peers_stats() const;

    // Ledger tracking
    void
    update_ledger_info(
        uint32_t sequence,
        const std::string& hash,
        uint32_t validation_count);
    LedgerInfo
    get_current_ledger() const;

    // Discovered peer endpoints
    void
    update_available_endpoints(std::vector<std::string> const& endpoints);
    std::vector<std::string>
    get_available_endpoints() const;

    // Shutdown callback - called when user quits the dashboard
    using shutdown_callback_t = std::function<void()>;
    void
    set_shutdown_callback(shutdown_callback_t callback)
    {
        shutdown_callback_ = std::move(callback);
    }

    std::atomic<uint64_t> ui_render_counter_{0};  // UI thread heartbeat

private:
    void
    run_ui();

    // UI thread
    std::unique_ptr<std::thread> ui_thread_;
    std::atomic<bool> running_{false};

    // Multi-peer tracking
    mutable std::mutex peers_mutex_;
    std::map<std::string, Stats> peer_stats_;  // peer_id -> Stats

    // Backward compatibility - default peer
    std::string default_peer_id_{"default"};

    // Connection info (deprecated - kept for backward compatibility)
    std::string peer_address_;
    std::string peer_version_;
    std::string network_id_;
    std::string protocol_version_;
    std::atomic<bool> connected_{false};

    // Packet stats (deprecated - kept for backward compatibility)
    mutable std::mutex packet_mutex_;
    std::map<std::string, uint64_t> packet_counts_;
    std::map<std::string, uint64_t> packet_bytes_;

    // Overall stats (deprecated - kept for backward compatibility)
    std::atomic<uint64_t> total_packets_{0};
    std::atomic<uint64_t> total_bytes_{0};
    std::atomic<double> elapsed_seconds_{0};

    // Throughput tracking (deprecated - kept for backward compatibility)
    struct ThroughputSample
    {
        std::chrono::steady_clock::time_point timestamp;
        uint64_t packets;
        uint64_t bytes;
    };

    mutable std::mutex throughput_mutex_;
    std::deque<ThroughputSample> throughput_history_;

    // Connection state (deprecated - kept for backward compatibility)
    mutable std::mutex state_mutex_;
    std::string connection_state_{"Disconnected"};
    std::chrono::steady_clock::time_point last_packet_time_;

    // Global ledger tracking
    mutable std::mutex ledger_mutex_;
    LedgerInfo current_validated_ledger_;
    std::map<uint32_t, uint32_t> ledger_validations_;  // seq -> count

    // Known peer endpoints (mtENDPOINTS)
    mutable std::mutex endpoints_mutex_;
    std::vector<std::string> available_endpoints_;

    // Shutdown callback
    shutdown_callback_t shutdown_callback_;
};

}  // namespace catl::peer::monitor
