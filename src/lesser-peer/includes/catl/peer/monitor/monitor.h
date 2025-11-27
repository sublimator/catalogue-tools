#pragma once

#include "catl/peer/monitor/command-line.h"
#include "catl/peer/monitor/packet-logger.h"
#include "catl/peer/monitor/packet-processor.h"
#include "catl/peer/monitor/peer-dashboard.h"
#include "catl/peer/monitor/types.h"
#include "catl/peer/peer-connection.h"
#include "catl/peer/peer-manager.h"

#include <boost/asio/strand.hpp>
#include <fstream>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

namespace catl::peer::monitor {

class peer_monitor
{
public:
    peer_monitor(monitor_config config);
    ~peer_monitor();

    // Run the monitor (blocking)
    void
    run();

    // Stop the monitor (blocking - waits for threads)
    void
    stop();

    // Request stop (non-blocking - safe from signal handler)
    void
    request_stop();

private:
    void
    setup_ssl_context();
    void
    start_accept();
    void
    handle_accept(
        std::shared_ptr<peer_connection> connection,
        boost::system::error_code ec);
    void
    handle_connection(std::shared_ptr<peer_connection> connection);
    void
    handle_event(PeerEvent const& event);
    void
    schedule_queries(
        std::string const& peer_id,
        std::shared_ptr<peer_connection> connection);
    void
    send_empty_endpoints(
        std::string const& peer_id,
        std::shared_ptr<peer_connection> connection);
    void
    send_status(
        std::string const& peer_id,
        std::shared_ptr<peer_connection> connection);
    void
    schedule_heartbeat(
        std::string const& peer_id,
        std::shared_ptr<peer_connection> connection);
    void
    update_dashboard_state(
        std::string const& peer_id,
        PeerStateEvent const& state);
    void
    update_dashboard_stats(
        std::string const& peer_id,
        PeerStatsEvent const& stats);

private:
    monitor_config config_;

    asio::io_context io_context_;
    std::unique_ptr<asio::ssl::context> ssl_context_;
    std::unique_ptr<tcp::acceptor> acceptor_;
    std::shared_ptr<PeerEventBus> bus_;
    std::unique_ptr<PeerManager> manager_;

    std::vector<std::thread> io_threads_;
    std::unique_ptr<packet_processor> processor_;
    std::unique_ptr<PacketLogger> logger_;
    std::shared_ptr<PeerDashboard> dashboard_;

    // Log file for dashboard mode
    std::ofstream log_file_;

    std::atomic<bool> running_{false};
    std::atomic<bool> stopping_{false};
    std::mutex shutdown_mutex_;
    std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>>
        work_guard_;

    // Event bus subscription id
    PeerEventBus::SubscriberId subscription_id_{0};
    std::unique_ptr<boost::asio::strand<asio::io_context::executor_type>>
        event_strand_;

    // Track scheduled query timers per peer
    std::mutex query_mutex_;
    std::unordered_set<std::string> queries_scheduled_;
    std::vector<std::shared_ptr<asio::steady_timer>> query_timers_;

    // Track if we already sent an endpoints announcement per peer
    std::mutex endpoints_mutex_;
    std::unordered_set<std::string> endpoints_sent_;

    // Heartbeat timers per peer
    std::mutex heartbeat_mutex_;
    std::unordered_map<std::string, std::shared_ptr<asio::steady_timer>>
        heartbeat_timers_;

    // Diagnostic heartbeat
    std::atomic<uint64_t> event_counter_{0};
    std::thread diagnostic_thread_;
    void
    run_diagnostics();

    // Per-peer connection start times for elapsed calculation
    std::mutex peer_start_mutex_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point>
        peer_start_times_;
};

}  // namespace catl::peer::monitor
