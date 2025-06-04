#pragma once

#include "catl/peer/monitor/command-line.h"
#include "catl/peer/monitor/packet-processor.h"
#include "catl/peer/monitor/types.h"
#include "catl/peer/peer-connection.h"

#include <memory>
#include <mutex>
#include <thread>
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

private:
    monitor_config config_;

    asio::io_context io_context_;
    std::unique_ptr<asio::ssl::context> ssl_context_;
    std::unique_ptr<tcp::acceptor> acceptor_;

    std::vector<std::thread> io_threads_;
    std::unique_ptr<packet_processor> processor_;

    std::atomic<bool> running_{false};
    std::atomic<bool> stopping_{false};
    std::mutex shutdown_mutex_;
    std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>>
        work_guard_;
};

}  // namespace catl::peer::monitor