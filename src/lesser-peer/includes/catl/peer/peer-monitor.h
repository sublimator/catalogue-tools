#pragma once

#include "command-line.h"
#include "packet-processor.h"
#include "peer-connection.h"
#include "types.h"

#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace catl::peer {

class peer_monitor
{
public:
    peer_monitor(connection_config config, packet_filter filter);
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
    connection_config config_;
    packet_filter filter_;

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

}  // namespace catl::peer