#pragma once

#include "catl/peer/peer-connection.h"
#include "catl/peer/types.h"
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace catl::peer::log_keeper {

class log_keeper
{
public:
    log_keeper(peer_config config);
    ~log_keeper();

    // Start the log keeper (blocking)
    void
    run();

    // Stop the log keeper
    void
    stop();

    // Request stop (non-blocking - safe from signal handler)
    void
    request_stop();

private:
    void
    setup_ssl_context();

    void
    connect_to_peer();

    void
    handle_connection(std::shared_ptr<peer_connection> connection);

    void
    handle_packet(
        packet_header const& header,
        std::vector<std::uint8_t> const& payload);

    // Packet handlers
    void
    handle_status_change(std::vector<std::uint8_t> const& payload);

    void
    handle_transaction(std::vector<std::uint8_t> const& payload);

    void
    handle_ledger_data(std::vector<std::uint8_t> const& payload);

    void
    handle_replay_delta_response(std::vector<std::uint8_t> const& payload);

    void
    handle_ping(std::vector<std::uint8_t> const& payload);

    // Request transactions for a specific ledger
    void
    request_ledger_transactions(std::string const& ledger_hash);

    // Request full ledger data using TMGetLedger
    void
    request_ledger_data(std::string const& ledger_hash);

    // Send a ping to test connectivity
    void
    send_ping();

    // Send our status to the peer
    void
    send_status();

private:
    peer_config config_;

    asio::io_context io_context_;
    std::unique_ptr<asio::ssl::context> ssl_context_;
    std::shared_ptr<peer_connection> connection_;

    std::vector<std::thread> io_threads_;

    std::atomic<bool> running_{false};
    std::atomic<bool> stopping_{false};
    std::mutex shutdown_mutex_;
    std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>>
        work_guard_;

    // Current ledger tracking
    std::uint32_t current_ledger_seq_{0};
    std::array<std::uint8_t, 32> current_ledger_hash_{};

    // Request tracking
    std::atomic<std::uint64_t> request_cookie_{1000};
};

}  // namespace catl::peer::log_keeper