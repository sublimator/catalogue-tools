#include "ripple.pb.h"
#include <catl/core/logger.h>
#include <catl/peer/log-keeper/log-keeper.h>
#include <catl/peer/packet-names.h>

namespace catl::peer::log_keeper {

log_keeper::log_keeper(peer_config config)
    : config_(std::move(config))
    , ssl_context_(
          std::make_unique<asio::ssl::context>(asio::ssl::context::tlsv12))
{
    setup_ssl_context();
}

log_keeper::~log_keeper()
{
    request_stop();
}

void
log_keeper::setup_ssl_context()
{
    ssl_context_->set_options(
        asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
        asio::ssl::context::no_sslv3 | asio::ssl::context::single_dh_use);

    // Set verification mode to none (we're not verifying peer certificates)
    ssl_context_->set_verify_mode(asio::ssl::verify_none);

    // Enable ECDH
    SSL_CTX_set_ecdh_auto(ssl_context_->native_handle(), 1);
}

void
log_keeper::run()
{
    {
        std::lock_guard<std::mutex> lock(shutdown_mutex_);
        if (running_)
        {
            LOGE("Log keeper is already running");
            return;
        }
        running_ = true;
        work_guard_ = std::make_unique<
            asio::executor_work_guard<asio::io_context::executor_type>>(
            asio::make_work_guard(io_context_));
    }

    try
    {
        // Connect to peer
        connect_to_peer();

        // Start IO threads
        for (std::size_t i = 0; i < config_.io_threads; ++i)
        {
            io_threads_.emplace_back([this]() {
                try
                {
                    io_context_.run();
                }
                catch (std::exception const& e)
                {
                    LOGE("IO thread exception: ", e.what());
                }
            });
        }

        // Wait for all IO threads to complete
        for (auto& thread : io_threads_)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
    }
    catch (std::exception const& e)
    {
        LOGE("Log keeper error: ", e.what());
    }

    running_ = false;
}

void
log_keeper::stop()
{
    {
        std::lock_guard<std::mutex> lock(shutdown_mutex_);
        if (!running_ || stopping_)
        {
            return;
        }
        stopping_ = true;
    }

    LOGI("Stopping log keeper...");

    // Close connection
    if (connection_)
    {
        connection_->close();
    }

    // Stop io_context
    work_guard_.reset();
    io_context_.stop();

    // Wait for threads
    for (auto& thread : io_threads_)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }

    io_threads_.clear();
    stopping_ = false;
    LOGI("Log keeper stopped");
}

void
log_keeper::request_stop()
{
    io_context_.post([this]() { stop(); });
}

void
log_keeper::connect_to_peer()
{
    LOGI("Connecting to ", config_.host, ":", config_.port);

    connection_ =
        std::make_shared<peer_connection>(io_context_, *ssl_context_, config_);

    connection_->async_connect([this](boost::system::error_code ec) {
        if (!ec)
        {
            LOGI("Connected successfully");
            handle_connection(connection_);
        }
        else
        {
            LOGE("Connection failed: ", ec.message());
            request_stop();
        }
    });
}

void
log_keeper::handle_connection(std::shared_ptr<peer_connection> connection)
{
    // Start reading packets
    connection->start_read([this](
                               packet_header const& header,
                               std::vector<std::uint8_t> const& payload) {
        handle_packet(header, payload);
    });
}

void
log_keeper::handle_packet(
    packet_header const& header,
    std::vector<std::uint8_t> const& payload)
{
    auto type = static_cast<packet_type>(header.type);

    LOGD(
        "Received packet: ",
        get_packet_name(header.type),
        " (",
        header.type,
        ") size=",
        header.payload_size);

    switch (type)
    {
        case packet_type::status_change:
            handle_status_change(payload);
            break;

        case packet_type::transaction:
            handle_transaction(payload);
            break;

        case packet_type::ledger_data:
            handle_ledger_data(payload);
            break;

        case packet_type::ping:
            // TODO: Handle ping/pong
            break;

        default:
            LOGD("Unhandled packet type: ", get_packet_name(header.type));
            break;
    }
}

void
log_keeper::handle_status_change(std::vector<std::uint8_t> const& payload)
{
    protocol::TMStatusChange status;
    if (!status.ParseFromArray(payload.data(), payload.size()))
    {
        LOGE("Failed to parse TMStatusChange");
        return;
    }

    if (status.has_ledgerseq())
    {
        current_ledger_seq_ = status.ledgerseq();
        LOGI("Current ledger: ", current_ledger_seq_);
    }

    if (status.has_ledgerhash())
    {
        auto const& hash = status.ledgerhash();
        if (hash.size() == 32)
        {
            std::copy(hash.begin(), hash.end(), current_ledger_hash_.begin());
        }
    }
}

void
log_keeper::handle_transaction(std::vector<std::uint8_t> const& payload)
{
    protocol::TMTransaction txn;
    if (!txn.ParseFromArray(payload.data(), payload.size()))
    {
        LOGE("Failed to parse TMTransaction");
        return;
    }

    // TODO: Process and store transaction
    LOGI("Received transaction, size=", txn.rawtransaction().size());
}

void
log_keeper::handle_ledger_data(std::vector<std::uint8_t> const& payload)
{
    protocol::TMLedgerData data;
    if (!data.ParseFromArray(payload.data(), payload.size()))
    {
        LOGE("Failed to parse TMLedgerData");
        return;
    }

    LOGI(
        "Received ledger data: seq=",
        data.ledgerseq(),
        " nodes=",
        data.nodes_size());

    // TODO: Process and store ledger nodes
}

}  // namespace catl::peer::log_keeper