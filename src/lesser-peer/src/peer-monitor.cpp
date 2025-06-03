#include <catl/core/logger.h>
#include <catl/peer/peer-monitor.h>
#include <iostream>

namespace catl::peer {

peer_monitor::peer_monitor(connection_config config, packet_filter filter)
    : config_(std::move(config))
    , filter_(std::move(filter))
    , ssl_context_(
          std::make_unique<asio::ssl::context>(asio::ssl::context::tlsv12))
    , processor_(std::make_unique<packet_processor>(config_, filter_))
{
    setup_ssl_context();
}

peer_monitor::~peer_monitor()
{
    // Ensure cleanup on destruction
    request_stop();
}

void
peer_monitor::setup_ssl_context()
{
    ssl_context_->set_options(
        asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
        asio::ssl::context::no_sslv3 | asio::ssl::context::single_dh_use);

    // Set verification mode to none (we're not verifying peer certificates)
    ssl_context_->set_verify_mode(asio::ssl::verify_none);

    // Enable ECDH
    SSL_CTX_set_ecdh_auto(ssl_context_->native_handle(), 1);

    if (config_.listen_mode)
    {
        // Load server certificate and key
        try
        {
            ssl_context_->use_certificate_file(
                config_.cert_path, asio::ssl::context::pem);
            ssl_context_->use_private_key_file(
                config_.key_path, asio::ssl::context::pem);
        }
        catch (std::exception const& e)
        {
            throw std::runtime_error(
                "Failed to load certificate/key files: " +
                std::string(e.what()) +
                "\nTry: openssl req -nodes -new -x509 -keyout " +
                config_.key_path + " -out " + config_.cert_path);
        }
    }
}

void
peer_monitor::run()
{
    running_ = true;

    // Create work guard to keep io_context alive
    work_guard_ = std::make_unique<
        asio::executor_work_guard<asio::io_context::executor_type>>(
        io_context_.get_executor());

    try
    {
        if (config_.listen_mode)
        {
            // Setup acceptor
            acceptor_ = std::make_unique<tcp::acceptor>(io_context_);
            tcp::endpoint endpoint(tcp::v4(), config_.port);
            acceptor_->open(endpoint.protocol());
            acceptor_->set_option(tcp::acceptor::reuse_address(true));
            acceptor_->bind(endpoint);
            acceptor_->listen();

            LOGI(
                "Listening on ",
                endpoint.address().to_string(),
                ":",
                config_.port);
            start_accept();
        }
        else
        {
            // Connect to peer
            auto connection = std::make_shared<peer_connection>(
                io_context_, *ssl_context_, config_);
            connection->async_connect(
                [this, connection](boost::system::error_code ec) {
                    if (!ec)
                    {
                        LOGI(
                            "Connected and upgraded to ",
                            connection->remote_endpoint());
                        // Connection is now fully established with HTTP upgrade
                        // complete
                        handle_connection(connection);
                    }
                    else
                    {
                        LOGE("Connection failed: ", ec.message());
                        stop();
                    }
                });
        }

        // Start IO threads
        for (std::size_t i = 0; i < config_.io_threads; ++i)
        {
            io_threads_.emplace_back([this] {
                try
                {
                    LOGI("IO thread started");
                    io_context_.run();
                    LOGI("IO thread finished");
                }
                catch (std::exception const& e)
                {
                    LOGE("IO thread exception: ", e.what());
                }
            });
        }

        // Wait for threads to complete
        {
            std::lock_guard<std::mutex> lock(shutdown_mutex_);
            for (auto& thread : io_threads_)
            {
                if (thread.joinable())
                {
                    thread.join();
                }
            }
            io_threads_.clear();
        }
    }
    catch (std::exception const& e)
    {
        LOGE("Fatal error: ", e.what());
        std::cerr << "Fatal error: " << e.what() << "\n";
        // Clean up threads even on error
        try
        {
            stop();
        }
        catch (...)
        {
        }
    }
}

void
peer_monitor::request_stop()
{
    bool expected = false;
    if (stopping_.compare_exchange_strong(expected, true))
    {
        running_ = false;

        // Reset work guard to allow io_context to finish
        work_guard_.reset();

        // Stop the io_context
        io_context_.stop();
    }
}

void
peer_monitor::stop()
{
    // Request stop first
    request_stop();

    // Use a lock to ensure we only join threads once
    std::lock_guard<std::mutex> lock(shutdown_mutex_);

    // Wait for threads
    for (auto& thread : io_threads_)
    {
        if (thread.joinable())
        {
            try
            {
                thread.join();
            }
            catch (std::exception const& e)
            {
                LOGE("Error joining thread: ", e.what());
            }
        }
    }

    // Clear the threads vector
    io_threads_.clear();
}

void
peer_monitor::start_accept()
{
    auto connection =
        std::make_shared<peer_connection>(io_context_, *ssl_context_, config_);

    acceptor_->async_accept(
        connection->socket_->lowest_layer(),
        [this, connection](boost::system::error_code ec) {
            handle_accept(connection, ec);
        });
}

void
peer_monitor::handle_accept(
    std::shared_ptr<peer_connection> connection,
    boost::system::error_code ec)
{
    if (!ec && running_)
    {
        LOGI(
            "Accepted connection from ",
            connection->socket_->lowest_layer().remote_endpoint());

        connection->async_accept(
            *acceptor_, [this, connection](boost::system::error_code ec) {
                if (!ec)
                {
                    handle_connection(connection);
                }
                else
                {
                    LOGE("Accept handshake failed: ", ec.message());
                }

                // Accept next connection
                if (running_)
                {
                    start_accept();
                }
            });
    }
    else if (running_)
    {
        LOGE("Accept failed: ", ec.message());
        // Try accepting again
        start_accept();
    }
}

void
peer_monitor::handle_connection(std::shared_ptr<peer_connection> connection)
{
    // Start reading packets
    connection->start_read([this, connection](
                               packet_header const& header,
                               std::vector<std::uint8_t> const& payload) {
        processor_->process_packet(connection, header, payload);
    });
}

}  // namespace catl::peer