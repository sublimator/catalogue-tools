#include <boost/filesystem.hpp>
#include <catl/core/logger.h>
#include <catl/peer/monitor/monitor.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

namespace catl::peer::monitor {

peer_monitor::peer_monitor(monitor_config config)
    : config_(std::move(config))
    , ssl_context_(
          std::make_unique<asio::ssl::context>(asio::ssl::context::tlsv12))
{
    // Force no_dump when using dashboard to prevent output interference
    if (config_.display.use_dashboard)
    {
        config_.display.no_dump = true;
    }

    // When querying transactions, use special query mode
    if (!config_.query_tx_hashes.empty())
    {
        config_.display.query_mode =
            true;  // Special query mode - only show transaction results
        config_.display.no_stats = true;  // No packet statistics
        config_.display.no_dump = true;   // No packet dumps
        config_.display.no_hex = true;    // No hex dumps
        config_.display.no_json = true;   // No JSON dumps
    }

    // Create the packet processor with the updated config
    processor_ = std::make_unique<packet_processor>(config_);

    setup_ssl_context();

    // Create dashboard only if enabled
    if (config_.display.use_dashboard)
    {
        dashboard_ = std::make_shared<PeerDashboard>();
        processor_->set_dashboard(dashboard_);
    }

    // Set shutdown callback for manifests-only mode
    processor_->set_shutdown_callback([this]() { request_stop(); });
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

    if (config_.peer.listen_mode)
    {
        // Load server certificate and key
        try
        {
            ssl_context_->use_certificate_file(
                config_.peer.cert_path, asio::ssl::context::pem);
            ssl_context_->use_private_key_file(
                config_.peer.key_path, asio::ssl::context::pem);
        }
        catch (std::exception const& e)
        {
            throw std::runtime_error(
                "Failed to load certificate/key files: " +
                std::string(e.what()) +
                "\nTry: openssl req -nodes -new -x509 -keyout " +
                config_.peer.key_path + " -out " + config_.peer.cert_path);
        }
    }
}

void
peer_monitor::run()
{
    running_ = true;

    // Set up log redirection and start dashboard if enabled
    if (config_.display.use_dashboard)
    {
        std::cout << "\n🎨 Starting dashboard..." << std::endl;
        std::cout << "   Redirecting logs to peermon.log" << std::endl;
        std::cout << "   Press 'q' in dashboard to quit\n" << std::endl;

        // Give user a moment to see the message
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Clear the screen for dashboard
        std::cout << "\033[2J\033[H" << std::flush;

        // Open log file for redirecting output
        namespace fs = boost::filesystem;
        fs::path log_path = fs::current_path() / "peermon.log";
        log_file_.open(log_path.string(), std::ios::out | std::ios::trunc);

        if (log_file_.is_open())
        {
            // Redirect Logger output and error streams to the log file
            Logger::set_output_stream(&log_file_);  // INFO/DEBUG logs
            Logger::set_error_stream(&log_file_);   // ERROR/WARNING logs
        }

        // Start the dashboard UI
        dashboard_->start();
    }

    // Create work guard to keep io_context alive
    work_guard_ = std::make_unique<
        asio::executor_work_guard<asio::io_context::executor_type>>(
        io_context_.get_executor());

    try
    {
        if (config_.peer.listen_mode)
        {
            // Setup acceptor
            acceptor_ = std::make_unique<tcp::acceptor>(io_context_);
            tcp::endpoint endpoint(tcp::v4(), config_.peer.port);
            acceptor_->open(endpoint.protocol());
            acceptor_->set_option(tcp::acceptor::reuse_address(true));
            acceptor_->bind(endpoint);
            acceptor_->listen();

            LOGI(
                "Listening on ",
                endpoint.address().to_string(),
                ":",
                config_.peer.port);
            start_accept();
        }
        else
        {
            // Connect to peer
            auto connection = std::make_shared<peer_connection>(
                io_context_, *ssl_context_, config_.peer);
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
        for (std::size_t i = 0; i < config_.peer.io_threads; ++i)
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

    // Stop the dashboard
    if (dashboard_)
    {
        dashboard_->stop();
    }

    // Restore logger streams to defaults and close log file
    if (log_file_.is_open())
    {
        Logger::reset_streams();
        log_file_.close();

        if (config_.display.use_dashboard)
        {
            std::cout << "\n✅ Dashboard stopped - logs saved to peermon.log"
                      << std::endl;
        }
    }

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
    auto connection = std::make_shared<peer_connection>(
        io_context_, *ssl_context_, config_.peer);

    acceptor_->async_accept(
        connection->socket().lowest_layer(),
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
            connection->socket().lowest_layer().remote_endpoint());

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
    // Start reading packets first
    connection->start_read([this, connection](
                               packet_header const& header,
                               std::vector<std::uint8_t> const& payload) {
        processor_->process_packet(connection, header, payload);
    });

    // Send transaction queries after a short delay to ensure connection is
    // ready
    if (!config_.query_tx_hashes.empty())
    {
        // Silently schedule queries after a delay

        // Schedule the queries after a delay
        auto timer = std::make_shared<asio::steady_timer>(io_context_);
        timer->expires_after(std::chrono::seconds(2));
        timer->async_wait(
            [this, connection, timer](boost::system::error_code ec) {
                if (!ec)
                {
                    // Query user's transactions
                    for (const auto& tx_hash : config_.query_tx_hashes)
                    {
                        connection->send_transaction_query(tx_hash);
                    }
                }
            });
    }
}

}  // namespace catl::peer::monitor