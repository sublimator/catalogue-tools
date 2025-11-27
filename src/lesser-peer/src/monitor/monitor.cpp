#include "ripple.pb.h"
#include <boost/filesystem.hpp>
#include <catl/core/logger.h>
#include <catl/peer/monitor/monitor.h>
#include <catl/peer/monitor/packet-logger.h>
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
    // Create the packet processor with the updated config
    processor_ = std::make_unique<packet_processor>(config_);

    // Create the packet logger
    logger_ = std::make_unique<PacketLogger>(config_);

    setup_ssl_context();

    // Event bus and manager for multi-peer support (even when using a single
    // peer)
    bus_ = std::make_shared<PeerEventBus>();
    manager_ = std::make_unique<PeerManager>(io_context_, *ssl_context_, bus_);

    // Single-thread event processing using a strand
    event_strand_ =
        std::make_unique<boost::asio::strand<asio::io_context::executor_type>>(
            io_context_.get_executor());

    // Subscribe once to the event bus to handle packets/states for all peers
    subscription_id_ = bus_->subscribe([this](PeerEvent const& event) {
        if (event_strand_)
        {
            asio::dispatch(
                *event_strand_, [this, event]() { handle_event(event); });
        }
        else
        {
            handle_event(event);
        }
    });

    // Create dashboard only if enabled
    if (config_.view == ViewMode::Dashboard)
    {
        dashboard_ = std::make_shared<PeerDashboard>();
        processor_->set_dashboard(dashboard_);
        // Register shutdown callback so 'q' in dashboard stops the monitor
        dashboard_->set_shutdown_callback([this]() { request_stop(); });
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
    if (config_.view == ViewMode::Dashboard)
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
        log_file_ << std::unitbuf;  // Flush after every insertion

        if (log_file_.is_open())
        {
            // Redirect Logger output and error streams to the log file
            Logger::set_output_stream(&log_file_);  // INFO/DEBUG logs
            Logger::set_error_stream(&log_file_);   // ERROR/WARNING logs
        }
        else
        {
            std::cerr << "❌ Failed to open peermon.log! Disabling logging to "
                         "prevent UI corruption."
                      << std::endl;
            // Disable logger output to stdout/stderr to save the dashboard
            Logger::set_output_stream(nullptr);
            Logger::set_error_stream(nullptr);
        }

        // Start the dashboard UI
        dashboard_->start();
    }

    // Start diagnostic heartbeat thread
    diagnostic_thread_ = std::thread(&peer_monitor::run_diagnostics, this);

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
            // Connect to primary peer via the manager
            auto id = manager_->add_peer(config_.peer);
            LOGI(
                "Connecting to ",
                config_.peer.host,
                ":",
                config_.peer.port,
                " as ",
                id);

            // Connect to additional peers
            for (const auto& [host, port] : config_.additional_peers)
            {
                peer::peer_config peer_cfg = config_.peer;  // Copy base config
                peer_cfg.host = host;
                peer_cfg.port = port;
                auto peer_id = manager_->add_peer(peer_cfg);
                LOGI("Connecting to ", host, ":", port, " as ", peer_id);
            }
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

    // Stop peers and unsubscribe
    if (manager_)
    {
        manager_->stop_all();
    }
    if (bus_ && subscription_id_ != 0)
    {
        bus_->unsubscribe(subscription_id_);
        subscription_id_ = 0;
    }

    {
        std::lock_guard<std::mutex> lock(query_mutex_);
        query_timers_.clear();
        queries_scheduled_.clear();
    }

    // Stop heartbeats
    {
        std::lock_guard<std::mutex> lock(heartbeat_mutex_);
        heartbeat_timers_.clear();
    }

    // Stop the dashboard
    if (dashboard_)
    {
        dashboard_->stop();
    }

    // Stop diagnostic thread
    if (diagnostic_thread_.joinable())
    {
        diagnostic_thread_.join();
    }

    // Restore logger streams to defaults and close log file
    if (log_file_.is_open())
    {
        Logger::reset_streams();
        log_file_.close();

        if (config_.view == ViewMode::Dashboard)
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
    std::string peer_id = "listener:" + connection->remote_endpoint();
    connection->start_read([this, connection, peer_id](
                               packet_header const& header,
                               std::vector<std::uint8_t> const& payload) {
        processor_->process_packet(peer_id, connection, header, payload);
    });

    // Send transaction queries after a short delay to ensure connection is
    // ready
    schedule_queries("listener", connection);
}

void
peer_monitor::handle_event(PeerEvent const& event)
{
    event_counter_++;

    // 1. Logic Processor (State, Replies, etc.)
    switch (event.type)
    {
        case PeerEventType::Packet: {
            auto const& pkt = std::get<PeerPacketEvent>(event.data);
            processor_->process_packet(
                event.peer_id, pkt.connection, pkt.header, pkt.payload);
            break;
        }
        default:
            break;
    }

    // 2. Logger Observer (Text Output)
    if (logger_)
    {
        logger_->on_event(event);
    }

    // 3. Monitor Logic (Connection State)
    switch (event.type)
    {
        case PeerEventType::State: {
            auto const& st = std::get<PeerStateEvent>(event.data);
            if (st.state == PeerStateEvent::State::Connected && st.connection)
            {
                schedule_queries(event.peer_id, st.connection);
                send_empty_endpoints(event.peer_id, st.connection);
                send_status(event.peer_id, st.connection);
                schedule_heartbeat(event.peer_id, st.connection);
            }
            break;
        }
        default:
            break;
    }
}

void
peer_monitor::schedule_queries(
    std::string const& peer_id,
    std::shared_ptr<peer_connection> connection)
{
    if (config_.query_tx_hashes.empty())
        return;

    {
        std::lock_guard<std::mutex> lock(query_mutex_);
        if (!queries_scheduled_.insert(peer_id).second)
        {
            return;  // already scheduled
        }
    }

    auto timer = std::make_shared<asio::steady_timer>(io_context_);
    timer->expires_after(std::chrono::seconds(2));
    timer->async_wait([this, connection, timer](boost::system::error_code ec) {
        if (!ec)
        {
            for (const auto& tx_hash : config_.query_tx_hashes)
            {
                connection->send_transaction_query(tx_hash);
            }
        }
    });

    std::lock_guard<std::mutex> lock(query_mutex_);
    query_timers_.push_back(timer);
}

void
peer_monitor::send_empty_endpoints(
    std::string const& peer_id,
    std::shared_ptr<peer_connection> connection)
{
    {
        std::lock_guard<std::mutex> lock(endpoints_mutex_);
        if (!endpoints_sent_.insert(peer_id).second)
        {
            return;  // already sent
        }
    }

    protocol::TMEndpoints eps;
    eps.set_version(2);

    std::string serialized;
    if (!eps.SerializeToString(&serialized))
    {
        LOGE("Failed to serialize empty TMEndpoints");
        return;
    }

    connection->async_send_packet(
        packet_type::endpoints,
        std::vector<std::uint8_t>(serialized.begin(), serialized.end()),
        [](boost::system::error_code ec) {
            if (ec)
            {
                LOGE("Failed to send empty TMEndpoints: ", ec.message());
            }
        });
}

void
peer_monitor::send_status(
    std::string const& peer_id,
    std::shared_ptr<peer_connection> connection)
{
    protocol::TMStatusChange status;
    status.set_newstatus(protocol::NodeStatus::nsMONITORING);
    status.set_newevent(
        protocol::NodeEvent::neLOST_SYNC);  // We are just starting, so we are
                                            // effectively out of sync

    // Use basic network time (seconds since Ripple epoch)
    static constexpr uint32_t RIPPLE_EPOCH = 946684800;
    auto unix_now = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
    status.set_networktime(unix_now - RIPPLE_EPOCH);

    std::string serialized;
    if (!status.SerializeToString(&serialized))
    {
        LOGE("Failed to serialize status");
        return;
    }

    connection->async_send_packet(
        packet_type::status_change,
        std::vector<std::uint8_t>(serialized.begin(), serialized.end()),
        [](boost::system::error_code ec) {
            if (ec)
            {
                LOGE("Failed to send status: ", ec.message());
            }
        });
}

void
peer_monitor::schedule_heartbeat(
    std::string const& peer_id,
    std::shared_ptr<peer_connection> connection)
{
    std::lock_guard<std::mutex> lock(heartbeat_mutex_);

    auto timer = std::make_shared<asio::steady_timer>(io_context_);
    heartbeat_timers_[peer_id] = timer;

    auto send_ping = std::make_shared<
        std::function<void(boost::system::error_code)>>();  // NOLINT
    *send_ping = [this, peer_id, connection, timer, send_ping](
                     boost::system::error_code ec) {
        if (ec || stopping_)
            return;

        // Build a simple ping
        protocol::TMPing ping;
        ping.set_type(protocol::TMPing_pingType_ptPING);
        ping.set_seq(1);

        std::vector<std::uint8_t> ping_data(ping.ByteSizeLong());
        if (ping.SerializeToArray(ping_data.data(), ping_data.size()))
        {
            connection->async_send_packet(
                packet_type::ping,
                ping_data,
                [](boost::system::error_code send_ec) {
                    if (send_ec)
                    {
                        LOGD("Heartbeat ping failed: ", send_ec.message());
                    }
                });
        }

        // Reschedule
        timer->expires_after(std::chrono::seconds(10));
        timer->async_wait(*send_ping);
    };

    timer->expires_after(std::chrono::seconds(10));
    timer->async_wait(*send_ping);
}

void
peer_monitor::run_diagnostics()
{
    uint64_t last_strand_events_count = 0;
    uint64_t last_ui_renders_count = 0;
    while (running_)
    {
        // Sleep for 30 seconds (or check regularly for shutdown)
        for (int i = 0; i < 300 && running_; ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!running_)
            break;

        uint64_t current_strand_events = event_counter_.load();
        uint64_t diff_strand_events =
            current_strand_events - last_strand_events_count;

        uint64_t current_ui_renders = 0;
        uint64_t diff_ui_renders = 0;
        if (dashboard_)
        {
            current_ui_renders = dashboard_->ui_render_counter_.load();
            diff_ui_renders = current_ui_renders - last_ui_renders_count;
        }

        LOGI(
            "❤️ Heartbeat: Strand processed ",
            current_strand_events,
            " (+ ",
            diff_strand_events,
            " events in last 30s) | UI rendered ",
            current_ui_renders,
            " (+ ",
            diff_ui_renders,
            " times in last 30s)");
        last_strand_events_count = current_strand_events;
        last_ui_renders_count = current_ui_renders;
    }
}

}  // namespace catl::peer::monitor