#include <catl/core/logger.h>
#include <catl/peer/monitor/command-line.h>
#include <catl/peer/monitor/monitor.h>

#include <atomic>
#include <csignal>
#include <iostream>
#include <memory>

namespace {
std::unique_ptr<catl::peer::monitor::peer_monitor> g_monitor;
std::atomic<bool> g_shutdown_requested{false};

void
signal_handler(int signal)
{
    if (signal == SIGINT || signal == SIGTERM)
    {
        // Only process first signal
        bool expected = false;
        if (g_shutdown_requested.compare_exchange_strong(expected, true))
        {
            LOGI(
                "Shutdown signal received (",
                signal == SIGINT ? "SIGINT" : "SIGTERM",
                ")");
            if (g_monitor)
            {
                // Just request stop - the monitor will handle it gracefully
                g_monitor->request_stop();
            }
        }
        else
        {
            LOGI("Shutdown already in progress, ignoring signal");
        }
    }
}
}  // anonymous namespace

int
main(int argc, char* argv[])
{
    // Set logger level
    Logger::set_level(LogLevel::INFO);

    // Parse command line
    catl::peer::monitor::command_line_parser parser;
    auto config_opt = parser.parse(argc, argv);

    if (!config_opt)
    {
        return 1;  // Help or error was displayed
    }

    auto config = *config_opt;
    auto filter = parser.get_packet_filter();

    // Set up signal handlers
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    try
    {
        // Create and run peer monitor
        g_monitor =
            std::make_unique<catl::peer::monitor::peer_monitor>(config, filter);

        LOGI("Starting XRPL Peer Monitor");
        LOGI("Connecting to ", config.host, ":", config.port);

        g_monitor->run();

        LOGI("Peer monitor stopped");
    }
    catch (std::exception const& e)
    {
        LOGE("Fatal error: ", e.what());
        std::cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}