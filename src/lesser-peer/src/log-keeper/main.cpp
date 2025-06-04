#include <atomic>
#include <boost/program_options.hpp>
#include <catl/core/logger.h>
#include <catl/peer/log-keeper/log-keeper.h>
#include <csignal>
#include <iostream>
#include <memory>

namespace po = boost::program_options;

namespace {
std::unique_ptr<catl::peer::log_keeper::log_keeper> g_log_keeper;
std::atomic<bool> g_shutdown_requested{false};

void
signal_handler(int signal)
{
    if (signal == SIGINT || signal == SIGTERM)
    {
        bool expected = false;
        if (g_shutdown_requested.compare_exchange_strong(expected, true))
        {
            LOGI(
                "Shutdown signal received (",
                signal == SIGINT ? "SIGINT" : "SIGTERM",
                ")");
            if (g_log_keeper)
            {
                g_log_keeper->request_stop();
            }
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
    po::options_description desc("Log Keeper Options");
    desc.add_options()("help,h", "Show this help message")(
        "version,v", "Show version information")(
        "host", po::value<std::string>()->required(), "Peer host address")(
        "port", po::value<std::uint16_t>()->required(), "Peer port number")(
        "threads",
        po::value<std::size_t>()->default_value(2),
        "Number of IO threads")(
        "protocol-definitions",
        po::value<std::string>()->default_value(
            std::string(PROJECT_ROOT) +
            "src/lesser-peer/definitions/xrpl_definitions.json"),
        "Path to protocol definitions JSON")(
        "debug,d", po::bool_switch(), "Enable debug logging");

    po::positional_options_description pos_desc;
    pos_desc.add("host", 1).add("port", 1);

    try
    {
        po::variables_map vm;
        po::store(
            po::command_line_parser(argc, argv)
                .options(desc)
                .positional(pos_desc)
                .run(),
            vm);

        if (vm.count("help"))
        {
            std::cout << "XRPL Log Keeper - Maintains ledger archives\n\n";
            std::cout << "Usage: " << argv[0] << " <host> <port> [options]\n\n";
            std::cout << desc << "\n";
            return 0;
        }

        if (vm.count("version"))
        {
            std::cout << "log-keeper version 1.0.0\n";
            return 0;
        }

        po::notify(vm);

        // Set debug logging if requested
        if (vm["debug"].as<bool>())
        {
            Logger::set_level(LogLevel::DEBUG);
        }

        // Create peer config
        catl::peer::peer_config config;
        config.host = vm["host"].as<std::string>();
        config.port = vm["port"].as<std::uint16_t>();
        config.io_threads = vm["threads"].as<std::size_t>();
        config.protocol_definitions_path =
            vm["protocol-definitions"].as<std::string>();

        // Set up signal handlers
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);

        // Create and run log keeper
        g_log_keeper =
            std::make_unique<catl::peer::log_keeper::log_keeper>(config);

        LOGI("Starting XRPL Log Keeper");
        LOGI("Connecting to ", config.host, ":", config.port);

        g_log_keeper->run();

        LOGI("Log keeper stopped");
    }
    catch (std::exception const& e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}