#include <atomic>
#include <boost/program_options.hpp>
#include <catl/base58/base58.h>
#include <catl/core/logger.h>
#include <catl/peer/crypto-utils.h>
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
    Logger::set_log_counter(true);
    Logger::set_relative_time(true);

    // Parse command line
    po::options_description desc("Log Keeper Options");
    desc.add_options()("help,h", "Show this help message")(
        "version,v", "Show version information")(
        "create-keys", "Generate a new node keypair and exit")(
        "host", po::value<std::string>(), "Peer host address")(
        "port", po::value<std::uint16_t>(), "Peer port number")(
        "threads",
        po::value<std::size_t>()->default_value(2),
        "Number of IO threads")(
        "protocol-definitions",
        po::value<std::string>()->default_value(
            std::string(PROJECT_ROOT) +
            "src/lesser-peer/definitions/xrpl_definitions.json"),
        "Path to protocol definitions JSON")(
        "node-private",
        po::value<std::string>(),
        "Node private key (base58-encoded)")(
        "network-id",
        po::value<std::uint32_t>()->default_value(21338),
        "Network-ID header (e.g. 21338 testnet, 21337 mainnet)")(
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

        if (vm.count("create-keys"))
        {
            // Generate a new keypair
            catl::peer::crypto_utils crypto;
            auto keys = crypto.generate_node_keys();

            // Encode the private key to base58
            std::string private_key_b58 =
                catl::base58::xrpl_codec.encode_versioned(
                    keys.secret_key.data(),
                    keys.secret_key.size(),
                    catl::base58::NODE_PRIVATE);

            std::cout << "Generated new node keypair:\n";
            std::cout << "Private Key: " << private_key_b58 << "\n";
            std::cout << "Public Key:  " << keys.public_key_b58 << "\n";
            return 0;
        }

        po::notify(vm);

        // Set debug logging if requested
        if (vm["debug"].as<bool>())
        {
            Logger::set_level(LogLevel::DEBUG);
        }

        // Check required arguments for normal operation
        if (!vm.count("host") || !vm.count("port"))
        {
            std::cerr << "Error: host and port are required\n\n";
            std::cout << "Usage: " << argv[0] << " <host> <port> [options]\n\n";
            std::cout << desc << "\n";
            return 1;
        }

        // Create peer config
        catl::peer::peer_config config;
        config.host = vm["host"].as<std::string>();
        config.port = vm["port"].as<std::uint16_t>();
        config.io_threads = vm["threads"].as<std::size_t>();
        config.protocol_definitions_path =
            vm["protocol-definitions"].as<std::string>();
        config.network_id = vm["network-id"].as<std::uint32_t>();

        // Set node private key if provided
        if (vm.count("node-private"))
        {
            config.node_private_key = vm["node-private"].as<std::string>();
        }

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
