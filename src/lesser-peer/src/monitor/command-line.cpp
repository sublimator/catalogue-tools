#include <boost/algorithm/string.hpp>
#include <catl/core/logger.h>
#include <catl/peer/monitor/command-line.h>
#include <catl/peer/packet-names.h>
#include <iostream>
#include <sstream>

namespace catl::peer::monitor {

constexpr char VERSION[] = "2.0.0";

command_line_parser::command_line_parser()
    : general_desc_("General options")
    , connection_desc_("Connection options")
    , display_desc_("Display options")
    , filter_desc_("Packet filter options")
    , all_desc_("All options")
{
    general_desc_.add_options()("help,h", "Show this help message")(
        "version,V", "Show version information")(
        "verbose,v", "Increase log verbosity (can be repeated)")(
        "quiet,q", "Decrease log verbosity");

    connection_desc_.add_options()(
        "host",
        po::value<std::string>()->required(),
        "Host IP address or hostname")(
        "port", po::value<std::uint16_t>()->required(), "Port number")(
        "peer,p",
        po::value<std::vector<std::string>>()->multitoken(),
        "Additional peer(s) as host:port (can specify multiple)")(
        "network-id",
        po::value<std::uint32_t>()->default_value(21338),
        "Network-ID header (e.g. 21338 testnet, 21337 mainnet)")(
        "listen,l", po::bool_switch(), "Listen mode (act as server)")(
        "cert",
        po::value<std::string>()->default_value("listen.cert"),
        "TLS certificate file (listen mode)")(
        "key",
        po::value<std::string>()->default_value("listen.key"),
        "TLS key file (listen mode)")(
        "threads",
        po::value<std::size_t>()->default_value(1),
        "Number of IO threads")(
        "timeout",
        po::value<int>()->default_value(30),
        "Connection timeout in seconds")(
        "protocol-definitions",
        po::value<std::string>()->default_value(
            std::string(PROJECT_ROOT) +
            "src/lesser-peer/definitions/xrpl_definitions.json"),
        "Path to XRPL protocol definitions JSON file");

    display_desc_.add_options()(
        "dashboard",
        po::bool_switch(),
        "Enable FTXUI dashboard (implies --quiet output to stdout)")(
        "manifests-only",
        po::bool_switch(),
        "Harvest manifests (specialized mode)")(
        "txset-acquire",
        po::bool_switch(),
        "Enable transaction set acquisition");

    filter_desc_.add_options()(
        "show",
        po::value<std::string>(),
        "Show only these packet types (comma-separated)")(
        "hide",
        po::value<std::string>(),
        "Hide these packet types (comma-separated)")(
        "query-tx",
        po::value<std::vector<std::string>>()->multitoken(),
        "Query specific transactions by hash");

    // Positional arguments for host and port
    pos_desc_.add("host", 1);
    pos_desc_.add("port", 1);

    all_desc_.add(general_desc_)
        .add(connection_desc_)
        .add(display_desc_)
        .add(filter_desc_);
}

std::optional<monitor_config>
command_line_parser::parse(int argc, char* argv[])
{
    try
    {
        po::variables_map vm;
        po::store(
            po::command_line_parser(argc, argv)
                .options(all_desc_)
                .positional(pos_desc_)
                .run(),
            vm);

        if (vm.count("help"))
        {
            print_help(std::cout);
            return std::nullopt;
        }

        if (vm.count("version"))
        {
            print_version(std::cout);
            return std::nullopt;
        }

        po::notify(vm);

        monitor_config config;

        // Populate peer config
        config.peer.host = vm["host"].as<std::string>();
        config.peer.port = vm["port"].as<std::uint16_t>();
        config.peer.listen_mode = vm["listen"].as<bool>();
        config.peer.cert_path = vm["cert"].as<std::string>();
        config.peer.key_path = vm["key"].as<std::string>();
        config.peer.io_threads = vm["threads"].as<std::size_t>();
        config.peer.connection_timeout =
            std::chrono::seconds(vm["timeout"].as<int>());
        config.peer.protocol_definitions_path =
            vm["protocol-definitions"].as<std::string>();
        config.peer.network_id = vm["network-id"].as<std::uint32_t>();

        // Parse additional peers (--peer host:port)
        if (vm.count("peer"))
        {
            auto peers = vm["peer"].as<std::vector<std::string>>();
            for (const auto& peer_str : peers)
            {
                auto colon_pos = peer_str.rfind(':');
                if (colon_pos == std::string::npos)
                {
                    throw std::runtime_error(
                        "Invalid peer format: " + peer_str +
                        " (expected host:port)");
                }
                std::string host = peer_str.substr(0, colon_pos);
                std::uint16_t port = static_cast<std::uint16_t>(
                    std::stoi(peer_str.substr(colon_pos + 1)));
                config.additional_peers.emplace_back(host, port);
            }
        }

        // Determine Operational Mode
        config.mode = MonitorMode::Monitor;
        if (vm["manifests-only"].as<bool>())
        {
            config.mode = MonitorMode::Harvest;
        }
        else if (vm.count("query-tx"))
        {
            config.mode = MonitorMode::Query;
        }

        // Determine View Mode
        if (vm["dashboard"].as<bool>())
        {
            config.view = ViewMode::Dashboard;
        }
        else
        {
            config.view = ViewMode::Stream;
        }

        // Other flags
        config.enable_txset_acquire = vm["txset-acquire"].as<bool>();

        // Filter setup
        if (vm.count("show") && vm.count("hide"))
        {
            throw std::runtime_error("Cannot use both --show and --hide");
        }

        if (vm.count("show"))
        {
            parse_packet_filter(vm["show"].as<std::string>(), "");
        }
        else if (vm.count("hide"))
        {
            parse_packet_filter("", vm["hide"].as<std::string>());
        }

        config.filter = filter_;

        // Parse transaction query hashes
        if (vm.count("query-tx"))
        {
            auto hashes = vm["query-tx"].as<std::vector<std::string>>();
            config.query_tx_hashes.clear();
            for (const auto& hash_str : hashes)
            {
                std::vector<std::string> items;
                boost::split(items, hash_str, boost::is_any_of(","));
                for (auto& item : items)
                {
                    boost::trim(item);
                    if (!item.empty())
                        config.query_tx_hashes.push_back(item);
                }
            }
        }

        // Configure Logging based on verbosity
        // Default level: INFO
        // -q: ERROR
        // -v: DEBUG
        // -vv: TRACE (Hex dumps enabled via partitions)

        LogLevel level = LogLevel::INFO;
        int v_count = vm.count("verbose");
        if (vm.count("quiet"))
        {
            level = LogLevel::ERROR;
        }
        else if (v_count == 1)
        {
            level = LogLevel::DEBUG;
        }
        else if (v_count >= 2)
        {
            level = LogLevel::TRACE;
        }

        Logger::set_level(level);

        // If in dashboard mode, we might want to force some log settings later
        // but for now we set global defaults.

        return config;
    }
    catch (std::exception const& e)
    {
        std::cerr << "Error: " << e.what() << "\n\n";
        print_help(std::cerr);
        return std::nullopt;
    }
}

void
command_line_parser::parse_packet_filter(
    std::string const& show_list,
    std::string const& hide_list)
{
    auto parse_list = [](std::string const& list) {
        std::set<int> result;
        if (list.empty())
            return result;

        std::vector<std::string> items;
        boost::split(items, list, boost::is_any_of(","));

        for (auto const& item : items)
        {
            auto trimmed = boost::trim_copy(item);
            if (auto type = string_to_packet_type(trimmed))
            {
                result.insert(static_cast<int>(*type));
            }
            else
            {
                throw std::runtime_error("Unknown packet type: " + trimmed);
            }
        }
        return result;
    };

    filter_.show = parse_list(show_list);
    filter_.hide = parse_list(hide_list);
}

void
command_line_parser::print_help(std::ostream& os) const
{
    os << "XRPL Peer Monitor v" << VERSION << "\n";
    os << "Usage: peermon HOST PORT [options]\n\n";
    os << all_desc_ << "\n";
}

void
command_line_parser::print_version(std::ostream& os) const
{
    os << "XRPL Peer Monitor v" << VERSION << "\n";
}

}  // namespace catl::peer::monitor