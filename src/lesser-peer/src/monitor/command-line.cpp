#include <boost/algorithm/string.hpp>
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
        "version,v", "Show version information");

    connection_desc_.add_options()(
        "host",
        po::value<std::string>()->required(),
        "Host IP address or hostname")(
        "port", po::value<std::uint16_t>()->required(), "Port number")(
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
        "no-cls", po::bool_switch(), "Don't clear screen between updates")(
        "no-dump", po::bool_switch(), "Don't dump packet contents")(
        "no-stats", po::bool_switch(), "Don't show statistics")(
        "no-http", po::bool_switch(), "Don't show HTTP upgrade messages")(
        "no-hex", po::bool_switch(), "Don't show hex dumps")(
        "no-json", po::bool_switch(), "Don't show JSON output for transactions/validations")(
        "raw-hex", po::bool_switch(), "Show raw hex without formatting")(
        "slow",
        po::bool_switch(),
        "Update display at most once every 5 seconds")(
        "manifests-only",
        po::bool_switch(),
        "Only collect manifests then exit");

    filter_desc_.add_options()(
        "show",
        po::value<std::string>(),
        "Show only these packet types (comma-separated)")(
        "hide",
        po::value<std::string>(),
        "Hide these packet types (comma-separated)");

    // Positional arguments for host and port
    pos_desc_.add("host", 1);
    pos_desc_.add("port", 1);

    all_desc_.add(general_desc_)
        .add(connection_desc_)
        .add(display_desc_)
        .add(filter_desc_);
}

std::optional<connection_config>
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

        connection_config config;
        config.host = vm["host"].as<std::string>();
        config.port = vm["port"].as<std::uint16_t>();
        config.listen_mode = vm["listen"].as<bool>();
        config.cert_path = vm["cert"].as<std::string>();
        config.key_path = vm["key"].as<std::string>();
        config.io_threads = vm["threads"].as<std::size_t>();
        config.connection_timeout =
            std::chrono::seconds(vm["timeout"].as<int>());

        config.use_cls = !vm["no-cls"].as<bool>();
        config.no_dump = vm["no-dump"].as<bool>();
        config.no_stats = vm["no-stats"].as<bool>();
        config.no_http = vm["no-http"].as<bool>();
        config.no_hex = vm["no-hex"].as<bool>();
        config.no_json = vm["no-json"].as<bool>();
        config.raw_hex = vm["raw-hex"].as<bool>();
        config.slow = vm["slow"].as<bool>();
        config.manifests_only = vm["manifests-only"].as<bool>();
        config.protocol_definitions_path =
            vm["protocol-definitions"].as<std::string>();

        if (config.no_hex && config.raw_hex)
        {
            throw std::runtime_error("Cannot use both --no-hex and --raw-hex");
        }

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
    os << "A tool to connect to an XRPL node as a peer and monitor traffic\n\n";
    os << "Usage: peermon HOST PORT [options]\n\n";
    os << all_desc_ << "\n";

    os << "Packet types:\n";
    os << "  mtMANIFESTS, mtPING, mtCLUSTER, mtENDPOINTS, mtTRANSACTION,\n";
    os << "  mtGET_LEDGER, mtLEDGER_DATA, mtPROPOSE_LEDGER, mtSTATUS_CHANGE,\n";
    os << "  mtHAVE_SET, mtVALIDATION, mtGET_OBJECTS, mtGET_SHARD_INFO,\n";
    os << "  mtSHARD_INFO, mtGET_PEER_SHARD_INFO, mtPEER_SHARD_INFO,\n";
    os << "  mtVALIDATORLIST, mtSQUELCH, mtVALIDATORLISTCOLLECTION,\n";
    os << "  mtPROOF_PATH_REQ, mtPROOF_PATH_RESPONSE, mtREPLAY_DELTA_REQ,\n";
    os << "  mtREPLAY_DELTA_RESPONSE, mtGET_PEER_SHARD_INFO_V2,\n";
    os << "  mtPEER_SHARD_INFO_V2, mtHAVE_TRANSACTIONS, mtTRANSACTIONS,\n";
    os << "  mtRESOURCE_REPORT\n\n";

    os << "Examples:\n";
    os << "  peermon r.ripple.com 51235 --no-dump\n";
    os << "  peermon r.ripple.com 51235 --show mtGET_LEDGER,mtVALIDATION\n";
    os << "  peermon 0.0.0.0 51235 --listen\n";
}

void
command_line_parser::print_version(std::ostream& os) const
{
    os << "XRPL Peer Monitor v" << VERSION << "\n";
    os << "Built with modern C++ and Boost.Asio\n";
}

}  // namespace catl::peer::monitor