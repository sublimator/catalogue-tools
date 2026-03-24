#include "commands.h"

#include <catl/core/logger.h>
#include <catl/peer-client/endpoint-tracker.h>
#include <catl/xdata/protocol.h>

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

bool
parse_endpoint(std::string const& endpoint, std::string& host, uint16_t& port)
{
    return catl::peer_client::EndpointTracker::parse_endpoint(
        endpoint, host, port);
}

struct LogOverride
{
    std::string pattern;
    LogLevel level;
    bool prefix = false;
};

struct LoggingOptions
{
    bool debug_xproof = false;
    bool debug_peer = false;
    bool list_partitions = false;
    std::vector<LogOverride> overrides;
};

static bool
parse_log_override(std::string const& spec, LogOverride& out)
{
    auto const eq = spec.find('=');
    if (eq == std::string::npos || eq == 0 || eq + 1 >= spec.size())
    {
        return false;
    }

    out.pattern = spec.substr(0, eq);
    if (out.pattern.ends_with('*'))
    {
        out.prefix = true;
        out.pattern.pop_back();
        if (out.pattern.empty())
        {
            return false;
        }
    }

    return Logger::try_parse_level(spec.substr(eq + 1), out.level);
}

static bool
parse_cli_args(
    int argc,
    char* argv[],
    std::string& command,
    std::vector<std::string>& command_args,
    LoggingOptions& logging,
    std::string& error)
{
    for (int pos = 1; pos < argc; ++pos)
    {
        std::string arg = argv[pos];

        if (arg == "--debug")
        {
            logging.debug_xproof = true;
            continue;
        }

        if (arg == "--debug-peer")
        {
            logging.debug_peer = true;
            continue;
        }

        if (arg == "--list-log-partitions")
        {
            logging.list_partitions = true;
            continue;
        }

        if (arg == "--log")
        {
            if (pos + 1 >= argc)
            {
                error = "Missing value for --log";
                return false;
            }

            LogOverride override_spec;
            if (!parse_log_override(argv[++pos], override_spec))
            {
                error = std::string("Invalid --log spec: ") + argv[pos];
                return false;
            }

            logging.overrides.push_back(std::move(override_spec));
            continue;
        }

        if (command.empty())
        {
            command = std::move(arg);
        }
        else
        {
            command_args.push_back(std::move(arg));
        }
    }

    return true;
}

static bool
configure_logging(LoggingOptions const& opts, std::string& error)
{
    auto const set_exact = [](std::string_view name, LogLevel level) {
        Logger::set_partition_level(name, level);
    };

    set_exact("xproof", LogLevel::INFO);
    set_exact("verify", LogLevel::INFO);
    set_exact("anchor-verify", LogLevel::INFO);

    set_exact("peer-client", LogLevel::WARNING);
    set_exact("peer-conn", LogLevel::WARNING);
    set_exact("peer-set", LogLevel::WARNING);
    set_exact("tree-walk", LogLevel::WARNING);
    set_exact("rpc-client", LogLevel::WARNING);
    set_exact("vl-client", LogLevel::WARNING);

    if (opts.debug_xproof)
    {
        set_exact("xproof", LogLevel::DEBUG);
        set_exact("verify", LogLevel::DEBUG);
        set_exact("anchor-verify", LogLevel::DEBUG);
    }

    if (opts.debug_peer)
    {
        set_exact("peer-client", LogLevel::DEBUG);
        set_exact("peer-conn", LogLevel::DEBUG);
        set_exact("peer-set", LogLevel::DEBUG);
        set_exact("tree-walk", LogLevel::DEBUG);
        set_exact("rpc-client", LogLevel::DEBUG);
        set_exact("vl-client", LogLevel::DEBUG);
    }

    for (auto const& override_spec : opts.overrides)
    {
        bool matched = override_spec.prefix
            ? Logger::set_partition_prefix_level(
                  override_spec.pattern, override_spec.level) > 0
            : Logger::set_partition_level(
                  override_spec.pattern, override_spec.level);

        if (!matched)
        {
            error = "Unknown log partition: " + override_spec.pattern;
            if (override_spec.prefix)
            {
                error += "*";
            }
            return false;
        }
    }

    return true;
}

static void
print_log_partitions()
{
    auto names = Logger::partition_names();
    std::cout << "Registered log partitions:\n";
    for (auto const& name : names)
    {
        std::cout << "  " << name << "\n";
    }
}

static void
print_usage()
{
    std::cerr
        << "xproof - XRPL Proof Chain Tool\n"
        << "\n"
        << "Usage:\n"
        << "  xproof prove <tx_hash> [options]    build a proof chain\n"
        << "  xproof verify <proof_file> [key]    verify a proof chain\n"
        << "  xproof ping <peer:port>             peer protocol ping\n"
        << "  xproof header <peer:port> <seq>     fetch ledger header\n"
        << "\n"
        << "Options for prove:\n"
        << "  --rpc <host:port>   RPC endpoint (default: " << DEFAULT_RPC
        << ")\n"
        << "  --peer <host:port>  peer endpoint (default: " << DEFAULT_PEER
        << ")\n"
        << "  --binary            output binary format only\n"
        << "  --gzip              output compressed binary format only\n"
        << "  --output <stem>     output file stem (default: proof-<ledger>-<txid12>)\n"
        << "\n"
        << "Logging options:\n"
        << "  --debug                 enable DEBUG for xproof partitions\n"
        << "  --debug-peer            enable DEBUG for peer/rpc/vl partitions\n"
        << "  --log <name=level>      override a partition level\n"
        << "  --log <prefix*=level>   override matching partition prefix\n"
        << "  --list-log-partitions   print registered partitions and exit\n"
        << "\n"
        << "By default, prove outputs both proof.json and proof.bin.\n"
        << "Verify auto-detects JSON vs binary format.\n"
        << "Publisher key defaults to vl.ripple.com if not specified.\n"
        << "\n";
}

int
main(int argc, char* argv[])
{
    if (argc < 2)
    {
        print_usage();
        return 1;
    }

    std::string command;
    std::vector<std::string> command_args;
    LoggingOptions logging;
    std::string error;

    if (!parse_cli_args(argc, argv, command, command_args, logging, error))
    {
        std::cerr << error << "\n";
        return 1;
    }

    if (!configure_logging(logging, error))
    {
        std::cerr << error << "\n";
        return 1;
    }

    if (logging.list_partitions)
    {
        print_log_partitions();
        return 0;
    }

    if (command.empty())
    {
        print_usage();
        return 1;
    }

    if (command == "ping" && command_args.size() >= 1)
    {
        return cmd_ping(command_args[0]);
    }

    if (command == "header" && command_args.size() >= 2)
    {
        return cmd_header(command_args[0], std::stoul(command_args[1]));
    }

    if (command == "prove" || command == "prove-tx")
    {
        auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
        ProveOptions opts;
        std::string tx_hash;

        std::size_t pos = 0;
        while (pos < command_args.size())
        {
            std::string const& arg = command_args[pos];

            if (arg == "--binary")
            {
                opts.binary = true;
                ++pos;
            }
            else if (arg == "--gzip")
            {
                opts.binary = true;
                opts.compress = true;
                ++pos;
            }
            else if (arg == "--rpc" && pos + 1 < command_args.size())
            {
                opts.rpc_endpoint = command_args[pos + 1];
                pos += 2;
            }
            else if (arg == "--peer" && pos + 1 < command_args.size())
            {
                opts.peer_endpoint = command_args[pos + 1];
                pos += 2;
            }
            else if (arg == "--output" && pos + 1 < command_args.size())
            {
                opts.output = command_args[pos + 1];
                pos += 2;
            }
            else if (arg[0] == '-')
            {
                std::cerr << "Unknown option: " << arg << "\n";
                return 1;
            }
            else
            {
                // Positional: tx_hash
                tx_hash = arg;
                pos++;
            }
        }

        if (tx_hash.empty())
        {
            std::cerr << "Usage: xproof prove <tx_hash> [options]\n";
            return 1;
        }

        opts.tx_hash = tx_hash;
        return cmd_prove(opts, protocol);
    }

    if (command == "verify")
    {
        auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
        if (command_args.size() < 1)
        {
            std::cerr << "Usage: xproof verify <proof_file> [publisher_key]\n";
            return 1;
        }

        std::string proof_path = command_args[0];
        std::string trusted_key = (command_args.size() >= 2)
            ? command_args[1]
            : std::string(DEFAULT_PUBLISHER_KEY);

        return cmd_verify(proof_path, trusted_key, protocol);
    }

    // Unlisted dev commands
    if (command == "dev:check-ledger" && command_args.size() >= 1)
    {
        auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
        return cmd_dev_check_ledger(command_args[0], protocol);
    }

    if (command == "dev:tx" && command_args.size() >= 2)
    {
        auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
        return cmd_dev_tx(command_args[0], command_args[1], protocol);
    }

    print_usage();
    return 1;
}
