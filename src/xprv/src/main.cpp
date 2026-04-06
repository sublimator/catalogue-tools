#include "commands.h"
#include "config.h"

#include <catl/core/logger.h>
#include <catl/core/request-context.h>
#include <catl/peer-client/endpoint-tracker.h>
#include <catl/xdata/protocol.h>
#include <xprv/network-config.h>

#include <cstring>
#include <iostream>
#include <sstream>
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
    bool debug_xprv = false;
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
            logging.debug_xprv = true;
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

    Logger::set_run_id(true);

    // Wire up per-request log prefix via thread_local set by ContextExecutor
    Logger::set_request_context_hook([]() -> Logger::RequestIdView const* {
        static thread_local Logger::RequestIdView view;
        auto const* ctx = catl::core::g_current_request;
        if (!ctx)
            return nullptr;
        view.request_id = ctx->request_id;
        return &view;
    });

    set_exact("xprv", LogLevel::INFO);
    set_exact("val-buffer", LogLevel::INFO);
    set_exact("verify", LogLevel::INFO);
    set_exact("anchor-verify", LogLevel::INFO);

    set_exact("node-cache", LogLevel::WARNING);
    set_exact("peer-cache", LogLevel::WARNING);
    set_exact("peer-client", LogLevel::WARNING);
    set_exact("peer-conn", LogLevel::WARNING);
    set_exact("peer-crawl", LogLevel::WARNING);
    set_exact("peer-set", LogLevel::WARNING);
    set_exact("tree-walk", LogLevel::WARNING);
    set_exact("rpc-client", LogLevel::WARNING);
    set_exact("vl-cache", LogLevel::WARNING);
    set_exact("vl-client", LogLevel::WARNING);

    if (opts.debug_xprv)
    {
        set_exact("xprv", LogLevel::DEBUG);
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

    // Log effective levels for all partitions at startup.
    // Catches silent INHERIT→ERROR bugs where a new partition
    // was added but not registered in configure_logging().
    // Uses log_internal directly since global level is still ERROR
    // at this point and LOGI would be suppressed.
    {
        auto partitions = Logger::partition_info();
        std::ostringstream oss;
        oss << "Log partitions:";
        for (auto const& p : partitions)
        {
            auto level_str = [](LogLevel l) -> char const* {
                switch (l)
                {
                    case LogLevel::ERROR: return "ERROR";
                    case LogLevel::WARNING: return "WARN";
                    case LogLevel::INFO: return "INFO";
                    case LogLevel::DEBUG: return "DEBUG";
                    case LogLevel::TRACE: return "TRACE";
                    case LogLevel::NONE: return "NONE";
                    case LogLevel::INHERIT: return "INHERIT";
                    default: return "?";
                }
            };
            oss << " " << p.name << "=" << level_str(p.effective);
            if (p.configured == LogLevel::INHERIT)
                oss << "(inherit)";
        }
        Logger::log_internal(LogLevel::INFO, oss.str());
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
        << "xprv - XRPL Proof Chain Tool\n"
        << "\n"
        << "Usage:\n"
        << "  xprv prove <tx_hash> [options]    build a proof chain\n"
        << "  xprv verify <proof_file> [key]    verify a proof chain\n"
        << "  xprv serve [options]              run HTTP proof server\n"
        << "  xprv ping <peer:port>             peer protocol ping\n"
        << "  xprv header <peer:port> <seq>     fetch ledger header\n"
        << "\n"
        << "Options for prove/serve:\n"
        << "  --rpc <host:port>   RPC endpoint (default: " << DEFAULT_RPC
        << ")\n"
        << "  --peer <host:port>  peer endpoint (default: " << DEFAULT_PEER
        << ")\n"
        << "  --network <id>      network ID (0=XRPL, 21337=Xahau)\n"
        << "\n"
        << "Options for prove:\n"
        << "  --binary            output binary format only\n"
        << "  --gzip              output compressed binary format only\n"
        << "  --output <stem>     output file stem (default: "
           "proof-<ledger>-<txid12>)\n"
        << "\n"
        << "Options for serve:\n"
        << "  --bind <addr:port>  listen address (default: 127.0.0.1:8080)\n"
        << "  --threads <N>       I/O threads (default: 1)\n"
        << "\n"
        << "Logging options:\n"
        << "  --debug                 enable DEBUG for xprv partitions\n"
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
    // Raise fd limit for serve mode (deferred — done in cmd_serve).
    // Single-shot commands don't need elevated limits.

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
            else if (arg == "--network" && pos + 1 < command_args.size())
            {
                opts.network_id = std::stoul(command_args[pos + 1]);
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
            std::cerr << "Usage: xprv prove <tx_hash> [options]\n";
            return 1;
        }

        opts.tx_hash = tx_hash;
        auto net_config = xprv::NetworkConfig::for_network(opts.network_id);
        auto protocol = net_config.load_protocol();
        return cmd_prove(opts, protocol);
    }

    if (command == "verify")
    {
        std::string proof_path;
        std::string explicit_key;
        uint32_t verify_network_id = 0;

        std::size_t pos = 0;
        while (pos < command_args.size())
        {
            std::string const& arg = command_args[pos];
            if (arg == "--network" && pos + 1 < command_args.size())
            {
                verify_network_id = std::stoul(command_args[pos + 1]);
                pos += 2;
            }
            else if (arg[0] == '-')
            {
                std::cerr << "Unknown option: " << arg << "\n";
                return 1;
            }
            else if (proof_path.empty())
            {
                proof_path = arg;
                pos++;
            }
            else
            {
                explicit_key = arg;
                pos++;
            }
        }

        if (proof_path.empty())
        {
            std::cerr
                << "Usage: xprv verify <proof_file> [publisher_key] "
                   "[--network N]\n";
            return 1;
        }

        // Resolve publisher key: explicit arg > network default
        std::string trusted_key;
        if (!explicit_key.empty())
        {
            trusted_key = explicit_key;
        }
        else
        {
            auto net_config =
                xprv::NetworkConfig::for_network(verify_network_id);
            trusted_key = net_config.publisher_key;
        }

        auto net_config =
            xprv::NetworkConfig::for_network(verify_network_id);
        auto protocol = net_config.load_protocol();
        return cmd_verify(proof_path, trusted_key, protocol);
    }

    if (command == "serve")
    {
        // CLI flags applied as env vars — Config layering handles the rest.
        // This keeps main.cpp thin: just map flags → env → call cmd_serve().
        std::size_t pos = 0;
        while (pos < command_args.size())
        {
            std::string const& arg = command_args[pos];

            if (arg == "--bind" && pos + 1 < command_args.size())
            {
                auto bind_str = command_args[pos + 1];
                auto colon = bind_str.rfind(':');
                if (colon != std::string::npos)
                {
                    setenv("XPRV_BIND", bind_str.substr(0, colon).c_str(), 1);
                    setenv("XPRV_PORT", bind_str.substr(colon + 1).c_str(), 1);
                }
                else
                {
                    setenv("XPRV_BIND", bind_str.c_str(), 1);
                }
                pos += 2;
            }
            else if (arg == "--network" && pos + 1 < command_args.size())
            {
                setenv("XPRV_NETWORK_ID", command_args[pos + 1].c_str(), 1);
                pos += 2;
            }
            else if (arg == "--threads" && pos + 1 < command_args.size())
            {
                setenv("XPRV_THREADS", command_args[pos + 1].c_str(), 1);
                pos += 2;
            }
            else if (arg == "--rpc" && pos + 1 < command_args.size())
            {
                // Parse host:port into separate env vars
                std::string host;
                uint16_t port;
                if (parse_endpoint(command_args[pos + 1], host, port))
                {
                    setenv("XPRV_RPC_HOST", host.c_str(), 1);
                    setenv("XPRV_RPC_PORT", std::to_string(port).c_str(), 1);
                }
                pos += 2;
            }
            else if (arg == "--peer" && pos + 1 < command_args.size())
            {
                std::string host;
                uint16_t port;
                if (parse_endpoint(command_args[pos + 1], host, port))
                {
                    setenv("XPRV_PEER_HOST", host.c_str(), 1);
                    setenv("XPRV_PEER_PORT", std::to_string(port).c_str(), 1);
                }
                pos += 2;
            }
            else if (arg == "--no-cache")
            {
                setenv("XPRV_NO_CACHE", "1", 1);
                ++pos;
            }
            else if (arg == "--node-cache-size" && pos + 1 < command_args.size())
            {
                setenv("XPRV_NODE_CACHE_SIZE", command_args[pos + 1].c_str(), 1);
                pos += 2;
            }
            else if (
                (arg == "--peer-cache" || arg == "--peer-cache-path") &&
                pos + 1 < command_args.size())
            {
                setenv("XPRV_PEER_CACHE_PATH", command_args[pos + 1].c_str(), 1);
                pos += 2;
            }
            else if (arg == "--fd-limit" && pos + 1 < command_args.size())
            {
                setenv("XPRV_FD_LIMIT", command_args[pos + 1].c_str(), 1);
                pos += 2;
            }
            else if (arg[0] == '-')
            {
                std::cerr << "Unknown option: " << arg << "\n";
                return 1;
            }
            else
            {
                std::cerr << "Unexpected argument: " << arg << "\n";
                return 1;
            }
        }

        return cmd_serve();
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
