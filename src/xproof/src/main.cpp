#include "commands.h"

#include <catl/xdata/protocol.h>

#include <cstring>
#include <iostream>
#include <string>

bool
parse_endpoint(std::string const& endpoint, std::string& host, uint16_t& port)
{
    auto colon = endpoint.rfind(':');
    if (colon == std::string::npos)
        return false;
    host = endpoint.substr(0, colon);
    port = static_cast<uint16_t>(std::stoul(endpoint.substr(colon + 1)));
    return true;
}

static void
print_usage()
{
    std::cerr
        << "xproof - XRPL Proof Chain Tool\n"
        << "\n"
        << "Usage:\n"
        << "  xproof prove-tx [options] <rpc:port> <peer:port> <tx_hash>\n"
        << "  xproof verify <proof_file> <publisher_key>\n"
        << "  xproof ping <peer:port>\n"
        << "  xproof header <peer:port> <ledger_seq>\n"
        << "\n"
        << "Options for prove-tx:\n"
        << "  --binary          output binary format (default: JSON)\n"
        << "  --gzip            output binary+zlib compressed format\n"
        << "  --output <name>   output file base name (default: proof)\n"
        << "\n"
        << "Dev commands:\n"
        << "  xproof dev:check-ledger <peer:port>\n"
        << "  xproof dev:tx <rpc:port> <tx_hash>\n"
        << "\n"
        << "The verify command auto-detects JSON vs binary format.\n"
        << "Peer port is typically 51235. RPC port is 443 or 51234.\n"
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

    std::string command = argv[1];
    auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();

    if (command == "ping" && argc >= 3)
    {
        return cmd_ping(argv[2]);
    }

    if (command == "header" && argc >= 4)
    {
        return cmd_header(argv[2], std::stoul(argv[3]));
    }

    if (command == "prove-tx")
    {
        ProveOptions opts;

        // Parse options before positional args
        int pos = 2;
        while (pos < argc && argv[pos][0] == '-')
        {
            if (std::strcmp(argv[pos], "--binary") == 0)
            {
                opts.binary = true;
                pos++;
            }
            else if (std::strcmp(argv[pos], "--gzip") == 0)
            {
                opts.binary = true;
                opts.compress = true;
                pos++;
            }
            else if (std::strcmp(argv[pos], "--output") == 0 && pos + 1 < argc)
            {
                opts.output = argv[pos + 1];
                pos += 2;
            }
            else
            {
                std::cerr << "Unknown option: " << argv[pos] << "\n";
                return 1;
            }
        }

        if (pos + 3 > argc)
        {
            std::cerr << "Usage: xproof prove-tx [options] <rpc:port> "
                         "<peer:port> <tx_hash>\n";
            return 1;
        }

        opts.rpc_endpoint = argv[pos];
        opts.peer_endpoint = argv[pos + 1];
        opts.tx_hash = argv[pos + 2];

        return cmd_prove(opts, protocol);
    }

    if (command == "verify" && argc >= 4)
    {
        return cmd_verify(argv[2], argv[3], protocol);
    }

    if (command == "dev:check-ledger" && argc >= 3)
    {
        return cmd_dev_check_ledger(argv[2], protocol);
    }

    if (command == "dev:tx" && argc >= 4)
    {
        return cmd_dev_tx(argv[2], argv[3], protocol);
    }

    print_usage();
    return 1;
}
