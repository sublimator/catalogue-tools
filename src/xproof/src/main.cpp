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
        << "  xproof prove <tx_hash> [options]    build a proof chain\n"
        << "  xproof verify <proof_file> [key]    verify a proof chain\n"
        << "  xproof ping <peer:port>             peer protocol ping\n"
        << "  xproof header <peer:port> <seq>     fetch ledger header\n"
        << "\n"
        << "Options for prove:\n"
        << "  --rpc <host:port>   RPC endpoint (default: "
        << DEFAULT_RPC << ")\n"
        << "  --peer <host:port>  peer endpoint (default: "
        << DEFAULT_PEER << ")\n"
        << "  --binary            output binary format only\n"
        << "  --gzip              output compressed binary format only\n"
        << "  --output <name>     output file base name (default: proof)\n"
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

    if (command == "prove" || command == "prove-tx")
    {
        ProveOptions opts;
        std::string tx_hash;

        int pos = 2;
        while (pos < argc)
        {
            std::string arg = argv[pos];

            if (arg == "--binary")
            {
                opts.binary = true;
                pos++;
            }
            else if (arg == "--gzip")
            {
                opts.binary = true;
                opts.compress = true;
                pos++;
            }
            else if (arg == "--rpc" && pos + 1 < argc)
            {
                opts.rpc_endpoint = argv[pos + 1];
                pos += 2;
            }
            else if (arg == "--peer" && pos + 1 < argc)
            {
                opts.peer_endpoint = argv[pos + 1];
                pos += 2;
            }
            else if (arg == "--output" && pos + 1 < argc)
            {
                opts.output = argv[pos + 1];
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
        if (argc < 3)
        {
            std::cerr << "Usage: xproof verify <proof_file> [publisher_key]\n";
            return 1;
        }

        std::string proof_path = argv[2];
        std::string trusted_key = (argc >= 4)
            ? argv[3]
            : std::string(DEFAULT_PUBLISHER_KEY);

        return cmd_verify(proof_path, trusted_key, protocol);
    }

    // Unlisted dev commands
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
