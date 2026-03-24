#include "commands.h"

#include "xproof/hex-utils.h"
#include "xproof/proof-builder.h"
#include "xproof/proof-chain-binary.h"
#include "xproof/proof-chain-json.h"
#include "xproof/proof-resolver.h"

#include <catl/core/logger.h>
#include <catl/xdata/pretty_print.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <fstream>
#include <iostream>

static LogPartition log_("xproof", LogLevel::INFO);

static void
write_file(std::string const& path, std::vector<uint8_t> const& data)
{
    std::ofstream out(path, std::ios::binary);
    out.write(
        reinterpret_cast<const char*>(data.data()),
        static_cast<std::streamsize>(data.size()));
    out.close();
}

int
cmd_prove(ProveOptions const& opts, catl::xdata::Protocol const& protocol)
{
    std::string rpc_host, peer_host;
    uint16_t rpc_port, peer_port;
    if (!parse_endpoint(opts.rpc_endpoint, rpc_host, rpc_port) ||
        !parse_endpoint(opts.peer_endpoint, peer_host, peer_port))
    {
        std::cerr << "Invalid endpoint(s)\n";
        return 1;
    }

    boost::asio::io_context io;
    int result = 1;

    boost::asio::co_spawn(
        io,
        [&]() -> boost::asio::awaitable<void> {
            auto build_result = co_await xproof::build_proof(
                io, rpc_host, rpc_port, peer_host, peer_port, opts.tx_hash);

            // Write output files
            std::string json_path, bin_path;
            size_t json_size = 0, bin_size = 0;

            if (opts.binary)
            {
                // Binary only
                auto binary = xproof::to_binary(
                    build_result.chain, {.compress = opts.compress});
                bin_path = opts.output + ".bin";
                write_file(bin_path, binary);
                bin_size = binary.size();
            }
            else
            {
                // JSON to stdout + file
                auto chain_json = xproof::to_json(build_result.chain);
                catl::xdata::pretty_print(std::cout, chain_json);

                json_path = opts.output + ".json";
                {
                    std::ofstream out(json_path);
                    catl::xdata::pretty_print(out, chain_json);
                }
                json_size = boost::json::serialize(chain_json).size();

                // Also write compressed binary alongside
                auto compressed =
                    xproof::to_binary(build_result.chain, {.compress = true});
                bin_path = opts.output + ".bin";
                write_file(bin_path, compressed);
                bin_size = compressed.size();
            }

            // Verify
            xproof::resolve_proof_chain(
                build_result.chain, protocol, build_result.publisher_key_hex);

            // Summary at the end
            PLOGI(log_, "");
            PLOGI(log_, "Output:");
            if (!json_path.empty())
            {
                PLOGI(
                    log_, "  ", json_path, " (", json_size, " bytes)");
            }
            if (!bin_path.empty())
            {
                PLOGI(
                    log_,
                    "  ",
                    bin_path,
                    " (",
                    bin_size,
                    " bytes",
                    opts.compress || json_path.empty() ? "" : ", zlib",
                    ")");
            }

            result = 0;
        },
        [&](std::exception_ptr ep) {
            if (ep)
            {
                try
                {
                    std::rethrow_exception(ep);
                }
                catch (std::exception const& e)
                {
                    PLOGE(log_, "Fatal: ", e.what());
                }
            }
            io.stop();
        });

    // No outer timeout — peer discovery and wait_for_peer have their
    // own timeouts. Use Ctrl-C to cancel.

    io.run();
    return result;
}
