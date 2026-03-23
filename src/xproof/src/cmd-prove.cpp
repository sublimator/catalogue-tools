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

            if (opts.binary)
            {
                auto binary = xproof::to_binary(
                    build_result.chain, {.compress = opts.compress});
                std::string path = opts.output + ".bin";
                write_file(path, binary);
                PLOGI(
                    log_,
                    "Wrote binary proof: ",
                    path,
                    " (",
                    binary.size(),
                    " bytes",
                    opts.compress ? ", zlib compressed" : "",
                    ")");
            }
            else
            {
                // JSON to stdout + file
                auto chain_json = xproof::to_json(build_result.chain);
                catl::xdata::pretty_print(std::cout, chain_json);

                std::string json_path = opts.output + ".json";
                {
                    std::ofstream out(json_path);
                    catl::xdata::pretty_print(out, chain_json);
                }
                PLOGI(log_, "Wrote JSON proof: ", json_path);

                // Also write binary alongside for comparison
                auto binary = xproof::to_binary(build_result.chain);
                auto compressed =
                    xproof::to_binary(build_result.chain, {.compress = true});
                std::string bin_path = opts.output + ".bin";
                write_file(bin_path, compressed);

                PLOGI(
                    log_,
                    "Wrote binary proof: ",
                    bin_path,
                    " (binary=",
                    binary.size(),
                    ", zlib=",
                    compressed.size(),
                    " bytes)");
            }

            // Verify
            xproof::resolve_proof_chain(
                build_result.chain, protocol, build_result.publisher_key_hex);

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

    boost::asio::steady_timer timer(io, std::chrono::seconds(60));
    timer.async_wait([&](boost::system::error_code) {
        PLOGE(log_, "Timeout");
        io.stop();
    });

    io.run();
    return result;
}
