#include "commands.h"

#include "xproof/proof-chain-binary.h"
#include "xproof/proof-chain-json.h"
#include "xproof/proof-engine.h"
#include "xproof/proof-resolver.h"

#include <catl/core/logger.h>
#include <catl/xdata/pretty_print.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
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

static std::string
default_output_stem(
    uint32_t tx_ledger_seq,
    std::string const& tx_hash,
    std::string const& configured_output)
{
    if (!configured_output.empty())
    {
        return configured_output;
    }

    std::string tx_hash_short =
        tx_hash.substr(0, std::min<size_t>(12, tx_hash.size()));
    for (char& ch : tx_hash_short)
    {
        ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    }

    return "proof-" + std::to_string(tx_ledger_seq) + "-" + tx_hash_short;
}

int
cmd_prove(ProveOptions const& opts, catl::xdata::Protocol const& protocol)
{
    // Build NetworkConfig from CLI options
    xproof::NetworkConfig config;
    config.network_id = opts.network_id;

    if (!opts.rpc_endpoint.empty())
    {
        if (!parse_endpoint(
                opts.rpc_endpoint, config.rpc_host, config.rpc_port))
        {
            std::cerr << "Invalid RPC endpoint: " << opts.rpc_endpoint << "\n";
            return 1;
        }
    }

    if (!opts.peer_endpoint.empty())
    {
        if (!parse_endpoint(
                opts.peer_endpoint, config.peer_host, config.peer_port))
        {
            std::cerr << "Invalid peer endpoint: " << opts.peer_endpoint
                      << "\n";
            return 1;
        }
    }

    config.peer_cache_path = opts.peer_cache_path;
    config.apply_defaults();

    boost::asio::io_context io;
    int result = 1;

    auto engine = xproof::ProofEngine::create(io, std::move(config));
    engine->start();

    boost::asio::co_spawn(
        io,
        [&, engine]() -> boost::asio::awaitable<void> {
            auto prove_result = co_await engine->prove(opts.tx_hash);
            auto& chain = prove_result.chain;

            auto const output_stem = default_output_stem(
                prove_result.tx_ledger_seq, opts.tx_hash, opts.output);

            // Write output files
            std::string json_path, bin_path;
            size_t json_size = 0, bin_size = 0;

            if (opts.binary)
            {
                auto binary =
                    xproof::to_binary(chain, {.compress = opts.compress});
                bin_path = output_stem + ".bin";
                write_file(bin_path, binary);
                bin_size = binary.size();
            }
            else
            {
                auto chain_json = xproof::to_json(chain);
                json_path = output_stem + ".json";
                {
                    std::ofstream out(json_path);
                    catl::xdata::pretty_print(out, chain_json);
                }
                json_size = boost::json::serialize(chain_json).size();

                auto compressed = xproof::to_binary(chain, {.compress = true});
                bin_path = output_stem + ".bin";
                write_file(bin_path, compressed);
                bin_size = compressed.size();
            }

            // Verify using the actual publisher key from the VL fetch
            xproof::resolve_proof_chain(
                chain, protocol, prove_result.publisher_key_hex);

            // Summary
            PLOGI(log_, "");
            PLOGI(log_, "Output:");
            if (!json_path.empty())
            {
                PLOGI(log_, "  ", json_path, " (", json_size, " bytes)");
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

    io.run();
    return result;
}
