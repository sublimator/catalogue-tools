#include "commands.h"

#include "xproof/http-server.h"
#include "xproof/network-config.h"
#include "xproof/proof-engine.h"

#include <catl/core/logger.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <iostream>

static LogPartition log_("xproof", LogLevel::INFO);

int
cmd_serve(ServeOptions const& opts)
{
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

    auto engine = xproof::ProofEngine::create(io, std::move(config));
    engine->start();

    xproof::HttpServerOptions http_opts;
    http_opts.bind_address = opts.bind_address;
    http_opts.port = opts.port;

    auto server = xproof::HttpServer::create(io, engine, http_opts);
    server->start();

    // SIGINT/SIGTERM → stop accepting, let in-flight drain
    boost::asio::signal_set signals(io, SIGINT, SIGTERM);
    signals.async_wait([&](boost::system::error_code, int sig) {
        PLOGI(log_, "Signal ", sig, " received, shutting down...");
        server->stop();
    });

    io.run();
    return 0;
}
