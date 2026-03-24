#include "commands.h"

#include "xproof/http-server.h"
#include "xproof/network-config.h"
#include "xproof/proof-engine.h"

#include <catl/core/logger.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <iostream>
#include <pthread.h>
#include <thread>
#include <vector>

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
    if (opts.no_cache)
    {
        engine->set_cache_enabled(false);
    }
    engine->start();

    xproof::HttpServerOptions http_opts;
    http_opts.bind_address = opts.bind_address;
    http_opts.port = opts.port;

    auto server = xproof::HttpServer::create(io, engine, http_opts);
    server->start();

    // SIGINT/SIGTERM → stop server, stop engine, stop io
    boost::asio::signal_set signals(io, SIGINT, SIGTERM);
    signals.async_wait([&](boost::system::error_code, int sig) {
        PLOGI(log_, "Signal ", sig, " received, shutting down...");
        server->stop();
        engine->stop();
        io.stop();
    });

    // Run io_context on N threads
    auto const n_threads = opts.threads;
    if (n_threads > 1)
    {
        PLOGI(log_, "Running with ", n_threads, " threads");
    }

    // Worker threads need enough stack for secp256k1 signature
    // verification (deep scalar math) + boost::json + SHAMap
    // reconstruction. std::thread defaults to 512KB on macOS
    // which overflows. Use pthread for explicit 8MB stack.
    std::vector<std::thread> threads;
    for (unsigned int i = 1; i < n_threads; ++i)
    {
        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setstacksize(&attr, 8 * 1024 * 1024);
        pthread_create(
            &tid,
            &attr,
            [](void* arg) -> void* {
                static_cast<boost::asio::io_context*>(arg)->run();
                return nullptr;
            },
            &io);
        pthread_attr_destroy(&attr);
        threads.emplace_back(std::thread([tid]() {
            pthread_join(tid, nullptr);
        }));
    }

    io.run();  // main thread participates

    for (auto& t : threads)
    {
        t.join();
    }

    return 0;
}
