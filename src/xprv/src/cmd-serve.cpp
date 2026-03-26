#include "commands.h"
#include "config.h"

#include "xprv/http-server.h"
#include "xprv/proof-engine.h"

#include <catl/core/logger.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <iostream>
#include <pthread.h>
#include <sys/resource.h>
#include <thread>
#include <vector>

static LogPartition log_("xprv", LogLevel::INFO);

int
cmd_serve()
{
    // Load full config: defaults → config.toml → env vars
    // CLI flags are applied by main.cpp before calling us.
    auto config = xprv::load_config();

    // Dump resolved config
    xprv::dump_config(config, std::cerr);

    // Raise fd limit for server mode
    if (config.fd_limit > 0)
    {
        struct rlimit rl;
        if (getrlimit(RLIMIT_NOFILE, &rl) == 0)
        {
            auto old_soft = rl.rlim_cur;
            rl.rlim_cur = std::min<rlim_t>(rl.rlim_max, config.fd_limit);
            if (setrlimit(RLIMIT_NOFILE, &rl) == 0 && old_soft != rl.rlim_cur)
            {
                PLOGI(log_, "fd limit: ", old_soft, " → ", rl.rlim_cur);
            }
        }
    }

    auto net_config = xprv::to_network_config(config);
    boost::asio::io_context io;

    auto engine = xprv::ProofEngine::create(io, std::move(net_config));
    if (config.no_cache)
        engine->set_cache_enabled(false);
    engine->set_node_cache_size(config.node_cache_size);
    engine->set_fetch_timeout(config.fetch_timeout_secs);
    engine->set_max_walk_peer_retries(config.max_walk_peer_retries);
    engine->set_fetch_stale_multiplier(config.fetch_stale_multiplier);
    engine->set_rpc_max_concurrent(config.rpc_max_concurrent);
    engine->start();

    xprv::HttpServerOptions http_opts;
    http_opts.bind_address = config.bind_address;
    http_opts.port = config.port;

    auto server = xprv::HttpServer::create(io, engine, http_opts);
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
    auto const n_threads = config.threads;
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
        threads.emplace_back(
            std::thread([tid]() { pthread_join(tid, nullptr); }));
    }

    io.run();  // main thread participates

    for (auto& t : threads)
    {
        t.join();
    }

    return 0;
}
