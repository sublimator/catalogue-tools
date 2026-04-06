#include "commands.h"
#include "config.h"

#include "xprv/http-server.h"
#include "xprv/proof-engine.h"

#include <catl/core/logger.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <pthread.h>
#include <sstream>
#include <sys/resource.h>
#include <thread>
#include <vector>

#ifdef __APPLE__
#include <mach/mach.h>
#endif

static LogPartition log_("xprv", LogLevel::INFO);

namespace {

std::string
network_label_for_id(uint32_t network_id)
{
    switch (network_id)
    {
        case 0:
            return "xrpl-mainnet";
        case 21337:
            return "xahau-mainnet";
        default:
            return std::to_string(network_id);
    }
}

std::size_t
bucketize(std::size_t value, std::size_t bucket_size)
{
    return bucket_size == 0 ? value : value / bucket_size;
}

struct LoggedNetworkStatus
{
    std::string label;
    std::size_t known_endpoints = 0;
    std::size_t tracked_endpoints = 0;
    std::size_t connected_peers = 0;
    std::size_t ready_peers = 0;
    std::size_t in_flight_connects = 0;
    std::size_t queued_connects = 0;
    std::size_t crawl_in_flight = 0;
    std::size_t queued_crawls = 0;
    bool vl_loaded = false;
    bool has_quorum = false;
    uint32_t latest_quorum_seq = 0;
    std::size_t validation_waiters = 0;
    std::size_t recent_quorums = 0;
    std::size_t collector_ledgers = 0;
    std::size_t collector_validations = 0;
    std::size_t proof_cache_entries = 0;
    std::size_t node_cache_entries = 0;
    std::size_t node_cache_resident_entries = 0;
    std::size_t node_cache_header_entries = 0;
    std::string recent_failures;
    std::string top_failing_endpoints;
};

struct LoggedNetworkTrigger
{
    std::size_t known_endpoints_bucket = 0;
    std::size_t tracked_endpoints_bucket = 0;
    std::size_t connected_peers = 0;
    std::size_t ready_peers = 0;
    std::size_t in_flight_connects = 0;
    std::size_t queued_connects = 0;
    std::size_t crawl_in_flight = 0;
    std::size_t queued_crawls = 0;
    bool vl_loaded = false;
    bool has_quorum = false;
    uint32_t latest_quorum_bucket = 0;
    std::size_t validation_waiters = 0;
    std::size_t recent_quorums = 0;
    std::size_t collector_ledgers_bucket = 0;
    std::size_t collector_validations_bucket = 0;
    std::size_t proof_cache_entries_bucket = 0;
    std::size_t node_cache_entries_bucket = 0;
    std::size_t node_cache_resident_entries_bucket = 0;
    std::size_t node_cache_header_entries_bucket = 0;
    std::string recent_failures;
    std::string top_failing_endpoints;
};

struct LoggedStatusState
{
    bool initialized = false;
    int rss_bucket = -1;
    std::map<uint32_t, LoggedNetworkTrigger> networks;
};

bool
same_trigger(
    LoggedNetworkTrigger const& lhs,
    LoggedNetworkTrigger const& rhs)
{
    return lhs.known_endpoints_bucket == rhs.known_endpoints_bucket &&
        lhs.tracked_endpoints_bucket == rhs.tracked_endpoints_bucket &&
        lhs.connected_peers == rhs.connected_peers &&
        lhs.ready_peers == rhs.ready_peers &&
        lhs.in_flight_connects == rhs.in_flight_connects &&
        lhs.queued_connects == rhs.queued_connects &&
        lhs.crawl_in_flight == rhs.crawl_in_flight &&
        lhs.queued_crawls == rhs.queued_crawls &&
        lhs.vl_loaded == rhs.vl_loaded &&
        lhs.has_quorum == rhs.has_quorum &&
        lhs.latest_quorum_bucket == rhs.latest_quorum_bucket &&
        lhs.validation_waiters == rhs.validation_waiters &&
        lhs.recent_quorums == rhs.recent_quorums &&
        lhs.collector_ledgers_bucket == rhs.collector_ledgers_bucket &&
        lhs.collector_validations_bucket ==
            rhs.collector_validations_bucket &&
        lhs.proof_cache_entries_bucket == rhs.proof_cache_entries_bucket &&
        lhs.node_cache_entries_bucket == rhs.node_cache_entries_bucket &&
        lhs.node_cache_resident_entries_bucket ==
            rhs.node_cache_resident_entries_bucket &&
        lhs.node_cache_header_entries_bucket ==
            rhs.node_cache_header_entries_bucket &&
        lhs.recent_failures == rhs.recent_failures &&
        lhs.top_failing_endpoints == rhs.top_failing_endpoints;
}

std::string
format_failure_buckets(
    std::vector<catl::peer_client::PeerSet::Snapshot::FailureBucket> const&
        buckets,
    std::size_t limit = 3)
{
    std::ostringstream out;
    auto const count = std::min(limit, buckets.size());
    for (std::size_t i = 0; i < count; ++i)
    {
        if (i > 0)
            out << ',';
        out << buckets[i].kind << 'x' << buckets[i].count;
    }
    return out.str();
}

std::string
format_failure_endpoints(
    std::vector<catl::peer_client::PeerSet::Snapshot::FailureEndpoint> const&
        endpoints,
    std::size_t limit = 2)
{
    std::ostringstream out;
    auto const count = std::min(limit, endpoints.size());
    for (std::size_t i = 0; i < count; ++i)
    {
        if (i > 0)
            out << ',';
        out << endpoints[i].endpoint << 'x' << endpoints[i].count;
    }
    return out.str();
}

std::string
format_network_status(LoggedNetworkStatus const& status)
{
    std::ostringstream out;
    out << '[' << status.label << " peers=" << status.ready_peers << '/'
        << status.connected_peers << " dial=q" << status.queued_connects
        << "/f" << status.in_flight_connects << " crawl=q"
        << status.queued_crawls << "/f" << status.crawl_in_flight
        << " known=" << status.known_endpoints
        << " tracked=" << status.tracked_endpoints << " quorum=";
    if (status.has_quorum)
        out << status.latest_quorum_seq;
    else
        out << "none";
    out << " vl=" << (status.vl_loaded ? "loaded" : "empty")
        << " valbuf=w" << status.validation_waiters << "/q"
        << status.recent_quorums << "/l" << status.collector_ledgers
        << "/v" << status.collector_validations
        << " proofs=" << status.proof_cache_entries << " node_cache=e"
        << status.node_cache_entries << "/r"
        << status.node_cache_resident_entries << "/h"
        << status.node_cache_header_entries;
    if (!status.recent_failures.empty())
    {
        out << " fail=" << status.recent_failures;
        if (!status.top_failing_endpoints.empty())
            out << " top=" << status.top_failing_endpoints;
    }
    out << ']';
    return out.str();
}

void
log_status_if_changed(
    size_t rss_bytes,
    std::map<uint32_t, LoggedNetworkStatus> const& networks,
    LoggedStatusState& previous)
{
    auto const rss_mb =
        static_cast<int>(rss_bytes / (1024 * 1024));
    auto const rss_bucket = rss_mb / 10;

    std::map<uint32_t, LoggedNetworkTrigger> current;
    for (auto const& [network_id, status] : networks)
    {
        current.emplace(
            network_id,
            LoggedNetworkTrigger{
                .known_endpoints_bucket =
                    bucketize(status.known_endpoints, 32),
                .tracked_endpoints_bucket =
                    bucketize(status.tracked_endpoints, 32),
                .connected_peers = status.connected_peers,
                .ready_peers = status.ready_peers,
                .in_flight_connects = status.in_flight_connects,
                .queued_connects = status.queued_connects,
                .crawl_in_flight = status.crawl_in_flight,
                .queued_crawls = status.queued_crawls,
                .vl_loaded = status.vl_loaded,
                .has_quorum = status.has_quorum,
                .latest_quorum_bucket =
                    status.has_quorum ? (status.latest_quorum_seq / 64) : 0,
                .validation_waiters = status.validation_waiters,
                .recent_quorums = status.recent_quorums,
                .collector_ledgers_bucket =
                    bucketize(status.collector_ledgers, 8),
                .collector_validations_bucket =
                    bucketize(status.collector_validations, 32),
                .proof_cache_entries_bucket =
                    bucketize(status.proof_cache_entries, 8),
                .node_cache_entries_bucket =
                    bucketize(status.node_cache_entries, 32),
                .node_cache_resident_entries_bucket =
                    bucketize(status.node_cache_resident_entries, 32),
                .node_cache_header_entries_bucket =
                    bucketize(status.node_cache_header_entries, 8),
                .recent_failures = status.recent_failures,
                .top_failing_endpoints = status.top_failing_endpoints,
            });
    }

    bool changed = !previous.initialized || rss_bucket != previous.rss_bucket ||
        current.size() != previous.networks.size();
    if (!changed)
    {
        for (auto const& [network_id, trigger] : current)
        {
            auto it = previous.networks.find(network_id);
            if (it == previous.networks.end() ||
                !same_trigger(it->second, trigger))
            {
                changed = true;
                break;
            }
        }
    }

    if (!changed)
        return;

    previous.initialized = true;
    previous.rss_bucket = rss_bucket;
    previous.networks = std::move(current);

    std::ostringstream line;
    line << "status: RSS=" << rss_mb << "MB";
    for (auto const& [_, status] : networks)
    {
        line << ' ' << format_network_status(status);
    }
    PLOGI(log_, line.str());
}

}  // namespace

/// Get current process RSS in bytes. Returns 0 on failure.
static size_t
get_rss_bytes()
{
#ifdef __APPLE__
    mach_task_basic_info_data_t info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(
            mach_task_self(), MACH_TASK_BASIC_INFO,
            reinterpret_cast<task_info_t>(&info), &count) == KERN_SUCCESS)
    {
        return info.resident_size;
    }
    return 0;
#else
    // Linux: read /proc/self/status → VmRSS
    std::ifstream f("/proc/self/status");
    std::string line;
    while (std::getline(f, line))
    {
        if (line.compare(0, 6, "VmRSS:") == 0)
        {
            // "VmRSS:    12345 kB"
            size_t kb = 0;
            std::sscanf(line.c_str(), "VmRSS: %zu", &kb);
            return kb * 1024;
        }
    }
    return 0;
#endif
}

static void
log_memory(char const* label)
{
    auto rss = get_rss_bytes();
    if (rss == 0)
        return;

    auto mb = static_cast<double>(rss) / (1024.0 * 1024.0);
    PLOGI(log_, label, ": RSS=", static_cast<int>(mb), "MB");
}

/// Create and configure a ProofEngine for a specific network, using
/// the shared (non-network) settings from config.
static std::shared_ptr<xprv::ProofEngine>
make_engine(
    boost::asio::io_context& io,
    xprv::Config const& config,
    uint32_t network_id)
{
    auto net_config = xprv::NetworkConfig::for_network(network_id);

    // If this is the primary network, use any user-overridden endpoints
    if (network_id == config.network_id)
    {
        net_config = xprv::to_network_config(config);
    }

    auto engine = xprv::ProofEngine::create(io, std::move(net_config));
    if (config.no_cache)
        engine->set_cache_enabled(false);
    engine->set_node_cache_size(config.node_cache_size);
    engine->set_fetch_timeout(config.fetch_timeout);
    engine->set_max_walk_peer_retries(config.max_walk_peer_retries);
    engine->set_fetch_stale_multiplier(config.fetch_stale_multiplier);
    engine->set_rpc_max_concurrent(config.rpc_max_concurrent);
    return engine;
}

int
cmd_serve()
{
    // Load full config: defaults → config.toml → env vars
    // CLI flags are applied by main.cpp before calling us.
    auto config = xprv::load_config();

    // Dump resolved config
    xprv::dump_config(config, std::cerr);

    // Raise fd limit for server mode (only increase, never lower)
    if (config.fd_limit > 0)
    {
        struct rlimit rl;
        if (getrlimit(RLIMIT_NOFILE, &rl) == 0)
        {
            auto old_soft = rl.rlim_cur;
            auto target = std::min<rlim_t>(rl.rlim_max, config.fd_limit);
            if (target > old_soft)
            {
                rl.rlim_cur = target;
                if (setrlimit(RLIMIT_NOFILE, &rl) == 0)
                {
                    PLOGI(log_, "fd limit: ", old_soft, " → ", rl.rlim_cur);
                }
            }
        }
    }

    boost::asio::io_context io;

    // Create engines for all enabled networks
    std::map<uint32_t, std::shared_ptr<xprv::ProofEngine>> engines;
    for (auto network_id : config.enabled_networks)
    {
        PLOGI(
            log_,
            "Creating engine for network ",
            network_id,
            " (",
            xprv::NetworkConfig::for_network(network_id).peer_host,
            ")");
        engines[network_id] = make_engine(io, config, network_id);
    }

    // Start all engines
    for (auto& [net_id, engine] : engines)
    {
        engine->start();
    }

    xprv::HttpServerOptions http_opts;
    http_opts.bind_address = config.bind_address;
    http_opts.port = config.port;
#ifdef XPRV_BUILD_ID
    http_opts.build_id = XPRV_BUILD_ID;
#endif

    auto server = (engines.size() == 1)
        ? xprv::HttpServer::create(
              io, engines.begin()->second, http_opts)
        : xprv::HttpServer::create(
              io, engines, config.network_id, http_opts);
    server->start();

    log_memory("startup");

    auto shutting_down = std::make_shared<std::atomic_bool>(false);
    auto status_timer = std::make_shared<boost::asio::steady_timer>(io);
    auto status_state = std::make_shared<LoggedStatusState>();
    std::function<void()> schedule_status_report;
    schedule_status_report =
        [&io,
         &engines,
         shutting_down,
         status_timer,
         status_state,
         &schedule_status_report]() {
            if (shutting_down->load())
                return;

            status_timer->expires_after(std::chrono::seconds(60));
            status_timer->async_wait(
                [&io,
                 &engines,
                 shutting_down,
                 status_timer,
                 status_state,
                 &schedule_status_report](boost::system::error_code ec) {
                    if (ec || shutting_down->load())
                        return;

                    boost::asio::co_spawn(
                        io,
                        [&engines,
                         shutting_down,
                         status_state,
                         &schedule_status_report]()
                            -> boost::asio::awaitable<void> {
                            try
                            {
                                if (!shutting_down->load())
                                {
                                    auto const rss = get_rss_bytes();
                                    if (rss != 0)
                                    {
                                        std::map<uint32_t, LoggedNetworkStatus>
                                            networks;
                                        for (auto const& [network_id, engine] :
                                             engines)
                                        {
                                            auto peer_snapshot =
                                                co_await engine->peers()
                                                    ->co_snapshot();
                                            auto health =
                                                co_await engine->co_health();
                                            auto proof_cache =
                                                engine->cache_stats();
                                            auto node_cache =
                                                engine->node_cache_stats();

                                            LoggedNetworkStatus status;
                                            status.label =
                                                network_label_for_id(
                                                    network_id);
                                            status.known_endpoints =
                                                peer_snapshot.known_endpoints;
                                            status.tracked_endpoints =
                                                peer_snapshot.tracked_endpoints;
                                            status.connected_peers =
                                                peer_snapshot.connected_peers;
                                            status.ready_peers =
                                                peer_snapshot.ready_peers;
                                            status.in_flight_connects =
                                                peer_snapshot
                                                    .in_flight_connects;
                                            status.queued_connects =
                                                peer_snapshot.queued_connects;
                                            status.crawl_in_flight =
                                                peer_snapshot.crawl_in_flight;
                                            status.queued_crawls =
                                                peer_snapshot.queued_crawls;
                                            status.vl_loaded = health.vl_loaded;
                                            status.has_quorum =
                                                health.latest_quorum_seq
                                                    .has_value();
                                            status.latest_quorum_seq =
                                                health.latest_quorum_seq
                                                    .value_or(0);
                                            status.validation_waiters =
                                                health.validation_buffer
                                                    .waiters;
                                            status.recent_quorums =
                                                health.validation_buffer
                                                    .recent_quorums;
                                            status.collector_ledgers =
                                                health.validation_buffer
                                                    .collector_ledgers;
                                            status.collector_validations =
                                                health.validation_buffer
                                                    .collector_validations;
                                            status.proof_cache_entries =
                                                proof_cache.entries;
                                            status.node_cache_entries =
                                                node_cache.entries;
                                            status.node_cache_resident_entries =
                                                node_cache.resident_entries;
                                            status.node_cache_header_entries =
                                                node_cache.header_entries;
                                            status.recent_failures =
                                                format_failure_buckets(
                                                    peer_snapshot
                                                        .recent_failures);
                                            status.top_failing_endpoints =
                                                format_failure_endpoints(
                                                    peer_snapshot
                                                        .top_failing_endpoints);
                                            networks.emplace(
                                                network_id, std::move(status));
                                        }

                                        log_status_if_changed(
                                            rss, networks, *status_state);
                                    }
                                }
                            }
                            catch (std::exception const& e)
                            {
                                PLOGW(
                                    log_,
                                    "status report failed: ",
                                    e.what());
                            }

                            if (!shutting_down->load())
                                schedule_status_report();
                            co_return;
                        },
                        boost::asio::detached);
                });
        };
    schedule_status_report();

    // SIGINT/SIGTERM → stop server, stop engines, stop io
    boost::asio::signal_set signals(io, SIGINT, SIGTERM);
    signals.async_wait([&](boost::system::error_code, int sig) {
        shutting_down->store(true);
        log_memory("shutdown");
        PLOGI(log_, "Signal ", sig, " received, shutting down...");
        status_timer->cancel();
        server->stop();
        for (auto& [net_id, engine] : engines)
        {
            engine->stop();
        }
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
