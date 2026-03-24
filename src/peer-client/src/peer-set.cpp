#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-set.h>

#include <catl/core/logger.h>

#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <limits>

namespace catl::peer_client {

static LogPartition log_("peer-set", LogLevel::INHERIT);

namespace {

std::string
canonical_endpoint(std::string const& endpoint)
{
    std::string host;
    uint16_t port = 0;
    if (EndpointTracker::parse_endpoint(endpoint, host, port))
    {
        return host + ":" + std::to_string(port);
    }
    return endpoint;
}

}  // namespace

// ── Bootstrap peers by network ──────────────────────────────────

static const std::vector<BootstrapPeer> XRPL_BOOTSTRAP = {
    {"r.ripple.com", 51235},
    {"sahyadri.isrdc.in", 51235},
    {"hubs.xrpkuwait.com", 51235},
    {"hub.xrpl-commons.org", 51235},
};

static const std::vector<BootstrapPeer> XAHAU_BOOTSTRAP = {
    {"bacab.alloy.ee", 21337},
};

static const std::vector<BootstrapPeer> EMPTY_BOOTSTRAP = {};

std::vector<BootstrapPeer> const&
get_bootstrap_peers(uint32_t network_id)
{
    switch (network_id)
    {
        case 0:
            return XRPL_BOOTSTRAP;
        case 21337:
            return XAHAU_BOOTSTRAP;
        default:
            return EMPTY_BOOTSTRAP;
    }
}

// ── PeerSet ─────────────────────────────────────────────────────

PeerSet::PeerSet(boost::asio::io_context& io, PeerSetOptions const& options)
    : io_(io)
    , strand_(asio::make_strand(io))
    , options_(options)
    , network_id_(options.network_id)
    , tracker_(std::make_shared<EndpointTracker>())
{
    if (!options_.endpoint_cache_path.empty())
    {
        try
        {
            endpoint_cache_ =
                PeerEndpointCache::open(options_.endpoint_cache_path);
            load_cached_endpoints();
        }
        catch (std::exception const& e)
        {
            PLOGW(
                log_,
                "Peer cache disabled (",
                options_.endpoint_cache_path,
                "): ",
                e.what());
        }
    }
    // Observer wiring deferred to start() — needs shared_from_this()
}

void
PeerSet::start()
{
    configure_tracker_persistence();
}

std::int64_t
PeerSet::now_unix()
{
    using clock = std::chrono::system_clock;
    return std::chrono::duration_cast<std::chrono::seconds>(
               clock::now().time_since_epoch())
        .count();
}

void
PeerSet::update_endpoint_stats(PeerEndpointCache::Entry const& entry)
{
    auto key = canonical_endpoint(entry.endpoint);
    auto& stats = endpoint_stats_[key];
    stats.status = entry.status;
    stats.last_seen_at = entry.last_seen_at;
    stats.last_success_at = entry.last_success_at;
    stats.last_failure_at = entry.last_failure_at;
    stats.success_count = entry.success_count;
    stats.failure_count = entry.failure_count;
}

void
PeerSet::note_discovered(std::string const& endpoint)
{
    auto key = canonical_endpoint(endpoint);
    auto& stats = endpoint_stats_[key];
    stats.last_seen_at = now_unix();
    queue_crawl(key);
    if (should_connect_endpoint(key))
    {
        queue_connect(key);
    }
    sort_pending_connects();
    sort_pending_crawls();
    pump_crawls();
    pump_connects();
}

void
PeerSet::note_status(std::string const& endpoint, PeerStatus const& status)
{
    auto key = canonical_endpoint(endpoint);
    auto& stats = endpoint_stats_[key];
    stats.status = status;
    stats.last_seen_at = now_unix();
    queue_crawl(key);
    if (should_connect_endpoint(key))
    {
        queue_connect(key);
    }
    sort_pending_connects();
    sort_pending_crawls();
    pump_crawls();
    pump_connects();
}

void
PeerSet::note_connect_success(
    std::string const& endpoint,
    PeerStatus const& status)
{
    auto key = canonical_endpoint(endpoint);
    auto& stats = endpoint_stats_[key];
    auto const now = now_unix();
    stats.status = status;
    stats.last_seen_at = now;
    stats.last_success_at = now;
    stats.success_count++;
    sort_pending_connects();
}

void
PeerSet::note_connect_failure(std::string const& endpoint)
{
    auto key = canonical_endpoint(endpoint);
    auto& stats = endpoint_stats_[key];
    stats.last_failure_at = now_unix();
    stats.failure_count++;
    sort_pending_connects();
}

std::optional<PeerStatus>
PeerSet::choose_crawl_status(std::vector<CrawlLedgerRange> const& ranges) const
{
    if (ranges.empty())
        return std::nullopt;

    auto better_range = [this](
                            CrawlLedgerRange const& lhs,
                            CrawlLedgerRange const& rhs) {
        auto const preferred = preferred_ledger_seq_;
        auto const lhs_covers =
            preferred && *preferred >= lhs.first_seq && *preferred <= lhs.last_seq;
        auto const rhs_covers =
            preferred && *preferred >= rhs.first_seq && *preferred <= rhs.last_seq;
        if (lhs_covers != rhs_covers)
        {
            return lhs_covers;
        }

        auto const lhs_span = lhs.last_seq - lhs.first_seq;
        auto const rhs_span = rhs.last_seq - rhs.first_seq;
        if (lhs.last_seq != rhs.last_seq)
        {
            return lhs.last_seq > rhs.last_seq;
        }
        if (lhs_span != rhs_span)
        {
            return lhs_span > rhs_span;
        }
        return lhs.first_seq < rhs.first_seq;
    };

    auto best = std::max_element(
        ranges.begin(), ranges.end(), [&](auto const& lhs, auto const& rhs) {
            return better_range(rhs, lhs);
        });
    if (best == ranges.end())
        return std::nullopt;

    return PeerStatus{
        .first_seq = best->first_seq,
        .last_seq = best->last_seq,
        .current_seq = best->last_seq,
        .last_seen = std::chrono::steady_clock::now()};
}

bool
PeerSet::endpoint_has_range(std::string const& endpoint) const
{
    auto key = canonical_endpoint(endpoint);
    auto it = endpoint_stats_.find(key);
    if (it == endpoint_stats_.end())
        return false;

    auto const& status = it->second.status;
    return status.first_seq != 0 && status.last_seq != 0;
}

bool
PeerSet::endpoint_covers_preferred_ledger(std::string const& endpoint) const
{
    if (!preferred_ledger_seq_)
        return false;

    auto key = canonical_endpoint(endpoint);
    auto it = endpoint_stats_.find(key);
    if (it == endpoint_stats_.end())
        return false;

    auto const& status = it->second.status;
    return status.first_seq != 0 && status.last_seq != 0 &&
        *preferred_ledger_seq_ >= status.first_seq &&
        *preferred_ledger_seq_ <= status.last_seq;
}

bool
PeerSet::should_connect_endpoint(std::string const& endpoint) const
{
    if (at_connection_cap())
        return false;

    auto key = canonical_endpoint(endpoint);

    if (connections_.count(key) || in_flight_.count(key) || queued_.count(key))
        return false;

    auto it = endpoint_stats_.find(key);
    auto const has_success =
        it != endpoint_stats_.end() && it->second.last_success_at > 0;

    if (endpoint_covers_preferred_ledger(key))
        return true;

    // If crawl already had a chance and still did not give us range data,
    // fall back to a real peer handshake rather than starving on crawl-only
    // nodes.
    if (crawled_.count(key) && !endpoint_has_range(key))
        return true;

    // We still need at least one ready peer for validations and anchor work,
    // so allow known-good cached peers to race in even before they have been
    // proven to cover the target ledger.
    if (!any_peer() && (endpoint_has_range(key) || has_success))
        return true;

    return false;
}

bool
PeerSet::candidate_better(std::string const& lhs, std::string const& rhs) const
{
    static EndpointStats const empty{};

    auto lhs_it = endpoint_stats_.find(lhs);
    auto rhs_it = endpoint_stats_.find(rhs);
    auto const& lhs_stats =
        lhs_it != endpoint_stats_.end() ? lhs_it->second : empty;
    auto const& rhs_stats =
        rhs_it != endpoint_stats_.end() ? rhs_it->second : empty;

    auto const ledger = preferred_ledger_seq_;
    auto const lhs_has_range =
        lhs_stats.status.first_seq != 0 && lhs_stats.status.last_seq != 0;
    auto const rhs_has_range =
        rhs_stats.status.first_seq != 0 && rhs_stats.status.last_seq != 0;
    auto const lhs_covers = ledger && lhs_has_range &&
        *ledger >= lhs_stats.status.first_seq &&
        *ledger <= lhs_stats.status.last_seq;
    auto const rhs_covers = ledger && rhs_has_range &&
        *ledger >= rhs_stats.status.first_seq &&
        *ledger <= rhs_stats.status.last_seq;

    if (lhs_covers != rhs_covers)
        return lhs_covers;

    auto const lhs_success = lhs_stats.last_success_at > 0;
    auto const rhs_success = rhs_stats.last_success_at > 0;
    if (lhs_success != rhs_success)
        return lhs_success;

    if (lhs_has_range != rhs_has_range)
        return lhs_has_range;

    if (lhs_stats.last_success_at != rhs_stats.last_success_at)
    {
        return lhs_stats.last_success_at > rhs_stats.last_success_at;
    }

    if (lhs_stats.failure_count != rhs_stats.failure_count)
    {
        return lhs_stats.failure_count < rhs_stats.failure_count;
    }

    auto const lhs_failure = lhs_stats.last_failure_at == 0
        ? std::numeric_limits<std::int64_t>::min()
        : lhs_stats.last_failure_at;
    auto const rhs_failure = rhs_stats.last_failure_at == 0
        ? std::numeric_limits<std::int64_t>::min()
        : rhs_stats.last_failure_at;
    if (lhs_failure != rhs_failure)
    {
        return lhs_failure < rhs_failure;
    }

    if (lhs_stats.success_count != rhs_stats.success_count)
    {
        return lhs_stats.success_count > rhs_stats.success_count;
    }

    if (lhs_stats.status.current_seq != rhs_stats.status.current_seq)
    {
        return lhs_stats.status.current_seq > rhs_stats.status.current_seq;
    }

    if (lhs_stats.last_seen_at != rhs_stats.last_seen_at)
    {
        return lhs_stats.last_seen_at > rhs_stats.last_seen_at;
    }

    return lhs < rhs;
}

void
PeerSet::sort_pending_connects()
{
    std::sort(
        pending_connects_.begin(),
        pending_connects_.end(),
        [this](std::string const& lhs, std::string const& rhs) {
            return candidate_better(lhs, rhs);
        });
}

void
PeerSet::sort_pending_crawls()
{
    std::sort(
        pending_crawls_.begin(),
        pending_crawls_.end(),
        [this](std::string const& lhs, std::string const& rhs) {
            return candidate_better(lhs, rhs);
        });
}

void
PeerSet::queue_connect(std::string const& endpoint)
{
    auto key = canonical_endpoint(endpoint);
    auto const now = std::chrono::steady_clock::now();

    if (connections_.count(key) || in_flight_.count(key) || queued_.count(key))
        return;

    auto failed = failed_at_.find(key);
    if (failed != failed_at_.end())
    {
        if (now - failed->second < options_.retry_backoff)
        {
            return;
        }
        failed_at_.erase(failed);
    }

    pending_connects_.push_back(key);
    queued_.insert(key);
    sort_pending_connects();
}

void
PeerSet::queue_crawl(std::string const& endpoint)
{
    if (options_.max_in_flight_crawls == 0)
        return;

    auto key = canonical_endpoint(endpoint);
    if (key.empty() || crawl_in_flight_.count(key) || crawl_queued_.count(key) ||
        crawled_.count(key))
    {
        return;
    }

    pending_crawls_.push_back(key);
    crawl_queued_.insert(key);
    sort_pending_crawls();
}

void
PeerSet::pump_connects()
{
    while (in_flight_.size() < options_.max_in_flight_connects &&
           !pending_connects_.empty())
    {
        auto key = pending_connects_.front();
        pending_connects_.erase(pending_connects_.begin());
        queued_.erase(key);

        std::string host;
        uint16_t port = 0;
        if (!EndpointTracker::parse_endpoint(key, host, port))
            continue;

        auto const now = std::chrono::steady_clock::now();
        if (connections_.count(key) || in_flight_.count(key))
            continue;

        auto failed = failed_at_.find(key);
        if (failed != failed_at_.end())
        {
            if (now - failed->second < options_.retry_backoff)
            {
                continue;
            }
            failed_at_.erase(failed);
        }

        in_flight_.insert(key);

        auto self = shared_from_this();
        boost::asio::co_spawn(
            strand_,
            [self, host, port, key]() -> boost::asio::awaitable<void> {
                co_await self->try_connect(host, port);
                self->in_flight_.erase(key);
                self->pump_connects();
            },
            boost::asio::detached);
    }
}

void
PeerSet::pump_crawls()
{
    while (crawl_in_flight_.size() < options_.max_in_flight_crawls &&
           !pending_crawls_.empty())
    {
        auto key = pending_crawls_.front();
        pending_crawls_.erase(pending_crawls_.begin());
        crawl_queued_.erase(key);

        std::string host;
        uint16_t port = 0;
        if (!EndpointTracker::parse_endpoint(key, host, port))
            continue;

        if (crawl_in_flight_.count(key) || crawled_.count(key))
            continue;

        crawl_in_flight_.insert(key);

        auto self = shared_from_this();
        boost::asio::co_spawn(
            strand_,
            [self, host, port, key]() -> boost::asio::awaitable<void> {
                try
                {
                    auto response = co_await co_fetch_peer_crawl(host, port);

                    std::size_t ranged = 0;
                    for (auto const& peer : response.peers)
                    {
                        if (peer.endpoint.empty())
                            continue;

                        if (auto status =
                                self->choose_crawl_status(peer.complete_ledgers))
                        {
                            ++ranged;
                            self->tracker_->update(peer.endpoint, *status);
                        }
                        else
                        {
                            self->tracker_->add_discovered(peer.endpoint);
                        }
                    }

                    PLOGD(
                        log_,
                        "Crawled ",
                        key,
                        " via ",
                        response.used_tls ? "HTTPS" : "HTTP",
                        ": ",
                        response.peers.size(),
                        " public peers, ",
                        ranged,
                        " with ledger ranges");
                }
                catch (std::exception const& e)
                {
                    PLOGD(log_, "Crawl failed for ", key, ": ", e.what());
                }

                self->crawl_in_flight_.erase(key);
                self->crawled_.insert(key);

                if (self->should_connect_endpoint(key))
                {
                    self->queue_connect(key);
                }

                self->pump_crawls();
                self->pump_connects();
            },
            boost::asio::detached);
    }
}

void
PeerSet::load_cached_endpoints()
{
    if (!endpoint_cache_ || options_.cached_endpoint_limit == 0)
        return;

    std::size_t total_cached = 0;
    try
    {
        total_cached = endpoint_cache_->count_endpoints(network_id_);
    }
    catch (std::exception const& e)
    {
        PLOGW(log_, "Peer cache count failed: ", e.what());
        return;
    }

    auto cached = endpoint_cache_->load_bootstrap_candidates(
        network_id_, options_.cached_endpoint_limit);
    PLOGI(
        log_,
        "Peer cache has ",
        total_cached,
        " endpoints for network ",
        network_id_,
        "; loading ",
        cached.size(),
        " into bootstrap");

    for (auto const& entry : cached)
    {
        update_endpoint_stats(entry);
        if (entry.status.first_seq != 0 && entry.status.last_seq != 0)
        {
            tracker_->update(entry.endpoint, entry.status);
        }
        else
        {
            tracker_->add_discovered(entry.endpoint);
        }
    }
    if (!cached.empty())
    {
        PLOGI(log_, "Peer cache path: ", endpoint_cache_->path());
    }
}

void
PeerSet::configure_tracker_persistence()
{
    auto self = shared_from_this();
    auto cache = endpoint_cache_;
    auto const network_id = network_id_;

    // Observers fire from PeerClient strands (TMStatusChange/TMEndpoints).
    // Repost onto our strand so note_discovered/note_status touch PeerSet
    // state safely.
    tracker_->set_discovered_observer(
        [self, cache, network_id](std::string const& endpoint) {
            asio::post(self->strand_, [self, cache, network_id, endpoint]() {
                self->note_discovered(endpoint);
                if (cache)
                {
                    try
                    {
                        cache->remember_discovered(network_id, endpoint);
                    }
                    catch (std::exception const& e)
                    {
                        PLOGW(
                            log_,
                            "Peer cache write failed for ",
                            endpoint,
                            ": ",
                            e.what());
                    }
                }
            });
        });

    tracker_->set_status_observer(
        [self, cache, network_id](
            std::string const& endpoint, PeerStatus const& status) {
            asio::post(
                self->strand_,
                [self, cache, network_id, endpoint, status]() {
                    self->note_status(endpoint, status);
                    if (cache)
                    {
                        try
                        {
                            cache->remember_status(
                                network_id, endpoint, status);
                        }
                        catch (std::exception const& e)
                        {
                            PLOGW(
                                log_,
                                "Peer cache write failed for ",
                                endpoint,
                                ": ",
                                e.what());
                        }
                    }
                });
        });
}

void
PeerSet::start_connect(std::string const& endpoint)
{
    queue_connect(endpoint);
    pump_connects();
}

void
PeerSet::start_connect(std::string const& host, uint16_t port)
{
    queue_connect(make_key(host, port));
    pump_connects();
}

boost::asio::awaitable<std::shared_ptr<PeerClient>>
PeerSet::try_connect(std::string const& host, uint16_t port)
{
    // Hop to strand — touches connections_, in_flight_, etc.
    co_await asio::post(strand_, asio::use_awaitable);

    auto key = make_key(host, port);

    // Already connected?
    auto it = connections_.find(key);
    if (it != connections_.end())
    {
        co_return (it->second && it->second->is_ready()) ? it->second : nullptr;
    }

    // Don't exceed connection cap
    if (at_connection_cap())
    {
        PLOGD(
            log_,
            "Connection cap reached (",
            options_.max_connected_peers,
            "), skipping ",
            key);
        co_return nullptr;
    }

    PLOGI(log_, "Connecting to ", key, "...");

    std::shared_ptr<PeerClient> client;

    try
    {
        // Pass all handlers via ConnectOptions so they're installed
        // before the read loop starts — no cross-strand races.
        PeerClient::ConnectOptions connect_opts;
        connect_opts.network_id = network_id_;
        connect_opts.tracker = tracker_;
        connect_opts.unsolicited_handler = unsolicited_handler_;
        {
            auto self2 = shared_from_this();
            auto owned_key2 = key;
            connect_opts.on_disconnect = [self2, owned_key2]() {
                asio::post(self2->strand_, [self2, owned_key2]() {
                    self2->remove_peer(owned_key2);
                });
            };
        }

        co_await co_connect(
            io_, host, port, std::move(connect_opts), client);
        connections_[key] = client;
        failed_at_.erase(key);
        note_connect_success(
            key,
            {.first_seq = client->peer_first_seq(),
             .last_seq = client->peer_last_seq(),
             .current_seq = client->peer_ledger_seq()});
        if (endpoint_cache_)
        {
            try
            {
                endpoint_cache_->remember_connect_success(
                    network_id_,
                    key,
                    {.first_seq = client->peer_first_seq(),
                     .last_seq = client->peer_last_seq(),
                     .current_seq = client->peer_ledger_seq()});
            }
            catch (std::exception const& e)
            {
                PLOGW(
                    log_, "Peer cache write failed for ", key, ": ", e.what());
            }
        }
        PLOGI(
            log_,
            "Connected to ",
            key,
            " (range: ",
            client->peer_first_seq(),
            "-",
            client->peer_last_seq(),
            ")");
        co_return client;
    }
    catch (std::exception const& e)
    {
        PLOGD(log_, "Failed to connect to ", key, ": ", e.what());
        failed_at_[key] = std::chrono::steady_clock::now();
        note_connect_failure(key);
        if (endpoint_cache_)
        {
            try
            {
                endpoint_cache_->remember_connect_failure(network_id_, key);
            }
            catch (std::exception const& cache_error)
            {
                PLOGW(
                    log_,
                    "Peer cache write failed for ",
                    key,
                    ": ",
                    cache_error.what());
            }
        }

        // Harvest 503 redirect IPs
        if (client)
        {
            auto const& ips = client->raw_connection().redirect_ips();
            if (!ips.empty())
            {
                PLOGI(log_, "Got ", ips.size(), " redirect peers from ", key);
                for (auto const& ip : ips)
                {
                    tracker_->add_discovered(ip);
                }
                // Immediately try the redirect peers
                try_undiscovered();
            }
        }

        co_return nullptr;
    }
}

void
PeerSet::bootstrap()
{
    auto const& boot_peers = get_bootstrap_peers(network_id_);
    auto tracked_endpoints = tracker_->all_endpoints();

    if (boot_peers.empty() && tracked_endpoints.empty())
    {
        PLOGW(log_, "No bootstrap peers for network ", network_id_);
        return;
    }

    PLOGI(
        log_,
        "Bootstrapping: ",
        boot_peers.size(),
        " built-in seeds, ",
        tracked_endpoints.size(),
        " cached/discovered endpoints");

    for (auto const& bp : boot_peers)
    {
        queue_crawl(make_key(bp.host, bp.port));
        queue_connect(make_key(bp.host, bp.port));
    }

    for (auto const& endpoint : tracked_endpoints)
    {
        queue_crawl(endpoint);
        if (should_connect_endpoint(endpoint))
        {
            queue_connect(endpoint);
        }
    }

    pump_crawls();
    pump_connects();
}

void
PeerSet::start_tracked_endpoints()
{
    auto endpoints = tracker_->all_endpoints();
    for (auto const& endpoint : endpoints)
    {
        queue_crawl(endpoint);
        if (should_connect_endpoint(endpoint))
        {
            queue_connect(endpoint);
        }
    }
    pump_crawls();
    pump_connects();
}

void
PeerSet::try_undiscovered()
{
    auto candidates = tracker_->undiscovered();
    if (candidates.empty())
        return;

    int crawls = 0;
    int connects = 0;
    for (auto const& ep : candidates)
    {
        auto const before_crawls = crawl_queued_.size();
        queue_crawl(ep);
        if (crawl_queued_.size() != before_crawls)
        {
            crawls++;
        }

        auto const before_connects = queued_.size();
        if (should_connect_endpoint(ep))
        {
            queue_connect(ep);
        }
        if (queued_.size() != before_connects)
        {
            connects++;
        }
    }

    if (crawls > 0 || connects > 0)
    {
        PLOGI(
            log_,
            "Queued ",
            crawls,
            " crawl jobs and ",
            connects,
            " connect attempts from discovered endpoints (",
            in_flight_.size(),
            "/",
            options_.max_in_flight_connects,
            " in flight)");
    }

    pump_crawls();
    pump_connects();
}

void
PeerSet::prioritize_ledger(uint32_t ledger_seq)
{
    // Note: this sets a shared preference. With concurrent prove() calls,
    // the last writer wins. This is acceptable — it's a hint, not a contract.
    preferred_ledger_seq_ = ledger_seq;
    sort_pending_connects();
    sort_pending_crawls();
    pump_crawls();
    pump_connects();
}

void
PeerSet::try_candidates_for(uint32_t ledger_seq)
{
    // Don't call prioritize_ledger() — with concurrent prove() calls,
    // overwriting the shared preference would retune discovery for all.
    // Just queue/pump without reranking.

    auto candidates = tracker_->all_endpoints();
    for (auto const& endpoint : candidates)
    {
        queue_crawl(endpoint);
        if (should_connect_endpoint(endpoint))
        {
            queue_connect(endpoint);
        }
    }

    // Keep expanding the graph even when cached range data is incomplete.
    try_undiscovered();
    sort_pending_connects();
    sort_pending_crawls();
    pump_crawls();
    pump_connects();
}

boost::asio::awaitable<std::shared_ptr<PeerClient>>
PeerSet::add(std::string const& host, uint16_t port)
{
    auto client = co_await try_connect(host, port);
    if (!client)
    {
        throw std::runtime_error(
            "PeerSet: failed to connect to " + make_key(host, port));
    }
    co_return client;
}

std::optional<std::shared_ptr<PeerClient>>
PeerSet::peer_for(uint32_t ledger_seq) const
{
    static std::unordered_set<std::string> const empty;
    return peer_for(ledger_seq, empty);
}

std::optional<std::shared_ptr<PeerClient>>
PeerSet::peer_for(
    uint32_t ledger_seq,
    std::unordered_set<std::string> const& excluded) const
{
    std::shared_ptr<PeerClient> best;
    std::string best_key;

    for (auto const& [key, client] : connections_)
    {
        if (excluded.count(key))
            continue;
        if (!client || !client->is_ready())
            continue;
        if (client->peer_first_seq() != 0 &&
            ledger_seq >= client->peer_first_seq() &&
            ledger_seq <= client->peer_last_seq())
        {
            if (!best || candidate_better(key, best_key))
            {
                best = client;
                best_key = key;
            }
        }
    }
    if (best)
    {
        return best;
    }
    return std::nullopt;
}

boost::asio::awaitable<std::optional<std::shared_ptr<PeerClient>>>
PeerSet::wait_for_any_peer(int timeout_secs)
{
    static std::unordered_set<std::string> const empty;
    co_return co_await wait_for_any_peer(timeout_secs, empty);
}

boost::asio::awaitable<std::optional<std::shared_ptr<PeerClient>>>
PeerSet::wait_for_any_peer(
    int timeout_secs,
    std::unordered_set<std::string> const& excluded)
{
    // Hop to strand — all PeerSet state access must be serialized
    co_await asio::post(strand_, asio::use_awaitable);

    if (auto client = any_peer(excluded))
    {
        co_return client;
    }

    // Pick up any redirect or TMEndpoints discoveries we already know about
    // before we begin waiting.
    start_tracked_endpoints();
    try_undiscovered();

    PLOGI(
        log_,
        "Waiting for any ready peer (",
        connections_.size(),
        " connected, ",
        in_flight_.size(),
        " in-flight, ",
        tracker_->size(),
        " known, Ctrl-C to cancel)");

    boost::asio::steady_timer timer(io_);
    for (int elapsed_ms = 0; elapsed_ms < timeout_secs * 1000;
         elapsed_ms += 200)
    {
        timer.expires_after(std::chrono::milliseconds(200));
        boost::system::error_code ec;
        co_await timer.async_wait(
            boost::asio::redirect_error(boost::asio::use_awaitable, ec));

        if (auto client = any_peer(excluded))
        {
            co_return client;
        }

        if (((elapsed_ms + 200) % 1000) == 0)
        {
            start_tracked_endpoints();
            try_undiscovered();
        }

        if (((elapsed_ms + 200) % 5000) == 0)
        {
            PLOGI(
                log_,
                "Still waiting for any peer (",
                connections_.size(),
                " connected, ",
                in_flight_.size(),
                " in-flight, ",
                tracker_->size(),
                " known)");
        }
    }

    PLOGW(log_, "No ready peer found after ", timeout_secs, "s");
    co_return std::nullopt;
}

boost::asio::awaitable<std::optional<std::shared_ptr<PeerClient>>>
PeerSet::wait_for_peer(uint32_t ledger_seq, int timeout_secs)
{
    static std::unordered_set<std::string> const empty;
    co_return co_await wait_for_peer(ledger_seq, timeout_secs, empty);
}

boost::asio::awaitable<std::optional<std::shared_ptr<PeerClient>>>
PeerSet::wait_for_peer(
    uint32_t ledger_seq,
    int timeout_secs,
    std::unordered_set<std::string> const& excluded)
{
    // Hop to strand — all PeerSet state access must be serialized
    co_await asio::post(strand_, asio::use_awaitable);

    if (auto p = peer_for(ledger_seq, excluded))
    {
        co_return p;
    }

    // Immediately try any undiscovered endpoints
    try_candidates_for(ledger_seq);

    PLOGI(
        log_,
        "Waiting for a peer with ledger ",
        ledger_seq,
        " (",
        connections_.size(),
        " connected, ",
        in_flight_.size(),
        " in-flight, ",
        tracker_->size(),
        " known, Ctrl-C to cancel)");

    boost::asio::steady_timer timer(io_);
    for (int elapsed_ms = 0; elapsed_ms < timeout_secs * 1000;
         elapsed_ms += 200)
    {
        timer.expires_after(std::chrono::milliseconds(200));
        boost::system::error_code ec;
        co_await timer.async_wait(
            boost::asio::redirect_error(boost::asio::use_awaitable, ec));

        if (auto p = peer_for(ledger_seq, excluded))
        {
            PLOGI(log_, "Found peer for ledger ", ledger_seq);
            co_return p;
        }

        if (((elapsed_ms + 200) % 1000) == 0)
        {
            try_candidates_for(ledger_seq);
        }

        if (((elapsed_ms + 200) % 5000) == 0)
        {
            PLOGI(
                log_,
                "Still waiting for ledger ",
                ledger_seq,
                " (",
                connections_.size(),
                " connected, ",
                in_flight_.size(),
                " in-flight, ",
                tracker_->size(),
                " known)");
        }
    }

    PLOGW(
        log_,
        "No peer found with ledger ",
        ledger_seq,
        " after ",
        timeout_secs,
        "s");

    co_return std::nullopt;
}

std::shared_ptr<PeerClient>
PeerSet::any_peer() const
{
    static std::unordered_set<std::string> const empty;
    return any_peer(empty);
}

std::shared_ptr<PeerClient>
PeerSet::any_peer(std::unordered_set<std::string> const& excluded) const
{
    std::shared_ptr<PeerClient> best;
    std::string best_key;

    for (auto const& [key, client] : connections_)
    {
        if (excluded.count(key))
            continue;
        if (client && client->is_ready())
        {
            if (!best || candidate_better(key, best_key))
            {
                best = client;
                best_key = key;
            }
        }
    }
    return best;
}

size_t
PeerSet::connected_count() const
{
    size_t count = 0;
    for (auto const& [_, client] : connections_)
    {
        if (client && client->is_ready())
        {
            count++;
        }
    }
    return count;
}

bool
PeerSet::at_connection_cap() const
{
    return connected_count() >= options_.max_connected_peers;
}

void
PeerSet::remove_peer(std::string const& key)
{
    auto it = connections_.find(key);
    if (it != connections_.end())
    {
        PLOGI(log_, "Removing dead peer: ", key);
        connections_.erase(it);
        tracker_->remove(key);
    }
}

}  // namespace catl::peer_client
