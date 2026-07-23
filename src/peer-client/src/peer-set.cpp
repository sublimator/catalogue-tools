#include <catl/peer-client/peer-client-connect-coro.h>
#include <catl/peer-client/peer-selector.h>
#include <catl/peer-client/peer-set.h>

#include <catl/core/logger.h>

#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <cassert>
#include <cctype>
#include <limits>

// Diagnose PeerSet state access outside its strand. Live regression runs can
// opt into fail-fast behavior so a violation cannot be hidden in noisy logs.
#define ASSERT_ON_STRAND()                            \
    do                                                \
    {                                                 \
        if (!strand_.running_in_this_thread())        \
        {                                             \
            PLOGE(                                    \
                log_,                                 \
                "OFF-STRAND: ",                       \
                __func__,                             \
                " at ",                               \
                __FILE__,                             \
                ":",                                  \
                __LINE__);                            \
            if (std::getenv("XPRV_STRAND_FAIL_FAST")) \
            {                                         \
                std::abort();                         \
            }                                         \
        }                                             \
    } while (0)

// Auto-repost onto strand if called from off-strand. Use for public methods.
// The method must be callable via shared_from_this().
#define ENSURE_ON_STRAND(method_call)                           \
    do                                                          \
    {                                                           \
        if (!strand_.running_in_this_thread())                  \
        {                                                       \
            asio::post(strand_, [self = shared_from_this()]() { \
                self->method_call;                              \
            });                                                 \
            return;                                             \
        }                                                       \
    } while (0)

namespace catl::peer_client {

static LogPartition log_("peer-set", LogLevel::INHERIT);

enum class WaitOutcome {
    signaled,
    timed_out,
    canceled,
};

static asio::awaitable<WaitOutcome>
wait_for_signal_or_timeout(
    asio::strand<asio::io_context::executor_type>& strand,
    std::shared_ptr<asio::steady_timer> signal,
    std::chrono::milliseconds timeout,
    std::shared_ptr<std::atomic<bool>> cancel = nullptr)
{
    if (cancel && cancel->load(std::memory_order_relaxed))
        co_return WaitOutcome::canceled;
    if (timeout <= std::chrono::milliseconds::zero())
        co_return WaitOutcome::timed_out;

    static constexpr auto kCancelPoll = std::chrono::milliseconds(25);
    auto remaining = timeout;

    while (remaining > std::chrono::milliseconds::zero())
    {
        auto slice = cancel ? std::min(remaining, kCancelPoll) : remaining;
        auto [order, ex0, ec0, ex1, ec1] =
            co_await asio::experimental::make_parallel_group(
                asio::co_spawn(
                    strand,
                    [signal]() -> asio::awaitable<boost::system::error_code> {
                        boost::system::error_code ec;
                        co_await signal->async_wait(
                            asio::redirect_error(asio::use_awaitable, ec));
                        co_return ec;
                    },
                    asio::deferred),
                asio::co_spawn(
                    strand,
                    [strand,
                     slice]() -> asio::awaitable<boost::system::error_code> {
                        asio::steady_timer timer(strand, slice);
                        boost::system::error_code ec;
                        co_await timer.async_wait(
                            asio::redirect_error(asio::use_awaitable, ec));
                        co_return ec;
                    },
                    asio::deferred))
                .async_wait(
                    asio::experimental::wait_for_one(), asio::use_awaitable);

        if (order[0] == 0)
            co_return WaitOutcome::signaled;

        remaining -= slice;
        if (cancel && cancel->load(std::memory_order_relaxed))
            co_return WaitOutcome::canceled;
    }

    co_return WaitOutcome::timed_out;
}

namespace {

std::string
canonical_endpoint(std::string const& endpoint)
{
    return EndpointTracker::canonical_endpoint(endpoint);
}

std::string
trim_connect_failure_detail(std::string detail)
{
    auto const body = detail.find(" body=\"");
    if (body != std::string::npos)
        detail.erase(body);
    return detail;
}

void
append_source_label(std::string& out, char const* label)
{
    if (!out.empty())
        out += "+";
    out += label;
}

std::string
discovery_source_label(bool seen_crawl, bool seen_endpoints, bool seen_redirect)
{
    std::string label;
    if (seen_crawl)
        append_source_label(label, "crawl");
    if (seen_endpoints)
        append_source_label(label, "tmendpoints");
    if (seen_redirect)
        append_source_label(label, "redirect");
    if (label.empty())
        label = "unknown";
    return label;
}

std::string
trim_copy(std::string_view text)
{
    auto first = text.begin();
    auto last = text.end();
    while (first != last && std::isspace(static_cast<unsigned char>(*first)))
        ++first;
    while (first != last &&
           std::isspace(static_cast<unsigned char>(*(last - 1))))
        --last;
    return std::string(first, last);
}

std::string
lower_copy(std::string_view text)
{
    std::string out(text);
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return out;
}

std::string
classify_failure_kind(std::string_view detail, bool crawl)
{
    auto lowered = lower_copy(detail);
    std::string suffix;

    if (lowered.find("status=403") != std::string::npos ||
        lowered.find("status: 403") != std::string::npos ||
        lowered.find("forbidden") != std::string::npos)
    {
        suffix = "403";
    }
    else if (
        lowered.find("status=503") != std::string::npos ||
        lowered.find("status: 503") != std::string::npos ||
        lowered.find("service unavailable") != std::string::npos)
    {
        suffix = "503";
    }
    else if (lowered.find("no route to host") != std::string::npos)
    {
        suffix = "no-route";
    }
    else if (lowered.find("timeout") != std::string::npos)
    {
        suffix = "timeout";
    }
    else if (lowered.find("stream truncated") != std::string::npos)
    {
        suffix = "truncated";
    }
    else if (lowered.find("end of stream") != std::string::npos)
    {
        suffix = "eof";
    }
    else if (lowered.find("tls handshake") != std::string::npos)
    {
        suffix = "tls";
    }
    else if (
        lowered.find("resolve") != std::string::npos ||
        lowered.find("host not found") != std::string::npos)
    {
        suffix = "dns";
    }
    else if (
        lowered.empty() || lowered.find("disconnected") != std::string::npos)
    {
        suffix = "disconnect";
    }
    else
    {
        suffix = "other";
    }

    return std::string(crawl ? "crawl-" : "connect-") + suffix;
}

}  // namespace

static constexpr size_t kMaxTrackedEndpoints = 4096;
static constexpr size_t kMaxCrawlHistory = 4096;
static constexpr auto kCrawlRetention = std::chrono::minutes(30);
static constexpr auto kPrivateCrawlRecheck = std::chrono::hours(6);
static constexpr auto kFailedEndpointRetention = std::chrono::minutes(30);
static constexpr auto kRecentFailureRetention = std::chrono::minutes(10);
static constexpr auto kFailedLedgerTtl = std::chrono::seconds(60);
static constexpr size_t kMaxRecentFailureEvents = 256;

// ── Bootstrap peers by network ──────────────────────────────────

static const std::vector<BootstrapPeer> XRPL_BOOTSTRAP = {
    {"r.ripple.com", 51235},
    {"s1.ripple.com", 51235},
    {"hubs.xrpkuwait.com", 51235},
    {"hub.xrpl-commons.org", 51235},
};

static const std::vector<BootstrapPeer> XAHAU_BOOTSTRAP = {
    {"bacab.alloy.ee", 21337},
    {"hubs.xahau.as16089.net", 21337},
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

static std::string
peer_net_label(uint32_t network_id)
{
    switch (network_id)
    {
        case 0:
            return "xrpl";
        case 21337:
            return "xahau";
        default:
            return "net-" + std::to_string(network_id);
    }
}

PeerSet::PeerSet(boost::asio::io_context& io, PeerSetOptions const& options)
    : io_(io)
    , strand_(asio::make_strand(io))
    , options_(options)
    , network_id_(options.network_id)
    , net_label_(peer_net_label(options.network_id))
    , tracker_(std::make_shared<EndpointTracker>())
{
    if (!options_.endpoint_cache_path.empty())
    {
        try
        {
            endpoint_cache_ =
                PeerEndpointCache::open(options_.endpoint_cache_path);
        }
        catch (std::exception const& e)
        {
            PLOGW(
                log_,
                "[",
                net_label_,
                "] ",
                "Peer cache disabled (",
                options_.endpoint_cache_path,
                "): ",
                e.what());
        }
    }
    // load_cached_endpoints + observer wiring deferred to start() —
    // both need shared_from_this() which is unsafe in the constructor.
}

void
PeerSet::start()
{
    if (endpoint_cache_)
    {
        try
        {
            load_cached_endpoints();
        }
        catch (std::exception const& e)
        {
            PLOGW(
                log_,
                "[",
                net_label_,
                "] ",
                "Peer cache load failed: ",
                e.what());
        }
    }
    configure_tracker_persistence();
}

void
PeerSet::set_unsolicited_handler(UnsolicitedHandler handler)
{
    {
        std::lock_guard lock(unsolicited_handler_mutex_);
        unsolicited_handler_ = std::move(handler);
    }

    auto self = shared_from_this();
    asio::post(strand_, [self]() {
        UnsolicitedHandler current_handler;
        {
            std::lock_guard lock(self->unsolicited_handler_mutex_);
            current_handler = self->unsolicited_handler_;
        }

        for (auto& [_, client] : self->connections_)
        {
            if (client)
            {
                client->set_unsolicited_handler(current_handler);
            }
        }
    });
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
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), entry]() {
            self->update_endpoint_stats(entry);
        });
        return;
    }
    auto key = canonical_endpoint(entry.endpoint);
    auto& stats = endpoint_stats_[key];
    stats.status = entry.status;
    stats.last_seen_at = entry.last_seen_at;
    stats.last_success_at = entry.last_success_at;
    stats.last_failure_at = entry.last_failure_at;
    stats.success_count = entry.success_count;
    stats.failure_count = entry.failure_count;
    stats.seen_crawl = entry.seen_crawl;
    stats.seen_endpoints = entry.seen_endpoints;
    stats.seen_redirect = entry.seen_redirect;
    stats.connected_ok = entry.connected_ok;
}

void
PeerSet::note_discovered(std::string const& endpoint, DiscoverySource source)
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), endpoint, source]() {
            self->note_discovered(endpoint, source);
        });
        return;
    }
    auto key = canonical_endpoint(endpoint);
    auto& stats = endpoint_stats_[key];
    stats.last_seen_at = now_unix();
    switch (source)
    {
        case DiscoverySource::Crawl:
            stats.seen_crawl = true;
            break;
        case DiscoverySource::Endpoints:
            stats.seen_endpoints = true;
            break;
        case DiscoverySource::Redirect:
            stats.seen_redirect = true;
            break;
        case DiscoverySource::Unknown:
            break;
    }
    queue_crawl(key);
    if (should_connect_endpoint(key))
    {
        queue_connect(key);
    }
    sort_pending_connects();
    sort_pending_crawls();
    prune_discovery_state();
    pump_crawls();
    pump_connects();
    notify_waiters();
}

void
PeerSet::note_status(std::string const& endpoint, PeerStatus const& status)
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), endpoint, status]() {
            self->note_status(endpoint, status);
        });
        return;
    }
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
    prune_discovery_state();
    pump_crawls();
    pump_connects();
    notify_waiters();
}

void
PeerSet::note_connect_success(
    std::string const& endpoint,
    PeerStatus const& status)
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), endpoint, status]() {
            self->note_connect_success(endpoint, status);
        });
        return;
    }
    auto key = canonical_endpoint(endpoint);
    auto& stats = endpoint_stats_[key];
    auto const now = now_unix();
    stats.status = status;
    stats.last_seen_at = now;
    stats.last_success_at = now;
    stats.success_count++;
    stats.connected_ok = true;
    sort_pending_connects();
    notify_waiters();
}

void
PeerSet::note_peer_headers(
    std::string const& endpoint,
    std::map<std::string, std::string> const& headers)
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), endpoint, headers]() {
            self->note_peer_headers(endpoint, headers);
        });
        return;
    }

    auto const key = canonical_endpoint(endpoint);
    auto it = headers.find("Crawl");
    if (it == headers.end())
        return;

    auto const policy = lower_copy(trim_copy(it->second));
    auto& stats = endpoint_stats_[key];
    auto const now = std::chrono::steady_clock::now();

    if (policy == "private")
    {
        auto const until = now + kPrivateCrawlRecheck;
        auto const was_suppressed = stats.crawl_private_until > now;
        if (stats.crawl_private_until < until)
            stats.crawl_private_until = until;
        if (!was_suppressed)
        {
            PLOGI(
                log_,
                "[",
                net_label_,
                "] Peer ",
                key,
                " advertises Crawl: private; suppressing /crawl for 6h");
        }
        return;
    }

    if (policy == "public")
    {
        stats.crawl_private_until = {};
    }
}

void
PeerSet::note_connect_failure(std::string const& endpoint, std::string detail)
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(
            strand_,
            [self = shared_from_this(),
             endpoint,
             detail = std::move(detail)]() mutable {
                self->note_connect_failure(endpoint, std::move(detail));
            });
        return;
    }
    auto key = canonical_endpoint(endpoint);
    auto& stats = endpoint_stats_[key];
    auto const now = std::chrono::steady_clock::now();
    stats.last_failure_at = now_unix();
    stats.last_seen_at = stats.last_failure_at;
    stats.failure_count++;
    stats.connected_ok = false;
    record_failure_event(key, classify_failure_kind(detail, false), now);
    sort_pending_connects();
    notify_waiters();
}

void
PeerSet::note_crawl_failure(std::string const& endpoint, std::string detail)
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(
            strand_,
            [self = shared_from_this(),
             endpoint,
             detail = std::move(detail)]() mutable {
                self->note_crawl_failure(endpoint, std::move(detail));
            });
        return;
    }
    auto const now = std::chrono::steady_clock::now();
    record_failure_event(
        canonical_endpoint(endpoint), classify_failure_kind(detail, true), now);
}

std::chrono::steady_clock::duration
PeerSet::retry_backoff_for(std::string const& endpoint) const
{
    ASSERT_ON_STRAND();
    auto const key = canonical_endpoint(endpoint);
    auto stats_it = endpoint_stats_.find(key);
    uint64_t failures =
        stats_it != endpoint_stats_.end() ? stats_it->second.failure_count : 0;

    if (failures == 0)
        return std::chrono::steady_clock::duration::zero();

    auto const exponent = std::min<uint64_t>(failures - 1, 6);
    auto const backoff =
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
            options_.retry_backoff * (uint64_t{1} << exponent));
    auto const cap =
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(
            std::chrono::minutes(5));
    return std::min(backoff, cap);
}

void
PeerSet::record_failure_event(
    std::string const& endpoint,
    std::string const& kind,
    std::chrono::steady_clock::time_point now)
{
    ASSERT_ON_STRAND();
    recent_failures_.push_back(
        FailureEvent{.at = now, .endpoint = endpoint, .kind = kind});
    prune_recent_failure_events(now);
}

void
PeerSet::prune_recent_failure_events(std::chrono::steady_clock::time_point now)
{
    ASSERT_ON_STRAND();
    auto const cutoff = now - kRecentFailureRetention;
    while (!recent_failures_.empty() &&
           (recent_failures_.front().at < cutoff ||
            recent_failures_.size() > kMaxRecentFailureEvents))
    {
        recent_failures_.pop_front();
    }
}

std::shared_ptr<asio::steady_timer>
PeerSet::attach_wait_signal()
{
    ASSERT_ON_STRAND();
    if (!wait_signal_)
    {
        wait_signal_ = std::make_shared<asio::steady_timer>(
            strand_, asio::steady_timer::time_point::max());
    }
    return wait_signal_;
}

void
PeerSet::notify_waiters()
{
    ASSERT_ON_STRAND();
    auto wake = wait_signal_;
    wait_signal_ = std::make_shared<asio::steady_timer>(
        strand_, asio::steady_timer::time_point::max());
    if (wake)
        wake->cancel();
}

std::vector<PeerSessionPtr>
PeerSet::select_peers_for(
    uint32_t ledger_seq,
    int max_count,
    std::unordered_set<std::string> const& excluded)
{
    ASSERT_ON_STRAND();

    // Budget: never use more than 1/3 of ready peers, cap at 4
    int ready = 0;
    for (auto const& [_, c] : connections_)
        if (c && c->is_ready())
            ++ready;
    int budget = std::min({max_count, std::max(1, ready / 3), 4});

    // Build candidates sorted by range width descending
    struct Candidate
    {
        std::string key;
        uint64_t span;
        PeerSessionPtr client;
    };
    std::vector<Candidate> candidates;
    auto const now = std::chrono::steady_clock::now();

    for (auto const& [key, client] : connections_)
    {
        if (!client || !client->is_ready())
            continue;
        if (excluded.count(key))
            continue;
        if (peer_failed_for_ledger(key, ledger_seq, now))
            continue;

        uint64_t span = 0;
        auto f = client->peer_first_seq();
        auto l = client->peer_last_seq();
        if (l > f)
            span = static_cast<uint64_t>(l - f);
        candidates.push_back({key, span, client});
    }

    // Sort: range-matched first, then by span descending
    std::sort(
        candidates.begin(),
        candidates.end(),
        [ledger_seq](auto const& a, auto const& b) {
            bool a_covers = a.client->peer_first_seq() <= ledger_seq &&
                ledger_seq <= a.client->peer_last_seq();
            bool b_covers = b.client->peer_first_seq() <= ledger_seq &&
                ledger_seq <= b.client->peer_last_seq();
            if (a_covers != b_covers)
                return a_covers > b_covers;  // range-matched first
            return a.span > b.span;          // wider range preferred
        });

    std::vector<PeerSessionPtr> result;
    for (auto const& c : candidates)
    {
        if (static_cast<int>(result.size()) >= budget)
            break;
        note_peer_selected(c.key);
        result.push_back(c.client);
    }
    return result;
}

boost::asio::awaitable<std::vector<PeerSessionPtr>>
PeerSet::co_select_peers_for(
    uint32_t ledger_seq,
    int max_count,
    std::unordered_set<std::string> excluded)
{
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self, ledger_seq, max_count, excluded = std::move(excluded)]() mutable
        -> asio::awaitable<std::vector<PeerSessionPtr>> {
            co_return self->select_peers_for(ledger_seq, max_count, excluded);
        },
        asio::use_awaitable);
}

boost::asio::awaitable<PeerSet::ConnectionStats>
PeerSet::co_connection_stats()
{
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<ConnectionStats> {
            co_return ConnectionStats{
                self->connections_.size(),
                self->total_connects_,
                self->total_disconnects_};
        },
        asio::use_awaitable);
}

boost::asio::awaitable<size_t>
PeerSet::co_size()
{
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<size_t> {
            co_return self->connections_.size();
        },
        asio::use_awaitable);
}

void
PeerSet::note_ledger_failure(std::string const& endpoint, uint32_t ledger_seq)
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(
            strand_, [self = shared_from_this(), endpoint, ledger_seq]() {
                self->note_ledger_failure(endpoint, ledger_seq);
            });
        return;
    }
    auto key = canonical_endpoint(endpoint);
    auto const now = std::chrono::steady_clock::now();
    auto& stats = endpoint_stats_[key];
    auto prior = stats.failed_ledgers.find(ledger_seq);
    if (prior == stats.failed_ledgers.end() ||
        now - prior->second.at >= kFailedLedgerTtl)
    {
        ++stats.ledger_failure_count;
    }
    stats.last_ledger_failure_at = now;
    stats.failed_ledgers[ledger_seq] = {now};
    sort_pending_connects();
    PLOGD(log_, "Noted ledger failure: peer=", key, " ledger=", ledger_seq);
}

bool
PeerSet::peer_failed_for_ledger(
    std::string const& endpoint,
    uint32_t ledger_seq,
    std::chrono::steady_clock::time_point now) const
{
    ASSERT_ON_STRAND();
    auto const key = canonical_endpoint(endpoint);
    auto stats = endpoint_stats_.find(key);
    if (stats == endpoint_stats_.end())
        return false;
    auto failure = stats->second.failed_ledgers.find(ledger_seq);
    return failure != stats->second.failed_ledgers.end() &&
        now - failure->second.at < kFailedLedgerTtl;
}

PeerSessionPtr
PeerSet::choose_any_peer(std::unordered_set<std::string> const& excluded)
{
    ASSERT_ON_STRAND();
    PeerSessionPtr best;
    std::string best_key;

    for (auto const& [key, client] : connections_)
    {
        if (excluded.count(key))
            continue;
        if (!client || !client->is_ready())
            continue;

        if (!best)
        {
            best = client;
            best_key = key;
            continue;
        }

        auto const& lhs_stats = endpoint_stats_[key];
        auto const& rhs_stats = endpoint_stats_[best_key];

        if (lhs_stats.selection_count != rhs_stats.selection_count)
        {
            if (lhs_stats.selection_count < rhs_stats.selection_count)
            {
                best = client;
                best_key = key;
            }
            continue;
        }

        if (lhs_stats.last_selected_ticket != rhs_stats.last_selected_ticket)
        {
            if (lhs_stats.last_selected_ticket < rhs_stats.last_selected_ticket)
            {
                best = client;
                best_key = key;
            }
            continue;
        }

        if (candidate_better(key, best_key))
        {
            best = client;
            best_key = key;
        }
    }

    if (best)
    {
        note_peer_selected(best_key);
    }
    return best;
}

PeerSessionPtr
PeerSet::choose_peer_for(
    uint32_t ledger_seq,
    std::unordered_set<std::string> const& excluded)
{
    ASSERT_ON_STRAND();
    PeerSessionPtr best;
    std::string best_key;
    uint32_t best_span = std::numeric_limits<uint32_t>::max();

    for (auto const& [key, client] : connections_)
    {
        if (excluded.count(key))
            continue;
        if (!client || !client->is_ready())
            continue;

        auto first = client->peer_first_seq();
        auto last = client->peer_last_seq();
        if (first == 0 || last == 0 || ledger_seq < first || ledger_seq > last)
            continue;

        auto const span = last - first;
        if (!best)
        {
            best = client;
            best_key = key;
            best_span = span;
            continue;
        }

        if (span != best_span)
        {
            if (span < best_span)
            {
                best = client;
                best_key = key;
                best_span = span;
            }
            continue;
        }

        auto const& lhs_stats = endpoint_stats_[key];
        auto const& rhs_stats = endpoint_stats_[best_key];

        if (lhs_stats.failure_count != rhs_stats.failure_count)
        {
            if (lhs_stats.failure_count < rhs_stats.failure_count)
            {
                best = client;
                best_key = key;
                best_span = span;
            }
            continue;
        }

        if (lhs_stats.selection_count != rhs_stats.selection_count)
        {
            if (lhs_stats.selection_count < rhs_stats.selection_count)
            {
                best = client;
                best_key = key;
                best_span = span;
            }
            continue;
        }

        if (lhs_stats.last_selected_ticket != rhs_stats.last_selected_ticket)
        {
            if (lhs_stats.last_selected_ticket < rhs_stats.last_selected_ticket)
            {
                best = client;
                best_key = key;
                best_span = span;
            }
            continue;
        }

        if (candidate_better(key, best_key))
        {
            best = client;
            best_key = key;
            best_span = span;
        }
    }

    if (best)
    {
        note_peer_selected(best_key);
        return best;
    }
    return nullptr;
}

void
PeerSet::note_peer_selected(std::string const& key)
{
    ASSERT_ON_STRAND();
    auto& stats = endpoint_stats_[key];
    stats.selection_count++;
    stats.last_selected_ticket = ++next_selection_ticket_;
}

std::optional<PeerStatus>
PeerSet::choose_crawl_status(std::vector<CrawlLedgerRange> const& ranges) const
{
    if (ranges.empty())
        return std::nullopt;

    auto better_range = [this](
                            CrawlLedgerRange const& lhs,
                            CrawlLedgerRange const& rhs) -> bool {
        // Check if either range covers any wanted ledger
        auto covers_wanted = [&](CrawlLedgerRange const& r) {
            for (auto const& [seq, _] : wanted_ledgers_)
            {
                if (seq >= r.first_seq && seq <= r.last_seq)
                    return true;
            }
            return false;
        };
        auto const lhs_covers = covers_wanted(lhs);
        auto const rhs_covers = covers_wanted(rhs);
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
    ASSERT_ON_STRAND();
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
    ASSERT_ON_STRAND();
    if (wanted_ledgers_.empty())
        return false;

    auto key = canonical_endpoint(endpoint);
    auto it = endpoint_stats_.find(key);
    if (it == endpoint_stats_.end())
        return false;

    auto const& status = it->second.status;
    if (status.first_seq == 0 || status.last_seq == 0)
        return false;

    for (auto const& [seq, _] : wanted_ledgers_)
    {
        if (seq >= status.first_seq && seq <= status.last_seq)
            return true;
    }
    return false;
}

bool
PeerSet::should_connect_endpoint(std::string const& endpoint) const
{
    ASSERT_ON_STRAND();
    auto key = canonical_endpoint(endpoint);
    auto const now = std::chrono::steady_clock::now();

    if (connections_.count(key) || in_flight_.count(key) || queued_.count(key))
        return false;

    auto it = endpoint_stats_.find(key);
    auto const has_success =
        it != endpoint_stats_.end() && it->second.last_success_at > 0;

    // Ledger-specific waits explicitly budget replacement probes at a full
    // pool. General discovery may fill spare capacity, but it must not queue
    // an unbounded set of cap-evicting candidates.
    if (endpoint_covers_preferred_ledger(key))
    {
        return is_archival_endpoint(key) ? !at_archival_cap()
                                         : !at_connection_cap();
    }

    // Archival peers have their own pool — separate cap from hubs.
    if (is_archival_endpoint(key))
    {
        if (at_archival_cap())
            return false;
        // Below archival cap — always connect
        return true;
    }

    // Hub peers — check hub cap
    if (at_connection_cap())
        return false;

    // If crawl already had a chance and still did not give us range data,
    // fall back to a real peer handshake rather than starving on crawl-only
    // nodes.
    if (auto crawled = crawled_.find(key); crawled != crawled_.end() &&
        now - crawled->second < kCrawlRetention && !endpoint_has_range(key))
    {
        return true;
    }

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
    ASSERT_ON_STRAND();
    static EndpointStats const empty{};

    auto lhs_it = endpoint_stats_.find(lhs);
    auto rhs_it = endpoint_stats_.find(rhs);
    auto const& lhs_stats =
        lhs_it != endpoint_stats_.end() ? lhs_it->second : empty;
    auto const& rhs_stats =
        rhs_it != endpoint_stats_.end() ? rhs_it->second : empty;

    auto const lhs_has_range =
        lhs_stats.status.first_seq != 0 && lhs_stats.status.last_seq != 0;
    auto const rhs_has_range =
        rhs_stats.status.first_seq != 0 && rhs_stats.status.last_seq != 0;

    // Check if either covers any wanted ledger
    auto covers_any_wanted = [&](EndpointStats const& stats) {
        if (stats.status.first_seq == 0 || stats.status.last_seq == 0)
            return false;
        for (auto const& [seq, _] : wanted_ledgers_)
        {
            if (seq >= stats.status.first_seq && seq <= stats.status.last_seq)
                return true;
        }
        return false;
    };
    auto const lhs_covers = covers_any_wanted(lhs_stats);
    auto const rhs_covers = covers_any_wanted(rhs_stats);

    if (lhs_covers != rhs_covers)
        return lhs_covers;

    auto const lhs_penalized = lhs_stats.last_failure_at > 0 &&
        (lhs_stats.last_success_at == 0 ||
         lhs_stats.last_failure_at > lhs_stats.last_success_at);
    auto const rhs_penalized = rhs_stats.last_failure_at > 0 &&
        (rhs_stats.last_success_at == 0 ||
         rhs_stats.last_failure_at > rhs_stats.last_success_at);
    if (lhs_penalized != rhs_penalized)
        return !lhs_penalized;

    if (lhs_stats.connected_ok != rhs_stats.connected_ok)
        return lhs_stats.connected_ok;

    auto const lhs_success = lhs_stats.last_success_at > 0;
    auto const rhs_success = rhs_stats.last_success_at > 0;
    if (lhs_success != rhs_success)
        return lhs_success;

    if (lhs_has_range != rhs_has_range)
        return lhs_has_range;

    if (lhs_stats.ledger_failure_count != rhs_stats.ledger_failure_count)
    {
        return lhs_stats.ledger_failure_count < rhs_stats.ledger_failure_count;
    }

    auto const lhs_span = lhs_stats.status.last_seq > lhs_stats.status.first_seq
        ? static_cast<uint64_t>(
              lhs_stats.status.last_seq - lhs_stats.status.first_seq)
        : 0;
    auto const rhs_span = rhs_stats.status.last_seq > rhs_stats.status.first_seq
        ? static_cast<uint64_t>(
              rhs_stats.status.last_seq - rhs_stats.status.first_seq)
        : 0;
    if (lhs_span != rhs_span)
        return lhs_span > rhs_span;

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
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this()]() {
            self->sort_pending_connects();
        });
        return;
    }
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
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this()]() {
            self->sort_pending_crawls();
        });
        return;
    }
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
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), endpoint]() {
            self->queue_connect(endpoint);
        });
        return;
    }
    auto key = canonical_endpoint(endpoint);
    auto const now = std::chrono::steady_clock::now();

    if (connections_.count(key) || in_flight_.count(key) || queued_.count(key))
        return;

    auto failed = failed_at_.find(key);
    if (failed != failed_at_.end())
    {
        auto backoff = retry_backoff_for(key);
        if (now - failed->second < backoff)
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
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), endpoint]() {
            self->queue_crawl(endpoint);
        });
        return;
    }
    if (options_.max_in_flight_crawls == 0)
        return;

    auto key = canonical_endpoint(endpoint);
    auto const now = std::chrono::steady_clock::now();
    if (key.empty() || crawl_in_flight_.count(key) || crawl_queued_.count(key))
    {
        return;
    }
    if (auto crawled = crawled_.find(key);
        crawled != crawled_.end() && now - crawled->second < kCrawlRetention)
    {
        return;
    }
    if (auto stats = endpoint_stats_.find(key); stats != endpoint_stats_.end())
    {
        if (stats->second.crawl_private_until > now)
            return;
        stats->second.crawl_private_until = {};
    }

    pending_crawls_.push_back(key);
    crawl_queued_.insert(key);
    sort_pending_crawls();
}

void
PeerSet::pump_connects()
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(
            strand_, [self = shared_from_this()]() { self->pump_connects(); });
        return;
    }
    while (in_flight_.size() < options_.max_in_flight_connects &&
           !pending_connects_.empty())
    {
        auto key = pending_connects_.front();
        pending_connects_.erase(pending_connects_.begin());
        queued_.erase(key);
        std::vector<TargetedAdmissionLeasePtr> admission_leases;
        if (auto targeted = targeted_connects_.find(key);
            targeted != targeted_connects_.end())
        {
            for (auto& authorization : targeted->second)
            {
                if (authorization)
                {
                    if (auto lease = authorization->claim())
                        admission_leases.push_back(std::move(lease));
                }
            }
            targeted_connects_.erase(targeted);
        }

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
            if (now - failed->second < retry_backoff_for(key))
            {
                continue;
            }
            failed_at_.erase(failed);
        }

        in_flight_.insert(key);

        auto self = shared_from_this();
        boost::asio::co_spawn(
            strand_,
            [self,
             host,
             port,
             key,
             admission_leases = std::move(
                 admission_leases)]() mutable -> boost::asio::awaitable<void> {
                co_await self->try_connect_on_strand(
                    host, port, std::move(admission_leases));
                self->in_flight_.erase(key);
                self->pump_connects();
            },
            boost::asio::detached);
    }
}

void
PeerSet::pump_crawls()
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(
            strand_, [self = shared_from_this()]() { self->pump_crawls(); });
        return;
    }
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

        auto const now = std::chrono::steady_clock::now();
        if (auto crawled = crawled_.find(key); crawl_in_flight_.count(key) ||
            (crawled != crawled_.end() &&
             now - crawled->second < kCrawlRetention))
        {
            continue;
        }

        crawl_in_flight_.insert(key);

        auto self = shared_from_this();
        boost::asio::co_spawn(
            strand_,
            [self, host, port, key]() -> boost::asio::awaitable<void> {
                try
                {
                    auto response = co_await co_fetch_peer_crawl(host, port);

                    std::size_t accepted = 0;
                    std::size_t ranged = 0;
                    std::size_t filtered = 0;
                    for (auto const& peer : response.peers)
                    {
                        if (peer.endpoint.empty())
                            continue;

                        auto normalized =
                            EndpointTracker::normalize_discovered_endpoint(
                                peer.endpoint);
                        if (!normalized)
                        {
                            ++filtered;
                            continue;
                        }

                        ++accepted;

                        // Mark as seen in crawl
                        if (self->endpoint_cache_)
                        {
                            try
                            {
                                self->endpoint_cache_->remember_seen_crawl(
                                    self->network_id_, *normalized);
                            }
                            catch (...)
                            {
                            }
                        }

                        if (auto status = self->choose_crawl_status(
                                peer.complete_ledgers))
                        {
                            ++ranged;
                            self->tracker_->update(*normalized, *status);
                        }
                        else
                        {
                            self->tracker_->add_discovered(
                                *normalized, DiscoverySource::Crawl);
                        }
                    }

                    PLOGI(
                        log_,
                        "[",
                        self->net_label_,
                        "] ",
                        "Crawled ",
                        key,
                        " via ",
                        response.used_tls ? "HTTPS" : "HTTP",
                        ": ",
                        response.peers.size(),
                        " raw peers, ",
                        accepted,
                        " accepted public peers, ",
                        ranged,
                        " with ledger ranges, ",
                        filtered,
                        " filtered");
                }
                catch (std::exception const& e)
                {
                    auto const summary = summarize_crawl_error(e.what());
                    PLOGW(
                        log_,
                        "[",
                        self->net_label_,
                        "] Crawl failed for ",
                        key,
                        ": ",
                        summary);
                    self->note_crawl_failure(key, summary);
                }

                self->crawl_in_flight_.erase(key);
                self->crawled_[key] = std::chrono::steady_clock::now();

                if (self->should_connect_endpoint(key))
                {
                    self->queue_connect(key);
                }

                self->prune_discovery_state();
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
        PLOGW(log_, "[", net_label_, "] Peer cache count failed: ", e.what());
        return;
    }

    auto cached = endpoint_cache_->load_bootstrap_candidates(
        network_id_, options_.cached_endpoint_limit);
    PLOGI(
        log_,
        "[",
        net_label_,
        "] Peer cache has ",
        total_cached,
        " endpoints for network ",
        network_id_,
        "; loading ",
        cached.size(),
        " into bootstrap");

    std::size_t filtered = 0;
    std::size_t loaded = 0;
    std::size_t loaded_with_range = 0;
    std::size_t loaded_with_success = 0;
    std::size_t loaded_connected_ok = 0;
    std::size_t loaded_seen_crawl = 0;
    std::size_t loaded_seen_endpoints = 0;
    std::size_t loaded_seen_redirect = 0;
    for (auto const& entry : cached)
    {
        auto normalized =
            EndpointTracker::normalize_discovered_endpoint(entry.endpoint);
        if (!normalized)
        {
            ++filtered;
            continue;
        }

        auto normalized_entry = entry;
        normalized_entry.endpoint = *normalized;
        update_endpoint_stats(normalized_entry);
        auto const& key = *normalized;
        if (entry.status.first_seq != 0 && entry.status.last_seq != 0)
        {
            tracker_->update(key, entry.status);
            ++loaded_with_range;
        }
        else
        {
            tracker_->add_discovered(key);
        }
        if (entry.last_success_at > 0)
            ++loaded_with_success;
        if (entry.connected_ok)
            ++loaded_connected_ok;
        if (entry.seen_crawl)
            ++loaded_seen_crawl;
        if (entry.seen_endpoints)
            ++loaded_seen_endpoints;
        if (entry.seen_redirect)
            ++loaded_seen_redirect;
        ++loaded;
    }
    if (!cached.empty())
    {
        PLOGI(
            log_,
            "[",
            net_label_,
            "] Peer cache path: ",
            endpoint_cache_->path());
        PLOGI(
            log_,
            "[",
            net_label_,
            "] Cached bootstrap summary: loaded=",
            loaded,
            " ranged=",
            loaded_with_range,
            " success=",
            loaded_with_success,
            " connected_ok=",
            loaded_connected_ok,
            " seen_crawl=",
            loaded_seen_crawl,
            " seen_endpoints=",
            loaded_seen_endpoints,
            " seen_redirect=",
            loaded_seen_redirect);

        auto const detail_count = std::min<std::size_t>(cached.size(), 5);
        for (std::size_t i = 0; i < detail_count; ++i)
        {
            auto const& entry = cached[i];
            auto normalized =
                EndpointTracker::normalize_discovered_endpoint(entry.endpoint);
            if (!normalized)
                continue;

            PLOGI(
                log_,
                "[",
                net_label_,
                "] Cached candidate #",
                i + 1,
                ": ",
                *normalized,
                " connected_ok=",
                entry.connected_ok ? 1 : 0,
                " success=",
                entry.success_count,
                " fail=",
                entry.failure_count,
                " source=",
                discovery_source_label(
                    entry.seen_crawl,
                    entry.seen_endpoints,
                    entry.seen_redirect),
                " range=",
                entry.status.first_seq,
                "-",
                entry.status.last_seq);
        }
    }
    if (filtered > 0)
    {
        PLOGI(
            log_,
            "[",
            net_label_,
            "] Ignored ",
            filtered,
            " non-public cached endpoints while loading ",
            loaded,
            " bootstrap candidates");
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
    tracker_->set_discovered_observer([self, cache, network_id](
                                          std::string const& endpoint,
                                          DiscoverySource source) {
        asio::post(
            self->strand_, [self, cache, network_id, endpoint, source]() {
                self->note_discovered(endpoint, source);
                if (cache)
                {
                    try
                    {
                        cache->remember_discovered(network_id, endpoint);
                        if (source == DiscoverySource::Endpoints)
                        {
                            cache->remember_seen_endpoints(
                                network_id, endpoint);
                        }
                        else if (source == DiscoverySource::Redirect)
                        {
                            cache->remember_seen_redirect(network_id, endpoint);
                        }
                    }
                    catch (std::exception const& e)
                    {
                        PLOGW(
                            log_,
                            "[",
                            self->net_label_,
                            "] ",
                            "Peer cache write failed for ",
                            endpoint,
                            ": ",
                            e.what());
                    }
                }
            });
    });

    tracker_->set_status_observer([self, cache, network_id](
                                      std::string const& endpoint,
                                      PeerStatus const& status) {
        asio::post(
            self->strand_, [self, cache, network_id, endpoint, status]() {
                self->note_status(endpoint, status);
                if (cache)
                {
                    try
                    {
                        cache->remember_status(network_id, endpoint, status);
                    }
                    catch (std::exception const& e)
                    {
                        PLOGW(
                            log_,
                            "[",
                            self->net_label_,
                            "] ",
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
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), endpoint]() {
            self->start_connect(endpoint);
        });
        return;
    }
    queue_connect(endpoint);
    pump_connects();
}

void
PeerSet::start_connect(std::string const& host, uint16_t port)
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), host, port]() {
            self->start_connect(host, port);
        });
        return;
    }
    queue_connect(make_key(host, port));
    pump_connects();
}

boost::asio::awaitable<PeerSessionPtr>
PeerSet::try_connect(std::string const& host, uint16_t port)
{
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self,
         host = std::string(host),
         port]() mutable -> asio::awaitable<PeerSessionPtr> {
            co_return co_await self->try_connect_on_strand(
                std::move(host), port, {});
        },
        asio::use_awaitable);
}

boost::asio::awaitable<PeerSessionPtr>
PeerSet::try_connect_on_strand(
    std::string host,
    uint16_t port,
    std::vector<TargetedAdmissionLeasePtr> admission_leases)
{
    ASSERT_ON_STRAND();

    auto key = make_key(host, port);

    // Already connected? This can change while co_connect() is suspended,
    // so the result is checked again before insertion below.
    auto it = connections_.find(key);
    if (it != connections_.end())
    {
        co_return (it->second && it->second->is_ready()) ? it->second : nullptr;
    }

    // Cached crawl data is only a pre-connect hint. A successful handshake is
    // the authority for pool classification and admission below.
    bool expected_archival = is_archival_endpoint(key);
    bool const expected_pool_full =
        expected_archival ? at_archival_cap() : at_connection_cap();
    auto const has_active_authorization = std::any_of(
        admission_leases.begin(),
        admission_leases.end(),
        [](auto const& lease) { return lease && lease->is_available(); });
    if (endpoint_has_range(key) && expected_pool_full &&
        !has_active_authorization)
    {
        PLOGD(
            log_,
            "[",
            net_label_,
            "] Skipping surplus connection candidate at pool cap: ",
            key);
        co_return nullptr;
    }

    PLOGI(
        log_,
        "[",
        net_label_,
        "] Connecting to ",
        key,
        expected_archival ? " (archival candidate)" : "",
        " [hubs=",
        hub_peer_count(),
        "/",
        options_.max_connected_peers,
        " archival=",
        archival_peer_count(),
        "/",
        options_.max_archival_peers,
        "]");

    std::shared_ptr<PeerClient> client;

    try
    {
        // Pass all handlers via ConnectOptions so they're installed
        // before the read loop starts — no cross-strand races.
        PeerClient::ConnectOptions connect_opts;
        connect_opts.network_id = network_id_;
        connect_opts.tracker = tracker_;
        {
            std::lock_guard lock(unsolicited_handler_mutex_);
            connect_opts.unsolicited_handler = unsolicited_handler_;
        }
        {
            auto self_weak = weak_from_this();
            auto owned_key2 = key;
            connect_opts.on_disconnect = [self_weak, owned_key2]() {
                if (auto self2 = self_weak.lock())
                {
                    asio::post(self2->strand_, [self2, owned_key2]() {
                        self2->remove_peer(owned_key2);
                    });
                }
            };
        }

        co_await co_connect(io_, host, port, std::move(connect_opts), client);
        ASSERT_ON_STRAND();

        // Re-check because another strand operation may have inserted this
        // key while the connection attempt was suspended.
        // Drop ours — first one wins.
        if (auto existing = connections_.find(key);
            existing != connections_.end())
        {
            PLOGD(
                log_,
                "[",
                net_label_,
                "] Discarding duplicate connection for ",
                key);
            client->disconnect();
            co_return existing->second;
        }

        if (!client || !client->is_ready())
        {
            PLOGD(
                log_,
                "[",
                net_label_,
                "] Connected candidate became unavailable before admission: ",
                key);
            if (client)
                client->disconnect();
            co_return nullptr;
        }

        if (!make_room_for_peer(key, client, admission_leases))
        {
            PLOGI(
                log_,
                "[",
                net_label_,
                "] Connected candidate not admitted: ",
                key,
                " (",
                peer_is_archival(client) ? "archival" : "hub",
                " pool at cap)");
            client->disconnect();
            co_return nullptr;
        }

        connections_[key] = client;
        ++total_connects_;
        failed_at_.erase(key);
        note_connect_success(
            key,
            {.first_seq = client->peer_first_seq(),
             .last_seq = client->peer_last_seq(),
             .current_seq = client->peer_ledger_seq()});
        note_peer_headers(key, client->peer_headers());
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
                    log_,
                    "[",
                    net_label_,
                    "] Peer cache write failed for ",
                    key,
                    ": ",
                    e.what());
            }
        }
        PLOGI(
            log_,
            "[",
            net_label_,
            "] Connected to ",
            key,
            " (range: ",
            client->peer_first_seq(),
            "-",
            client->peer_last_seq(),
            ")");
        co_return client;
    }
    catch (PeerClientException const& e)
    {
        auto redirected = false;
        std::size_t redirect_count = 0;
        std::size_t filtered = 0;

        // Harvest 503 redirect IPs
        if (client)
        {
            auto const& ips = client->raw_connection().redirect_ips();
            redirect_count = ips.size();
            redirected = redirect_count > 0;
            for (auto const& ip : ips)
            {
                auto normalized =
                    EndpointTracker::normalize_discovered_host(ip, port);
                if (!normalized)
                {
                    ++filtered;
                    continue;
                }
                tracker_->add_discovered(
                    *normalized, DiscoverySource::Redirect);
            }
            if (redirected)
            {
                auto detail = trim_connect_failure_detail(e.detail);
                PLOGI(
                    log_,
                    "[",
                    net_label_,
                    "] Connect redirected by ",
                    key,
                    ": ",
                    detail.empty() ? std::string("redirect_peers=") +
                            std::to_string(redirect_count)
                                   : detail);
                if (filtered > 0)
                {
                    PLOGI(
                        log_,
                        "[",
                        net_label_,
                        "] Ignored ",
                        filtered,
                        " non-public redirect peers from ",
                        key);
                }
                // Immediately try the redirect peers
                try_undiscovered();
            }
        }

        if (!redirected)
        {
            auto const detail = trim_connect_failure_detail(e.detail);
            PLOGW(
                log_,
                "[",
                net_label_,
                "] Failed to connect to ",
                key,
                ": ",
                e.what());
            note_connect_failure(key, detail.empty() ? e.what() : detail);
        }
        else
        {
            auto const detail = trim_connect_failure_detail(e.detail);
            note_connect_failure(key, detail.empty() ? e.what() : detail);
        }
        failed_at_[key] = std::chrono::steady_clock::now();
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
                    "[",
                    net_label_,
                    "] ",
                    "Peer cache write failed for ",
                    key,
                    ": ",
                    cache_error.what());
            }
        }

        // Break shared_ptr cycle: PeerClient <-> peer_connection
        if (client)
            client->disconnect();
        co_return nullptr;
    }
    catch (std::exception const& e)
    {
        PLOGW(
            log_,
            "[",
            net_label_,
            "] Failed to connect to ",
            key,
            ": ",
            e.what());
        failed_at_[key] = std::chrono::steady_clock::now();
        note_connect_failure(key, e.what());
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
                    "[",
                    net_label_,
                    "] ",
                    "Peer cache write failed for ",
                    key,
                    ": ",
                    cache_error.what());
            }
        }
        // Break shared_ptr cycle: PeerClient <-> peer_connection
        if (client)
            client->disconnect();
        co_return nullptr;
    }
}

void
PeerSet::bootstrap()
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(
            strand_, [self = shared_from_this()]() { self->bootstrap(); });
        return;
    }
    auto const& boot_peers = get_bootstrap_peers(network_id_);
    auto tracked_endpoints = tracker_->all_endpoints();

    if (boot_peers.empty() && tracked_endpoints.empty())
    {
        PLOGW(
            log_,
            "[",
            net_label_,
            "] No bootstrap peers for network ",
            network_id_);
        return;
    }

    PLOGI(
        log_,
        "[",
        net_label_,
        "] Bootstrapping: ",
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
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this()]() {
            self->start_tracked_endpoints();
        });
        return;
    }
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
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this()]() {
            self->try_undiscovered();
        });
        return;
    }
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
            "[",
            net_label_,
            "] Queued ",
            crawls,
            " crawl jobs and ",
            connects,
            " connect attempts from discovered endpoints (",
            in_flight_.size(),
            "/",
            options_.max_in_flight_connects,
            " in flight)");
    }

    prune_endpoint_stats();
    prune_discovery_state();
    pump_crawls();
    pump_connects();
}

void
PeerSet::prioritize_ledger(uint32_t ledger_seq)
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), ledger_seq]() {
            self->prioritize_ledger(ledger_seq);
        });
        return;
    }
    if (prioritized_ledgers_.insert(ledger_seq).second)
        ++wanted_ledgers_[ledger_seq];
    while (prioritized_ledgers_.size() > 10)
    {
        auto const demoted = *prioritized_ledgers_.begin();
        prioritized_ledgers_.erase(prioritized_ledgers_.begin());
        release_wanted_ledger(demoted);
    }
    sort_pending_connects();
    sort_pending_crawls();
    pump_crawls();
    pump_connects();
}

PeerSet::TargetedAdmissionLeasePtr
PeerSet::TargetedConnectAuthorization::claim()
{
    std::lock_guard lock(leases_mutex);
    if (!accepting.load(std::memory_order_acquire))
        return nullptr;

    auto lease = std::make_shared<TargetedAdmissionLease>(shared_from_this());
    leases.emplace_back(lease);
    return lease;
}

void
PeerSet::TargetedConnectAuthorization::close(bool revoke_claimed) noexcept
{
    accepting.store(false, std::memory_order_release);
    if (!revoke_claimed)
        return;

    std::lock_guard lock(leases_mutex);
    for (auto it = leases.begin(); it != leases.end();)
    {
        if (auto lease = it->lock())
        {
            lease->revoke();
            ++it;
        }
        else
        {
            it = leases.erase(it);
        }
    }
}

void
PeerSet::acquire_wanted_ledger(
    uint32_t ledger_seq,
    TargetedConnectAuthorizationPtr const& authorization)
{
    ASSERT_ON_STRAND();
    ++wanted_ledgers_[ledger_seq];
    if (authorization)
        active_target_authorizations_.push_back(authorization);
}

void
PeerSet::release_wanted_ledger(
    uint32_t ledger_seq,
    TargetedConnectAuthorizationPtr const& authorization)
{
    ASSERT_ON_STRAND();
    if (authorization)
    {
        auto it = std::find(
            active_target_authorizations_.begin(),
            active_target_authorizations_.end(),
            authorization);
        if (it != active_target_authorizations_.end())
            active_target_authorizations_.erase(it);
    }

    auto it = wanted_ledgers_.find(ledger_seq);
    if (it == wanted_ledgers_.end())
        return;
    if (it->second > 1)
        --it->second;
    else
        wanted_ledgers_.erase(it);
}

void
PeerSet::finish_targeted_authorization(
    TargetedConnectAuthorizationPtr const& authorization,
    bool revoke_claimed)
{
    ASSERT_ON_STRAND();
    if (!authorization)
        return;
    authorization->close(revoke_claimed);
    for (auto it = targeted_connects_.begin(); it != targeted_connects_.end();)
    {
        auto& authorizations = it->second;
        authorizations.erase(
            std::remove_if(
                authorizations.begin(),
                authorizations.end(),
                [&](auto const& current) {
                    return !current || !current->is_active() ||
                        current == authorization;
                }),
            authorizations.end());
        if (authorizations.empty())
            it = targeted_connects_.erase(it);
        else
            ++it;
    }
}

std::vector<std::string>
PeerSet::try_candidates_for(
    TargetedConnectAuthorizationPtr const& authorization,
    std::unordered_set<std::string> const& selection_excluded,
    std::size_t connect_budget)
{
    ASSERT_ON_STRAND();
    if (!authorization || !authorization->is_active())
        return {};
    auto const ledger_seq = authorization->ledger_seq;

    auto candidates = tracker_->all_endpoints();
    for (auto const& endpoint : candidates)
        queue_crawl(endpoint);

    auto const now = std::chrono::steady_clock::now();
    candidates.erase(
        std::remove_if(
            candidates.begin(),
            candidates.end(),
            [&](std::string const& raw_endpoint) {
                auto const key = canonical_endpoint(raw_endpoint);
                if (key.empty() || selection_excluded.count(key) ||
                    connections_.count(key) || in_flight_.count(key) ||
                    peer_failed_for_ledger(key, ledger_seq, now))
                {
                    return true;
                }
                auto failed = failed_at_.find(key);
                return failed != failed_at_.end() &&
                    now - failed->second < retry_backoff_for(key);
            }),
        candidates.end());

    auto range_for = [&](std::string const& raw_endpoint) {
        auto const key = canonical_endpoint(raw_endpoint);
        auto stats = endpoint_stats_.find(key);
        return stats != endpoint_stats_.end() ? stats->second.status
                                              : PeerStatus{};
    };
    std::sort(
        candidates.begin(),
        candidates.end(),
        [&](std::string const& lhs, std::string const& rhs) {
            auto const lhs_key = canonical_endpoint(lhs);
            auto const rhs_key = canonical_endpoint(rhs);
            auto const lhs_status = range_for(lhs_key);
            auto const rhs_status = range_for(rhs_key);
            auto const lhs_covers = lhs_status.first_seq != 0 &&
                lhs_status.last_seq != 0 &&
                ledger_seq >= lhs_status.first_seq &&
                ledger_seq <= lhs_status.last_seq;
            auto const rhs_covers = rhs_status.first_seq != 0 &&
                rhs_status.last_seq != 0 &&
                ledger_seq >= rhs_status.first_seq &&
                ledger_seq <= rhs_status.last_seq;
            if (lhs_covers != rhs_covers)
                return lhs_covers;

            auto const lhs_archival = is_archival_endpoint(lhs_key);
            auto const rhs_archival = is_archival_endpoint(rhs_key);
            if (lhs_archival != rhs_archival)
                return lhs_archival;

            auto const lhs_span = lhs_status.last_seq > lhs_status.first_seq
                ? static_cast<uint64_t>(
                      lhs_status.last_seq - lhs_status.first_seq)
                : 0;
            auto const rhs_span = rhs_status.last_seq > rhs_status.first_seq
                ? static_cast<uint64_t>(
                      rhs_status.last_seq - rhs_status.first_seq)
                : 0;
            if (lhs_span != rhs_span)
                return lhs_span > rhs_span;
            return candidate_better(lhs_key, rhs_key);
        });

    std::vector<std::string> queued;
    for (auto const& endpoint : candidates)
    {
        if (queued.size() >= connect_budget)
            break;
        auto const key = canonical_endpoint(endpoint);
        if (queued_.count(key))
        {
            auto& authorizations = targeted_connects_[key];
            if (std::find(
                    authorizations.begin(),
                    authorizations.end(),
                    authorization) == authorizations.end())
            {
                authorizations.push_back(authorization);
            }
            queued.push_back(key);
            continue;
        }
        auto const before = queued_.size();
        queue_connect(key);
        if (queued_.size() != before)
        {
            targeted_connects_[key].push_back(authorization);
            queued.push_back(key);
        }
    }

    sort_pending_connects();
    sort_pending_crawls();
    pump_crawls();
    pump_connects();
    return queued;
}

boost::asio::awaitable<PeerSessionPtr>
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

PeerSessionPtr
PeerSet::peer_for(uint32_t ledger_seq) const
{
    ASSERT_ON_STRAND();
    static std::unordered_set<std::string> const empty;
    return peer_for(ledger_seq, empty);
}

PeerSessionPtr
PeerSet::peer_for(
    uint32_t ledger_seq,
    std::unordered_set<std::string> const& excluded) const
{
    ASSERT_ON_STRAND();
    PeerSessionPtr best;
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
    return nullptr;
}

boost::asio::awaitable<PeerSessionPtr>
PeerSet::wait_for_any_peer(int timeout_secs)
{
    static std::unordered_set<std::string> const empty;
    co_return co_await wait_for_any_peer(timeout_secs, empty);
}

boost::asio::awaitable<PeerSessionPtr>
PeerSet::wait_for_any_peer(
    int timeout_secs,
    std::unordered_set<std::string> const& excluded)
{
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self,
         timeout_secs,
         excluded]() mutable -> asio::awaitable<PeerSessionPtr> {
            co_return co_await self->wait_for_any_peer_on_strand(
                timeout_secs, std::move(excluded));
        },
        asio::use_awaitable);
}

boost::asio::awaitable<PeerSessionPtr>
PeerSet::wait_for_any_peer_on_strand(
    int timeout_secs,
    std::unordered_set<std::string> excluded)
{
    ASSERT_ON_STRAND();

    if (auto client = choose_any_peer(excluded))
    {
        co_return client;
    }

    // Pick up any redirect or TMEndpoints discoveries we already know about
    // before we begin waiting.
    start_tracked_endpoints();
    try_undiscovered();

    PLOGI(
        log_,
        "[",
        net_label_,
        "] Waiting for any ready peer (",
        connections_.size(),
        " connected, ",
        in_flight_.size(),
        " in-flight, ",
        tracker_->size(),
        " known, Ctrl-C to cancel)");

    auto const start = std::chrono::steady_clock::now();
    auto const deadline = start + std::chrono::seconds(timeout_secs);
    auto next_maintenance = start + std::chrono::seconds(1);
    auto next_log = start + std::chrono::seconds(5);

    for (;;)
    {
        auto const now = std::chrono::steady_clock::now();

        if (now >= next_maintenance)
        {
            start_tracked_endpoints();
            try_undiscovered();
            while (next_maintenance <= now)
                next_maintenance += std::chrono::seconds(1);
        }

        if (auto client = choose_any_peer(excluded))
        {
            co_return client;
        }

        if (now >= deadline)
        {
            break;
        }

        if (now >= next_log)
        {
            PLOGI(
                log_,
                "[",
                net_label_,
                "] Still waiting for any peer (",
                connections_.size(),
                " connected, ",
                in_flight_.size(),
                " in-flight, ",
                tracker_->size(),
                " known)");
            while (next_log <= now)
                next_log += std::chrono::seconds(5);
        }

        auto wait_until = std::min({deadline, next_maintenance, next_log});
        auto wait_signal = attach_wait_signal();
        auto const wait_for =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                wait_until - std::chrono::steady_clock::now());
        co_await wait_for_signal_or_timeout(strand_, wait_signal, wait_for);
        ASSERT_ON_STRAND();
    }

    PLOGW(
        log_,
        "[",
        net_label_,
        "] No ready peer found after ",
        timeout_secs,
        "s");
    co_return nullptr;
}

boost::asio::awaitable<PeerSessionPtr>
PeerSet::wait_for_peer(
    uint32_t ledger_seq,
    int timeout_secs,
    std::shared_ptr<std::atomic<bool>> cancel)
{
    static std::unordered_set<std::string> const empty;
    co_return co_await wait_for_peer(ledger_seq, timeout_secs, empty, cancel);
}

boost::asio::awaitable<PeerSessionPtr>
PeerSet::wait_for_peer(
    uint32_t ledger_seq,
    int timeout_secs,
    std::unordered_set<std::string> const& excluded,
    std::shared_ptr<std::atomic<bool>> cancel)
{
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self,
         ledger_seq,
         timeout_secs,
         excluded,
         cancel =
             std::move(cancel)]() mutable -> asio::awaitable<PeerSessionPtr> {
            co_return co_await self->wait_for_peer_on_strand(
                ledger_seq,
                timeout_secs,
                std::move(excluded),
                std::move(cancel));
        },
        asio::use_awaitable);
}

boost::asio::awaitable<PeerSessionPtr>
PeerSet::wait_for_peer_on_strand(
    uint32_t ledger_seq,
    int timeout_secs,
    std::unordered_set<std::string> excluded,
    std::shared_ptr<std::atomic<bool>> cancel)
{
    ASSERT_ON_STRAND();

    if (cancel && cancel->load(std::memory_order_relaxed))
        co_return nullptr;

    auto self = shared_from_this();
    auto authorization =
        std::make_shared<TargetedConnectAuthorization>(ledger_seq, excluded);
    struct WaitTargetCleanup
    {
        std::weak_ptr<PeerSet> self;
        asio::strand<asio::io_context::executor_type> strand;
        uint32_t ledger_seq = 0;
        TargetedConnectAuthorizationPtr authorization;
        bool active = true;

        void
        clean_now(bool preserve_claimed = false)
        {
            if (!active)
                return;
            if (auto locked = self.lock())
            {
                locked->finish_targeted_authorization(
                    authorization, !preserve_claimed);
                locked->release_wanted_ledger(ledger_seq, authorization);
            }
            active = false;
        }

        ~WaitTargetCleanup()
        {
            if (!active)
                return;

            // Capability invalidation must not wait behind a queued connect
            // completion. Map/refcount cleanup remains strand-confined.
            if (authorization)
                authorization->invalidate();

            if (strand.running_in_this_thread())
            {
                if (auto locked = self.lock())
                {
                    locked->finish_targeted_authorization(authorization, true);
                    locked->release_wanted_ledger(ledger_seq, authorization);
                }
                active = false;
                return;
            }

            try
            {
                asio::post(
                    strand,
                    [self = std::move(self),
                     ledger_seq = ledger_seq,
                     authorization = std::move(authorization)]() mutable {
                        if (auto locked = self.lock())
                        {
                            locked->finish_targeted_authorization(
                                authorization, true);
                            locked->release_wanted_ledger(
                                ledger_seq, authorization);
                        }
                    });
            }
            catch (...)
            {
            }
        }
    } cleanup{self, strand_, ledger_seq, authorization};

    // Keep a reference-counted target registration for the complete wait so
    // concurrent requests for the same ledger cannot clear each other.
    acquire_wanted_ledger(ledger_seq, authorization);

    auto const fallback_ms = static_cast<int>(options_.peer_fallback.count());
    auto const timeout_ms = timeout_secs * 1000;

    auto snapshot_peers = [&]() -> std::vector<PeerCandidate> {
        auto const now = std::chrono::steady_clock::now();
        std::vector<PeerCandidate> candidates;
        candidates.reserve(connections_.size());
        for (auto const& [key, client] : connections_)
        {
            if (!client)
                continue;
            auto& stats = endpoint_stats_[key];
            // Check if this peer has a recent failure for the target
            bool failed = false;
            auto it = stats.failed_ledgers.find(ledger_seq);
            if (it != stats.failed_ledgers.end())
            {
                if (now - it->second.at < kFailedLedgerTtl)
                    failed = true;
                else
                    stats.failed_ledgers.erase(it);  // expired, clean up
            }
            candidates.push_back(
                {key,
                 client->peer_first_seq(),
                 client->peer_last_seq(),
                 stats.selection_count,
                 client->is_ready(),
                 failed});
        }
        return candidates;
    };

    std::unordered_set<std::string> targeted_candidates;
    std::size_t targeted_connects = 0;
    auto queue_targeted_candidate = [&]() {
        if (targeted_connects >= options_.max_targeted_connect_attempts)
            return;
        auto connect_excluded = excluded;
        connect_excluded.insert(
            targeted_candidates.begin(), targeted_candidates.end());
        auto queued = try_candidates_for(authorization, connect_excluded, 1);
        targeted_connects += queued.size();
        targeted_candidates.insert(queued.begin(), queued.end());
    };

    // Initial check before entering the poll loop
    {
        auto sel = select_peer(
            ledger_seq, snapshot_peers(), excluded, 0, fallback_ms, timeout_ms);
        if (sel.decision == PeerDecision::use_range_match ||
            sel.decision == PeerDecision::use_fallback)
        {
            auto it = connections_.find(sel.endpoint);
            if (it != connections_.end() && it->second)
            {
                PLOGI(
                    log_,
                    "[",
                    net_label_,
                    "] ",
                    sel.decision == PeerDecision::use_range_match
                        ? "Range match: "
                        : "Fallback: ",
                    sel.endpoint,
                    " for ledger ",
                    ledger_seq);
                note_peer_selected(sel.endpoint);
                cleanup.clean_now(sel.decision == PeerDecision::use_fallback);
                co_return it->second;
            }
        }
    }

    // Probe a bounded number of disconnected candidates while this wait is
    // active. Admission may rotate one unsuitable peer in the same pool.
    queue_targeted_candidate();

    PLOGI(
        log_,
        "[",
        net_label_,
        "] Waiting for a peer with ledger ",
        ledger_seq,
        " (",
        connections_.size(),
        " connected, ",
        in_flight_.size(),
        " in-flight, ",
        tracker_->size(),
        " known)");

    auto const start = std::chrono::steady_clock::now();
    auto const deadline = start + std::chrono::milliseconds(timeout_ms);
    auto next_maintenance = start + std::chrono::seconds(1);
    auto next_log = start + std::chrono::seconds(5);

    for (;;)
    {
        if (cancel && cancel->load(std::memory_order_relaxed))
        {
            cleanup.clean_now();
            PLOGD(
                log_,
                "[",
                net_label_,
                "] wait_for_peer canceled for ledger ",
                ledger_seq);
            co_return nullptr;
        }

        auto const now = std::chrono::steady_clock::now();
        auto const elapsed_ms = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(now - start)
                .count());

        if (now >= next_maintenance)
        {
            queue_targeted_candidate();
            while (next_maintenance <= now)
                next_maintenance += std::chrono::seconds(1);
        }

        if (cancel && cancel->load(std::memory_order_relaxed))
        {
            cleanup.clean_now();
            PLOGD(
                log_,
                "[",
                net_label_,
                "] wait_for_peer canceled for ledger ",
                ledger_seq);
            co_return nullptr;
        }

        auto sel = select_peer(
            ledger_seq,
            snapshot_peers(),
            excluded,
            elapsed_ms,
            fallback_ms,
            timeout_ms);

        switch (sel.decision)
        {
            case PeerDecision::use_range_match:
            case PeerDecision::use_fallback: {
                auto it = connections_.find(sel.endpoint);
                if (it != connections_.end() && it->second)
                {
                    PLOGI(
                        log_,
                        "[",
                        net_label_,
                        "] ",
                        sel.decision == PeerDecision::use_range_match
                            ? "Found peer: "
                            : "Fallback peer: ",
                        sel.endpoint,
                        " for ledger ",
                        ledger_seq,
                        " (range ",
                        it->second->peer_first_seq(),
                        "-",
                        it->second->peer_last_seq(),
                        ")");
                    note_peer_selected(sel.endpoint);
                    cleanup.clean_now(
                        sel.decision == PeerDecision::use_fallback);
                    co_return it->second;
                }
                break;  // selected peer vanished, keep polling
            }
            case PeerDecision::give_up:
                cleanup.clean_now();
                PLOGW(
                    log_,
                    "[",
                    net_label_,
                    "] No peer found with ledger ",
                    ledger_seq,
                    " after ",
                    timeout_secs,
                    "s");
                co_return nullptr;

            case PeerDecision::wait:
                break;
        }

        if (now >= deadline)
        {
            break;
        }

        if (now >= next_log)
        {
            PLOGI(
                log_,
                "[",
                net_label_,
                "] Still waiting for ledger ",
                ledger_seq,
                " (",
                connections_.size(),
                " connected, ",
                in_flight_.size(),
                " in-flight, ",
                tracker_->size(),
                " known)");
            while (next_log <= now)
                next_log += std::chrono::seconds(5);
        }

        auto wait_until = std::min({deadline, next_maintenance, next_log});
        auto wait_signal = attach_wait_signal();
        auto const wait_for =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                wait_until - std::chrono::steady_clock::now());
        auto outcome = co_await wait_for_signal_or_timeout(
            strand_, wait_signal, wait_for, cancel);
        ASSERT_ON_STRAND();
        if (outcome == WaitOutcome::canceled)
        {
            cleanup.clean_now();
            PLOGD(
                log_,
                "[",
                net_label_,
                "] wait_for_peer canceled for ledger ",
                ledger_seq);
            co_return nullptr;
        }
    }

    cleanup.clean_now();
    PLOGW(
        log_,
        "[",
        net_label_,
        "] No peer found with ledger ",
        ledger_seq,
        " after ",
        timeout_secs,
        "s");

    co_return nullptr;
}

PeerSessionPtr
PeerSet::any_peer() const
{
    ASSERT_ON_STRAND();
    static std::unordered_set<std::string> const empty;
    return any_peer(empty);
}

PeerSessionPtr
PeerSet::any_peer(std::unordered_set<std::string> const& excluded) const
{
    ASSERT_ON_STRAND();
    PeerSessionPtr best;
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

boost::asio::awaitable<PeerSet::Snapshot>
PeerSet::co_snapshot()
{
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<Snapshot> {
            co_return self->snapshot_unsafe();
        },
        asio::use_awaitable);
}

size_t
PeerSet::connected_count() const
{
    ASSERT_ON_STRAND();
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
    ASSERT_ON_STRAND();
    // Hub cap only — archival peers have their own budget
    return hub_peer_count() >= options_.max_connected_peers;
}

bool
PeerSet::at_archival_cap() const
{
    ASSERT_ON_STRAND();
    return archival_peer_count() >= options_.max_archival_peers;
}

bool
PeerSet::is_archival_endpoint(std::string const& key) const
{
    ASSERT_ON_STRAND();
    auto it = endpoint_stats_.find(key);
    if (it == endpoint_stats_.end())
        return false;
    auto const& s = it->second.status;
    return s.first_seq != 0 && s.last_seq != 0 &&
        (s.last_seq - s.first_seq) > options_.archival_range_threshold;
}

size_t
PeerSet::hub_peer_count() const
{
    ASSERT_ON_STRAND();
    return connected_count() - archival_peer_count();
}

size_t
PeerSet::archival_peer_count() const
{
    ASSERT_ON_STRAND();
    size_t count = 0;
    for (auto const& [_, client] : connections_)
    {
        if (!client || !client->is_ready())
            continue;
        auto first = client->peer_first_seq();
        auto last = client->peer_last_seq();
        if (first != 0 && last != 0 &&
            (last - first) > options_.archival_range_threshold)
        {
            count++;
        }
    }
    return count;
}

bool
PeerSet::peer_is_archival(PeerSessionPtr const& peer) const
{
    ASSERT_ON_STRAND();
    if (!peer || !peer->is_ready())
        return false;
    auto const first = peer->peer_first_seq();
    auto const last = peer->peer_last_seq();
    return first != 0 && last > first &&
        (last - first) > options_.archival_range_threshold;
}

bool
PeerSet::has_usable_covering_peer(
    uint32_t ledger_seq,
    std::unordered_set<std::string> const& excluded) const
{
    ASSERT_ON_STRAND();
    auto const now = std::chrono::steady_clock::now();
    for (auto const& [key, peer] : connections_)
    {
        if (!peer || !peer->is_ready())
            continue;
        if (excluded.count(key))
            continue;
        auto const first = peer->peer_first_seq();
        auto const last = peer->peer_last_seq();
        if (first != 0 && last != 0 && ledger_seq >= first &&
            ledger_seq <= last && !peer_failed_for_ledger(key, ledger_seq, now))
        {
            return true;
        }
    }
    return false;
}

bool
PeerSet::would_drop_wanted_coverage(
    std::string const& victim_key,
    PeerSessionPtr const& victim,
    RotationTarget const& target,
    std::chrono::steady_clock::time_point now) const
{
    ASSERT_ON_STRAND();
    if (!victim)
        return false;

    auto covers = [](PeerSessionPtr const& peer, uint32_t ledger_seq) {
        if (!peer || !peer->is_ready())
            return false;
        auto const first = peer->peer_first_seq();
        auto const last = peer->peer_last_seq();
        return first != 0 && last != 0 && ledger_seq >= first &&
            ledger_seq <= last;
    };
    auto usable = [&](std::string const& key,
                      PeerSessionPtr const& peer,
                      uint32_t ledger_seq,
                      std::unordered_set<std::string> const& excluded) {
        return !excluded.count(key) && covers(peer, ledger_seq) &&
            !peer_failed_for_ledger(key, ledger_seq, now);
    };
    auto candidate_usable =
        [&](uint32_t ledger_seq,
            std::unordered_set<std::string> const& excluded) {
            return target.candidate_first_seq != 0 &&
                target.candidate_last_seq != 0 &&
                ledger_seq >= target.candidate_first_seq &&
                ledger_seq <= target.candidate_last_seq &&
                !excluded.count(target.candidate_key) &&
                !peer_failed_for_ledger(target.candidate_key, ledger_seq, now);
        };
    auto loses_coverage = [&](uint32_t ledger_seq,
                              std::unordered_set<std::string> const& excluded) {
        if (!usable(victim_key, victim, ledger_seq, excluded) ||
            candidate_usable(ledger_seq, excluded))
            return false;

        return !std::any_of(
            connections_.begin(),
            connections_.end(),
            [&](auto const& connection) {
                auto const& [key, peer] = connection;
                return key != victim_key &&
                    usable(key, peer, ledger_seq, excluded);
            });
    };

    std::unordered_map<uint32_t, std::size_t> constrained_counts;
    auto const target_authorization =
        target.lease ? target.lease->authorization : nullptr;
    for (auto const& authorization : active_target_authorizations_)
    {
        if (!authorization || !authorization->is_active())
            continue;
        ++constrained_counts[authorization->ledger_seq];
        if (authorization == target_authorization)
            continue;
        if (loses_coverage(authorization->ledger_seq, authorization->excluded))
            return true;
    }

    static std::unordered_set<std::string> const no_exclusions;
    for (auto const& [wanted_seq, count] : wanted_ledgers_)
    {
        auto const represented = constrained_counts[wanted_seq];
        if (count > represented && loses_coverage(wanted_seq, no_exclusions))
            return true;
    }

    return false;
}

std::optional<PeerSet::RotationTarget>
PeerSet::rotation_target_for(
    std::string const& candidate_key,
    PeerSessionPtr const& candidate,
    std::vector<TargetedAdmissionLeasePtr> const& admission_leases) const
{
    ASSERT_ON_STRAND();
    if (!candidate || !candidate->is_ready())
        return std::nullopt;

    auto const first = candidate->peer_first_seq();
    auto const last = candidate->peer_last_seq();
    auto const candidate_archival = peer_is_archival(candidate);

    auto candidate_covers = [&](uint32_t ledger_seq) {
        return first != 0 && last != 0 && ledger_seq >= first &&
            ledger_seq <= last;
    };
    auto has_known_bad_victim = [&](TargetedConnectAuthorization const& auth) {
        auto const now = std::chrono::steady_clock::now();
        for (auto const& [key, peer] : connections_)
        {
            if (!peer || !peer->is_ready() || peer->pending_count() > 0 ||
                peer_is_archival(peer) != candidate_archival)
            {
                continue;
            }
            if (auth.excluded.count(key) ||
                peer_failed_for_ledger(key, auth.ledger_seq, now))
            {
                return true;
            }
        }
        return false;
    };

    // Prefer a candidate that explicitly advertises the requested ledger.
    for (auto const& lease : admission_leases)
    {
        auto const authorization = lease ? lease->authorization : nullptr;
        if (!lease || !lease->is_available() || !authorization ||
            !candidate_covers(authorization->ledger_seq) ||
            has_usable_covering_peer(
                authorization->ledger_seq, authorization->excluded))
        {
            continue;
        }
        return RotationTarget{
            .ledger_seq = authorization->ledger_seq,
            .excluded = authorization->excluded,
            .candidate_covers = true,
            .candidate_key = candidate_key,
            .candidate_first_seq = first,
            .candidate_last_seq = last,
            .lease = lease,
        };
    }

    // Advertised ranges can be incomplete. A bounded targeted probe may still
    // replace a peer already known to be unusable for this operation.
    for (auto const& lease : admission_leases)
    {
        auto const authorization = lease ? lease->authorization : nullptr;
        if (!lease || !lease->is_available() || !authorization ||
            has_usable_covering_peer(
                authorization->ledger_seq, authorization->excluded) ||
            !has_known_bad_victim(*authorization))
        {
            continue;
        }
        return RotationTarget{
            .ledger_seq = authorization->ledger_seq,
            .excluded = authorization->excluded,
            .candidate_covers = false,
            .candidate_key = candidate_key,
            .candidate_first_seq = first,
            .candidate_last_seq = last,
            .lease = lease,
        };
    }
    return std::nullopt;
}

bool
PeerSet::make_room_for_peer(
    std::string const& candidate_key,
    PeerSessionPtr const& candidate,
    std::vector<TargetedAdmissionLeasePtr> const& admission_leases)
{
    ASSERT_ON_STRAND();
    if (!candidate || !candidate->is_ready())
        return false;

    auto const archival = peer_is_archival(candidate);
    auto const at_cap = archival ? at_archival_cap() : at_connection_cap();
    if (!at_cap)
        return true;

    auto const target =
        rotation_target_for(candidate_key, candidate, admission_leases);
    if (!target)
        return false;

    if (!candidate->is_ready() || !target->lease ||
        !target->lease->try_consume())
        return false;

    auto const evicted =
        archival ? evict_archival_for(*target) : evict_hub_for(*target);
    if (!evicted)
        return false;
    return archival ? !at_archival_cap() : !at_connection_cap();
}

PeerSet::Snapshot
PeerSet::snapshot_unsafe() const
{
    ASSERT_ON_STRAND();

    Snapshot out;
    auto const now = std::chrono::steady_clock::now();
    out.known_endpoints = tracker_->size();
    out.tracked_endpoints = endpoint_stats_.size();
    out.crawled_size = crawled_.size();
    out.failed_at_size = failed_at_.size();
    out.connected_peers = connections_.size();
    out.hub_peers = hub_peer_count();
    out.archival_peers = archival_peer_count();
    out.total_connects = total_connects_;
    out.total_disconnects = total_disconnects_;
    out.in_flight_connects = in_flight_.size();
    out.queued_connects = pending_connects_.size();
    out.crawl_in_flight = crawl_in_flight_.size();
    out.queued_crawls = pending_crawls_.size();
    out.wanted_ledgers.reserve(wanted_ledgers_.size());
    for (auto const& [ledger_seq, _] : wanted_ledgers_)
        out.wanted_ledgers.push_back(ledger_seq);

    {
        std::unordered_map<std::string, std::size_t> kind_counts;
        std::unordered_map<std::string, std::size_t> endpoint_counts;
        auto const cutoff = now - kRecentFailureRetention;

        for (auto const& failure : recent_failures_)
        {
            if (failure.at < cutoff)
                continue;
            kind_counts[failure.kind]++;
            endpoint_counts[failure.endpoint]++;
        }

        out.recent_failures.reserve(kind_counts.size());
        for (auto const& [kind, count] : kind_counts)
        {
            out.recent_failures.push_back(
                Snapshot::FailureBucket{.kind = kind, .count = count});
        }
        std::sort(
            out.recent_failures.begin(),
            out.recent_failures.end(),
            [](Snapshot::FailureBucket const& lhs,
               Snapshot::FailureBucket const& rhs) {
                if (lhs.count != rhs.count)
                    return lhs.count > rhs.count;
                return lhs.kind < rhs.kind;
            });

        out.top_failing_endpoints.reserve(endpoint_counts.size());
        for (auto const& [endpoint, count] : endpoint_counts)
        {
            out.top_failing_endpoints.push_back(Snapshot::FailureEndpoint{
                .endpoint = endpoint,
                .count = count,
            });
        }
        std::sort(
            out.top_failing_endpoints.begin(),
            out.top_failing_endpoints.end(),
            [](Snapshot::FailureEndpoint const& lhs,
               Snapshot::FailureEndpoint const& rhs) {
                if (lhs.count != rhs.count)
                    return lhs.count > rhs.count;
                return lhs.endpoint < rhs.endpoint;
            });
    }

    std::set<std::string> endpoints;
    for (auto const& [key, _] : endpoint_stats_)
        endpoints.insert(key);
    for (auto const& [key, _] : connections_)
        endpoints.insert(key);
    endpoints.insert(in_flight_.begin(), in_flight_.end());
    endpoints.insert(queued_.begin(), queued_.end());
    endpoints.insert(crawl_in_flight_.begin(), crawl_in_flight_.end());
    endpoints.insert(crawl_queued_.begin(), crawl_queued_.end());
    for (auto const& [key, _] : crawled_)
        endpoints.insert(key);

    for (auto const& key : endpoints)
    {
        SnapshotEntry entry;
        entry.endpoint = key;
        entry.in_flight = in_flight_.count(key) > 0;
        entry.queued_connect = queued_.count(key) > 0;
        entry.crawl_in_flight = crawl_in_flight_.count(key) > 0;
        entry.queued_crawl = crawl_queued_.count(key) > 0;
        entry.crawled = crawled_.count(key) > 0;

        if (auto stats_it = endpoint_stats_.find(key);
            stats_it != endpoint_stats_.end())
        {
            auto const& stats = stats_it->second;
            entry.first_seq = stats.status.first_seq;
            entry.last_seq = stats.status.last_seq;
            entry.current_seq = stats.status.current_seq;
            entry.last_seen_at = stats.last_seen_at;
            entry.last_success_at = stats.last_success_at;
            entry.last_failure_at = stats.last_failure_at;
            entry.success_count = stats.success_count;
            entry.failure_count = stats.failure_count;
            entry.ledger_failure_count = stats.ledger_failure_count;
            for (auto const& [_, failure] : stats.failed_ledgers)
            {
                if (now - failure.at < kFailedLedgerTtl)
                    ++entry.recent_failed_ledgers;
            }
            entry.selection_count = stats.selection_count;
            entry.last_selected_ticket = stats.last_selected_ticket;
        }

        if (auto conn_it = connections_.find(key);
            conn_it != connections_.end())
        {
            entry.connected = true;
            if (conn_it->second)
            {
                entry.peer_headers = conn_it->second->peer_headers();
            }
            if (conn_it->second && conn_it->second->is_ready())
            {
                entry.ready = true;
                out.ready_peers++;
            }
        }

        out.peers.push_back(std::move(entry));
    }

    std::sort(
        out.peers.begin(),
        out.peers.end(),
        [](SnapshotEntry const& lhs, SnapshotEntry const& rhs) {
            if (lhs.ready != rhs.ready)
                return lhs.ready > rhs.ready;
            if (lhs.connected != rhs.connected)
                return lhs.connected > rhs.connected;
            if (lhs.selection_count != rhs.selection_count)
                return lhs.selection_count > rhs.selection_count;
            return lhs.endpoint < rhs.endpoint;
        });

    return out;
}

bool
PeerSet::evict_hub_for(RotationTarget const& target)
{
    ASSERT_ON_STRAND();
    std::string worst_key;
    uint32_t worst_range_span = std::numeric_limits<uint32_t>::max();
    bool worst_known_bad = false;
    uint64_t worst_failure_count = 0;
    auto const now = std::chrono::steady_clock::now();

    for (auto const& [key, client] : connections_)
    {
        if (!client || !client->is_ready() || peer_is_archival(client))
            continue;
        if (client->pending_count() > 0)
            continue;

        auto const first = client->peer_first_seq();
        auto const last = client->peer_last_seq();
        auto const covers = target.ledger_seq != 0 && first != 0 && last != 0 &&
            target.ledger_seq >= first && target.ledger_seq <= last;
        auto const failed = peer_failed_for_ledger(key, target.ledger_seq, now);
        auto const excluded = target.excluded.count(key) > 0;
        if (covers && !failed && !excluded)
            continue;
        auto const known_bad = failed || excluded;
        if (!target.candidate_covers && !known_bad)
            continue;
        if (would_drop_wanted_coverage(key, client, target, now))
            continue;

        auto const span = last > first ? last - first : 0;
        auto const failures = endpoint_stats_[key].ledger_failure_count;
        if (worst_key.empty() || (known_bad && !worst_known_bad) ||
            (known_bad == worst_known_bad && failures > worst_failure_count) ||
            (known_bad == worst_known_bad && failures == worst_failure_count &&
             span < worst_range_span))
        {
            worst_key = key;
            worst_range_span = span;
            worst_known_bad = known_bad;
            worst_failure_count = failures;
        }
    }

    if (worst_key.empty())
        return false;

    PLOGI(
        log_,
        "[",
        net_label_,
        "] Rotating idle hub ",
        worst_key,
        " (range span ",
        worst_range_span,
        ") to make room for ledger ",
        target.ledger_seq);

    auto it = connections_.find(worst_key);
    auto victim = it != connections_.end() ? it->second : nullptr;
    remove_peer(worst_key, false);
    if (victim)
        victim->disconnect();
    return true;
}

bool
PeerSet::evict_archival_for(RotationTarget const& target)
{
    ASSERT_ON_STRAND();
    std::string worst_key;
    uint32_t worst_range_span = std::numeric_limits<uint32_t>::max();
    bool worst_known_bad = false;
    uint64_t worst_failure_count = 0;
    auto const now = std::chrono::steady_clock::now();

    for (auto const& [key, client] : connections_)
    {
        if (!client || !client->is_ready() || !peer_is_archival(client) ||
            client->pending_count() > 0)
        {
            continue;
        }

        auto const first = client->peer_first_seq();
        auto const last = client->peer_last_seq();
        auto const covers = target.ledger_seq != 0 && first != 0 && last != 0 &&
            target.ledger_seq >= first && target.ledger_seq <= last;
        auto const failed = peer_failed_for_ledger(key, target.ledger_seq, now);
        auto const excluded = target.excluded.count(key) > 0;
        if (covers && !failed && !excluded)
            continue;
        auto const known_bad = failed || excluded;
        if (!target.candidate_covers && !known_bad)
            continue;
        if (would_drop_wanted_coverage(key, client, target, now))
            continue;

        auto const span = last > first ? last - first : 0;
        auto const failures = endpoint_stats_[key].ledger_failure_count;
        if (worst_key.empty() || (known_bad && !worst_known_bad) ||
            (known_bad == worst_known_bad && failures > worst_failure_count) ||
            (known_bad == worst_known_bad && failures == worst_failure_count &&
             span < worst_range_span))
        {
            worst_key = key;
            worst_range_span = span;
            worst_known_bad = known_bad;
            worst_failure_count = failures;
        }
    }

    if (worst_key.empty())
        return false;

    PLOGI(
        log_,
        "[",
        net_label_,
        "] Rotating idle archival peer ",
        worst_key,
        " (range span ",
        worst_range_span,
        ") to make room for ledger ",
        target.ledger_seq);

    auto it = connections_.find(worst_key);
    auto victim = it != connections_.end() ? it->second : nullptr;
    remove_peer(worst_key, false);
    if (victim)
        victim->disconnect();
    return true;
}

void
PeerSet::remove_peer(std::string const& key, bool forget_endpoint)
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(
            strand_, [self = shared_from_this(), key, forget_endpoint]() {
                self->remove_peer(key, forget_endpoint);
            });
        return;
    }
    auto it = connections_.find(key);
    if (it != connections_.end())
    {
        PLOGI(log_, "[", net_label_, "] Removing dead peer: ", key);
        if (!forget_endpoint)
            tracker_->add_discovered(key);
        connections_.erase(it);
        ++total_disconnects_;
        if (forget_endpoint)
            tracker_->remove(key);
        notify_waiters();
    }
}

void
PeerSet::prune_endpoint_stats()
{
    ASSERT_ON_STRAND();

    // Prune expired failed_ledgers from all entries
    auto const now = std::chrono::steady_clock::now();
    for (auto& [key, stats] : endpoint_stats_)
    {
        for (auto it = stats.failed_ledgers.begin();
             it != stats.failed_ledgers.end();)
        {
            if (now - it->second.at >= kFailedLedgerTtl)
                it = stats.failed_ledgers.erase(it);
            else
                ++it;
        }
    }

    // Cap total endpoint_stats_ entries — keep connected/active, evict stale
    static constexpr size_t kMaxEndpointStats = 2000;
    if (endpoint_stats_.size() <= kMaxEndpointStats)
        return;

    // Build set of endpoints we must keep (connected, in-flight, queued)
    std::unordered_set<std::string> keep;
    for (auto const& [key, _] : connections_)
        keep.insert(key);
    keep.insert(in_flight_.begin(), in_flight_.end());
    keep.insert(queued_.begin(), queued_.end());
    keep.insert(crawl_in_flight_.begin(), crawl_in_flight_.end());
    keep.insert(crawl_queued_.begin(), crawl_queued_.end());

    // Collect eviction candidates (not in keep set)
    struct Candidate
    {
        std::string key;
        std::int64_t last_seen_at;
    };
    std::vector<Candidate> candidates;
    for (auto const& [key, stats] : endpoint_stats_)
    {
        if (!keep.count(key))
            candidates.push_back({key, stats.last_seen_at});
    }

    // Sort by last_seen_at ascending (oldest first)
    std::sort(
        candidates.begin(), candidates.end(), [](auto const& a, auto const& b) {
            return a.last_seen_at < b.last_seen_at;
        });

    // Evict oldest until under cap
    size_t to_evict = endpoint_stats_.size() - kMaxEndpointStats;
    size_t evicted = 0;
    for (auto const& c : candidates)
    {
        if (evicted >= to_evict)
            break;
        endpoint_stats_.erase(c.key);
        ++evicted;
    }

    if (evicted > 0)
    {
        PLOGI(
            log_,
            "[",
            net_label_,
            "] Pruned ",
            evicted,
            " stale endpoint stats (",
            endpoint_stats_.size(),
            " remaining)");
    }
}

void
PeerSet::prune_discovery_state()
{
    ASSERT_ON_STRAND();

    auto const now = std::chrono::steady_clock::now();
    size_t tracker_removed = 0;
    size_t crawled_removed = 0;
    size_t failed_removed = 0;

    for (auto it = crawled_.begin(); it != crawled_.end();)
    {
        if (now - it->second >= kCrawlRetention)
        {
            it = crawled_.erase(it);
            ++crawled_removed;
        }
        else
        {
            ++it;
        }
    }

    for (auto it = failed_at_.begin(); it != failed_at_.end();)
    {
        if (now - it->second >= kFailedEndpointRetention)
        {
            it = failed_at_.erase(it);
            ++failed_removed;
        }
        else
        {
            ++it;
        }
    }

    std::unordered_set<std::string> active;
    for (auto const& [key, _] : connections_)
        active.insert(key);
    active.insert(in_flight_.begin(), in_flight_.end());
    active.insert(queued_.begin(), queued_.end());
    active.insert(crawl_in_flight_.begin(), crawl_in_flight_.end());
    active.insert(crawl_queued_.begin(), crawl_queued_.end());

    if (tracker_->size() > kMaxTrackedEndpoints)
    {
        auto tracked = tracker_->all_endpoints();
        std::sort(
            tracked.begin(),
            tracked.end(),
            [this](std::string const& lhs, std::string const& rhs) {
                return candidate_better(lhs, rhs);
            });

        std::unordered_set<std::string> retained_keys;
        size_t retained_non_active = 0;
        for (auto const& raw_endpoint : tracked)
        {
            auto key = canonical_endpoint(raw_endpoint);
            if (active.count(key))
            {
                if (!retained_keys.insert(key).second)
                {
                    tracker_->remove(raw_endpoint);
                    ++tracker_removed;
                }
                continue;
            }

            if (!retained_keys.insert(key).second)
            {
                tracker_->remove(raw_endpoint);
                ++tracker_removed;
                continue;
            }

            if (retained_non_active < kMaxTrackedEndpoints)
            {
                ++retained_non_active;
                continue;
            }

            retained_keys.erase(key);
            tracker_->remove(raw_endpoint);
            ++tracker_removed;
        }
    }

    std::unordered_set<std::string> tracked_keys;
    for (auto const& endpoint : tracker_->all_endpoints())
        tracked_keys.insert(canonical_endpoint(endpoint));

    for (auto it = crawled_.begin(); it != crawled_.end();)
    {
        if (!active.count(it->first) && !tracked_keys.count(it->first))
        {
            it = crawled_.erase(it);
            ++crawled_removed;
        }
        else
        {
            ++it;
        }
    }

    if (crawled_.size() > kMaxCrawlHistory)
    {
        struct Candidate
        {
            std::string key;
            std::chrono::steady_clock::time_point at;
        };
        std::vector<Candidate> candidates;
        candidates.reserve(crawled_.size());
        for (auto const& [key, at] : crawled_)
        {
            if (!active.count(key))
                candidates.push_back({key, at});
        }
        std::sort(
            candidates.begin(),
            candidates.end(),
            [](Candidate const& lhs, Candidate const& rhs) {
                return lhs.at < rhs.at;
            });
        auto to_remove = crawled_.size() - kMaxCrawlHistory;
        for (auto const& candidate : candidates)
        {
            if (to_remove == 0)
                break;
            if (crawled_.erase(candidate.key) > 0)
            {
                --to_remove;
                ++crawled_removed;
            }
        }
    }

    for (auto it = failed_at_.begin(); it != failed_at_.end();)
    {
        if (!active.count(it->first) && !tracked_keys.count(it->first))
        {
            it = failed_at_.erase(it);
            ++failed_removed;
        }
        else
        {
            ++it;
        }
    }

    if (failed_at_.size() > kMaxTrackedEndpoints)
    {
        struct Candidate
        {
            std::string key;
            std::chrono::steady_clock::time_point at;
        };
        std::vector<Candidate> candidates;
        candidates.reserve(failed_at_.size());
        for (auto const& [key, at] : failed_at_)
        {
            if (!active.count(key))
                candidates.push_back({key, at});
        }
        std::sort(
            candidates.begin(),
            candidates.end(),
            [](Candidate const& lhs, Candidate const& rhs) {
                return lhs.at < rhs.at;
            });
        auto to_remove = failed_at_.size() - kMaxTrackedEndpoints;
        for (auto const& candidate : candidates)
        {
            if (to_remove == 0)
                break;
            if (failed_at_.erase(candidate.key) > 0)
            {
                --to_remove;
                ++failed_removed;
            }
        }
    }

    if (tracker_removed > 0 || crawled_removed > 0 || failed_removed > 0)
    {
        PLOGI(
            log_,
            "[",
            net_label_,
            "] Pruned discovery state: tracker=",
            tracker_removed,
            " crawled=",
            crawled_removed,
            " failed=",
            failed_removed,
            " (known=",
            tracker_->size(),
            " crawled=",
            crawled_.size(),
            " failed=",
            failed_at_.size(),
            ")");
    }

    // Also prune endpoint_stats_ here — it was previously only pruned from
    // try_undiscovered() which doesn't run during idle operation.
    prune_endpoint_stats();
}

}  // namespace catl::peer_client
