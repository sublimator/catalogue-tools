#include <catl/peer-client/peer-client-connect-coro.h>
#include <catl/peer-client/peer-selector.h>
#include <catl/peer-client/peer-set.h>

#include <catl/core/context-executor.h>
#include <catl/core/logger.h>
#include <catl/core/request-context.h>

#include <algorithm>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <cassert>
#include <cctype>
#include <iterator>
#include <limits>
#include <new>
#include <sstream>
#include <stdexcept>
#include <utility>

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

namespace {

std::atomic<std::uint64_t> next_peer_set_instance_id{0};

constexpr std::size_t kGenerationDrainMaxAttempts = 3;
constexpr auto kGenerationDrainRetryDelay = std::chrono::milliseconds(10);

std::uint64_t
mint_peer_set_instance_id() noexcept
{
    auto current = next_peer_set_instance_id.load(std::memory_order_relaxed);
    while (current != std::numeric_limits<std::uint64_t>::max())
    {
        if (next_peer_set_instance_id.compare_exchange_weak(
                current,
                current + 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed))
        {
            return current + 1;
        }
    }
    return 0;
}

}  // namespace

static LogPartition log_("peer-set", LogLevel::INHERIT);

#define PEER_SET_TRACE(...)           \
    do                                \
    {                                 \
        try                           \
        {                             \
            PLOGD(log_, __VA_ARGS__); \
        }                             \
        catch (...)                   \
        {                             \
        }                             \
    } while (false)

enum class WaitOutcome {
    signaled,
    timed_out,
    canceled,
};

std::string
describe_peer_history_coverage(PeerHistoryCoverage const& coverage)
{
    if (!coverage.observed)
        return "peer coverage unavailable";

    std::ostringstream out;
    out << "peers=" << coverage.ready_peers;
    if (coverage.connected_peers != coverage.ready_peers)
        out << " ready/" << coverage.connected_peers << " connected";
    out << ", advertising=" << coverage.advertising_peers
        << ", advertised_history=";
    if (coverage.advertised_ranges.empty())
    {
        out << "none";
        return out.str();
    }

    out << '[';
    for (std::size_t i = 0; i < coverage.advertised_ranges.size(); ++i)
    {
        if (i != 0)
            out << ", ";
        auto const& range = coverage.advertised_ranges[i];
        out << range.first_seq << '-' << range.last_seq;
    }
    out << ']';
    return out.str();
}

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

bool
reported_intervals_touch(
    CrawlLedgerRange const& lhs,
    CrawlLedgerRange const& rhs)
{
    return rhs.first_seq <= lhs.last_seq ||
        (lhs.last_seq != std::numeric_limits<uint32_t>::max() &&
         rhs.first_seq == lhs.last_seq + 1);
}

std::size_t
merge_reported_intervals(
    std::vector<CrawlLedgerRange>& retained,
    std::vector<CrawlLedgerRange> const& incoming)
{
    std::size_t truncated = 0;
    for (auto const& range : incoming)
    {
        auto position = std::lower_bound(
            retained.begin(),
            retained.end(),
            range.first_seq,
            [](CrawlLedgerRange const& current, uint32_t first_seq) {
                return current.last_seq !=
                    std::numeric_limits<uint32_t>::max() &&
                    current.last_seq + 1 < first_seq;
            });

        if (position == retained.end() ||
            !reported_intervals_touch(range, *position))
        {
            if (retained.size() >= kMaxCrawlIntervalsPerPeer)
            {
                ++truncated;
                continue;
            }
            retained.insert(position, range);
            continue;
        }

        position->first_seq = std::min(position->first_seq, range.first_seq);
        position->last_seq = std::max(position->last_seq, range.last_seq);
        auto next = position + 1;
        while (next != retained.end() &&
               reported_intervals_touch(*position, *next))
        {
            position->last_seq = std::max(position->last_seq, next->last_seq);
            next = retained.erase(next);
        }
    }
    return truncated;
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
    , peer_set_instance_id_(mint_peer_set_instance_id())
{
    static constexpr std::size_t kMaxHistoryGlobalTokens = 4096;
    options_.cached_endpoint_limit = std::min(
        options_.cached_endpoint_limit,
        PeerEndpointCache::max_endpoints_per_network);
    options_.max_transient_peer_generations = std::clamp<std::size_t>(
        options_.max_transient_peer_generations, 1, kMaxTrackedEndpoints);
    options_.physical_drain_reserve = std::clamp<std::size_t>(
        options_.physical_drain_reserve, 1, kMaxTrackedEndpoints);
    options_.max_global_history_dials = std::clamp<std::size_t>(
        options_.max_global_history_dials, 1, kMaxHistoryGlobalTokens);
    options_.max_global_history_blind_uses = std::clamp<std::size_t>(
        options_.max_global_history_blind_uses, 1, kMaxHistoryGlobalTokens);
    options_.global_history_token_refill = std::max(
        options_.global_history_token_refill, std::chrono::milliseconds{1});
    options_.max_history_attachments_per_generation =
        std::clamp<std::size_t>(
            options_.max_history_attachments_per_generation,
            1,
            kMaxHistorySearchCandidates);
    options_.history_connect_timeout = std::clamp(
        options_.history_connect_timeout,
        std::chrono::milliseconds{1},
        std::chrono::duration_cast<std::chrono::milliseconds>(
            kOrdinaryConnectDeadline));

    auto prepare_global_tokens = [this](std::size_t count, auto& destination) {
        destination.reserve(count);
        for (std::size_t i = 0; i < count; ++i)
        {
            if (next_history_budget_token_id_ ==
                std::numeric_limits<std::uint64_t>::max())
            {
                throw std::overflow_error(
                    "history global token identity exhausted");
            }
            destination.push_back(LedgerDemandRecord::BudgetToken{
                .id = next_history_budget_token_id_++});
        }
    };
    prepare_global_tokens(
        options_.max_global_history_dials, global_history_dial_tokens_);
    prepare_global_tokens(
        options_.max_global_history_blind_uses, global_history_blind_tokens_);

    for (auto const& bootstrap : get_bootstrap_peers(network_id_))
    {
        if (auto endpoint = EndpointTracker::normalize_endpoint_identity(
                bootstrap.host, bootstrap.port))
        {
            configured_endpoints_.insert(std::move(*endpoint));
        }
    }
    if (!options_.endpoint_cache_path.empty())
    {
        try
        {
            endpoint_cache_ =
                PeerEndpointCache::open(options_.endpoint_cache_path);
        }
        catch (std::exception const& e)
        {
            endpoint_cache_error_ = e.what();
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

//@@authority transition=prv.T0084 status=provisional scope=function
// bind=catl::peer_client::co_stop@a62dab8774ffc758
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0034,prv.S0035,prv.S0036,prv.S0037,prv.S0038,prv.S0062
// label=seal_and_join_peer_sessions_connects_crawls_and_pins
asio::awaitable<void>
PeerSet::co_stop()
{
    co_await asio::this_coro::reset_cancellation_state(
        asio::disable_cancellation());

    // Publish retained stop policy before making the cross-thread seal
    // observable. A crawl already resuming from HTTP failure can then never
    // cross into HTTPS after it observes the sealed boundary.
    // clang-format off
    //@@authority transition=prv.T0190 status=provisional scope=statement
    // bind=expression_statement@ee69b19653a17ebe
    // owner=unresolved owner_hypothesis=peer_set
    // commit=caller_thread_stop_state touches=prv.S0062
    // label=publish_retained_crawl_stop_fact_before_ingress_seal
    //@@mutation-site crawl-stop-fact-precedes-seal
    crawl_stop_source_.request_stop(); //@@authority-target ids=prv.T0190
    PEER_SET_TRACE("[", net_label_, "] Shutdown stop requested");

    // This is the immediate cross-thread admission boundary. Once committed,
    // no projection failure is allowed to reopen it.
    shutdown_ingress_sealed_.store(true, std::memory_order_release);
    if (after_crawl_ingress_seal_for_test_)
        after_crawl_ingress_seal_for_test_();
    //@@mutation-end crawl-stop-fact-precedes-seal
    // clang-format on

    auto self = shared_from_this();
    co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<void> {
            auto progress = std::make_shared<asio::steady_timer>(
                self->strand_, asio::steady_timer::time_point::max());
            self->shutdown_waiters_.push_back(progress);
            self->begin_stop_on_strand();
            // Stop makes every transport callback for a pooled use inert
            // before admitted sessions begin their physical close. The close
            // receipt below remains responsible for draining socket work.
            self->cancel_all_peer_uses_for_shutdown();
            PEER_SET_TRACE(
                "[",
                self->net_label_,
                "] Shutdown drain wait entered: crawl_in_flight=",
                self->crawl_in_flight_.size(),
                " crawl_queued=",
                self->crawl_queued_.size(),
                " pending_crawls=",
                self->pending_crawls_.size());

            // A live generation pin is a predecessor's promise that its exact
            // admitted session remains usable. Closing first would invalidate
            // authority that PeerSet has already exported.
            for (;;)
            {
                self->prune_expired_peer_pins();
                if (self->active_peer_pins_.empty())
                    break;

                // Release normally posts a notification to this strand. The
                // weak handle is nevertheless the durable truth: if that post
                // is lost during executor teardown, periodically re-prune so
                // an expired row cannot wedge shutdown forever.
                progress->expires_after(std::chrono::milliseconds(25));
                self->prune_expired_peer_pins();
                if (self->active_peer_pins_.empty())
                    break;

                boost::system::error_code ec;
                co_await progress->async_wait(
                    asio::redirect_error(asio::use_awaitable, ec));
            }

            // Consume each physical session lifecycle on its own strand. The
            // exact-generation check makes a concurrent natural terminal and
            // this clean shutdown idempotent without misclassifying shutdown
            // as peer failure.
            while (!self->connections_.empty())
            {
                auto const current = self->connections_.begin();
                auto const endpoint = current->first;
                auto const generation = current->second.generation;
                auto const session = current->second.session;

                self->begin_generation_close(endpoint, generation);

                if (session)
                {
                    //@@authority transition=prv.T0147 status=provisional
                    // bind=co_disconnect@fe212b8dead6afae
                    // scope=statement owner=unresolved
                    // owner_hypothesis=peer_set commit=peer_set_strand
                    // touches=prv.S0035,prv.S0046
                    // mutation_class=stop_driver calls=co_disconnect
                    // label=join_exact_session_physical_drain_receipt
                    co_await session
                        ->co_disconnect();  //@@authority-target ids=prv.T0147
                }
                assert(self->strand_.running_in_this_thread());
                self->finish_generation_peer_uses_after_drain(
                    generation, PeerUseOutcome::shutdown);

                if (auto generation_row =
                        self->peer_generations_.find(endpoint);
                    generation_row != self->peer_generations_.end() &&
                    generation_row->second.lifecycle.generation() == generation)
                {
                    generation_row->second.session.reset();
                }

                auto live = self->connections_.find(endpoint);
                if (live != self->connections_.end() &&
                    //@@mutation-site peer-set-stop-exact-generation-guard
                    live->second.generation == generation &&
                    //@@mutation-end peer-set-stop-exact-generation-guard
                    //@@mutation-site peer-set-stop-exact-session-guard
                    live->second.session == session)
                //@@mutation-end peer-set-stop-exact-session-guard
                {
                    self->connections_.erase(live);
                    ++self->total_disconnects_;
                    self->tracker_->remove(endpoint);
                    self->notify_waiters();
                }
            }

            // A deadline may have removed a generation from the selectable
            // connection map before its asynchronous drain could be launched.
            // Stop owns the final fallback: consume every still-retained
            // physical session directly, then acknowledge its uses.
            for (;;)
            {
                auto retained = std::find_if(
                    self->peer_generations_.begin(),
                    self->peer_generations_.end(),
                    [self](auto const& item) {
                        return item.second.session &&
                            !self->draining_generations_.count(
                                item.second.lifecycle.generation());
                    });
                if (retained == self->peer_generations_.end())
                    break;

                auto const endpoint = retained->first;
                auto const generation = retained->second.lifecycle.generation();
                auto const session = retained->second.session;
                self->begin_generation_close(endpoint, generation);
                if (session)
                    co_await session->co_disconnect();
                assert(self->strand_.running_in_this_thread());

                retained = self->peer_generations_.find(endpoint);
                if (retained != self->peer_generations_.end() &&
                    retained->second.lifecycle.generation() == generation &&
                    retained->second.session == session)
                {
                    retained->second.session.reset();
                }
                self->finish_generation_peer_uses_after_drain(
                    generation, PeerUseOutcome::shutdown);
            }

            // Connect/crawl servants that crossed the seal own their frames
            // until their bounded operations finish. Their terminal funnels
            // notify this timer after removing the exact census row.
            while (!self->shutdown_drained_on_strand())
            {
                auto failed_drain = std::find_if(
                    self->draining_generations_.begin(),
                    self->draining_generations_.end(),
                    [](auto const& item) {
                        return item.second.receipt_failed;
                    });
                if (failed_drain != self->draining_generations_.end())
                {
                    auto const generation = failed_drain->first;
                    auto const session = failed_drain->second.session;
                    // The background close did not yield a receipt. Stop gets
                    // one exact-session retry; a second refusal propagates to
                    // its caller rather than manufacturing a drained state.
                    co_await session->co_disconnect();
                    assert(self->strand_.running_in_this_thread());
                    self->finish_generation_peer_uses_after_drain(
                        generation, PeerUseOutcome::shutdown);
                    self->draining_generations_.erase(generation);
                    self->notify_shutdown_progress();
                    continue;
                }

                progress->expires_at(asio::steady_timer::time_point::max());
                if (self->shutdown_drained_on_strand())
                    break;

                boost::system::error_code ec;
                co_await progress->async_wait(
                    asio::redirect_error(asio::use_awaitable, ec));
            }

            self->lifecycle_ = Lifecycle::stopped;
            PEER_SET_TRACE(
                "[",
                self->net_label_,
                "] Shutdown drain census empty: crawl_in_flight=",
                self->crawl_in_flight_.size(),
                " crawl_queued=",
                self->crawl_queued_.size(),
                " pending_crawls=",
                self->pending_crawls_.size());
            self->notify_shutdown_progress();
            if (self->before_stop_receipt_return_for_test_)
                self->before_stop_receipt_return_for_test_();
            try
            {
                PLOGI(
                    log_,
                    "[",
                    self->net_label_,
                    "] PeerSet sessions and servants drained");
            }
            catch (...)
            {
            }
        },
        asio::use_awaitable);
}

//@@authority transition=prv.T0085 status=provisional scope=function
// bind=catl::peer_client::begin_stop_on_strand@678597c2eaef130c
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0035,prv.S0037,prv.S0038
// label=commit_peer_set_stopping_and_clear_queued_admission
void
PeerSet::begin_stop_on_strand()
{
    ASSERT_ON_STRAND();
    if (lifecycle_ != Lifecycle::running)
        return;

    lifecycle_ = Lifecycle::stopping;
    PEER_SET_TRACE(
        "[",
        net_label_,
        "] Shutdown stop census: crawl_in_flight=",
        crawl_in_flight_.size(),
        " crawl_queued=",
        crawl_queued_.size(),
        " pending_crawls=",
        pending_crawls_.size());
    tracker_->set_discovered_observer({});
    tracker_->set_status_observer({});

    for (auto const& authorization : active_target_authorizations_)
    {
        if (authorization)
            authorization->invalidate();
    }
    active_target_authorizations_.clear();
    for (auto const& [endpoint, authority] : targeted_connects_)
        abandon_unstarted_generation(endpoint, authority.generation);
    targeted_connects_.clear();
    wanted_ledgers_.clear();
    prioritized_ledgers_.clear();

    for (auto const& [id, _] : ledger_demands_)
        terminalize_ledger_demand(id, LedgerDemandTerminal::shutdown);
    history_acquisitions_.clear();
    ledger_demand_keys_.clear();
    ledger_demands_.clear();

    pending_connects_.clear();
    queued_.clear();
    pending_crawls_.clear();
    crawl_queued_.clear();

    // Cancellation is intent, not terminality. Each row remains in S0037
    // until its servant awaits this exact session's physical drain and T0091
    // consumes the row.
    for (auto& [_, servant] : in_flight_)
    {
        if (!servant.session)
            continue;
        try
        {
            servant.stop_cancellation_requested = true;
            //@@authority transition=prv.T0182 status=provisional
            // bind=disconnect@cd31ff180f9a6cb6
            // scope=statement owner=unresolved owner_hypothesis=peer_set
            // commit=peer_set_strand touches=prv.S0037
            // calls=disconnect
            // label=request_connect_servant_cancellation_without_retirement
            servant.session->disconnect();  //@@authority-target ids=prv.T0182
        }
        catch (...)
        {
            servant.stop_cancellation_requested = false;
            // A projection refusal cannot counterfeit completion. The servant
            // remains counted, retains no false owner-issued disposition, and
            // its normal bounded terminal path is still joined by co_stop().
        }
    }

    // S0038 owns both the admitted crawl census and its cancellation
    // capability. Cancellation is intent: the row remains until the co_spawn
    // completion funnel delivers the exact T0092 physical receipt.
    for (auto& [_, servant] : crawl_in_flight_)
    {
        if (!servant.cancellation || servant.cancellation_emitted)
            continue;
        servant.cancellation_emitted = true;
        try
        {
            // clang-format off
            //@@authority transition=prv.T0189 status=provisional bind=declaration@138f3c75ab7f4ee0
            // scope=statement owner=unresolved owner_hypothesis=peer_set
            // commit=peer_set_strand touches=prv.S0038
            // label=request_crawl_servant_cancellation_without_retirement
            auto const cancellation_type = asio::cancellation_type::all; //@@authority-target ids=prv.T0189
            // clang-format on
            //@@mutation-site crawl-stop-emits-owned-cancellation
            servant.cancellation->emit(cancellation_type);
            PEER_SET_TRACE(
                "[",
                net_label_,
                "] Crawl stop cancellation emitted key=",
                _,
                " crawl_in_flight=",
                crawl_in_flight_.size());
            //@@mutation-end crawl-stop-emits-owned-cancellation
        }
        catch (...)
        {
            // The retained stop fact still prevents fallback. The exact row
            // remains owned until the operation's bounded terminal receipt.
            servant.cancellation_emitted = false;
        }
    }

    notify_waiters();
}

//@@authority transition=prv.T0086 status=provisional scope=function
// bind=catl::peer_client::notify_shutdown_progress@7558d4b16007c984
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0036 label=wake_joined_peer_set_stop_waiters
void
PeerSet::notify_shutdown_progress() noexcept
{
    ASSERT_ON_STRAND();
    for (auto it = shutdown_waiters_.begin(); it != shutdown_waiters_.end();)
    {
        if (auto waiter = it->lock())
        {
            try
            {
                waiter->cancel();
            }
            catch (...)
            {
            }
            ++it;
        }
        else
        {
            it = shutdown_waiters_.erase(it);
        }
    }
}

void
PeerSet::complete_connect_servant(
    std::string const& key,
    std::exception_ptr failure,
    SuccessorPump successor) noexcept
{
    ASSERT_ON_STRAND();
    auto const row = in_flight_.find(key);
    auto const generation =
        row == in_flight_.end() ? 0 : row->second.generation;

    // The exact census consumption and stop wake are the no-throw receipt
    // commit. Diagnostics and successor pumping are optional downstream work.
    //@@authority transition=prv.T0091 status=provisional scope=statement
    // bind=expression_statement@3482ee1a8d9bce89
    // owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
    // touches=prv.S0037 label=consume_background_connect_servant_receipt
    in_flight_.erase(key);  //@@authority-target ids=prv.T0091
    abandon_unstarted_generation(key, generation);
    notify_shutdown_progress();
    PEER_SET_TRACE(
        "[",
        net_label_,
        "] targeted connect completion endpoint=",
        key,
        " outcome=",
        failure ? "failed" : "completed",
        " in_flight_remaining=",
        in_flight_.size());

    try
    {
        if (connect_completion_diagnostic_hook_for_test_)
            connect_completion_diagnostic_hook_for_test_();
    }
    catch (...)
    {
    }

    if (failure)
    {
        try
        {
            std::rethrow_exception(failure);
        }
        catch (std::exception const& e)
        {
            try
            {
                PLOGE(
                    log_,
                    "[",
                    net_label_,
                    "] Connect servant failed for ",
                    key,
                    ": ",
                    e.what());
            }
            catch (...)
            {
            }
        }
        catch (...)
        {
            try
            {
                PLOGE(
                    log_,
                    "[",
                    net_label_,
                    "] Connect servant failed for ",
                    key);
            }
            catch (...)
            {
            }
        }
    }

    if (successor == SuccessorPump::run &&
        !shutdown_ingress_sealed_.load(std::memory_order_acquire))
    {
        try
        {
            pump_connects();
        }
        catch (...)
        {
        }
    }
}

boost::asio::awaitable<void>
PeerSet::finish_connect_servant(
    std::string key,
    std::exception_ptr failure,
    SuccessorPump successor)
{
    ASSERT_ON_STRAND();

    // A failure which escapes after T0181 may still own a live PeerClient.
    // Join its exact physical receipt before allowing T0091 to consume S0037.
    // If physical drain itself refuses, retain the row so shutdown fails
    // closed instead of manufacturing terminality.
    if (failure)
    {
        auto row = in_flight_.find(key);
        auto session =
            row != in_flight_.end() ? row->second.session : PeerSessionPtr{};
        if (session)
        {
            std::exception_ptr drain_failure;
            try
            {
                //@@authority transition=prv.T0183 status=provisional
                // bind=co_disconnect@8c60a2bcef4fffc7
                // scope=statement owner=unresolved owner_hypothesis=peer_set
                // commit=peer_set_strand touches=prv.S0037
                // calls=co_disconnect
                // label=join_escaped_connect_servant_physical_receipt
                co_await session
                    ->co_disconnect();  //@@authority-target ids=prv.T0183
            }
            catch (...)
            {
                drain_failure = std::current_exception();
            }

            if (drain_failure)
            {
                try
                {
                    std::rethrow_exception(drain_failure);
                }
                catch (std::exception const& e)
                {
                    PLOGE(
                        log_,
                        "[",
                        net_label_,
                        "] Connect servant physical drain failed for ",
                        key,
                        ": ",
                        e.what());
                }
                catch (...)
                {
                    PLOGE(
                        log_,
                        "[",
                        net_label_,
                        "] Connect servant physical drain failed for ",
                        key);
                }
                co_return;
            }
        }
    }

    complete_connect_servant(key, std::move(failure), successor);
}

//@@authority transition=prv.T0181 status=provisional scope=function
// bind=catl::peer_client::install_connect_servant_session@98ed45e580566bc0
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0037
// label=install_connect_servant_cancellation_capability
void
PeerSet::install_connect_servant_session(
    std::string const& key,
    std::uint64_t generation,
    PeerSessionPtr const& session) noexcept
{
    ASSERT_ON_STRAND();
    auto row = in_flight_.find(key);
    if (row == in_flight_.end() || row->second.session ||
        row->second.generation != generation)
        return;

    row->second.session = session;
}

void
PeerSet::install_generation_session(
    std::string const& key,
    std::uint64_t generation,
    PeerSessionPtr const& session) noexcept
{
    ASSERT_ON_STRAND();
    auto row = peer_generations_.find(key);
    if (row == peer_generations_.end() ||
        row->second.lifecycle.generation() != generation ||
        row->second.lifecycle.terminal())
    {
        return;
    }
    row->second.session = session;
    if (row->second.history_origin &&
        row->second.history_origin->close_requested && session)
    {
        try
        {
            session->disconnect();
        }
        catch (...)
        {
        }
    }
}

std::uint64_t
//@@authority transition=prv.T0290 status=provisional scope=function bind=catl::peer_client::reserve_connection_generation@a109c5943fcae182
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0088,prv.S0115,prv.S0118
// label=reserve_nonreused_generation_before_physical_queue_visibility
PeerSet::reserve_connection_generation(
    std::string const& key,
    std::chrono::steady_clock::time_point physical_deadline)
{
    ASSERT_ON_STRAND();
    if (key.empty() ||
        physical_deadline == std::chrono::steady_clock::time_point{} ||
        next_connection_generation_ ==
            std::numeric_limits<std::uint64_t>::max())
    {
        return 0;
    }
    if (auto prior = peer_generations_.find(key);
        prior != peer_generations_.end() &&
        !prior->second.lifecycle.terminal())
    {
        return 0;
    }
    auto const generation = next_connection_generation_ + 1;
    peer_generations_.insert_or_assign(
        key, PeerGeneration{key, generation, physical_deadline});
    next_connection_generation_ = generation;
    advance_history_pool_revision();
    return generation;
}

void
PeerSet::abandon_unstarted_generation(
    std::string const& key,
    std::uint64_t generation) noexcept
{
    ASSERT_ON_STRAND();
    auto row = peer_generations_.find(key);
    if (row == peer_generations_.end() ||
        row->second.lifecycle.generation() != generation ||
        row->second.lifecycle.lifecycle() != PeerLifecycle::created ||
        row->second.session)
    {
        return;
    }
    release_generation_history_origin(
        row->second, HistoryWorkReceipt::closed);
    peer_generations_.erase(row);
    advance_history_pool_revision();
}

void
PeerSet::begin_generation_close(
    std::string const& key,
    std::uint64_t generation) noexcept
{
    ASSERT_ON_STRAND();
    auto row = peer_generations_.find(key);
    if (row == peer_generations_.end() ||
        row->second.lifecycle.generation() != generation)
    {
        return;
    }
    (void)row->second.lifecycle.begin_local_close();
}

PeerStatusSnapshot const*
PeerSet::ready_status_for(std::string const& key, std::uint64_t generation)
    const noexcept
{
    ASSERT_ON_STRAND();
    auto const row = peer_generations_.find(key);
    if (row == peer_generations_.end() ||
        row->second.lifecycle.generation() != generation ||
        !row->second.lifecycle.ready())
    {
        return nullptr;
    }
    return &*row->second.lifecycle.status();
}

PeerStatusSnapshot const*
PeerSet::ready_status_for(PeerSessionPtr const& session) const noexcept
{
    ASSERT_ON_STRAND();
    if (!session)
        return nullptr;
    auto const key = canonical_endpoint(session->endpoint());
    auto const generation = peer_generations_.find(key);
    if (generation == peer_generations_.end() ||
        generation->second.session != session)
        return nullptr;
    return ready_status_for(key, generation->second.lifecycle.generation());
}

//@@authority transition=prv.T0241 status=provisional scope=function
// bind=catl::peer_client::start_generation_drain@a6fe3b9146e73d48
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0088,prv.S0089,prv.S0090,prv.S0097,prv.S0098,prv.S0099
// label=retain_closing_or_terminal_generation_until_disconnect_receipt
bool
PeerSet::start_generation_drain(
    std::string const& key,
    std::uint64_t generation,
    GenerationDrainCause cause,
    PeerUseOutcome trigger_outcome,
    std::optional<PeerUseTicket> trigger) noexcept
{
    ASSERT_ON_STRAND();
    auto row = peer_generations_.find(key);
    if (draining_generations_.count(generation))
        return true;
    if (row == peer_generations_.end() ||
        row->second.lifecycle.generation() != generation ||
        (!row->second.lifecycle.terminal() &&
         row->second.lifecycle.lifecycle() != PeerLifecycle::closing) ||
        !row->second.session)
    {
        return false;
    }

    auto const session = row->second.session;
    try
    {
        auto retry_timer = std::make_shared<asio::steady_timer>(strand_);
        auto const [drain, inserted] = draining_generations_.emplace(
            generation,
            DrainingGeneration{key, session, std::move(retry_timer)});
        if (!inserted)
            return true;
        if (fail_generation_drain_launches_for_test_ != 0)
        {
            --fail_generation_drain_launches_for_test_;
            throw std::bad_alloc{};
        }
        auto self = shared_from_this();
        asio::co_spawn(
            strand_,
            [self, generation, session]() -> asio::awaitable<void> {
                // co_spawn may enter inline on its target executor. Always
                // yield once so the owner can commit the drain row, revoke
                // the generation uses, and remove the selectable session
                // before any physical receipt can arrive.
                co_await asio::post(self->strand_, asio::use_awaitable);
                if (self->fail_generation_drain_servants_after_start_for_test_ !=
                    0)
                {
                    --self->fail_generation_drain_servants_after_start_for_test_;
                    throw std::runtime_error(
                        "generation drain servant post-accept failpoint");
                }
                for (;;)
                {
                    auto retained =
                        self->draining_generations_.find(generation);
                    if (retained == self->draining_generations_.end() ||
                        retained->second.session != session)
                    {
                        co_return;
                    }
                    ++retained->second.attempts;

                    bool drained = false;
                    try
                    {
                        //@@authority transition=prv.T0254 status=provisional
                        //scope=statement
                        // bind=expression_statement@11bcc2d1692f1c01
                        // owner=unresolved owner_hypothesis=peer_set
                        // commit=peer_set_strand
                        // touches=prv.S0089,prv.S0097,prv.S0099
                        // label=retry_exact_generation_drain_before_fault_publication
                        //@@mutation-site
                        //peer-use-release-follows-physical-drain
                        // clang-format off
                        co_await session->co_disconnect(); //@@authority-target ids=prv.T0254
                        // clang-format on
                        drained = true;
                    }
                    catch (std::exception const& e)
                    {
                        PLOGE(
                            log_,
                            "generation drain attempt failed gen=",
                            generation,
                            " attempt=",
                            retained->second.attempts,
                            ": ",
                            e.what());
                    }
                    catch (...)
                    {
                        PLOGE(
                            log_,
                            "generation drain attempt failed gen=",
                            generation,
                            " attempt=",
                            retained->second.attempts);
                    }

                    if (drained)
                    {
                        self->finish_generation_peer_uses_after_drain(
                            generation, PeerUseOutcome::transport_failure);
                        //@@mutation-end peer-use-release-follows-physical-drain
                        self->draining_generations_.erase(generation);
                        self->notify_shutdown_progress();
                        co_return;
                    }

                    if (retained->second.attempts >=
                        kGenerationDrainMaxAttempts)
                    {
                        retained->second.receipt_failed = true;
                        self->fail_generation_peer_uses_without_drain(
                            generation);
                        self->notify_shutdown_progress();
                        co_return;
                    }

                    retained->second.retry_timer->expires_after(
                        kGenerationDrainRetryDelay);
                    if (self->fail_generation_drain_retry_waits_for_test_ != 0)
                    {
                        --self->fail_generation_drain_retry_waits_for_test_;
                        throw std::runtime_error(
                            "generation drain retry-wait failpoint");
                    }
                    boost::system::error_code ec;
                    co_await retained->second.retry_timer->async_wait(
                        asio::redirect_error(asio::use_awaitable, ec));
                }
            },
            [self, generation, session](std::exception_ptr failure) noexcept {
                self->complete_generation_drain_servant(
                    generation, session, std::move(failure));
            });

        // The retry servant and its retained session now exist. Only after
        // that fallible setup succeeds may the owner revoke submitted work.
        row->second.session.reset();
        mark_generation_peer_uses_finishing(
            generation, cause, trigger_outcome, trigger);
        return true;
    }
    catch (...)
    {
        draining_generations_.erase(generation);
        // No physical receipt exists. Revoke future callback authority, retain
        // the exact session in peer_generations_, and publish a typed fault to
        // structured waiters without erasing the use or claiming release.
        mark_generation_peer_uses_finishing(
            generation, cause, trigger_outcome, trigger);
        fail_generation_peer_uses_without_drain(generation);
        return false;
    }
}

//@@authority transition=prv.T0257 status=provisional scope=function
// bind=catl::peer_client::complete_generation_drain_servant@01d2312c62bc1e31
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0089,prv.S0090,prv.S0097,prv.S0099
// label=publish_exact_generation_drain_servant_failure
void
PeerSet::complete_generation_drain_servant(
    std::uint64_t generation,
    PeerSessionPtr const& session,
    std::exception_ptr failure) noexcept
{
    ASSERT_ON_STRAND();
    if (!failure)
        return;

    auto retained = draining_generations_.find(generation);
    if (retained == draining_generations_.end() ||
        retained->second.session != session)
    {
        return;
    }

    // The servant was accepted, so this row remains the exact physical owner.
    // Any exception before a disconnect receipt is a visible retained fault,
    // never a reason to erase the row or publish a false release.
    //@@mutation-site generation-drain-servant-failure-is-receipt-fault
    retained->second.receipt_failed = true;
    fail_generation_peer_uses_without_drain(generation);
    notify_shutdown_progress();
    //@@mutation-end generation-drain-servant-failure-is-receipt-fault

    try
    {
        std::rethrow_exception(failure);
    }
    catch (std::exception const& e)
    {
        try
        {
            PLOGE(
                log_,
                "generation drain servant failed gen=",
                generation,
                ": ",
                e.what());
        }
        catch (...)
        {
        }
    }
    catch (...)
    {
        try
        {
            PLOGE(log_, "generation drain servant failed gen=", generation);
        }
        catch (...)
        {
        }
    }
}

//@@authority transition=prv.T0242 status=provisional scope=function
// bind=catl::peer_client::consume_lifecycle_event@fbb8acfd936ed751
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0088
// label=apply_generation_event_before_selection_wake_or_terminal_removal
void
PeerSet::consume_lifecycle_event(PeerLifecycleEvent event)
{
    ASSERT_ON_STRAND();
    auto const key = canonical_endpoint(event.endpoint);
    auto row = peer_generations_.find(key);
    if (row == peer_generations_.end())
        return;
    event.endpoint = key;
    PeerLifecycleApply applied = PeerLifecycleApply::illegal_transition;
    try
    {
        applied = row->second.lifecycle.apply(event);
    }
    catch (std::exception const& e)
    {
        PLOGE(log_, "lifecycle event projection failed: ", e.what());
        return;
    }
    catch (...)
    {
        PLOGE(log_, "lifecycle event projection failed");
        return;
    }
    if (applied != PeerLifecycleApply::accepted)
        return;

    if (event.lifecycle == PeerLifecycle::ready)
    {
        release_generation_history_attachments(row->second);
        auto const& status = event.status;
        note_status(
            key,
            {.first_seq = status.advertised_range.first_seq,
             .last_seq = status.advertised_range.last_seq,
             .current_seq = status.current_seq});
        note_peer_headers(key, status.headers);
        notify_waiters();
        return;
    }

    if (event.lifecycle == PeerLifecycle::closed ||
        event.lifecycle == PeerLifecycle::failed)
    {
        release_generation_history_attachments(row->second);
        consume_peer_terminal(key, event.generation, std::move(event.reason));
        (void)start_generation_drain(key, event.generation);
    }
}

//@@authority transition=prv.T0187 status=provisional scope=function
// bind=catl::peer_client::classify_unadmitted_connect_terminal@7cb2098a2e785948
// owner=unresolved owner_hypothesis=peer_set
// commit=peer_set_strand touches=prv.S0037
// mutation_class=terminal_receipt
// label=consume_post_ready_terminal_before_connect_servant_retirement
void
PeerSet::classify_unadmitted_connect_terminal(
    std::string const& key,
    std::uint64_t generation,
    std::shared_ptr<PeerClient> const& client)
{
    ASSERT_ON_STRAND();
    auto row = in_flight_.find(key);
    if (row == in_flight_.end() || row->second.session != client ||
        row->second.generation != generation || row->second.terminal_classified)
    {
        return;
    }

    //@@mutation-site unadmitted-peer-terminal-is-classified
    // Consume the exact row's disposition before servant retirement. Ready is
    // immutable; this is the separate live-session cause which won after Ready
    // but before PeerSet either adopted or refused the candidate.
    auto const cause = client->terminal_cause();
    if (cause == PeerLogicalTerminalCause::owner_disconnect)
    {
        row->second.terminal_classified = true;
        return;
    }

    std::string detail;
    try
    {
        detail = trim_connect_failure_detail(
            client->raw_connection().connect_failure_detail());
    }
    catch (...)
    {
    }
    if (detail.empty())
    {
        detail = cause == PeerLogicalTerminalCause::peer_failure
            ? "peer became terminal before admission"
            : "connect terminal cause missing before admission";
    }

    note_connect_failure(key, detail);
    if (endpoint_cache_)
    {
        try
        {
            endpoint_cache_->remember_connect_failure(network_id_, key);
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
    row->second.terminal_classified = true;
    //@@mutation-end unadmitted-peer-terminal-is-classified
}

void
PeerSet::complete_crawl_servant(
    std::string const& key,
    CrawlServantOutcome const& outcome,
    std::exception_ptr failure,
    SuccessorPump successor) noexcept
{
    ASSERT_ON_STRAND();

    auto const census_before = crawl_in_flight_.size();

    // As above, optional retention and diagnostics cannot sit between census
    // consumption and the shared stop wake.
    //@@authority transition=prv.T0092 status=provisional scope=statement
    // bind=expression_statement@caf7c41aa56bab58
    // owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
    // touches=prv.S0038 label=consume_background_crawl_servant_receipt
    crawl_in_flight_.erase(key);  //@@authority-target ids=prv.T0092
    try
    {
        PLOGD(
            log_,
            "[",
            net_label_,
            "] Crawl servant retired key=",
            key,
            " outcome=",
            failure ? "failure" : "success",
            " crawl_census=",
            census_before,
            "->",
            crawl_in_flight_.size(),
            " stop_requested=",
            crawl_stop_source_.stop_requested() ? "yes" : "no");
    }
    catch (...)
    {
    }
    notify_shutdown_progress();

    try
    {
        if (crawl_completion_diagnostic_hook_for_test_)
            crawl_completion_diagnostic_hook_for_test_();
    }
    catch (...)
    {
    }

    if (failure)
    {
        try
        {
            std::string summary = "unknown error";
            try
            {
                std::rethrow_exception(failure);
            }
            catch (std::exception const& e)
            {
                try
                {
                    summary = summarize_crawl_error(e.what());
                }
                catch (...)
                {
                }
            }
            catch (...)
            {
            }

            try
            {
                note_crawl_failure(key, summary);
            }
            catch (...)
            {
            }
            try
            {
                PLOGW(
                    log_,
                    "[",
                    net_label_,
                    "] Crawl failed for ",
                    key,
                    ": ",
                    summary);
            }
            catch (...)
            {
            }
        }
        catch (...)
        {
        }
    }
    else
    {
        try
        {
            crawled_[key] = std::chrono::steady_clock::now();
        }
        catch (...)
        {
        }
        try
        {
            PLOGI(
                log_,
                "[",
                net_label_,
                "] ",
                "Crawled ",
                key,
                " via ",
                outcome.used_tls ? "HTTPS" : "HTTP",
                ": ",
                outcome.raw,
                " raw peers, ",
                outcome.accepted,
                " accepted public peers, ",
                outcome.ranged,
                " with ledger ranges, ",
                outcome.filtered,
                " filtered, truncation peers=",
                outcome.truncated_peer_rows,
                " intervals=",
                outcome.truncated_interval_rows,
                " keys=",
                outcome.truncated_public_keys);
        }
        catch (...)
        {
        }
    }

    try
    {
        if (!shutdown_ingress_sealed_.load(std::memory_order_acquire) &&
            should_connect_endpoint(key))
        {
            queue_connect(key);
        }

        prune_discovery_state();
        if (successor == SuccessorPump::run &&
            !shutdown_ingress_sealed_.load(std::memory_order_acquire))
        {
            pump_crawls();
            pump_connects();
        }
    }
    catch (...)
    {
    }
}

bool
PeerSet::shutdown_drained_on_strand() const noexcept
{
    ASSERT_ON_STRAND();
    return connections_.empty() && active_peer_pins_.empty() &&
        active_peer_uses_.empty() && draining_generations_.empty() &&
        in_flight_.empty() && queued_.empty() && pending_connects_.empty() &&
        //@@mutation-site crawl-physical-receipt-required-for-terminal
        crawl_in_flight_.empty() && crawl_queued_.empty() &&
        //@@mutation-end crawl-physical-receipt-required-for-terminal
        pending_crawls_.empty() && targeted_connects_.empty() &&
        active_target_authorizations_.empty() && wanted_ledgers_.empty();
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

        for (auto& [_, client_entry] : self->connections_)
        {
            auto const& client = client_entry.session;
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
    stats.range_is_current_live = false;
    stats.range_is_persistent_hint =
        entry.status.first_seq != 0 && entry.status.last_seq != 0;
}

void
PeerSet::note_discovered(std::string const& endpoint, DiscoverySource source)
{
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        return;
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), endpoint, source]() {
            self->note_discovered(endpoint, source);
        });
        return;
    }
    auto key = canonical_endpoint(endpoint);
    auto& stats = endpoint_stats_[key];
    auto const current_status = tracker_->get_status(key);
    auto const current_range_known = current_status &&
        current_status->first_seq != 0 && current_status->last_seq != 0;
    if (!current_range_known)
    {
        stats.range_is_current_live = false;
        if (!stats.range_is_persistent_hint)
            stats.status = {};
    }
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
    if (source == DiscoverySource::Crawl)
    {
        // Cut 4: crawl is a bounded locator/hint lane only. It cannot enter
        // ordinary discovery, recursively crawl its claims, or create a
        // connection outside an exact demand-backed plan.
        prune_discovery_state();
        notify_waiters();
        return;
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
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        return;
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
    stats.range_is_current_live = true;
    stats.range_is_persistent_hint = false;
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
    stats.range_is_current_live = true;
    stats.range_is_persistent_hint = false;
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

//@@authority transition=prv.T0225 status=provisional scope=function
// bind=catl::peer_client::note_connect_failure@2ef93397e1fa8ae8
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0008,prv.S0076
// label=increment_retry_gate_from_its_retained_magnitude
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
    stats.connected_ok = false;
    auto& gate = retry_gates_[key];
    gate.failed_at = now;
    gate.failure_count = std::max(gate.failure_count, stats.failure_count) + 1;
    stats.failure_count = gate.failure_count;
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
    auto const gate = retry_gates_.find(key);
    auto const failures =
        gate != retry_gates_.end() ? gate->second.failure_count : 0;

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
    advance_history_pool_revision();
    auto wake = wait_signal_;
    wait_signal_ = std::make_shared<asio::steady_timer>(
        strand_, asio::steady_timer::time_point::max());
    if (wake)
        wake->cancel();
}

void
PeerSet::advance_history_pool_revision() noexcept
{
    ASSERT_ON_STRAND();
    if (history_pool_revision_ == std::numeric_limits<std::uint64_t>::max())
        history_pool_revision_ = 0;
    else if (history_pool_revision_ != 0)
        ++history_pool_revision_;
}

HistoryPhysicalCensus
PeerSet::physical_census_on_strand() const noexcept
{
    ASSERT_ON_STRAND();
    std::size_t connecting = 0;
    std::size_t ready_transient = 0;
    std::size_t admitted = 0;
    for (auto const& [_, connection] : connections_)
    {
        if (!draining_generations_.contains(connection.generation))
            ++admitted;
    }
    for (auto const& [endpoint, generation] : peer_generations_)
    {
        auto const id = generation.lifecycle.generation();
        auto const admitted_generation = connections_.find(endpoint);
        if (id == 0 || draining_generations_.contains(id) ||
            (admitted_generation != connections_.end() &&
             admitted_generation->second.generation == id))
        {
            continue;
        }
        switch (generation.lifecycle.lifecycle())
        {
            case PeerLifecycle::created:
            case PeerLifecycle::connecting:
                ++connecting;
                break;
            case PeerLifecycle::ready:
                ++ready_transient;
                break;
            case PeerLifecycle::closing:
            case PeerLifecycle::closed:
            case PeerLifecycle::failed:
                break;
        }
    }
    return HistoryPhysicalCensus{
        .queued_or_connecting = connecting,
        .ready_transient = ready_transient,
        .admitted = admitted,
        .draining = draining_generations_.size(),
        .max_admitted =
            options_.max_connected_peers + options_.max_archival_peers,
        .max_transient = options_.max_transient_peer_generations,
        .drain_reserve = options_.physical_drain_reserve};
}

//@@authority transition=prv.T0057 status=provisional scope=function
// bind=catl::peer_client::select_covering_peers_for@066b9356328fef15
// owner=unresolved owner_hypothesis=peer_set commit=strand
// label=mint_only_generations_advertising_target_ledger
std::vector<PeerGenerationPin>
PeerSet::select_covering_peers_for(
    uint32_t ledger_seq,
    int max_count,
    std::unordered_set<std::string> const& excluded)
{
    ASSERT_ON_STRAND();

    // Budget: never use more than 1/3 of ready peers, cap at 4
    int ready = 0;
    for (auto const& [key, c] : connections_)
        if (c.session && ready_status_for(key, c.generation))
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

    for (auto const& [key, client_entry] : connections_)
    {
        auto const& client = client_entry.session;
        auto const* status = ready_status_for(key, client_entry.generation);
        if (!client || !status)
            continue;
        if (excluded.count(key))
            continue;
        if (peer_failed_for_ledger(key, ledger_seq, now))
            continue;

        uint64_t span = 0;
        auto const range = status->advertised_range;
        if (!range.contains(ledger_seq))
            continue;
        if (range.last_seq > range.first_seq)
            span = static_cast<uint64_t>(range.last_seq - range.first_seq);
        candidates.push_back({key, span, client});
    }

    // All candidates cover the target. Prefer the widest advertised range.
    std::sort(
        candidates.begin(), candidates.end(), [](auto const& a, auto const& b) {
            return a.span > b.span;
        });

    std::vector<PeerGenerationPin> result;
    for (auto const& c : candidates)
    {
        if (static_cast<int>(result.size()) >= budget)
            break;
        note_peer_selected(c.key);
        if (auto pin = make_peer_pin(c.client))
            result.push_back(std::move(pin));
    }
    return result;
}

//@@authority transition=prv.T0058 status=provisional scope=function
// bind=catl::peer_client::co_select_covering_peers_for@b0dbba65438ae8e1
// owner=unresolved owner_hypothesis=peer_set commit=strand
// label=project_strict_covering_selection_to_caller_executor
boost::asio::awaitable<std::vector<PeerGenerationPin>>
PeerSet::co_select_covering_peers_for(
    uint32_t ledger_seq,
    int max_count,
    std::unordered_set<std::string> excluded)
{
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self, ledger_seq, max_count, excluded = std::move(excluded)]() mutable
        -> asio::awaitable<std::vector<PeerGenerationPin>> {
            co_return self->select_covering_peers_for(
                ledger_seq, max_count, excluded);
        },
        asio::use_awaitable);
}

boost::asio::awaitable<void>
PeerSet::co_generation_pin_release_barrier()
{
    // All PeerGenerationPin release callbacks use post(strand_, ...). This
    // child is spawned only after the sender has destroyed every predecessor
    // pin, so strand ordering makes its completion the receiver-side receipt.
    // co_spawn keeps the parent coroutine on its birth executor after the
    // child completes.
    auto self = shared_from_this();
    co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<void> {
            assert(self->strand_.running_in_this_thread());
            self->prune_expired_peer_pins();
            if (self->before_generation_pin_release_barrier_return_for_test_)
            {
                self->before_generation_pin_release_barrier_return_for_test_();
            }
            co_return;
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

boost::asio::awaitable<PeerHistoryCoverage>
PeerSet::co_history_coverage()
{
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<PeerHistoryCoverage> {
            PeerHistoryCoverage coverage;
            coverage.observed = true;
            coverage.advertised_ranges.reserve(self->connections_.size());

            for (auto const& [_, entry] : self->connections_)
            {
                auto const& peer = entry.session;
                if (!peer)
                    continue;
                ++coverage.connected_peers;
                auto const* status =
                    self->ready_status_for(_, entry.generation);
                if (!status)
                    continue;
                ++coverage.ready_peers;

                auto const range = status->advertised_range;
                if (!range.valid())
                    continue;
                ++coverage.advertising_peers;
                coverage.advertised_ranges.push_back(range);
            }

            std::sort(
                coverage.advertised_ranges.begin(),
                coverage.advertised_ranges.end(),
                [](AdvertisedLedgerRange const& lhs,
                   AdvertisedLedgerRange const& rhs) {
                    if (lhs.first_seq != rhs.first_seq)
                        return lhs.first_seq < rhs.first_seq;
                    return lhs.last_seq < rhs.last_seq;
                });

            std::vector<AdvertisedLedgerRange> merged;
            merged.reserve(coverage.advertised_ranges.size());
            for (auto const& range : coverage.advertised_ranges)
            {
                if (merged.empty())
                {
                    merged.push_back(range);
                    continue;
                }

                auto& tail = merged.back();
                auto const adjacent = tail.last_seq !=
                        std::numeric_limits<std::uint32_t>::max() &&
                    range.first_seq == tail.last_seq + 1;
                if (range.first_seq <= tail.last_seq || adjacent)
                {
                    tail.last_seq = std::max(tail.last_seq, range.last_seq);
                    continue;
                }
                merged.push_back(range);
            }
            coverage.advertised_ranges = std::move(merged);
            co_return coverage;
        },
        asio::use_awaitable);
}

//@@authority transition=prv.T0024 status=provisional scope=function
// bind=catl::peer_client::note_ledger_failure@afbf33599592273c
// owner=unresolved owner_hypothesis=peer_set commit=strand
// touches=prv.S0008 label=record_peer_failure_for_exact_ledger
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

    for (auto const& [key, client_entry] : connections_)
    {
        auto const& client = client_entry.session;
        if (excluded.count(key))
            continue;
        if (!client || !ready_status_for(key, client_entry.generation))
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

    for (auto const& [key, client_entry] : connections_)
    {
        auto const& client = client_entry.session;
        if (excluded.count(key))
            continue;
        auto const* status = ready_status_for(key, client_entry.generation);
        if (!client || !status)
            continue;

        auto const range = status->advertised_range;
        if (!range.contains(ledger_seq))
            continue;

        auto const span = range.last_seq - range.first_seq;
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
    auto const has_ready_peer = std::any_of(
        connections_.begin(), connections_.end(), [this](auto const& entry) {
            return entry.second.session &&
                ready_status_for(entry.first, entry.second.generation);
        });
    if (!has_ready_peer && (endpoint_has_range(key) || has_success))
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
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        return;
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), endpoint]() {
            self->queue_connect(endpoint);
        });
        return;
    }
    auto normalized = EndpointTracker::normalize_endpoint_identity(endpoint);
    if (!normalized)
        return;
    auto key = std::move(*normalized);
    auto const now = std::chrono::steady_clock::now();

    if (connections_.count(key) || in_flight_.count(key))
        return;

    if (queued_.count(key))
    {
        auto& authority = targeted_connects_[key];
        if (authority.history_demand_authorized)
            return;
        authority.ordinary_discovery_authorized = true;
        authority.ordinary_deadline = now + kOrdinaryConnectDeadline;
        return;
    }

    auto failed = retry_gates_.find(key);
    if (failed != retry_gates_.end())
    {
        auto backoff = retry_backoff_for(key);
        if (now - failed->second.failed_at < backoff)
        {
            return;
        }
    }

    if (!physical_census_on_strand().can_create())
    {
        PLOGD(
            log_,
            "[",
            net_label_,
            "] physical census refused queued connection ",
            key);
        return;
    }

    QueuedConnectAuthority authority;
    authority.physical_deadline = now + kOrdinaryConnectDeadline;
    authority.generation =
        reserve_connection_generation(key, authority.physical_deadline);
    if (authority.generation == 0)
        return;
    auto const generation = authority.generation;
    authority.ordinary_discovery_authorized = true;
    authority.ordinary_deadline = now + kOrdinaryConnectDeadline;
    try
    {
        targeted_connects_.emplace(key, std::move(authority));
        queued_.insert(key);
        pending_connects_.push_back(key);
    }
    catch (...)
    {
        abandon_unstarted_generation(key, generation);
        targeted_connects_.erase(key);
        queued_.erase(key);
        std::erase(pending_connects_, key);
        throw;
    }
    sort_pending_connects();
}

//@@authority transition=prv.T0027 status=provisional scope=function
// bind=catl::peer_client::queue_targeted_connect@596394d6699d659d
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0008,prv.S0009
// label=queue_bounded_history_targeted_connection_authority
bool
PeerSet::queue_targeted_connect(
    std::string const& endpoint,
    TargetedConnectAuthorizationPtr const& authorization)
{
    ASSERT_ON_STRAND();
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire) ||
        !authorization || !authorization->is_active())
    {
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] targeted queue rejected endpoint=",
            endpoint,
            " reason=inactive_or_shutdown");
        return false;
    }

    auto normalized = EndpointTracker::normalize_endpoint_identity(endpoint);
    if (!normalized)
    {
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] targeted queue rejected endpoint=",
            endpoint,
            " reason=invalid_endpoint");
        return false;
    }
    auto key = std::move(*normalized);
    auto const now = std::chrono::steady_clock::now();
    if (connections_.count(key) || in_flight_.count(key))
    {
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] targeted queue rejected endpoint=",
            key,
            " reason=already_connected_or_in_flight");
        return false;
    }

    if (queued_.count(key))
    {
        auto row = targeted_connects_.find(key);
        if (row == targeted_connects_.end())
        {
            PEER_SET_TRACE(
                "[",
                net_label_,
                "] targeted queue rejected endpoint=",
                key,
                " reason=queued_authority_missing");
            return false;
        }
        if (row->second.history_demand_authorized)
        {
            PEER_SET_TRACE(
                "[",
                net_label_,
                "] targeted queue rejected endpoint=",
                key,
                " reason=demand_origin_requires_exact_adoption");
            return false;
        }
        auto& authorizations = row->second.target_authorizations;
        if (std::find(
                authorizations.begin(), authorizations.end(), authorization) !=
            authorizations.end())
        {
            PEER_SET_TRACE(
                "[",
                net_label_,
                "] targeted queue accepted endpoint=",
                key,
                " outcome=already_claimed");
            return true;
        }
        if (authorizations.size() >= kMaxQueuedTargetAuthorizations)
        {
            PEER_SET_TRACE(
                "[",
                net_label_,
                "] targeted queue rejected endpoint=",
                key,
                " reason=authorization_cap");
            return false;
        }
        try
        {
            authorizations.push_back(authorization);
        }
        catch (...)
        {
            PEER_SET_TRACE(
                "[",
                net_label_,
                "] targeted queue rejected endpoint=",
                key,
                " reason=allocation_failure");
            return false;
        }
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] targeted queue accepted endpoint=",
            key,
            " outcome=joined_existing");
        return true;
    }

    if (auto failed = retry_gates_.find(key); failed != retry_gates_.end())
    {
        if (now - failed->second.failed_at < retry_backoff_for(key))
        {
            PEER_SET_TRACE(
                "[",
                net_label_,
                "] targeted queue rejected endpoint=",
                key,
                " reason=retry_backoff");
            return false;
        }
    }

    if (!physical_census_on_strand().can_create())
    {
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] targeted queue rejected endpoint=",
            key,
            " reason=physical_census_cap");
        return false;
    }

    QueuedConnectAuthority authority;
    authority.physical_deadline =
        std::min(authorization->deadline, now + kOrdinaryConnectDeadline);
    authority.generation =
        reserve_connection_generation(key, authority.physical_deadline);
    if (authority.generation == 0)
        return false;
    auto const generation = authority.generation;
    try
    {
        authority.target_authorizations.push_back(authorization);
        targeted_connects_.emplace(key, std::move(authority));
        queued_.insert(key);
        pending_connects_.push_back(key);
        sort_pending_connects();
    }
    catch (...)
    {
        abandon_unstarted_generation(key, generation);
        targeted_connects_.erase(key);
        queued_.erase(key);
        std::erase(pending_connects_, key);
        return false;
    }
    PEER_SET_TRACE(
        "[",
        net_label_,
        "] targeted queue accepted endpoint=",
        key,
        " outcome=new_pending");
    return true;
}

PeerSet::ClaimedConnectAuthority
PeerSet::claim_queued_connect_authority(
    QueuedConnectAuthority authority,
    std::chrono::steady_clock::time_point now)
{
    ClaimedConnectAuthority claimed;
    claimed.generation = authority.generation;
    claimed.physical_deadline = authority.physical_deadline;
    if (claimed.generation == 0 || claimed.physical_deadline <= now)
        return claimed;
    if (authority.history_demand_authorized)
        claimed.absolute_deadline = authority.physical_deadline;
    if (authority.ordinary_discovery_authorized &&
        authority.ordinary_deadline > now)
        claimed.absolute_deadline = authority.ordinary_deadline;

    for (auto& authorization : authority.target_authorizations)
    {
        if (!authorization)
            continue;
        if (auto lease = authorization->claim_at(now))
        {
            claimed.absolute_deadline = claimed.absolute_deadline
                ? std::max(*claimed.absolute_deadline, authorization->deadline)
                : authorization->deadline;
            claimed.admission_leases.push_back(std::move(lease));
        }
    }
    if (claimed.absolute_deadline)
    {
        *claimed.absolute_deadline = std::min(
            *claimed.absolute_deadline, claimed.physical_deadline);
    }
    return claimed;
}

void
PeerSet::queue_crawl(std::string const& endpoint)
{
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        return;
    if (!strand_.running_in_this_thread())
    {
        asio::post(strand_, [self = shared_from_this(), endpoint]() {
            self->queue_crawl(endpoint);
        });
        return;
    }
    if (options_.max_in_flight_crawls == 0)
        return;

    auto normalized = EndpointTracker::normalize_endpoint_identity(endpoint);
    if (!normalized)
        return;
    auto key = std::move(*normalized);
    auto const now = std::chrono::steady_clock::now();
    if (crawl_in_flight_.count(key) || crawl_queued_.count(key))
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

//@@authority transition=prv.T0087 status=provisional scope=function
// bind=catl::peer_client::pump_connects@29dc9a52e35a56eb
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0009,prv.S0034,prv.S0037
// label=admit_owned_background_connect_servant
void
PeerSet::pump_connects()
{
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        return;
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
        auto queued_authority = targeted_connects_.find(key);
        if (queued_authority == targeted_connects_.end())
            continue;
        auto authority = std::move(queued_authority->second);
        targeted_connects_.erase(queued_authority);

        auto const now = std::chrono::steady_clock::now();
        auto claimed =
            claim_queued_connect_authority(std::move(authority), now);
        auto const generation = claimed.generation;
        if (!claimed.absolute_deadline)
        {
            abandon_unstarted_generation(key, generation);
            PEER_SET_TRACE(
                "[",
                net_label_,
                "] targeted connect claim rejected endpoint=",
                key,
                " reason=expired_or_no_active_authorization");
            continue;
        }
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] targeted connect claim accepted endpoint=",
            key,
            " leases=",
            claimed.admission_leases.size());
        auto const absolute_deadline = *claimed.absolute_deadline;
        auto admission_leases = std::move(claimed.admission_leases);

        std::vector<uint32_t> targeted_ledgers;
        targeted_ledgers.reserve(admission_leases.size());
        for (auto const& lease : admission_leases)
        {
            if (lease && lease->authorization)
                targeted_ledgers.push_back(lease->authorization->ledger_seq);
        }
        std::sort(targeted_ledgers.begin(), targeted_ledgers.end());
        targeted_ledgers.erase(
            std::unique(targeted_ledgers.begin(), targeted_ledgers.end()),
            targeted_ledgers.end());

        std::string host;
        uint16_t port = 0;
        if (!EndpointTracker::parse_endpoint(key, host, port))
        {
            abandon_unstarted_generation(key, generation);
            PEER_SET_TRACE(
                "[",
                net_label_,
                "] targeted connect claim rejected endpoint=",
                key,
                " reason=parse_failure");
            continue;
        }

        if (connections_.count(key) || in_flight_.count(key))
        {
            abandon_unstarted_generation(key, generation);
            continue;
        }

        auto failed = retry_gates_.find(key);
        if (failed != retry_gates_.end())
        {
            if (now - failed->second.failed_at < retry_backoff_for(key))
            {
                abandon_unstarted_generation(key, generation);
                PEER_SET_TRACE(
                    "[",
                    net_label_,
                    "] targeted connect claim rejected endpoint=",
                    key,
                    " reason=retry_backoff");
                continue;
            }
        }

        auto self = shared_from_this();
        auto servant =
            [self,
             host,
             port,
             key,
             generation,
             absolute_deadline,
             admission_leases = std::move(
                 admission_leases)]() mutable -> boost::asio::awaitable<void> {
            std::exception_ptr failure;
            try
            {
                co_await self->try_connect_on_strand(
                    host,
                    port,
                    generation,
                    std::move(admission_leases),
                    absolute_deadline);
            }
            catch (...)
            {
                failure = std::current_exception();
            }
            co_await self->finish_connect_servant(key, std::move(failure));
        };

        bool census_committed = false;
        try
        {
            in_flight_.try_emplace(
                key,
                ConnectServant{
                    .generation = generation,
                    .targeted_ledgers = std::move(targeted_ledgers),
                });
            census_committed = true;
            if (fail_connect_servant_launches_for_test_ != 0)
            {
                --fail_connect_servant_launches_for_test_;
                throw std::runtime_error("connect servant launch failpoint");
            }
            boost::asio::co_spawn(
                strand_, std::move(servant), boost::asio::detached);
        }
        catch (...)
        {
            if (!census_committed)
                throw;
            complete_connect_servant(
                key, std::current_exception(), SuccessorPump::defer);
        }
    }
}

//@@authority transition=prv.T0235 status=provisional scope=function
// bind=catl::peer_client::capture_crawl_observations@d7ff56a15d21a61b
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0081
// label=replace_reporter_scoped_bounded_crawl_observation_batch
bool
PeerSet::capture_crawl_observations(
    std::string const& reporter,
    CrawlResponse const& response) noexcept
{
    ASSERT_ON_STRAND();
    if (!options_.capture_crawl_observations)
        return true;

    try
    {
        if (before_crawl_observation_build_for_test_)
            before_crawl_observation_build_for_test_();

        auto const now = std::chrono::steady_clock::now();
        std::size_t aggregate_truncated_intervals = 0;
        std::map<std::string, CrawlObservation> deduplicated;
        for (auto const& peer : response.peers)
        {
            auto normalized =
                EndpointTracker::normalize_discovered_endpoint(peer.endpoint);
            if (!normalized)
                continue;

            auto [position, inserted] = deduplicated.try_emplace(
                *normalized,
                CrawlObservation{
                    .subject_endpoint = *normalized,
                    .reported_subject_key = peer.reported_public_key,
                    .intervals = peer.reported_intervals,
                    .truncated = peer.truncated});
            if (inserted)
                continue;

            auto& retained = position->second;
            if (retained.reported_subject_key && peer.reported_public_key &&
                retained.reported_subject_key != peer.reported_public_key)
            {
                retained.reported_subject_key.reset();
                retained.reported_key_conflict = true;
            }
            else if (
                !retained.reported_subject_key &&
                !retained.reported_key_conflict && peer.reported_public_key)
            {
                retained.reported_subject_key = peer.reported_public_key;
            }
            auto const dropped = merge_reported_intervals(
                retained.intervals, peer.reported_intervals);
            aggregate_truncated_intervals += dropped;
            retained.truncated =
                retained.truncated || peer.truncated || dropped != 0;
        }

        std::size_t retained_elsewhere = 0;
        for (auto const& [key, batch] : crawl_observation_batches_)
        {
            if (key == reporter || now - batch.observed_at >= kCrawlRetention)
                continue;
            auto const remaining = kMaxCrawlObservations - retained_elsewhere;
            retained_elsewhere = batch.observations.size() >= remaining
                ? kMaxCrawlObservations
                : retained_elsewhere + batch.observations.size();
        }
        auto const available = retained_elsewhere >= kMaxCrawlObservations
            ? 0
            : kMaxCrawlObservations - retained_elsewhere;

        CrawlObservationBatch batch;
        batch.observed_at = now;
        batch.observed_at_unix =
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        batch.crawl_epoch = next_crawl_observation_epoch_ + 1;
        batch.observations.reserve(std::min(available, deduplicated.size()));
        std::size_t truncated_observations = 0;
        for (auto& [_, observation] : deduplicated)
        {
            if (batch.observations.size() >= available)
            {
                ++truncated_observations;
                continue;
            }
            batch.observations.push_back(std::move(observation));
        }

        // Allocate the replacement map node before touching the published
        // table. merge() then commits that prepared node without allocation.
        std::map<std::string, CrawlObservationBatch> staged;
        if (!batch.observations.empty())
            staged.emplace(reporter, std::move(batch));

        if (before_crawl_observation_commit_for_test_)
            before_crawl_observation_commit_for_test_();

        for (auto it = crawl_observation_batches_.begin();
             it != crawl_observation_batches_.end();)
        {
            if (now - it->second.observed_at >= kCrawlRetention)
                it = crawl_observation_batches_.erase(it);
            else
                ++it;
        }
        crawl_observation_batches_.erase(reporter);
        crawl_observation_batches_.merge(staged);

        ++next_crawl_observation_epoch_;
        crawl_truncated_peer_rows_ += response.truncated_peer_rows;
        crawl_truncated_interval_rows_ +=
            response.truncated_interval_rows + aggregate_truncated_intervals;
        crawl_truncated_public_keys_ += response.truncated_public_keys;
        crawl_invalid_public_keys_ += response.invalid_public_keys;
        crawl_malformed_interval_tokens_ += response.malformed_interval_tokens;
        crawl_truncated_observations_ += truncated_observations;
        return true;
    }
    catch (...)
    {
        ++crawl_observation_capture_failures_;
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] Ignoring crawl observation capture failure reporter=",
            reporter);
        return false;
    }
}

void
PeerSet::apply_crawl_response(
    std::string const& reporter,
    CrawlResponse const& response,
    CrawlServantOutcome& outcome)
{
    ASSERT_ON_STRAND();
    outcome.raw = response.reported_peer_rows;
    outcome.used_tls = response.used_tls;
    outcome.truncated_peer_rows = response.truncated_peer_rows;
    outcome.truncated_interval_rows = response.truncated_interval_rows;
    outcome.truncated_public_keys = response.truncated_public_keys;

    // Cut 4 evidence cutover: crawl contributes only a bounded locator and a
    // reporter-scoped observation. It never calls the direct-status path.
    for (auto const& peer : response.peers)
    {
        if (peer.endpoint.empty())
            continue;

        auto normalized =
            EndpointTracker::normalize_discovered_endpoint(peer.endpoint);
        if (!normalized)
        {
            ++outcome.filtered;
            continue;
        }

        ++outcome.accepted;
        if (endpoint_cache_)
        {
            try
            {
                endpoint_cache_->remember_seen_crawl(network_id_, *normalized);
            }
            catch (...)
            {
            }
        }

        if (choose_crawl_status(peer.complete_ledgers))
            ++outcome.ranged;
        tracker_->add_discovered(*normalized, DiscoverySource::Crawl);
    }
    capture_crawl_observations(reporter, response);
}

//@@authority transition=prv.T0088 status=provisional scope=function
// bind=catl::peer_client::pump_crawls@98ba9d5913454cfb
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0034,prv.S0038,prv.S0062
// label=admit_owned_background_crawl_servant
void
PeerSet::pump_crawls()
{
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        return;
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

        auto self = shared_from_this();
        auto cancellation = std::make_shared<asio::cancellation_signal>();
        auto stop_token = crawl_stop_source_.get_token();
        // Allocate the exact once-only retirement capability before S0038 is
        // visible. Every post-admission failure path can then consume it
        // directly on the owner strand without allocating or scheduling.
        auto receipt_consumed = std::make_shared<std::atomic_bool>(false);
        auto before_stop_check = before_crawl_fallback_stop_check_for_test_;
        auto before_https_fallback = before_crawl_https_fallback_for_test_;
        auto servant = [self,
                        key,
                        host,
                        port,
                        stop_token,
                        before_stop_check = std::move(before_stop_check),
                        before_https_fallback =
                            std::move(before_https_fallback)]() mutable
            -> boost::asio::awaitable<CrawlServantOutcome> {
            PEER_SET_TRACE(
                "[",
                self->net_label_,
                "] Crawl servant running key=",
                host,
                ":",
                port,
                " stop_requested=",
                stop_token.stop_requested() ? "yes" : "no");
            CrawlServantOutcome outcome;
            auto response = co_await detail::co_fetch_peer_crawl_with_stop(
                host,
                port,
                stop_token,
                detail::CrawlFetchHooks{
                    .before_stop_check = std::move(before_stop_check),
                    .before_https_fallback = std::move(before_https_fallback)});

            self->apply_crawl_response(key, response, outcome);
            co_return outcome;
        };

        bool census_committed = false;
        try
        {
            auto [_, inserted] = crawl_in_flight_.try_emplace(
                key,
                CrawlServant{
                    .cancellation = cancellation,
                    .stop = stop_token,
                    .receipt_consumed = receipt_consumed});
            if (!inserted)
                continue;
            census_committed = true;
            PEER_SET_TRACE(
                "[",
                net_label_,
                "] Crawl servant admitted key=",
                key,
                " crawl_census=",
                crawl_in_flight_.size(),
                " queued=",
                crawl_queued_.size(),
                " pending=",
                pending_crawls_.size());
            if (fail_crawl_servant_launches_for_test_ != 0)
            {
                --fail_crawl_servant_launches_for_test_;
                throw std::runtime_error("crawl servant launch failpoint");
            }
            boost::asio::co_spawn(
                strand_,
                std::move(servant),
                asio::bind_cancellation_slot(
                    cancellation->slot(),
                    [self, key, receipt_consumed](
                        std::exception_ptr failure,
                        CrawlServantOutcome outcome) mutable {
                        assert(self->strand_.running_in_this_thread());
                        try
                        {
                            if (self->fail_crawl_completion_setup_for_test_ !=
                                0)
                            {
                                --self->fail_crawl_completion_setup_for_test_;
                                throw std::bad_alloc{};
                            }
                            auto finish = [self,
                                           key,
                                           outcome = std::move(outcome),
                                           failure = std::move(failure),
                                           receipt_consumed]() mutable {
                                if (receipt_consumed->exchange(true))
                                    return;
                                self->complete_crawl_servant(
                                    key, outcome, std::move(failure));
                            };
                            if (self->defer_crawl_servant_receipt_for_test_)
                            {
                                self->defer_crawl_servant_receipt_for_test_(
                                    key, finish);
                                return;
                            }
                            finish();
                        }
                        catch (...)
                        {
                            //@@mutation-site crawl-setup-failure-retires-row
                            if (!receipt_consumed->exchange(true))
                                self->complete_crawl_servant(
                                    key, {}, std::current_exception());
                            //@@mutation-end crawl-setup-failure-retires-row
                        }
                    }));
        }
        catch (...)
        {
            if (!census_committed)
                throw;
            if (!receipt_consumed->exchange(true))
                complete_crawl_servant(
                    key, {}, std::current_exception(), SuccessorPump::defer);
        }
    }
}

void
PeerSet::load_cached_endpoints()
{
    if (!endpoint_cache_)
        return;

    // Candidate import is optional policy; admitted-endpoint retry evidence
    // is not. Restore the complete bounded failure projection even when the
    // configured bootstrap/search candidate limit is zero.
    std::vector<PeerEndpointCache::Entry> active_failures;
    try
    {
        active_failures = endpoint_cache_->load_active_failures(network_id_);
    }
    catch (std::exception const& e)
    {
        PLOGW(
            log_,
            "[",
            net_label_,
            "] Peer cache failure restore failed: ",
            e.what());
        return;
    }
    asio::post(
        strand_,
        [self = shared_from_this(),
         active_failures = std::move(active_failures)]() {
            auto const steady_now = std::chrono::steady_clock::now();
            auto const unix_now = self->now_unix();
            for (auto const& entry : active_failures)
            {
                auto normalized = EndpointTracker::normalize_endpoint_identity(
                    entry.endpoint);
                if (normalized)
                {
                    self->restore_persistent_failure(
                        *normalized, entry, steady_now, unix_now);
                }
            }
        });

    if (options_.cached_endpoint_limit == 0)
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
        auto source = DiscoverySource::Unknown;
        if (entry.last_success_at <= 0)
        {
            if (entry.seen_endpoints)
                source = DiscoverySource::Endpoints;
            else if (entry.seen_redirect)
                source = DiscoverySource::Redirect;
            else if (entry.seen_crawl)
                source = DiscoverySource::Crawl;
        }
        tracker_->add_discovered(key, source);
        if (entry.status.first_seq != 0 && entry.status.last_seq != 0)
            ++loaded_with_range;
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

//@@authority transition=prv.T0221 status=provisional scope=function
// bind=catl::peer_client::restore_persistent_failure@812f6c5cac16ae48
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0008,prv.S0076
// label=restore_bounded_durable_retry_evidence
void
PeerSet::restore_persistent_failure(
    std::string const& endpoint,
    PeerEndpointCache::Entry const& entry,
    std::chrono::steady_clock::time_point steady_now,
    std::int64_t unix_now)
{
    ASSERT_ON_STRAND();
    auto const durable_failure_active = entry.failure_count > 0 &&
        (entry.last_failure_at > entry.last_success_at ||
         (entry.last_failure_at == entry.last_success_at &&
          !entry.connected_ok));
    if (!durable_failure_active)
        return;

    // Retry time and retry magnitude are one fact. Keep them in the same
    // bounded gate so endpoint_stats_ eviction cannot silently reduce an
    // admitted backoff to zero.
    auto& stats = endpoint_stats_[endpoint];
    stats.last_success_at =
        std::max(stats.last_success_at, entry.last_success_at);
    stats.last_failure_at =
        std::max(stats.last_failure_at, entry.last_failure_at);
    stats.success_count = std::max(stats.success_count, entry.success_count);
    stats.failure_count = std::max(stats.failure_count, entry.failure_count);
    stats.connected_ok = false;

    auto const age_seconds =
        std::max<std::int64_t>(0, unix_now - entry.last_failure_at);
    auto const reconstructed = steady_now - std::chrono::seconds(age_seconds);
    auto& gate = retry_gates_[endpoint];
    gate.failed_at = std::max(gate.failed_at, reconstructed);
    gate.failure_count = std::max(gate.failure_count, entry.failure_count);
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

//@@authority transition=prv.T0089 status=provisional scope=function
// bind=catl::peer_client::start_connect@41e1125ff34a6e3c
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0034,prv.S0037
// label=queue_endpoint_under_owned_connect_servant_census
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
    if (auto configured =
            EndpointTracker::normalize_endpoint_identity(endpoint))
        configured_endpoints_.insert(*configured);
    queue_connect(endpoint);
    pump_connects();
}

//@@authority transition=prv.T0090 status=provisional scope=function
// bind=catl::peer_client::start_connect@e1eb79fe7d36ee43
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0034,prv.S0037
// label=queue_host_port_under_owned_connect_servant_census
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
    auto const endpoint = make_key(host, port);
    if (auto configured =
            EndpointTracker::normalize_endpoint_identity(endpoint))
        configured_endpoints_.insert(*configured);
    queue_connect(endpoint);
    pump_connects();
}

boost::asio::awaitable<PeerGenerationPin>
PeerSet::try_connect(std::string const& host, uint16_t port)
{
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        co_return PeerGenerationPin{};

    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self,
         host = std::string(host),
         port]() mutable -> asio::awaitable<PeerGenerationPin> {
            if (auto configured =
                    EndpointTracker::normalize_endpoint_identity(host, port))
            {
                self->configured_endpoints_.insert(*configured);
            }
            auto client = co_await self->try_connect_on_strand(
                std::move(host), port, 0, {});
            if (!client)
                co_return PeerGenerationPin{};
            co_return self->make_peer_pin(client);
        },
        asio::use_awaitable);
}

boost::asio::awaitable<PeerSessionPtr>
PeerSet::try_connect_on_strand(
    std::string host,
    uint16_t port,
    std::uint64_t reserved_generation,
    std::vector<TargetedAdmissionLeasePtr> admission_leases,
    std::optional<std::chrono::steady_clock::time_point> absolute_deadline)
{
    ASSERT_ON_STRAND();
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        co_return nullptr;

    auto key = make_key(host, port);
    if (key.empty())
        co_return nullptr;

    // Already connected? This can change while co_connect() is suspended,
    // so the result is checked again before insertion below.
    auto it = connections_.find(key);
    if (it != connections_.end())
    {
        co_return (
            it->second.session && ready_status_for(key, it->second.generation))
            ? it->second.session
            : nullptr;
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
    std::uint64_t generation = reserved_generation;

    try
    {
        // Pass all handlers via ConnectOptions so they're installed
        // before the read loop starts — no cross-strand races.
        PeerClient::ConnectOptions connect_opts;
        connect_opts.network_id = network_id_;
        connect_opts.tracker = tracker_;
        connect_opts.initial_connect_absolute_deadline = absolute_deadline;
        {
            std::lock_guard lock(unsolicited_handler_mutex_);
            connect_opts.unsolicited_handler = unsolicited_handler_;
        }
        // Mint this candidate's instance identity before any handler can
        // fire. The generation travels both in terminal closures and in the
        // session's sealed command checks: a death between connect and
        // admission then consumes as a stale instance instead of erasing
        // whichever peer currently owns the key.
        //@@authority transition=peer_set.mint_connection_generation v4=T31
        // bind=catl::peer_client::try_connect_on_strand@c7999a3ac8888e4b
        // owner=peer_set commit=strand
        if (generation == 0)
        {
            generation = reserve_connection_generation(
                key,
                absolute_deadline.value_or(
                    std::chrono::steady_clock::now() +
                    kOrdinaryConnectDeadline));
        }
        auto generation_row = peer_generations_.find(key);
        if (generation == 0 || generation_row == peer_generations_.end() ||
            generation_row->second.lifecycle.generation() != generation ||
            generation_row->second.lifecycle.lifecycle() !=
                PeerLifecycle::created ||
            generation_row->second.physical_deadline <=
                std::chrono::steady_clock::now())
        {
            PLOGW(
                log_,
                "[",
                net_label_,
                "] refusing missing or expired generation reservation for ",
                key);
            abandon_unstarted_generation(key, generation);
            co_return nullptr;
        }
        connect_opts.connection_generation = generation;
        {
            auto self_weak = weak_from_this();
            connect_opts.on_lifecycle = [self_weak](PeerLifecycleEvent event) {
                if (auto self2 = self_weak.lock())
                {
                    asio::post(
                        self2->strand_,
                        [self2, event = std::move(event)]() mutable {
                            self2->consume_lifecycle_event(std::move(event));
                        });
                }
            };
        }
        co_await co_connect(
            io_,
            host,
            port,
            std::move(connect_opts),
            client,
            10,
            [self = shared_from_this(), key, generation](
                std::shared_ptr<PeerClient> const& created) noexcept {
                self->install_connect_servant_session(key, generation, created);
                self->install_generation_session(key, generation, created);
            });
        ASSERT_ON_STRAND();
        // The client publishes Ready before completing the connect bridge.
        // Yield once on this strand so that value event is consumed before
        // admission consults the canonical generation record.
        co_await asio::post(strand_, asio::use_awaitable);
        ASSERT_ON_STRAND();
        if (after_connect_ready_before_admission_for_test_)
        {
            co_await after_connect_ready_before_admission_for_test_(client);
            ASSERT_ON_STRAND();
        }

        // A candidate whose handshake crossed the seal was never admitted.
        // Consume its physical lifecycle before returning so the stop receipt
        // cannot race an unowned socket close.
        if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        {
            if (client)
            {
                co_await client->co_disconnect();
                classify_unadmitted_connect_terminal(key, generation, client);
            }
            co_return nullptr;
        }

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
            co_await client->co_disconnect();
            classify_unadmitted_connect_terminal(key, generation, client);
            co_return existing->second.session;
        }

        generation_row = peer_generations_.find(key);
        // The lifecycle snapshot and the session's live state are separate
        // facts. A peer failure can commit on the client strand after Ready
        // while its terminal lifecycle event is still queued behind this
        // admission coroutine. Require both facts at the owner boundary so a
        // stale Ready observation cannot admit an already-terminal session.
        //@@mutation-site admission-requires-live-ready-session
        if (!client || !client->is_ready() ||
            generation_row == peer_generations_.end() ||
            generation_row->second.lifecycle.generation() != generation ||
            !generation_row->second.lifecycle.ready())
        //@@mutation-end admission-requires-live-ready-session
        {
            PLOGD(
                log_,
                "[",
                net_label_,
                "] Connected candidate became unavailable before admission: ",
                key);
            if (client)
            {
                co_await client->co_disconnect();
                classify_unadmitted_connect_terminal(key, generation, client);
            }
            co_return nullptr;
        }

        if (generation_row->second.history_origin)
        {
            // Demand-origin generations stay outside ordinary/persistent pool
            // membership. Their exact origin and independently sponsored uses
            // are the only authority for retaining the ready-transient slot.
            retry_gates_.erase(key);
            advance_history_pool_revision();
            co_return client;
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
            co_await client->co_disconnect();
            classify_unadmitted_connect_terminal(key, generation, client);
            co_return nullptr;
        }

        connections_[key] = ConnectionEntry{client, generation};
        ++total_connects_;
        retry_gates_.erase(key);
        auto const& ready_status = *generation_row->second.lifecycle.status();
        auto const advertised_range = ready_status.advertised_range;
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] targeted connect handshake endpoint=",
            key,
            " outcome=live range=",
            advertised_range.first_seq,
            "-",
            advertised_range.last_seq);
        note_connect_success(
            key,
            {.first_seq = advertised_range.first_seq,
             .last_seq = advertised_range.last_seq,
             .current_seq = ready_status.current_seq});
        note_peer_headers(key, ready_status.headers);
        if (endpoint_cache_)
        {
            try
            {
                endpoint_cache_->remember_connect_success(
                    network_id_,
                    key,
                    {.first_seq = advertised_range.first_seq,
                     .last_seq = advertised_range.last_seq,
                     .current_seq = ready_status.current_seq});
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
            advertised_range.first_seq,
            "-",
            advertised_range.last_seq,
            ")");
        co_return client;
    }
    catch (PeerClientException const& e)
    {
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] targeted connect handshake endpoint=",
            key,
            " outcome=failed detail=",
            e.what());
        auto const stop_owned_cancellation = [&]() {
            if (!client ||
                //@@mutation-site connect-stop-requires-explicit-owner-cause
                e.connect_disposition !=
                    ConnectAttemptDisposition::owner_disconnect
                //@@mutation-end connect-stop-requires-explicit-owner-cause
            )
            {
                return false;
            }
            auto const row = in_flight_.find(key);
            return row != in_flight_.end() && row->second.session == client &&
                row->second.stop_cancellation_requested;
        }();

        if (stop_owned_cancellation)
        {
            PLOGD(
                log_,
                "[",
                net_label_,
                "] Connect servant canceled by shutdown: ",
                key);
        }
        else
        {
            if (connect_failure_classification_hook_for_test_)
                connect_failure_classification_hook_for_test_();

            auto redirected = false;
            std::size_t redirect_count = 0;
            std::size_t filtered = 0;

            // Harvest 503 redirect IPs
            if (client)
            {
                auto const& ips = client->raw_connection().redirect_ips();
                redirect_count = ips.size();
                redirected = redirect_count > 0;
                auto history_redirects = capture_history_redirect_successors(
                    key, generation, port, ips);
                if (history_redirects)
                {
                    redirect_count = history_redirects->first;
                    filtered = history_redirects->second;
                }
                else
                {
                    for (auto const& ip : ips)
                    {
                        auto normalized =
                            EndpointTracker::normalize_discovered_host(
                                ip, port);
                        if (!normalized || *normalized == key)
                        {
                            ++filtered;
                            continue;
                        }
                        tracker_->add_discovered(
                            *normalized, DiscoverySource::Redirect);
                    }
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
                    // Service-owned redirects may feed ordinary discovery.
                    // Demand-owned successors remain acquisition-scoped and
                    // require a fresh exact creation grant.
                    if (!history_redirects)
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
        }
    }
    catch (std::exception const& e)
    {
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] targeted connect handshake endpoint=",
            key,
            " outcome=failed detail=",
            e.what());
        PLOGW(
            log_,
            "[",
            net_label_,
            "] Failed to connect to ",
            key,
            ": ",
            e.what());
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
    }

    // Exception handlers above only classify and persist failure evidence.
    // Physical lifecycle consumption remains awaitable and outside the catch
    // handlers, where coroutine suspension is legal.
    if (client)
        co_await client->co_disconnect();
    else
        abandon_unstarted_generation(key, generation);
    co_return nullptr;
}

void
PeerSet::bootstrap()
{
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        return;
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
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        return;
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
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        return;
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
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        return;
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
    return claim_at(std::chrono::steady_clock::now());
}

PeerSet::TargetedAdmissionLeasePtr
PeerSet::TargetedConnectAuthorization::claim_at(
    std::chrono::steady_clock::time_point now)
{
    std::lock_guard lock(leases_mutex);
    if (!is_active_at(now))
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

//@@authority transition=prv.T0025 status=provisional scope=function
// bind=catl::peer_client::acquire_wanted_ledger@14f6aab6caf926b8
// owner=unresolved owner_hypothesis=peer_set commit=strand
// touches=prv.S0010 label=register_history_target_interest
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

//@@authority transition=prv.T0026 status=provisional scope=function
// bind=catl::peer_client::release_wanted_ledger@18500505df2f9513
// owner=unresolved owner_hypothesis=peer_set commit=strand
// touches=prv.S0010 label=release_history_target_interest
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
        auto& authorizations = it->second.target_authorizations;
        authorizations.erase(
            std::remove_if(
                authorizations.begin(),
                authorizations.end(),
                [&](auto const& current) {
                    return !current || !current->is_active() ||
                        current == authorization;
                }),
            authorizations.end());
        if (authorizations.empty() && !it->second.ordinary_discovery_authorized)
        {
            auto const endpoint = it->first;
            auto const generation = it->second.generation;
            queued_.erase(it->first);
            std::erase(pending_connects_, it->first);
            it = targeted_connects_.erase(it);
            abandon_unstarted_generation(endpoint, generation);
        }
        else
            ++it;
    }
}

//@@authority transition=prv.T0217 status=provisional scope=function
// bind=catl::peer_client::load_cached_candidates_for@75f0027fce6aa348
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0008,prv.S0076
// label=merge_bounded_persistent_history_hints_without_overwriting_live_state
std::vector<std::string>
PeerSet::load_cached_candidates_for(uint32_t ledger_seq)
{
    ASSERT_ON_STRAND();
    if (!endpoint_cache_ || options_.cached_endpoint_limit == 0)
    {
        if (!endpoint_cache_error_.empty())
        {
            catl::core::emit_status(
                "peer history cache unavailable; searching live discoveries "
                "only");
        }
        return {};
    }

    std::vector<PeerEndpointCache::Entry> cached;
    try
    {
        cached = endpoint_cache_->load_covering_candidates(
            network_id_, ledger_seq, options_.cached_endpoint_limit);
    }
    catch (std::exception const& e)
    {
        endpoint_cache_error_ = e.what();
        PLOGW(
            log_,
            "[",
            net_label_,
            "] Targeted peer cache lookup failed for ledger ",
            ledger_seq,
            ": ",
            e.what());
        catl::core::emit_status(
            "peer history cache unavailable; searching live discoveries "
            "only");
        return {};
    }

    std::vector<std::string> candidates;
    std::unordered_set<std::string> seen;
    candidates.reserve(cached.size());
    auto const steady_now = std::chrono::steady_clock::now();
    auto const unix_now = now_unix();
    for (auto const& entry : cached)
    {
        auto normalized =
            EndpointTracker::normalize_discovered_endpoint(entry.endpoint);
        if (!normalized || !seen.insert(*normalized).second)
            continue;

        auto const& key = *normalized;
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] targeted ledger ",
            ledger_seq,
            " candidate ",
            key,
            " provenance=persistent_hint remembered_range=",
            entry.status.first_seq,
            "-",
            entry.status.last_seq);
        // A persisted range is a search hint, not a fresh observation. Keep
        // any current owner-local range intact, but let the hint fill an
        // otherwise unknown range so a recently rediscovered endpoint can be
        // ranked for this bounded search. Do not publish the imported range
        // through EndpointTracker's live-status observer.
        auto const current = endpoint_stats_.find(key);
        if (current == endpoint_stats_.end())
        {
            auto normalized_entry = entry;
            normalized_entry.endpoint = key;
            update_endpoint_stats(normalized_entry);
        }
        else
        {
            auto& stats = current->second;
            if (stats.status.first_seq == 0 || stats.status.last_seq == 0)
            {
                stats.status = entry.status;
                stats.range_is_current_live = false;
                stats.range_is_persistent_hint = true;
            }

            // Range provenance and durable retry evidence are independent.
            // A current live range must win without erasing a more recent
            // persisted failure for the same logical endpoint.
            stats.last_success_at =
                std::max(stats.last_success_at, entry.last_success_at);
            stats.last_failure_at =
                std::max(stats.last_failure_at, entry.last_failure_at);
            stats.success_count =
                std::max(stats.success_count, entry.success_count);
            stats.failure_count =
                std::max(stats.failure_count, entry.failure_count);
            stats.seen_crawl = stats.seen_crawl || entry.seen_crawl;
            stats.seen_endpoints = stats.seen_endpoints || entry.seen_endpoints;
            stats.seen_redirect = stats.seen_redirect || entry.seen_redirect;
            if (entry.failure_count > 0 &&
                (entry.last_failure_at > entry.last_success_at ||
                 (entry.last_failure_at == entry.last_success_at &&
                  !entry.connected_ok)))
            {
                stats.connected_ok = false;
            }
            else if (entry.last_success_at > entry.last_failure_at)
            {
                stats.connected_ok = true;
            }
        }

        // A persistent row authorizes bounded search, not immediate retry.
        // Restore its independent durable failure evidence through the same
        // owner-strand transition used by startup.
        restore_persistent_failure(key, entry, steady_now, unix_now);
        candidates.push_back(key);
    }
    return candidates;
}

std::vector<std::string>
PeerSet::try_candidates_for(
    TargetedConnectAuthorizationPtr const& authorization,
    std::unordered_set<std::string> const& selection_excluded,
    std::size_t connect_budget)
{
    ASSERT_ON_STRAND();
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
    {
        PLOGI(
            log_,
            "[",
            net_label_,
            "] Targeted peer search suppressed: shutdown ingress is sealed");
        return {};
    }
    if (!authorization)
    {
        PLOGW(
            log_,
            "[",
            net_label_,
            "] Targeted peer search suppressed: missing authorization");
        return {};
    }
    if (!authorization->is_active())
    {
        PLOGI(
            log_,
            "[",
            net_label_,
            "] Targeted peer search for ledger ",
            authorization->ledger_seq,
            " suppressed: authorization is inactive");
        return {};
    }
    auto const ledger_seq = authorization->ledger_seq;

    auto candidates = tracker_->all_endpoints();
    auto cached_candidates = load_cached_candidates_for(ledger_seq);
    candidates.insert(
        candidates.end(),
        std::make_move_iterator(cached_candidates.begin()),
        std::make_move_iterator(cached_candidates.end()));

    std::vector<std::string> unique_candidates;
    std::unordered_set<std::string> seen_candidates;
    unique_candidates.reserve(candidates.size());
    for (auto const& endpoint : candidates)
    {
        auto key = canonical_endpoint(endpoint);
        if (!key.empty() && seen_candidates.insert(key).second)
            unique_candidates.push_back(std::move(key));
    }
    candidates = std::move(unique_candidates);

    PLOGI(
        log_,
        "[",
        net_label_,
        "] Targeted peer search for ledger ",
        ledger_seq,
        ": ",
        cached_candidates.size(),
        " persistent covering hint(s), ",
        candidates.size(),
        " canonical candidate(s), budget=",
        connect_budget);

    auto stats_for =
        [&](std::string const& raw_endpoint) -> EndpointStats const* {
        auto const key = canonical_endpoint(raw_endpoint);
        auto stats = endpoint_stats_.find(key);
        return stats != endpoint_stats_.end() ? &stats->second : nullptr;
    };
    auto range_for = [&](std::string const& raw_endpoint) {
        auto const* stats = stats_for(raw_endpoint);
        return stats ? stats->status : PeerStatus{};
    };
    auto const now = std::chrono::steady_clock::now();
    candidates.erase(
        std::remove_if(
            candidates.begin(),
            candidates.end(),
            [&](std::string const& raw_endpoint) {
                auto const key = canonical_endpoint(raw_endpoint);
                auto const* stats = stats_for(key);
                auto const status = stats ? stats->status : PeerStatus{};
                auto const has_known_range =
                    status.first_seq != 0 && status.last_seq != 0;
                auto const range_authorizes_hard_exclusion = stats &&
                    (stats->range_is_current_live ||
                     stats->range_is_persistent_hint);
                auto const known_not_covering =
                    range_authorizes_hard_exclusion && has_known_range &&
                    (ledger_seq < status.first_seq ||
                     ledger_seq > status.last_seq);
                auto const provenance = stats && stats->range_is_current_live
                    ? "current_live"
                    : stats && stats->range_is_persistent_hint
                    ? "persistent_hint"
                    : "unknown";
                auto const covering = has_known_range &&
                    ledger_seq >= status.first_seq &&
                    ledger_seq <= status.last_seq;
                char const* reason = nullptr;
                if (key.empty())
                    reason = "invalid";
                else if (selection_excluded.count(key))
                    reason = "excluded";
                else if (connections_.count(key))
                    reason = "already_connected";
                else if (in_flight_.count(key))
                    reason = "in_flight";
                else if (peer_failed_for_ledger(key, ledger_seq, now))
                    reason = "ledger_failed";
                else if (known_not_covering)
                    reason = "noncovering";
                if (reason)
                {
                    PEER_SET_TRACE(
                        "[",
                        net_label_,
                        "] targeted ledger ",
                        ledger_seq,
                        " candidate ",
                        key,
                        " provenance=",
                        provenance,
                        " remembered_range=",
                        status.first_seq,
                        "-",
                        status.last_seq,
                        " covering=",
                        covering ? "yes" : "no",
                        " exclusion=",
                        reason);
                    return true;
                }
                auto failed = retry_gates_.find(key);
                auto const backed_off = failed != retry_gates_.end() &&
                    now - failed->second.failed_at < retry_backoff_for(key);
                if (backed_off)
                    PEER_SET_TRACE(
                        "[",
                        net_label_,
                        "] targeted ledger ",
                        ledger_seq,
                        " candidate ",
                        key,
                        " provenance=",
                        provenance,
                        " remembered_range=",
                        status.first_seq,
                        "-",
                        status.last_seq,
                        " covering=",
                        covering ? "yes" : "no",
                        " exclusion=retry_backoff");
                return backed_off;
            }),
        candidates.end());

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
    std::size_t rank = 0;
    for (auto const& endpoint : candidates)
    {
        if (queued.size() >= connect_budget)
            break;
        auto const key = canonical_endpoint(endpoint);
        auto const status = range_for(key);
        auto const* candidate_stats = stats_for(key);
        auto const provenance =
            candidate_stats && candidate_stats->range_is_current_live
            ? "current_live"
            : candidate_stats && candidate_stats->range_is_persistent_hint
            ? "persistent_hint"
            : "unknown";
        auto const covering = status.first_seq != 0 && status.last_seq != 0 &&
            ledger_seq >= status.first_seq && ledger_seq <= status.last_seq;
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] targeted ledger ",
            ledger_seq,
            " candidate ",
            key,
            " provenance=",
            provenance,
            " rank=",
            rank++,
            " covering=",
            covering ? "yes" : "no",
            " remembered_range=",
            status.first_seq,
            "-",
            status.last_seq,
            " queue_attempt");
        if (queue_targeted_connect(key, authorization))
        {
            queued.push_back(key);
            PEER_SET_TRACE(
                "[",
                net_label_,
                "] targeted ledger ",
                ledger_seq,
                " candidate ",
                key,
                " queue=claimed");
        }
        else
            PEER_SET_TRACE(
                "[",
                net_label_,
                "] targeted ledger ",
                ledger_seq,
                " candidate ",
                key,
                " queue=rejected");
    }

    sort_pending_connects();
    sort_pending_crawls();
    pump_crawls();
    pump_connects();
    return queued;
}

/// Projection into PeerSet's owner strand. This function mints nothing: the
/// authority-bearing seal lives in seal_node_batch_command_on_strand, which is
/// where the directive belongs. authority-coverage.py's capability-mint rule
/// proved the marker was on this wrapper while the mint was in the helper.
bool
PeerSet::seal_node_batch_command(
    NodeBatchIntent intent,
    PeerGenerationPin pin_share,
    asio::any_io_executor completion_executor,
    std::function<void(std::optional<NodeBatchCommand>)> on_sealed)
{
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire) ||
        !on_sealed || !completion_executor)
        return false;

    // Sealing is owner-strand authority, so enter it rather than validating
    // against connections_ from the caller's executor. `post` can throw before
    // ownership transfers, and a void API could not tell the caller that
    // nothing was queued, so the failure is reported rather than swallowed.
    try
    {
        if (fail_seal_projection_for_test_)
            throw std::runtime_error("injected seal projection failure");
        asio::post(
            strand_,
            [self = shared_from_this(),
             intent = std::move(intent),
             pin = std::move(pin_share),
             completion_executor = std::move(completion_executor),
             on_sealed = std::move(on_sealed)]() mutable {
                auto sealed = self->seal_node_batch_command_on_strand(
                    std::move(intent), pin);
                self->deliver_seal_result(
                    std::move(completion_executor),
                    std::move(on_sealed),
                    std::move(sealed));
            });
    }
    catch (...)
    {
        return false;
    }
    return true;
}

//@@callback-funnel
void
PeerSet::deliver_seal_result(
    asio::any_io_executor completion_executor,
    std::function<void(std::optional<NodeBatchCommand>)> on_sealed,
    std::optional<NodeBatchCommand> sealed)
{
    // The ONE place a seal result reaches caller code. The consumer runs on its
    // own executor, so it cannot unwind a PeerSet owner-strand handler and
    // cannot re-enter PeerSet while the sealing transition is still on the
    // stack. Containment is a property of this function rather than of every
    // call site, which is what makes try/catch around an arbitrary callback the
    // wrong tool here.
    //
    // If the completion cannot be queued the capability is DROPPED rather than
    // delivered from the wrong executor: an unsealed row is retryable, whereas
    // a command delivered onto PeerSet's strand is an authority leak.
    if (drop_seal_completions_for_test_ > 0)
    {
        --drop_seal_completions_for_test_;
        // Default-inert failpoint AT THE REAL BOUNDARY. It reproduces a lost
        // completion post exactly: the command is destroyed here and the
        // consumer is never invoked, so a sender that only reacts to callbacks
        // learns nothing, ever. There is deliberately no way to report this to
        // the caller — a void delivery path has nobody to report to — which is
        // the reason the coordinator owns a bind deadline.
        PLOGE(
            log_,
            "seal completion dropped by failpoint; capability destroyed and no "
            "consumer will run");
        return;
    }
    try
    {
        asio::post(
            std::move(completion_executor),
            [on_sealed = std::move(on_sealed),
             sealed = std::move(sealed)]() mutable {
                // Containment must be INSIDE the posted lambda. The try around
                // asio::post covers initiation only and has already returned by
                // the time this body runs on the completion executor, so a
                // throwing consumer would otherwise escape that executor's
                // io_context::run(). Codex found this by fixing the auditor's
                // containment search, which had been crossing the lambda
                // boundary while looking for a lexical try.
                try
                {
                    on_sealed(std::move(sealed));
                }
                catch (...)
                {
                    PLOGE(
                        log_,
                        "seal completion consumer threw; contained at the "
                        "callback boundary");
                }
            });
    }
    catch (...)
    {
        // Initiation failure only. The capability is dropped rather than
        // delivered from the wrong executor. Making this loss sender-VISIBLE
        // needs the coordinator's retained awaiting_bind state and a bounded
        // retry; it is deliberately not a second retry owner inside PeerSet.
        PLOGE(
            log_,
            "seal completion could not be queued; capability dropped. "
            "Recovery is the coordinator's bounded bind retry, not PeerSet's");
    }
}

//@@authority transition=peer_set.seal_node_batch_command owner=peer_set
// bind=catl::peer_client::seal_node_batch_command_on_strand@ff584bc894c3403a
// commit=strand
std::optional<NodeBatchCommand>
PeerSet::seal_node_batch_command_on_strand(
    NodeBatchIntent intent,
    PeerGenerationPin const& pin)
{
    ASSERT_ON_STRAND();
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire) ||
        !strand_.running_in_this_thread() || !pin)
        return std::nullopt;

    // The pin must have been minted by THIS PeerSet. A pin from another
    // instance names a generation this owner never admitted, so it can never
    // authorize a seal here.
    if (pin.release_owner().get() != this)
        return std::nullopt;

    // Its pin row must still be active. A released or pruned pin no longer
    // holds the eviction veto the seal depends on, even if the connection map
    // still happens to agree. Every field the bind claims is proved against the
    // authoritative row, including the endpoint: relying on the pin
    // constructor's history would be trusting provenance instead of checking.
    auto const active = active_peer_pins_.find(pin.pin_id());
    if (active == active_peer_pins_.end() ||
        active->second.connection_generation != pin.connection_generation() ||
        active->second.endpoint != pin.endpoint())
    {
        return std::nullopt;
    }

    // Exactness, mirroring make_peer_pin: the SAME session object, still
    // admitted at that endpoint, still in the generation the pin names, still
    // ready. Anything else is a refusal, never a retarget.
    auto const connection = connections_.find(pin.endpoint());
    if (connection == connections_.end() || !connection->second.session ||
        connection->second.session.get() != pin.get() ||
        connection->second.generation != pin.connection_generation() ||
        !ready_status_for(pin.endpoint(), connection->second.generation))
    {
        return std::nullopt;
    }

    // Commit section: move, never copy, and prove the WHOLE expression cannot
    // throw — not merely that the intent's move cannot. The narrowest useful
    // assertion is over the construction itself; asserting only
    // is_nothrow_move_constructible_v<NodeBatchIntent> left the unqualified
    // constructor potentially-throwing in the type system.
    // std::is_nothrow_constructible_v is the wrong instrument here: the trait
    // is evaluated as if in a context unrelated to the type, so PeerSet's
    // friendship is invisible to it and a private constructor reads as not
    // constructible at all. The noexcept operator is evaluated HERE, where the
    // friendship holds, so it actually tests the expression we commit with.
    static_assert(
        noexcept(NodeBatchCommand{
            std::declval<NodeBatchIntent>(),
            std::declval<PeerConnectionGenerationId>()}),
        "sealing runs in an owner-strand commit section after every guard has "
        "passed; the mint must not be able to throw");
    // The optional return moves the command, so that move must be nothrow too.
    static_assert(std::is_nothrow_move_constructible_v<NodeBatchCommand>);
    return NodeBatchCommand{
        std::move(intent),
        PeerConnectionGenerationId{connection->second.generation}};
}

PeerGenerationPin
PeerSet::make_peer_pin(PeerSessionPtr const& expected_session)
{
    ASSERT_ON_STRAND();
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire) ||
        !strand_.running_in_this_thread() || !expected_session ||
        next_peer_pin_id_ == std::numeric_limits<std::uint64_t>::max())
    {
        return {};
    }

    auto const key = canonical_endpoint(expected_session->endpoint());
    auto connection = connections_.find(key);
    if (connection == connections_.end() || !connection->second.session ||
        connection->second.session != expected_session ||
        !ready_status_for(key, connection->second.generation))
    {
        return {};
    }

    auto const pin_id = next_peer_pin_id_ + 1;
    auto const generation = connection->second.generation;
    auto weak = weak_from_this();
    PeerGenerationPin pin{
        connection->second.session,
        key,
        generation,
        pin_id,
        weak,
        [weak, pin_id, generation]() {
            if (auto self = weak.lock())
            {
                asio::post(
                    self->strand_, [self, pin_id, generation]() noexcept {
                        self->release_peer_pin(pin_id, generation);
                    });
            }
        }};

    auto [_, inserted] = active_peer_pins_.emplace(
        pin_id,
        ActivePeerPin{
            .endpoint = key,
            .connection_generation = generation,
            .live_handle = pin.state_,
        });
    if (!inserted)
        return {};

    next_peer_pin_id_ = pin_id;
    return pin;
}

PeerGenerationRef
PeerSet::make_generation_ref(PeerSessionPtr const& expected_session) const
{
    ASSERT_ON_STRAND();
    if (!expected_session || peer_set_instance_id_ == 0)
        return {};

    auto const key = canonical_endpoint(expected_session->endpoint());
    auto const connection = connections_.find(key);
    if (connection == connections_.end() || !connection->second.session ||
        connection->second.session != expected_session)
    {
        return {};
    }
    auto const* status = ready_status_for(key, connection->second.generation);
    if (!status)
        return {};
    return PeerGenerationRef{
        peer_set_instance_id_,
        key,
        connection->second.generation,
        peer_generations_.at(key).lifecycle.last_event_sequence(),
        *status};
}

//@@authority transition=prv.T0262 status=provisional scope=function
// bind=catl::peer_client::get_or_create_ledger_demand_on_strand@ed661f14cbeb9d85
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0102,prv.S0103,prv.S0104
// label=atomic_get_or_create_request_ledger_parent
LedgerDemandRef
PeerSet::get_or_create_ledger_demand_on_strand(
    std::shared_ptr<catl::core::RequestScope> const& request_scope,
    LedgerDemandOptions options)
{
    ASSERT_ON_STRAND();
    prune_ledger_demand_tombstones();

    if (shutdown_ingress_sealed_.load(std::memory_order_acquire) ||
        lifecycle_ != Lifecycle::running || peer_set_instance_id_ == 0)
    {
        throw PeerClientException(
            Error::Cancelled, "history demand owner is stopping");
    }
    if (!request_scope || !request_scope->id() || options.ledger_seq == 0)
        throw PeerClientException(
            Error::BadRequest, "invalid history demand identity");

    auto const now = std::chrono::steady_clock::now();
    auto const request = request_scope->snapshot(now);
    if (request.terminal)
    {
        throw PeerClientException(
            *request.terminal ==
                    catl::core::RequestScopeTerminal::deadline_elapsed
                ? Error::Timeout
                : Error::Cancelled,
            "history demand request is terminal");
    }

    if (options.fallback_at == std::chrono::steady_clock::time_point{})
        options.fallback_at =
            request.absolute_deadline - std::chrono::steady_clock::duration{1};
    if (options.fallback_at >= request.absolute_deadline)
        throw PeerClientException(
            Error::BadRequest,
            "history demand fallback must precede its deadline");

    options.remaining_new_dials = std::min(
        options.remaining_new_dials, options_.max_targeted_connect_attempts);
    options.remaining_promotions = std::min(
        options.remaining_promotions,
        options_.max_history_promotions_per_demand);
    options.remaining_blind_uses = std::min(
        options.remaining_blind_uses,
        options_.max_history_blind_uses_per_demand);

    auto const key = std::pair{request.id.value, options.ledger_seq};
    if (auto const existing_key = ledger_demand_keys_.find(key);
        existing_key != ledger_demand_keys_.end())
    {
        auto const existing = ledger_demands_.find(existing_key->second);
        if (existing == ledger_demands_.end())
            throw PeerClientException(
                Error::Cancelled, "history demand index is unavailable");
        auto const& row = existing->second;
        if (row.options != options ||
            row.absolute_deadline != request.absolute_deadline)
        {
            throw PeerClientException(
                Error::DuplicateRequest,
                "history demand options disagree with the existing parent");
        }
        return LedgerDemandRef{
            peer_set_instance_id_,
            row.id,
            row.request_scope_id,
            row.options.ledger_seq,
            row.absolute_deadline};
    }

    if (ledger_demands_.size() >= options_.max_active_ledger_demands ||
        next_ledger_demand_id_ == std::numeric_limits<std::uint64_t>::max())
    {
        throw PeerClientException(
            Error::ResourceExhausted, "history demand capacity exhausted");
    }

    LedgerDemandId const id{next_ledger_demand_id_};
    auto prepared_token_id = next_history_budget_token_id_;
    auto prepare_tokens = [&prepared_token_id](std::size_t count) {
        std::vector<LedgerDemandRecord::BudgetToken> tokens;
        tokens.reserve(count);
        for (std::size_t i = 0; i < count; ++i)
        {
            if (prepared_token_id == std::numeric_limits<std::uint64_t>::max())
            {
                throw PeerClientException(
                    Error::ResourceExhausted,
                    "history demand token identity exhausted");
            }
            tokens.push_back(
                LedgerDemandRecord::BudgetToken{.id = prepared_token_id++});
        }
        return tokens;
    };
    auto dial_tokens = prepare_tokens(options.remaining_new_dials);
    auto promotion_tokens = prepare_tokens(options.remaining_promotions);
    auto blind_tokens = prepare_tokens(options.remaining_blind_uses);
    auto timer = std::make_shared<asio::steady_timer>(
        strand_, request.absolute_deadline);
    auto weak = weak_from_this();
    auto subscription = request_scope->subscribe(
        [weak, id, scope = request.id](
            catl::core::RequestScopeTerminal terminal) noexcept {
            if (auto self = weak.lock())
            {
                try
                {
                    asio::post(
                        self->strand_, [self, id, scope, terminal]() noexcept {
                            self->on_request_scope_terminal(
                                id, scope, terminal);
                        });
                }
                catch (...)
                {
                }
            }
        });

    timer->async_wait([weak, id](boost::system::error_code const& ec) noexcept {
        if (!ec)
        {
            if (auto self = weak.lock())
                self->on_ledger_demand_deadline(id);
        }
    });

    std::map<LedgerDemandId, LedgerDemandRecord> prepared_rows;
    prepared_rows.emplace(
        id,
        LedgerDemandRecord{
            .id = id,
            .request_scope_id = request.id,
            .request_scope = request_scope,
            .options = options,
            .absolute_deadline = request.absolute_deadline,
            .terminal_subscription = std::move(subscription),
            .deadline_timer = std::move(timer),
            .dial_tokens = std::move(dial_tokens),
            .promotion_tokens = std::move(promotion_tokens),
            .blind_tokens = std::move(blind_tokens)});
    auto row_node = prepared_rows.extract(id);

    std::map<std::pair<std::uint64_t, std::uint32_t>, LedgerDemandId>
        prepared_keys;
    prepared_keys.emplace(key, id);
    auto key_node = prepared_keys.extract(key);

    // Prepared nodes make the two-home commit allocation-free. The ID advances
    // only after both canonical homes exist.
    ledger_demands_.insert(std::move(row_node));
    ledger_demand_keys_.insert(std::move(key_node));
    ++next_ledger_demand_id_;
    next_history_budget_token_id_ = prepared_token_id;

    return LedgerDemandRef{
        peer_set_instance_id_,
        id,
        request.id,
        options.ledger_seq,
        request.absolute_deadline};
}

boost::asio::awaitable<LedgerDemandRef>
PeerSet::co_get_or_create_ledger_demand(
    std::shared_ptr<catl::core::RequestScope> request_scope,
    LedgerDemandOptions options)
{
    if (!strand_.running_in_this_thread())
    {
        auto self = shared_from_this();
        auto owner_executor = catl::core::ContextExecutor(
            strand_, catl::core::retain_current_request());
        co_return co_await asio::co_spawn(
            std::move(owner_executor),
            [self,
             request_scope = std::move(request_scope),
             options]() mutable -> asio::awaitable<LedgerDemandRef> {
                co_return self->get_or_create_ledger_demand_on_strand(
                    request_scope, options);
            },
            asio::use_awaitable);
    }
    co_return get_or_create_ledger_demand_on_strand(request_scope, options);
}

//@@authority transition=prv.T0263 status=provisional scope=function
// bind=catl::peer_client::open_history_acquisition_on_strand@0d710e1547dc8c6c
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0104,prv.S0105
// label=mint_budgetless_history_acquisition_child
HistoryAcquisitionRef
PeerSet::open_history_acquisition_on_strand(
    LedgerDemandRef const& demand,
    HistoryAcquisitionOptions options)
{
    ASSERT_ON_STRAND();
    if (!demand || demand.peer_set_instance_ != peer_set_instance_id_)
    {
        throw PeerClientException(
            Error::BadRequest, "history acquisition has a foreign parent");
    }
    auto const row_it = ledger_demands_.find(demand.id_);
    if (row_it == ledger_demands_.end() ||
        row_it->second.request_scope_id != demand.scope_id_ ||
        row_it->second.options.ledger_seq != demand.ledger_seq_)
    {
        throw PeerClientException(
            Error::BadRequest, "history acquisition has a foreign parent");
    }

    auto& row = row_it->second;
    auto const now = std::chrono::steady_clock::now();
    auto scope = row.request_scope.lock();
    if (row.terminal || now >= row.absolute_deadline || !scope)
        throw PeerClientException(
            now >= row.absolute_deadline ? Error::Timeout : Error::Cancelled,
            "history demand is terminal");
    if (auto const state = scope->snapshot(now); state.terminal)
    {
        terminalize_ledger_demand(
            row.id,
            *state.terminal ==
                    catl::core::RequestScopeTerminal::deadline_elapsed
                ? LedgerDemandTerminal::deadline_elapsed
                : LedgerDemandTerminal::cancelled);
        throw PeerClientException(
            *state.terminal ==
                    catl::core::RequestScopeTerminal::deadline_elapsed
                ? Error::Timeout
                : Error::Cancelled,
            "history demand request is terminal");
    }
    if (!options.fetch)
        throw PeerClientException(
            Error::BadRequest, "history acquisition fetch identity is missing");
    if (row.acquisitions.size() >= options_.max_acquisitions_per_demand ||
        next_history_acquisition_id_ ==
            std::numeric_limits<std::uint64_t>::max())
    {
        throw PeerClientException(
            Error::ResourceExhausted, "history acquisition capacity exhausted");
    }

    AcquisitionId const id{next_history_acquisition_id_};
    HistorySelectionContext context{
        .acquisition = id,
        .fetch = options.fetch,
        .fetch_epoch = options.fetch_epoch,
        .ledger_seq = row.options.ledger_seq,
        .request_kind = options.request_kind,
        .tree_type = options.tree_type,
        .item = std::move(options.item)};

    std::map<AcquisitionId, HistoryAcquisitionRecord> prepared_rows;
    prepared_rows.emplace(
        id,
        HistoryAcquisitionRecord{
            .id = id,
            .demand = row.id,
            .context = context,
            .excluded_endpoints = std::move(options.excluded_endpoints)});
    auto acquisition_node = prepared_rows.extract(id);
    std::set<AcquisitionId> prepared_index;
    prepared_index.insert(id);
    auto index_node = prepared_index.extract(id);

    history_acquisitions_.insert(std::move(acquisition_node));
    row.acquisitions.insert(std::move(index_node));
    ++next_history_acquisition_id_;

    return HistoryAcquisitionRef{
        peer_set_instance_id_,
        row.id,
        row.request_scope_id,
        std::move(context),
        row.absolute_deadline};
}

boost::asio::awaitable<HistoryAcquisitionRef>
PeerSet::co_open_history_acquisition(
    LedgerDemandRef demand,
    HistoryAcquisitionOptions options)
{
    if (!strand_.running_in_this_thread())
    {
        auto self = shared_from_this();
        auto owner_executor = catl::core::ContextExecutor(
            strand_, catl::core::retain_current_request());
        co_return co_await asio::co_spawn(
            std::move(owner_executor),
            [self, demand, options = std::move(options)]() mutable
            -> asio::awaitable<HistoryAcquisitionRef> {
                co_return self->open_history_acquisition_on_strand(
                    demand, std::move(options));
            },
            asio::use_awaitable);
    }
    co_return open_history_acquisition_on_strand(demand, std::move(options));
}

void
//@@authority transition=prv.T0291 status=provisional scope=function bind=catl::peer_client::release_history_acquisition_attachments@96cdb28dd05705c5
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0110,prv.S0111,prv.S0115,prv.S0116,prv.S0117
// label=remove_acquisition_reverse_attachments_and_close_exact_grants
PeerSet::release_history_acquisition_attachments(
    AcquisitionId acquisition) noexcept
{
    ASSERT_ON_STRAND();
    auto row = history_acquisitions_.find(acquisition);
    if (row == history_acquisitions_.end())
        return;
    auto changed = false;
    for (auto const generation_id : row->second.attached_generations)
    {
        auto generation = std::find_if(
            peer_generations_.begin(),
            peer_generations_.end(),
            [generation_id](auto const& current) {
                return current.second.lifecycle.generation() == generation_id;
            });
        if (generation == peer_generations_.end())
            continue;
        auto attached =
            generation->second.history_attachments.find(acquisition);
        if (attached == generation->second.history_attachments.end())
            continue;
        auto fetch = std::move(attached->second.fetch);
        auto grant = attached->second.grant;
        generation->second.history_attachments.erase(attached);
        changed = true;
        if (fetch)
            (void)fetch->acknowledge_work(grant, HistoryWorkReceipt::closed);
    }
    row->second.attached_generations.clear();
    if (changed)
        advance_history_pool_revision();
}

void
//@@authority transition=prv.T0293 status=provisional scope=function bind=catl::peer_client::release_history_acquisition_origins@26b95fd2195edb5e
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0088,prv.S0110,prv.S0111,prv.S0115,prv.S0119,prv.S0120
// label=close_exact_demand_origin_or_request_its_physical_drain
PeerSet::release_history_acquisition_origins(
    AcquisitionId acquisition) noexcept
{
    ASSERT_ON_STRAND();
    auto owner = history_acquisitions_.find(acquisition);
    if (owner == history_acquisitions_.end())
        return;

    std::uint64_t cursor = 0;
    for (;;)
    {
        auto const next = owner->second.retained_generations.upper_bound(cursor);
        if (next == owner->second.retained_generations.end())
            break;
        auto const generation_id = *next;
        cursor = generation_id;
        auto generation = std::find_if(
            peer_generations_.begin(),
            peer_generations_.end(),
            [generation_id](auto const& current) {
                return current.second.lifecycle.generation() == generation_id;
            });
        if (generation == peer_generations_.end() ||
            !generation->second.history_origin ||
            generation->second.history_origin->retainer_control.acquisition !=
                acquisition)
        {
            owner->second.retained_generations.erase(generation_id);
            continue;
        }

        auto& origin = *generation->second.history_origin;
        auto adopted = false;
        for (auto attachment = generation->second.history_attachments.begin();
             attachment != generation->second.history_attachments.end();
             ++attachment)
        {
            auto adopter = history_acquisitions_.find(attachment->first);
            if (attachment->first == acquisition ||
                adopter == history_acquisitions_.end() ||
                adopter->second.terminal || !attachment->second.fetch ||
                attachment->second.fetch != origin.retainer_fetch ||
                !adopter->second.attached_generations.contains(generation_id))
            {
                continue;
            }
            if (!origin.retainer_fetch ||
                !origin.retainer_fetch->acknowledge_work(
                    origin.retainer_grant,
                    HistoryWorkReceipt::adopted,
                    attachment->second.grant))
            {
                continue;
            }

            auto reverse =
                adopter->second.attached_generations.extract(generation_id);
            assert(!reverse.empty());
            owner->second.retained_generations.erase(generation_id);
            auto retained =
                adopter->second.retained_generations.insert(std::move(reverse));
            assert(retained.inserted);
            origin.retainer_control = attachment->second.control;
            origin.retainer_fetch = std::move(attachment->second.fetch);
            origin.retainer_grant = attachment->second.grant;
            generation->second.history_attachments.erase(attachment);
            advance_history_pool_revision();
            adopted = true;
            break;
        }
        if (adopted)
            continue;

        auto const endpoint = generation->first;
        auto queued = targeted_connects_.find(endpoint);
        if (queued != targeted_connects_.end() &&
            queued->second.generation == generation_id &&
            queued->second.history_demand_authorized)
        {
            queued_.erase(endpoint);
            std::erase(pending_connects_, endpoint);
            targeted_connects_.erase(queued);
            abandon_unstarted_generation(endpoint, generation_id);
            continue;
        }

        generation->second.history_origin->close_requested = true;
        begin_generation_close(endpoint, generation_id);
        auto servant = in_flight_.find(endpoint);
        if (servant != in_flight_.end() &&
            servant->second.generation == generation_id)
        {
            servant->second.stop_cancellation_requested = true;
            if (servant->second.session)
            {
                try
                {
                    servant->second.session->disconnect();
                }
                catch (...)
                {
                }
            }
            continue;
        }
        if (generation->second.session)
        {
            (void)start_generation_drain(endpoint, generation_id);
            continue;
        }

        release_generation_history_origin(
            generation->second, HistoryWorkReceipt::closed);
        peer_generations_.erase(generation);
        advance_history_pool_revision();
    }
}

void
//@@authority transition=prv.T0292 status=provisional scope=function bind=catl::peer_client::release_generation_history_attachments@ee2f8e7143b8162e
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0110,prv.S0111,prv.S0115,prv.S0116,prv.S0117
// label=fanout_generation_terminal_and_close_exact_attachment_grants
PeerSet::release_generation_history_attachments(
    PeerGeneration& generation) noexcept
{
    ASSERT_ON_STRAND();
    if (generation.history_attachments.empty())
        return;
    auto const generation_id = generation.lifecycle.generation();
    for (auto& [acquisition_id, attachment] :
         generation.history_attachments)
    {
        auto acquisition = history_acquisitions_.find(acquisition_id);
        if (acquisition != history_acquisitions_.end())
            acquisition->second.attached_generations.erase(generation_id);
        if (attachment.fetch)
        {
            (void)attachment.fetch->acknowledge_work(
                attachment.grant, HistoryWorkReceipt::closed);
        }
    }
    generation.history_attachments.clear();
    advance_history_pool_revision();
}

void
//@@authority transition=prv.T0294 status=provisional scope=function bind=catl::peer_client::release_generation_history_origin@2eac0df594a7a700
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0110,prv.S0111,prv.S0115,prv.S0119,prv.S0120
// label=acknowledge_exact_history_generation_origin_after_release_receipt
PeerSet::release_generation_history_origin(
    PeerGeneration& generation,
    HistoryWorkReceipt receipt) noexcept
{
    ASSERT_ON_STRAND();
    if (!generation.history_origin)
        return;
    if (generation.history_watchdog)
    {
        try
        {
            generation.history_watchdog->cancel();
        }
        catch (...)
        {
        }
        generation.history_watchdog.reset();
    }
    auto origin = std::move(*generation.history_origin);
    generation.history_origin.reset();
    auto acquisition =
        history_acquisitions_.find(origin.retainer_control.acquisition);
    if (acquisition != history_acquisitions_.end())
    {
        acquisition->second.retained_generations.erase(
            generation.lifecycle.generation());
    }
    if (origin.retainer_fetch)
    {
        (void)origin.retainer_fetch->acknowledge_work(
            origin.retainer_grant, receipt);
    }
    advance_history_pool_revision();
}

void
//@@authority transition=prv.T0296 status=provisional scope=function bind=catl::peer_client::on_history_generation_watchdog@23ce312040ed58a7
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0088,prv.S0089,prv.S0115,prv.S0116,prv.S0117,prv.S0118,prv.S0119,prv.S0120,prv.S0121
// label=expire_exact_demand_generation_without_releasing_before_receipt
PeerSet::on_history_generation_watchdog(
    std::string const& endpoint,
    std::uint64_t generation_id) noexcept
{
    ASSERT_ON_STRAND();
    auto generation = peer_generations_.find(endpoint);
    if (generation == peer_generations_.end() ||
        generation->second.lifecycle.generation() != generation_id ||
        !generation->second.history_origin ||
        generation->second.physical_deadline >
            std::chrono::steady_clock::now())
    {
        return;
    }

    release_generation_history_attachments(generation->second);
    generation->second.history_origin->close_requested = true;
    auto queued = targeted_connects_.find(endpoint);
    if (queued != targeted_connects_.end() &&
        queued->second.generation == generation_id &&
        queued->second.history_demand_authorized)
    {
        queued_.erase(endpoint);
        std::erase(pending_connects_, endpoint);
        targeted_connects_.erase(queued);
        abandon_unstarted_generation(endpoint, generation_id);
        return;
    }

    begin_generation_close(endpoint, generation_id);
    auto servant = in_flight_.find(endpoint);
    if (servant != in_flight_.end() &&
        servant->second.generation == generation_id)
    {
        servant->second.stop_cancellation_requested = true;
        if (servant->second.session)
        {
            try
            {
                servant->second.session->disconnect();
            }
            catch (...)
            {
            }
        }
        return;
    }
    if (generation->second.session)
    {
        (void)start_generation_drain(endpoint, generation_id);
        return;
    }
    release_generation_history_origin(
        generation->second, HistoryWorkReceipt::closed);
    peer_generations_.erase(generation);
    advance_history_pool_revision();
}

std::optional<std::pair<std::size_t, std::size_t>>
//@@authority transition=prv.T0297 status=provisional scope=function bind=catl::peer_client::capture_history_redirect_successors@cea6928fe23b0306
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0115,prv.S0116,prv.S0119,prv.S0120,prv.S0122
// label=fanout_bounded_redirect_values_to_exact_generation_owners
PeerSet::capture_history_redirect_successors(
    std::string const& endpoint,
    std::uint64_t generation_id,
    std::uint16_t port,
    std::set<std::string> const& successors) noexcept
{
    ASSERT_ON_STRAND();
    auto generation = peer_generations_.find(endpoint);
    if (generation == peer_generations_.end() ||
        generation->second.lifecycle.generation() != generation_id ||
        !generation->second.history_origin)
    {
        return std::nullopt;
    }

    try
    {
        std::set<std::string> normalized;
        std::size_t filtered = 0;
        for (auto const& successor : successors)
        {
            auto value =
                EndpointTracker::normalize_discovered_host(successor, port);
            if (!value || *value == endpoint ||
                !normalized.insert(*value).second)
            {
                ++filtered;
                continue;
            }
        }

        auto changed = false;
        auto add_to = [&](AcquisitionId id) {
            auto acquisition = history_acquisitions_.find(id);
            if (acquisition == history_acquisitions_.end() ||
                acquisition->second.terminal)
            {
                return;
            }
            for (auto const& successor : normalized)
            {
                if (acquisition->second.redirect_successors.size() >=
                    kMaxHistoryRedirectSuccessors)
                {
                    break;
                }
                changed =
                    acquisition->second.redirect_successors.insert(successor)
                        .second || changed;
            }
        };
        add_to(
            generation->second.history_origin->retainer_control.acquisition);
        for (auto const& [acquisition, _] :
             generation->second.history_attachments)
        {
            add_to(acquisition);
        }
        if (changed)
            advance_history_pool_revision();
        return std::pair{normalized.size(), filtered};
    }
    catch (...)
    {
        return std::pair<std::size_t, std::size_t>{0, successors.size()};
    }
}

void
//@@authority transition=prv.T0266 status=provisional scope=function
// bind=catl::peer_client::finish_history_acquisition_on_strand@cfcaa0ac16e9ba4a
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0105
// label=commit_history_acquisition_terminal_once
PeerSet::finish_history_acquisition_on_strand(
    HistoryAcquisitionRef const& acquisition,
    HistoryAcquisitionTerminal terminal) noexcept
{
    ASSERT_ON_STRAND();
    if (!acquisition || acquisition.peer_set_instance_ != peer_set_instance_id_)
        return;
    auto const it =
        history_acquisitions_.find(acquisition.context_.acquisition);
    if (it == history_acquisitions_.end() ||
        it->second.demand != acquisition.demand_ ||
        it->second.context.fetch != acquisition.context_.fetch ||
        it->second.context.fetch_epoch != acquisition.context_.fetch_epoch)
        return;
    if (!it->second.terminal)
    {
        it->second.terminal = terminal;
        release_history_acquisition_origins(it->second.id);
        release_history_acquisition_attachments(it->second.id);
    }
}

boost::asio::awaitable<void>
PeerSet::co_finish_history_acquisition(
    HistoryAcquisitionRef acquisition,
    HistoryAcquisitionTerminal terminal)
{
    co_await asio::this_coro::reset_cancellation_state(
        asio::disable_cancellation());
    if (!strand_.running_in_this_thread())
    {
        auto self = shared_from_this();
        auto owner_executor = catl::core::ContextExecutor(
            strand_, catl::core::retain_current_request());
        co_await asio::co_spawn(
            std::move(owner_executor),
            [self, acquisition, terminal]() -> asio::awaitable<void> {
                self->finish_history_acquisition_on_strand(
                    acquisition, terminal);
                co_return;
            },
            asio::use_awaitable);
        co_return;
    }
    finish_history_acquisition_on_strand(acquisition, terminal);
}

void
//@@authority transition=prv.T0265 status=provisional scope=function
// bind=catl::peer_client::terminalize_ledger_demand@8d0ae5a02bb6f574
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0103,prv.S0104,prv.S0105
// label=propagate_parent_terminal_to_budgetless_children
PeerSet::terminalize_ledger_demand(
    LedgerDemandId demand,
    LedgerDemandTerminal terminal) noexcept
{
    ASSERT_ON_STRAND();
    auto const it = ledger_demands_.find(demand);
    if (it == ledger_demands_.end() || it->second.terminal)
        return;
    auto& row = it->second;
    row.terminal = terminal;
    row.terminal_subscription.reset();
    if (row.deadline_timer)
    {
        try
        {
            row.deadline_timer->cancel();
        }
        catch (...)
        {
        }
    }

    HistoryAcquisitionTerminal child_terminal =
        HistoryAcquisitionTerminal::fetch_terminal;
    switch (terminal)
    {
        case LedgerDemandTerminal::completed:
            child_terminal = HistoryAcquisitionTerminal::completed;
            break;
        case LedgerDemandTerminal::cancelled:
            child_terminal = HistoryAcquisitionTerminal::cancelled;
            break;
        case LedgerDemandTerminal::deadline_elapsed:
            child_terminal = HistoryAcquisitionTerminal::deadline_elapsed;
            break;
        case LedgerDemandTerminal::budget_exhausted:
            child_terminal = HistoryAcquisitionTerminal::exhausted;
            break;
        case LedgerDemandTerminal::shutdown:
            child_terminal = HistoryAcquisitionTerminal::shutdown;
            break;
    }
    for (auto const id : row.acquisitions)
    {
        auto const acquisition = history_acquisitions_.find(id);
        if (acquisition != history_acquisitions_.end() &&
            !acquisition->second.terminal)
        {
            acquisition->second.terminal = child_terminal;
            release_history_acquisition_origins(id);
            release_history_acquisition_attachments(id);
        }
    }
}

void
PeerSet::on_request_scope_terminal(
    LedgerDemandId demand,
    catl::core::RequestScopeId request_scope,
    catl::core::RequestScopeTerminal terminal) noexcept
{
    ASSERT_ON_STRAND();
    auto const row = ledger_demands_.find(demand);
    if (row == ledger_demands_.end() ||
        row->second.request_scope_id != request_scope)
        return;
    terminalize_ledger_demand(demand, [&] {
        switch (terminal)
        {
            case catl::core::RequestScopeTerminal::completed:
                return LedgerDemandTerminal::completed;
            case catl::core::RequestScopeTerminal::cancelled:
                return LedgerDemandTerminal::cancelled;
            case catl::core::RequestScopeTerminal::deadline_elapsed:
                return LedgerDemandTerminal::deadline_elapsed;
        }
        return LedgerDemandTerminal::cancelled;
    }());
}

void
PeerSet::on_ledger_demand_deadline(LedgerDemandId demand) noexcept
{
    ASSERT_ON_STRAND();
    terminalize_ledger_demand(demand, LedgerDemandTerminal::deadline_elapsed);
}

void
PeerSet::prune_ledger_demand_tombstones() noexcept
{
    ASSERT_ON_STRAND();
    for (auto it = ledger_demands_.begin(); it != ledger_demands_.end();)
    {
        auto const& row = it->second;
        if (!row.terminal || !row.request_scope.expired())
        {
            ++it;
            continue;
        }
        for (auto const acquisition : row.acquisitions)
            history_acquisitions_.erase(acquisition);
        ledger_demand_keys_.erase(
            std::pair{row.request_scope_id.value, row.options.ledger_seq});
        it = ledger_demands_.erase(it);
    }
}

boost::asio::awaitable<std::optional<LedgerDemandSnapshot>>
PeerSet::co_ledger_demand_snapshot(LedgerDemandRef demand)
{
    if (!strand_.running_in_this_thread())
    {
        auto self = shared_from_this();
        auto owner_executor = catl::core::ContextExecutor(
            strand_, catl::core::retain_current_request());
        co_return co_await asio::co_spawn(
            std::move(owner_executor),
            [self,
             demand]() -> asio::awaitable<std::optional<LedgerDemandSnapshot>> {
                co_return co_await self->co_ledger_demand_snapshot(demand);
            },
            asio::use_awaitable);
    }

    if (!demand || demand.peer_set_instance_ != peer_set_instance_id_)
        co_return std::nullopt;

    auto const it = ledger_demands_.find(demand.id_);
    if (it == ledger_demands_.end() ||
        it->second.request_scope_id != demand.scope_id_ ||
        it->second.options.ledger_seq != demand.ledger_seq_)
        co_return std::nullopt;

    auto const& row = it->second;
    auto const remaining = [](auto const& tokens) {
        return static_cast<std::size_t>(
            std::count_if(tokens.begin(), tokens.end(), [](auto const& token) {
                return !token.spent;
            }));
    };
    std::size_t active = 0;
    for (auto const id : row.acquisitions)
    {
        auto const acquisition = history_acquisitions_.find(id);
        if (acquisition != history_acquisitions_.end() &&
            !acquisition->second.terminal)
            ++active;
    }
    co_return LedgerDemandSnapshot{
        .id = row.id,
        .request_scope_id = row.request_scope_id,
        .ledger_seq = row.options.ledger_seq,
        .absolute_deadline = row.absolute_deadline,
        .fallback_at = row.options.fallback_at,
        .terminal = row.terminal,
        .remaining_new_dials = remaining(row.dial_tokens),
        .remaining_promotions = remaining(row.promotion_tokens),
        .remaining_blind_uses = remaining(row.blind_tokens),
        .acquisitions = row.acquisitions.size(),
        .active_acquisitions = active};
}

void
PeerSet::refresh_global_history_tokens(
    std::chrono::steady_clock::time_point now) noexcept
{
    ASSERT_ON_STRAND();
    auto refresh = [this, now](auto& tokens) {
        for (auto& token : tokens)
        {
            if (!token.spent || !token.refill_at || now < *token.refill_at)
                continue;
            if (token.revision == std::numeric_limits<std::uint64_t>::max())
            {
                // Exhaustion is absorbing. Reusing the scalar with a wrapped
                // revision would let a stale global grant address this row.
                token.refill_at.reset();
                continue;
            }
            token.spent = false;
            token.refill_at.reset();
            ++token.revision;
            advance_history_pool_revision();
        }
    };
    refresh(global_history_dial_tokens_);
    refresh(global_history_blind_tokens_);
}

HistorySearchSnapshot
PeerSet::history_search_snapshot_on_strand(
    HistoryAcquisitionRef const& acquisition,
    std::optional<HistoryInterestRef> const& interest)
{
    ASSERT_ON_STRAND();
    if (!acquisition ||
        acquisition.peer_set_instance_ != peer_set_instance_id_ ||
        history_pool_revision_ == 0)
    {
        throw PeerClientException(
            Error::BadRequest, "history search acquisition is foreign");
    }
    auto acquisition_it =
        history_acquisitions_.find(acquisition.context_.acquisition);
    if (acquisition_it == history_acquisitions_.end() ||
        acquisition_it->second.demand != acquisition.demand_ ||
        acquisition_it->second.context.fetch != acquisition.context_.fetch ||
        acquisition_it->second.context.fetch_epoch !=
            acquisition.context_.fetch_epoch)
    {
        throw PeerClientException(
            Error::BadRequest, "history search acquisition is stale");
    }
    auto demand_it = ledger_demands_.find(acquisition.demand_);
    if (demand_it == ledger_demands_.end() ||
        demand_it->second.request_scope_id != acquisition.scope_id_ ||
        demand_it->second.options.ledger_seq !=
            acquisition.context_.ledger_seq ||
        demand_it->second.absolute_deadline != acquisition.absolute_deadline_)
    {
        throw PeerClientException(
            Error::BadRequest, "history search parent is stale");
    }
    auto& demand = demand_it->second;
    if (interest &&
        (!*interest ||
         interest->participant_owner_instance() != peer_set_instance_id_ ||
         interest->demand() != demand.id ||
         interest->acquisition() != acquisition.context_.acquisition ||
         interest->fetch() != acquisition.context_.fetch ||
         interest->fetch_epoch() != acquisition.context_.fetch_epoch ||
         interest->request_scope() != demand.request_scope_id ||
         interest->deadline() != demand.absolute_deadline))
    {
        throw PeerClientException(
            Error::BadRequest, "history search interest is foreign");
    }

    auto control = HistoryControlRevision{
        .peer_set_instance = peer_set_instance_id_,
        .pool_revision = history_pool_revision_,
        .demand = demand.id,
        .acquisition = acquisition.context_.acquisition,
        .fetch = acquisition.context_.fetch,
        .fetch_epoch = acquisition.context_.fetch_epoch,
        .interest = interest ? interest->interest() : HistoryInterestId{},
        .participant_revision = interest ? interest->participant_revision() : 0,
        .request_scope = demand.request_scope_id};
    return project_history_search_snapshot(
        acquisition_it->second, demand, std::move(control));
}

HistorySearchSnapshot
//@@authority transition=prv.T0288 status=provisional scope=function bind=catl::peer_client::project_history_search_snapshot@fce59ab3514cad01
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0008,prv.S0073,prv.S0081,prv.S0088,prv.S0104,prv.S0105,prv.S0114,prv.S0115,prv.S0116,prv.S0117,prv.S0118
// label=project_distinct_evidence_lanes_tokens_and_physical_census
PeerSet::project_history_search_snapshot(
    HistoryAcquisitionRecord const& acquisition,
    LedgerDemandRecord& demand,
    HistoryControlRevision control)
{
    ASSERT_ON_STRAND();
    auto const now = std::chrono::steady_clock::now();
    refresh_global_history_tokens(now);
    control.pool_revision = history_pool_revision_;

    HistorySearchSnapshot snapshot;
    snapshot.control = std::move(control);
    snapshot.absolute_deadline = demand.absolute_deadline;
    snapshot.fallback_at = demand.options.fallback_at;
    snapshot.cancelled =
        demand.terminal.has_value() || acquisition.terminal.has_value();
    snapshot.initial_parent_dial_budget = demand.dial_tokens.size();
    snapshot.crawl_only_creations = demand.crawl_only_creations;
    snapshot.source_attempts = demand.source_attempts;
    snapshot.physical = physical_census_on_strand();
    snapshot.excluded_endpoints = acquisition.excluded_endpoints;
    if (!snapshot.physical.can_create())
    {
        auto earliest = std::chrono::steady_clock::time_point::max();
        std::string endpoint;
        std::uint64_t generation_id = 0;
        for (auto const& [candidate_endpoint, generation] : peer_generations_)
        {
            if (!generation.history_origin ||
                generation.physical_deadline <= now ||
                generation.physical_deadline > demand.absolute_deadline ||
                generation.physical_deadline >= earliest)
            {
                continue;
            }
            earliest = generation.physical_deadline;
            endpoint = candidate_endpoint;
            generation_id = generation.lifecycle.generation();
        }
        if (generation_id != 0)
        {
            snapshot.wakes.push_back(HistoryPlannedWake{
                .kind = HistoryWakeSourceKind::physical_watchdog,
                .at = earliest,
                .endpoint = std::move(endpoint),
                .generation = generation_id});
        }
    }

    auto first_live = [](auto const& tokens) -> auto const* {
        auto const found =
            std::find_if(tokens.begin(), tokens.end(), [](auto const& token) {
                return !token.spent;
            });
        return found == tokens.end() ? nullptr : &*found;
    };
    auto grant =
        [this](auto const* token) -> std::optional<HistoryBudgetGrantRef> {
        if (!token || token->id == 0 || token->revision == 0)
            return std::nullopt;
        return HistoryBudgetGrantRef{
            .owner_instance = peer_set_instance_id_,
            .token = token->id,
            .revision = token->revision};
    };
    snapshot.remaining_parent_dials = static_cast<std::size_t>(std::count_if(
        demand.dial_tokens.begin(),
        demand.dial_tokens.end(),
        [](auto const& token) { return !token.spent; }));
    snapshot.parent_dial_grant = grant(first_live(demand.dial_tokens));
    snapshot.parent_blind_grant = grant(first_live(demand.blind_tokens));
    snapshot.global_dial_grant = grant(first_live(global_history_dial_tokens_));
    snapshot.global_blind_grant =
        grant(first_live(global_history_blind_tokens_));

    auto add_global_wake = [&snapshot](auto const& tokens) {
        auto const found = std::min_element(
            tokens.begin(),
            tokens.end(),
            [](auto const& left, auto const& right) {
                if (!left.refill_at)
                    return false;
                if (!right.refill_at)
                    return true;
                return *left.refill_at < *right.refill_at;
            });
        if (found != tokens.end() && found->refill_at)
        {
            snapshot.wakes.push_back(HistoryPlannedWake{
                .kind = HistoryWakeSourceKind::global_token_refill,
                .at = *found->refill_at,
                .token = found->id});
        }
    };
    if (!snapshot.global_dial_grant)
        add_global_wake(global_history_dial_tokens_);
    if (!snapshot.global_blind_grant)
        add_global_wake(global_history_blind_tokens_);

    struct CandidateRow
    {
        HistorySearchCandidate value;
        bool crawl_positive = false;
    };
    std::map<std::string, CandidateRow> candidates;
    auto strengthen = [](HistorySourceLane& current, HistorySourceLane source) {
        if (static_cast<std::uint8_t>(source) <
            static_cast<std::uint8_t>(current))
        {
            current = source;
        }
    };
    auto ensure = [&candidates, &strengthen](
                      std::string const& endpoint,
                      HistorySourceLane source) -> CandidateRow& {
        auto [it, inserted] = candidates.try_emplace(endpoint);
        if (inserted)
        {
            it->second.value.target.endpoint = endpoint;
            it->second.value.source = source;
            it->second.value.eligible = true;
        }
        else
            strengthen(it->second.value.source, source);
        return it->second;
    };

    for (auto const& endpoint : configured_endpoints_)
        ensure(endpoint, HistorySourceLane::configured);

    for (auto const& endpoint : acquisition.redirect_successors)
        ensure(endpoint, HistorySourceLane::independently_discovered);

    for (auto const& [endpoint, stats] : endpoint_stats_)
    {
        auto source = HistorySourceLane::crawl_only;
        auto independent = false;
        if (configured_endpoints_.contains(endpoint))
            source = HistorySourceLane::configured;
        else if (stats.last_success_at > 0)
            source = HistorySourceLane::persistent_success;
        else if (stats.seen_endpoints || stats.seen_redirect)
        {
            source = HistorySourceLane::independently_discovered;
            independent = true;
        }
        else if (!stats.seen_crawl)
        {
            source = HistorySourceLane::independently_discovered;
            independent = true;
        }
        if (source != HistorySourceLane::crawl_only || independent)
            ensure(endpoint, source);
    }

    for (auto const& [_, batch] : crawl_observation_batches_)
    {
        for (auto const& observation : batch.observations)
        {
            auto const covers = std::any_of(
                observation.intervals.begin(),
                observation.intervals.end(),
                [&acquisition](CrawlLedgerRange const& interval) {
                    return interval.first_seq <=
                        acquisition.context.ledger_seq &&
                        acquisition.context.ledger_seq <= interval.last_seq;
                });
            if (!covers)
                continue;
            auto& candidate = ensure(
                observation.subject_endpoint, HistorySourceLane::crawl_only);
            candidate.crawl_positive = true;
        }
    }

    for (auto const& [endpoint, connection] : connections_)
    {
        auto& candidate = ensure(endpoint, HistorySourceLane::direct_verified);
        candidate.value.target.generation = connection.generation;
        candidate.value.disposition = HistoryGenerationDisposition::admitted;
        candidate.value.blind_eligible = true;
        if (auto const* status =
                ready_status_for(endpoint, connection.generation))
        {
            candidate.value.advertises_ledger =
                status->advertised_range.contains(
                    acquisition.context.ledger_seq);
        }
    }

    auto project_attachment =
        [&snapshot, &acquisition, &demand, &now, this](
            HistorySearchCandidate& candidate,
            std::uint64_t generation_id) {
            auto generation = peer_generations_.find(candidate.target.endpoint);
            if (generation == peer_generations_.end() || generation_id == 0 ||
                generation->second.lifecycle.generation() != generation_id ||
                generation->second.physical_deadline <= now)
            {
                candidate.eligible = false;
                return;
            }
            candidate.eligible = true;
            candidate.attached =
                acquisition.attached_generations.contains(generation_id) ||
                (generation->second.history_origin &&
                 generation->second.history_origin->retainer_control.acquisition ==
                     acquisition.id);
            candidate.attachable = !candidate.attached;
            if (candidate.attached &&
                snapshot.wakes.size() < kMaxHistorySearchWakes)
            {
                snapshot.wakes.push_back(HistoryPlannedWake{
                    .kind =
                        HistoryWakeSourceKind::attached_generation_deadline,
                    .at = std::min(
                        generation->second.physical_deadline,
                        demand.absolute_deadline),
                    .endpoint = candidate.target.endpoint,
                    .generation = generation_id});
            }
        };

    for (auto const& [endpoint, servant] : in_flight_)
    {
        auto& candidate =
            ensure(endpoint, HistorySourceLane::independently_discovered);
        if (servant.generation != 0)
        {
            candidate.value.target.generation = servant.generation;
            candidate.value.disposition =
                HistoryGenerationDisposition::queued_or_connecting;
            project_attachment(candidate.value, servant.generation);
        }
        else
            candidate.value.eligible = false;
    }
    for (auto const& [endpoint, queued] : targeted_connects_)
    {
        auto& candidate =
            ensure(endpoint, HistorySourceLane::independently_discovered);
        candidate.value.target.generation = queued.generation;
        candidate.value.disposition =
            HistoryGenerationDisposition::queued_or_connecting;
        project_attachment(candidate.value, queued.generation);
    }
    for (auto const& [endpoint, generation] : peer_generations_)
    {
        if (generation.lifecycle.lifecycle() != PeerLifecycle::ready ||
            connections_.contains(endpoint) ||
            draining_generations_.contains(
                generation.lifecycle.generation()))
        {
            continue;
        }
        auto& candidate =
            ensure(endpoint, HistorySourceLane::direct_verified);
        candidate.value.target.generation =
            generation.lifecycle.generation();
        candidate.value.disposition =
            HistoryGenerationDisposition::ready_transient;
        candidate.value.blind_eligible = true;
        if (generation.lifecycle.status())
        {
            candidate.value.advertises_ledger =
                generation.lifecycle.status()->advertised_range.contains(
                    acquisition.context.ledger_seq);
        }
        project_attachment(
            candidate.value, generation.lifecycle.generation());
    }
    for (auto const& [generation, draining] : draining_generations_)
    {
        auto& candidate =
            ensure(draining.endpoint, HistorySourceLane::direct_verified);
        candidate.value.target.generation = generation;
        candidate.value.disposition = HistoryGenerationDisposition::draining;
        candidate.value.eligible = false;
    }

    std::uint64_t fair_ticket = 1;
    std::array<std::vector<HistorySearchCandidate>, kHistorySourceLaneCount>
        lanes;
    auto const base_lane_cap =
        kMaxHistorySearchCandidates / kHistorySourceLaneCount;
    auto const extra_lanes =
        kMaxHistorySearchCandidates % kHistorySourceLaneCount;
    for (auto& [endpoint, row] : candidates)
    {
        if (row.value.source == HistorySourceLane::crawl_only &&
            !row.crawl_positive)
        {
            continue;
        }
        if (auto retry = retry_gates_.find(endpoint);
            retry != retry_gates_.end())
        {
            row.value.retry_not_before =
                retry->second.failed_at + retry_backoff_for(endpoint);
        }
        row.value.fair_ticket = fair_ticket++;
        if (row.value.disposition == HistoryGenerationDisposition::absent)
        {
            row.value.target.pool = "history-transient";
            if (next_connection_generation_ ==
                std::numeric_limits<std::uint64_t>::max())
            {
                row.value.eligible = false;
            }
            else
                row.value.target.generation = next_connection_generation_ + 1;
        }
        else
        {
            auto const generation = peer_generations_.find(endpoint);
            row.value.target.pool = generation != peer_generations_.end() &&
                    generation->second.history_origin
                ? "history-transient"
                : "peer-generation";
        }
        auto const lane = static_cast<std::size_t>(row.value.source);
        auto const lane_cap = base_lane_cap + (lane < extra_lanes ? 1 : 0);
        if (lane < lanes.size() && lanes[lane].size() < lane_cap)
            lanes[lane].push_back(std::move(row.value));
    }
    snapshot.candidates.reserve(kMaxHistorySearchCandidates);
    for (auto& lane : lanes)
    {
        std::move(
            lane.begin(), lane.end(), std::back_inserter(snapshot.candidates));
    }
    return snapshot;
}

boost::asio::awaitable<HistorySearchSnapshot>
PeerSet::co_history_search_snapshot(
    HistoryAcquisitionRef acquisition,
    std::optional<HistoryInterestRef> interest)
{
    if (!strand_.running_in_this_thread())
    {
        auto self = shared_from_this();
        auto owner_executor = catl::core::ContextExecutor(
            strand_, catl::core::retain_current_request());
        co_return co_await asio::co_spawn(
            std::move(owner_executor),
            [self,
             acquisition,
             interest]() -> asio::awaitable<HistorySearchSnapshot> {
                co_return self->history_search_snapshot_on_strand(
                    acquisition, interest);
            },
            asio::use_awaitable);
    }
    co_return history_search_snapshot_on_strand(acquisition, interest);
}

HistorySearchExecution
//@@authority transition=prv.T0289 status=provisional scope=function bind=catl::peer_client::execute_history_grant_on_strand@53fcbb8f477966f8
// owner=unresolved owner_hypothesis=peer_set commit=participant_then_request_mutex
// touches=prv.S0088,prv.S0090,prv.S0091,prv.S0094,prv.S0103,prv.S0104,prv.S0105,prv.S0110,prv.S0111,prv.S0115,prv.S0116,prv.S0117,prv.S0118,prv.S0119,prv.S0120
// label=replan_and_atomically_spend_ready_use_attachment_or_generation
PeerSet::execute_history_grant_on_strand(
    std::shared_ptr<SharedHistoryFetch> const& fetch,
    HistoryAuthorityGrantRef const& grant,
    PeerUseKind kind)
{
    ASSERT_ON_STRAND();
    HistorySearchExecution result;
    if (!fetch || !grant || history_pool_revision_ == 0 ||
        grant.participant_owner_instance() != peer_set_instance_id_)
    {
        return result;
    }

    auto acquisition = history_acquisitions_.find(grant.acquisition());
    if (acquisition == history_acquisitions_.end() ||
        acquisition->second.demand != grant.demand() ||
        acquisition->second.context.fetch != grant.fetch() ||
        acquisition->second.context.fetch_epoch != grant.fetch_epoch())
    {
        return result;
    }
    auto demand = ledger_demands_.find(grant.demand());
    if (demand == ledger_demands_.end() ||
        demand->second.request_scope_id != grant.request_scope() ||
        demand->second.absolute_deadline != grant.deadline() ||
        demand->second.options.ledger_seq !=
            acquisition->second.context.ledger_seq)
    {
        return result;
    }

    auto snapshot = project_history_search_snapshot(
        acquisition->second,
        demand->second,
        HistoryControlRevision{
            .peer_set_instance = peer_set_instance_id_,
            .pool_revision = history_pool_revision_,
            .demand = demand->second.id,
            .acquisition = acquisition->second.id,
            .fetch = acquisition->second.context.fetch,
            .fetch_epoch = acquisition->second.context.fetch_epoch,
            .interest = grant.interest(),
            .participant_revision = grant.participant_revision(),
            .request_scope = demand->second.request_scope_id});
    auto plan = plan_history_acquisition(
        snapshot, grant, std::chrono::steady_clock::now());
    result.plan = plan;

    auto const* known = std::get_if<HistorySubmitKnownUsePlan>(&plan);
    auto const* blind = std::get_if<HistorySubmitBlindUsePlan>(&plan);
    auto const* attach = std::get_if<HistoryAttachPlan>(&plan);
    auto const* create = std::get_if<HistoryCreateGenerationPlan>(&plan);
    if (!known && !blind && !attach && !create)
        return result;
    auto const& target = known ? known->target :
        blind                     ? blind->target :
        attach                    ? attach->target :
                                    create->target;
    std::unique_ptr<PreparedPeerUse> prepared;
    if (known || blind)
    {
        auto connection = connections_.find(target.endpoint);
        auto generation_row = peer_generations_.find(target.endpoint);
        if (generation_row == peer_generations_.end() ||
            !generation_row->second.session ||
            generation_row->second.lifecycle.generation() !=
                target.generation ||
            !generation_row->second.lifecycle.ready() ||
            generation_row->second.physical_deadline <=
                std::chrono::steady_clock::now())
        {
            return result;
        }
        auto const admitted = connection != connections_.end() &&
            connection->second.session == generation_row->second.session &&
            connection->second.generation == target.generation;
        auto const exact_transient_owner = !admitted &&
            ((generation_row->second.history_origin &&
              generation_row->second.history_origin->retainer_control.acquisition ==
                  acquisition->second.id &&
              !generation_row->second.history_origin->close_requested) ||
             acquisition->second.attached_generations.contains(
                 target.generation));
        if (!admitted && !exact_transient_owner)
            return result;
        auto generation = admitted
            ? make_generation_ref(connection->second.session)
            : PeerGenerationRef{
                  peer_set_instance_id_,
                  target.endpoint,
                  target.generation,
                  generation_row->second.lifecycle.last_event_sequence(),
                  *generation_row->second.lifecycle.status()};
        if (!generation || generation.generation() != target.generation)
            return result;
        prepared = prepare_peer_use(
            generation,
            PeerUseRequest{
                .kind = kind,
                .ledger_seq = demand->second.options.ledger_seq,
                .absolute_deadline = demand->second.absolute_deadline},
            known != nullptr,
            exact_transient_owner);
        if (!prepared)
            return result;
    }

    using AttachmentMap =
        std::map<AcquisitionId, HistoryGenerationAttachment>;
    AttachmentMap::node_type attachment_node;
    std::set<std::uint64_t>::node_type reverse_attachment_node;
    PeerGeneration* attachment_generation = nullptr;
    if (attach)
    {
        auto generation = peer_generations_.find(target.endpoint);
        if (generation == peer_generations_.end() ||
            generation->second.lifecycle.generation() != target.generation ||
            generation->second.lifecycle.terminal() ||
            generation->second.lifecycle.lifecycle() ==
                PeerLifecycle::closing ||
            generation->second.physical_deadline <=
                std::chrono::steady_clock::now() ||
            generation->second.history_attachments.contains(
                acquisition->second.id) ||
            acquisition->second.attached_generations.contains(
                target.generation) ||
            generation->second.history_attachments.size() >=
                options_.max_history_attachments_per_generation)
        {
            return result;
        }
        AttachmentMap prepared_attachments;
        prepared_attachments.emplace(
            acquisition->second.id,
            HistoryGenerationAttachment{
                .control = attach->control,
                .fetch = fetch,
                .grant = grant,
                .deadline = std::min(
                    generation->second.physical_deadline,
                    demand->second.absolute_deadline)});
        attachment_node =
            prepared_attachments.extract(acquisition->second.id);
        std::set<std::uint64_t> prepared_reverse;
        prepared_reverse.insert(target.generation);
        reverse_attachment_node =
            prepared_reverse.extract(target.generation);
        attachment_generation = &generation->second;
    }

    using GenerationMap = std::map<std::string, PeerGeneration>;
    using QueuedAuthorityMap =
        std::map<std::string, QueuedConnectAuthority>;
    GenerationMap::node_type created_generation_node;
    QueuedAuthorityMap::node_type created_authority_node;
    std::set<std::string>::node_type created_queue_node;
    std::set<std::uint64_t>::node_type created_origin_reverse_node;
    std::string created_pending_endpoint;
    std::uint64_t created_generation_id = 0;
    auto created_physical_deadline =
        std::chrono::steady_clock::time_point{};
    if (create)
    {
        auto normalized =
            EndpointTracker::normalize_endpoint_identity(target.endpoint);
        auto const current_now = std::chrono::steady_clock::now();
        if (!normalized || *normalized != target.endpoint ||
            target.generation == 0 || target.use != 0 ||
            target.pool != "history-transient" ||
            connections_.contains(target.endpoint) ||
            in_flight_.contains(target.endpoint) ||
            queued_.contains(target.endpoint) ||
            targeted_connects_.contains(target.endpoint) ||
            !physical_census_on_strand().can_create() ||
            pending_connects_.size() >= kMaxTrackedEndpoints ||
            next_connection_generation_ ==
                std::numeric_limits<std::uint64_t>::max())
        {
            return result;
        }
        if (target.generation != next_connection_generation_ + 1)
            return result;
        if (auto existing = peer_generations_.find(target.endpoint);
            existing != peer_generations_.end() &&
            !existing->second.lifecycle.terminal())
        {
            return result;
        }
        if (auto retry = retry_gates_.find(target.endpoint);
            retry != retry_gates_.end() &&
            current_now <
                retry->second.failed_at + retry_backoff_for(target.endpoint))
        {
            return result;
        }
        created_physical_deadline = std::min(
            demand->second.absolute_deadline,
            current_now + options_.history_connect_timeout);
        if (created_physical_deadline <= current_now)
            return result;
        created_generation_id = target.generation;

        GenerationMap prepared_generations;
        auto [prepared_generation, generation_inserted] =
            prepared_generations.emplace(
                target.endpoint,
                PeerGeneration{
                    target.endpoint,
                    created_generation_id,
                    created_physical_deadline});
        assert(generation_inserted);
        auto watchdog = std::make_shared<asio::steady_timer>(
            strand_, created_physical_deadline);
        auto weak = weak_from_this();
        watchdog->async_wait(
            [weak,
             endpoint = target.endpoint,
             generation = created_generation_id](
                boost::system::error_code const& ec) noexcept {
                if (ec == asio::error::operation_aborted)
                    return;
                if (auto owner = weak.lock())
                {
                    owner->on_history_generation_watchdog(
                        endpoint, generation);
                }
            });
        prepared_generation->second.history_origin = HistoryGenerationOrigin{
            .creation_control = create->control,
            .source = create->source,
            .retainer_control = create->control,
            .retainer_fetch = fetch,
            .retainer_grant = grant};
        prepared_generation->second.history_watchdog = std::move(watchdog);
        created_generation_node =
            prepared_generations.extract(prepared_generation);

        QueuedAuthorityMap prepared_authorities;
        prepared_authorities.emplace(
            target.endpoint,
            QueuedConnectAuthority{
                .generation = created_generation_id,
                .physical_deadline = created_physical_deadline,
                .history_demand_authorized = true});
        created_authority_node =
            prepared_authorities.extract(target.endpoint);
        std::set<std::string> prepared_queue;
        prepared_queue.insert(target.endpoint);
        created_queue_node = prepared_queue.extract(target.endpoint);
        std::set<std::uint64_t> prepared_reverse;
        prepared_reverse.insert(created_generation_id);
        created_origin_reverse_node =
            prepared_reverse.extract(created_generation_id);
        created_pending_endpoint = target.endpoint;
        pending_connects_.reserve(pending_connects_.size() + 1);
    }

    auto find_token = [this](
                          auto& tokens,
                          HistoryBudgetGrantRef const& ref)
        -> LedgerDemandRecord::BudgetToken* {
        if (!ref || ref.owner_instance != peer_set_instance_id_)
            return nullptr;
        auto token = std::find_if(
            tokens.begin(), tokens.end(), [&ref](auto const& current) {
                return current.id == ref.token &&
                    current.revision == ref.revision && !current.spent;
            });
        return token == tokens.end() ? nullptr : &*token;
    };
    LedgerDemandRecord::BudgetToken* parent_blind = nullptr;
    LedgerDemandRecord::BudgetToken* global_blind = nullptr;
    LedgerDemandRecord::BudgetToken* parent_dial = nullptr;
    LedgerDemandRecord::BudgetToken* global_dial = nullptr;
    if (blind)
    {
        //@@mutation-site blind-history-use-requires-parent-and-global-debit
        parent_blind =
            find_token(demand->second.blind_tokens, blind->parent_blind_grant);
        global_blind = find_token(
            global_history_blind_tokens_, blind->global_blind_grant);
        if (!parent_blind || !global_blind)
            return result;
        //@@mutation-end blind-history-use-requires-parent-and-global-debit
    }
    if (create)
    {
        //@@mutation-site history-generation-requires-parent-and-global-debit
        parent_dial =
            find_token(demand->second.dial_tokens, create->parent_dial_grant);
        global_dial = find_token(
            global_history_dial_tokens_, create->global_dial_grant);
        if (!parent_dial || !global_dial)
            return result;
        //@@mutation-end history-generation-requires-parent-and-global-debit
    }

    struct OwnerCommitContext
    {
        PeerSet* owner = nullptr;
        PreparedPeerUse* prepared = nullptr;
        PeerGeneration* attachment_generation = nullptr;
        HistoryAcquisitionRecord* attachment_acquisition = nullptr;
        AttachmentMap::node_type attachment_node;
        std::set<std::uint64_t>::node_type reverse_attachment_node;
        GenerationMap::node_type created_generation_node;
        QueuedAuthorityMap::node_type created_authority_node;
        std::set<std::string>::node_type created_queue_node;
        std::set<std::uint64_t>::node_type created_origin_reverse_node;
        HistoryAcquisitionRecord* created_origin_acquisition = nullptr;
        LedgerDemandRecord* created_origin_demand = nullptr;
        std::string created_pending_endpoint;
        std::uint64_t created_generation_id = 0;
        HistorySourceLane created_source = HistorySourceLane::crawl_only;
        LedgerDemandRecord::BudgetToken* parent_blind = nullptr;
        LedgerDemandRecord::BudgetToken* global_blind = nullptr;
        LedgerDemandRecord::BudgetToken* parent_dial = nullptr;
        LedgerDemandRecord::BudgetToken* global_dial = nullptr;
        std::chrono::steady_clock::time_point refill_at{};
        PeerUse peer_use;
    } context{
        .owner = this,
        .prepared = prepared.get(),
        .attachment_generation = attachment_generation,
        .attachment_acquisition =
            attach ? &acquisition->second : nullptr,
        .attachment_node = std::move(attachment_node),
        .reverse_attachment_node = std::move(reverse_attachment_node),
        .created_generation_node = std::move(created_generation_node),
        .created_authority_node = std::move(created_authority_node),
        .created_queue_node = std::move(created_queue_node),
        .created_origin_reverse_node =
            std::move(created_origin_reverse_node),
        .created_origin_acquisition =
            create ? &acquisition->second : nullptr,
        .created_origin_demand = create ? &demand->second : nullptr,
        .created_pending_endpoint = std::move(created_pending_endpoint),
        .created_generation_id = created_generation_id,
        .created_source =
            create ? create->source : HistorySourceLane::crawl_only,
        .parent_blind = parent_blind,
        .global_blind = global_blind,
        .parent_dial = parent_dial,
        .global_dial = global_dial,
        .refill_at = std::chrono::steady_clock::now() +
            options_.global_history_token_refill};
    auto const owner_commit = [](void* opaque,
                                 catl::core::RequestCommitId) noexcept {
        auto& commit = *static_cast<OwnerCommitContext*>(opaque);
        if (commit.parent_blind)
            commit.parent_blind->spent = true;
        if (commit.global_blind)
        {
            commit.global_blind->spent = true;
            commit.global_blind->refill_at = commit.refill_at;
        }
        if (commit.parent_dial)
            commit.parent_dial->spent = true;
        if (commit.global_dial)
        {
            commit.global_dial->spent = true;
            commit.global_dial->refill_at = commit.refill_at;
        }
        if (commit.created_generation_id != 0)
        {
            //@@mutation-site history-generation-queue-origin-and-debits-share-owner-flip
            //@@authority transition=prv.T0295 status=provisional scope=statement bind=declaration@b97cb1639915ef03
            // owner=unresolved owner_hypothesis=peer_set commit=participant_then_request_mutex
            // touches=prv.S0088,prv.S0103,prv.S0104,prv.S0105,prv.S0110,prv.S0111,prv.S0115,prv.S0118,prv.S0119,prv.S0120
            // label=atomically_debit_exact_dials_and_install_demand_origin_generation
            auto generation = commit.owner->peer_generations_.insert(
                std::move(commit.created_generation_node)); //@@authority-target ids=prv.T0295
            auto authority = commit.owner->targeted_connects_.insert(
                std::move(commit.created_authority_node));
            auto queued = commit.owner->queued_.insert(
                std::move(commit.created_queue_node));
            auto reverse =
                commit.created_origin_acquisition->retained_generations.insert(
                    std::move(commit.created_origin_reverse_node));
            assert(generation.inserted);
            assert(authority.inserted);
            assert(queued.inserted);
            assert(reverse.inserted);
            commit.owner->pending_connects_.push_back(
                std::move(commit.created_pending_endpoint));
            commit.owner->next_connection_generation_ =
                commit.created_generation_id;
            auto const lane = static_cast<std::size_t>(commit.created_source);
            if (lane < commit.created_origin_demand->source_attempts.size())
            {
                auto& attempts =
                    commit.created_origin_demand->source_attempts[lane];
                if (attempts != std::numeric_limits<std::size_t>::max())
                    ++attempts;
            }
            if (commit.created_source == HistorySourceLane::crawl_only)
            {
                auto& crawl =
                    commit.created_origin_demand->crawl_only_creations;
                if (crawl != std::numeric_limits<std::size_t>::max())
                    ++crawl;
            }
            //@@mutation-end history-generation-queue-origin-and-debits-share-owner-flip
        }
        else if (commit.attachment_generation)
        {
            //@@mutation-site history-attachment-has-forward-and-reverse-owner
            auto forward =
                commit.attachment_generation->history_attachments.insert(
                    std::move(commit.attachment_node));
            auto reverse =
                commit.attachment_acquisition->attached_generations.insert(
                    std::move(commit.reverse_attachment_node));
            assert(forward.inserted);
            assert(reverse.inserted);
            //@@mutation-end history-attachment-has-forward-and-reverse-owner
        }
        else
        {
            commit.peer_use =
                commit.owner->commit_prepared_peer_use(*commit.prepared);
            assert(commit.peer_use);
        }
        commit.owner->advance_history_pool_revision();
    };
    auto spend = fetch->try_spend_with_owner_commit(
        grant,
        known  ? HistoryAuthorityAction::submit_known_use :
        blind  ? HistoryAuthorityAction::submit_blind_use :
        attach ? HistoryAuthorityAction::attach_or_adopt :
                 HistoryAuthorityAction::create_generation,
        target,
        std::chrono::steady_clock::now(),
        &context,
        owner_commit);
    if (!spend)
        return result;
    result.spend = *spend;
    if (create)
    {
        result.created_generation = HistoryAuthorityTarget{
            .endpoint = target.endpoint,
            .generation = created_generation_id,
            .pool = "history-transient"};
        sort_pending_connects();
        pump_connects();
    }
    result.peer_use = std::move(context.peer_use);
    return result;
}

boost::asio::awaitable<HistorySearchExecution>
PeerSet::co_execute_history_grant(
    std::shared_ptr<SharedHistoryFetch> fetch,
    HistoryAuthorityGrantRef grant,
    PeerUseKind kind)
{
    if (!strand_.running_in_this_thread())
    {
        auto self = shared_from_this();
        auto owner_executor = catl::core::ContextExecutor(
            strand_, catl::core::retain_current_request());
        co_return co_await asio::co_spawn(
            std::move(owner_executor),
            [self,
             fetch = std::move(fetch),
             grant,
             kind]() -> asio::awaitable<HistorySearchExecution> {
                co_return self->execute_history_grant_on_strand(
                    fetch, grant, kind);
            },
            asio::use_awaitable);
    }
    co_return execute_history_grant_on_strand(fetch, grant, kind);
}

PeerUse
PeerSet::make_peer_use(
    PeerGenerationRef const& generation,
    PeerUseRequest const& request)
{
    auto prepared = prepare_peer_use(generation, request, true);
    return prepared ? commit_prepared_peer_use(*prepared) : PeerUse{};
}

PeerSet::PreparedPeerUse::~PreparedPeerUse() noexcept
{
    if (committed)
        return;
    if (control)
        control->finish = {};
    if (deadline)
    {
        try
        {
            deadline->cancel();
        }
        catch (...)
        {
        }
    }
}

std::unique_ptr<PeerSet::PreparedPeerUse>
PeerSet::prepare_peer_use(
    PeerGenerationRef const& generation,
    PeerUseRequest const& request,
    bool require_coverage,
    bool allow_ready_transient) noexcept
{
    ASSERT_ON_STRAND();
    auto const now = std::chrono::steady_clock::now();
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire) ||
        !generation || generation.peer_set_instance_ != peer_set_instance_id_ ||
        peer_set_instance_id_ == 0 ||
        request.absolute_deadline == std::chrono::steady_clock::time_point{} ||
        request.absolute_deadline <= now ||
        next_peer_use_id_ == std::numeric_limits<std::uint64_t>::max())
    {
        return {};
    }

    auto const generation_row = peer_generations_.find(generation.endpoint_);
    auto const connection = connections_.find(generation.endpoint_);
    auto const admitted = connection != connections_.end() &&
        connection->second.session &&
        connection->second.generation == generation.generation_;
    auto const transient = allow_ready_transient &&
        generation_row != peer_generations_.end() &&
        generation_row->second.session && !admitted &&
        !draining_generations_.contains(generation.generation_);
    if ((!admitted && !transient) ||
        generation_row == peer_generations_.end() ||
        !generation_row->second.session ||
        (admitted &&
         generation_row->second.session != connection->second.session) ||
        generation_row->second.lifecycle.generation() !=
            generation.generation_ ||
        !generation_row->second.lifecycle.ready() ||
        //@@mutation-site peer-use-request-must-match-current-range
        (require_coverage && request.ledger_seq != 0 &&
         !generation_row->second.lifecycle.status()->advertised_range.contains(
             request.ledger_seq)) ||
        //@@mutation-end peer-use-request-must-match-current-range
        //@@mutation-site peer-use-current-event-sequence-required
        generation_row->second.lifecycle.last_event_sequence() !=
            generation.event_sequence_)
    //@@mutation-end peer-use-current-event-sequence-required
    {
        return {};
    }

    try
    {
        auto prepared = std::make_unique<PreparedPeerUse>();
        prepared->use_id = next_peer_use_id_ + 1;
        prepared->endpoint = generation.endpoint_;
        prepared->generation = generation.generation_;
        PeerUseTicket const ticket{
            peer_set_instance_id_, prepared->use_id, generation.generation_};
        auto weak = weak_from_this();
        prepared->control = std::make_shared<detail::PeerUseControl>();
        prepared->state = std::make_shared<detail::PeerUseState>();
        auto receipt =
            std::make_shared<detail::PeerUseReceiptState>(strand_);
        prepared->control->session = generation_row->second.session;
        prepared->control->generation = generation;
        prepared->control->request = request;
        prepared->control->ticket = ticket;
        prepared->control->owner = weak;
        prepared->control->receipt = receipt;
        prepared->control->finish =
            [weak, ticket](PeerUseOutcome outcome,
                           detail::PeerUseProposalSource source) {
                if (auto self = weak.lock())
                {
                    auto remaining =
                        self->drop_peer_use_finish_posts_for_test_.load(
                            std::memory_order_acquire);
                    while (
                        remaining != 0 &&
                        !self->drop_peer_use_finish_posts_for_test_
                             .compare_exchange_weak(
                                 remaining,
                                 remaining - 1,
                                 std::memory_order_acq_rel,
                                 std::memory_order_acquire))
                    {
                    }
                    if (remaining != 0)
                        return;
                    asio::post(
                        self->strand_,
                        [self, ticket, outcome, source]() noexcept {
                            self->finish_peer_use(
                                ticket, outcome, source);
                        });
                }
            };
        prepared->state->control = prepared->control;
        prepared->deadline = std::make_shared<asio::steady_timer>(
            strand_, request.absolute_deadline);
        prepared->deadline->async_wait(
            [weak, ticket](boost::system::error_code const& ec) noexcept {
                if (ec == asio::error::operation_aborted)
                    return;
                if (auto self = weak.lock())
                    self->expire_peer_use(ticket);
            });

        std::map<std::uint64_t, ActivePeerUse> active;
        active.emplace(
            prepared->use_id,
            ActivePeerUse{
                .endpoint = generation.endpoint_,
                .connection_generation = generation.generation_,
                .request = request,
                .control = prepared->control,
                .deadline = prepared->deadline});
        prepared->active_node = active.extract(prepared->use_id);
        std::set<std::uint64_t> membership;
        membership.insert(prepared->use_id);
        prepared->generation_use_node =
            membership.extract(prepared->use_id);
        return prepared;
    }
    catch (...)
    {
        return {};
    }
}

PeerUse
//@@authority transition=prv.T0243 status=provisional scope=function bind=catl::peer_client::commit_prepared_peer_use@a6f1e4f58014d31c
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0088,prv.S0090,prv.S0091,prv.S0093,prv.S0094,prv.S0095,prv.S0096,prv.S0100
// label=install_exact_generation_peer_use_before_caller_resumes
PeerSet::commit_prepared_peer_use(PreparedPeerUse& prepared) noexcept
{
    ASSERT_ON_STRAND();
    auto generation = peer_generations_.find(prepared.endpoint);
    if (prepared.committed || prepared.use_id == 0 ||
        prepared.use_id != next_peer_use_id_ + 1 ||
        active_peer_uses_.contains(prepared.use_id) ||
        generation == peer_generations_.end() ||
        generation->second.lifecycle.generation() != prepared.generation ||
        generation->second.active_use_ids.contains(prepared.use_id) ||
        prepared.active_node.empty() || prepared.generation_use_node.empty() ||
        !prepared.state)
    {
        return {};
    }

    auto active = active_peer_uses_.insert(std::move(prepared.active_node));
    auto member = generation->second.active_use_ids.insert(
        std::move(prepared.generation_use_node));
    assert(active.inserted);
    assert(member.inserted);
    next_peer_use_id_ = prepared.use_id;
    prepared.committed = true;
    return PeerUse{std::move(prepared.state)};
}

boost::asio::awaitable<PeerUse>
PeerSet::co_acquire_peer_use(
    PeerGenerationRef generation,
    PeerUseRequest request)
{
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        co_return PeerUse{};
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self,
         generation = std::move(generation),
         request]() mutable -> asio::awaitable<PeerUse> {
            co_return self->make_peer_use(generation, request);
        },
        asio::use_awaitable);
}

//@@authority transition=prv.T0247 status=provisional scope=function
// bind=catl::peer_client::finish_peer_use@a75164122d3fbfd8
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0090,prv.S0097,prv.S0098
// label=arbitrate_peer_use_proposal_against_deadline_and_shutdown
void
PeerSet::finish_peer_use(
    PeerUseTicket ticket,
    PeerUseOutcome outcome,
    detail::PeerUseProposalSource source) noexcept
{
    ASSERT_ON_STRAND();
    if (ticket.peer_set_instance != peer_set_instance_id_)
        return;
    auto use = active_peer_uses_.find(ticket.use_id);
    if (use == active_peer_uses_.end() ||
        use->second.connection_generation != ticket.generation)
    {
        return;
    }

    if (use->second.finishing)
        return;
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
    {
        begin_forced_peer_use_finish(ticket, PeerUseOutcome::shutdown);
        return;
    }
    //@@mutation-site peer-use-owner-commit-checks-immutable-deadline
    if (std::chrono::steady_clock::now() >=
        use->second.request.absolute_deadline)
    {
        begin_forced_peer_use_finish(ticket, PeerUseOutcome::timeout);
        return;
    }
    //@@mutation-end peer-use-owner-commit-checks-immutable-deadline
    //@@mutation-site peer-use-local-finish-cannot-forge-physical-terminal
    if (source != detail::PeerUseProposalSource::physical_terminal &&
        use->second.control &&
        use->second.control->phase.load(std::memory_order_acquire) ==
            detail::PeerUsePhase::submitted)
    {
        begin_forced_peer_use_finish(ticket, outcome);
        return;
    }
    //@@mutation-end peer-use-local-finish-cannot-forge-physical-terminal
    finalize_peer_use(ticket, outcome);
}

//@@authority transition=prv.T0244 status=provisional scope=function
// bind=catl::peer_client::finalize_peer_use@c5f7720628a5b35b
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0088,prv.S0090,prv.S0092,prv.S0094,prv.S0096
// label=classify_release_and_ack_exact_peer_use_once
void
PeerSet::finalize_peer_use(
    PeerUseTicket ticket,
    PeerUseOutcome outcome) noexcept
{
    ASSERT_ON_STRAND();
    if (ticket.peer_set_instance != peer_set_instance_id_)
        return;
    auto use = active_peer_uses_.find(ticket.use_id);
    if (use == active_peer_uses_.end() ||
        use->second.connection_generation != ticket.generation)
    {
        return;
    }

    auto generation = peer_generations_.find(use->second.endpoint);
    if (generation != peer_generations_.end() &&
        generation->second.lifecycle.generation() == ticket.generation)
    {
        generation->second.active_use_ids.erase(ticket.use_id);
    }
    if (use->second.deadline)
    {
        try
        {
            use->second.deadline->cancel();
        }
        catch (...)
        {
        }
    }
    auto const outcome_index = static_cast<std::size_t>(outcome);
    if (outcome_index < peer_use_outcomes_.size())
        ++peer_use_outcomes_[outcome_index];
    if (auto const& control = use->second.control)
    {
        (void)control->revoke();
        if (control->receipt)
            control->receipt->complete_on_owner(outcome);
    }
    active_peer_uses_.erase(use);
    notify_shutdown_progress();
}

//@@authority transition=prv.T0248 status=provisional scope=function
// bind=catl::peer_client::begin_forced_peer_use_finish@17f25484b00565a7
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0088,prv.S0089,prv.S0090,prv.S0094,prv.S0097,prv.S0098,prv.S0099
// label=revoke_submitted_use_and_start_exact_generation_drain
void
PeerSet::begin_forced_peer_use_finish(
    PeerUseTicket ticket,
    PeerUseOutcome outcome) noexcept
{
    ASSERT_ON_STRAND();
    if (ticket.peer_set_instance != peer_set_instance_id_)
        return;
    auto trigger = active_peer_uses_.find(ticket.use_id);
    if (trigger == active_peer_uses_.end() ||
        trigger->second.connection_generation != ticket.generation)
    {
        return;
    }
    if (trigger->second.finishing)
        return;

    auto phase = detail::PeerUsePhase::reserved;
    if (auto const& control = trigger->second.control)
        phase = control->revoke_if_reserved();
    if (phase != detail::PeerUsePhase::submitted)
    {
        finalize_peer_use(ticket, outcome);
        return;
    }

    auto const endpoint = trigger->second.endpoint;
    auto const generation = trigger->second.connection_generation;

    auto connection = connections_.find(endpoint);
    if (connection != connections_.end() &&
        connection->second.generation == generation)
    {
        remove_peer(endpoint, false);
    }
    auto const drain_started = start_generation_drain(
        endpoint, generation, GenerationDrainCause::local_use, outcome, ticket);

    if (!drain_started && !draining_generations_.count(generation))
    {
        mark_generation_peer_uses_finishing(
            generation, GenerationDrainCause::local_use, outcome, ticket);
        fail_generation_peer_uses_without_drain(generation);
    }
}

void
PeerSet::mark_generation_peer_uses_finishing(
    std::uint64_t generation,
    GenerationDrainCause cause,
    PeerUseOutcome outcome,
    std::optional<PeerUseTicket> trigger) noexcept
{
    ASSERT_ON_STRAND();
    for (auto& [use_id, use] : active_peer_uses_)
    {
        if (use.connection_generation != generation)
            continue;
        use.finishing = true;
        if (!use.forced_outcome)
        {
            switch (cause)
            {
                case GenerationDrainCause::peer_terminal:
                    use.forced_outcome = PeerUseOutcome::transport_failure;
                    break;
                case GenerationDrainCause::local_use:
                    //@@mutation-site
                    //peer-use-local-drain-does-not-blame-sibling
                    use.forced_outcome = trigger && trigger->use_id == use_id
                        ? outcome
                        : PeerUseOutcome::cancelled;
                    //@@mutation-end peer-use-local-drain-does-not-blame-sibling
                    break;
                case GenerationDrainCause::shutdown:
                    use.forced_outcome = PeerUseOutcome::shutdown;
                    break;
            }
        }
        if (use.control)
            (void)use.control->revoke();
    }
}

void
PeerSet::fail_generation_peer_uses_without_drain(
    std::uint64_t generation) noexcept
{
    ASSERT_ON_STRAND();
    for (auto& [_, use] : active_peer_uses_)
    {
        if (use.connection_generation != generation || !use.control ||
            !use.control->receipt)
        {
            continue;
        }
        use.control->receipt->fail_on_owner(
            PeerUseReceiptFault::physical_drain_failed);
    }
}

void
PeerSet::finish_generation_peer_uses_after_drain(
    std::uint64_t generation,
    PeerUseOutcome fallback) noexcept
{
    ASSERT_ON_STRAND();
    for (auto use = active_peer_uses_.begin(); use != active_peer_uses_.end();)
    {
        if (use->second.connection_generation != generation)
        {
            ++use;
            continue;
        }
        PeerUseTicket const ticket{
            peer_set_instance_id_,
            use->first,
            use->second.connection_generation};
        auto const outcome = use->second.forced_outcome.value_or(fallback);
        ++use;
        finalize_peer_use(ticket, outcome);
    }
    auto origin = std::find_if(
        peer_generations_.begin(),
        peer_generations_.end(),
        [generation](auto const& current) {
            return current.second.lifecycle.generation() == generation;
        });
    if (origin != peer_generations_.end())
    {
        release_generation_history_origin(
            origin->second, HistoryWorkReceipt::closed);
    }
}

//@@authority transition=prv.T0249 status=provisional scope=function
// bind=catl::peer_client::expire_peer_use@a8f3cf0ef98a8918
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0088,prv.S0089,prv.S0090,prv.S0094,prv.S0097,prv.S0098
// label=deadline_forces_owner_finalization_even_after_callback_proposal
void
PeerSet::expire_peer_use(PeerUseTicket ticket) noexcept
{
    ASSERT_ON_STRAND();
    if (ticket.peer_set_instance != peer_set_instance_id_)
        return;
    auto use = active_peer_uses_.find(ticket.use_id);
    if (use == active_peer_uses_.end() ||
        use->second.connection_generation != ticket.generation)
    {
        return;
    }
    if (use->second.finishing)
        return;
    //@@mutation-site peer-use-expiry-ignores-lost-proposal-channel
    begin_forced_peer_use_finish(ticket, PeerUseOutcome::timeout);
    //@@mutation-end peer-use-expiry-ignores-lost-proposal-channel
}

//@@authority transition=prv.T0250 status=provisional scope=function
// bind=catl::peer_client::cancel_all_peer_uses_for_shutdown@2d012768006cd4c9
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0090,prv.S0094,prv.S0096,prv.S0097,prv.S0098
// label=shutdown_revokes_every_use_before_physical_generation_close
void
PeerSet::cancel_all_peer_uses_for_shutdown() noexcept
{
    ASSERT_ON_STRAND();
    for (auto use = active_peer_uses_.begin(); use != active_peer_uses_.end();)
    {
        PeerUseTicket const ticket{
            peer_set_instance_id_,
            use->first,
            use->second.connection_generation};
        auto phase = detail::PeerUsePhase::reserved;
        if (use->second.control)
            phase = use->second.control->revoke();

        if (phase == detail::PeerUsePhase::submitted || use->second.finishing)
        {
            use->second.finishing = true;
            use->second.forced_outcome = PeerUseOutcome::shutdown;
            ++use;
            continue;
        }

        // Increment before finalize: finalization erases the current row.
        ++use;
        finalize_peer_use(ticket, PeerUseOutcome::shutdown);
    }
}

void
PeerSet::release_peer_pin(
    std::uint64_t pin_id,
    std::uint64_t connection_generation) noexcept
{
    ASSERT_ON_STRAND();
    auto pin = active_peer_pins_.find(pin_id);
    if (pin == active_peer_pins_.end() ||
        pin->second.connection_generation != connection_generation)
    {
        return;
    }
    active_peer_pins_.erase(pin);
    notify_shutdown_progress();
}

void
PeerSet::prune_expired_peer_pins() const noexcept
{
    ASSERT_ON_STRAND();
    std::erase_if(active_peer_pins_, [](auto const& item) {
        return item.second.live_handle.expired();
    });
}

bool
PeerSet::generation_has_active_pin(std::uint64_t connection_generation) const
{
    ASSERT_ON_STRAND();
    prune_expired_peer_pins();
    if (std::any_of(
            active_peer_pins_.begin(),
            active_peer_pins_.end(),
            [connection_generation](auto const& entry) {
                return entry.second.connection_generation ==
                    connection_generation;
            }))
    {
        return true;
    }
    return std::any_of(
        active_peer_uses_.begin(),
        active_peer_uses_.end(),
        [connection_generation](auto const& entry) {
            return entry.second.connection_generation == connection_generation;
        });
}

boost::asio::awaitable<PeerGenerationPin>
PeerSet::add(std::string const& host, uint16_t port)
{
    auto client = co_await try_connect(host, port);
    if (!client)
    {
        throw std::runtime_error(
            "PeerSet: failed to connect to " + make_key(host, port));
    }
    co_return std::move(client);
}

PeerGenerationPin
PeerSet::peer_for(uint32_t ledger_seq)
{
    ASSERT_ON_STRAND();
    static std::unordered_set<std::string> const empty;
    return peer_for(ledger_seq, empty);
}

PeerGenerationPin
PeerSet::peer_for(
    uint32_t ledger_seq,
    std::unordered_set<std::string> const& excluded)
{
    ASSERT_ON_STRAND();
    PeerSessionPtr best;
    std::string best_key;

    for (auto const& [key, client_entry] : connections_)
    {
        auto const& client = client_entry.session;
        if (excluded.count(key))
            continue;
        auto const* status = ready_status_for(key, client_entry.generation);
        if (!client || !status)
            continue;
        if (status->advertised_range.contains(ledger_seq))
        {
            if (!best || candidate_better(key, best_key))
            {
                best = client;
                best_key = key;
            }
        }
    }
    if (!best)
        return {};
    note_peer_selected(best_key);
    return make_peer_pin(best);
}

boost::asio::awaitable<PeerGenerationPin>
PeerSet::wait_for_any_peer(int timeout_secs)
{
    static std::unordered_set<std::string> const empty;
    co_return co_await wait_for_any_peer(timeout_secs, empty);
}

boost::asio::awaitable<PeerGenerationPin>
PeerSet::wait_for_any_peer(
    int timeout_secs,
    std::unordered_set<std::string> const& excluded)
{
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        co_return PeerGenerationPin{};

    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self,
         timeout_secs,
         excluded]() mutable -> asio::awaitable<PeerGenerationPin> {
            auto client = co_await self->wait_for_any_peer_on_strand(
                timeout_secs, std::move(excluded));
            if (!client)
                co_return PeerGenerationPin{};
            co_return self->make_peer_pin(client);
        },
        asio::use_awaitable);
}

boost::asio::awaitable<PeerSessionPtr>
PeerSet::wait_for_any_peer_on_strand(
    int timeout_secs,
    std::unordered_set<std::string> excluded)
{
    ASSERT_ON_STRAND();
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        co_return nullptr;

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
        if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
            co_return nullptr;

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
        if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
            co_return nullptr;
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

boost::asio::awaitable<PeerGenerationPin>
PeerSet::wait_for_covering_peer(
    uint32_t ledger_seq,
    int timeout_secs,
    std::shared_ptr<std::atomic<bool>> cancel)
{
    static std::unordered_set<std::string> const empty;
    co_return co_await wait_for_covering_peer(
        ledger_seq, timeout_secs, empty, std::move(cancel));
}

//@@authority transition=prv.T0059 status=provisional scope=function
// bind=catl::peer_client::wait_for_covering_peer@7e749a32a465f363
// owner=unresolved owner_hypothesis=peer_set commit=strand
// label=mint_covering_generation_after_targeted_search
boost::asio::awaitable<PeerGenerationPin>
PeerSet::wait_for_covering_peer(
    uint32_t ledger_seq,
    int timeout_secs,
    std::unordered_set<std::string> const& excluded,
    std::shared_ptr<std::atomic<bool>> cancel)
{
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        co_return PeerGenerationPin{};

    auto self = shared_from_this();
    auto request_executor = catl::core::ContextExecutor(
        strand_, catl::core::retain_current_request());
    co_return co_await asio::co_spawn(
        request_executor,
        [self,
         ledger_seq,
         timeout_secs,
         excluded,
         cancel = std::move(
             cancel)]() mutable -> asio::awaitable<PeerGenerationPin> {
            auto client = co_await self->wait_for_covering_peer_on_strand(
                ledger_seq,
                timeout_secs,
                std::move(excluded),
                std::move(cancel));
            if (!client)
                co_return PeerGenerationPin{};
            auto const* status = self->ready_status_for(client);
            if (!status || !status->advertised_range.contains(ledger_seq))
                co_return PeerGenerationPin{};
            co_return self->make_peer_pin(client);
        },
        asio::use_awaitable);
}

//@@authority state=prv.S0077 status=provisional scope=function
// bind=catl::peer_client::wait_for_covering_peer_on_strand@94275c1f11b606ec
// owner=unresolved owner_hypothesis=peer_history_search_frame
// observed_home=PeerSet::wait_for_covering_peer_on_strand_coroutine_frame
// label=request_visible_history_search_deadline_and_zero_publication_latch
//@@authority transition=prv.T0028 status=provisional scope=function
// bind=catl::peer_client::wait_for_covering_peer_on_strand@94275c1f11b606ec
// owner=unresolved owner_hypothesis=peer_set commit=strand
// touches=prv.S0011,prv.S0077
// label=own_history_covered_peer_search_lifecycle
boost::asio::awaitable<PeerSessionPtr>
PeerSet::wait_for_covering_peer_on_strand(
    uint32_t ledger_seq,
    int timeout_secs,
    std::unordered_set<std::string> excluded,
    std::shared_ptr<std::atomic<bool>> cancel)
{
    ASSERT_ON_STRAND();

    if (shutdown_ingress_sealed_.load(std::memory_order_acquire) ||
        (cancel && cancel->load(std::memory_order_relaxed)))
        co_return nullptr;

    auto const timeout_ms = timeout_secs * 1000;
    auto const start = std::chrono::steady_clock::now();
    auto const deadline = start + std::chrono::milliseconds(timeout_ms);
    auto const completion_deadline = timeout_secs > 0
        ? deadline + std::chrono::milliseconds(timeout_ms)
        : deadline;
    // Stable request-visible terminal bound, also carried by every targeted
    // queue authorization minted by this request.
    auto const progress_deadline = options_.max_targeted_connect_attempts > 0
        ? completion_deadline
        : deadline;

    auto self = shared_from_this();
    auto authorization = std::make_shared<TargetedConnectAuthorization>(
        ledger_seq, excluded, progress_deadline);
    struct WaitTargetCleanup
    {
        std::weak_ptr<PeerSet> self;
        asio::strand<asio::io_context::executor_type> strand;
        uint32_t ledger_seq = 0;
        TargetedConnectAuthorizationPtr authorization;
        bool active = true;

        void
        clean_now()
        {
            if (!active)
                return;
            if (auto locked = self.lock())
            {
                locked->finish_targeted_authorization(authorization, true);
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

    auto snapshot_peers = [&]() -> std::vector<PeerCandidate> {
        auto const now = std::chrono::steady_clock::now();
        std::vector<PeerCandidate> candidates;
        candidates.reserve(connections_.size());
        for (auto const& [key, client_entry] : connections_)
        {
            auto const& client = client_entry.session;
            auto const* live = ready_status_for(key, client_entry.generation);
            if (!client || !live)
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
            auto const range = live->advertised_range;
            candidates.push_back(
                {key,
                 range.first_seq,
                 range.last_seq,
                 stats.selection_count,
                 true,
                 failed});
        }
        return candidates;
    };

    std::unordered_set<std::string> targeted_candidates;
    std::size_t targeted_connects = 0;
    bool targeted_budget_reported = false;
    bool zero_countdown_published = false;
    auto emit_search_progress = [&](std::chrono::steady_clock::time_point now) {
        auto const remaining_ms = std::max<std::int64_t>(
            0,
            std::chrono::duration_cast<std::chrono::milliseconds>(
                progress_deadline - now)
                .count());
        auto const remaining_secs = (remaining_ms + 999) / 1000;
        if (remaining_secs == 0)
            zero_countdown_published = true;
        auto const targeted_queued = std::ranges::count_if(
            targeted_candidates,
            [&](auto const& key) { return queued_.count(key) != 0; });
        auto const targeted_in_flight =
            std::ranges::count_if(in_flight_, [&](auto const& row) {
                return std::ranges::find(
                           row.second.targeted_ledgers, ledger_seq) !=
                    row.second.targeted_ledgers.end();
            });
        std::ostringstream status;
        status << "peer history search ledger " << ledger_seq
               << ": candidates=" << targeted_candidates.size() << '/'
               << options_.max_targeted_connect_attempts
               << ", queued=" << targeted_queued
               << ", in_flight=" << targeted_in_flight
               << ", connected=" << connections_.size()
               << ", remaining=" << remaining_secs << 's';
        PEER_SET_TRACE(
            "[",
            net_label_,
            "] waiting for covering peer ledger=",
            ledger_seq,
            " reason=search_in_progress candidates=",
            targeted_candidates.size(),
            " queued=",
            targeted_queued,
            " in_flight=",
            targeted_in_flight,
            " remaining=",
            remaining_secs,
            "s");
        //@@authority transition=prv.T0226 status=provisional scope=statement
        // bind=emit_status@5c5110c03f0c10b2
        // owner=unresolved owner_hypothesis=peer_history_search_frame
        // commit=status_mutex touches=prv.S0074,prv.S0077 calls=emit_status
        // label=publish_history_search_progress_and_latch_zero_if_exhausted
        catl::core::emit_status(
            status.str());  //@@authority-target ids=prv.T0226
    };
    auto emit_terminal_zero_progress = [&]() {
        if (!zero_countdown_published)
            emit_search_progress(progress_deadline);
    };
    auto queue_targeted_candidate = [&]() -> bool {
        if (targeted_connects >= options_.max_targeted_connect_attempts)
        {
            if (!targeted_budget_reported)
            {
                targeted_budget_reported = true;
                PLOGI(
                    log_,
                    "[",
                    net_label_,
                    "] Targeted peer search for ledger ",
                    ledger_seq,
                    " suppressed: attempted=",
                    targeted_connects,
                    ", limit=",
                    options_.max_targeted_connect_attempts);
            }
            return false;
        }
        auto connect_excluded = excluded;
        connect_excluded.insert(
            targeted_candidates.begin(), targeted_candidates.end());
        auto queued = try_candidates_for(authorization, connect_excluded, 1);
        targeted_connects += queued.size();
        targeted_candidates.insert(queued.begin(), queued.end());
        return !queued.empty();
    };

    // Initial check before entering the poll loop
    {
        auto sel =
            select_peer(ledger_seq, snapshot_peers(), excluded, 0, timeout_ms);
        if (sel.decision == PeerDecision::use_range_match)
        {
            auto it = connections_.find(sel.endpoint);
            if (it != connections_.end() && it->second.session)
            {
                PLOGI(
                    log_,
                    "[",
                    net_label_,
                    "] ",
                    "Range match: ",
                    sel.endpoint,
                    " for ledger ",
                    ledger_seq);
                note_peer_selected(sel.endpoint);
                cleanup.clean_now();
                co_return it->second.session;
            }
        }
    }

    // Probe a bounded number of disconnected candidates while this wait is
    // active. Admission may rotate one unsuitable peer in the same pool.
    queue_targeted_candidate();
    emit_search_progress(start);

    auto publish_no_covering_peer_after_zero = [&]() {
        //@@authority transition=prv.T0227 status=provisional scope=statement
        // bind=expression_statement@14b56be20164f4b7
        // owner=unresolved owner_hypothesis=peer_history_search_frame
        // commit=peer_set_strand touches=prv.S0077
        // label=establish_zero_countdown_before_no_covering_peer_status
        emit_terminal_zero_progress();  //@@authority-target ids=prv.T0227
        cleanup.clean_now();
        auto const elapsed_secs =
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - start)
                .count();
        std::vector<std::string> selected_candidates(
            targeted_candidates.begin(), targeted_candidates.end());
        std::sort(selected_candidates.begin(), selected_candidates.end());
        std::ostringstream candidates_text;
        candidates_text << '[';
        for (std::size_t index = 0; index < selected_candidates.size(); ++index)
        {
            if (index != 0)
                candidates_text << ", ";
            candidates_text << selected_candidates[index];
        }
        candidates_text << ']';
        PLOGW(
            log_,
            "[",
            net_label_,
            "] No peer advertising ledger ",
            ledger_seq,
            " after ",
            elapsed_secs,
            "s; targeted_candidates=",
            selected_candidates.size(),
            '/',
            options_.max_targeted_connect_attempts,
            ' ',
            candidates_text.str());
        std::ostringstream status;
        status << "peer history search ledger " << ledger_seq
               << ": no covering peer after " << elapsed_secs
               << "s; candidates=" << selected_candidates.size() << '/'
               << options_.max_targeted_connect_attempts;
        //@@authority transition=prv.T0228 status=provisional scope=statement
        // bind=emit_status@5c5110c03f0c10b2
        // owner=unresolved owner_hypothesis=peer_history_search_frame
        // commit=status_mutex touches=prv.S0074,prv.S0077 calls=emit_status
        // label=publish_no_covering_peer_status_after_zero_countdown
        catl::core::emit_status(
            status.str());  //@@authority-target ids=prv.T0228
    };

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

    auto next_maintenance = start + std::chrono::seconds(1);
    auto next_log = start + std::chrono::seconds(5);

    for (;;)
    {
        if (shutdown_ingress_sealed_.load(std::memory_order_acquire) ||
            (cancel && cancel->load(std::memory_order_relaxed)))
        {
            cleanup.clean_now();
            PLOGD(
                log_,
                "[",
                net_label_,
                "] wait_for_covering_peer canceled for ledger ",
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
            emit_search_progress(now);
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
                "] wait_for_covering_peer canceled for ledger ",
                ledger_seq);
            co_return nullptr;
        }

        auto sel = select_peer(
            ledger_seq, snapshot_peers(), excluded, elapsed_ms, timeout_ms);

        switch (sel.decision)
        {
            case PeerDecision::use_range_match: {
                auto it = connections_.find(sel.endpoint);
                auto const* status = it != connections_.end()
                    ? ready_status_for(sel.endpoint, it->second.generation)
                    : nullptr;
                if (it != connections_.end() && it->second.session && status)
                {
                    auto const range = status->advertised_range;
                    PLOGI(
                        log_,
                        "[",
                        net_label_,
                        "] ",
                        "Found covering peer: ",
                        sel.endpoint,
                        " for ledger ",
                        ledger_seq,
                        " (range ",
                        range.first_seq,
                        "-",
                        range.last_seq,
                        ")");
                    note_peer_selected(sel.endpoint);
                    cleanup.clean_now();
                    co_return it->second.session;
                }
                break;  // selected peer vanished, keep polling
            }
            case PeerDecision::give_up:
                if (now >= progress_deadline)
                {
                    publish_no_covering_peer_after_zero();
                    co_return nullptr;
                }
                break;

            case PeerDecision::wait:
                break;
        }

        if (now >= deadline)
        {
            if (now >= progress_deadline)
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

        auto wait_until =
            std::min({progress_deadline, next_maintenance, next_log});
        auto wait_signal = attach_wait_signal();
        auto const wait_for =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                wait_until - std::chrono::steady_clock::now());
        auto outcome = co_await wait_for_signal_or_timeout(
            strand_, wait_signal, wait_for, cancel);
        ASSERT_ON_STRAND();
        if (shutdown_ingress_sealed_.load(std::memory_order_acquire) ||
            outcome == WaitOutcome::canceled)
        {
            cleanup.clean_now();
            PLOGD(
                log_,
                "[",
                net_label_,
                "] wait_for_covering_peer canceled for ledger ",
                ledger_seq);
            co_return nullptr;
        }
    }

    publish_no_covering_peer_after_zero();

    co_return nullptr;
}

PeerGenerationPin
PeerSet::any_peer()
{
    ASSERT_ON_STRAND();
    static std::unordered_set<std::string> const empty;
    return any_peer(empty);
}

PeerGenerationPin
PeerSet::any_peer(std::unordered_set<std::string> const& excluded)
{
    ASSERT_ON_STRAND();
    PeerSessionPtr best;
    std::string best_key;

    for (auto const& [key, client_entry] : connections_)
    {
        auto const& client = client_entry.session;
        if (excluded.count(key))
            continue;
        if (client && ready_status_for(key, client_entry.generation))
        {
            if (!best || candidate_better(key, best_key))
            {
                best = client;
                best_key = key;
            }
        }
    }
    if (!best)
        return {};
    note_peer_selected(best_key);
    return make_peer_pin(best);
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
    for (auto const& [key, client_entry] : connections_)
    {
        auto const& client = client_entry.session;
        if (client && ready_status_for(key, client_entry.generation))
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
    for (auto const& [key, client_entry] : connections_)
    {
        auto const& client = client_entry.session;
        auto const* status = ready_status_for(key, client_entry.generation);
        if (!client || !status)
            continue;
        auto const range = status->advertised_range;
        if (range.valid() &&
            (range.last_seq - range.first_seq) >
                options_.archival_range_threshold)
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
    auto const* status = ready_status_for(peer);
    if (!peer || !status)
        return false;
    auto const range = status->advertised_range;
    return range.valid() && range.last_seq > range.first_seq &&
        (range.last_seq - range.first_seq) > options_.archival_range_threshold;
}

bool
PeerSet::has_usable_covering_peer(
    uint32_t ledger_seq,
    std::unordered_set<std::string> const& excluded) const
{
    ASSERT_ON_STRAND();
    auto const now = std::chrono::steady_clock::now();
    for (auto const& [key, peer_entry] : connections_)
    {
        auto const& peer = peer_entry.session;
        auto const* status = ready_status_for(key, peer_entry.generation);
        if (!peer || !status)
            continue;
        if (excluded.count(key))
            continue;
        if (status->advertised_range.contains(ledger_seq) &&
            !peer_failed_for_ledger(key, ledger_seq, now))
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

    auto covers = [this](PeerSessionPtr const& peer, uint32_t ledger_seq) {
        auto const* status = ready_status_for(peer);
        if (!peer || !status)
            return false;
        return status->advertised_range.contains(ledger_seq);
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
                    usable(key, peer.session, ledger_seq, excluded);
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
    auto const* status = ready_status_for(candidate);
    if (!candidate || !status)
        return std::nullopt;

    auto const range = status->advertised_range;

    auto const candidate_covers = [&](uint32_t ledger_seq) {
        return range.contains(ledger_seq);
    };

    // Targeted admission authority may only be spent on a peer that explicitly
    // advertises the requested ledger. Ordinary discovery can still admit a
    // non-covering peer when the relevant pool has spare capacity; it cannot
    // evict an established peer on behalf of a ledger it does not advertise.
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
            .candidate_key = candidate_key,
            .candidate_first_seq = range.first_seq,
            .candidate_last_seq = range.last_seq,
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
    if (!candidate || !ready_status_for(candidate))
        return false;

    auto const archival = peer_is_archival(candidate);
    auto const at_cap = archival ? at_archival_cap() : at_connection_cap();
    if (!at_cap)
        return true;

    auto const target =
        rotation_target_for(candidate_key, candidate, admission_leases);
    if (!target)
        return false;

    if (!ready_status_for(candidate) || !target->lease ||
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
    out.crawl_truncated_peer_rows = crawl_truncated_peer_rows_;
    out.crawl_truncated_interval_rows = crawl_truncated_interval_rows_;
    out.crawl_truncated_public_keys = crawl_truncated_public_keys_;
    out.crawl_invalid_public_keys = crawl_invalid_public_keys_;
    out.crawl_malformed_interval_tokens = crawl_malformed_interval_tokens_;
    out.crawl_truncated_observations = crawl_truncated_observations_;
    out.crawl_observation_capture_failures =
        crawl_observation_capture_failures_;
    for (auto const& [reporter, batch] : crawl_observation_batches_)
    {
        if (now - batch.observed_at >= kCrawlRetention)
            continue;
        ++out.crawl_observation_batches;
        out.crawl_observation_count += batch.observations.size();
        for (auto const& observation : batch.observations)
        {
            out.reported_crawl_observations.push_back(
                Snapshot::ReportedCrawlObservation{
                    .reporter_endpoint = reporter,
                    .subject_endpoint = observation.subject_endpoint,
                    .reported_subject_key = observation.reported_subject_key,
                    .intervals = observation.intervals,
                    .observed_at = batch.observed_at_unix,
                    .crawl_epoch = batch.crawl_epoch,
                    .truncated = observation.truncated,
                    .reported_key_conflict =
                        observation.reported_key_conflict});
        }
    }
    out.failed_at_size = retry_gates_.size();
    out.connected_peers = connections_.size();
    out.hub_peers = hub_peer_count();
    out.archival_peers = archival_peer_count();
    out.total_connects = total_connects_;
    out.total_disconnects = total_disconnects_;
    out.in_flight_connects = in_flight_.size();
    out.queued_connects = pending_connects_.size();
    out.crawl_in_flight = crawl_in_flight_.size();
    out.queued_crawls = pending_crawls_.size();
    std::unordered_set<std::string> targeted_endpoints;
    for (auto const& [key, authority] : targeted_connects_)
    {
        if (std::ranges::any_of(
                authority.target_authorizations, [](auto const& authorization) {
                    return authorization && authorization->is_active();
                }))
        {
            targeted_endpoints.insert(key);
        }
    }
    for (auto const& [key, servant] : in_flight_)
    {
        if (!servant.targeted_ledgers.empty())
            targeted_endpoints.insert(key);
    }
    out.targeted_connects = targeted_endpoints.size();
    out.cached_endpoint_limit = options_.cached_endpoint_limit;
    out.max_targeted_connect_attempts = options_.max_targeted_connect_attempts;
    if (options_.endpoint_cache_path.empty())
    {
        out.endpoint_cache_state = "not_configured";
    }
    else if (!endpoint_cache_)
    {
        out.endpoint_cache_state = "unavailable";
        out.endpoint_cache_error = endpoint_cache_error_;
    }
    else
    {
        auto health = endpoint_cache_->health();
        out.endpoint_cache_state = health.available ? "available" : "disabled";
        out.endpoint_cache_error = std::move(health.error);
    }
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
    for (auto const& [key, _] : in_flight_)
        endpoints.insert(key);
    endpoints.insert(queued_.begin(), queued_.end());
    for (auto const& [key, _] : crawl_in_flight_)
        endpoints.insert(key);
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
        if (auto targeted = targeted_connects_.find(key);
            targeted != targeted_connects_.end())
        {
            for (auto const& authorization :
                 targeted->second.target_authorizations)
            {
                if (authorization && authorization->is_active())
                    entry.targeted_ledgers.push_back(authorization->ledger_seq);
            }
        }
        if (auto in_flight = in_flight_.find(key);
            in_flight != in_flight_.end())
        {
            entry.targeted_ledgers.insert(
                entry.targeted_ledgers.end(),
                in_flight->second.targeted_ledgers.begin(),
                in_flight->second.targeted_ledgers.end());
        }
        std::sort(entry.targeted_ledgers.begin(), entry.targeted_ledgers.end());
        entry.targeted_ledgers.erase(
            std::unique(
                entry.targeted_ledgers.begin(), entry.targeted_ledgers.end()),
            entry.targeted_ledgers.end());
        entry.targeted_connect = !entry.targeted_ledgers.empty();

        if (auto stats_it = endpoint_stats_.find(key);
            stats_it != endpoint_stats_.end())
        {
            auto const& stats = stats_it->second;
            entry.first_seq = stats.status.first_seq;
            entry.last_seq = stats.status.last_seq;
            entry.current_seq = stats.status.current_seq;
            entry.range_is_persistent_hint = stats.range_is_persistent_hint;
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
            auto const* live =
                ready_status_for(key, conn_it->second.generation);
            if (live)
            {
                entry.peer_headers = live->headers;
                entry.ready = true;
                auto const range = live->advertised_range;
                entry.first_seq = range.first_seq;
                entry.last_seq = range.last_seq;
                entry.current_seq = live->current_seq;
                entry.range_is_persistent_hint = false;
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

    for (auto const& [key, client_entry] : connections_)
    {
        auto const& client = client_entry.session;
        auto const* status = ready_status_for(key, client_entry.generation);
        if (!client || !status || peer_is_archival(client))
            continue;
        if (generation_has_active_pin(client_entry.generation))
            continue;

        auto const range = status->advertised_range;
        auto const covers =
            target.ledger_seq != 0 && range.contains(target.ledger_seq);
        auto const failed = peer_failed_for_ledger(key, target.ledger_seq, now);
        auto const excluded = target.excluded.count(key) > 0;
        if (covers && !failed && !excluded)
            continue;
        auto const known_bad = failed || excluded;
        if (would_drop_wanted_coverage(key, client, target, now))
            continue;

        auto const span = range.valid() ? range.last_seq - range.first_seq : 0;
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
    auto victim = it != connections_.end() ? it->second.session : nullptr;
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

    for (auto const& [key, client_entry] : connections_)
    {
        auto const& client = client_entry.session;
        auto const* status = ready_status_for(key, client_entry.generation);
        if (!client || !status || !peer_is_archival(client) ||
            generation_has_active_pin(client_entry.generation))
        {
            continue;
        }

        auto const range = status->advertised_range;
        auto const covers =
            target.ledger_seq != 0 && range.contains(target.ledger_seq);
        auto const failed = peer_failed_for_ledger(key, target.ledger_seq, now);
        auto const excluded = target.excluded.count(key) > 0;
        if (covers && !failed && !excluded)
            continue;
        auto const known_bad = failed || excluded;
        if (would_drop_wanted_coverage(key, client, target, now))
            continue;

        auto const span = range.valid() ? range.last_seq - range.first_seq : 0;
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
    auto victim = it != connections_.end() ? it->second.session : nullptr;
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
        begin_generation_close(key, it->second.generation);
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
PeerSet::consume_peer_terminal(
    std::string const& key,
    std::uint64_t generation,
    PeerTerminalReason reason)
{
    if (!strand_.running_in_this_thread())
    {
        asio::post(
            strand_,
            [self = shared_from_this(),
             key,
             generation,
             reason = std::move(reason)]() mutable {
                self->consume_peer_terminal(key, generation, std::move(reason));
            });
        return;
    }
    auto it = connections_.find(key);
    if (it == connections_.end() || it->second.generation != generation)
    {
        // Pure no-op: absent, evicted, duplicate, never-admitted, or
        // generation-mismatched. A stale candidate's death must not touch
        // a live endpoint's stats (pre-admission failures are owned by
        // note_connect_failure), and a duplicate's authority was consumed
        // when the first matching record erased the row.
        PLOGD(
            log_,
            "[",
            net_label_,
            "] Unmatched terminal for ",
            key,
            " gen ",
            generation,
            " ignored (live gen ",
            it != connections_.end() ? it->second.generation : 0,
            ")");
        return;
    }

    // Exact live match: this admitted instance's death is recorded once.
    auto& stats = endpoint_stats_[key];
    stats.last_failure_at = now_unix();
    stats.last_seen_at = stats.last_failure_at;
    stats.failure_count++;
    stats.connected_ok = false;
    record_failure_event(
        key,
        classify_failure_kind(reason.detail, false),
        std::chrono::steady_clock::now());
    PLOGI(
        log_,
        "[",
        net_label_,
        "] Removing dead peer: ",
        key,
        " (gen ",
        generation,
        reason.ec ? ", " : "",
        reason.ec ? reason.ec.message() : std::string{},
        ")");
    connections_.erase(it);
    ++total_disconnects_;
    tracker_->remove(key);
    sort_pending_connects();
    notify_waiters();
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
    for (auto const& [key, _] : in_flight_)
        keep.insert(key);
    keep.insert(queued_.begin(), queued_.end());
    for (auto const& [key, _] : crawl_in_flight_)
        keep.insert(key);
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

//@@authority transition=prv.T0223 status=provisional scope=function
// bind=catl::peer_client::prune_discovery_state@2e89ccf0752f0eab
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0076,prv.S0081
// label=expire_and_bound_owned_discovery_and_observation_diagnostics
void
PeerSet::prune_discovery_state()
{
    ASSERT_ON_STRAND();

    auto const now = std::chrono::steady_clock::now();
    size_t tracker_removed = 0;
    size_t crawled_removed = 0;
    size_t crawl_observation_batches_removed = 0;
    size_t failed_removed = 0;

    std::unordered_set<std::string> active;
    for (auto const& [key, _] : connections_)
        active.insert(key);
    for (auto const& [key, _] : in_flight_)
        active.insert(key);
    active.insert(queued_.begin(), queued_.end());
    for (auto const& [key, _] : crawl_in_flight_)
        active.insert(key);
    active.insert(crawl_queued_.begin(), crawl_queued_.end());

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

    for (auto it = crawl_observation_batches_.begin();
         it != crawl_observation_batches_.end();)
    {
        if (now - it->second.observed_at >= kCrawlRetention)
        {
            it = crawl_observation_batches_.erase(it);
            ++crawl_observation_batches_removed;
        }
        else
        {
            ++it;
        }
    }

    for (auto it = retry_gates_.begin(); it != retry_gates_.end();)
    {
        if (!active.count(it->first) &&
            now - it->second.failed_at >= kFailedEndpointRetention)
        {
            it = retry_gates_.erase(it);
            ++failed_removed;
        }
        else
        {
            ++it;
        }
    }

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

    if (retry_gates_.size() > kMaxTrackedEndpoints)
    {
        struct Candidate
        {
            std::string key;
            std::chrono::steady_clock::time_point at;
        };
        std::vector<Candidate> candidates;
        candidates.reserve(retry_gates_.size());
        for (auto const& [key, gate] : retry_gates_)
        {
            if (!active.count(key))
                candidates.push_back({key, gate.failed_at});
        }
        std::sort(
            candidates.begin(),
            candidates.end(),
            [](Candidate const& lhs, Candidate const& rhs) {
                return lhs.at < rhs.at;
            });
        auto to_remove = retry_gates_.size() - kMaxTrackedEndpoints;
        for (auto const& candidate : candidates)
        {
            if (to_remove == 0)
                break;
            if (retry_gates_.erase(candidate.key) > 0)
            {
                --to_remove;
                ++failed_removed;
            }
        }
    }

    if (tracker_removed > 0 || crawled_removed > 0 ||
        crawl_observation_batches_removed > 0 || failed_removed > 0)
    {
        PLOGI(
            log_,
            "[",
            net_label_,
            "] Pruned discovery state: tracker=",
            tracker_removed,
            " crawled=",
            crawled_removed,
            " crawl_observations=",
            crawl_observation_batches_removed,
            " failed=",
            failed_removed,
            " (known=",
            tracker_->size(),
            " crawled=",
            crawled_.size(),
            " failed=",
            retry_gates_.size(),
            ")");
    }

    // Also prune endpoint_stats_ here — it was previously only pruned from
    // try_undiscovered() which doesn't run during idle operation.
    prune_endpoint_stats();
}

#undef PEER_SET_TRACE

}  // namespace catl::peer_client
