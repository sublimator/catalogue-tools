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

static LogPartition log_("peer-set", LogLevel::INHERIT);

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
    // bind=expression_statement@46b6d04e8bcc7736
    // owner=unresolved owner_hypothesis=peer_set
    // commit=caller_thread_stop_state touches=prv.S0062
    // label=publish_retained_crawl_stop_fact_before_ingress_seal
    //@@mutation-site crawl-stop-fact-precedes-seal
    crawl_stop_source_.request_stop(); //@@authority-target ids=prv.T0190

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

            // Connect/crawl servants that crossed the seal own their frames
            // until their bounded operations finish. Their terminal funnels
            // notify this timer after removing the exact census row.
            while (!self->shutdown_drained_on_strand())
            {
                progress->expires_at(asio::steady_timer::time_point::max());
                if (self->shutdown_drained_on_strand())
                    break;

                boost::system::error_code ec;
                co_await progress->async_wait(
                    asio::redirect_error(asio::use_awaitable, ec));
            }

            self->lifecycle_ = Lifecycle::stopped;
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
    tracker_->set_discovered_observer({});
    tracker_->set_status_observer({});

    for (auto const& authorization : active_target_authorizations_)
    {
        if (authorization)
            authorization->invalidate();
    }
    active_target_authorizations_.clear();
    targeted_connects_.clear();
    wanted_ledgers_.clear();
    prioritized_ledgers_.clear();

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

    // The exact census consumption and stop wake are the no-throw receipt
    // commit. Diagnostics and successor pumping are optional downstream work.
    //@@authority transition=prv.T0091 status=provisional scope=statement
    // bind=expression_statement@6f2db77abd01a92f
    // owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
    // touches=prv.S0037 label=consume_background_connect_servant_receipt
    in_flight_.erase(key);  //@@authority-target ids=prv.T0091
    notify_shutdown_progress();

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
    if (row == in_flight_.end() || row->second.session)
        return;

    row->second.session = session;
    row->second.generation = generation;
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
    failed_at_[key] = std::chrono::steady_clock::now();
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

    // As above, optional retention and diagnostics cannot sit between census
    // consumption and the shared stop wake.
    //@@authority transition=prv.T0092 status=provisional scope=statement
    // bind=expression_statement@660eab6dbbe007d7
    // owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
    // touches=prv.S0038 label=consume_background_crawl_servant_receipt
    crawl_in_flight_.erase(key);  //@@authority-target ids=prv.T0092
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
                " filtered");
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
    for (auto const& [_, c] : connections_)
        if (c.session && c.session->is_ready())
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
        if (!client || !client->is_ready())
            continue;
        if (excluded.count(key))
            continue;
        if (peer_failed_for_ledger(key, ledger_seq, now))
            continue;

        uint64_t span = 0;
        auto const range = client->advertised_ledger_range();
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
                if (!peer->is_ready())
                    continue;
                ++coverage.ready_peers;

                auto const range = peer->advertised_ledger_range();
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

    for (auto const& [key, client_entry] : connections_)
    {
        auto const& client = client_entry.session;
        if (excluded.count(key))
            continue;
        if (!client || !client->is_ready())
            continue;

        auto const range = client->advertised_ledger_range();
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
        connections_.begin(), connections_.end(), [](auto const& entry) {
            return entry.second.session && entry.second.session->is_ready();
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

//@@authority transition=prv.T0087 status=provisional scope=function
// bind=catl::peer_client::pump_connects@29dc9a52e35a56eb
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0034,prv.S0037
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

        auto self = shared_from_this();
        auto servant =
            [self,
             host,
             port,
             key,
             admission_leases = std::move(
                 admission_leases)]() mutable -> boost::asio::awaitable<void> {
            std::exception_ptr failure;
            try
            {
                co_await self->try_connect_on_strand(
                    host, port, std::move(admission_leases));
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
                        host,
                        port,
                        stop_token,
                        before_stop_check = std::move(before_stop_check),
                        before_https_fallback =
                            std::move(before_https_fallback)]() mutable
            -> boost::asio::awaitable<CrawlServantOutcome> {
            CrawlServantOutcome outcome;
            auto response = co_await detail::co_fetch_peer_crawl_with_stop(
                host,
                port,
                stop_token,
                detail::CrawlFetchHooks{
                    .before_stop_check = std::move(before_stop_check),
                    .before_https_fallback = std::move(before_https_fallback)});

            outcome.raw = response.peers.size();
            outcome.used_tls = response.used_tls;
            for (auto const& peer : response.peers)
            {
                if (peer.endpoint.empty())
                    continue;

                auto normalized =
                    EndpointTracker::normalize_discovered_endpoint(
                        peer.endpoint);
                if (!normalized)
                {
                    ++outcome.filtered;
                    continue;
                }

                ++outcome.accepted;

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

                if (auto status =
                        self->choose_crawl_status(peer.complete_ledgers))
                {
                    ++outcome.ranged;
                    self->tracker_->update(*normalized, *status);
                }
                else
                {
                    self->tracker_->add_discovered(
                        *normalized, DiscoverySource::Crawl);
                }
            }
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
    queue_connect(make_key(host, port));
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
            auto client =
                co_await self->try_connect_on_strand(std::move(host), port, {});
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
    std::vector<TargetedAdmissionLeasePtr> admission_leases)
{
    ASSERT_ON_STRAND();
    if (shutdown_ingress_sealed_.load(std::memory_order_acquire))
        co_return nullptr;

    auto key = make_key(host, port);

    // Already connected? This can change while co_connect() is suspended,
    // so the result is checked again before insertion below.
    auto it = connections_.find(key);
    if (it != connections_.end())
    {
        co_return (it->second.session && it->second.session->is_ready())
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
        // Mint this candidate's instance identity before any handler can
        // fire. The generation travels both in terminal closures and in the
        // session's sealed command checks: a death between connect and
        // admission then consumes as a stale instance instead of erasing
        // whichever peer currently owns the key.
        //@@authority transition=peer_set.mint_connection_generation v4=T31
        // bind=catl::peer_client::try_connect_on_strand@78b31effce858424
        // owner=peer_set commit=strand
        if (next_connection_generation_ ==
            std::numeric_limits<std::uint64_t>::max())
        {
            PLOGE(
                log_,
                "[",
                net_label_,
                "] connection generation space exhausted; refusing ",
                key);
            co_return nullptr;
        }
        auto const generation = ++next_connection_generation_;
        connect_opts.connection_generation = generation;
        {
            auto self_weak = weak_from_this();
            auto owned_key2 = key;
            connect_opts.on_disconnect = [self_weak, owned_key2, generation](
                                             PeerTerminalReason const& reason) {
                if (auto self2 = self_weak.lock())
                {
                    asio::post(
                        self2->strand_,
                        [self2, owned_key2, generation, reason]() {
                            self2->consume_peer_terminal(
                                owned_key2, generation, reason);
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
            });
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

        if (!client || !client->is_ready())
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
        failed_at_.erase(key);
        auto const advertised_range = client->advertised_ledger_range();
        note_connect_success(
            key,
            {.first_seq = advertised_range.first_seq,
             .last_seq = advertised_range.last_seq,
             .current_seq = client->peer_ledger_seq()});
        note_peer_headers(key, client->peer_headers());
        if (endpoint_cache_)
        {
            try
            {
                endpoint_cache_->remember_connect_success(
                    network_id_,
                    key,
                    {.first_seq = advertised_range.first_seq,
                     .last_seq = advertised_range.last_seq,
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
            advertised_range.first_seq,
            "-",
            advertised_range.last_seq,
            ")");
        co_return client;
    }
    catch (PeerClientException const& e)
    {
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
        }
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
    }

    // Exception handlers above only classify and persist failure evidence.
    // Physical lifecycle consumption remains awaitable and outside the catch
    // handlers, where coroutine suspension is legal.
    if (client)
        co_await client->co_disconnect();
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

//@@authority transition=prv.T0217 status=provisional scope=function
// bind=catl::peer_client::load_cached_candidates_for@75f0027fce6aa348
// owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
// touches=prv.S0008
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
        else if (
            current->second.status.first_seq == 0 ||
            current->second.status.last_seq == 0)
        {
            current->second.status = entry.status;
            current->second.range_is_persistent_hint = true;
            current->second.last_success_at = std::max(
                current->second.last_success_at, entry.last_success_at);
            current->second.last_failure_at = std::max(
                current->second.last_failure_at, entry.last_failure_at);
            current->second.success_count =
                std::max(current->second.success_count, entry.success_count);
            current->second.failure_count =
                std::max(current->second.failure_count, entry.failure_count);
            current->second.seen_crawl =
                current->second.seen_crawl || entry.seen_crawl;
            current->second.seen_endpoints =
                current->second.seen_endpoints || entry.seen_endpoints;
            current->second.seen_redirect =
                current->second.seen_redirect || entry.seen_redirect;
            current->second.connected_ok =
                current->second.connected_ok || entry.connected_ok;
        }

        // Reconstruct the monotonic retry gate from the durable failure
        // timestamp. The persistent row authorizes search, not immediate
        // retry, and a later discovery prune must not erase this evidence
        // merely because the endpoint was never in the live tracker.
        auto const stats = endpoint_stats_.find(key);
        if (stats != endpoint_stats_.end() &&
            stats->second.last_failure_at > 0 &&
            (stats->second.last_success_at == 0 ||
             stats->second.last_failure_at > stats->second.last_success_at))
        {
            auto const age_seconds = std::max<std::int64_t>(
                0, unix_now - stats->second.last_failure_at);
            auto const reconstructed =
                steady_now - std::chrono::seconds(age_seconds);
            auto failed = failed_at_.find(key);
            if (failed == failed_at_.end() || failed->second < reconstructed)
                failed_at_[key] = reconstructed;
        }
        candidates.push_back(key);
    }
    return candidates;
}

//@@authority transition=prv.T0027 status=provisional scope=function
// bind=catl::peer_client::try_candidates_for@2f8a29b36dd49275
// owner=unresolved owner_hypothesis=peer_set commit=strand
// touches=prv.S0008,prv.S0009
// label=queue_bounded_history_targeted_connections
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
        !connection->second.session->is_ready())
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
        !expected_session->is_ready())
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
    return std::any_of(
        active_peer_pins_.begin(),
        active_peer_pins_.end(),
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
        if (!client || !client->is_ready())
            continue;
        if (client->advertised_ledger_range().contains(ledger_seq))
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
            if (!client->advertised_ledger_range().contains(ledger_seq))
                co_return PeerGenerationPin{};
            co_return self->make_peer_pin(client);
        },
        asio::use_awaitable);
}

//@@authority transition=prv.T0028 status=provisional scope=function
// bind=catl::peer_client::wait_for_covering_peer_on_strand@94275c1f11b606ec
// owner=unresolved owner_hypothesis=peer_set commit=strand
// touches=prv.S0011 label=own_history_covered_peer_search_lifecycle
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

    auto const timeout_ms = timeout_secs * 1000;
    auto const start = std::chrono::steady_clock::now();
    auto const deadline = start + std::chrono::milliseconds(timeout_ms);

    auto snapshot_peers = [&]() -> std::vector<PeerCandidate> {
        auto const now = std::chrono::steady_clock::now();
        std::vector<PeerCandidate> candidates;
        candidates.reserve(connections_.size());
        for (auto const& [key, client_entry] : connections_)
        {
            auto const& client = client_entry.session;
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
            auto const range = client->advertised_ledger_range();
            candidates.push_back(
                {key,
                 range.first_seq,
                 range.last_seq,
                 stats.selection_count,
                 client->is_ready(),
                 failed});
        }
        return candidates;
    };

    std::unordered_set<std::string> targeted_candidates;
    std::size_t targeted_connects = 0;
    bool targeted_budget_reported = false;
    auto emit_search_progress = [&](std::chrono::steady_clock::time_point now) {
        auto const remaining_ms = std::max<std::int64_t>(
            0,
            std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - now)
                .count());
        auto const remaining_secs = (remaining_ms + 999) / 1000;
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
               << ", in_flight=" << targeted_in_flight
               << ", connected=" << connections_.size()
               << ", remaining=" << remaining_secs << 's';
        catl::core::emit_status(status.str());
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
        if (!queued.empty())
            emit_search_progress(std::chrono::steady_clock::now());
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
    if (!queue_targeted_candidate())
        emit_search_progress(start);

    auto log_no_covering_peer = [&]() {
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
            timeout_secs,
            "s; targeted_candidates=",
            selected_candidates.size(),
            '/',
            options_.max_targeted_connect_attempts,
            ' ',
            candidates_text.str());
        std::ostringstream status;
        status << "peer history search ledger " << ledger_seq
               << ": no covering peer after " << timeout_secs
               << "s; candidates=" << selected_candidates.size() << '/'
               << options_.max_targeted_connect_attempts;
        catl::core::emit_status(status.str());
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
                if (it != connections_.end() && it->second.session)
                {
                    auto const range =
                        it->second.session->advertised_ledger_range();
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
                cleanup.clean_now();
                log_no_covering_peer();
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
            emit_search_progress(now);
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

    cleanup.clean_now();
    log_no_covering_peer();

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
        if (client && client->is_ready())
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
    for (auto const& [_, client_entry] : connections_)
    {
        auto const& client = client_entry.session;
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
    for (auto const& [_, client_entry] : connections_)
    {
        auto const& client = client_entry.session;
        if (!client || !client->is_ready())
            continue;
        auto const range = client->advertised_ledger_range();
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
    if (!peer || !peer->is_ready())
        return false;
    auto const range = peer->advertised_ledger_range();
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
        if (!peer || !peer->is_ready())
            continue;
        if (excluded.count(key))
            continue;
        if (peer->advertised_ledger_range().contains(ledger_seq) &&
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

    auto covers = [](PeerSessionPtr const& peer, uint32_t ledger_seq) {
        if (!peer || !peer->is_ready())
            return false;
        return peer->advertised_ledger_range().contains(ledger_seq);
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
    if (!candidate || !candidate->is_ready())
        return std::nullopt;

    auto const range = candidate->advertised_ledger_range();

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
    std::unordered_set<std::string> targeted_endpoints;
    for (auto const& [key, authorizations] : targeted_connects_)
    {
        if (std::ranges::any_of(authorizations, [](auto const& authorization) {
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
            for (auto const& authorization : targeted->second)
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
            if (conn_it->second.session)
            {
                entry.peer_headers = conn_it->second.session->peer_headers();
            }
            if (conn_it->second.session && conn_it->second.session->is_ready())
            {
                entry.ready = true;
                auto const range =
                    conn_it->second.session->advertised_ledger_range();
                entry.first_seq = range.first_seq;
                entry.last_seq = range.last_seq;
                entry.current_seq = conn_it->second.session->peer_ledger_seq();
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
        if (!client || !client->is_ready() || peer_is_archival(client))
            continue;
        if (generation_has_active_pin(client_entry.generation))
            continue;

        auto const range = client->advertised_ledger_range();
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
        if (!client || !client->is_ready() || !peer_is_archival(client) ||
            generation_has_active_pin(client_entry.generation))
        {
            continue;
        }

        auto const range = client->advertised_ledger_range();
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
    for (auto const& [key, _] : in_flight_)
        active.insert(key);
    active.insert(queued_.begin(), queued_.end());
    for (auto const& [key, _] : crawl_in_flight_)
        active.insert(key);
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
    auto const has_persistent_hint = [&](std::string const& key) {
        auto const stats = endpoint_stats_.find(key);
        return stats != endpoint_stats_.end() &&
            stats->second.range_is_persistent_hint;
    };

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
        if (!active.count(it->first) && !tracked_keys.count(it->first) &&
            !has_persistent_hint(it->first))
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
