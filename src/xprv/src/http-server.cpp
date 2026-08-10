#include "xprv/http-server.h"
#include "http-child-completion.h"
#include "web_assets.h"
#include "xprv/peer-json.h"
#include "xprv/proof-chain-binary.h"
#include "xprv/proof-chain-json.h"
#include "xprv/static-path.h"

#include <cctype>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>

#include <catl/core/context-executor.h>
#include <catl/core/logger.h>
#include <catl/core/request-context.h>
#include <catl/core/request-scope.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/rpc-client/rpc-client.h>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>

#include <array>
#include <map>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>

#ifdef __GLIBC__
#include <malloc.h>
#endif

namespace xprv {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

static LogPartition log_("http", LogLevel::INFO);

// Process-wide X-XPRV header value, set once by HttpServer constructor.
static std::string g_xprv_header;

using CompletionChannel =
    asio::experimental::concurrent_channel<void(boost::system::error_code)>;

struct HttpServer::SessionControl
    : std::enable_shared_from_this<HttpServer::SessionControl>
{
    explicit SessionControl(tcp::socket socket) : stream(std::move(socket))
    {
    }

    void
    request_stop()
    {
        auto self = shared_from_this();
        asio::dispatch(
            stream.get_executor(), [self]() { self->stop_on_strand(); });
    }

    asio::awaitable<void>
    co_stop()
    {
        auto self = shared_from_this();
        co_await asio::co_spawn(
            stream.get_executor(),
            [self]() -> asio::awaitable<void> {
                self->stop_on_strand();
                co_return;
            },
            asio::use_awaitable);
    }

    //@@authority transition=prv.T0076 status=provisional scope=function
    // bind=xprv::HttpServer::SessionControl::stop_on_strand@18b20d74980d0cd2
    // owner=unresolved owner_hypothesis=http_session
    // commit=http_session_strand touches=prv.S0027,prv.S0028,prv.S0029
    // label=commit_http_session_cancellation_and_transport_close
    void
    stop_on_strand()
    {
        if (stop_committed)
            return;
        stop_committed = true;

        cancellation.emit(asio::cancellation_type::all);
        boost::system::error_code ignored;
        auto& socket = beast::get_lowest_layer(stream).socket();
        socket.cancel(ignored);
        socket.shutdown(tcp::socket::shutdown_both, ignored);
        socket.close(ignored);
    }

    //@@authority state=prv.S0027 status=provisional scope=field
    // bind=xprv::HttpServer::SessionControl::stream@dea06a95becc7ce5
    // owner=unresolved owner_hypothesis=http_session
    // observed_home=http_session label=owned_http_session_transport
    beast::tcp_stream stream;
    //@@authority state=prv.S0028 status=provisional scope=field
    // bind=xprv::HttpServer::SessionControl::cancellation@ec5397c2805fca13
    // owner=unresolved owner_hypothesis=http_session
    // observed_home=http_session label=http_session_cancellation_authority
    asio::cancellation_signal cancellation;
    //@@authority state=prv.S0029 status=provisional scope=field
    // bind=xprv::HttpServer::SessionControl::stop_committed@68cec1b5d1d830ea
    // owner=unresolved owner_hypothesis=http_session
    // observed_home=http_session label=idempotent_session_stop_commit
    bool stop_committed = false;
};

struct HttpServer::LifecycleState
{
    enum class Phase : std::uint8_t { accepting, stopping, stopped };

    explicit LifecycleState(asio::any_io_executor executor)
        : stop_progress(
              std::move(executor),
              asio::steady_timer::time_point::max())
    {
    }

    //@@authority state=prv.S0030 status=provisional scope=field
    // bind=xprv::HttpServer::LifecycleState::phase@403552449a5ec89d
    // owner=unresolved owner_hypothesis=http_server
    // observed_home=http_server label=http_admission_and_drain_lifecycle
    Phase phase = Phase::accepting;
    //@@authority state=prv.S0031 status=provisional scope=field
    // bind=xprv::HttpServer::LifecycleState::accept_loop_done@e23f9786b2f1a39c
    // owner=unresolved owner_hypothesis=http_server
    // observed_home=http_server label=accept_loop_terminal_receipt
    bool accept_loop_done = false;
    std::uint64_t epoch = 0;
    std::uint64_t next_session_id = 0;
    //@@authority state=prv.S0032 status=provisional scope=field
    // bind=xprv::HttpServer::LifecycleState::sessions@da9825a43220d2da
    // owner=unresolved owner_hypothesis=http_server
    // observed_home=http_server label=admitted_http_session_frame_registry
    std::unordered_map<std::uint64_t, std::shared_ptr<SessionControl>> sessions;
    //@@authority state=prv.S0033 status=provisional scope=field
    // bind=xprv::HttpServer::LifecycleState::stop_progress@5892d6ee0908f7e9
    // owner=unresolved owner_hypothesis=http_server
    // observed_home=http_server label=shared_http_stopped_receipt_latch
    asio::steady_timer stop_progress;
};

static std::string
generate_request_id()
{
    static constexpr char hex[] = "0123456789abcdef";
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<> dist(0, 15);
    std::string id(8, '\0');
    for (auto& c : id)
        c = hex[dist(gen)];
    return id;
}

//@@authority state=prv.S0124 status=provisional scope=function
// bind=xprv::co_wait_for_transport_failure@952ade70e9fa25ca
// owner=unresolved owner_hypothesis=http_transport_observation
// observed_home=co_wait_for_transport_failure_coroutine_result
// label=response_transport_failure_observation_excluding_data_and_read_half_close
//@@authority transition=prv.T0305 status=provisional scope=function
// bind=xprv::co_wait_for_transport_failure@952ade70e9fa25ca
// owner=unresolved owner_hypothesis=http_transport_observer
// commit=connection_executor touches=prv.S0124
// label=classify_read_readiness_without_promoting_half_close_to_abandonment
asio::awaitable<bool>
HttpServer::co_wait_for_transport_failure(tcp::socket& socket)
{
    boost::system::error_code wait_ec;
    co_await socket.async_wait(
        tcp::socket::wait_read,
        asio::redirect_error(asio::use_awaitable, wait_ec));
    if (wait_ec == asio::error::operation_aborted ||
        wait_ec == asio::error::eof)
        co_return false;
    if (wait_ec)
        co_return true;

    std::array<char, 1> probe{};
    boost::system::error_code probe_ec;
    (void)co_await socket.async_receive(
        asio::buffer(probe),
        tcp::socket::message_peek,
        asio::redirect_error(asio::use_awaitable, probe_ec));

    // Data belongs to a later HTTP request and must remain unread. EOF closes
    // only the peer's write half; both still permit this response to finish.
    if (!probe_ec || probe_ec == asio::error::eof ||
        probe_ec == asio::error::operation_aborted)
        co_return false;
    co_return true;
}

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

static boost::json::object
validation_quorum_tracker_json(ValidationQuorumTracker::Stats const& stats)
{
    boost::json::object result;
    result["recent_quorums"] = stats.recent_quorums;
    result["collector_ledgers"] = stats.collector_ledgers;
    result["collector_validations"] = stats.collector_validations;
    result["waiters"] = stats.pending_callbacks;
    result["queued_validations"] = stats.queued_validations;
    result["queued_validation_bytes"] = stats.queued_validation_bytes;
    result["validation_packets_received"] = stats.validation_packets_received;
    result["validation_packets_processed"] = stats.validation_packets_processed;
    result["validation_packets_deduplicated"] =
        stats.validation_packets_deduplicated;
    result["validation_packets_dropped"] = stats.validation_packets_dropped;
    result["manifest_packets_received"] = stats.manifest_packets_received;
    result["manifest_packets_admitted"] = stats.manifest_packets_admitted;
    result["manifest_packets_processed"] = stats.manifest_packets_processed;
    result["manifest_packets_dropped"] = stats.manifest_packets_dropped;
    result["manifest_packets_abandoned"] = stats.manifest_packets_abandoned;
    result["manifest_in_flight"] = stats.manifest_in_flight;
    result["manifest_pending"] = stats.manifest_pending;
    return result;
}

static boost::json::object
node_cache_json(catl::peer_client::NodeCache::Stats const& stats)
{
    boost::json::object result;
    result["entries"] = stats.entries;
    result["resident_entries"] = stats.resident_entries;
    result["header_entries"] = stats.header_entries;
    result["max_entries"] = stats.max_entries;
    result["hits"] = stats.hits;
    result["misses"] = stats.misses;
    result["fetches"] = stats.fetches;
    result["fetch_errors"] = stats.fetch_errors;
    result["managed_transport_selected"] = stats.managed_transport_selected;
    result["raw_fetches_projected"] = stats.raw_fetches_projected;
    result["raw_writes_confirmed"] = stats.raw_writes_confirmed;
    result["raw_not_written"] = stats.raw_not_written;
    result["managed_fetches_reserved"] = stats.managed_fetches_reserved;
    result["managed_fetches_refused"] = stats.managed_fetches_refused;
    result["managed_writes_confirmed"] = stats.managed_writes_confirmed;
    result["managed_not_written"] = stats.managed_not_written;
    result["managed_indeterminate"] = stats.managed_indeterminate;
    result["hash_mismatches"] = stats.hash_mismatches;
    result["waiter_wakeups"] = stats.waiter_wakeups;
    return result;
}

static boost::json::object
ledger_fetch_coordinator_json(ProofEngine::Status::Coordinator const& status)
{
    boost::json::object result;
    result["enabled"] = status.enabled;
    char const* lifecycle = "disabled";
    if (status.enabled)
    {
        switch (status.lifecycle)
        {
            case catl::peer_client::LedgerFetchCoordinator::Lifecycle::running:
                lifecycle = "running";
                break;
            case catl::peer_client::LedgerFetchCoordinator::Lifecycle::fencing:
                lifecycle = "fencing";
                break;
            case catl::peer_client::LedgerFetchCoordinator::Lifecycle::fenced:
                lifecycle = "fenced";
                break;
        }
    }
    result["lifecycle"] = lifecycle;
    result["ingress_sealed"] = status.ingress_sealed;
    result["work_outstanding"] = status.work_outstanding;
    result["outstanding_bind_projections"] =
        status.outstanding_bind_projections;
    result["receipt_obligations"] = status.receipt_obligations;
    result["accounting_broken"] = status.accounting_broken;
    return result;
}

static std::pair<std::string, std::string>
split_target(std::string_view target)
{
    auto q = target.find('?');
    if (q == std::string_view::npos)
        return {std::string(target), {}};
    return {
        std::string(target.substr(0, q)), std::string(target.substr(q + 1))};
}

static std::map<std::string, std::string>
parse_query(std::string_view qs)
{
    std::map<std::string, std::string> params;
    while (!qs.empty())
    {
        auto amp = qs.find('&');
        auto pair = qs.substr(0, amp);
        auto eq = pair.find('=');
        if (eq != std::string_view::npos)
        {
            params[std::string(pair.substr(0, eq))] =
                std::string(pair.substr(eq + 1));
        }
        if (amp == std::string_view::npos)
            break;
        qs = qs.substr(amp + 1);
    }
    return params;
}

static std::string
to_lower_copy(std::string_view s)
{
    std::string out(s);
    for (auto& c : out)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return out;
}

static bool
is_hex_64(std::string_view s)
{
    if (s.size() != 64)
        return false;
    for (unsigned char c : s)
    {
        if (!std::isxdigit(c))
            return false;
    }
    return true;
}

static std::string
network_slug_for_id(uint32_t network_id)
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

struct NetworkTxRoute
{
    std::string network;
    std::string tx_hash;
};

static std::optional<NetworkTxRoute>
parse_network_tx_route(std::string_view path)
{
    static constexpr std::string_view prefix = "/network/";
    static constexpr std::string_view tx_prefix = "tx/";

    if (!path.starts_with(prefix))
        return std::nullopt;

    auto rest = path.substr(prefix.size());
    auto slash = rest.find('/');
    if (slash == std::string_view::npos)
        return std::nullopt;

    auto network = rest.substr(0, slash);
    rest = rest.substr(slash + 1);
    if (!rest.starts_with(tx_prefix))
        return std::nullopt;

    auto tx_hash = rest.substr(tx_prefix.size());
    if (!tx_hash.empty() && tx_hash.back() == '/')
        tx_hash.remove_suffix(1);

    if (network.empty() || !is_hex_64(tx_hash))
        return std::nullopt;

    return NetworkTxRoute{to_lower_copy(network), std::string(tx_hash)};
}

static bool
route_matches_network(std::string_view route_network, uint32_t network_id)
{
    auto const configured_slug = network_slug_for_id(network_id);
    auto const numeric_id = std::to_string(network_id);
    auto const token = to_lower_copy(route_network);
    return token == configured_slug || token == numeric_id;
}

/// Parse /network/{slug}/... routes. Returns {slug, rest_of_path} or nullopt.
static std::optional<std::pair<std::string, std::string>>
parse_network_prefix(std::string_view path)
{
    static constexpr std::string_view prefix = "/network/";
    if (!path.starts_with(prefix))
        return std::nullopt;

    auto rest = path.substr(prefix.size());
    auto slash = rest.find('/');
    if (slash == std::string_view::npos)
    {
        // /network/{slug} with no trailing path — treat rest as the route
        return std::pair{std::string(rest), std::string("/")};
    }

    auto slug = rest.substr(0, slash);
    auto sub_path = rest.substr(slash);  // includes leading /
    return std::pair{std::string(slug), std::string(sub_path)};
}

static http::response<http::string_body>
json_response(
    unsigned status,
    boost::json::object const& body,
    unsigned version,
    bool keep_alive)
{
    http::response<http::string_body> res{
        static_cast<http::status>(status), version};
    res.set(http::field::content_type, "application/json");
    res.set(http::field::server, "xprv");
    if (!g_xprv_header.empty())
        res.set("X-XPRV", g_xprv_header);
    res.keep_alive(keep_alive);
    res.body() = boost::json::serialize(body);
    res.prepare_payload();
    return res;
}

static std::string_view
mime_for_extension(std::string_view ext)
{
    if (ext == ".html")
        return "text/html; charset=utf-8";
    if (ext == ".js")
        return "application/javascript; charset=utf-8";
    if (ext == ".css")
        return "text/css; charset=utf-8";
    if (ext == ".svg")
        return "image/svg+xml";
    if (ext == ".json")
        return "application/json";
    if (ext == ".png")
        return "image/png";
    if (ext == ".ico")
        return "image/x-icon";
    if (ext == ".py")
        return "text/x-python; charset=utf-8";
    return "application/octet-stream";
}

/// Map a URL path to an embedded web asset name.
/// "/" → "index", "/foo.js" → "foo", "/verify-xprv.py" → "verify_xprv"
static std::string
asset_name_for_path(std::string_view path)
{
    // Strip leading /
    if (!path.empty() && path[0] == '/')
        path = path.substr(1);
    if (path.empty())
        return "index";

    // Extract stem: "foo.js" → "foo", "verify-xprv.py" → "verify-xprv"
    auto dot = path.rfind('.');
    auto stem = (dot != std::string_view::npos) ? path.substr(0, dot) : path;

    // Normalize: replace - and . with _
    std::string name(stem);
    for (auto& c : name)
    {
        if (c == '-' || c == '.')
            c = '_';
    }
    return name;
}

/// Try to serve a static file: dev mode (disk) first, then embedded.
/// Returns true if served, false if not found.
///
/// SECURITY: `path` is attacker-controlled. The dev-mode disk branch is
/// guarded by resolve_static_path() (see static-path.h), which rejects NUL/
/// ".." and enforces weakly_canonical containment within the static root so
/// a request like "//etc/passwd" can't escape to an absolute path. The
/// resolution logic is factored into that header so it can be unit-tested.
static bool
try_serve_static(
    std::string_view path,
    std::string& out_body,
    std::string& out_content_type)
{
    namespace fs = std::filesystem;

    // Dev mode: serve from disk only if the path resolves to a regular file
    // contained within the static root. In production the dev tree is
    // absent, so this returns nullopt and we fall through to embedded assets.
    if (auto resolved =
            resolve_static_path(path, fs::path("src/xprv/web/static")))
    {
        std::ifstream f(*resolved, std::ios::binary);
        out_body.assign(
            std::istreambuf_iterator<char>(f),
            std::istreambuf_iterator<char>());
        out_content_type =
            std::string(mime_for_extension(resolved->extension().string()));
        return true;
    }

    // Embedded: look up by normalized name (fixed allowlist).
    auto name = asset_name_for_path(path);

    // Check known embedded assets
    if (name == "index")
    {
        out_body = std::string(web::index);
        out_content_type = "text/html; charset=utf-8";
        return true;
    }
    if (name == "verify_xprv")
    {
        out_body = std::string(web::verify_xprv);
        out_content_type = "text/x-python; charset=utf-8";
        return true;
    }

    return false;
}

static http::response<http::string_body>
error_response(
    unsigned status,
    std::string const& message,
    unsigned version,
    bool keep_alive)
{
    return json_response(status, {{"error", message}}, version, keep_alive);
}

// ═══════════════════════════════════════════════════════════════════════
// Lifecycle
// ═══════════════════════════════════════════════════════════════════════

HttpServer::HttpServer(
    asio::io_context& io,
    std::map<uint32_t, std::shared_ptr<ProofEngine>> engines,
    uint32_t default_network_id,
    HttpServerOptions opts)
    : io_(io)
    , engines_(std::move(engines))
    , default_network_id_(default_network_id)
    , opts_(std::move(opts))
    , control_strand_(asio::make_strand(io))
    , acceptor_(control_strand_)
    , request_drain_authority_(std::make_shared<RequestDrainAuthority>())
    , lifecycle_(std::make_shared<LifecycleState>(control_strand_))
{
    for (auto const& [_, engine] : engines_)
        engine->bind_request_drain_authority(request_drain_authority_);

    // Build X-XPRV header value once (stable for this process lifetime)
    xprv_header_ = "format_version=" + std::to_string(XPRV_FORMAT_VERSION) +
        "&build_id=" + (opts_.build_id.empty() ? "dev" : opts_.build_id) +
        "&run_id=" + Logger::get_run_id();
    g_xprv_header = xprv_header_;
}

std::shared_ptr<HttpServer>
HttpServer::create(
    asio::io_context& io,
    std::shared_ptr<ProofEngine> engine,
    HttpServerOptions opts)
{
    auto network_id = engine->config().network_id;
    std::map<uint32_t, std::shared_ptr<ProofEngine>> engines;
    engines[network_id] = std::move(engine);
    return std::shared_ptr<HttpServer>(
        new HttpServer(io, std::move(engines), network_id, std::move(opts)));
}

std::shared_ptr<HttpServer>
HttpServer::create(
    asio::io_context& io,
    std::map<uint32_t, std::shared_ptr<ProofEngine>> engines,
    uint32_t default_network_id,
    HttpServerOptions opts)
{
    return std::shared_ptr<HttpServer>(new HttpServer(
        io, std::move(engines), default_network_id, std::move(opts)));
}

void
HttpServer::start()
{
    std::lock_guard entry_lock(lifecycle_entry_mutex_);
    if (start_claimed_)
        throw std::logic_error("HttpServer::start may be called only once");
    start_claimed_ = true;

    try
    {
        auto endpoint = tcp::endpoint(
            asio::ip::make_address(opts_.bind_address), opts_.port);

        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(asio::socket_base::reuse_address(true));
        acceptor_.bind(endpoint);
        acceptor_.listen(asio::socket_base::max_listen_connections);

        // The service is intentionally one-shot, matching ProofEngine. Publish
        // the lifecycle and authority together while the entry mutex excludes
        // co_stop(); a failed co_spawn is rolled back before that mutex opens.
        lifecycle_->phase = LifecycleState::Phase::accepting;
        lifecycle_->accept_loop_done = false;
        lifecycle_->epoch = 1;
        request_drain_authority_->current_epoch_.store(
            lifecycle_->epoch, std::memory_order_release);
        stopping_.store(false, std::memory_order_release);
        start_committed_ = true;

        auto self = shared_from_this();
        asio::co_spawn(
            control_strand_,
            [self]() { return self->accept_loop(); },
            asio::detached);
    }
    catch (...)
    {
        boost::system::error_code ignored;
        acceptor_.cancel(ignored);
        acceptor_.close(ignored);
        lifecycle_->phase = LifecycleState::Phase::stopped;
        lifecycle_->accept_loop_done = true;
        lifecycle_->epoch = 0;
        request_drain_authority_->current_epoch_.store(
            0, std::memory_order_release);
        start_committed_ = false;
        throw;
    }

    try
    {
        PLOGI(
            log_,
            "Listening on ",
            opts_.bind_address,
            ":",
            opts_.port,
            " (",
            engines_.size(),
            " network",
            (engines_.size() != 1 ? "s" : ""),
            ")");
    }
    catch (...)
    {
    }
}

void
HttpServer::stop()
{
    auto self = shared_from_this();
    asio::co_spawn(
        io_,
        [self]() -> asio::awaitable<void> {
            try
            {
                (void)co_await self->co_stop();
            }
            catch (std::exception const& e)
            {
                PLOGE(log_, "HTTP stop failed: ", e.what());
            }
        },
        asio::detached);
}

//@@authority transition=prv.T0077 status=provisional scope=function
// bind=xprv::co_stop@4e4006c72eb275c1
// owner=unresolved owner_hypothesis=http_server commit=http_control_strand
// touches=prv.S0030,prv.S0031,prv.S0032,prv.S0033
// label=join_accept_loop_and_all_admitted_http_sessions
asio::awaitable<RequestDrainReceipt>
HttpServer::co_stop()
{
    auto self = shared_from_this();
    auto const epoch = co_await asio::co_spawn(
        control_strand_,
        [self]() -> asio::awaitable<std::uint64_t> {
            {
                // Serialize the stop entry with the synchronous one-shot start
                // transaction. Once this lock is released, start_claimed_
                // prevents any later admission epoch from being created.
                std::lock_guard entry_lock(self->lifecycle_entry_mutex_);
                if (!self->start_committed_)
                    throw std::logic_error(
                        "HttpServer::co_stop requires a committed start");
            }

            // The service drain is shared lifecycle work. Cancellation of one
            // caller must not repeatedly abort the shared stop latch and starve
            // the child receipts which make that latch true.
            co_await asio::this_coro::reset_cancellation_state(
                asio::disable_cancellation());
            self->begin_stop_on_control();
            while (self->lifecycle_->phase != LifecycleState::Phase::stopped)
            {
                self->try_complete_stop_on_control();
                if (self->lifecycle_->phase == LifecycleState::Phase::stopped)
                    break;

                boost::system::error_code ec;
                co_await self->lifecycle_->stop_progress.async_wait(
                    asio::redirect_error(asio::use_awaitable, ec));
            }
            co_return self->lifecycle_->epoch;
        },
        asio::use_awaitable);
    co_return RequestDrainReceipt{request_drain_authority_, epoch};
}

//@@authority transition=prv.T0078 status=provisional scope=function
// bind=xprv::begin_stop_on_control@ad06dde12bcf83ef
// owner=unresolved owner_hypothesis=http_server commit=http_control_strand
// touches=prv.S0030,prv.S0032 label=seal_http_admission_and_cancel_sessions
void
HttpServer::begin_stop_on_control()
{
    assert(control_strand_.running_in_this_thread());
    if (lifecycle_->phase == LifecycleState::Phase::stopped)
        return;

    if (lifecycle_->phase == LifecycleState::Phase::accepting)
    {
        lifecycle_->phase = LifecycleState::Phase::stopping;
        stopping_.store(true, std::memory_order_release);

        boost::system::error_code ec;
        acceptor_.cancel(ec);
        acceptor_.close(ec);
    }

    for (auto const& [_, session] : lifecycle_->sessions)
    {
        session->request_stop();
    }
    try_complete_stop_on_control();

    try
    {
        PLOGI(log_, "Stopped accepting connections");
    }
    catch (...)
    {
    }
}

//@@authority transition=prv.T0079 status=provisional scope=function
// bind=xprv::note_session_finished@6f0b18ff7313b3e0
// owner=unresolved owner_hypothesis=http_server commit=http_control_strand
// touches=prv.S0032,prv.S0033 label=consume_exact_http_session_receipt
void
HttpServer::note_session_finished(
    std::uint64_t session_id,
    std::exception_ptr failure) noexcept
{
    auto self = shared_from_this();
    try
    {
        asio::dispatch(
            control_strand_,
            [self, session_id, failure = std::move(failure)]() {
                // Consume the exact child and wake stop before diagnostics.
                // A logging allocation must never make the receipt unreachable.
                self->lifecycle_->sessions.erase(session_id);
                self->try_complete_stop_on_control();

                if (!failure)
                    return;

                try
                {
                    std::rethrow_exception(failure);
                }
                catch (std::exception const& e)
                {
                    try
                    {
                        PLOGE(log_, "HTTP session failed: ", e.what());
                    }
                    catch (...)
                    {
                    }
                }
                catch (...)
                {
                    try
                    {
                        PLOGE(log_, "HTTP session failed");
                    }
                    catch (...)
                    {
                    }
                }
            });
    }
    catch (...)
    {
        // dispatch() itself may allocate. There is no legal off-strand census
        // mutation; fail closed by terminating instead of fabricating a drain
        // receipt with an unconsumed session row.
        std::terminate();
    }
}

//@@authority transition=prv.T0080 status=provisional scope=function
// bind=xprv::try_complete_stop_on_control@53f74c8ca9ed8818
// owner=unresolved owner_hypothesis=http_server commit=http_control_strand
// touches=prv.S0030,prv.S0031,prv.S0032,prv.S0033
// label=commit_http_stopped_when_accept_and_session_children_empty
void
HttpServer::try_complete_stop_on_control() noexcept
{
    assert(control_strand_.running_in_this_thread());
    if (lifecycle_->phase != LifecycleState::Phase::stopping ||
        !lifecycle_->accept_loop_done || !lifecycle_->sessions.empty())
    {
        return;
    }

    lifecycle_->phase = LifecycleState::Phase::stopped;
    try
    {
        lifecycle_->stop_progress.cancel();
    }
    catch (...)
    {
    }
    try
    {
        PLOGI(log_, "HTTP sessions drained");
    }
    catch (...)
    {
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Engine lookup helpers
// ═══════════════════════════════════════════════════════════════════════

ProofEngine*
HttpServer::engine_for(uint32_t network_id) const
{
    auto it = engines_.find(network_id);
    return it != engines_.end() ? it->second.get() : nullptr;
}

std::pair<uint32_t, ProofEngine*>
HttpServer::engine_for_slug(std::string_view slug) const
{
    for (auto const& [id, engine] : engines_)
    {
        if (route_matches_network(slug, id))
            return {id, engine.get()};
    }
    return {0, nullptr};
}

//@@authority transition=prv.T0081 status=provisional scope=function
// bind=xprv::lookup_tx_all@b1451890f88b43d5
// owner=unresolved owner_hypothesis=http_session commit=session_executor
// label=dynamic_join_all_network_lookup_children
asio::awaitable<std::pair<uint32_t, uint32_t>>
HttpServer::lookup_tx_all(std::string const& tx_hash)
{
    // Single engine — skip the parallel dispatch overhead
    if (engines_.size() == 1)
    {
        auto& [net_id, engine] = *engines_.begin();
        auto seq = co_await engine->lookup_tx(tx_hash);
        co_return std::pair{seq ? net_id : 0u, seq};
    }

    auto ex = co_await asio::this_coro::executor;
    struct LookupChildResult
    {
        uint32_t network_id = 0;
        uint32_t ledger_seq = 0;
        std::exception_ptr failure;
    };
    using LookupChannel = asio::experimental::concurrent_channel<void(
        boost::system::error_code, LookupChildResult)>;

    auto completions = std::make_shared<LookupChannel>(ex, engines_.size());
    std::vector<std::shared_ptr<asio::cancellation_signal>> child_cancellations;
    child_cancellations.reserve(engines_.size());
    std::size_t launched = 0;
    std::exception_ptr launch_failure;
    for (auto const& [net_id, engine] : engines_)
    {
        try
        {
            auto cancellation = std::make_shared<asio::cancellation_signal>();
            child_cancellations.push_back(cancellation);
            asio::co_spawn(
                ex,
                [engine, tx_hash]() -> asio::awaitable<uint32_t> {
                    co_return co_await engine->lookup_tx(tx_hash);
                },
                asio::bind_cancellation_slot(
                    cancellation->slot(),
                    [completions, net_id](
                        std::exception_ptr failure, uint32_t ledger_seq) {
                        completions->try_send(
                            boost::system::error_code{},
                            LookupChildResult{
                                .network_id = net_id,
                                .ledger_seq = ledger_seq,
                                .failure = std::move(failure)});
                    }));
            ++launched;
        }
        catch (...)
        {
            launch_failure = std::current_exception();
            break;
        }
    }

    bool canceled = false;
    if (launch_failure)
    {
        for (auto const& cancellation : child_cancellations)
            cancellation->emit(asio::cancellation_type::all);
    }

    uint32_t result_net = 0;
    uint32_t result_seq = 0;
    std::exception_ptr child_failure;
    std::size_t received = 0;
    while (received < launched)
    {
        boost::system::error_code receive_ec;
        auto result = co_await completions->async_receive(
            asio::redirect_error(asio::use_awaitable, receive_ec));
        if (receive_ec)
        {
            if (receive_ec != asio::error::operation_aborted)
                throw boost::system::system_error(receive_ec);

            canceled = true;
            for (auto const& cancellation : child_cancellations)
                cancellation->emit(asio::cancellation_type::all);
            co_await asio::this_coro::reset_cancellation_state(
                asio::disable_cancellation());
            continue;
        }

        ++received;
        if (result.failure && !child_failure)
            child_failure = std::move(result.failure);
        if (result_seq == 0 && result.ledger_seq != 0)
        {
            result_net = result.network_id;
            result_seq = result.ledger_seq;
        }
    }

    PLOGD(
        log_,
        "lookup_tx_all: net=",
        result_net,
        " seq=",
        result_seq,
        " children=",
        received);

    if (launch_failure)
        std::rethrow_exception(launch_failure);
    if (canceled)
        throw boost::system::system_error(asio::error::operation_aborted);
    if (result_seq == 0 && child_failure)
        std::rethrow_exception(child_failure);
    co_return std::pair{result_net, result_seq};
}

// ═══════════════════════════════════════════════════════════════════════
// Accept loop
// ═══════════════════════════════════════════════════════════════════════

//@@authority transition=prv.T0082 status=provisional scope=function
// bind=xprv::accept_loop@f720319d06f9e7fa
// owner=unresolved owner_hypothesis=http_server commit=http_control_strand
// touches=prv.S0030,prv.S0031,prv.S0032
// label=admit_http_session_or_receipt_late_accept
asio::awaitable<void>
HttpServer::accept_loop()
{
    auto self = shared_from_this();

    std::exception_ptr failure;
    try
    {
        while (lifecycle_->phase == LifecycleState::Phase::accepting)
        {
            boost::system::error_code ec;
            // Per-connection strand (security #0059). beast::tcp_stream
            // implements its expires_after timeout by keeping the socket op AND
            // an internal deadline timer outstanding at once; on a multi-thread
            // io_context (the deploy runs --threads 2) their completions can
            // land on two threads and race basic_stream's internal state — UB.
            // Accepting onto this strand binds the socket → tcp_stream → its
            // timer to one strand, and spawning the session on it (below) makes
            // the inner co_spawns that inherit
            // stream.get_executor()/this_coro::executor serialize too.
            // Empirically verified with a TSan reproducer
            // (~/projects/asio-patterns, beast-stream-strand-tsan): nostrand
            // races, strand is clean.
            auto strand = asio::make_strand(io_);
            boost::asio::ip::tcp::socket socket =
                co_await acceptor_.async_accept(
                    strand, asio::redirect_error(asio::use_awaitable, ec));
            if (ec)
            {
                if (lifecycle_->phase == LifecycleState::Phase::accepting)
                    throw boost::system::system_error(ec);
                break;
            }

            if (lifecycle_->phase != LifecycleState::Phase::accepting)
            {
                auto session =
                    std::make_shared<SessionControl>(std::move(socket));
                co_await session->co_stop();
                continue;
            }

            // Per-connection request context for log prefixes.
            // Covers handle_session and prove coroutine co_awaits;
            // inner strand hops (peer/RPC) won't have it.
            auto req_ctx = std::shared_ptr<catl::core::RequestContext>(
                new catl::core::RequestContext(
                    generate_request_id(),
                    {},  // tx_hash filled later
                    std::chrono::steady_clock::now()));
            // Wrap the per-connection strand so the session coroutine runs on
            // it.
            auto ctx_ex = catl::core::ContextExecutor(strand, req_ctx);
            if (lifecycle_->next_session_id ==
                std::numeric_limits<std::uint64_t>::max())
            {
                PLOGE(log_, "HTTP session identity exhausted");
                auto session =
                    std::make_shared<SessionControl>(std::move(socket));
                co_await session->co_stop();
                continue;
            }
            auto const session_id = ++lifecycle_->next_session_id;
            auto session = std::make_shared<SessionControl>(std::move(socket));
            lifecycle_->sessions.emplace(session_id, session);
            std::exception_ptr spawn_failure;
            try
            {
                asio::co_spawn(
                    ctx_ex,
                    [self, session, req_ctx]() mutable {
                        return self->handle_session(
                            std::move(session), std::move(req_ctx));
                    },
                    asio::bind_cancellation_slot(
                        session->cancellation.slot(),
                        [self, session_id](std::exception_ptr failure) {
                            self->note_session_finished(
                                session_id, std::move(failure));
                        }));
            }
            catch (...)
            {
                spawn_failure = std::current_exception();
            }
            if (spawn_failure)
            {
                lifecycle_->sessions.erase(session_id);
                co_await session->co_stop();
                std::rethrow_exception(spawn_failure);
            }
        }
    }
    catch (...)
    {
        failure = std::current_exception();
    }

    complete_accept_loop(std::move(failure));
}

void
HttpServer::complete_accept_loop(std::exception_ptr failure) noexcept
{
    assert(control_strand_.running_in_this_thread());

    if (failure && lifecycle_->phase == LifecycleState::Phase::accepting)
    {
        try
        {
            begin_stop_on_control();
        }
        catch (...)
        {
            // The accept servant has already terminated. If its owner cannot
            // seal the remaining children, no truthful stopped receipt exists.
            std::terminate();
        }
    }

    // Publish the exact servant receipt and wake the shared stop latch before
    // any diagnostic which may allocate.
    lifecycle_->accept_loop_done = true;
    try_complete_stop_on_control();

    if (!failure)
        return;

    try
    {
        std::rethrow_exception(failure);
    }
    catch (std::exception const& e)
    {
        try
        {
            PLOGE(log_, "HTTP accept loop failed: ", e.what());
        }
        catch (...)
        {
        }
    }
    catch (...)
    {
        try
        {
            PLOGE(log_, "HTTP accept loop failed");
        }
        catch (...)
        {
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Session handler
// ═══════════════════════════════════════════════════════════════════════

//@@authority transition=prv.T0083 status=provisional scope=function
// bind=xprv::handle_session@d325a77423727e44
// owner=unresolved owner_hypothesis=http_session commit=http_session_strand
// touches=prv.S0027,prv.S0028,prv.S0029
// label=own_request_and_sse_child_frames_until_session_terminal
asio::awaitable<void>
HttpServer::handle_session(
    std::shared_ptr<SessionControl> session,
    std::shared_ptr<catl::core::RequestContext> req_ctx)
{
    auto& stream = session->stream;
    beast::flat_buffer buffer;

    // Helper: get the default engine (for single-network backwards compat)
    auto default_engine = [this]() -> ProofEngine* {
        return engine_for(default_network_id_);
    };

    for (;;)
    {
        if (is_stopping())
            break;

        // Read request with timeout and body limit
        stream.expires_after(opts_.read_timeout);

        http::request_parser<http::vector_body<uint8_t>> parser;
        parser.body_limit(opts_.max_request_body);

        boost::system::error_code ec;
        co_await http::async_read(
            stream,
            buffer,
            parser,
            asio::redirect_error(asio::use_awaitable, ec));

        if (ec)
        {
            // Return proper HTTP error for body limit exceeded
            if (ec == http::error::body_limit)
            {
                stream.expires_after(opts_.write_timeout);
                co_await http::async_write(
                    stream,
                    error_response(413, "request body too large", 11, false),
                    asio::redirect_error(asio::use_awaitable, ec));
            }
            break;
        }

        auto& req = parser.get();
        auto const version = req.version();
        auto const keep_alive = req.keep_alive();

        auto [path, query_string] = split_target(req.target());
        auto params = parse_query(query_string);

        // Fresh request ID per HTTP request (not per connection).
        // Priority: X-Request-Id header > xrid query param > generated.
        req_ctx->request_id = generate_request_id();
        req_ctx->started_at = std::chrono::steady_clock::now();
        auto hdr = req["X-Request-Id"];
        if (hdr.empty())
        {
            if (auto it = params.find("xrid"); it != params.end())
                hdr = it->second;
        }
        if (!hdr.empty())
        {
            // Sanitize: alphanumeric + dash/underscore, max 64 chars
            std::string safe;
            safe.reserve(std::min(hdr.size(), std::size_t(64)));
            for (char c : hdr)
            {
                if (safe.size() >= 64)
                    break;
                if (std::isalnum(static_cast<unsigned char>(c)) || c == '-' ||
                    c == '_')
                    safe += c;
            }
            if (!safe.empty())
                req_ctx->request_id = std::move(safe);
        }

        // One immutable authority identity and absolute deadline per HTTP
        // request. It remains observational in Cut 3; Cut 4 binds participant
        // grants and all peer work to it.
        auto request_scope = catl::core::RequestScope::create(
            req_ctx->started_at + opts_.request_work_timeout);
        if (!request_scope)
        {
            stream.expires_after(opts_.write_timeout);
            co_await http::async_write(
                stream,
                error_response(
                    503, "request authority unavailable", version, keep_alive),
                asio::redirect_error(asio::use_awaitable, ec));
            if (ec || !keep_alive)
                break;
            continue;
        }

        PLOGD(log_, req.method_string(), " ", path);
        ++total_requests_;

        // ── Check for /network/{slug}/... prefix routing ──────────
        ProofEngine* routed_engine = nullptr;
        uint32_t routed_network_id = 0;
        std::string effective_path = path;

        auto net_prefix = parse_network_prefix(path);
        if (net_prefix)
        {
            auto [slug, sub_path] = *net_prefix;
            auto [net_id, eng] = engine_for_slug(slug);
            if (!eng)
            {
                // Unknown network slug — but might be a deep link for the web
                // UI Check if this is a /network/{slug}/tx/{hash} route
                auto route = parse_network_tx_route(path);
                if (route && req.method() == http::verb::get)
                {
                    // Deep link — serve index.html for any known network
                    bool any_match = false;
                    for (auto const& [id, _] : engines_)
                    {
                        if (route_matches_network(route->network, id))
                        {
                            any_match = true;
                            break;
                        }
                    }
                    if (any_match)
                    {
                        std::string static_body, content_type;
                        if (try_serve_static("/", static_body, content_type))
                        {
                            http::response<http::string_body> res{
                                http::status::ok, version};
                            res.set(http::field::content_type, content_type);
                            res.set(http::field::server, "xprv");
                            if (!g_xprv_header.empty())
                                res.set("X-XPRV", g_xprv_header);
                            res.keep_alive(keep_alive);
                            res.body() = std::move(static_body);
                            res.prepare_payload();
                            stream.expires_after(opts_.write_timeout);
                            co_await http::async_write(
                                stream,
                                res,
                                asio::redirect_error(asio::use_awaitable, ec));
                            if (ec || !keep_alive)
                                break;
                            continue;
                        }
                    }
                }

                stream.expires_after(opts_.write_timeout);
                co_await http::async_write(
                    stream,
                    error_response(
                        404, "unknown network: " + slug, version, keep_alive),
                    asio::redirect_error(asio::use_awaitable, ec));
                if (ec || !keep_alive)
                    break;
                continue;
            }
            routed_engine = eng;
            routed_network_id = net_id;
            effective_path = sub_path;
        }

        // ── Route dispatch ────────────────────────────────────────

        std::optional<http::response<http::string_body>> caught_error;
        std::string sse_error_msg;  // set in catch for SSE tx-not-found
        try
        {
            if (effective_path == "/health" && req.method() == http::verb::get)
            {
                if (routed_engine)
                {
                    // Single network health
                    auto status = co_await routed_engine->co_health();
                    auto cs = routed_engine->cache_stats();
                    boost::json::object body;
                    body["network_id"] = routed_network_id;
                    body["network"] = network_slug_for_id(routed_network_id);
                    body["peer_count"] = status.peer_count;
                    body["total_connects"] = status.total_connects;
                    body["total_disconnects"] = status.total_disconnects;
                    body["vl_loaded"] = status.vl_loaded;
                    if (status.latest_quorum_seq)
                        body["latest_quorum_seq"] = *status.latest_quorum_seq;
                    body["validation_quorum_tracker"] =
                        validation_quorum_tracker_json(
                            status.validation_quorum_tracker);

                    boost::json::object cache;
                    cache["entries"] = cs.entries;
                    cache["max_entries"] = cs.max_entries;
                    cache["hits"] = cs.hits;
                    cache["misses"] = cs.misses;
                    body["proof_cache"] = cache;

                    body["node_cache"] =
                        node_cache_json(routed_engine->node_cache_stats());
                    body["ledger_fetch_coordinator"] =
                        ledger_fetch_coordinator_json(
                            status.ledger_fetch_coordinator);

                    stream.expires_after(opts_.write_timeout);
                    co_await http::async_write(
                        stream,
                        json_response(200, body, version, keep_alive),
                        asio::redirect_error(asio::use_awaitable, ec));
                }
                else
                {
                    // Aggregate health across all engines
                    boost::json::object body;
                    boost::json::object networks;

                    for (auto const& [net_id, engine] : engines_)
                    {
                        auto status = co_await engine->co_health();
                        auto cs = engine->cache_stats();
                        boost::json::object net_body;
                        net_body["network_id"] = net_id;
                        net_body["peer_count"] = status.peer_count;
                        net_body["total_connects"] = status.total_connects;
                        net_body["total_disconnects"] =
                            status.total_disconnects;
                        net_body["vl_loaded"] = status.vl_loaded;
                        if (status.latest_quorum_seq)
                            net_body["latest_quorum_seq"] =
                                *status.latest_quorum_seq;
                        net_body["validation_quorum_tracker"] =
                            validation_quorum_tracker_json(
                                status.validation_quorum_tracker);

                        boost::json::object cache;
                        cache["entries"] = cs.entries;
                        cache["max_entries"] = cs.max_entries;
                        cache["hits"] = cs.hits;
                        cache["misses"] = cs.misses;
                        net_body["proof_cache"] = cache;

                        net_body["node_cache"] =
                            node_cache_json(engine->node_cache_stats());
                        net_body["ledger_fetch_coordinator"] =
                            ledger_fetch_coordinator_json(
                                status.ledger_fetch_coordinator);

                        networks[network_slug_for_id(net_id)] =
                            std::move(net_body);
                    }

                    body["networks"] = std::move(networks);
                    body["run_id"] = Logger::get_run_id();
                    if (!opts_.build_id.empty())
                        body["build_id"] = opts_.build_id;

                    // Request stats
                    boost::json::object req_stats;
                    req_stats["total_requests"] = total_requests_.load();
                    req_stats["total_proofs"] = total_proofs_.load();
                    req_stats["active_proofs"] = active_proofs_.load();
                    body["requests"] = std::move(req_stats);

                    // Allocator stats (glibc only)
#ifdef __GLIBC__
                    {
                        auto mi = mallinfo2();
                        boost::json::object alloc;
                        alloc["arena_bytes"] = static_cast<int64_t>(mi.arena);
                        alloc["mmap_bytes"] = static_cast<int64_t>(mi.hblkhd);
                        alloc["used_bytes"] = static_cast<int64_t>(mi.uordblks);
                        alloc["free_bytes"] = static_cast<int64_t>(mi.fordblks);
                        body["allocator"] = std::move(alloc);
                    }
#endif

                    // For backwards compat with single-engine mode,
                    // also include top-level fields from the default engine
                    if (engines_.size() == 1)
                    {
                        auto& engine = engines_.begin()->second;
                        auto status = co_await engine->co_health();
                        auto cs = engine->cache_stats();
                        body["peer_count"] = status.peer_count;
                        body["total_connects"] = status.total_connects;
                        body["total_disconnects"] = status.total_disconnects;
                        body["vl_loaded"] = status.vl_loaded;
                        if (status.latest_quorum_seq)
                            body["latest_quorum_seq"] =
                                *status.latest_quorum_seq;
                        body["validation_quorum_tracker"] =
                            validation_quorum_tracker_json(
                                status.validation_quorum_tracker);

                        boost::json::object cache;
                        cache["entries"] = cs.entries;
                        cache["max_entries"] = cs.max_entries;
                        cache["hits"] = cs.hits;
                        cache["misses"] = cs.misses;
                        body["proof_cache"] = cache;

                        body["node_cache"] =
                            node_cache_json(engine->node_cache_stats());
                        body["ledger_fetch_coordinator"] =
                            ledger_fetch_coordinator_json(
                                status.ledger_fetch_coordinator);
                    }

                    stream.expires_after(opts_.write_timeout);
                    co_await http::async_write(
                        stream,
                        json_response(200, body, version, keep_alive),
                        asio::redirect_error(asio::use_awaitable, ec));
                }
            }
            else if (
                effective_path == "/peers" && req.method() == http::verb::get)
            {
                auto const filters = parse_peer_snapshot_filters(params);

                boost::json::object body;
                if (routed_engine || engines_.size() == 1)
                {
                    auto* engine =
                        routed_engine ? routed_engine : default_engine();
                    auto snapshot = co_await engine->peers()->co_snapshot();
                    body = peer_snapshot_to_json(
                        snapshot,
                        routed_engine
                            ? std::optional<uint32_t>(routed_network_id)
                            : std::optional<uint32_t>(),
                        filters,
                        true);
                }
                else
                {
                    if (!filters.empty())
                    {
                        boost::json::object filter_body;
                        if (filters.connected.has_value())
                            filter_body["connected"] = *filters.connected;
                        body["filters"] = std::move(filter_body);
                    }

                    boost::json::array networks;
                    for (auto const& [network_id, engine] : engines_)
                    {
                        auto snapshot = co_await engine->peers()->co_snapshot();
                        networks.push_back(peer_snapshot_to_json(
                            snapshot, network_id, filters, false));
                    }
                    body["networks"] = std::move(networks);
                    body["run_id"] = Logger::get_run_id();
                    if (!opts_.build_id.empty())
                        body["build_id"] = opts_.build_id;
                }

                stream.expires_after(opts_.write_timeout);
                co_await http::async_write(
                    stream,
                    json_response(200, body, version, keep_alive),
                    asio::redirect_error(asio::use_awaitable, ec));
            }
            else if (
                effective_path == "/prove" && req.method() == http::verb::get)
            {
                ++total_proofs_;
                ++active_proofs_;
                auto proof_guard = [this] { --active_proofs_; };
                struct ProofGuard
                {
                    std::function<void()> fn;
                    ~ProofGuard()
                    {
                        if (fn)
                            fn();
                    }
                } proof_scope{std::move(proof_guard)};

                auto tx_it = params.find("tx");
                if (tx_it == params.end() || tx_it->second.empty())
                {
                    stream.expires_after(opts_.write_timeout);
                    co_await http::async_write(
                        stream,
                        error_response(
                            400, "missing tx parameter", version, keep_alive),
                        asio::redirect_error(asio::use_awaitable, ec));
                }
                else
                {
                    // Determine which engine handles this tx
                    ProofEngine* prove_engine = nullptr;
                    uint32_t ledger_seq_hint = 0;

                    // Optional ledger_index hint from query
                    auto li_it = params.find("ledger_index");
                    if (li_it != params.end() && !li_it->second.empty())
                    {
                        try
                        {
                            ledger_seq_hint = std::stoul(li_it->second);
                        }
                        catch (...)
                        {
                        }
                    }

                    // Check if client wants SSE (needed before tx lookup
                    // so not-found errors can be sent as SSE events)
                    auto accept = std::string(req[http::field::accept]);
                    bool wants_sse =
                        accept.find("text/event-stream") != std::string::npos;

                    if (routed_engine)
                    {
                        // Network-specific route — use that engine directly
                        prove_engine = routed_engine;
                    }
                    else if (engines_.size() == 1)
                    {
                        // Single engine — no lookup needed
                        prove_engine = engines_.begin()->second.get();
                    }
                    else
                    {
                        // Multi-engine: look up tx on all networks
                        auto [found_net, found_seq] =
                            co_await lookup_tx_all(tx_it->second);
                        if (found_seq == 0)
                        {
                            throw catl::rpc::RpcTxNotFound(
                                "tx " + tx_it->second.substr(0, 16) +
                                "...: not found on any configured network");
                        }
                        prove_engine = engine_for(found_net);
                        if (ledger_seq_hint == 0)
                            ledger_seq_hint = found_seq;
                    }

                    // Optional max_anchor_age — reuse recent anchor (seconds).
                    // Default 10s — reuse the cached anchor for repeated
                    // requests.
                    uint32_t max_anchor_age = 10;
                    auto ma_it = params.find("max_anchor_age");
                    if (ma_it != params.end() && !ma_it->second.empty())
                    {
                        try
                        {
                            max_anchor_age = std::stoul(ma_it->second);
                        }
                        catch (...)
                        {
                        }
                    }

                    // Cancel token: atomic flag checked at cancellation
                    // boundaries (peer waits and node walks) for fast
                    // exit without disrupting shared NodeCache state.
                    auto cancel_token =
                        std::make_shared<std::atomic<bool>>(false);

                    // Capture engine as shared_ptr for coroutine safety
                    auto engine_sp =
                        engines_.at(prove_engine->config().network_id);

                    if (wants_sse)
                    {
                        PLOGI(log_, "Entering SSE path");
                        // ── SSE path ──
                        // Write raw HTTP response + SSE frames directly.
                        // No chunked encoding — SSE streams are
                        // connection-scoped, closed when done.
                        stream.expires_after(opts_.write_timeout);

                        // Disable Nagle so each write flushes immediately
                        beast::get_lowest_layer(stream).socket().set_option(
                            asio::ip::tcp::no_delay(true));

                        std::string hdr_str =
                            "HTTP/1.1 200 OK\r\n"
                            "Content-Type: text/event-stream\r\n"
                            "Cache-Control: no-cache\r\n"
                            "Connection: close\r\n"
                            "Server: xprv\r\n"
                            "X-XPRV: " +
                            g_xprv_header +
                            "\r\n"
                            "\r\n";
                        co_await asio::async_write(
                            stream,
                            asio::buffer(hdr_str),
                            asio::redirect_error(asio::use_awaitable, ec));
                        PLOGI(log_, "SSE headers sent, ec=", ec.message());
                        if (ec)
                            break;

                        using SseFrame = std::optional<std::string>;
                        using SseChannel =
                            asio::experimental::concurrent_channel<void(
                                boost::system::error_code, SseFrame)>;
                        using CompletionChannel =
                            asio::experimental::concurrent_channel<void(
                                boost::system::error_code)>;

                        auto const sse_executor = stream.get_executor();
                        auto frames =
                            std::make_shared<SseChannel>(sse_executor, 128);
                        auto writer_done = std::make_shared<CompletionChannel>(
                            sse_executor, 1);
                        auto status_done = std::make_shared<CompletionChannel>(
                            sse_executor, 1);
                        auto watcher_done = std::make_shared<CompletionChannel>(
                            sse_executor, 1);
                        auto proof_cancel = std::make_shared<CompletionChannel>(
                            sse_executor, 1);
                        auto writer_failed =
                            std::make_shared<std::atomic<bool>>(false);
                        auto request_proof_cancel = [cancel_token,
                                                     proof_cancel,
                                                     request_scope]() noexcept {
                            request_scope->cancel();
                            cancel_token->store(true);
                            proof_cancel->try_send(boost::system::error_code{});
                        };
                        auto enqueue_sse =
                            [frames](
                                std::string data) -> asio::awaitable<bool> {
                            auto frame = "data: " + std::move(data) + "\n\n";
                            boost::system::error_code qec;
                            co_await frames->async_send(
                                boost::system::error_code{},
                                SseFrame{std::move(frame)},
                                asio::redirect_error(asio::use_awaitable, qec));
                            co_return !qec;
                        };
                        auto stop_watcher =
                            std::make_shared<std::atomic<bool>>(false);
                        auto stop_status =
                            std::make_shared<std::atomic<bool>>(false);
                        auto drain_timer =
                            std::make_shared<asio::steady_timer>(sse_executor);
                        auto cancel_drain_timer = [drain_timer]() noexcept {
                            try
                            {
                                drain_timer->cancel();
                            }
                            catch (...)
                            {
                            }
                        };
                        auto drain_ctx = req_ctx;

                        // One coroutine owns every composed write operation.
                        // Producers only enqueue complete frames, so status and
                        // proof events cannot overlap writes on the stream.
                        std::exception_ptr child_spawn_failure;
                        try
                        {
                            asio::co_spawn(
                                sse_executor,
                                [&stream,
                                 frames,
                                 writer_failed,
                                 request_proof_cancel,
                                 write_timeout = opts_.write_timeout]()
                                    -> asio::awaitable<void> {
                                    try
                                    {
                                        for (;;)
                                        {
                                            auto frame =
                                                co_await frames->async_receive(
                                                    asio::use_awaitable);
                                            if (!frame)
                                                break;
                                            if (writer_failed->load())
                                                continue;

                                            PLOGI(
                                                log_,
                                                "SSE write: ",
                                                frame->size(),
                                                "B");
                                            stream.expires_after(write_timeout);
                                            boost::system::error_code wec;
                                            co_await asio::async_write(
                                                stream,
                                                asio::buffer(*frame),
                                                asio::redirect_error(
                                                    asio::use_awaitable, wec));
                                            if (wec)
                                            {
                                                PLOGD(
                                                    log_,
                                                    "SSE write stopped: ",
                                                    wec.message());
                                                writer_failed->store(true);
                                                request_proof_cancel();
                                            }
                                        }
                                    }
                                    catch (std::exception const& e)
                                    {
                                        PLOGE(
                                            log_,
                                            "SSE writer failed: ",
                                            e.what());
                                        writer_failed->store(true);
                                        request_proof_cancel();
                                        frames->cancel();
                                        frames->close();
                                    }
                                },
                                detail::HttpChildCompletion{
                                    writer_done, [](std::exception_ptr const&) {
                                        PLOGE(log_, "SSE writer terminated");
                                    }});
                        }
                        catch (...)
                        {
                            child_spawn_failure = std::current_exception();
                        }
                        if (child_spawn_failure)
                            std::rethrow_exception(child_spawn_failure);

                        // SSE dedicates the connection to this response. Any
                        // read readiness is only a hint. A proxy may half-close
                        // its request side while still consuming this stream.
                        try
                        {
                            asio::co_spawn(
                                sse_executor,
                                [&stream,
                                 request_proof_cancel,
                                 stop_watcher]() -> asio::awaitable<void> {
                                    auto const failed = co_await HttpServer::
                                        co_wait_for_transport_failure(
                                            beast::get_lowest_layer(stream)
                                                .socket());
                                    if (failed && !stop_watcher->load())
                                        request_proof_cancel();
                                },
                                detail::HttpChildCompletion{
                                    watcher_done,
                                    [](std::exception_ptr const&) {
                                        PLOGE(log_, "SSE watcher terminated");
                                    }});
                        }
                        catch (...)
                        {
                            child_spawn_failure = std::current_exception();
                        }
                        if (child_spawn_failure)
                        {
                            co_await asio::this_coro::reset_cancellation_state(
                                asio::disable_cancellation());
                            if (!frames->try_send(
                                    boost::system::error_code{}, SseFrame{}))
                                frames->cancel();
                            co_await detail::join_http_child(writer_done);
                            std::rethrow_exception(child_spawn_failure);
                        }

                        int step_index = 0;

                        auto on_step = [&](ChainStep const& step)
                            -> asio::awaitable<void> {
                            boost::json::object ev;
                            ev["type"] = "step";
                            ev["index"] = step_index++;
                            ev["step"] = step_to_json(step);
                            if (!(co_await enqueue_sse(
                                    boost::json::serialize(ev))))
                            {
                                request_proof_cancel();
                                throw std::runtime_error("SSE queue stopped");
                            }
                        };

                        try
                        {
                            asio::co_spawn(
                                sse_executor,
                                [stop_status,
                                 drain_timer,
                                 drain_ctx,
                                 enqueue_sse,
                                 request_proof_cancel]()
                                    -> asio::awaitable<void> {
                                    try
                                    {
                                        bool queue_open = true;
                                        while (!stop_status->load() &&
                                               queue_open)
                                        {
                                            drain_timer->expires_after(
                                                std::chrono::milliseconds(300));
                                            boost::system::error_code tec;
                                            co_await drain_timer->async_wait(
                                                asio::redirect_error(
                                                    asio::use_awaitable, tec));
                                            if (stop_status->load())
                                                break;
                                            auto msgs = drain_ctx->drain();
                                            for (auto& msg : msgs)
                                            {
                                                boost::json::object ev;
                                                ev["type"] = "log";
                                                ev["msg"] = std::move(msg);
                                                queue_open =
                                                    co_await enqueue_sse(
                                                        boost::json::serialize(
                                                            ev));
                                                if (!queue_open)
                                                {
                                                    request_proof_cancel();
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    catch (std::exception const& e)
                                    {
                                        PLOGE(
                                            log_,
                                            "SSE status drain failed: ",
                                            e.what());
                                        request_proof_cancel();
                                    }
                                },
                                detail::HttpChildCompletion{
                                    status_done, [](std::exception_ptr const&) {
                                        PLOGE(
                                            log_,
                                            "SSE status drain terminated");
                                    }});
                        }
                        catch (...)
                        {
                            child_spawn_failure = std::current_exception();
                        }
                        if (child_spawn_failure)
                        {
                            co_await asio::this_coro::reset_cancellation_state(
                                asio::disable_cancellation());
                            stop_watcher->store(true);
                            boost::system::error_code cancel_ec;
                            beast::get_lowest_layer(stream).socket().cancel(
                                cancel_ec);
                            co_await detail::join_http_child(watcher_done);

                            if (!frames->try_send(
                                    boost::system::error_code{}, SseFrame{}))
                                frames->cancel();
                            co_await detail::join_http_child(writer_done);
                            std::rethrow_exception(child_spawn_failure);
                        }

                        bool status_joined = false;
                        bool writer_joined = false;
                        bool writer_stop_enqueued = false;
                        std::exception_ptr pipeline_failure;
                        try
                        {
                            std::optional<uint32_t> done_network_id;
                            std::string sse_error;
                            try
                            {
                                //@@authority transition=prv.T0035
                                // bind=prove@45d197729a40e4e4
                                // status=provisional scope=statement
                                // owner=unresolved
                                // owner_hypothesis=request_session
                                // commit=connection_strand calls=prove
                                // label=submit_sse_proof_without_opaque_operation_id
                                // clang-format off
                                auto ex = co_await asio::this_coro::executor; //@@authority-target ids=prv.T0035
                                // clang-format on
                                auto [order, prove_exception, result, cancel_exception] =
                                    co_await asio::experimental::make_parallel_group(
                                        asio::co_spawn(
                                            ex,
                                            engine_sp->prove(
                                                tx_it->second,
                                                cancel_token,
                                                ledger_seq_hint,
                                                max_anchor_age,
                                                std::move(on_step),
                                                request_scope),
                                            asio::deferred),
                                        asio::co_spawn(
                                            ex,
                                            [proof_cancel]()
                                                -> asio::awaitable<void> {
                                                co_await asio::this_coro::
                                                    reset_cancellation_state(
                                                        asio::
                                                            enable_total_cancellation());
                                                boost::system::error_code
                                                    cancel_ec;
                                                co_await proof_cancel
                                                    ->async_receive(
                                                        asio::redirect_error(
                                                            asio::use_awaitable,
                                                            cancel_ec));
                                            },
                                            asio::deferred))
                                        .async_wait(
                                            asio::experimental::wait_for_one(),
                                            asio::use_awaitable);

                                if (order[0] == 0)
                                {
                                    if (prove_exception)
                                        std::rethrow_exception(prove_exception);
                                    done_network_id = result.chain.network_id;
                                }
                                else
                                {
                                    PLOGD(
                                        log_,
                                        "SSE client disconnected during "
                                        "prove");
                                }
                            }
                            catch (std::exception const& e)
                            {
                                if (!cancel_token->load())
                                    sse_error = e.what();
                            }

                            // Join the status producer before the terminal
                            // event so no later log can overtake it.
                            // From this point onward the session owns
                            // structured cleanup. Parent cancellation must not
                            // counterfeit any child receipt.
                            co_await asio::this_coro::reset_cancellation_state(
                                asio::disable_cancellation());
                            stop_status->store(true);
                            cancel_drain_timer();
                            co_await detail::join_http_child(status_done);
                            status_joined = true;

                            auto msgs = req_ctx->drain();
                            for (auto& msg : msgs)
                            {
                                boost::json::object ev;
                                ev["type"] = "log";
                                ev["msg"] = std::move(msg);
                                if (!(co_await enqueue_sse(
                                        boost::json::serialize(ev))))
                                    break;
                            }

                            if (done_network_id)
                            {
                                boost::json::object done;
                                done["type"] = "done";
                                done["network_id"] = *done_network_id;
                                co_await enqueue_sse(
                                    boost::json::serialize(done));
                            }
                            else if (!sse_error.empty())
                            {
                                boost::json::object err;
                                err["type"] = "error";
                                err["error"] = std::move(sse_error);
                                co_await enqueue_sse(
                                    boost::json::serialize(err));
                            }

                            boost::system::error_code stop_ec;
                            co_await frames->async_send(
                                boost::system::error_code{},
                                SseFrame{},
                                asio::redirect_error(
                                    asio::use_awaitable, stop_ec));
                            writer_stop_enqueued = !stop_ec;
                            co_await detail::join_http_child(writer_done);
                            writer_joined = true;
                        }
                        catch (...)
                        {
                            pipeline_failure = std::current_exception();
                            request_scope->cancel();
                            cancel_token->store(true);
                        }

                        // Unconditional asynchronous cleanup: every spawned
                        // coroutine is joined before its borrowed stream or
                        // request context can leave scope.
                        co_await asio::this_coro::reset_cancellation_state(
                            asio::disable_cancellation());
                        stop_status->store(true);
                        cancel_drain_timer();
                        if (!status_joined)
                        {
                            co_await detail::join_http_child(status_done);
                        }
                        if (!writer_joined)
                        {
                            if (!writer_stop_enqueued)
                            {
                                boost::system::error_code stop_ec;
                                co_await frames->async_send(
                                    boost::system::error_code{},
                                    SseFrame{},
                                    asio::redirect_error(
                                        asio::use_awaitable, stop_ec));
                                if (stop_ec)
                                    frames->cancel();
                            }
                            co_await detail::join_http_child(writer_done);
                        }
                        stop_watcher->store(true);
                        boost::system::error_code cancel_ec;
                        beast::get_lowest_layer(stream).socket().cancel(
                            cancel_ec);
                        co_await detail::join_http_child(watcher_done);

                        if (pipeline_failure)
                        {
                            try
                            {
                                std::rethrow_exception(pipeline_failure);
                            }
                            catch (std::exception const& e)
                            {
                                try
                                {
                                    PLOGE(
                                        log_,
                                        "SSE pipeline failed: ",
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
                                    PLOGE(log_, "SSE pipeline failed");
                                }
                                catch (...)
                                {
                                }
                            }
                        }

                        // Close the connection — SSE is done
                        break;
                    }
                    else
                    {
                        // ── Regular JSON/binary path ──

                        // Cancel timer: set to max, cancelled only when the
                        // socket watcher observes a transport failure.
                        auto cancel = std::make_shared<asio::steady_timer>(
                            stream.get_executor(),
                            asio::steady_timer::time_point::max());
                        auto watcher_join = std::make_shared<
                            detail::HttpChildCompletionChannel>(
                            stream.get_executor(), 1);

                        // Socket watcher: only an actual transport error can
                        // prove the response consumer is gone. Read EOF alone
                        // may be a proxy's normal request-side half-close.
                        // Safe: stream lives in handle_session which is
                        // blocked on the parallel_group below.
                        auto watcher_done =
                            std::make_shared<std::atomic<bool>>(false);
                        auto watcher_receipt_hook =
                            test_before_regular_watcher_receipt_;
                        auto ex = co_await asio::this_coro::executor;
                        // clang-format off
                        //@@authority transition=prv.T0129
                        // bind=expression_statement@a95e51eede414092
                        // status=provisional scope=statement owner=unresolved
                        // owner_hypothesis=http_session
                        // commit=connection_executor touches=prv.S0044
                        // mutation_class=running_guarded
                        // label=launch_regular_http_transport_failure_borrower
                        co_spawn(
                            stream.get_executor(),
                            [&stream,
                             cancel,
                             cancel_token,
                             request_scope,
                             watcher_done,
                             watcher_receipt_hook =
                                 std::move(watcher_receipt_hook)]()
                                -> asio::awaitable<void> {
                                auto const failed = co_await
                                    HttpServer::co_wait_for_transport_failure(
                                        beast::get_lowest_layer(stream)
                                            .socket());
                                if (failed && !watcher_done->load())
                                {
                                    request_scope->cancel();
                                    cancel_token->store(true);
                                    // This pinned Boost exposes only the
                                    // throwing timer cancel overload. Any throw
                                    // exits through the co_spawn completion
                                    // funnel below, which still publishes the
                                    // watcher receipt exactly once.
                                    cancel->cancel();
                                }
                                if (watcher_receipt_hook)
                                    co_await watcher_receipt_hook();
                            },
                            detail::HttpChildCompletion{
                                watcher_join}); //@@authority-target ids=prv.T0129
                        // clang-format on

                        // Race prove against transport failure using
                        // parallel_group with wait_for_one(). Unlike
                        // ||/wait_for_one_success, this cancels the other
                        // branch when EITHER completes — including when prove
                        // throws an exception.
                        std::exception_ptr race_failure;
                        std::exception_ptr prove_failure;
                        std::exception_ptr cancel_failure;
                        std::optional<ProofEngine::ProveResult> prove_result;
                        bool prove_won = false;
                        try
                        {
                            if (test_before_regular_race_)
                                test_before_regular_race_();

                            // ContextExecutor — prove's top-level co_awaits
                            // get the request ID; inner strand hops don't.
                            auto [order, ex0, r0, ex1] =
                                co_await asio::experimental::make_parallel_group(
                                    asio::co_spawn(
                                        ex,
                                        engine_sp->prove(
                                            tx_it->second,
                                            cancel_token,
                                            ledger_seq_hint,
                                            max_anchor_age,
                                            nullptr,
                                            request_scope),
                                        asio::deferred),
                                    asio::co_spawn(
                                        ex,
                                        [cancel]() -> asio::awaitable<void> {
                                            co_await asio::this_coro::
                                                reset_cancellation_state(
                                                    asio::
                                                        enable_total_cancellation());
                                            boost::system::error_code ec;
                                            co_await cancel->async_wait(
                                                asio::redirect_error(
                                                    asio::use_awaitable, ec));
                                        },
                                        asio::deferred))
                                    .async_wait(
                                        asio::experimental::wait_for_one(),
                                        asio::use_awaitable);

                            prove_won = order[0] == 0;
                            prove_failure = std::move(ex0);
                            cancel_failure = std::move(ex1);
                            if (prove_won && !prove_failure)
                                prove_result.emplace(std::move(r0));
                        }
                        catch (...)
                        {
                            race_failure = std::current_exception();
                        }

                        // Every path after the watcher launch, including an
                        // exception or parent cancellation from the race,
                        // enters this uncancellable join phase before touching
                        // or releasing stream. A canceled receive is not
                        // evidence that the watcher released its borrow.
                        co_await asio::this_coro::reset_cancellation_state(
                            asio::disable_cancellation());
                        watcher_done->store(true);
                        boost::system::error_code watcher_cancel_ec;
                        beast::get_lowest_layer(stream).socket().cancel(
                            watcher_cancel_ec);
                        //@@authority transition=prv.T0130
                        // bind=join_http_child@a266c62b3fdc921a
                        // status=provisional scope=statement owner=unresolved
                        // owner_hypothesis=http_session
                        // commit=uncancellable_join touches=prv.S0044
                        // mutation_class=terminal_receipt
                        // calls=join_http_child
                        // label=join_regular_http_transport_watcher_before_exit
                        co_await detail::join_http_child(
                            watcher_join);  //@@authority-target ids=prv.T0130

                        if (race_failure)
                            std::rethrow_exception(race_failure);
                        if (prove_won)
                        {
                            if (prove_failure)
                                std::rethrow_exception(prove_failure);
                        }
                        else
                        {
                            if (cancel_failure)
                                std::rethrow_exception(cancel_failure);
                            // Cancel branch fired = client disconnected
                            PLOGD(log_, "Client disconnected during prove");
                            break;
                        }

                        if (!prove_result)
                            throw std::logic_error(
                                "prove won without result or exception");
                        auto result = std::move(*prove_result);
                        auto format_it = params.find("format");
                        bool binary = format_it != params.end() &&
                            format_it->second == "bin";

                        stream.expires_after(opts_.write_timeout);

                        if (binary)
                        {
                            auto data =
                                to_binary(result.chain, {.compress = true});
                            http::response<http::vector_body<uint8_t>> res{
                                http::status::ok, version};
                            res.set(
                                http::field::content_type,
                                "application/octet-stream");
                            res.set(http::field::server, "xprv");
                            res.keep_alive(keep_alive);
                            res.body() = std::move(data);
                            res.prepare_payload();
                            co_await http::async_write(
                                stream,
                                res,
                                asio::redirect_error(asio::use_awaitable, ec));
                        }
                        else
                        {
                            auto chain_json = to_json(result.chain);
                            auto body_str = boost::json::serialize(chain_json);
                            http::response<http::string_body> res{
                                http::status::ok, version};
                            res.set(
                                http::field::content_type, "application/json");
                            res.set(http::field::server, "xprv");
                            res.keep_alive(keep_alive);
                            res.body() = std::move(body_str);
                            res.prepare_payload();
                            co_await http::async_write(
                                stream,
                                res,
                                asio::redirect_error(asio::use_awaitable, ec));
                        }
                    }  // regular path
                }
            }
            else if (
                effective_path == "/verify" && req.method() == http::verb::post)
            {
                auto const& body = req.body();
                if (body.empty())
                {
                    stream.expires_after(opts_.write_timeout);
                    co_await http::async_write(
                        stream,
                        error_response(
                            400, "empty request body", version, keep_alive),
                        asio::redirect_error(asio::use_awaitable, ec));
                }
                else
                {
                    // Use default engine for verify (it's a static
                    // function that auto-detects network from the proof)
                    auto vr = default_engine()->verify(
                        std::span<const uint8_t>(body.data(), body.size()));

                    boost::json::object result;
                    result["verified"] = vr.ok;
                    if (!vr.error.empty())
                    {
                        result["error"] = vr.error;
                    }

                    stream.expires_after(opts_.write_timeout);
                    co_await http::async_write(
                        stream,
                        json_response(200, result, version, keep_alive),
                        asio::redirect_error(asio::use_awaitable, ec));
                }
            }
            else
            {
                // Try static file (dev disk → embedded)
                std::string static_body, content_type;

                // Deep link: /network/{slug}/tx/{hash} → serve index.html
                auto route = req.method() == http::verb::get
                    ? parse_network_tx_route(path)
                    : std::nullopt;
                bool deep_link = false;
                if (route)
                {
                    for (auto const& [id, _] : engines_)
                    {
                        if (route_matches_network(route->network, id))
                        {
                            deep_link = try_serve_static(
                                "/", static_body, content_type);
                            break;
                        }
                    }
                }

                if (deep_link ||
                    (req.method() == http::verb::get &&
                     try_serve_static(path, static_body, content_type)))
                {
                    http::response<http::string_body> res{
                        http::status::ok, version};
                    res.set(http::field::content_type, content_type);
                    res.set(http::field::server, "xprv");
                    if (!g_xprv_header.empty())
                        res.set("X-XPRV", g_xprv_header);
                    if (path == "/verify-xprv.py")
                        res.set(
                            http::field::content_disposition,
                            "attachment; filename=\"verify-xprv.py\"");
                    res.keep_alive(keep_alive);
                    res.body() = std::move(static_body);
                    res.prepare_payload();
                    stream.expires_after(opts_.write_timeout);
                    co_await http::async_write(
                        stream,
                        res,
                        asio::redirect_error(asio::use_awaitable, ec));
                }
                else
                {
                    stream.expires_after(opts_.write_timeout);
                    co_await http::async_write(
                        stream,
                        error_response(404, "not found", version, keep_alive),
                        asio::redirect_error(asio::use_awaitable, ec));
                }
            }
        }
        catch (catl::rpc::RpcTxNotFound const& e)
        {
            PLOGW(log_, "TX not found: ", e.what());
            auto accept_hdr = std::string(req[http::field::accept]);
            if (accept_hdr.find("text/event-stream") != std::string::npos)
            {
                // SSE client — send error as SSE event so the frontend
                // shows the actual message instead of "failed to connect".
                // Can't co_await in catch (C++20), so stash and handle below.
                // Serialize via boost::json so e.what() (which can carry
                // request-derived text) is escaped — raw concatenation here
                // would let a crafted message break out of the JSON string
                // (sec #0054).
                boost::json::object sse_err;
                sse_err["type"] = "error";
                sse_err["error"] = e.what();
                sse_error_msg =
                    "data: " + boost::json::serialize(sse_err) + "\n\n";
            }
            else
            {
                caught_error =
                    error_response(404, e.what(), version, keep_alive);
            }
        }
        catch (catl::rpc::RpcTransientError const& e)
        {
            PLOGW(log_, "RPC transient error: ", e.what());
            caught_error = error_response(503, e.what(), version, keep_alive);
        }
        catch (catl::rpc::RpcConnectionError const& e)
        {
            PLOGE(log_, "RPC connection error: ", e.what());
            caught_error = error_response(502, e.what(), version, keep_alive);
        }
        catch (catl::peer_client::PeerClientException const& e)
        {
            PLOGW(log_, "Peer error: ", e.what());
            caught_error = error_response(504, e.what(), version, keep_alive);
        }
        catch (std::exception const& e)
        {
            // Log the real reason, but don't leak internal exception detail
            // (paths, type names, internal state) to the client (sec #0054).
            // The specific RPC/peer catches above surface controlled,
            // user-meaningful messages; this catch-all is the unknown bucket.
            PLOGE(log_, "Request failed: ", e.what());
            caught_error =
                error_response(500, "internal error", version, keep_alive);
        }

        // SSE error for tx-not-found (can't co_await in catch)
        if (!sse_error_msg.empty())
        {
            std::string sse_resp =
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/event-stream\r\n"
                "Cache-Control: no-cache\r\n"
                "Connection: close\r\n\r\n" +
                sse_error_msg;
            stream.expires_after(opts_.write_timeout);
            co_await asio::async_write(
                stream,
                asio::buffer(sse_resp),
                asio::redirect_error(asio::use_awaitable, ec));
            break;
        }

        // Send error response outside the catch block
        if (caught_error)
        {
            stream.expires_after(opts_.write_timeout);
            co_await http::async_write(
                stream,
                *caught_error,
                asio::redirect_error(asio::use_awaitable, ec));
        }

        if (ec || !keep_alive)
            break;
    }
}

}  // namespace xprv
