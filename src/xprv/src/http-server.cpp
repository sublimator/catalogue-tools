#include "xprv/http-server.h"
#include "xprv/proof-chain-binary.h"
#include "xprv/proof-chain-json.h"
#include "web_assets.h"

#include <cctype>
#include <filesystem>
#include <fstream>
#include <optional>

#include <catl/core/context-executor.h>
#include <catl/core/logger.h>
#include <catl/core/request-context.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/rpc-client/rpc-client.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>

#include <map>
#include <random>
#include <string>
#include <string_view>

namespace xprv {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

static LogPartition log_("http", LogLevel::INFO);

// Process-wide X-XPRV header value, set once by HttpServer constructor.
static std::string g_xprv_header;

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

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

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
        c = static_cast<char>(
            std::tolower(static_cast<unsigned char>(c)));
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

    return NetworkTxRoute{
        to_lower_copy(network),
        std::string(tx_hash)};
}

static bool
route_matches_network(std::string_view route_network, uint32_t network_id)
{
    auto const configured_slug = network_slug_for_id(network_id);
    auto const numeric_id = std::to_string(network_id);
    auto const token = to_lower_copy(route_network);
    return token == configured_slug || token == numeric_id;
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
    if (ext == ".html") return "text/html; charset=utf-8";
    if (ext == ".js")   return "application/javascript; charset=utf-8";
    if (ext == ".css")  return "text/css; charset=utf-8";
    if (ext == ".svg")  return "image/svg+xml";
    if (ext == ".json") return "application/json";
    if (ext == ".png")  return "image/png";
    if (ext == ".ico")  return "image/x-icon";
    if (ext == ".py")   return "text/x-python; charset=utf-8";
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
static bool
try_serve_static(
    std::string_view path,
    std::string& out_body,
    std::string& out_content_type)
{
    // Sanitize: reject paths with ".." or starting with "/"
    if (path.find("..") != std::string_view::npos)
        return false;

    // Strip leading /
    auto relative = path;
    if (!relative.empty() && relative[0] == '/')
        relative = relative.substr(1);
    if (relative.empty())
        relative = "index.html";

    // Dev mode: try disk first
    auto dev_path = std::filesystem::path("src/xprv/web/static") / relative;
    if (std::filesystem::exists(dev_path))
    {
        std::ifstream f(dev_path);
        out_body.assign(
            std::istreambuf_iterator<char>(f),
            std::istreambuf_iterator<char>());
        auto ext = dev_path.extension().string();
        out_content_type = std::string(mime_for_extension(ext));
        return true;
    }

    // Embedded: look up by normalized name
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
    std::shared_ptr<ProofEngine> engine,
    HttpServerOptions opts)
    : io_(io), engine_(std::move(engine)), opts_(std::move(opts)), acceptor_(io)
{
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
    return std::shared_ptr<HttpServer>(
        new HttpServer(io, std::move(engine), std::move(opts)));
}

void
HttpServer::start()
{
    auto endpoint =
        tcp::endpoint(asio::ip::make_address(opts_.bind_address), opts_.port);

    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(asio::socket_base::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen(asio::socket_base::max_listen_connections);
    accepting_ = true;

    PLOGI(log_, "Listening on ", opts_.bind_address, ":", opts_.port);

    auto self = shared_from_this();
    asio::co_spawn(
        io_, [self]() { return self->accept_loop(); }, asio::detached);
}

void
HttpServer::stop()
{
    accepting_ = false;
    boost::system::error_code ec;
    acceptor_.close(ec);
    PLOGI(log_, "Stopped accepting connections");
}

// ═══════════════════════════════════════════════════════════════════════
// Accept loop
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<void>
HttpServer::accept_loop()
{
    auto self = shared_from_this();

    while (accepting_)
    {
        boost::system::error_code ec;
        auto socket = co_await acceptor_.async_accept(
            asio::redirect_error(asio::use_awaitable, ec));
        if (ec)
        {
            if (accepting_)
            {
                PLOGE(log_, "Accept error: ", ec.message());
            }
            break;
        }

        // Per-connection request context for log prefixes.
        // Covers handle_session and prove coroutine co_awaits;
        // inner strand hops (peer/RPC) won't have it.
        auto req_ctx = std::shared_ptr<catl::core::RequestContext>(
            new catl::core::RequestContext(
                generate_request_id(),
                {},  // tx_hash filled later
                std::chrono::steady_clock::now()));
        auto ctx_ex = catl::core::ContextExecutor(
            io_.get_executor(), req_ctx);
        asio::co_spawn(
            ctx_ex,
            [self, s = std::move(socket), req_ctx]() mutable {
                return self->handle_session(std::move(s), std::move(req_ctx));
            },
            asio::detached);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Session handler
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<void>
HttpServer::handle_session(
    tcp::socket socket,
    std::shared_ptr<catl::core::RequestContext> req_ctx)
{
    beast::tcp_stream stream(std::move(socket));
    beast::flat_buffer buffer;

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
                if (std::isalnum(static_cast<unsigned char>(c)) ||
                    c == '-' || c == '_')
                    safe += c;
            }
            if (!safe.empty())
                req_ctx->request_id = std::move(safe);
        }

        PLOGI(log_, req.method_string(), " ", path);

        // ── Route dispatch ────────────────────────────────────────

        std::optional<http::response<http::string_body>> caught_error;
        try
        {
            if (path == "/health" && req.method() == http::verb::get)
            {
                auto status = co_await engine_->co_health();
                auto cs = engine_->cache_stats();
                boost::json::object body;
                body["peer_count"] = status.peer_count;
                body["vl_loaded"] = status.vl_loaded;
                if (status.latest_quorum_seq)
                {
                    body["latest_quorum_seq"] = *status.latest_quorum_seq;
                }
                boost::json::object cache;
                cache["entries"] = cs.entries;
                cache["max_entries"] = cs.max_entries;
                cache["hits"] = cs.hits;
                cache["misses"] = cs.misses;
                body["proof_cache"] = cache;

                body["run_id"] = Logger::get_run_id();
                if (!opts_.build_id.empty())
                    body["build_id"] = opts_.build_id;

                auto ncs = engine_->node_cache_stats();
                boost::json::object nc;
                nc["entries"] = ncs.entries;
                nc["max_entries"] = ncs.max_entries;
                nc["hits"] = ncs.hits;
                nc["misses"] = ncs.misses;
                nc["fetches"] = ncs.fetches;
                nc["fetch_errors"] = ncs.fetch_errors;
                nc["hash_mismatches"] = ncs.hash_mismatches;
                nc["waiter_wakeups"] = ncs.waiter_wakeups;
                body["node_cache"] = nc;

                stream.expires_after(opts_.write_timeout);
                co_await http::async_write(
                    stream,
                    json_response(200, body, version, keep_alive),
                    asio::redirect_error(asio::use_awaitable, ec));
            }
            else if (path == "/peers" && req.method() == http::verb::get)
            {
                auto snapshot = co_await engine_->peers()->co_snapshot();

                boost::json::object body;
                body["known_endpoints"] = snapshot.known_endpoints;
                body["tracked_endpoints"] = snapshot.tracked_endpoints;
                body["connected_peers"] = snapshot.connected_peers;
                body["ready_peers"] = snapshot.ready_peers;
                body["in_flight_connects"] = snapshot.in_flight_connects;
                body["queued_connects"] = snapshot.queued_connects;
                body["crawl_in_flight"] = snapshot.crawl_in_flight;
                body["queued_crawls"] = snapshot.queued_crawls;

                boost::json::array wanted_ledgers;
                for (auto ledger_seq : snapshot.wanted_ledgers)
                {
                    wanted_ledgers.push_back(ledger_seq);
                }
                body["wanted_ledgers"] = std::move(wanted_ledgers);

                boost::json::array peers;
                for (auto const& peer : snapshot.peers)
                {
                    boost::json::object item;
                    item["endpoint"] = peer.endpoint;
                    item["connected"] = peer.connected;
                    item["ready"] = peer.ready;
                    item["in_flight"] = peer.in_flight;
                    item["queued_connect"] = peer.queued_connect;
                    item["crawl_in_flight"] = peer.crawl_in_flight;
                    item["queued_crawl"] = peer.queued_crawl;
                    item["crawled"] = peer.crawled;
                    item["first_seq"] = peer.first_seq;
                    item["last_seq"] = peer.last_seq;
                    item["current_seq"] = peer.current_seq;
                    item["last_seen_at"] = peer.last_seen_at;
                    item["last_success_at"] = peer.last_success_at;
                    item["last_failure_at"] = peer.last_failure_at;
                    item["success_count"] = peer.success_count;
                    item["failure_count"] = peer.failure_count;
                    item["selection_count"] = peer.selection_count;
                    item["last_selected_ticket"] = peer.last_selected_ticket;
                    peers.push_back(std::move(item));
                }
                body["peers"] = std::move(peers);

                stream.expires_after(opts_.write_timeout);
                co_await http::async_write(
                    stream,
                    json_response(200, body, version, keep_alive),
                    asio::redirect_error(asio::use_awaitable, ec));
            }
            else if (path == "/prove" && req.method() == http::verb::get)
            {
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
                    // Check if client wants SSE
                    auto accept = std::string(
                        req[http::field::accept]);
                    bool wants_sse =
                        accept.find("text/event-stream") != std::string::npos;

                    // Optional ledger_index hint — skips RPC lookup
                    uint32_t ledger_seq_hint = 0;
                    auto li_it = params.find("ledger_index");
                    if (li_it != params.end() && !li_it->second.empty())
                    {
                        try { ledger_seq_hint = std::stoul(li_it->second); }
                        catch (...) {}
                    }

                    // Optional max_anchor_age — reuse recent anchor (seconds).
                    // 0 (default) = always wait for latest quorum.
                    uint32_t max_anchor_age = 0;
                    auto ma_it = params.find("max_anchor_age");
                    if (ma_it != params.end() && !ma_it->second.empty())
                    {
                        try { max_anchor_age = std::stoul(ma_it->second); }
                        catch (...) {}
                    }

                    // Cancel token: atomic flag checked at cancellation
                    // boundaries (walk_to, with_peer_failover) for fast
                    // exit without disrupting shared NodeCache state.
                    auto cancel_token =
                        std::make_shared<std::atomic<bool>>(false);

                    if (wants_sse)
                    {
                        PLOGI(log_, "Entering SSE path");
                        // ── SSE path ──
                        // Write raw HTTP response + SSE frames directly.
                        // No chunked encoding — SSE streams are
                        // connection-scoped, closed when done.
                        stream.expires_never();

                        // Disable Nagle so each write flushes immediately
                        beast::get_lowest_layer(stream)
                            .socket()
                            .set_option(
                                asio::ip::tcp::no_delay(true));

                        std::string hdr_str =
                            "HTTP/1.1 200 OK\r\n"
                            "Content-Type: text/event-stream\r\n"
                            "Cache-Control: no-cache\r\n"
                            "Connection: close\r\n"
                            "Server: xprv\r\n"
                            "X-XPRV: " + g_xprv_header + "\r\n"
                            "\r\n";
                        co_await asio::async_write(
                            stream,
                            asio::buffer(hdr_str),
                            asio::redirect_error(asio::use_awaitable, ec));
                        PLOGI(log_, "SSE headers sent, ec=", ec.message());
                        if (ec) break;

                        int step_index = 0;
                        auto write_sse = [&](std::string data)
                            -> asio::awaitable<void> {
                            auto frame = "data: " + data + "\n\n";
                            PLOGI(log_, "SSE write: ", frame.size(), "B type=", data.substr(0, 40));
                            co_await asio::async_write(
                                stream,
                                asio::buffer(frame),
                                asio::redirect_error(asio::use_awaitable, ec));
                            PLOGI(log_, "SSE write done, ec=", ec.message());
                        };

                        auto on_step = [&](ChainStep const& step)
                            -> asio::awaitable<void> {
                            boost::json::object ev;
                            ev["type"] = "step";
                            ev["index"] = step_index++;
                            ev["step"] = step_to_json(step);
                            co_await write_sse(boost::json::serialize(ev));
                        };

                        // Status drain loop — runs concurrently with prove,
                        // flushing status events every 500ms. Single-threaded
                        // io_context means writes interleave at co_await
                        // points, never concurrently.
                        auto drain_done = std::make_shared<bool>(false);
                        auto drain_timer = std::make_shared<asio::steady_timer>(
                            stream.get_executor());
                        auto drain_ctx = req_ctx;
                        asio::co_spawn(
                            co_await asio::this_coro::executor,
                            [drain_done, drain_timer, drain_ctx, &write_sse]()
                                -> asio::awaitable<void> {
                                while (!*drain_done)
                                {
                                    drain_timer->expires_after(
                                        std::chrono::milliseconds(300));
                                    boost::system::error_code tec;
                                    co_await drain_timer->async_wait(
                                        asio::redirect_error(
                                            asio::use_awaitable, tec));
                                    if (*drain_done)
                                        break;
                                    auto msgs = drain_ctx->drain();
                                    for (auto& msg : msgs)
                                    {
                                        boost::json::object ev;
                                        ev["type"] = "log";
                                        ev["msg"] = std::move(msg);
                                        co_await write_sse(
                                            boost::json::serialize(ev));
                                    }
                                }
                            },
                            asio::detached);

                        std::string sse_error;
                        try
                        {
                            auto result = co_await engine_->prove(
                                tx_it->second, cancel_token,
                                ledger_seq_hint, max_anchor_age,
                                std::move(on_step));

                            // Stop drain loop and flush remaining
                            *drain_done = true;
                            drain_timer->cancel();
                            auto msgs = req_ctx->drain();
                            for (auto& msg : msgs)
                            {
                                boost::json::object ev;
                                ev["type"] = "log";
                                ev["msg"] = std::move(msg);
                                co_await write_sse(
                                    boost::json::serialize(ev));
                            }

                            // Send done event — client stitches proof
                            // from streamed steps + network_id
                            boost::json::object done;
                            done["type"] = "done";
                            done["network_id"] = result.chain.network_id;
                            co_await write_sse(boost::json::serialize(done));
                        }
                        catch (std::exception const& e)
                        {
                            *drain_done = true;
                            drain_timer->cancel();
                            boost::json::object err;
                            err["type"] = "error";
                            err["error"] = e.what();
                            sse_error = boost::json::serialize(err);
                        }

                        if (!sse_error.empty())
                            co_await write_sse(sse_error);

                        // Close the connection — SSE is done
                        break;
                    }
                    else
                    {
                    // ── Regular JSON/binary path ──

                    // Cancel timer: set to max, cancelled by socket
                    // watcher when the client disconnects.
                    auto cancel = std::make_shared<asio::steady_timer>(
                        stream.get_executor(),
                        asio::steady_timer::time_point::max());

                    // Socket watcher: monitors client disconnect.
                    // Safe: stream lives in handle_session which is
                    // blocked on the parallel_group below.
                    auto watcher_done =
                        std::make_shared<std::atomic<bool>>(false);
                    co_spawn(
                        stream.get_executor(),
                        [&stream, cancel, cancel_token, watcher_done]()
                            -> asio::awaitable<void> {
                            boost::system::error_code wec;
                            co_await beast::get_lowest_layer(stream)
                                .socket()
                                .async_wait(
                                    asio::ip::tcp::socket::wait_read,
                                    asio::redirect_error(
                                        asio::use_awaitable, wec));
                            if (!watcher_done->load())
                            {
                                cancel_token->store(true);
                                cancel->cancel();
                            }
                        },
                        asio::detached);

                    // ContextExecutor — prove's top-level co_awaits get
                    // the request ID; inner strand hops don't.
                    auto ex = co_await asio::this_coro::executor;

                    // Race prove against disconnect using parallel_group
                    // with wait_for_one(). Unlike ||/wait_for_one_success,
                    // this cancels the other branch when EITHER completes
                    // — including when prove throws an exception.
                    auto [order, ex0, r0, ex1] =
                        co_await asio::experimental::make_parallel_group(
                            asio::co_spawn(
                                ex,
                                engine_->prove(
                                    tx_it->second, cancel_token,
                                    ledger_seq_hint, max_anchor_age),
                                asio::deferred),
                            asio::co_spawn(
                                ex,
                                [cancel]()
                                    -> asio::awaitable<void> {
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

                    // Cancel the socket watcher before touching stream
                    watcher_done->store(true);
                    beast::get_lowest_layer(stream).socket().cancel();

                    // Check which branch completed first
                    if (order[0] == 0)
                    {
                        // Prove finished first (success or error)
                        if (ex0)
                            std::rethrow_exception(ex0); // → caught below
                    }
                    else
                    {
                        // Cancel branch fired = client disconnected
                        PLOGD(log_, "Client disconnected during prove");
                        break;
                    }

                    auto result = std::move(r0);
                    auto format_it = params.find("format");
                    bool binary =
                        format_it != params.end() && format_it->second == "bin";

                    stream.expires_after(opts_.write_timeout);

                    if (binary)
                    {
                        auto data = to_binary(result.chain, {.compress = true});
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
                        res.set(http::field::content_type, "application/json");
                        res.set(http::field::server, "xprv");
                        res.keep_alive(keep_alive);
                        res.body() = std::move(body_str);
                        res.prepare_payload();
                        co_await http::async_write(
                            stream,
                            res,
                            asio::redirect_error(asio::use_awaitable, ec));
                    }
                    } // regular path
                }
            }
            else if (path == "/verify" && req.method() == http::verb::post)
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
                    auto vr = engine_->verify(
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
                auto route = req.method() == http::verb::get
                    ? parse_network_tx_route(path)
                    : std::nullopt;
                bool deep_link =
                    route &&
                    route_matches_network(
                        route->network, engine_->config().network_id) &&
                    try_serve_static("/", static_body, content_type);

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
                        res.set(http::field::content_disposition,
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
            caught_error = error_response(404, e.what(), version, keep_alive);
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
            PLOGE(log_, "Request failed: ", e.what());
            caught_error = error_response(500, e.what(), version, keep_alive);
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
