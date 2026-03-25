#include "xproof/http-server.h"
#include "xproof/proof-chain-binary.h"
#include "xproof/proof-chain-json.h"

#include <catl/core/logger.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/rpc-client/rpc-client.h>
#include <xproof/request-context.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

using namespace boost::asio::experimental::awaitable_operators;
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>

#include <map>
#include <string>
#include <string_view>

namespace xproof {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

static LogPartition log_("http", LogLevel::INFO);

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
    res.set(http::field::server, "xproof");
    res.keep_alive(keep_alive);
    res.body() = boost::json::serialize(body);
    res.prepare_payload();
    return res;
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

        asio::co_spawn(
            io_,
            [self, s = std::move(socket)]() mutable {
                return self->handle_session(std::move(s));
            },
            asio::detached);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Session handler
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<void>
HttpServer::handle_session(tcp::socket socket)
{
    beast::tcp_stream stream(std::move(socket));
    beast::flat_buffer buffer;

    // Per-session cancel timer. Set to max — only fires when cancelled.
    // Shared with prove coroutines via || operator. On session error,
    // cancel() fires, the || cancels the in-flight prove.
    auto session_cancel = std::make_shared<asio::steady_timer>(
        stream.get_executor(), asio::steady_timer::time_point::max());

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

        PLOGD(log_, req.method_string(), " ", req.target());

        // Per-request context for cancellation + future session state
        auto request_ctx = std::make_shared<RequestContext>();

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
                    auto result = co_await engine_->prove(
                        tx_it->second, request_ctx);
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
                        res.set(http::field::server, "xproof");
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
                        res.set(http::field::server, "xproof");
                        res.keep_alive(keep_alive);
                        res.body() = std::move(body_str);
                        res.prepare_payload();
                        co_await http::async_write(
                            stream,
                            res,
                            asio::redirect_error(asio::use_awaitable, ec));
                    }
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
                stream.expires_after(opts_.write_timeout);
                co_await http::async_write(
                    stream,
                    error_response(404, "not found", version, keep_alive),
                    asio::redirect_error(asio::use_awaitable, ec));
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
        catch (RequestCancelled const&)
        {
            // Client disconnected during prove — no response needed.
            PLOGD(log_, "Request cancelled (client disconnected)");
            break;
        }
        catch (boost::system::system_error const& e)
        {
            if (e.code() == asio::error::operation_aborted)
            {
                PLOGD(log_, "Operation aborted: ", e.what());
                break;
            }
            PLOGE(log_, "System error: ", e.what());
            caught_error = error_response(500, e.what(), version, keep_alive);
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

    // Session ending — no in-flight proves to cancel (request_ctx is
    // per-iteration and already out of scope). The session_cancel timer
    // is kept for future use with the || operator pattern.
    session_cancel->cancel();
}

}  // namespace xproof
