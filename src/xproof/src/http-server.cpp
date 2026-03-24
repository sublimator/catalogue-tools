#include "xproof/http-server.h"
#include "xproof/proof-chain-binary.h"
#include "xproof/proof-chain-json.h"

#include <catl/core/logger.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
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
    return {std::string(target.substr(0, q)), std::string(target.substr(q + 1))};
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
    return json_response(
        status, {{"error", message}}, version, keep_alive);
}

// ═══════════════════════════════════════════════════════════════════════
// Lifecycle
// ═══════════════════════════════════════════════════════════════════════

HttpServer::HttpServer(
    asio::io_context& io,
    std::shared_ptr<ProofEngine> engine,
    HttpServerOptions opts)
    : io_(io)
    , engine_(std::move(engine))
    , opts_(std::move(opts))
    , acceptor_(io)
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
    auto endpoint = tcp::endpoint(
        asio::ip::make_address(opts_.bind_address), opts_.port);

    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(asio::socket_base::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen(asio::socket_base::max_listen_connections);
    accepting_ = true;

    PLOGI(
        log_,
        "Listening on ",
        opts_.bind_address,
        ":",
        opts_.port);

    auto self = shared_from_this();
    asio::co_spawn(io_, [self]() { return self->accept_loop(); }, asio::detached);
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
            stream, buffer, parser,
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

        PLOGD(
            log_,
            req.method_string(),
            " ",
            req.target());

        // ── Route dispatch ────────────────────────────────────────

        std::optional<http::response<http::string_body>> caught_error;
        try
        {
            if (path == "/health" &&
                req.method() == http::verb::get)
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
                body["cache"] = cache;

                stream.expires_after(opts_.write_timeout);
                co_await http::async_write(
                    stream,
                    json_response(200, body, version, keep_alive),
                    asio::redirect_error(asio::use_awaitable, ec));
            }
            else if (
                path == "/prove" &&
                req.method() == http::verb::get)
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
                    auto result = co_await engine_->prove(tx_it->second);
                    auto format_it = params.find("format");
                    bool binary =
                        format_it != params.end() && format_it->second == "bin";

                    stream.expires_after(opts_.write_timeout);

                    if (binary)
                    {
                        auto data = to_binary(
                            result.chain, {.compress = true});
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
                            stream, res,
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
                        res.set(http::field::server, "xproof");
                        res.keep_alive(keep_alive);
                        res.body() = std::move(body_str);
                        res.prepare_payload();
                        co_await http::async_write(
                            stream, res,
                            asio::redirect_error(asio::use_awaitable, ec));
                    }
                }
            }
            else if (
                path == "/verify" &&
                req.method() == http::verb::post)
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
        catch (std::exception const& e)
        {
            // Can't co_await in catch — stash the error response
            PLOGE(log_, "Request failed: ", e.what());
            caught_error = error_response(500, e.what(), version, keep_alive);
        }

        // Send error response outside the catch block
        if (caught_error)
        {
            stream.expires_after(opts_.write_timeout);
            co_await http::async_write(
                stream, *caught_error,
                asio::redirect_error(asio::use_awaitable, ec));
        }

        if (ec || !keep_alive)
            break;
    }
}

}  // namespace xproof
