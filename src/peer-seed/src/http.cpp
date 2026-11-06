#include "http.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <exception>
#include <optional>
#include <stdexcept>

namespace peer_seed {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;

HttpResponse
http_post(std::string const& url, std::string const& body, int timeout_secs)
{
    if (url.rfind("http://", 0) != 0)
        throw std::runtime_error("not an http:// URL: " + url);
    auto const rest = url.substr(7);
    auto const slash = rest.find('/');
    auto const authority = rest.substr(0, slash);
    auto const target =
        slash == std::string::npos ? std::string("/") : rest.substr(slash);
    auto const colon = authority.rfind(':');
    auto const host = authority.substr(0, colon);
    auto const port = colon == std::string::npos ? std::string("80")
                                                 : authority.substr(colon + 1);

    http::request<http::string_body> request(http::verb::post, target, 11);
    request.set(http::field::host, authority);
    request.set(http::field::user_agent, "catl-peer-seed/1");
    request.set(http::field::content_type, "application/json");
    request.body() = body;
    request.prepare_payload();

    // Beast's stream deadline applies only to asynchronous operations, so the
    // exchange runs as one coroutine with a single deadline over connect,
    // write and read. The resolver has no deadline of its own; run_for
    // bounds it, and anything still pending is cancelled and drained.
    auto const timeout = std::chrono::seconds(timeout_secs);
    asio::io_context io;
    asio::ip::tcp::resolver resolver(io);
    beast::tcp_stream stream(io);
    beast::flat_buffer buffer;
    http::response<http::string_body> response;
    bool done = false;
    std::exception_ptr failure;

    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            auto const endpoints = co_await resolver.async_resolve(
                host, port, asio::use_awaitable);
            stream.expires_after(timeout);
            co_await stream.async_connect(endpoints, asio::use_awaitable);
            co_await http::async_write(stream, request, asio::use_awaitable);
            co_await http::async_read(
                stream, buffer, response, asio::use_awaitable);
            done = true;
        },
        [&](std::exception_ptr e) { failure = e; });

    io.run_for(timeout + std::chrono::seconds(1));
    if (!done && !failure)
    {
        resolver.cancel();
        beast::error_code ignored;
        stream.socket().close(ignored);
        io.restart();
        io.run();
        throw std::runtime_error(
            url + ": no response within " + std::to_string(timeout_secs) + "s");
    }
    if (failure)
    {
        try
        {
            std::rethrow_exception(failure);
        }
        catch (boost::system::system_error const& e)
        {
            // what() carries Beast's source location; the code's message is
            // the part a person needs.
            throw std::runtime_error(url + ": " + e.code().message());
        }
        catch (std::exception const& e)
        {
            throw std::runtime_error(url + ": " + e.what());
        }
    }

    HttpResponse out;
    out.status = response.result_int();
    out.body = std::move(response.body());
    for (auto const& field : response)
    {
        std::string name(field.name_string());
        std::transform(
            name.begin(), name.end(), name.begin(), [](unsigned char c) {
                return static_cast<char>(std::tolower(c));
            });
        out.headers[name] = std::string(field.value());
    }
    return out;
}

}  // namespace peer_seed
