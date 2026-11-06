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
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

namespace peer_seed {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;

namespace {

std::vector<asio::ip::tcp::endpoint>
resolve_before(
    std::string const& host,
    std::string const& port,
    std::chrono::steady_clock::time_point deadline,
    std::string const& url)
{
    unsigned long const port_number = std::stoul(port);
    boost::system::error_code ec;
    auto const literal = asio::ip::make_address(host, ec);
    if (!ec)
        return {{literal, static_cast<unsigned short>(port_number)}};

    using Endpoints = std::vector<asio::ip::tcp::endpoint>;
    auto promise = std::make_shared<std::promise<Endpoints>>();
    auto result = promise->get_future();
    std::thread([promise, host, port] {
        try
        {
            asio::io_context io;
            asio::ip::tcp::resolver resolver(io);
            Endpoints found;
            for (auto const& entry : resolver.resolve(host, port))
                found.push_back(entry.endpoint());
            promise->set_value(std::move(found));
        }
        catch (...)
        {
            promise->set_exception(std::current_exception());
        }
    }).detach();
    if (result.wait_until(deadline) != std::future_status::ready)
        throw std::runtime_error(
            url + ": DNS lookup of " + host + " timed out");
    try
    {
        return result.get();
    }
    catch (boost::system::system_error const& e)
    {
        throw std::runtime_error(url + ": " + host + ": " + e.code().message());
    }
    catch (std::exception const& e)
    {
        throw std::runtime_error(url + ": " + e.what());
    }
}

}  // namespace

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

    // One deadline covers the whole exchange. getaddrinfo can't be
    // cancelled once it starts, so a hostname is resolved on a detached
    // thread that is waited for only until the deadline; an IP literal skips
    // DNS. Beast's stream deadline applies only to asynchronous operations,
    // so connect, write and read run as one coroutine under it.
    auto const deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(timeout_secs);
    auto const endpoints = resolve_before(host, port, deadline, url);

    asio::io_context io;
    beast::tcp_stream stream(io);
    beast::flat_buffer buffer;
    http::response<http::string_body> response;
    bool done = false;
    std::exception_ptr failure;

    stream.expires_at(deadline);
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            co_await stream.async_connect(endpoints, asio::use_awaitable);
            co_await http::async_write(stream, request, asio::use_awaitable);
            co_await http::async_read(
                stream, buffer, response, asio::use_awaitable);
            done = true;
        },
        [&](std::exception_ptr e) { failure = e; });

    io.run_until(deadline + std::chrono::seconds(1));
    if (!done && !failure)
    {
        // Socket operations finish promptly once the socket is closed.
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
