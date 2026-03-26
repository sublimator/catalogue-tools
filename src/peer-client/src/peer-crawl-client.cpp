#include <catl/peer-client/peer-crawl-client.h>

#include <catl/core/logger.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/json.hpp>

#include <algorithm>
#include <cctype>
#include <sstream>
#include <stdexcept>

namespace catl::peer_client {

namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = boost::asio::ssl;
using tcp = boost::asio::ip::tcp;

namespace {

LogPartition log_("peer-crawl", LogLevel::INHERIT);

std::string
trim_copy(std::string_view input)
{
    auto first = input.begin();
    auto last = input.end();
    while (first != last && std::isspace(static_cast<unsigned char>(*first)))
    {
        ++first;
    }
    while (first != last &&
           std::isspace(static_cast<unsigned char>(*(last - 1))))
    {
        --last;
    }
    return std::string(first, last);
}

std::string
format_endpoint(std::string const& host, uint16_t port)
{
    if (host.find(':') != std::string::npos && host.find(']') == std::string::npos)
    {
        return "[" + host + "]:" + std::to_string(port);
    }
    return host + ":" + std::to_string(port);
}

template <class Stream>
boost::asio::awaitable<CrawlResponse>
read_crawl_response(
    Stream& stream,
    beast::tcp_stream& tcp_stream,
    std::string const& host,
    bool used_tls)
{
    http::request<http::empty_body> req(http::verb::get, "/crawl", 11);
    req.set(http::field::host, host);
    req.set(http::field::accept, "application/json");
    req.set(http::field::user_agent, "catalogue-tools/xprv");

    tcp_stream.expires_after(std::chrono::seconds(5));
    co_await http::async_write(stream, req, boost::asio::use_awaitable);

    beast::flat_buffer buffer;
    http::response<http::string_body> res;
    tcp_stream.expires_after(std::chrono::seconds(10));
    co_await http::async_read(stream, buffer, res, boost::asio::use_awaitable);

    if (res.result() != http::status::ok)
    {
        throw std::runtime_error(
            "crawl returned HTTP " + std::to_string(res.result_int()));
    }

    auto root = boost::json::parse(res.body());
    auto const* root_obj = root.if_object();
    if (!root_obj)
    {
        throw std::runtime_error("crawl response root is not an object");
    }

    std::vector<CrawlPeer> peers;

    auto const* overlay = root_obj->if_contains("overlay");
    if (overlay && overlay->is_object())
    {
        auto const* active = overlay->as_object().if_contains("active");
        if (active && active->is_array())
        {
            for (auto const& item : active->as_array())
            {
                auto const* obj = item.if_object();
                if (!obj)
                    continue;

                auto const* ip_value = obj->if_contains("ip");
                auto const* port_value = obj->if_contains("port");
                if (!ip_value || !port_value || !ip_value->is_string() ||
                    !port_value->is_string())
                {
                    continue;
                }

                auto const host_part =
                    std::string(ip_value->as_string().c_str());
                auto const port_part =
                    std::string(port_value->as_string().c_str());

                uint16_t port = 0;
                try
                {
                    port = static_cast<uint16_t>(std::stoul(port_part));
                }
                catch (...)
                {
                    continue;
                }

                CrawlPeer peer;
                peer.endpoint = format_endpoint(host_part, port);

                if (auto const* ledgers = obj->if_contains("complete_ledgers");
                    ledgers && ledgers->is_string())
                {
                    peer.complete_ledgers =
                        parse_complete_ledgers(ledgers->as_string().c_str());
                }

                peers.push_back(std::move(peer));
            }
        }
    }

    PLOGD(
        log_,
        "Crawl ",
        host,
        " returned ",
        peers.size(),
        " public peers via ",
        used_tls ? "HTTPS" : "HTTP");

    co_return CrawlResponse{.used_tls = used_tls, .peers = std::move(peers)};
}

boost::asio::awaitable<CrawlResponse>
fetch_crawl_https(std::string const& host, uint16_t port)
{
    auto executor = co_await boost::asio::this_coro::executor;

    ssl::context ctx(ssl::context::tls_client);
    ctx.set_verify_mode(ssl::verify_none);

    tcp::resolver resolver(executor);
    auto endpoints = co_await resolver.async_resolve(
        host, std::to_string(port), boost::asio::use_awaitable);

    beast::ssl_stream<beast::tcp_stream> stream(executor, ctx);
    if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str()))
    {
        throw std::runtime_error("failed to set crawl SNI hostname");
    }

    auto& tcp_stream = beast::get_lowest_layer(stream);
    tcp_stream.expires_after(std::chrono::seconds(5));
    co_await tcp_stream.async_connect(endpoints, boost::asio::use_awaitable);

    tcp_stream.expires_after(std::chrono::seconds(5));
    co_await stream.async_handshake(
        ssl::stream_base::client, boost::asio::use_awaitable);

    auto response = co_await read_crawl_response(stream, tcp_stream, host, true);

    boost::system::error_code ec;
    tcp_stream.expires_after(std::chrono::seconds(2));
    co_await stream.async_shutdown(
        boost::asio::redirect_error(boost::asio::use_awaitable, ec));

    co_return response;
}

boost::asio::awaitable<CrawlResponse>
fetch_crawl_http(std::string const& host, uint16_t port)
{
    auto executor = co_await boost::asio::this_coro::executor;

    tcp::resolver resolver(executor);
    auto endpoints = co_await resolver.async_resolve(
        host, std::to_string(port), boost::asio::use_awaitable);

    beast::tcp_stream stream(executor);
    stream.expires_after(std::chrono::seconds(5));
    co_await stream.async_connect(endpoints, boost::asio::use_awaitable);

    auto response = co_await read_crawl_response(stream, stream, host, false);

    boost::system::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    stream.socket().close(ec);

    co_return response;
}

}  // namespace

std::vector<CrawlLedgerRange>
parse_complete_ledgers(std::string_view value)
{
    std::vector<CrawlLedgerRange> ranges;
    std::string current;
    std::stringstream ss{std::string(value)};

    while (std::getline(ss, current, ','))
    {
        auto token = trim_copy(current);
        if (token.empty() || token == "empty")
            continue;

        auto dash = token.find('-');
        auto first_text = dash == std::string::npos ? token : token.substr(0, dash);
        auto last_text = dash == std::string::npos ? token : token.substr(dash + 1);

        try
        {
            auto const first = static_cast<uint32_t>(std::stoul(first_text));
            auto const last = static_cast<uint32_t>(std::stoul(last_text));
            if (first == 0 || last == 0 || first > last)
                continue;
            ranges.push_back({.first_seq = first, .last_seq = last});
        }
        catch (...)
        {
            continue;
        }
    }

    return ranges;
}

boost::asio::awaitable<CrawlResponse>
co_fetch_peer_crawl(std::string host, uint16_t port)
{
    try
    {
        co_return co_await fetch_crawl_https(host, port);
    }
    catch (std::exception const& https_error)
    {
        PLOGD(
            log_,
            "HTTPS crawl failed for ",
            host,
            ":",
            port,
            ": ",
            https_error.what(),
            " — falling back to HTTP");
    }

    co_return co_await fetch_crawl_http(host, port);
}

}  // namespace catl::peer_client
