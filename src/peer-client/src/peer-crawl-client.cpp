#include <catl/peer-client/peer-crawl-client.h>

#include <catl/base58/base58.h>
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
#include <charconv>
#include <limits>
#include <stdexcept>

namespace catl::peer_client {

namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = boost::asio::ssl;
using tcp = boost::asio::ip::tcp;

namespace {

LogPartition log_("peer-crawl", LogLevel::INHERIT);

#define CRAWL_TRACE(...)              \
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
strip_diagnostic(std::string_view error)
{
    auto summary = trim_copy(error);
    auto const diagnostic = summary.find(" [");
    if (diagnostic != std::string::npos)
        summary.erase(diagnostic);
    return summary;
}

std::string
truncate_body(std::string_view body, std::size_t limit = 120)
{
    auto text = trim_copy(body);
    if (text.size() <= limit)
        return text;
    text.resize(limit);
    text += "...";
    return text;
}

std::runtime_error
make_crawl_stage_error(std::string_view stage, std::string_view error)
{
    return std::runtime_error(
        "crawl " + std::string(stage) + ": " + strip_diagnostic(error));
}

std::runtime_error
make_crawl_http_error(
    std::string_view stage,
    http::response<http::string_body> const& res)
{
    auto message = std::string("crawl ") + std::string(stage) + ": HTTP " +
        std::to_string(res.result_int());
    auto const reason = trim_copy(res.reason());
    if (!reason.empty())
        message += " " + reason;
    auto const body = truncate_body(res.body());
    if (!body.empty())
        message += " body=\"" + body + "\"";
    return std::runtime_error(message);
}

std::string
format_endpoint(std::string const& host, uint16_t port)
{
    if (host.find(':') != std::string::npos &&
        host.find(']') == std::string::npos)
    {
        return "[" + host + "]:" + std::to_string(port);
    }
    return host + ":" + std::to_string(port);
}

bool
parse_decimal_u32(std::string_view text, uint32_t& value)
{
    if (text.empty())
        return false;
    auto const [end, error] =
        std::from_chars(text.data(), text.data() + text.size(), value);
    return error == std::errc{} && end == text.data() + text.size();
}

std::optional<uint16_t>
parse_peer_port(boost::json::value const& value)
{
    std::uint64_t parsed = 0;
    if (value.is_string())
    {
        auto const& json = value.as_string();
        auto const text = std::string_view(json.data(), json.size());
        uint32_t decimal = 0;
        if (!parse_decimal_u32(text, decimal))
            return std::nullopt;
        parsed = decimal;
    }
    else if (value.is_int64())
    {
        auto const integer = value.as_int64();
        if (integer <= 0)
            return std::nullopt;
        parsed = static_cast<std::uint64_t>(integer);
    }
    else if (value.is_uint64())
    {
        parsed = value.as_uint64();
    }
    else
    {
        return std::nullopt;
    }

    if (parsed == 0 || parsed > std::numeric_limits<uint16_t>::max())
        return std::nullopt;
    return static_cast<uint16_t>(parsed);
}

bool
ranges_touch(CrawlLedgerRange const& lhs, CrawlLedgerRange const& rhs)
{
    return rhs.first_seq <= lhs.last_seq ||
        (lhs.last_seq != std::numeric_limits<uint32_t>::max() &&
         rhs.first_seq == lhs.last_seq + 1);
}

struct ParsedLedgerRanges
{
    std::vector<CrawlLedgerRange> legacy_ranges;
    std::vector<CrawlLedgerRange> reported_ranges;
    std::size_t malformed = 0;
    std::size_t truncated = 0;
};

ParsedLedgerRanges
parse_complete_ledgers_bounded(std::string_view value, std::size_t limit)
{
    ParsedLedgerRanges out;
    std::size_t begin = 0;
    while (begin <= value.size())
    {
        auto const comma = value.find(',', begin);
        auto const end = comma == std::string_view::npos ? value.size() : comma;
        auto const token = trim_copy(value.substr(begin, end - begin));
        begin = comma == std::string_view::npos ? value.size() + 1 : comma + 1;

        if (token.empty() || token == "empty")
            continue;

        auto const dash = token.find('-');
        if (dash != std::string::npos &&
            token.find('-', dash + 1) != std::string::npos)
        {
            ++out.malformed;
            continue;
        }

        auto const first_text = dash == std::string::npos
            ? std::string_view(token)
            : std::string_view(token).substr(0, dash);
        auto const last_text = dash == std::string::npos
            ? std::string_view(token)
            : std::string_view(token).substr(dash + 1);
        uint32_t first = 0;
        uint32_t last = 0;
        if (!parse_decimal_u32(first_text, first) ||
            !parse_decimal_u32(last_text, last) || first == 0 || last == 0 ||
            first > last)
        {
            ++out.malformed;
            continue;
        }

        CrawlLedgerRange incoming{.first_seq = first, .last_seq = last};
        if (out.legacy_ranges.size() >= limit)
        {
            ++out.truncated;
            continue;
        }
        out.legacy_ranges.push_back(incoming);

        auto position = std::lower_bound(
            out.reported_ranges.begin(),
            out.reported_ranges.end(),
            incoming.first_seq,
            [](CrawlLedgerRange const& range, uint32_t first_seq) {
                return range.last_seq != std::numeric_limits<uint32_t>::max() &&
                    range.last_seq + 1 < first_seq;
            });

        if (position == out.reported_ranges.end() ||
            !ranges_touch(incoming, *position))
        {
            out.reported_ranges.insert(position, incoming);
            continue;
        }

        position->first_seq = std::min(position->first_seq, incoming.first_seq);
        position->last_seq = std::max(position->last_seq, incoming.last_seq);
        auto next = position + 1;
        while (next != out.reported_ranges.end() &&
               ranges_touch(*position, *next))
        {
            position->last_seq = std::max(position->last_seq, next->last_seq);
            next = out.reported_ranges.erase(next);
        }
    }
    return out;
}

template <class Stream>
boost::asio::awaitable<CrawlResponse>
read_crawl_response(
    Stream& stream,
    beast::tcp_stream& tcp_stream,
    std::string const& host,
    bool used_tls)
{
    auto const scheme = used_tls ? "https" : "http";
    http::request<http::empty_body> req(http::verb::get, "/crawl", 11);
    req.set(http::field::host, host);
    req.set(http::field::accept, "application/json");
    req.set(http::field::user_agent, "catalogue-tools/xprv");

    try
    {
        tcp_stream.expires_after(std::chrono::seconds(5));
        co_await http::async_write(stream, req, boost::asio::use_awaitable);
    }
    catch (std::exception const& e)
    {
        throw make_crawl_stage_error(std::string(scheme) + "-write", e.what());
    }

    beast::flat_buffer buffer;
    http::response_parser<http::string_body> parser;
    parser.body_limit(kMaxCrawlBodyBytes);
    try
    {
        tcp_stream.expires_after(std::chrono::seconds(10));
        co_await http::async_read(
            stream, buffer, parser, boost::asio::use_awaitable);
    }
    catch (std::exception const& e)
    {
        throw make_crawl_stage_error(std::string(scheme) + "-read", e.what());
    }

    auto res = parser.release();

    if (res.result() != http::status::ok)
    {
        throw make_crawl_http_error(std::string(scheme) + "-read", res);
    }

    CrawlResponse response;
    try
    {
        response = parse_peer_crawl_body(res.body());
    }
    catch (std::exception const& e)
    {
        throw make_crawl_stage_error(std::string(scheme) + "-parse", e.what());
    }
    response.used_tls = used_tls;

    PLOGD(
        log_,
        "Crawl ",
        host,
        " returned ",
        response.peers.size(),
        " public peers via ",
        used_tls ? "HTTPS" : "HTTP");

    co_return response;
}

boost::asio::awaitable<CrawlResponse>
fetch_crawl_https(std::string const& host, uint16_t port)
{
    auto executor = co_await boost::asio::this_coro::executor;

    ssl::context ctx(ssl::context::tls_client);
    ctx.set_verify_mode(ssl::verify_none);

    tcp::resolver resolver(executor);
    tcp::resolver::results_type endpoints;
    try
    {
        endpoints = co_await resolver.async_resolve(
            host, std::to_string(port), boost::asio::use_awaitable);
    }
    catch (std::exception const& e)
    {
        throw make_crawl_stage_error("https-resolve", e.what());
    }

    beast::ssl_stream<beast::tcp_stream> stream(executor, ctx);
    if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str()))
    {
        throw std::runtime_error("failed to set crawl SNI hostname");
    }

    auto& tcp_stream = beast::get_lowest_layer(stream);
    try
    {
        tcp_stream.expires_after(std::chrono::seconds(5));
        co_await tcp_stream.async_connect(
            endpoints, boost::asio::use_awaitable);
    }
    catch (std::exception const& e)
    {
        throw make_crawl_stage_error("https-connect", e.what());
    }

    try
    {
        tcp_stream.expires_after(std::chrono::seconds(5));
        co_await stream.async_handshake(
            ssl::stream_base::client, boost::asio::use_awaitable);
    }
    catch (std::exception const& e)
    {
        throw make_crawl_stage_error("https-tls", e.what());
    }

    auto response =
        co_await read_crawl_response(stream, tcp_stream, host, true);

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
    tcp::resolver::results_type endpoints;
    try
    {
        endpoints = co_await resolver.async_resolve(
            host, std::to_string(port), boost::asio::use_awaitable);
    }
    catch (std::exception const& e)
    {
        throw make_crawl_stage_error("http-resolve", e.what());
    }

    beast::tcp_stream stream(executor);
    try
    {
        stream.expires_after(std::chrono::seconds(5));
        co_await stream.async_connect(endpoints, boost::asio::use_awaitable);
    }
    catch (std::exception const& e)
    {
        throw make_crawl_stage_error("http-connect", e.what());
    }

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
    return parse_complete_ledgers_bounded(value, kMaxCrawlIntervalsPerPeer)
        .legacy_ranges;
}

CrawlResponse
parse_peer_crawl_body(std::string_view body, CrawlParseLimits const& limits)
{
    if (body.size() > kMaxCrawlBodyBytes)
        throw std::length_error("crawl response body exceeds 4 MiB limit");

    auto const root = boost::json::parse(body);
    auto const* root_obj = root.if_object();
    if (!root_obj)
        throw std::runtime_error("response root is not an object");

    CrawlResponse response;
    auto const effective_limits = CrawlParseLimits{
        .peers = std::min(limits.peers, kMaxCrawlPeersPerBatch),
        .intervals_per_peer =
            std::min(limits.intervals_per_peer, kMaxCrawlIntervalsPerPeer),
        .public_key_chars =
            std::min(limits.public_key_chars, kMaxCrawlPublicKeyChars)};
    auto const* overlay = root_obj->if_contains("overlay");
    if (!overlay || !overlay->is_object())
        return response;
    auto const* active = overlay->as_object().if_contains("active");
    if (!active || !active->is_array())
        return response;

    auto const& rows = active->as_array();
    response.reported_peer_rows = rows.size();
    auto const retained_rows = std::min(rows.size(), effective_limits.peers);
    response.truncated_peer_rows = rows.size() - retained_rows;
    response.peers.reserve(retained_rows);

    for (std::size_t index = 0; index < retained_rows; ++index)
    {
        auto const* obj = rows[index].if_object();
        if (!obj)
            continue;

        auto const* ip_value = obj->if_contains("ip");
        auto const* port_value = obj->if_contains("port");
        if (!ip_value || !port_value || !ip_value->is_string())
        {
            continue;
        }

        auto const& host_json = ip_value->as_string();
        auto const host_view =
            std::string_view(host_json.data(), host_json.size());
        if (host_view.find('\0') != std::string_view::npos)
            continue;
        auto const parsed_port = parse_peer_port(*port_value);
        if (!parsed_port)
            continue;

        CrawlPeer peer;
        peer.endpoint = format_endpoint(std::string(host_view), *parsed_port);

        if (auto const* key = obj->if_contains("public_key");
            key && key->is_string())
        {
            auto const& key_json = key->as_string();
            auto const key_view =
                std::string_view(key_json.data(), key_json.size());
            if (key_view.size() > effective_limits.public_key_chars)
            {
                peer.truncated = true;
                ++response.truncated_public_keys;
            }
            else if (auto reported = std::string(key_view);
                     base58::decode_node_public(reported))
            {
                peer.reported_public_key = std::move(reported);
            }
            else
            {
                ++response.invalid_public_keys;
            }
        }

        if (auto const* ledgers = obj->if_contains("complete_ledgers");
            ledgers && ledgers->is_string())
        {
            auto const& ledgers_json = ledgers->as_string();
            auto parsed = parse_complete_ledgers_bounded(
                std::string_view(ledgers_json.data(), ledgers_json.size()),
                effective_limits.intervals_per_peer);
            peer.complete_ledgers = std::move(parsed.legacy_ranges);
            peer.reported_intervals = std::move(parsed.reported_ranges);
            peer.truncated = peer.truncated || parsed.truncated != 0;
            response.truncated_interval_rows += parsed.truncated;
            response.malformed_interval_tokens += parsed.malformed;
        }

        response.peers.push_back(std::move(peer));
    }

    return response;
}

std::string
summarize_crawl_error(std::string_view error)
{
    auto summary = trim_copy(error);
    auto const diagnostic = summary.find(" [");
    if (diagnostic != std::string::npos)
        summary.erase(diagnostic);
    return summary;
}

boost::asio::awaitable<CrawlResponse>
detail::co_fetch_peer_crawl_with_stop(
    std::string host,
    uint16_t port,
    std::stop_token stop,
    CrawlFetchHooks hooks)
{
    CRAWL_TRACE(
        "Crawl fetch admitted host=",
        host,
        ":",
        port,
        " stage=http stop_requested=",
        stop.stop_requested() ? "yes" : "no");
    // Try HTTP first — XRPL/Xahau peer crawl endpoints serve plain HTTP
    // on the peer port. HTTPS wastes time on TLS handshake timeout.
    std::string http_summary;
    std::exception_ptr http_failure;
    try
    {
        co_return co_await fetch_crawl_http(host, port);
    }
    catch (std::exception const& http_error)
    {
        http_failure = std::current_exception();
        http_summary = strip_diagnostic(http_error.what());
        CRAWL_TRACE(
            "Crawl fetch host=",
            host,
            ":",
            port,
            " stage=http terminal=failure stop_requested=",
            stop.stop_requested() ? "yes" : "no",
            ": ",
            http_summary);
    }

    // The Asio cancellation edge can be consumed or translated by the nested
    // HTTP await. The PeerSet-issued stop token is the retained owner fact.
    if (hooks.before_stop_check)
        hooks.before_stop_check();

    //@@mutation-site crawl-stop-intent-prevents-https-fallback
    if (stop.stop_requested())
    {
        CRAWL_TRACE(
            "Crawl fetch host=",
            host,
            ":",
            port,
            " stage=http stop_observed=yes https_fallback=no");
        std::rethrow_exception(http_failure);
    }
    //@@mutation-end crawl-stop-intent-prevents-https-fallback

    if (hooks.before_https_fallback)
        hooks.before_https_fallback();

    CRAWL_TRACE(
        "Crawl fetch host=",
        host,
        ":",
        port,
        " stage=https fallback=yes stop_observed=no http_error=",
        http_summary,
        " — falling back to HTTPS");

    try
    {
        co_return co_await fetch_crawl_https(host, port);
    }
    catch (std::exception const& https_error)
    {
        throw std::runtime_error(
            "crawl fallback failed: http=" + http_summary +
            "; https=" + strip_diagnostic(https_error.what()));
    }
}

boost::asio::awaitable<CrawlResponse>
co_fetch_peer_crawl(std::string host, uint16_t port)
{
    co_return co_await detail::co_fetch_peer_crawl_with_stop(
        std::move(host), port, {}, {});
}

#undef CRAWL_TRACE

}  // namespace catl::peer_client
