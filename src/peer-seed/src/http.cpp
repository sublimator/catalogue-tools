#include "http.h"

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
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

    asio::io_context io;
    asio::ip::tcp::resolver resolver(io);
    beast::tcp_stream stream(io);
    stream.expires_after(std::chrono::seconds(timeout_secs));
    stream.connect(resolver.resolve(host, port));
    http::request<http::string_body> request(http::verb::post, target, 11);
    request.set(http::field::host, authority);
    request.set(http::field::user_agent, "catl-peer-seed/1");
    request.set(http::field::content_type, "application/json");
    request.body() = body;
    request.prepare_payload();
    http::write(stream, request);
    beast::flat_buffer buffer;
    http::response<http::string_body> response;
    http::read(stream, buffer, response);

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
