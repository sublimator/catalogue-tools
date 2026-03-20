#include <catl/rpc-client/rpc-client.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

namespace catl::rpc {

namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;

LogPartition RpcClient::log_("rpc-client", LogLevel::DEBUG);

RpcClient::RpcClient(asio::io_context& io, std::string host, uint16_t port)
    : io_(io), host_(std::move(host)), port_(port)
{
}

void
RpcClient::call(
    std::string const& method,
    boost::json::object params,
    RpcCallback callback)
{
    PLOGD(log_, "call: ", method, " → ", host_, ":", port_);

    // Build JSON-RPC request body
    boost::json::object request_body;
    request_body["method"] = method;
    request_body["params"] = boost::json::array{boost::json::value(params)};
    auto body_str = boost::json::serialize(request_body);

    PLOGT(log_, "  request: ", body_str.substr(0, 200));

    // Run the HTTP request as a coroutine, deliver result via callback
    auto host = host_;
    auto port = port_;

    boost::asio::co_spawn(
        io_,
        [host,
         port,
         body_str = std::move(body_str),
         callback = std::move(callback)]() mutable -> asio::awaitable<void> {
            RpcResult rpc_result;

            try
            {
                auto executor = co_await asio::this_coro::executor;

                // SSL context
                ssl::context ctx(ssl::context::tlsv12_client);
                ctx.set_verify_mode(ssl::verify_none);

                // Resolve
                tcp::resolver resolver(executor);
                auto endpoints = co_await resolver.async_resolve(
                    host, std::to_string(port), asio::use_awaitable);

                // Connect
                beast::ssl_stream<beast::tcp_stream> stream(executor, ctx);
                auto& tcp_stream = beast::get_lowest_layer(stream);
                tcp_stream.expires_after(std::chrono::seconds(10));
                co_await tcp_stream.async_connect(
                    endpoints, asio::use_awaitable);

                // SSL handshake
                tcp_stream.expires_after(std::chrono::seconds(10));
                co_await stream.async_handshake(
                    ssl::stream_base::client, asio::use_awaitable);

                // Build HTTP request
                http::request<http::string_body> req(http::verb::post, "/", 11);
                req.set(http::field::host, host);
                req.set(http::field::content_type, "application/json");
                req.body() = body_str;
                req.prepare_payload();

                // Send
                tcp_stream.expires_after(std::chrono::seconds(10));
                co_await http::async_write(stream, req, asio::use_awaitable);

                // Receive
                beast::flat_buffer buffer;
                http::response<http::string_body> res;
                tcp_stream.expires_after(std::chrono::seconds(30));
                co_await http::async_read(
                    stream, buffer, res, asio::use_awaitable);

                PLOGD(
                    RpcClient::log_,
                    "  response: ",
                    res.result_int(),
                    " ",
                    res.body().size(),
                    " bytes");
                PLOGT(RpcClient::log_, "  body: ", res.body().substr(0, 500));

                // Parse JSON
                auto jv = boost::json::parse(res.body());
                auto const& obj = jv.as_object();

                if (obj.contains("result"))
                {
                    rpc_result.result = obj.at("result");
                    auto const& result_obj = rpc_result.result.as_object();
                    if (result_obj.contains("status"))
                    {
                        rpc_result.success =
                            (result_obj.at("status").as_string() == "success");
                    }
                    if (result_obj.contains("error_message"))
                    {
                        rpc_result.error = std::string(
                            result_obj.at("error_message").as_string());
                    }
                    else if (result_obj.contains("error"))
                    {
                        rpc_result.error =
                            std::string(result_obj.at("error").as_string());
                    }
                }
                else
                {
                    rpc_result.error = "no 'result' field in response";
                }

                if (!rpc_result.success)
                {
                    PLOGW(RpcClient::log_, "  RPC error: ", rpc_result.error);
                }
                else
                {
                    PLOGD(RpcClient::log_, "  RPC success");
                }

                // Graceful SSL shutdown (best-effort)
                boost::system::error_code ec;
                tcp_stream.expires_after(std::chrono::seconds(2));
                co_await stream.async_shutdown(
                    asio::redirect_error(asio::use_awaitable, ec));
            }
            catch (std::exception const& e)
            {
                PLOGE(RpcClient::log_, "  HTTP/SSL error: ", e.what());
                rpc_result.error = e.what();
                rpc_result.success = false;
            }

            callback(std::move(rpc_result));
        },
        asio::detached);
}

void
RpcClient::server_definitions(RpcCallback callback)
{
    PLOGI(log_, "fetching server_definitions from ", host_, ":", port_);
    call("server_definitions", {}, std::move(callback));
}

void
RpcClient::tx(std::string const& tx_hash, RpcCallback callback)
{
    PLOGI(log_, "fetching tx: ", tx_hash.substr(0, 16), "...");
    boost::json::object params;
    params["transaction"] = tx_hash;
    params["binary"] = false;
    call("tx", params, std::move(callback));
}

}  // namespace catl::rpc
