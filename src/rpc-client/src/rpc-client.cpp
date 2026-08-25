#include <catl/rpc-client/rpc-client-coro.h>

#include <writ/home.hpp>

#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include <exception>
#include <optional>
#include <stdexcept>

namespace catl::rpc {

namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;

LogPartition RpcClient::log_("rpc-client", LogLevel::INHERIT);

namespace detail {

/// One callback adapter operation. All fields below are confined to the
/// operation-local strand supplied at construction. Foreign callback threads
/// can only post a candidate terminal; caller cancellation is classified by
/// the strand-born waiter. The first terminal task to commit wins.
class RpcAwaitState : public std::enable_shared_from_this<RpcAwaitState>
{
public:
    RpcAwaitState(writ::home home, RpcPublicationHook before_terminal_commit)
        : home_(std::move(home))
        , signal_(home_.executor(), asio::steady_timer::time_point::max())
        , before_terminal_commit_(std::move(before_terminal_commit))
    {
    }

    /// Foreign-thread gateway. Failure to enqueue cannot be converted into a
    /// strand-owned terminal without violating the serialization contract, so
    /// refuse a silent immortal waiter by terminating.
    void
    post_result(RpcResult result) noexcept
    {
        try
        {
            asio::post(
                home_.executor(),
                [self = shared_from_this(),
                 result = std::move(result)]() mutable noexcept {
                    self->commit_result(std::move(result));
                });
        }
        catch (...)
        {
            std::terminate();
        }
    }

    //@@authority transition=prv.T0351 status=provisional scope=function
    // bind=catl::rpc::detail::RpcAwaitState::wait@490ca1d8d4f81aae
    // owner=unresolved owner_hypothesis=rpc_callback_adapter
    // commit=rpc_callback_operation_strand touches=prv.S0143
    // writ_roles=retained_signal.subscribe,retained_signal.revalidate,retained_signal.deadline_or_cancel
    // label=consume_exact_rpc_terminal_or_caller_cancellation
    writ::home_awaitable<RpcResult>
    wait()
    {
        WRIT_ASSERT_HOME(home_);
        auto cancellation = co_await asio::this_coro::cancellation_state;
        if (cancellation.cancelled() != asio::cancellation_type::none)
            commit_canceled();

        if (terminal_ == Terminal::pending)
        {
            boost::system::error_code error;
            co_await signal_.async_wait(asio::redirect_error(
                asio::use_awaitable_t<writ::home::executor_type>{}, error));
            WRIT_ASSERT_HOME(home_);

            cancellation = co_await asio::this_coro::cancellation_state;
            if (terminal_ == Terminal::pending &&
                cancellation.cancelled() != asio::cancellation_type::none)
            {
                commit_canceled();
            }
            else if (terminal_ == Terminal::pending)
            {
                failure_ =
                    std::make_exception_ptr(boost::system::system_error(error));
                terminal_ = Terminal::failed;
            }
        }

        switch (terminal_)
        {
            case Terminal::completed:
                co_return std::move(*result_);
            case Terminal::failed:
                std::rethrow_exception(failure_);
            case Terminal::canceled:
                throw boost::system::system_error(
                    asio::error::operation_aborted);
            case Terminal::pending:
                break;
        }
        std::terminate();
    }

private:
    enum class Terminal : std::uint8_t {
        pending,
        completed,
        failed,
        canceled,
    };

    //@@authority transition=prv.T0350 status=provisional scope=function
    // bind=catl::rpc::detail::RpcAwaitState::commit_result@f87abd680b8fa033
    // owner=unresolved owner_hypothesis=rpc_callback_adapter
    // commit=rpc_callback_operation_strand touches=prv.S0143
    // writ_roles=home.serialized_commit,retained_signal.retain,retained_signal.publish
    // label=retain_rpc_result_or_publication_failure_before_wake
    void
    commit_result(RpcResult result) noexcept
    {
        WRIT_ASSERT_HOME(home_);
        if (terminal_ != Terminal::pending)
            return;

        try
        {
            result_.emplace(std::move(result));
            if (before_terminal_commit_)
                before_terminal_commit_();
            terminal_ = Terminal::completed;
        }
        catch (...)
        {
            result_.reset();
            failure_ = std::current_exception();
            terminal_ = Terminal::failed;
        }
        wake_waiter();
    }

    void
    commit_canceled() noexcept
    {
        WRIT_ASSERT_HOME(home_);
        if (terminal_ == Terminal::pending)
            terminal_ = Terminal::canceled;
    }

    void
    wake_waiter() noexcept
    {
        WRIT_ASSERT_HOME(home_);
        try
        {
            signal_.cancel();
        }
        catch (...)
        {
            std::terminate();
        }
    }

    writ::home home_;
    asio::steady_timer signal_;
    RpcPublicationHook before_terminal_commit_;
    std::optional<RpcResult> result_;
    std::exception_ptr failure_;
    //@@authority state=prv.S0143 status=provisional scope=field
    // bind=catl::rpc::detail::RpcAwaitState::terminal_@e5190587b98e4970
    // owner=unresolved owner_hypothesis=rpc_callback_adapter
    // observed_home=rpc_callback_operation_strand
    // label=retained_rpc_callback_operation_terminal
    Terminal terminal_ = Terminal::pending;
};

asio::awaitable<RpcResult>
callback_to_awaitable(
    std::function<void(RpcCallback)> initiator,
    RpcPublicationHook before_terminal_commit)
{
    auto caller_executor = co_await asio::this_coro::executor;
    writ::home operation_home(asio::make_strand(caller_executor));

    // clang-format off
    //@@authority transition=prv.T0349 status=provisional scope=statement calls=spawn bind=spawn@5baf1e803a05960d
    // owner=unresolved owner_hypothesis=rpc_callback_adapter
    // commit=rpc_callback_operation_strand touches=prv.S0143
    // writ_roles=home.birth
    // label=establish_rpc_callback_operation_owner_birth
    co_return co_await operation_home.spawn(
        [operation_home,
         initiator = std::move(initiator),
         before_terminal_commit = std::move(
             before_terminal_commit)]() mutable
            -> writ::home_awaitable<RpcResult> {
            WRIT_ASSERT_HOME(operation_home);
            auto state = std::make_shared<RpcAwaitState>(
                operation_home, std::move(before_terminal_commit));
            initiator([state](RpcResult result) noexcept {
                state->post_result(std::move(result));
            });
            co_return co_await state->wait();
        }); //@@authority-target ids=prv.T0349
    // clang-format on
}

}  // namespace detail

detail::RpcConcurrencyGate::RpcConcurrencyGate(
    asio::any_io_executor executor,
    int capacity)
    : permits_(std::move(executor), capacity > 0 ? capacity : 1)
{
    if (capacity <= 0)
        throw std::invalid_argument("RPC concurrency must be positive");
    for (int i = 0; i < capacity; ++i)
    {
        if (!permits_.try_send(boost::system::error_code{}))
            throw std::logic_error("failed to seed RPC concurrency permits");
    }
}

asio::awaitable<void>
detail::RpcConcurrencyGate::acquire()
{
    boost::system::error_code ec;
    co_await permits_.async_receive(
        asio::redirect_error(asio::use_awaitable, ec));
    if (ec)
        throw boost::system::system_error(ec);
}

void
detail::RpcConcurrencyGate::release() noexcept
{
    try
    {
        if (!permits_.try_send(boost::system::error_code{}))
            std::terminate();
    }
    catch (...)
    {
        std::terminate();
    }
}

RpcClient::RpcClient(
    asio::io_context& io,
    std::string host,
    uint16_t port,
    int max_concurrent)
    : io_(io)
    , host_(std::move(host))
    , port_(port)
    , concurrency_gate_(std::make_shared<detail::RpcConcurrencyGate>(
          io.get_executor(),
          max_concurrent))
{
}

//@@authority transition=prv.T0352 status=provisional scope=function
// bind=catl::rpc::call@25401f75a8cb0bda
// owner=unresolved owner_hypothesis=rpc_client
// commit=rpc_request_completion_handler
// label=publish_request_frame_terminal_through_observable_completion
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
    auto concurrency_gate = concurrency_gate_;

    boost::asio::co_spawn(
        io_,
        [host,
         port,
         concurrency_gate = std::move(concurrency_gate),
         body_str =
             std::move(body_str)]() mutable -> asio::awaitable<RpcResult> {
            RpcResult rpc_result;
            struct PermitReceipt
            {
                std::shared_ptr<detail::RpcConcurrencyGate> gate;
                bool held = false;

                ~PermitReceipt()
                {
                    release();
                }

                void
                release() noexcept
                {
                    if (!held)
                        return;
                    held = false;
                    gate->release();
                }
            } permit{concurrency_gate};

            try
            {
                co_await concurrency_gate->acquire();
                permit.held = true;
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

                // Set SNI hostname (required by Cloudflare and most modern TLS
                // servers)
                SSL_set_tlsext_host_name(stream.native_handle(), host.c_str());

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
                PLOGD(RpcClient::log_, "  body: ", res.body());

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

            permit.release();
            co_return rpc_result;
        },
        [callback = std::move(callback)](
            std::exception_ptr failure, RpcResult result) mutable {
            if (failure)
            {
                result.success = false;
                try
                {
                    std::rethrow_exception(failure);
                }
                catch (std::exception const& error)
                {
                    result.error = error.what();
                }
                catch (...)
                {
                    result.error = "unknown RPC request failure";
                }
            }
            callback(std::move(result));
        });
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
