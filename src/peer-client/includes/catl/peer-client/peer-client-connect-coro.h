#pragma once

// Coroutine connect helpers for concrete PeerClient sessions.

#include "peer-client-coro.h"
#include "peer-client.h"

#include <chrono>
#include <functional>
#include <optional>
#include <string>

namespace catl::peer_client {

namespace asio = boost::asio;

namespace detail {

inline char const*
connect_state_name(State state)
{
    switch (state)
    {
        case State::Disconnected:
            return "Disconnected";
        case State::Connecting:
            return "Connecting";
        case State::Connected:
            return "Connected";
        case State::ExchangingStatus:
            return "ExchangingStatus";
        case State::Ready:
            return "Ready";
        case State::Failed:
            return "Failed";
    }
    return "Unknown";
}

inline bool
is_post_handshake_state(State state)
{
    return state == State::Connected || state == State::ExchangingStatus;
}

inline std::string
connect_phase_detail(std::shared_ptr<PeerClient> const& client)
{
    if (!client)
        return {};
    return std::string("phase=") + connect_state_name(client->state());
}

inline std::string
append_detail(std::string lhs, std::string rhs)
{
    if (lhs.empty())
        return rhs;
    if (rhs.empty())
        return lhs;
    return lhs + "; " + rhs;
}

/// Owns the connect deadline timer on one private strand. Terminal delivery
/// is separate retained state in ConnectAttemptState; cancelling this timer is
/// cleanup only and never acts as the result notification.
class ConnectDeadlineController
    : public std::enable_shared_from_this<ConnectDeadlineController>
{
public:
    using BeforeArmHook = std::function<void(bool allow_post_handshake_grace)>;

    ConnectDeadlineController(
        asio::any_io_executor executor,
        std::shared_ptr<ConnectAttemptState> attempt,
        std::weak_ptr<PeerClient> client,
        int timeout_seconds,
        std::chrono::steady_clock::duration post_handshake_grace =
            std::chrono::seconds(15),
        int post_handshake_grace_seconds = 15,
        BeforeArmHook before_arm = {})
        : strand_(asio::make_strand(std::move(executor)))
        , timer_(strand_)
        , attempt_(std::move(attempt))
        , client_(std::move(client))
        , timeout_seconds_(timeout_seconds)
        , post_handshake_grace_(post_handshake_grace)
        , post_handshake_grace_seconds_(post_handshake_grace_seconds)
        , before_arm_(std::move(before_arm))
    {
    }

    void
    start()
    {
        auto self = shared_from_this();
        asio::dispatch(strand_, [self]() {
            self->arm_or_fail(
                std::chrono::seconds(self->timeout_seconds_), true);
        });
    }

    void
    cancel() noexcept
    {
        try
        {
            auto self = shared_from_this();
            asio::post(strand_, [self]() noexcept {
                try
                {
                    self->timer_.cancel();
                }
                catch (...)
                {
                }
            });
        }
        catch (...)
        {
        }
    }

private:
    void
    publish_infrastructure_failure(std::exception_ptr failure) noexcept
    {
        attempt_->try_commit_initial(
            ConnectAttemptTerminal{
                ConnectAttemptTerminalKind::infrastructure_failure,
                {},
                0,
                0,
                std::move(failure)},
            []() noexcept {});
    }

    void
    arm_or_fail(
        std::chrono::steady_clock::duration duration,
        bool allow_post_handshake_grace) noexcept
    {
        try
        {
            arm(duration, allow_post_handshake_grace);
        }
        catch (...)
        {
            //@@mutation-site connect-deadline-arm-failure-publishes
            publish_infrastructure_failure(std::current_exception());
            //@@mutation-end connect-deadline-arm-failure-publishes
        }
    }

    void
    arm(std::chrono::steady_clock::duration duration,
        bool allow_post_handshake_grace)
    {
        if (before_arm_)
            before_arm_(allow_post_handshake_grace);
        timer_.expires_after(duration);
        auto self = shared_from_this();
        timer_.async_wait([self, allow_post_handshake_grace](
                              boost::system::error_code ec) {
            try
            {
                if (ec)
                    return;

                auto client = self->client_.lock();
                if (allow_post_handshake_grace && !self->attempt_->snapshot() &&
                    client && is_post_handshake_state(client->state()))
                {
                    self->timeout_seconds_ +=
                        self->post_handshake_grace_seconds_;
                    //@@mutation-site connect-deadline-grace-is-bounded
                    self->arm_or_fail(self->post_handshake_grace_, false);
                    //@@mutation-end connect-deadline-grace-is-bounded
                    return;
                }

                self->attempt_->try_commit_initial(
                    ConnectAttemptTerminal{
                        ConnectAttemptTerminalKind::deadline,
                        asio::error::timed_out,
                        0,
                        self->timeout_seconds_},
                    []() noexcept {});
            }
            catch (...)
            {
                self->publish_infrastructure_failure(std::current_exception());
            }
        });
    }

    asio::strand<asio::any_io_executor> strand_;
    asio::steady_timer timer_;
    std::shared_ptr<ConnectAttemptState> attempt_;
    std::weak_ptr<PeerClient> client_;
    int timeout_seconds_;
    std::chrono::steady_clock::duration post_handshake_grace_;
    int post_handshake_grace_seconds_;
    BeforeArmHook before_arm_;
};

struct ConnectAttemptSetupHooks
{
    std::function<void()> before_deadline_construction;
    std::function<void()> before_deadline_start;
    std::function<void()> before_terminal_wait;
    ConnectDeadlineController::BeforeArmHook before_deadline_arm;
};

/// Converts inherited coroutine cancellation into a retained attempt result.
/// Returning none shields the composed connect and its physical cleanup from
/// edge-triggered cancellation after the request has been recorded.
class ConnectCancellationFilter
{
public:
    explicit ConnectCancellationFilter(
        std::shared_ptr<ConnectAttemptState> attempt)
        : attempt_(std::move(attempt))
    {
    }

    asio::cancellation_type_t
    operator()(asio::cancellation_type_t type) const noexcept
    {
        if (type != asio::cancellation_type::none)
        {
            attempt_->try_commit_initial(
                ConnectAttemptTerminal{
                    ConnectAttemptTerminalKind::caller_cancelled,
                    asio::error::operation_aborted,
                    0,
                    0},
                []() noexcept {});
        }
        return asio::cancellation_type::none;
    }

private:
    std::shared_ptr<ConnectAttemptState> attempt_;
};

}  // namespace detail

//------------------------------------------------------------------------------
// co_connect — await connection + status exchange
//
// Returns the peer's ledger seq when ready.
//------------------------------------------------------------------------------

inline asio::awaitable<uint32_t>
co_connect(
    asio::io_context& io_context,
    std::string const& host,
    uint16_t port,
    PeerClient::ConnectOptions opts,
    std::shared_ptr<PeerClient>& out_client,
    int timeout_secs = 10,
    std::function<void(std::shared_ptr<PeerClient> const&)> on_client_created =
        {},
    detail::ConnectAttemptSetupHooks setup_hooks = {})
{
    auto executor = co_await asio::this_coro::executor;

    out_client = PeerClient::connect(io_context, host, port, std::move(opts));
    auto client = out_client;
    auto attempt = client->initial_connect_attempt();

    std::shared_ptr<detail::ConnectDeadlineController> deadline;
    std::exception_ptr setup_failure;
    try
    {
        auto cancellation_filter = detail::ConnectCancellationFilter{attempt};
        co_await asio::this_coro::reset_cancellation_state(cancellation_filter);

        if (on_client_created)
            on_client_created(client);

        if (setup_hooks.before_deadline_construction)
            setup_hooks.before_deadline_construction();
        deadline = std::make_shared<detail::ConnectDeadlineController>(
            executor,
            attempt,
            client,
            timeout_secs,
            std::chrono::seconds(15),
            15,
            std::move(setup_hooks.before_deadline_arm));
        if (setup_hooks.before_deadline_start)
            setup_hooks.before_deadline_start();
        deadline->start();
        if (setup_hooks.before_terminal_wait)
            setup_hooks.before_terminal_wait();
    }
    catch (...)
    {
        setup_failure = std::current_exception();
    }
    if (setup_failure)
    {
        if (deadline)
            deadline->cancel();
        co_await client->co_disconnect();
        std::rethrow_exception(setup_failure);
    }

    boost::system::error_code receive_ec;
    std::exception_ptr receive_failure;
    try
    {
        receive_ec = co_await attempt->wait_terminal();
    }
    catch (...)
    {
        receive_failure = std::current_exception();
    }
    if (receive_failure)
    {
        deadline->cancel();
        co_await client->co_disconnect();
        std::rethrow_exception(receive_failure);
    }
    auto const terminal = attempt->snapshot();

    if (!terminal)
    {
        if (receive_ec)
            throw boost::system::system_error(receive_ec);
        throw std::logic_error("connect attempt woke without terminal result");
    }

    deadline->cancel();

    if (terminal->kind == detail::ConnectAttemptTerminalKind::caller_cancelled)
    {
        co_await client->co_disconnect();
        throw PeerClientException(
            Error::Cancelled, "initial connect caller cancelled");
    }

    if (terminal->kind == detail::ConnectAttemptTerminalKind::ready)
        co_return terminal->peer_seq;

    if (terminal->kind ==
        detail::ConnectAttemptTerminalKind::infrastructure_failure)
    {
        //@@mutation-site connect-infrastructure-failure-joins-physical-drain
        co_await client->co_disconnect();
        //@@mutation-end connect-infrastructure-failure-joins-physical-drain
        if (terminal->failure)
            std::rethrow_exception(terminal->failure);
        throw std::runtime_error(
            "connect attempt infrastructure failed without exception");
    }

    if (terminal->kind != detail::ConnectAttemptTerminalKind::ready)
    {
        std::string detail_text = detail::connect_phase_detail(client);
        auto const suppress_generic_ec =
            terminal->error == boost::asio::error::invalid_argument;
        if (client)
        {
            auto conn_detail =
                client->raw_connection().connect_failure_detail();
            if (!conn_detail.empty())
            {
                if (!suppress_generic_ec && terminal->error)
                    detail_text = detail::append_detail(
                        detail_text, terminal->error.message());
                if (detail_text != conn_detail)
                    detail_text =
                        detail::append_detail(detail_text, conn_detail);
            }
        }
        if (terminal->kind == detail::ConnectAttemptTerminalKind::deadline)
        {
            detail_text = detail::append_detail(
                std::move(detail_text),
                "deadline=" + std::to_string(terminal->deadline_seconds) + "s");
            throw PeerClientException(
                Error::Timeout,
                std::move(detail_text),
                ConnectAttemptDisposition::deadline);
        }
        if (terminal->error)
            detail_text = detail::append_detail(
                std::move(detail_text), terminal->error.message());
        throw PeerClientException(
            Error::Disconnected,
            std::move(detail_text),
            terminal->kind ==
                    detail::ConnectAttemptTerminalKind::owner_disconnect
                ? ConnectAttemptDisposition::owner_disconnect
                : ConnectAttemptDisposition::peer_failure);
    }

    throw std::logic_error("unhandled connect attempt disposition");
}

/// Legacy overload — wraps into ConnectOptions.
inline asio::awaitable<uint32_t>
co_connect(
    asio::io_context& io_context,
    std::string const& host,
    uint16_t port,
    uint32_t network_id,
    std::shared_ptr<PeerClient>& out_client,
    int timeout_secs = 10)
{
    PeerClient::ConnectOptions opts;
    opts.network_id = network_id;
    co_return co_await co_connect(
        io_context, host, port, std::move(opts), out_client, timeout_secs);
}

inline asio::awaitable<uint32_t>
co_connect(
    asio::io_context& io_context,
    std::string const& host,
    uint16_t port,
    PeerClient::ConnectOptions opts,
    PeerSessionPtr& out_client,
    int timeout_secs = 10)
{
    std::shared_ptr<PeerClient> concrete;
    auto const peer_seq = co_await co_connect(
        io_context, host, port, std::move(opts), concrete, timeout_secs);
    out_client = std::move(concrete);
    co_return peer_seq;
}

inline asio::awaitable<uint32_t>
co_connect(
    asio::io_context& io_context,
    std::string const& host,
    uint16_t port,
    uint32_t network_id,
    PeerSessionPtr& out_client,
    int timeout_secs = 10)
{
    std::shared_ptr<PeerClient> concrete;
    auto const peer_seq = co_await co_connect(
        io_context, host, port, network_id, concrete, timeout_secs);
    out_client = std::move(concrete);
    co_return peer_seq;
}

}  // namespace catl::peer_client
