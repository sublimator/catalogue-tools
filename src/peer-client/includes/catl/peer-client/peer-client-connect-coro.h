#pragma once

// Coroutine connect helpers for concrete PeerClient sessions.

#include "peer-client-coro.h"
#include "peer-client.h"

#include <algorithm>
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

inline char const*
connect_deadline_reason_name(ConnectDeadlineReason reason)
{
    switch (reason)
    {
        case ConnectDeadlineReason::none:
            return "unspecified";
        case ConnectDeadlineReason::initial:
            return "initial";
        case ConnectDeadlineReason::sampled_inactivity:
            return "sampled_inactivity";
        case ConnectDeadlineReason::absolute:
            return "absolute";
    }
    return "unknown";
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

enum class ConnectDeadlineDecisionKind : std::uint8_t {
    stop,
    arm_at,
    commit_initial_timeout,
    commit_inactivity_timeout,
    commit_absolute_timeout,
};

struct ConnectDeadlineDecision
{
    ConnectDeadlineDecisionKind kind = ConnectDeadlineDecisionKind::stop;
    std::chrono::steady_clock::time_point arm_time{};
    std::optional<InboundFrameProgress> baseline{};
};

/// Clock-parameterized deadline policy. It owns no Asio object and can be
/// driven at exact synthetic time points by unit tests.
class ConnectDeadlinePolicy
{
public:
    using Clock = std::chrono::steady_clock;

    ConnectDeadlinePolicy(
        Clock::duration initial_timeout,
        Clock::duration inactivity_timeout,
        Clock::time_point absolute_deadline)
        : initial_timeout_(initial_timeout)
        , inactivity_timeout_(inactivity_timeout)
        , absolute_deadline_(absolute_deadline)
    {
    }

    ConnectDeadlineDecision
    start(Clock::time_point now, bool attempt_is_terminal) const
    {
        if (attempt_is_terminal)
            return {};
        if (now >= absolute_deadline_)
            return {ConnectDeadlineDecisionKind::commit_absolute_timeout};
        return arm(now + initial_timeout_);
    }

    //@@authority transition=prv.T0231 status=provisional scope=function
    // bind=catl::peer_client::detail::ConnectDeadlinePolicy::evaluate@945c28a71c131dc1
    // owner=unresolved owner_hypothesis=connect_deadline_controller
    // commit=deadline_strand touches=prv.S0078,prv.S0079
    // label=sample_inbound_frame_progress_and_choose_bounded_deadline_action
    ConnectDeadlineDecision
    evaluate(
        Clock::time_point now,
        State state,
        bool attempt_is_terminal,
        InboundFrameProgress progress)
    {
        if (attempt_is_terminal)
            return {};
        if (now >= absolute_deadline_)
            return {ConnectDeadlineDecisionKind::commit_absolute_timeout};

        if (!baseline_)
        {
            if (!is_post_handshake_state(state))
                return {ConnectDeadlineDecisionKind::commit_initial_timeout};
            baseline_ = progress;
            return arm(now + inactivity_timeout_);
        }

        if (!is_post_handshake_state(state) ||
            !progress.advanced_from(*baseline_))
            return {ConnectDeadlineDecisionKind::commit_inactivity_timeout};

        baseline_ = progress;
        return arm(now + inactivity_timeout_);
    }

    Clock::time_point
    absolute_deadline() const noexcept
    {
        return absolute_deadline_;
    }

private:
    ConnectDeadlineDecision
    arm(Clock::time_point desired) const
    {
        return {
            ConnectDeadlineDecisionKind::arm_at,
            std::min(desired, absolute_deadline_),
            baseline_};
    }

    Clock::duration initial_timeout_;
    Clock::duration inactivity_timeout_;
    Clock::time_point absolute_deadline_;
    std::optional<InboundFrameProgress> baseline_;
};

/// Owns the connect deadline timer on one private strand. Terminal delivery
/// is separate retained state in ConnectAttemptState; cancelling this timer is
/// cleanup only and never acts as the result notification.
class ConnectDeadlineController
    : public std::enable_shared_from_this<ConnectDeadlineController>
{
public:
    using Clock = std::chrono::steady_clock;
    using BeforeArmHook = std::function<void(bool initial_phase)>;

    ConnectDeadlineController(
        asio::any_io_executor executor,
        std::shared_ptr<ConnectAttemptState> attempt,
        std::weak_ptr<PeerClient> client,
        int timeout_seconds,
        Clock::duration post_handshake_inactivity = std::chrono::seconds(15),
        int post_handshake_inactivity_seconds = 15,
        BeforeArmHook before_arm = {},
        std::optional<Clock::time_point> absolute_deadline = {})
        : strand_(asio::make_strand(std::move(executor)))
        , timer_(strand_)
        , attempt_(std::move(attempt))
        , client_(std::move(client))
        , started_at_(Clock::now())
        , policy_(
              std::chrono::seconds(timeout_seconds),
              post_handshake_inactivity,
              absolute_deadline.value_or(
                  started_at_ + std::chrono::seconds(60)))
        , timeout_seconds_(timeout_seconds)
        , post_handshake_inactivity_seconds_(post_handshake_inactivity_seconds)
        , before_arm_(std::move(before_arm))
    {
    }

    void
    start()
    {
        auto self = shared_from_this();
        asio::dispatch(strand_, [self]() {
            self->apply_or_fail(self->policy_.start(
                Clock::now(), static_cast<bool>(self->attempt_->snapshot())));
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

    Clock::time_point
    expiry_for_test() const noexcept
    {
        return timer_.expiry();
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
    apply_or_fail(ConnectDeadlineDecision decision) noexcept
    {
        try
        {
            apply(decision);
        }
        catch (...)
        {
            //@@mutation-site connect-deadline-arm-failure-publishes
            publish_infrastructure_failure(std::current_exception());
            //@@mutation-end connect-deadline-arm-failure-publishes
        }
    }

    void
    apply(ConnectDeadlineDecision const& decision)
    {
        switch (decision.kind)
        {
            case ConnectDeadlineDecisionKind::stop:
                return;
            case ConnectDeadlineDecisionKind::commit_initial_timeout:
                commit_deadline(
                    ConnectDeadlineReason::initial, timeout_seconds_);
                return;
            case ConnectDeadlineDecisionKind::commit_inactivity_timeout:
                commit_deadline(
                    ConnectDeadlineReason::sampled_inactivity,
                    timeout_seconds_ + post_handshake_inactivity_seconds_);
                return;
            case ConnectDeadlineDecisionKind::commit_absolute_timeout:
                commit_deadline(
                    ConnectDeadlineReason::absolute,
                    static_cast<int>(
                        std::chrono::duration_cast<std::chrono::seconds>(
                            policy_.absolute_deadline() - started_at_)
                            .count()));
                return;
            case ConnectDeadlineDecisionKind::arm_at:
                break;
        }

        // A terminal can race the pure decision on another strand. Refuse the
        // ordinary stale arm here. If it wins after this check, at most one
        // transient timer is installed; its handler rechecks terminality.
        if (attempt_->snapshot())
            return;
        if (before_arm_)
            before_arm_(!decision.baseline.has_value());
        timer_.expires_at(decision.arm_time);
        auto self = shared_from_this();
        timer_.async_wait([self](boost::system::error_code ec) {
            try
            {
                if (ec)
                    return;
                auto client = self->client_.lock();
                auto const terminal =
                    static_cast<bool>(self->attempt_->snapshot());
                auto const state =
                    client ? client->state() : State::Disconnected;
                auto const progress = client ? client->inbound_frame_progress()
                                             : InboundFrameProgress{};
                self->apply_or_fail(self->policy_.evaluate(
                    Clock::now(), state, terminal, progress));
            }
            catch (...)
            {
                self->publish_infrastructure_failure(std::current_exception());
            }
        });
    }

    void
    commit_deadline(ConnectDeadlineReason reason, int seconds) noexcept
    {
        attempt_->try_commit_initial(
            ConnectAttemptTerminal{
                ConnectAttemptTerminalKind::deadline,
                asio::error::timed_out,
                0,
                std::max(0, seconds),
                {},
                reason},
            []() noexcept {});
    }

    asio::strand<asio::any_io_executor> strand_;
    asio::steady_timer timer_;
    std::shared_ptr<ConnectAttemptState> attempt_;
    std::weak_ptr<PeerClient> client_;
    Clock::time_point started_at_;
    //@@authority state=prv.S0079 status=provisional scope=field
    // bind=catl::peer_client::detail::ConnectDeadlineController::policy_@ba21c58744d40dde
    // owner=unresolved owner_hypothesis=connect_deadline_controller
    // observed_home=connect_deadline_controller_strand
    // label=connect_progress_baseline_and_absolute_deadline_policy
    ConnectDeadlinePolicy policy_;
    int timeout_seconds_;
    int post_handshake_inactivity_seconds_;
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

inline asio::awaitable<uint32_t>
co_connect_with_setup_hooks(
    asio::io_context& io_context,
    std::string const& host,
    uint16_t port,
    PeerClient::ConnectOptions opts,
    std::shared_ptr<PeerClient>& out_client,
    int timeout_secs,
    std::function<void(std::shared_ptr<PeerClient> const&)> on_client_created,
    ConnectAttemptSetupHooks setup_hooks);

}  // namespace detail

//------------------------------------------------------------------------------
// co_connect — await connection + status exchange
//
// Returns the peer's ledger seq when ready.
//------------------------------------------------------------------------------

inline asio::awaitable<uint32_t>
detail::co_connect_with_setup_hooks(
    asio::io_context& io_context,
    std::string const& host,
    uint16_t port,
    PeerClient::ConnectOptions opts,
    std::shared_ptr<PeerClient>& out_client,
    int timeout_secs,
    std::function<void(std::shared_ptr<PeerClient> const&)> on_client_created,
    ConnectAttemptSetupHooks setup_hooks)
{
    auto executor = co_await asio::this_coro::executor;

    // Mint the immutable outer bound before PeerClient starts any work. A
    // targeted caller may have supplied its own already-running request
    // deadline; ordinary connects receive the named 60-second policy bound.
    auto const connect_absolute_deadline =
        opts.initial_connect_absolute_deadline.value_or(
            std::chrono::steady_clock::now() + std::chrono::seconds(60));

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
            std::move(setup_hooks.before_deadline_arm),
            connect_absolute_deadline);
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
                "deadline=" + std::to_string(terminal->deadline_seconds) +
                    "s; deadline_reason=" +
                    detail::connect_deadline_reason_name(
                        terminal->deadline_reason));
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

/// Public API-compatible entry point. Test-only setup fault injection belongs
/// to detail::co_connect_with_setup_hooks and must not change this function
/// type.
inline asio::awaitable<uint32_t>
co_connect(
    asio::io_context& io_context,
    std::string const& host,
    uint16_t port,
    PeerClient::ConnectOptions opts,
    std::shared_ptr<PeerClient>& out_client,
    int timeout_secs = 10,
    std::function<void(std::shared_ptr<PeerClient> const&)> on_client_created =
        {})
{
    co_return co_await detail::co_connect_with_setup_hooks(
        io_context,
        host,
        port,
        std::move(opts),
        out_client,
        timeout_secs,
        std::move(on_client_created),
        {});
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
