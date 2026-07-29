#pragma once

// Coroutine connect helpers for concrete PeerClient sessions.

#include "peer-client-coro.h"
#include "peer-client.h"

#include <chrono>
#include <functional>
#include <mutex>
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

struct ConnectAttemptTerminal
{
    ConnectAttemptDisposition disposition;
    boost::system::error_code error;
    uint32_t peer_seq = 0;
    int deadline_seconds = 0;
};

/// One first-wins cell shared by the PeerClient completion and connect
/// deadline executors. Neither consumer reconstructs cause from mutable
/// PeerClient state after this commit.
class ConnectAttemptState
{
public:
    //@@authority transition=prv.T0186 status=provisional scope=function
    // bind=catl::peer_client::detail::ConnectAttemptState::try_commit@18d343e8ef5b958c
    // owner=unresolved owner_hypothesis=connect_attempt
    // commit=mutex touches=prv.S0059 mutation_class=terminal_receipt
    // label=commit_first_connect_attempt_terminal_disposition
    bool
    try_commit(ConnectAttemptTerminal terminal)
    {
        std::lock_guard lock(mutex_);
        if (terminal_)
            return false;
        terminal_ = std::move(terminal);
        return true;
    }

    std::optional<ConnectAttemptTerminal>
    snapshot() const
    {
        std::lock_guard lock(mutex_);
        return terminal_;
    }

private:
    mutable std::mutex mutex_;
    //@@authority state=prv.S0059 status=provisional scope=field
    // bind=catl::peer_client::detail::ConnectAttemptState::terminal_@3a9e1ad6fc4a0214
    // owner=unresolved owner_hypothesis=connect_attempt
    // observed_home=connect_attempt
    // label=first_wins_initial_connect_attempt_terminal_result
    std::optional<ConnectAttemptTerminal> terminal_;
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
        {})
{
    auto executor = co_await asio::this_coro::executor;

    // Both timers as shared_ptr — they may outlive this coroutine frame
    // if cancel() races with async_wait completion on a different strand.
    auto signal = std::make_shared<asio::steady_timer>(
        executor, asio::steady_timer::time_point::max());
    auto deadline = std::make_shared<asio::steady_timer>(
        executor, std::chrono::seconds(timeout_secs));
    auto attempt = std::make_shared<detail::ConnectAttemptState>();

    auto signal_executor = signal->get_executor();
    auto user_on_complete = std::move(opts.on_complete);

    // Install the completion callback into opts — it fires on
    // PeerClient's strand, then reposts the wakeup onto this coroutine's
    // executor so the timer is only touched from its owning executor.
    opts.on_complete = [attempt,
                        signal_executor,
                        signal,
                        user_on_complete = std::move(user_on_complete)](
                           boost::system::error_code ec,
                           uint32_t seq,
                           ConnectAttemptDisposition disposition) mutable {
        auto const committed = attempt->try_commit(
            detail::ConnectAttemptTerminal{disposition, ec, seq, 0});
        if (committed)
        {
            asio::post(signal_executor, [signal]() { signal->cancel(); });
        }
        if (user_on_complete)
        {
            user_on_complete(ec, seq, disposition);
        }
    };

    out_client = PeerClient::connect(io_context, host, port, std::move(opts));
    if (on_client_created)
        on_client_created(out_client);
    auto client = out_client;

    auto arm_deadline = [deadline, signal, attempt, client](
                            std::chrono::seconds duration,
                            int total_timeout_seconds,
                            bool allow_post_handshake_grace) {
        deadline->expires_after(duration);
        deadline->async_wait(
            [signal,
             attempt,
             client,
             total_timeout_seconds,
             allow_post_handshake_grace](boost::system::error_code ec) {
                if (ec)
                    return;

                if (allow_post_handshake_grace && !attempt->snapshot() &&
                    client && detail::is_post_handshake_state(client->state()))
                {
                    // The first deadline grants one bounded status-exchange
                    // grace period. It wakes the owner but does not mint a
                    // terminal result.
                    signal->cancel();
                    return;
                }

                attempt->try_commit(detail::ConnectAttemptTerminal{
                    ConnectAttemptDisposition::deadline,
                    asio::error::timed_out,
                    0,
                    total_timeout_seconds});
                signal->cancel();
            });
    };

    arm_deadline(std::chrono::seconds(timeout_secs), timeout_secs, true);

    boost::system::error_code ec;
    co_await signal->async_wait(asio::redirect_error(asio::use_awaitable, ec));

    constexpr int post_handshake_grace_secs = 15;
    int total_timeout_secs = timeout_secs;
    auto terminal = attempt->snapshot();
    if (!terminal)
    {
        total_timeout_secs += post_handshake_grace_secs;
        arm_deadline(
            std::chrono::seconds(post_handshake_grace_secs),
            total_timeout_secs,
            false);
        co_await signal->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));
        terminal = attempt->snapshot();
    }

    deadline->cancel();

    if (!terminal)
        throw std::logic_error("connect attempt woke without terminal result");

    auto disposition = terminal->disposition;
    auto connect_ec = terminal->error;
    if (disposition == ConnectAttemptDisposition::ready &&
        client->state() != State::Ready)
    {
        switch (client->terminal_cause())
        {
            case PeerLogicalTerminalCause::peer_failure:
                disposition = ConnectAttemptDisposition::peer_failure;
                break;
            case PeerLogicalTerminalCause::owner_disconnect:
                disposition = ConnectAttemptDisposition::owner_disconnect;
                break;
            case PeerLogicalTerminalCause::none:
                disposition = ConnectAttemptDisposition::peer_failure;
                break;
        }
    }

    if (disposition != ConnectAttemptDisposition::ready)
    {
        std::string detail_text = detail::connect_phase_detail(client);
        auto const suppress_generic_ec =
            connect_ec == boost::asio::error::invalid_argument;
        if (client)
        {
            auto conn_detail =
                client->raw_connection().connect_failure_detail();
            if (!conn_detail.empty())
            {
                if (!suppress_generic_ec && connect_ec)
                    detail_text = detail::append_detail(
                        detail_text, connect_ec.message());
                if (detail_text != conn_detail)
                    detail_text =
                        detail::append_detail(detail_text, conn_detail);
            }
        }
        if (disposition == ConnectAttemptDisposition::deadline)
        {
            detail_text = detail::append_detail(
                std::move(detail_text),
                "deadline=" + std::to_string(total_timeout_secs) + "s");
            throw PeerClientException(
                Error::Timeout,
                std::move(detail_text),
                ConnectAttemptDisposition::deadline);
        }
        if (connect_ec)
            detail_text = detail::append_detail(
                std::move(detail_text), connect_ec.message());
        throw PeerClientException(
            Error::Disconnected, std::move(detail_text), disposition);
    }

    co_return terminal->peer_seq;
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
