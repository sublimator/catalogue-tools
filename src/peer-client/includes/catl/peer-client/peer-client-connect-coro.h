#pragma once

// Coroutine connect helpers for concrete PeerClient sessions.

#include "peer-client-coro.h"
#include "peer-client.h"

#include <atomic>
#include <chrono>
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
    int timeout_secs = 10)
{
    auto executor = co_await asio::this_coro::executor;

    // Both timers as shared_ptr — they may outlive this coroutine frame
    // if cancel() races with async_wait completion on a different strand.
    auto signal = std::make_shared<asio::steady_timer>(
        executor, asio::steady_timer::time_point::max());
    auto deadline = std::make_shared<asio::steady_timer>(
        executor, std::chrono::seconds(timeout_secs));
    auto peer_seq = std::make_shared<std::atomic<uint32_t>>(0);
    auto connect_ec = std::make_shared<boost::system::error_code>();
    auto timed_out = std::make_shared<bool>(false);

    auto signal_executor = signal->get_executor();
    auto user_on_complete = std::move(opts.on_complete);

    // Install the completion callback into opts — it fires on
    // PeerClient's strand, then reposts the wakeup onto this coroutine's
    // executor so the timer is only touched from its owning executor.
    opts.on_complete = [peer_seq,
                        connect_ec,
                        signal_executor,
                        signal,
                        user_on_complete = std::move(user_on_complete)](
                           boost::system::error_code ec, uint32_t seq) mutable {
        *connect_ec = ec;
        if (!ec)
        {
            peer_seq->store(seq);
        }
        if (user_on_complete)
        {
            user_on_complete(ec, seq);
        }
        asio::post(signal_executor, [signal]() { signal->cancel(); });
    };

    out_client = PeerClient::connect(io_context, host, port, std::move(opts));

    auto arm_deadline =
        [deadline, signal, timed_out](std::chrono::seconds duration) {
            *timed_out = false;
            deadline->expires_after(duration);
            deadline->async_wait([signal, timed_out](boost::system::error_code ec) {
                if (!ec)
                {
                    *timed_out = true;
                    signal->cancel();
                }
            });
        };

    arm_deadline(std::chrono::seconds(timeout_secs));

    boost::system::error_code ec;
    co_await signal->async_wait(asio::redirect_error(asio::use_awaitable, ec));

    constexpr int post_handshake_grace_secs = 15;
    int total_timeout_secs = timeout_secs;
    if (*timed_out && out_client && !*connect_ec &&
        detail::is_post_handshake_state(out_client->state()))
    {
        total_timeout_secs += post_handshake_grace_secs;
        arm_deadline(std::chrono::seconds(post_handshake_grace_secs));
        co_await signal->async_wait(asio::redirect_error(asio::use_awaitable, ec));
    }

    deadline->cancel();

    if (out_client->state() == State::Failed ||
        out_client->state() != State::Ready)
    {
        std::string detail_text = detail::connect_phase_detail(out_client);
        auto const suppress_generic_ec =
            *connect_ec == boost::asio::error::invalid_argument;
        if (out_client)
        {
            auto conn_detail =
                out_client->raw_connection().connect_failure_detail();
            if (!conn_detail.empty())
            {
                if (!suppress_generic_ec && *connect_ec)
                    detail_text =
                        detail::append_detail(detail_text, connect_ec->message());
                if (detail_text != conn_detail)
                    detail_text = detail::append_detail(detail_text, conn_detail);
            }
        }
        if (*timed_out)
        {
            detail_text = detail::append_detail(
                std::move(detail_text),
                "deadline=" + std::to_string(total_timeout_secs) + "s");
            throw PeerClientException(Error::Timeout, std::move(detail_text));
        }
        if (*connect_ec)
            detail_text = detail::append_detail(
                std::move(detail_text), connect_ec->message());
        throw PeerClientException(Error::Disconnected, std::move(detail_text));
    }

    co_return peer_seq->load();
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
