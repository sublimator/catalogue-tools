#pragma once

// Coroutine layer for PeerClient.
//
// co_ prefixed methods that wrap the callback API into
// boost::asio::awaitable<T> for use with co_await.
//
// Usage:
//   auto hdr = co_await co_get_ledger_header(client, seq);
//   auto nodes = co_await co_get_tx_nodes(client, hash, {root_id});
//
// Throws PeerClientException on non-Success errors.
// Must be called from a coroutine running on the same io_context
// as the PeerClient.

#include "peer-client.h"
#include "types.h"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <functional>
#include <stdexcept>

namespace catl::peer_client {

namespace asio = boost::asio;

//------------------------------------------------------------------------------
// Exception type
//------------------------------------------------------------------------------

class PeerClientException : public std::runtime_error
{
public:
    Error error;

    static std::string
    error_name(Error e)
    {
        switch (e)
        {
            case Error::Success:
                return "Success";
            case Error::Timeout:
                return "Timeout (peer did not respond in time)";
            case Error::Disconnected:
                return "Disconnected";
            case Error::NoLedger:
                return "NoLedger (peer does not have this ledger)";
            case Error::NoNode:
                return "NoNode (requested node not found)";
            case Error::BadRequest:
                return "BadRequest";
            case Error::FeatureDisabled:
                return "FeatureDisabled";
            case Error::ParseError:
                return "ParseError";
            case Error::Cancelled:
                return "Cancelled";
            default:
                return "Unknown(" + std::to_string(static_cast<int>(e)) + ")";
        }
    }

    explicit PeerClientException(Error e)
        : std::runtime_error("PeerClient: " + error_name(e)), error(e)
    {
    }
};

//------------------------------------------------------------------------------
// Bridge: callback → awaitable
//
// Uses a timer set to max as a signal. The callback cancels the timer,
// waking the coroutine. Works because both run on the same io_context.
//------------------------------------------------------------------------------

namespace detail {

template <typename T>
asio::awaitable<T>
callback_to_awaitable(std::function<void(Callback<T>)> initiator)
{
    auto executor = co_await asio::this_coro::executor;
    asio::steady_timer signal(executor, asio::steady_timer::time_point::max());

    Error err = Error::Success;
    T result;

    initiator([&signal, &err, &result](Error e, T r) {
        err = e;
        result = std::move(r);
        signal.cancel();
    });

    boost::system::error_code ec;
    co_await signal.async_wait(asio::redirect_error(asio::use_awaitable, ec));

    if (err != Error::Success)
        throw PeerClientException(err);

    co_return std::move(result);
}

}  // namespace detail

//------------------------------------------------------------------------------
// co_ methods — free functions taking shared_ptr<PeerClient>
//------------------------------------------------------------------------------

inline asio::awaitable<LedgerHeaderResult>
co_get_ledger_header(
    std::shared_ptr<PeerClient> client,
    uint32_t ledger_seq,
    RequestOptions opts = {})
{
    co_await asio::post(client->strand(), asio::use_awaitable);
    co_return co_await detail::callback_to_awaitable<LedgerHeaderResult>(
        [&](Callback<LedgerHeaderResult> cb) {
            client->get_ledger_header(ledger_seq, std::move(cb), opts);
        });
}

inline asio::awaitable<LedgerHeaderResult>
co_get_ledger_header(
    std::shared_ptr<PeerClient> client,
    Hash256 const& ledger_hash,
    RequestOptions opts = {})
{
    co_await asio::post(client->strand(), asio::use_awaitable);
    co_return co_await detail::callback_to_awaitable<LedgerHeaderResult>(
        [&](Callback<LedgerHeaderResult> cb) {
            client->get_ledger_header(ledger_hash, std::move(cb), opts);
        });
}

inline asio::awaitable<LedgerNodesResult>
co_get_state_nodes(
    std::shared_ptr<PeerClient> client,
    Hash256 const& ledger_hash,
    std::vector<SHAMapNodeID> const& node_ids,
    RequestOptions opts = {})
{
    co_await asio::post(client->strand(), asio::use_awaitable);
    co_return co_await detail::callback_to_awaitable<LedgerNodesResult>(
        [&](Callback<LedgerNodesResult> cb) {
            client->get_state_nodes(ledger_hash, node_ids, std::move(cb), opts);
        });
}

inline asio::awaitable<LedgerNodesResult>
co_get_tx_nodes(
    std::shared_ptr<PeerClient> client,
    Hash256 const& ledger_hash,
    std::vector<SHAMapNodeID> const& node_ids,
    RequestOptions opts = {})
{
    co_await asio::post(client->strand(), asio::use_awaitable);
    co_return co_await detail::callback_to_awaitable<LedgerNodesResult>(
        [&](Callback<LedgerNodesResult> cb) {
            client->get_tx_nodes(ledger_hash, node_ids, std::move(cb), opts);
        });
}

inline asio::awaitable<ProofPathResult>
co_get_tx_proof_path(
    std::shared_ptr<PeerClient> client,
    Hash256 const& ledger_hash,
    Hash256 const& key,
    RequestOptions opts = {})
{
    co_await asio::post(client->strand(), asio::use_awaitable);
    co_return co_await detail::callback_to_awaitable<ProofPathResult>(
        [&](Callback<ProofPathResult> cb) {
            client->get_tx_proof_path(ledger_hash, key, std::move(cb), opts);
        });
}

inline asio::awaitable<ProofPathResult>
co_get_state_proof_path(
    std::shared_ptr<PeerClient> client,
    Hash256 const& ledger_hash,
    Hash256 const& key,
    RequestOptions opts = {})
{
    co_await asio::post(client->strand(), asio::use_awaitable);
    co_return co_await detail::callback_to_awaitable<ProofPathResult>(
        [&](Callback<ProofPathResult> cb) {
            client->get_state_proof_path(ledger_hash, key, std::move(cb), opts);
        });
}

inline asio::awaitable<PingResult>
co_ping(std::shared_ptr<PeerClient> client, RequestOptions opts = {})
{
    co_await asio::post(client->strand(), asio::use_awaitable);
    co_return co_await detail::callback_to_awaitable<PingResult>(
        [&](Callback<PingResult> cb) { client->ping(std::move(cb), opts); });
}

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

    auto signal_executor = signal->get_executor();
    auto user_on_complete = std::move(opts.on_complete);

    // Install the completion callback into opts — it fires on
    // PeerClient's strand, then reposts the wakeup onto this coroutine's
    // executor so the timer is only touched from its owning executor.
    opts.on_complete =
        [peer_seq,
         signal_executor,
         signal,
         user_on_complete = std::move(user_on_complete)](
            boost::system::error_code ec, uint32_t seq) mutable {
            if (!ec)
            {
                peer_seq->store(seq);
            }
            if (user_on_complete)
            {
                user_on_complete(ec, seq);
            }
            asio::post(signal_executor, [signal]() {
                signal->cancel();
            });
        };

    out_client = PeerClient::connect(
        io_context, host, port, std::move(opts));

    deadline->async_wait([signal](boost::system::error_code ec) {
        if (!ec)
        {
            signal->cancel();
        }
    });

    boost::system::error_code ec;
    co_await signal->async_wait(asio::redirect_error(asio::use_awaitable, ec));

    deadline->cancel();

    if (out_client->state() == State::Failed ||
        out_client->state() != State::Ready)
    {
        throw PeerClientException(Error::Disconnected);
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

}  // namespace catl::peer_client
