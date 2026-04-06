#pragma once

// Coroutine request layer for PeerSession.
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
// as the PeerSession.

#include "peer-session.h"

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
    std::string detail;

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

    explicit PeerClientException(Error e, std::string detail_text = {})
        : std::runtime_error(
              detail_text.empty()
                  ? "PeerClient: " + error_name(e)
                  : "PeerClient: " + error_name(e) + " [" + detail_text + "]")
        , error(e)
        , detail(std::move(detail_text))
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

/// Bridge callback API to awaitable. Signal and result are shared_ptr
/// so they survive coroutine destruction if cancellation fires while
/// the callback is still in-flight (e.g. || operator cancels the prove
/// but the peer hasn't responded yet).
template <typename T>
asio::awaitable<T>
callback_to_awaitable(std::function<void(Callback<T>)> initiator)
{
    auto executor = co_await asio::this_coro::executor;
    auto signal = std::make_shared<asio::steady_timer>(
        executor, asio::steady_timer::time_point::max());
    auto err = std::make_shared<Error>(Error::Success);
    auto result = std::make_shared<T>();

    initiator([signal, err, result](Error e, T r) {
        *err = e;
        *result = std::move(r);
        signal->cancel();
    });

    boost::system::error_code ec;
    co_await signal->async_wait(asio::redirect_error(asio::use_awaitable, ec));

    if (*err != Error::Success)
        throw PeerClientException(*err);

    co_return std::move(*result);
}

}  // namespace detail

//------------------------------------------------------------------------------
// co_ methods — free functions taking PeerSessionPtr
//------------------------------------------------------------------------------

inline asio::awaitable<LedgerHeaderResult>
co_get_ledger_header(
    PeerSessionPtr client,
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
    PeerSessionPtr client,
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
    PeerSessionPtr client,
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
    PeerSessionPtr client,
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
    PeerSessionPtr client,
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
    PeerSessionPtr client,
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
co_ping(PeerSessionPtr client, RequestOptions opts = {})
{
    co_await asio::post(client->strand(), asio::use_awaitable);
    co_return co_await detail::callback_to_awaitable<PingResult>(
        [&](Callback<PingResult> cb) { client->ping(std::move(cb), opts); });
}

}  // namespace catl::peer_client
