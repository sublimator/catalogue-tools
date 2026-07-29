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
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <functional>
#include <optional>
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
    std::optional<ConnectAttemptDisposition> connect_disposition;

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
            case Error::PeerNoLedger:
                return "PeerNoLedger (uniquely correlated peer observation)";
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
            case Error::DuplicateRequest:
                return "DuplicateRequest";
            case Error::ResourceExhausted:
                return "ResourceExhausted";
            case Error::NoSuitablePeer:
                return "NoSuitablePeer (no eligible peer advertises the "
                       "ledger)";
            case Error::PeerCoverageChanged:
                return "PeerCoverageChanged (selected peer revoked coverage "
                       "before send)";
            default:
                return "Unknown(" + std::to_string(static_cast<int>(e)) + ")";
        }
    }

    explicit PeerClientException(
        Error e,
        std::string detail_text = {},
        std::optional<ConnectAttemptDisposition> disposition = std::nullopt)
        : std::runtime_error(
              detail_text.empty()
                  ? "PeerClient: " + error_name(e)
                  : "PeerClient: " + error_name(e) + " [" + detail_text + "]")
        , error(e)
        , detail(std::move(detail_text))
        , connect_disposition(disposition)
    {
    }
};

//------------------------------------------------------------------------------
// Bridge: callback → awaitable
//
// Uses a timer set to max as a signal. Each bridge creates a private strand
// from the coroutine's birth executor. Completion, cancellation arbitration,
// timer operations, and result consumption all run on that strand—even when
// the birth executor is a multi-threaded raw io_context executor.
//------------------------------------------------------------------------------

namespace detail {

template <typename T>
struct AwaitableCompletion
{
    enum class Phase : std::uint8_t { pending, completed, cancelled, consumed };

    explicit AwaitableCompletion(asio::any_io_executor executor)
        : arbitration(asio::make_strand(std::move(executor)))
        , signal(arbitration, asio::steady_timer::time_point::max())
    {
    }

    asio::strand<asio::any_io_executor> arbitration;
    asio::steady_timer signal;
    Phase phase = Phase::pending;
    Error error = Error::Success;
    std::optional<T> result;
};

/// Bridge callback API to awaitable. The retained cell survives coroutine
/// cancellation and serializes completion/cancellation on a private strand.
/// Whichever terminal event reaches that strand first wins; late completion is
/// consumed as stale and never mutates a returned result.
template <typename T>
asio::awaitable<T>
callback_to_awaitable(std::function<void(Callback<T>)> initiator)
{
    auto executor = co_await asio::this_coro::executor;
    auto completion = std::make_shared<AwaitableCompletion<T>>(executor);

    try
    {
        initiator([completion](Error error, T result) mutable {
            asio::post(
                completion->arbitration,
                [completion, error, result = std::move(result)]() mutable {
                    if (completion->phase !=
                        AwaitableCompletion<T>::Phase::pending)
                    {
                        return;
                    }
                    completion->error = error;
                    completion->result.emplace(std::move(result));
                    completion->phase =
                        AwaitableCompletion<T>::Phase::completed;
                    completion->signal.cancel();
                });
        });
    }
    catch (...)
    {
        throw;
    }

    co_return co_await asio::co_spawn(
        completion->arbitration,
        [completion]() -> asio::awaitable<T> {
            if (completion->phase == AwaitableCompletion<T>::Phase::pending)
            {
                boost::system::error_code ec;
                co_await completion->signal.async_wait(
                    asio::redirect_error(asio::use_awaitable, ec));
            }

            if (completion->phase != AwaitableCompletion<T>::Phase::completed)
            {
                completion->phase = AwaitableCompletion<T>::Phase::cancelled;
                throw PeerClientException(Error::Cancelled);
            }

            auto const error = completion->error;
            auto result = std::move(*completion->result);
            completion->result.reset();
            completion->phase = AwaitableCompletion<T>::Phase::consumed;

            if (error != Error::Success)
                throw PeerClientException(error);

            co_return result;
        },
        asio::use_awaitable);
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
    co_return co_await detail::callback_to_awaitable<ProofPathResult>(
        [&](Callback<ProofPathResult> cb) {
            client->get_state_proof_path(ledger_hash, key, std::move(cb), opts);
        });
}

inline asio::awaitable<PingResult>
co_ping(PeerSessionPtr client, RequestOptions opts = {})
{
    co_return co_await detail::callback_to_awaitable<PingResult>(
        [&](Callback<PingResult> cb) { client->ping(std::move(cb), opts); });
}

}  // namespace catl::peer_client
