#pragma once

#include "peer-client.h"
#include "types.h"

#include <future>
#include <memory>
#include <stdexcept>

namespace catl::peer_client {

//------------------------------------------------------------------------------
// Exception type for future-based API
//------------------------------------------------------------------------------

class PeerClientException : public std::runtime_error
{
public:
    Error error;

    explicit PeerClientException(Error e)
        : std::runtime_error("PeerClient error: " + std::to_string(static_cast<int>(e)))
        , error(e)
    {
    }
};

//------------------------------------------------------------------------------
// Layer 2: Future wrappers
//
// Thin adapter: callback → promise/future.
// Each method mirrors the callback version but returns std::future<T>.
//------------------------------------------------------------------------------

namespace detail {

/// Generic adapter: wraps any PeerClient callback method into a future.
template <typename ResultT>
std::future<ResultT>
to_future(std::function<void(Callback<ResultT>)> initiator)
{
    auto promise = std::make_shared<std::promise<ResultT>>();
    initiator([promise](Error err, ResultT result) {
        if (err != Error::Success)
            promise->set_exception(
                std::make_exception_ptr(PeerClientException(err)));
        else
            promise->set_value(std::move(result));
    });
    return promise->get_future();
}

}  // namespace detail

//------------------------------------------------------------------------------
// Free functions — async versions of PeerClient methods
//------------------------------------------------------------------------------

inline std::future<LedgerHeaderResult>
get_ledger_header_async(
    PeerClient& client,
    uint32_t ledger_seq,
    RequestOptions opts = {})
{
    return detail::to_future<LedgerHeaderResult>(
        [&client, ledger_seq, opts](Callback<LedgerHeaderResult> cb) {
            client.get_ledger_header(ledger_seq, std::move(cb), opts);
        });
}

inline std::future<LedgerHeaderResult>
get_ledger_header_async(
    PeerClient& client,
    Hash256 const& ledger_hash,
    RequestOptions opts = {})
{
    return detail::to_future<LedgerHeaderResult>(
        [&client, &ledger_hash, opts](Callback<LedgerHeaderResult> cb) {
            client.get_ledger_header(ledger_hash, std::move(cb), opts);
        });
}

inline std::future<ProofPathResult>
get_tx_proof_path_async(
    PeerClient& client,
    Hash256 const& ledger_hash,
    Hash256 const& key,
    RequestOptions opts = {})
{
    return detail::to_future<ProofPathResult>(
        [&client, &ledger_hash, &key, opts](Callback<ProofPathResult> cb) {
            client.get_tx_proof_path(ledger_hash, key, std::move(cb), opts);
        });
}

inline std::future<ProofPathResult>
get_state_proof_path_async(
    PeerClient& client,
    Hash256 const& ledger_hash,
    Hash256 const& key,
    RequestOptions opts = {})
{
    return detail::to_future<ProofPathResult>(
        [&client, &ledger_hash, &key, opts](Callback<ProofPathResult> cb) {
            client.get_state_proof_path(ledger_hash, key, std::move(cb), opts);
        });
}

inline std::future<LedgerNodesResult>
get_tx_tree_nodes_async(
    PeerClient& client,
    Hash256 const& ledger_hash,
    std::vector<SHAMapNodeID> const& node_ids,
    RequestOptions opts = {})
{
    return detail::to_future<LedgerNodesResult>(
        [&client, &ledger_hash, &node_ids, opts](Callback<LedgerNodesResult> cb) {
            client.get_tx_tree_nodes(ledger_hash, node_ids, std::move(cb), opts);
        });
}

inline std::future<LedgerNodesResult>
get_state_tree_nodes_async(
    PeerClient& client,
    Hash256 const& ledger_hash,
    std::vector<SHAMapNodeID> const& node_ids,
    RequestOptions opts = {})
{
    return detail::to_future<LedgerNodesResult>(
        [&client, &ledger_hash, &node_ids, opts](Callback<LedgerNodesResult> cb) {
            client.get_state_tree_nodes(ledger_hash, node_ids, std::move(cb), opts);
        });
}

inline std::future<LedgerNodesResult>
get_txset_nodes_async(
    PeerClient& client,
    Hash256 const& txset_hash,
    std::vector<SHAMapNodeID> const& node_ids,
    RequestOptions opts = {})
{
    return detail::to_future<LedgerNodesResult>(
        [&client, &txset_hash, &node_ids, opts](Callback<LedgerNodesResult> cb) {
            client.get_txset_nodes(txset_hash, node_ids, std::move(cb), opts);
        });
}

inline std::future<ReplayDeltaResult>
get_replay_delta_async(
    PeerClient& client,
    Hash256 const& ledger_hash,
    RequestOptions opts = {})
{
    return detail::to_future<ReplayDeltaResult>(
        [&client, &ledger_hash, opts](Callback<ReplayDeltaResult> cb) {
            client.get_replay_delta(ledger_hash, std::move(cb), opts);
        });
}

inline std::future<PingResult>
ping_async(PeerClient& client, RequestOptions opts = {})
{
    return detail::to_future<PingResult>(
        [&client, opts](Callback<PingResult> cb) {
            client.ping(std::move(cb), opts);
        });
}

}  // namespace catl::peer_client
