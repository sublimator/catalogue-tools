#pragma once

/// Shared test-only access to PeerClient's already-declared friend authority.
///
/// Extracted from peer-client-gtest.cpp so a second test translation unit can
/// use it. This is a MOVE, not a widening: `PeerClientTestAccess` is already
/// named in node-transport.h's friend lists, so no new production friend was
/// added, no constructor was made public, and the mint surface is unchanged.
/// One definition instead of a local one avoids ODR and semantic drift.
///
/// It is the only route by which a test can mint a NodeBatchIntent or a
/// NodeBatchCommand. That restriction is deliberate — forging a receipt or an
/// intent from arbitrary test code is exactly what the friend lists prevent.

#include <catl/peer-client/connection-types.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-client.h>
#include <catl/peer-client/types.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace catl::peer_client {

class PeerClientTestAccess
{
public:
    static std::shared_ptr<PeerClient>
    make(
        asio::io_context& io,
        State state,
        std::uint64_t connection_generation = 1)
    {
        auto client = std::shared_ptr<PeerClient>(new PeerClient(io));
        client->state_ = state;
        client->connection_generation_ = connection_generation;
        return client;
    }

    /// One intent, without sealing it. The bind controls need an intent to hand
    /// to PeerSet; they must NOT be able to mint the sealed command themselves,
    /// since PeerSet is its sole minter and the whole point of the bind is that
    /// it decides.
    static NodeBatchIntent
    node_batch_intent(
        std::uint64_t attempt_id,
        std::uint64_t sender_generation = 1,
        Hash256 ledger_hash = {},
        NodeTree tree = NodeTree::account_state,
        std::vector<SHAMapNodeID> node_ids = {SHAMapNodeID{}})
    {
        return NodeBatchIntent{
            TransportAttemptId{attempt_id},
            SenderGenerationId{sender_generation},
            std::move(ledger_hash),
            tree,
            std::move(node_ids)};
    }

    static NodeBatchCommand
    node_batch_command(
        std::uint64_t attempt_id,
        std::uint64_t peer_generation,
        Hash256 ledger_hash = {},
        NodeTree tree = NodeTree::transaction,
        std::vector<SHAMapNodeID> node_ids = {SHAMapNodeID{}},
        std::uint64_t sender_generation = 1)
    {
        NodeBatchIntent intent{
            TransportAttemptId{attempt_id},
            SenderGenerationId{sender_generation},
            std::move(ledger_hash),
            tree,
            std::move(node_ids)};
        return NodeBatchCommand{
            std::move(intent),
            PeerConnectionGenerationId{peer_generation}};
    }

    static void
    seed_pending_node_batch(
        PeerClient& client,
        NodeBatchCommand command,
        NodeBatchWriteHandler on_receipt)
    {
        auto const attempt_id = command.attempt_id();
        ASSERT_EQ(
            std::count_if(
                client.node_batch_write_records_.begin(),
                client.node_batch_write_records_.end(),
                [&](PeerClient::NodeBatchWriteRecord const& record) {
                    return record.attempt_id == attempt_id;
                }),
            0);
        client.node_batch_write_records_.emplace_back(
            std::move(command), std::move(on_receipt));
        ++client.reserved_node_batch_receipt_sequences_;
        client.node_batch_work_projection_.fetch_add(
            1, std::memory_order_release);
        client.refresh_pending_count_projection();
    }

    static void
    submit_node_batch_on_strand(
        PeerClient& client,
        NodeBatchCommand command,
        NodeBatchWriteHandler on_receipt)
    {
        client.node_batch_work_projection_.fetch_add(
            1, std::memory_order_release);
        if (!client.submit_node_batch_on_strand(
                std::move(command), std::move(on_receipt)))
        {
            client.node_batch_work_projection_.fetch_sub(
                1, std::memory_order_release);
        }
    }

    static void
    complete_node_batch_write(
        PeerClient& client,
        std::uint64_t attempt_id,
        boost::system::error_code ec = {})
    {
        client.complete_node_batch_write(
            SenderGenerationId{1}, TransportAttemptId{attempt_id}, ec);
    }

    static NodeBatchReceiptAck
    acknowledgment(NodeBatchWriteReceipt const& receipt)
    {
        if (receipt.receiver_commit_sequence() == 0)
            throw std::logic_error{"receipt has no retained commit"};
        return NodeBatchReceiptAck{receipt};
    }

    static void
    seed_receipt_sequence(
        PeerClient& client,
        std::uint64_t next_sequence)
    {
        client.next_node_batch_receipt_sequence_ = next_sequence;
    }

    static std::size_t
    deferred_count(PeerClient const& client)
    {
        return client.pending_queue_.size();
    }

    static bool
    seed_pending_header(
        PeerClient& client,
        Callback<LedgerHeaderResult> callback,
        std::chrono::seconds timeout = std::chrono::seconds{30},
        bool dedupe = true)
    {
        RequestOptions opts;
        opts.timeout = timeout;
        auto const key = PeerClient::ledger_key(1, 0);
        return client.register_callback(
            client.pending_headers_, key, std::move(callback), opts, dedupe);
    }

    static void
    resolve_pending_header(PeerClient& client)
    {
        auto const key = PeerClient::ledger_key(1, 0);
        client.resolve(
            client.pending_headers_, key, Error::Success, LedgerHeaderResult{});
    }

    static void
    defer(
        PeerClient& client,
        PeerClient::DeferredRequest request)
    {
        client.defer_request(std::move(request));
    }

    static void
    become_ready_and_flush(PeerClient& client)
    {
        client.state_ = State::Ready;
        client.flush_queue();
    }

    static void
    seed_incarnations(
        PeerClient& client,
        std::uint64_t pending,
        std::uint64_t callback)
    {
        client.next_pending_incarnation_ = pending;
        client.next_callback_incarnation_ = callback;
    }

    /// Fill the receiver's bounded node-batch ingress so every further public
    /// submission or acknowledgement is refused before reaching the strand.
    ///
    /// Used by the coordinator controls to force the acknowledgement-refused retry
    /// path, which is otherwise unreachable: it is the receiver's own bound, and a
    /// sender-side simulation of it would prove nothing about the real boundary.
    static void
    seed_node_batch_ingress_saturated(PeerClient& client)
    {
        client.node_batch_ingress_projections_.store(
            kMaxNodeBatchIngressProjections, std::memory_order_release);
    }

    /// Release the receiver's node-batch ingress, as its own strand does when it
    /// drains an item. Paired with seed_node_batch_ingress_saturated so a control
    /// can force a refusal, then prove a later attempt succeeds — which is the
    /// difference between a retry that yields and one that burns its budget in a
    /// single strand pass.
    static void
    drain_node_batch_ingress(PeerClient& client)
    {
        client.node_batch_ingress_projections_.store(
            0, std::memory_order_release);
    }

    static void
    seed_open_connection(PeerClient& client, asio::ssl::context& ssl_context)
    {
        peer_config config;
        client.connection_ = std::make_shared<peer_connection>(
            client.strand_, ssl_context, std::move(config));
        boost::system::error_code ec;
        client.connection_->socket().lowest_layer().open(tcp::v4(), ec);
        ASSERT_FALSE(ec);
    }
};

}  // namespace catl::peer_client
