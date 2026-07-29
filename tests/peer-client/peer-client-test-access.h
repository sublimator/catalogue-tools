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
        client->publish_advertised_ledger_range(
            {.first_seq = 1,
             .last_seq = std::numeric_limits<std::uint32_t>::max()});
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
        std::vector<SHAMapNodeID> node_ids = {SHAMapNodeID{}},
        std::uint32_t ledger_seq = 1)
    {
        return NodeBatchIntent{
            TransportAttemptId{attempt_id},
            SenderGenerationId{sender_generation},
            std::move(ledger_hash),
            ledger_seq,
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
        std::uint64_t sender_generation = 1,
        std::uint32_t ledger_seq = 1)
    {
        NodeBatchIntent intent{
            TransportAttemptId{attempt_id},
            SenderGenerationId{sender_generation},
            std::move(ledger_hash),
            ledger_seq,
            tree,
            std::move(node_ids)};
        return NodeBatchCommand{
            std::move(intent), PeerConnectionGenerationId{peer_generation}};
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
    seed_receipt_sequence(PeerClient& client, std::uint64_t next_sequence)
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
        auto const registered = client.register_callback(
            client.pending_headers_, key, std::move(callback), opts, dedupe);
        if (registered)
            client.pending_headers_.at(key).network_request_initiated = true;
        return registered;
    }

    static bool
    seed_pending_header_by_sequence(
        PeerClient& client,
        std::uint32_t ledger_seq,
        int type,
        Callback<LedgerHeaderResult> callback)
    {
        RequestOptions opts;
        auto const key = PeerClient::ledger_key(ledger_seq, type);
        auto const registered = client.register_callback(
            client.pending_headers_,
            key,
            std::move(callback),
            opts,
            false);
        if (registered)
            client.pending_headers_.at(key).network_request_initiated = true;
        return registered;
    }

    static bool
    seed_pending_header_by_hash(
        PeerClient& client,
        Hash256 const& ledger_hash,
        int type,
        Callback<LedgerHeaderResult> callback)
    {
        RequestOptions opts;
        auto const key = PeerClient::ledger_key(ledger_hash, type);
        auto const registered = client.register_callback(
            client.pending_headers_,
            key,
            std::move(callback),
            opts,
            false);
        if (registered)
            client.pending_headers_.at(key).network_request_initiated = true;
        return registered;
    }

    static void
    resolve_pending_header(PeerClient& client)
    {
        auto const key = PeerClient::ledger_key(1, 0);
        client.resolve(
            client.pending_headers_, key, Error::Success, LedgerHeaderResult{});
    }

    static bool
    seed_pending_nodes(
        PeerClient& client,
        Hash256 const& ledger_hash,
        int type,
        std::vector<SHAMapNodeID> const& node_ids,
        Callback<LedgerNodesResult> callback,
        bool network_request_initiated = true)
    {
        RequestOptions opts;
        auto const key = PeerClient::nodes_key(ledger_hash, type, node_ids);
        auto const registered = client.register_callback(
            client.pending_nodes_, key, std::move(callback), opts, false);
        if (registered)
        {
            auto& entry = client.pending_nodes_.at(key);
            entry.match_key = PeerClient::ledger_key(ledger_hash, type);
            entry.requested_nodeids = node_ids;
            entry.network_request_initiated = network_request_initiated;
        }
        return registered;
    }

    static bool
    replace_pending_nodes_same_key(
        PeerClient& client,
        Hash256 const& ledger_hash,
        int type,
        std::vector<SHAMapNodeID> const& node_ids,
        Callback<LedgerNodesResult> callback)
    {
        assert(client.strand_.running_in_this_thread());
        auto const key = PeerClient::nodes_key(ledger_hash, type, node_ids);
        if (client.pending_nodes_.erase(key) != 1)
            return false;
        client.refresh_pending_count_projection();
        return seed_pending_nodes(
            client,
            ledger_hash,
            type,
            node_ids,
            std::move(callback));
    }

    static void
    set_before_ledger_attempt_consume(
        PeerClient& client,
        std::function<void()> hook)
    {
        client.before_ledger_attempt_consume_for_test_ = std::move(hook);
    }

    static void
    dispatch_ledger_data(
        PeerClient& client,
        std::vector<std::uint8_t> const& data)
    {
        client.dispatch_ledger_data(data);
    }

    static void
    defer(PeerClient& client, PeerClient::DeferredRequest request)
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
    set_initial_connect_observers(
        PeerClient& client,
        ReadyCallback ready,
        ConnectCompletionCallback complete)
    {
        client.ready_callback_ = std::move(ready);
        client.connect_completion_callback_ = std::move(complete);
    }

    static void
    become_ready_on_strand(PeerClient& client)
    {
        client.become_ready();
    }

    static std::optional<State>
    commit_failed_on_strand(
        PeerClient& client,
        boost::system::error_code ec = asio::error::connection_reset)
    {
        return client.commit_failed_on_strand(ec);
    }

    static void
    fail_initial_connect_on_strand(
        PeerClient& client,
        boost::system::error_code ec = asio::error::connection_reset)
    {
        auto const previous = client.commit_failed_on_strand(ec);
        if (previous)
            client.complete_connect(ec);
    }

    static void
    disconnect_on_strand(PeerClient& client)
    {
        client.disconnect_on_strand();
    }

    static bool
    hold_initial_connect_terminal_publication(PeerClient& client)
    {
        return client.initial_connect_attempt_
            ->hold_terminal_publication_for_test();
    }

    static bool
    release_initial_connect_terminal_publication(PeerClient& client)
    {
        return client.initial_connect_attempt_
            ->release_terminal_publication_for_test();
    }

    static std::optional<detail::ConnectAttemptTerminal>
    initial_connect_terminal(PeerClient const& client)
    {
        return client.initial_connect_attempt_->snapshot();
    }

    static detail::ConnectAttemptCommit
    commit_initial_connect_deadline(PeerClient& client, int seconds)
    {
        return client.initial_connect_attempt_->try_commit_initial(
            detail::ConnectAttemptTerminal{
                detail::ConnectAttemptTerminalKind::deadline,
                asio::error::timed_out,
                0,
                seconds},
            []() noexcept {});
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
    /// Used by the coordinator controls to force the acknowledgement-refused
    /// retry path, which is otherwise unreachable: it is the receiver's own
    /// bound, and a sender-side simulation of it would prove nothing about the
    /// real boundary.
    static void
    seed_node_batch_ingress_saturated(PeerClient& client)
    {
        client.node_batch_ingress_projections_.store(
            kMaxNodeBatchIngressProjections, std::memory_order_release);
    }

    /// Release the receiver's node-batch ingress, as its own strand does when
    /// it drains an item. Paired with seed_node_batch_ingress_saturated so a
    /// control can force a refusal, then prove a later attempt succeeds — which
    /// is the difference between a retry that yields and one that burns its
    /// budget in a single strand pass.
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

    static void
    publish_advertised_ledger_range(
        PeerClient& client,
        AdvertisedLedgerRange range)
    {
        client.publish_advertised_ledger_range(range);
    }

    static void
    set_endpoint(PeerClient& client, std::string endpoint)
    {
        client.endpoint_str_ = std::move(endpoint);
    }

    static void
    seed_successful_packet_writes(PeerClient& client)
    {
        ASSERT_TRUE(client.connection_);
        client.connection_->set_write_initiator_for_test(
            [](asio::const_buffer,
               peer_connection::test_write_completion complete) {
                complete({});
            });
    }
};

}  // namespace catl::peer_client
