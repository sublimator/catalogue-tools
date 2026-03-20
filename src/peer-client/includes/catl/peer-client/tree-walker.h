#pragma once

// TreeWalker — coroutine wrapper that drives TreeWalkState via PeerClient.
//
// Connects the pure state machine (TreeWalkState) to the network.
// Fetches nodes as requested by the state machine, feeds responses back.
//
// Usage:
//   TreeWalker walker(client, ledger_hash, TreeWalkState::TreeType::tx);
//   walker.add_target(tx_hash);
//   walker.set_on_placeholder([&](auto nid, auto hash) { ... });
//   walker.set_on_leaf([&](auto nid, auto key, auto data) { ... });
//   co_await walker.walk();

#include "peer-client-coro.h"
#include "tree-walk-state.h"

#include <boost/asio/awaitable.hpp>
#include <memory>

namespace catl::peer_client {

class TreeWalker
{
public:
    TreeWalker(
        std::shared_ptr<PeerClient> client,
        Hash256 const& ledger_hash,
        TreeWalkState::TreeType type)
        : client_(std::move(client))
        , ledger_hash_(ledger_hash)
        , state_(type)
        , type_(type)
    {
    }

    void
    add_target(Hash256 const& key)
    {
        state_.add_target(key);
    }

    void
    set_on_placeholder(TreeWalkState::PlaceholderCallback cb)
    {
        state_.set_on_placeholder(std::move(cb));
    }

    void
    set_on_leaf(TreeWalkState::LeafCallback cb)
    {
        state_.set_on_leaf(std::move(cb));
    }

    /// Walk the tree until all targets are found.
    /// Fetches nodes from the peer, feeds them to the state machine.
    boost::asio::awaitable<void>
    walk()
    {
        while (!state_.done())
        {
            auto requests = state_.pending_requests();
            if (requests.empty())
                break;

            // Convert to peer_client::SHAMapNodeID format
            std::vector<SHAMapNodeID> node_ids;
            for (auto const& req : requests)
            {
                if (req.size() != 33)
                    continue;
                SHAMapNodeID nid;
                std::memcpy(nid.id.data(), req.data(), 32);
                nid.depth = req[32];
                node_ids.push_back(nid);
            }

            // Fetch from peer
            LedgerNodesResult result;
            if (type_ == TreeWalkState::TreeType::tx)
                result =
                    co_await co_get_tx_nodes(client_, ledger_hash_, node_ids);
            else
                result = co_await co_get_state_nodes(
                    client_, ledger_hash_, node_ids);

            // Feed each node to the state machine
            for (int i = 0; i < result.node_count(); ++i)
            {
                auto nid = result.node_id(i);
                auto data = result.node_data(i);

                // node_data returns without wire type byte, but we need it.
                // Use the raw view which includes the type byte.
                auto raw = result.node_view(i).raw();

                if (nid.size() == 33)
                    state_.feed_node(nid, raw);
            }
        }
    }

    int
    found_count() const
    {
        return state_.found_count();
    }
    bool
    done() const
    {
        return state_.done();
    }

private:
    std::shared_ptr<PeerClient> client_;
    Hash256 ledger_hash_;
    TreeWalkState state_;
    TreeWalkState::TreeType type_;
};

}  // namespace catl::peer_client
