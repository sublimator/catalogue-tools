#pragma once

// TreeWalker — coroutine wrapper that drives TreeWalkState via PeerSession.
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

#include <algorithm>
#include <boost/asio/awaitable.hpp>
#include <memory>

namespace catl::peer_client {

class TreeWalker
{
public:
    TreeWalker(
        PeerSessionPtr client,
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
        targets_.push_back(key);
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

    /// Set speculative batch size for parallel depth requests.
    /// Default: 1 (sequential). Higher values fire requests for
    /// multiple depths along the known key path in a single message.
    void
    set_speculative_depth(int depth)
    {
        speculative_depth_ = depth;
    }

    /// Walk the tree until all targets are found.
    /// Fetches nodes from the peer, feeds them to the state machine.
    ///
    /// When speculative_depth > 1, builds nodeIDs for multiple depths
    /// along the target key path and sends them in one request.
    /// Responses are sorted by depth and fed in order.
    boost::asio::awaitable<void>
    walk()
    {
        while (!state_.done())
        {
            auto requests = state_.pending_requests();
            if (requests.empty())
                break;

            // Convert state machine requests to SHAMapNodeID format
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

            // Add speculative requests along the known key path.
            // Batch size halves each round: 8 → 4 → 2 → 1
            int batch = speculative_depth_;
            if (batch > 1 && node_ids.size() == 1)
            {
                auto base = node_ids[0];
                uint8_t base_depth = base.depth;

                // We know the target key, so we know the nibble at
                // each depth. Build nodeIDs for deeper depths.
                for (int d = 1; d < batch; ++d)
                {
                    uint8_t next_depth = base_depth + d;
                    if (next_depth >= 64)
                        break;

                    // Build the path: copy base, extend with nibbles
                    // from the target key
                    SHAMapNodeID spec = base;
                    for (int dd = base_depth; dd < next_depth; ++dd)
                    {
                        // Get nibble from target key at depth dd
                        int byte_idx = dd / 2;
                        int nibble;
                        if (dd % 2 == 0)
                            nibble = (targets_[0].data()[byte_idx] >> 4) & 0xF;
                        else
                            nibble = targets_[0].data()[byte_idx] & 0xF;

                        // Set nibble in the path
                        int set_byte = dd / 2;
                        if (dd % 2 == 0)
                        {
                            spec.id.data()[set_byte] =
                                (spec.id.data()[set_byte] & 0x0F) |
                                (nibble << 4);
                        }
                        else
                        {
                            spec.id.data()[set_byte] =
                                (spec.id.data()[set_byte] & 0xF0) |
                                (nibble & 0xF);
                        }
                    }
                    spec.depth = next_depth;
                    node_ids.push_back(spec);
                }
            }

            // Fetch from peer — all nodeIDs in one request
            LedgerNodesResult result;
            if (type_ == TreeWalkState::TreeType::tx)
            {
                result =
                    co_await co_get_tx_nodes(client_, ledger_hash_, node_ids);
            }
            else
            {
                result = co_await co_get_state_nodes(
                    client_, ledger_hash_, node_ids);
            }

            // Sort responses by depth (parent before child)
            struct NodeResponse
            {
                std::span<const uint8_t> nid;
                std::span<const uint8_t> raw;
                uint8_t depth;
            };
            std::vector<NodeResponse> responses;
            for (int i = 0; i < result.node_count(); ++i)
            {
                auto nid = result.node_id(i);
                auto raw = result.node_view(i).raw();
                if (nid.size() == 33)
                {
                    responses.push_back({nid, raw, nid[32]});
                }
            }
            std::sort(
                responses.begin(),
                responses.end(),
                [](auto const& a, auto const& b) {
                    return a.depth < b.depth;
                });

            // Feed to state machine in depth order
            for (auto const& resp : responses)
            {
                if (!state_.done())
                {
                    state_.feed_node(resp.nid, resp.raw);
                }
            }

            // Halve the speculation for next round
            if (speculative_depth_ > 1)
            {
                speculative_depth_ = std::max(1, speculative_depth_ / 2);
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
    PeerSessionPtr client_;
    Hash256 ledger_hash_;
    TreeWalkState state_;
    TreeWalkState::TreeType type_;
    std::vector<Hash256> targets_;
    int speculative_depth_ = 1;
};

}  // namespace catl::peer_client
