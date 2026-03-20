#pragma once

// TreeWalkState — pure state machine for walking a SHAMap tree.
//
// Manages the logic of following nibble paths toward target keys,
// emitting placeholder hashes for off-path branches and target leaf
// data when found. No I/O — just state transitions.
//
// Usage:
//   1. add_target(key) — register keys to walk toward
//   2. pending_requests() — get the nodeIDs that need fetching
//   3. feed_node(nid, wire_data) — provide fetched node data
//   4. Repeat 2-3 until done()
//
// Events are delivered via callbacks set before walking:
//   on_placeholder(nid, hash) — off-path branch hash (for abbreviated tree)
//   on_leaf(nid, key, data) — target leaf found
//
// Testable without network — feed fixture data, verify events.

#include <catl/core/logger.h>
#include <catl/core/types.h>

#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <set>
#include <span>
#include <vector>

namespace catl::peer_client {

class TreeWalkState
{
public:
    enum class TreeType { tx, state };

    /// Called for each off-path branch hash encountered during the walk.
    /// These become placeholders in the abbreviated tree.
    using PlaceholderCallback = std::function<void(
        std::span<const uint8_t> nodeid,  // 33 bytes
        Hash256 const& hash)>;

    /// Called when a target leaf is found.
    using LeafCallback = std::function<void(
        std::span<const uint8_t> nodeid,  // 33 bytes
        Hash256 const& key,               // leaf's item key (last 32 bytes)
        std::span<const uint8_t> data)>;  // leaf data (wire type stripped)

    explicit TreeWalkState(TreeType type);

    // ── Setup ────────────────────────────────────────────────────

    void
    add_target(Hash256 const& key);

    void
    set_on_placeholder(PlaceholderCallback cb)
    {
        on_placeholder_ = std::move(cb);
    }
    void
    set_on_leaf(LeafCallback cb)
    {
        on_leaf_ = std::move(cb);
    }

    // ── State machine ────────────────────────────────────────────

    /// NodeIDs that need to be fetched next.
    /// Returns 33-byte nodeIDs suitable for the peer protocol.
    std::vector<std::vector<uint8_t>>
    pending_requests() const;

    /// Feed a node received from the peer.
    /// nodeid: 33 bytes (32-byte path + 1-byte depth)
    /// wire_data: raw node data INCLUDING the wire type byte at the end
    void
    feed_node(
        std::span<const uint8_t> nodeid,
        std::span<const uint8_t> wire_data);

    /// Are all target keys resolved (found or unreachable)?
    bool
    done() const;

    /// How many targets have been found?
    int
    found_count() const
    {
        return found_count_;
    }

    /// How many targets are still pending?
    int
    remaining_count() const
    {
        return static_cast<int>(targets_.size()) - found_count_;
    }

private:
    TreeType type_;

    // Target keys we're walking toward
    std::set<Hash256> targets_;
    int found_count_ = 0;

    // NodeIDs we need to fetch (depth → set of 33-byte nodeIDs)
    // Using a vector<uint8_t> as key since we need 33-byte blobs
    struct NodeIDHash
    {
        size_t
        operator()(std::vector<uint8_t> const& v) const
        {
            size_t h = 0;
            for (size_t i = 0; i < v.size() && i < 8; ++i)
                h = h * 131 + v[i];
            return h;
        }
    };
    std::set<std::vector<uint8_t>> pending_;  // nodeIDs to fetch
    std::set<std::vector<uint8_t>> fetched_;  // nodeIDs already processed

    // Callbacks
    PlaceholderCallback on_placeholder_;
    LeafCallback on_leaf_;

    static LogPartition log_;

    // ── Helpers ──────────────────────────────────────────────────

    /// Extract nibble at depth d from a 32-byte key
    static int
    nibble_at(uint8_t const* key, int depth);

    /// Build a 33-byte nodeID from path + depth
    static std::vector<uint8_t>
    make_nodeid(uint8_t const* path, uint8_t depth);

    /// Build a child nodeID: copy parent path, set nibble at parent depth,
    /// depth+1
    static std::vector<uint8_t>
    child_nodeid(uint8_t const* parent_path, uint8_t parent_depth, int branch);

    /// Check if any target key has nibble `branch` at `depth`
    bool
    any_target_on_branch(int branch, int depth, uint8_t const* inner_path)
        const;
};

}  // namespace catl::peer_client
