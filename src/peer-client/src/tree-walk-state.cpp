#include <catl/crypto/sha512-half-hasher.h>
#include <catl/peer-client/tree-walk-state.h>

namespace catl::peer_client {

LogPartition TreeWalkState::log_("tree-walk", LogLevel::INHERIT);

TreeWalkState::TreeWalkState(TreeType type) : type_(type)
{
}

void
TreeWalkState::add_target(Hash256 const& key)
{
    targets_.insert(key);
    PLOGD(log_, "add_target: ", key.hex().substr(0, 16), "...");

    // If this is the first target, seed with root request
    if (pending_.empty() && fetched_.empty())
    {
        pending_.insert(make_nodeid(Hash256::zero().data(), 0));
        PLOGD(log_, "  seeded root request");
    }
}

std::vector<std::vector<uint8_t>>
TreeWalkState::pending_requests() const
{
    return {pending_.begin(), pending_.end()};
}

bool
TreeWalkState::done() const
{
    return pending_.empty() &&
        found_count_ >= static_cast<int>(targets_.size());
}

void
TreeWalkState::feed_node(
    std::span<const uint8_t> nodeid,
    std::span<const uint8_t> wire_data)
{
    if (nodeid.size() < 33 || wire_data.empty())
        return;

    std::vector<uint8_t> nid_vec(nodeid.begin(), nodeid.end());

    // Mark as fetched, remove from pending
    fetched_.insert(nid_vec);
    pending_.erase(nid_vec);

    uint8_t depth = nodeid[32];
    uint8_t wire_type = wire_data.back();

    PLOGD(
        log_,
        "feed_node: depth=",
        static_cast<int>(depth),
        " wire_type=",
        static_cast<int>(wire_type),
        " size=",
        wire_data.size());

    // Inner node: wire_type 2 (uncompressed) or 3 (compressed)
    if (wire_type == 2 || wire_type == 3)
    {
        if (wire_type == 2 && wire_data.size() >= 513)
        {
            // Uncompressed: 16 × 32-byte hashes + type byte
            for (int branch = 0; branch < 16; ++branch)
            {
                Hash256 child_hash(wire_data.data() + branch * 32);
                if (child_hash == Hash256::zero())
                    continue;

                auto child_nid = child_nodeid(nodeid.data(), depth, branch);

                if (any_target_on_branch(branch, depth, nodeid.data()))
                {
                    // On-path: request this child next
                    if (!fetched_.count(child_nid))
                    {
                        pending_.insert(child_nid);
                        PLOGD(log_, "  branch ", branch, " on-path → request");
                    }
                }
                else
                {
                    // Off-path: emit as placeholder
                    if (on_placeholder_)
                        on_placeholder_(child_nid, child_hash);
                    PLOGT(log_, "  branch ", branch, " off-path → placeholder");
                }
            }
        }
        else if (wire_type == 3)
        {
            // Compressed: [hash(32)][branch(1)] pairs + type byte
            auto body_size = wire_data.size() - 1;
            for (size_t pos = 0; pos + 33 <= body_size; pos += 33)
            {
                Hash256 child_hash(wire_data.data() + pos);
                int branch = wire_data[pos + 32];

                auto child_nid = child_nodeid(nodeid.data(), depth, branch);

                if (any_target_on_branch(branch, depth, nodeid.data()))
                {
                    if (!fetched_.count(child_nid))
                    {
                        pending_.insert(child_nid);
                        PLOGD(log_, "  branch ", branch, " on-path → request");
                    }
                }
                else
                {
                    if (on_placeholder_)
                        on_placeholder_(child_nid, child_hash);
                    PLOGT(log_, "  branch ", branch, " off-path → placeholder");
                }
            }
        }
    }
    // Leaf node: wire_type 0 (tx), 1 (state), 4 (tx+meta)
    else
    {
        auto data_size = wire_data.size() - 1;  // strip wire type byte
        if (data_size < 32)
            return;

        // Extract key from last 32 bytes of leaf data
        Hash256 leaf_key(wire_data.data() + data_size - 32);

        if (targets_.count(leaf_key))
        {
            // Target leaf found!
            found_count_++;
            if (on_leaf_)
                on_leaf_(nodeid, leaf_key, wire_data.subspan(0, data_size));
            PLOGD(
                log_,
                "  TARGET LEAF found: ",
                leaf_key.hex().substr(0, 16),
                "...");
        }
        else
        {
            // Sibling leaf — compute hash, emit as placeholder
            // Hash depends on tree type
            auto prefix = (type_ == TreeType::tx)
                ? std::array<uint8_t, 4>{'S', 'N', 'D', 0x00}   // tx_node
                : std::array<uint8_t, 4>{'M', 'L', 'N', 0x00};  // leaf_node

            catl::crypto::Sha512HalfHasher hasher;
            hasher.update(prefix.data(), prefix.size());
            hasher.update(wire_data.data(), data_size - 32);  // item data
            hasher.update(leaf_key.data(), 32);
            auto leaf_hash = hasher.finalize();

            if (on_placeholder_)
                on_placeholder_(nodeid, leaf_hash);
            PLOGT(log_, "  sibling leaf → placeholder");
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────

int
TreeWalkState::nibble_at(uint8_t const* key, int depth)
{
    int byte_idx = depth / 2;
    if (depth % 2 == 0)
        return (key[byte_idx] >> 4) & 0xF;
    else
        return key[byte_idx] & 0xF;
}

std::vector<uint8_t>
TreeWalkState::make_nodeid(uint8_t const* path, uint8_t depth)
{
    std::vector<uint8_t> nid(33);
    std::memcpy(nid.data(), path, 32);
    nid[32] = depth;
    return nid;
}

std::vector<uint8_t>
TreeWalkState::child_nodeid(
    uint8_t const* parent_path,
    uint8_t parent_depth,
    int branch)
{
    std::vector<uint8_t> nid(33);
    std::memcpy(nid.data(), parent_path, 32);
    int d = parent_depth;
    int byte_idx = d / 2;
    if (d % 2 == 0)
        nid[byte_idx] = (nid[byte_idx] & 0x0F) | (branch << 4);
    else
        nid[byte_idx] = (nid[byte_idx] & 0xF0) | (branch & 0xF);
    nid[32] = d + 1;
    return nid;
}

bool
TreeWalkState::any_target_on_branch(
    int branch,
    int depth,
    uint8_t const* /*inner_path*/) const
{
    for (auto const& target : targets_)
    {
        if (nibble_at(target.data(), depth) == branch)
            return true;
    }
    return false;
}

}  // namespace catl::peer_client
