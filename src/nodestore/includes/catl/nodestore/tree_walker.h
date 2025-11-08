#pragma once

#include "catl/core/logger.h"
#include "catl/core/types.h"
#include "catl/nodestore/backend.h"
#include "catl/nodestore/inner_node_format.h"
#include "catl/nodestore/node_blob.h"
#include <optional>
#include <vector>

namespace catl::nodestore {

/**
 * Lazy view wrapper for a decompressed inner node blob.
 * Provides easy access to branch hashes for tree walking.
 * Does NOT copy data - just references the blob.
 *
 * Usage:
 *   node_blob decompressed = nodeobject_decompress(compressed);
 *   inner_node_view view(decompressed);
 *   Hash256 branch3 = view.get_branch(3);
 */
class inner_node_view
{
private:
    node_blob const* blob_;

public:
    /**
     * Construct from decompressed node_blob.
     * The payload must be 512 bytes (16 * 32-byte hashes).
     * Does NOT copy - caller must keep blob alive.
     */
    explicit inner_node_view(node_blob const& decompressed)
        : blob_(&decompressed)
    {
        auto payload = blob_->payload();
        if (payload.size() != format::INNER_NODE_HASH_ARRAY_SIZE)
        {
            throw std::runtime_error(
                "inner_node_view: expected 512-byte payload, got " +
                std::to_string(payload.size()));
        }
    }

    /**
     * Get branch hash by index (0-15).
     * Returns zero hash if branch is empty.
     */
    Hash256
    get_branch(int index) const
    {
        if (index < 0 ||
            static_cast<std::size_t>(index) >= format::INNER_NODE_BRANCH_COUNT)
        {
            throw std::runtime_error(
                "inner_node_view: branch index out of range: " +
                std::to_string(index));
        }

        auto payload = blob_->payload();
        return Hash256(
            payload.data() +
            (static_cast<std::size_t>(index) * format::INNER_NODE_HASH_SIZE));
    }

    /**
     * Check if branch exists (non-zero hash).
     */
    bool
    has_branch(int index) const
    {
        return get_branch(index) != Hash256::zero();
    }

    /**
     * Get payload span (all 16 hashes as raw bytes).
     */
    std::span<std::uint8_t const>
    payload() const
    {
        return blob_->payload();
    }
};

/**
 * Result of a tree walk operation.
 */
struct WalkResult
{
    /** The final node blob (leaf or last inner node before key not found) */
    node_blob blob;

    /** Path taken through the tree (list of hashes visited) */
    std::vector<Hash256> path;

    /** True if we found the exact leaf, false if we stopped early */
    bool found;

    /** Depth reached (0 = root, increments for each inner node descended) */
    int depth;
};

/**
 * Tree walker for traversing SHAMap trees in the nodestore.
 *
 * Given a root hash and a target key, walks down the tree following
 * nibbles of the key until reaching a leaf or hitting a missing node.
 */
class TreeWalker
{
private:
    Backend& backend_;

    // Logging partition for tree walking operations
    inline static LogPartition log_partition_{"TREE_WALK", LogLevel::INHERIT};

public:
    explicit TreeWalker(Backend& backend) : backend_(backend)
    {
    }

    static LogPartition&
    get_log_partition()
    {
        return log_partition_;
    }

    /**
     * Walk the tree from root_hash, following the path indicated by target_key.
     *
     * The walk proceeds by:
     * 1. Fetch node at current_hash
     * 2. If it's a leaf (hot_account_node or hot_transaction_node), stop
     * 3. If it's an inner node (hot_unknown), extract nibble from target_key
     *    at current depth and follow that branch
     * 4. Repeat until we reach a leaf or a missing node
     *
     * @param root_hash Hash of the root node to start walking from
     * @param target_key The key we're searching for (determines path)
     * @return WalkResult with the final node and path taken
     */
    WalkResult
    walk(Hash256 const& root_hash, Hash256 const& target_key)
    {
        PLOGD(
            log_partition_,
            "Starting tree walk from root: ",
            root_hash.hex(),
            " looking for key: ",
            target_key.hex());

        WalkResult result;
        result.found = false;
        result.depth = 0;

        Hash256 current_hash = root_hash;
        result.path.push_back(current_hash);

        // Walk until we hit a leaf or missing node
        while (true)
        {
            PLOGD(
                log_partition_,
                "Depth ",
                result.depth,
                ": Fetching node ",
                current_hash.hex());

            // Fetch current node
            auto compressed_opt = backend_.get(current_hash);
            if (!compressed_opt)
            {
                // Node not found - tree is incomplete or corrupt
                PLOGE(
                    log_partition_,
                    "Missing node at hash ",
                    current_hash.hex());
                throw std::runtime_error(
                    "TreeWalker: missing node at hash " + current_hash.hex());
            }

            // Decompress the node
            node_blob decompressed =
                nodeobject_decompress(compressed_opt.value());
            auto node_type = decompressed.get_type();

            PLOGD(
                log_partition_,
                "Node type: ",
                static_cast<int>(node_type),
                " (0=inner, 3=account, 4=tx)");

            // Check if we've reached a leaf
            if (node_type == node_type::hot_account_node ||
                node_type == node_type::hot_transaction_node)
            {
                // Reached a leaf - verify it's the right key by checking
                // last 32 bytes of payload
                auto payload = decompressed.payload();
                PLOGD(
                    log_partition_,
                    "Reached leaf node, payload size: ",
                    payload.size(),
                    " bytes");

                if (payload.size() >= 32)
                {
                    // Last 32 bytes encode the key
                    Hash256 leaf_key(payload.data() + payload.size() - 32);
                    result.found = (leaf_key == target_key);

                    PLOGD(
                        log_partition_,
                        "Leaf key: ",
                        leaf_key.hex(),
                        " | Target: ",
                        target_key.hex(),
                        " | Match: ",
                        (result.found ? "YES" : "NO"));
                }

                result.blob = std::move(decompressed);
                return result;
            }

            // Must be an inner node
            if (node_type != node_type::hot_unknown)
            {
                PLOGE(
                    log_partition_,
                    "Unexpected node type: ",
                    static_cast<int>(node_type));
                throw std::runtime_error(
                    "TreeWalker: unexpected node type " +
                    std::to_string(static_cast<int>(node_type)));
            }

            // Extract nibble from target_key at current depth
            // Depth 0 = high nibble of byte 0, depth 1 = low nibble of byte 0,
            // etc.
            int byte_index = result.depth / 2;
            int nibble_index = result.depth % 2;

            if (byte_index >= 32)
            {
                // We've exhausted all nibbles (depth 64) - something is wrong
                PLOGE(log_partition_, "Exceeded max depth (64 nibbles)");
                throw std::runtime_error(
                    "TreeWalker: exceeded max depth (64 nibbles)");
            }

            uint8_t byte = target_key.data()[byte_index];
            int branch =
                (nibble_index == 0) ? (byte >> 4) : (byte & 0x0F);  // 0-15

            PLOGD(
                log_partition_,
                "Following branch ",
                branch,
                " (byte_idx=",
                byte_index,
                ", nibble_idx=",
                nibble_index,
                ", byte=0x",
                std::hex,
                static_cast<int>(byte),
                std::dec,
                ")");

            // Get branch hash from inner node
            inner_node_view view(decompressed);
            Hash256 branch_hash = view.get_branch(branch);

            if (branch_hash == Hash256::zero())
            {
                // Branch doesn't exist - key not in tree
                PLOGD(
                    log_partition_,
                    "Branch ",
                    branch,
                    " is empty (zero hash) - key not in tree");
                result.blob = std::move(decompressed);
                return result;
            }

            PLOGD(
                log_partition_,
                "Branch ",
                branch,
                " points to: ",
                branch_hash.hex());

            // Follow the branch
            current_hash = branch_hash;
            result.path.push_back(current_hash);
            ++result.depth;
        }
    }

    /**
     * Simple lookup - just returns the blob if found.
     *
     * @param root_hash Hash of the root node
     * @param target_key The key to search for
     * @return The decompressed leaf node blob if found, nullopt otherwise
     */
    std::optional<node_blob>
    lookup(Hash256 const& root_hash, Hash256 const& target_key)
    {
        WalkResult result = walk(root_hash, target_key);
        if (result.found)
        {
            return std::move(result.blob);
        }
        return std::nullopt;
    }
};

}  // namespace catl::nodestore
