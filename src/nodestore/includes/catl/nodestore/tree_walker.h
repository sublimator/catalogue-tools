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

            // Fetch current node (compressed)
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

            node_blob const& compressed = compressed_opt.value();
            auto node_type = compressed.get_type();

            PLOGD(
                log_partition_,
                "Node type: ",
                static_cast<int>(node_type),
                " (0=inner, 3=account, 4=tx)");

            // Check if we've reached a leaf
            if (node_type == node_type::hot_account_node ||
                node_type == node_type::hot_transaction_node)
            {
                // Reached a leaf - need to decompress to verify key
                node_blob decompressed = nodeobject_decompress(compressed);
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

            // Must be an inner node - validate type
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

            // Get branch hash from compressed inner node (zero-copy!)
            compressed_inner_node_view view(compressed);
            auto branch_hash_opt = view.get_child_hash(branch);

            if (!branch_hash_opt)
            {
                // Branch doesn't exist - key not in tree
                PLOGD(
                    log_partition_,
                    "Branch ",
                    branch,
                    " is empty - key not in tree");
                // Return decompressed version for consistency
                result.blob = nodeobject_decompress(compressed);
                return result;
            }

            Hash256 branch_hash = branch_hash_opt.value();

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

    /**
     * Walk all leaves in a tree (depth-first traversal).
     * Calls the visitor function for each leaf node found.
     *
     * @param root_hash Hash of the root node
     * @param visitor Function called for each leaf: visitor(hash,
     * decompressed_blob)
     */
    template <typename Visitor>
    void
    walk_all(Hash256 const& root_hash, Visitor&& visitor)
    {
        PLOGD(
            log_partition_,
            "Starting depth-first walk from root: ",
            root_hash.hex());

        walk_all_recursive(root_hash, std::forward<Visitor>(visitor), 0);

        PLOGD(log_partition_, "Depth-first walk completed");
    }

private:
    /**
     * Recursive helper for depth-first tree traversal.
     */
    template <typename Visitor>
    void
    walk_all_recursive(
        Hash256 const& current_hash,
        Visitor&& visitor,
        int depth)
    {
        // Fetch current node (compressed)
        auto compressed_opt = backend_.get(current_hash);
        if (!compressed_opt)
        {
            PLOGE(log_partition_, "Missing node at hash ", current_hash.hex());
            throw std::runtime_error(
                "TreeWalker: missing node at hash " + current_hash.hex());
        }

        node_blob const& compressed = compressed_opt.value();
        auto node_type = compressed.get_type();

        // Check if we've reached a leaf
        if (node_type == node_type::hot_account_node ||
            node_type == node_type::hot_transaction_node)
        {
            // Decompress leaf and call visitor
            node_blob decompressed = nodeobject_decompress(compressed);
            visitor(current_hash, decompressed);
            return;
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

        // Visit all branches (depth-first)
        compressed_inner_node_view view(compressed);
        for (int branch = 0; branch < 16; ++branch)
        {
            auto branch_hash_opt = view.get_child_hash(branch);
            if (branch_hash_opt)
            {
                walk_all_recursive(
                    branch_hash_opt.value(),
                    std::forward<Visitor>(visitor),
                    depth + 1);
            }
        }
    }
};

}  // namespace catl::nodestore
