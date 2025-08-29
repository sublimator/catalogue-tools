#include "catl/crypto/sha512-half-hasher.h"
#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/poly-node-operations.h"
#include "catl/shamap/shamap-hashprefix.h"
#include "catl/shamap/shamap-utils.h"
#include <vector>

namespace catl::hybrid_shamap {

using catl::crypto::Sha512HalfHasher;

/**
 * Compute synthetic hash for skipped levels (collapsed tree support)
 */
static Hash256
compute_synthetic_hash(
    const PolyNodePtr& child_node,
    const Key& rep_key,
    int start_depth,
    int end_depth)
{
    // Work backwards from the actual child to the start depth
    std::vector<Hash256> level_hashes(end_depth - start_depth + 1);

    for (int depth = end_depth; depth >= start_depth; --depth)
    {
        Sha512HalfHasher hasher;

        // Add inner node prefix
        auto prefix = catl::shamap::HashPrefix::inner_node;
        hasher.update(prefix.data(), prefix.size());

        // Determine which branch the path goes through at this depth
        int selected_branch = catl::shamap::select_branch(rep_key, depth);

        // Hash all 16 branches
        for (int i = 0; i < 16; ++i)
        {
            if (i == selected_branch)
            {
                if (depth == end_depth)
                {
                    // Terminal level - use the actual child's hash
                    Hash256 child_hash = child_node.get_hash();
                    hasher.update(child_hash.data(), Hash256::size());
                }
                else
                {
                    // Non-terminal - use hash from next level
                    hasher.update(
                        level_hashes[depth - start_depth + 1].data(),
                        Hash256::size());
                }
            }
            else
            {
                // Empty branch - use zero hash
                Hash256 zero = Hash256::zero();
                hasher.update(zero.data(), Hash256::size());
            }
        }

        level_hashes[depth - start_depth] = hasher.finalize();
    }

    return level_hashes[0];
}

/**
 * Update hash for inner node
 * Handles both mmap children (with perma-cached hashes) and heap children
 */
void
HmapInnerNode::update_hash()
{
    Sha512HalfHasher hasher;

    // Add inner node prefix
    auto prefix = catl::shamap::HashPrefix::inner_node;
    hasher.update(prefix.data(), prefix.size());

    // Process all 16 branches
    for (int i = 0; i < 16; ++i)
    {
        Hash256 child_hash = Hash256::zero();

        auto child = get_child(i);
        if (!child.is_empty())
        {
            // Check for collapsed tree (depth skip) in inner nodes
            if (child.is_inner() && child.is_materialized())
            {
                auto* inner_child = child.get_materialized<HmapInnerNode>();
                int child_depth = inner_child->get_depth();
                int expected_depth = depth_ + 1;

                if (child_depth > expected_depth)
                {
                    // We have a collapsed section - need synthetic hashes
                    // Use the inner node's first_leaf_key() method to get
                    // representative key
                    try
                    {
                        Key rep_key = inner_child->first_leaf_key();
                        child_hash = compute_synthetic_hash(
                            child, rep_key, expected_depth, child_depth - 1);
                    }
                    catch (const std::runtime_error&)
                    {
                        // No leaf found - shouldn't happen in valid tree
                        // Use the node's hash directly
                        child_hash = child.get_hash();
                    }
                }
                else
                {
                    // Normal case - no skip
                    child_hash = child.get_hash();
                }
            }
            else
            {
                // For leaves, placeholders, and mmap nodes - just get hash
                // directly. Mmap nodes have perma-cached hashes that already
                // account for any synthetic hashes that were needed when the
                // CATL file was created
                child_hash = child.get_hash();
            }
        }

        // Add this branch's hash to the hasher
        hasher.update(child_hash.data(), Hash256::size());
    }

    // Finalize and store
    hash_ = hasher.finalize();
}

}  // namespace catl::hybrid_shamap