#include "catl/crypto/sha512-half-hasher.h"
#include "catl/hybrid-shamap-v2/hybrid-shamap.h"
#include "catl/shamap/shamap-hashprefix.h"
#include <vector>

namespace catl::hybrid_shamap {

using catl::crypto::Sha512HalfHasher;

/**
 * Update hash for a leaf node
 * Simply hashes: prefix + data + key
 */
void
HmapLeafNode::update_hash()
{
    Sha512HalfHasher hasher;

    // Use leaf_node prefix (we're always dealing with state tree for now)
    auto prefix = catl::shamap::HashPrefix::leaf_node;
    hasher.update(prefix.data(), prefix.size());

    // Add data
    auto data_slice = get_data();
    hasher.update(data_slice.data(), data_slice.size());

    // Add key
    hasher.update(key_.data(), Key::size());

    // Finalize and store
    hash_ = hasher.finalize();
}

/**
 * Helper to find first leaf key in a subtree (for collapsed tree handling)
 * Returns the key of the first leaf found
 * @param node The node to search from
 * @param found_key Output: the key if found
 * @return true if a leaf was found
 */
static bool
find_first_leaf_key(const PolyNodePtr& node, Key& found_key)
{
    if (node.is_empty())
        return false;

    // If it's a leaf, extract the key
    if (node.is_leaf())
    {
        if (node.is_materialized())
        {
            auto* leaf = static_cast<HmapLeafNode*>(node.get_materialized());
            found_key = leaf->get_key();
            return true;
        }
        else
        {
            // Get key from mmap leaf header
            const uint8_t* raw = node.get_raw_memory();
            catl::v2::MemPtr<catl::v2::LeafHeader> header(raw);
            const auto& h = header.get_uncopyable();
            found_key = Key(h.key.data());
            return true;
        }
    }

    // It's an inner node - search its children
    if (node.is_materialized())
    {
        auto* inner = static_cast<HmapInnerNode*>(node.get_materialized());
        for (int i = 0; i < 16; ++i)
        {
            auto child = inner->get_child(i);
            if (!child.is_empty())
            {
                if (find_first_leaf_key(child, found_key))
                    return true;
            }
        }
    }
    else
    {
        // Navigate through mmap inner node
        const uint8_t* raw = node.get_raw_memory();
        InnerNodeView view = MemTreeOps::get_inner_node(raw);

        // Try each branch
        for (int i = 0; i < 16; ++i)
        {
            auto child_type = view.get_child_type(i);
            if (child_type != catl::v2::ChildType::EMPTY)
            {
                const uint8_t* child_ptr = view.get_child_ptr(i);
                PolyNodePtr child =
                    PolyNodePtr::make_raw_memory(child_ptr, child_type);
                if (find_first_leaf_key(child, found_key))
                    return true;
            }
        }
    }

    return false;
}

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
            if (child.is_materialized())
            {
                // Heap node - check for depth skip (collapsed tree)
                auto* node = child.get_materialized();

                if (node->get_type() == Type::INNER)
                {
                    auto* inner_child = static_cast<HmapInnerNode*>(node);
                    int child_depth = inner_child->get_depth();
                    int expected_depth = depth_ + 1;

                    if (child_depth > expected_depth)
                    {
                        // We have a collapsed section - need synthetic hashes
                        int skips = child_depth - expected_depth;

                        // Find a representative key from the child subtree
                        Key rep_key(nullptr);
                        if (find_first_leaf_key(child, rep_key))
                        {
                            // Compute synthetic hashes for skipped levels
                            child_hash = compute_synthetic_hash(
                                child,
                                rep_key,
                                expected_depth,
                                child_depth - 1);
                        }
                        else
                        {
                            // No leaf found - this shouldn't happen in a valid
                            // tree Use the node's hash directly
                            child_hash = node->get_hash();
                        }
                    }
                    else
                    {
                        // Normal case - no skip
                        child_hash = node->get_hash();
                    }
                }
                else
                {
                    // Leaf or placeholder - just get hash
                    child_hash = node->get_hash();
                }
            }
            else
            {
                // Mmap node - use perma-cached hash
                // The perma-cached hash already accounts for any synthetic
                // hashes that were needed when the CATL file was created
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