#include "catl/shamap/shamap-options.h"
#include <array>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>
#include <stack>

#include "catl/shamap/shamap-innernode.h"

#include "catl/crypto/sha512-half-hasher.h"

#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-hashprefix.h"
#include "catl/shamap/shamap-utils.h"
#include <string>
#include <vector>

using catl::crypto::Sha512HalfHasher;

namespace catl::shamap {
/**
 * Calculate the hash for this inner node using the collapsed tree approach.
 *
 * NOTE: We use direct Hash256 objects instead of pointer manipulation to avoid
 * dangling pointer issues. The previous implementation used .data() on
 * temporaries returned from hash functions, which could become invalid when the
 * temporary is destroyed. Different compilers handle temporary lifetimes
 * differently during optimization, causing inconsistent results between
 * GCC/Clang and between debug/release builds. By storing complete Hash256
 * objects and only getting .data() at the point of use, we ensure memory
 * remains valid regardless of compiler optimization.
 */
template <typename Traits>
void
SHAMapInnerNodeT<Traits>::update_hash_collapsed(SHAMapOptions const& options)
{
    auto children = this->get_children();
    uint16_t branch_mask = children->get_branch_mask();

    if (branch_mask == 0)
    {
        this->hash = Hash256::zero();
        this->hash_valid_ = true;
        OLOGD("Empty node (no branches), using zero hash");
        return;
    }

    OLOGD("Calculating hash for node with branch mask ", branch_mask);

    try
    {
        Sha512HalfHasher hasher;

        // Add the prefix
        auto prefix = HashPrefix::inner_node;
        hasher.update(prefix.data(), prefix.size());

        // Use local variable, not static
        const Hash256 zero_hash = Hash256::zero();

        for (int i = 0; i < 16; i++)
        {
            // Create a local Hash256 for each branch
            Hash256 child_hash = zero_hash;

            if (auto child = children->get_child(i))
            {
                if (child->is_inner())
                {
                    auto inner_child =
                        boost::static_pointer_cast<SHAMapInnerNodeT<Traits>>(
                            child);
                    int skips = inner_child->get_depth() - this->depth_ - 1;

                    if (skips > 0)
                    {
                        OLOGD(
                            "Branch ",
                            i,
                            " has skipped inner nodes: ",
                            skips,
                            " levels");

                        auto leaf = first_leaf(inner_child);
                        if (!leaf)
                        {
                            // No leaf found, use regular hash
                            OLOGD(
                                "No leaf found in branch ",
                                i,
                                ", using regular inner hash");
                            child_hash = child->get_hash(options);
                        }
                        else
                        {
                            // Use leaf key for path
                            auto index = leaf->get_item()->key();
                            OLOGD_KEY(
                                "Found leaf for path in branch " +
                                    std::to_string(i) + " with key: ",
                                index);

                            if (options.skipped_inners_hash_impl ==
                                SkippedInnersHashImpl::recursive_simple)
                            {
                                child_hash = compute_skipped_hash_recursive(
                                    options, inner_child, index, 1, skips);
                            }
                            else
                            {
                                child_hash = compute_skipped_hash_stack(
                                    options, inner_child, index, 1, skips);
                            }
                        }
                    }
                    else
                    {
                        // Normal case, no skips
                        OLOGD(
                            "Branch ", i, " has normal inner node (no skips)");
                        child_hash = child->get_hash(options);
                    }
                }
                else
                {
                    // Leaf node
                    OLOGD("Branch ", i, " has leaf node");
                    child_hash = child->get_hash(options);
                }
            }
            else
            {
                OLOGD("Branch ", i, " is empty, using zero hash");
                // child_hash already contains zero hash
            }

            // Use this branch's hash for the digest update
            hasher.update(child_hash.data(), Hash256::size());
        }

        // Finalize hash and take first 256 bits
        this->hash = hasher.finalize();
        this->hash_valid_ = true;

        OLOGD("Hash calculation complete: ", this->hash.hex());

        // Once hash is calculated, canonicalize to save memory
        if (auto canonical = children->canonicalize())
        {
            this->set_children(canonical);
        }
    }
    catch (const std::exception& e)
    {
        throw HashCalculationException(
            std::string("Hash calculation failed: ") + e.what());
    }
}

// Helper method to find the first leaf in a subtree
template <typename Traits>
boost::intrusive_ptr<SHAMapLeafNodeT<Traits>>
SHAMapInnerNodeT<Traits>::first_leaf(
    const boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& inner) const
{
    OLOGD(
        "Searching for first leaf in inner node at depth ",
        static_cast<int>(inner->get_depth()));

    // Create a stack to hold inner nodes to process
    std::stack<boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>> node_stack;
    node_stack.push(inner);

    while (!node_stack.empty())
    {
        auto current = node_stack.top();
        node_stack.pop();

        OLOGD(
            "Processing inner node at depth ",
            static_cast<int>(current->get_depth()));

        // Check all children of this node
        for (int i = 0; i < 16; i++)
        {
            if (!current->has_child(i))
                continue;

            auto child = current->get_child(i);
            OLOGD(
                "Checking branch ",
                i,
                " at depth ",
                static_cast<int>(current->get_depth()));

            if (child->is_leaf())
            {
                // Found a leaf - return it immediately
                OLOGD("Found leaf node at branch ", i);
                return boost::static_pointer_cast<SHAMapLeafNodeT<Traits>>(
                    child);
            }

            if (child->is_inner())
            {
                // Push inner node to stack to process its children later
                OLOGD(
                    "Found inner node at branch ",
                    i,
                    ", adding to processing stack");
                node_stack.push(
                    boost::static_pointer_cast<SHAMapInnerNodeT<Traits>>(
                        child));
            }
        }
    }

    // No leaf found
    OLOGW("No leaf found in inner node subtree");
    return nullptr;
}

template <typename Traits>
Hash256
SHAMapInnerNodeT<Traits>::compute_skipped_hash_recursive(
    const SHAMapOptions& options,
    const boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& inner,
    const Key& index,
    int round,
    int skips) const
{
    // Static zero hash for non-path branches
    static const Hash256 zero_hash = Hash256::zero();

    try
    {
        Sha512HalfHasher hasher;

        // Add the prefix
        auto prefix = HashPrefix::inner_node;
        hasher.update(prefix.data(), prefix.size());

        // Calculate the path depth - this is the parent depth (this->depth_)
        // plus the current round
        int path_depth = this->depth_ + round;

        // Determine which branch we're on at this depth level
        int selected_branch = select_branch(index, path_depth);

        OLOGD(
            "Recursive skipped hash - round=",
            round,
            ", depth=",
            path_depth,
            ", branch=",
            selected_branch,
            ", terminal=",
            (round == skips));

        // Process all 16 branches
        for (int i = 0; i < 16; i++)
        {
            const uint8_t* hash_data = nullptr;

            if (i == selected_branch)
            {
                // This branch is on our path
                if (round == skips)
                {
                    // We're at the terminal level, use the inner node's hash
                    // directly
                    hash_data = inner->get_hash(options).data();
                    OLOGD(
                        "Terminal branch ",
                        i,
                        " using hash: ",
                        inner->get_hash(options).hex().substr(0, 16));
                }
                else
                {
                    // We need to recurse deeper
                    Hash256 next_hash = compute_skipped_hash_recursive(
                        options, inner, index, round + 1, skips);
                    hash_data = next_hash.data();
                    OLOGD(
                        "Non-terminal branch ",
                        i,
                        " using recursive hash: ",
                        next_hash.hex().substr(0, 16));
                }
            }
            else
            {
                // Not on our path - use zero hash
                hash_data = zero_hash.data();
                OLOGD("Branch ", i, " not on path, using zero hash");
            }

            hasher.update(hash_data, Hash256::size());
        }

        // Finalize the hash
        Hash256 result = hasher.finalize();

        OLOGD(
            "Completed round ",
            round,
            " recursive hash: ",
            result.hex().substr(0, 16));

        return result;
    }
    catch (const std::exception& e)
    {
        throw HashCalculationException(
            std::string("Hash calculation failed: ") + e.what());
    }
}

template <typename Traits>
Hash256
SHAMapInnerNodeT<Traits>::compute_skipped_hash_stack(
    const SHAMapOptions& options,
    const boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& inner,
    const Key& index,
    int round,
    int skips) const
{
    // Static zero hash
    static const Hash256 zero_hash = Hash256::zero();

    // Store computed hashes for each level
    std::vector<Hash256> level_hashes(skips - round + 1);

    try
    {
        // Compute hash for each level, starting from the terminal level
        for (int current_round = skips; current_round >= round; current_round--)
        {
            Sha512HalfHasher hasher;

            // Add the prefix
            auto prefix = HashPrefix::inner_node;
            hasher.update(prefix.data(), prefix.size());

            int path_depth = this->depth_ + current_round;
            int selected_branch = select_branch(index, path_depth);

            for (int i = 0; i < 16; i++)
            {
                const uint8_t* hash_data;

                if (i == selected_branch)
                {
                    if (current_round == skips)
                    {
                        // Terminal level - use inner's hash
                        hash_data = inner->get_hash(options).data();
                    }
                    else
                    {
                        // Non-terminal level - use hash from next level
                        hash_data =
                            level_hashes[current_round - round + 1].data();
                    }
                }
                else
                {
                    // Branch not on path - use zero hash
                    hash_data = zero_hash.data();
                }

                hasher.update(hash_data, Hash256::size());
            }

            // Finalize the hash
            level_hashes[current_round - round] = hasher.finalize();
        }

        // Return the hash for the initial level
        return level_hashes[0];
    }
    catch (const std::exception& e)
    {
        throw HashCalculationException(
            std::string("Hash calculation failed: ") + e.what());
    }
}
// Explicit template instantiation for default traits
template void
SHAMapInnerNodeT<DefaultNodeTraits>::update_hash_collapsed(
    SHAMapOptions const& options);
template boost::intrusive_ptr<SHAMapLeafNodeT<DefaultNodeTraits>>
SHAMapInnerNodeT<DefaultNodeTraits>::first_leaf(
    const boost::intrusive_ptr<SHAMapInnerNodeT<DefaultNodeTraits>>& inner)
    const;
template Hash256
SHAMapInnerNodeT<DefaultNodeTraits>::compute_skipped_hash_recursive(
    const SHAMapOptions& options,
    const boost::intrusive_ptr<SHAMapInnerNodeT<DefaultNodeTraits>>& inner,
    const Key& index,
    int round,
    int skips) const;
template Hash256
SHAMapInnerNodeT<DefaultNodeTraits>::compute_skipped_hash_stack(
    const SHAMapOptions& options,
    const boost::intrusive_ptr<SHAMapInnerNodeT<DefaultNodeTraits>>& inner,
    const Key& index,
    int round,
    int skips) const;

}  // namespace catl::shamap