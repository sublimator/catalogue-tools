#include "catl/core/logger.h"
#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-leafnode.h"
#include "catl/hybrid-shamap-v2/hmap-pathfinder.h"
#include "catl/hybrid-shamap-v2/hmap.h"
#include "catl/hybrid-shamap-v2/poly-node-operations.h"
#include "catl/shamap/shamap-utils.h"

namespace catl::hybrid_shamap {

shamap::SetResult
Hmap::set_item(const Key& key, const Slice& data, shamap::SetMode mode)
{
    // If we have no root, create one
    if (!root_)
    {
        // Create a new root at depth 0
        root_ = PolyNodePtr::adopt_materialized(new HmapInnerNode(0));
    }

    // Find the path to where this key should go
    HmapPathFinder pathfinder(key);
    pathfinder.find_path(root_);

    // Handle divergence if needed
    if (pathfinder.has_divergence())
    {
        pathfinder.materialize_path();
        pathfinder.add_node_at_divergence();
    }
    else
    {
        // Materialize the path so we can modify it
        pathfinder.materialize_path();
    }

    // Update root if it was materialized
    const auto& path = pathfinder.get_path();
    if (!path.empty())
    {
        root_ = path[0].first;  // Update root to materialized version
    }

    // Check if we found an existing leaf with this key
    if (pathfinder.found_leaf() && pathfinder.key_matches())
    {
        // UPDATE case - replace the old leaf with a new one
        // (Leaves are immutable, so we always create a new leaf)

        // Find the parent of the leaf
        for (size_t i = 0; i < path.size(); ++i)
        {
            if (i > 0 && path[i].first.is_leaf())
            {
                // Previous node is the parent
                auto& parent_node = path[i - 1].first;
                assert(parent_node.is_inner() && parent_node.is_materialized());

                auto* parent = parent_node.get_materialized<HmapInnerNode>();
                int branch = path[i].second;

                // Create new leaf with updated data
                parent->set_child(
                    branch,
                    PolyNodePtr::adopt_materialized(
                        new HmapLeafNode(key, data)),
                    v2::ChildType::LEAF);

                // Check mode constraints
                if (mode == shamap::SetMode::ADD_ONLY)
                {
                    // Item exists but ADD_ONLY was specified
                    return shamap::SetResult::FAILED;
                }
                return shamap::SetResult::UPDATE;
            }
        }

        // This shouldn't happen - we found a leaf but can't find its parent
        throw std::runtime_error("Found leaf but couldn't find parent in path");
    }

    // ADD case - need to insert a new leaf

    // Get the last inner node in the path
    if (path.empty())
    {
        throw std::runtime_error(
            "Path should not be empty after materialization");
    }

    // Find the deepest inner node
    HmapInnerNode* insert_parent = nullptr;
    int insert_depth = 0;
    int terminal_branch = pathfinder.get_terminal_branch();

    // If we have a terminal branch from divergence handling, use that
    if (terminal_branch >= 0 && !path.empty())
    {
        // Get the last inner node
        for (auto it = path.rbegin(); it != path.rend(); ++it)
        {
            if (it->first.is_inner() && it->first.is_materialized())
            {
                insert_parent = it->first.get_materialized<HmapInnerNode>();
                insert_depth = insert_parent->get_depth();
                break;
            }
        }
    }
    else
    {
        // Normal case - find insertion point
        for (auto it = path.rbegin(); it != path.rend(); ++it)
        {
            if (it->first.is_inner() && it->first.is_materialized())
            {
                insert_parent = it->first.get_materialized<HmapInnerNode>();
                insert_depth = insert_parent->get_depth();
                terminal_branch = shamap::select_branch(key, insert_depth);
                break;
            }
        }
    }

    if (!insert_parent)
    {
        throw std::runtime_error("No inner node found in path");
    }

    // Check what's at that branch
    auto existing = insert_parent->get_child(terminal_branch);

    if (existing.is_empty())
    {
        // Simple case - empty branch, just insert the leaf
        insert_parent->set_child(
            terminal_branch,
            PolyNodePtr::adopt_materialized(new HmapLeafNode(key, data)),
            v2::ChildType::LEAF);

        // Check mode constraints
        if (mode == shamap::SetMode::UPDATE_ONLY)
        {
            // Item doesn't exist but UPDATE_ONLY was specified
            return shamap::SetResult::FAILED;
        }
        return shamap::SetResult::ADD;
    }
    else if (existing.is_leaf())
    {
        // Collision - need to create intermediate inner node(s)

        // Get the existing leaf's key
        Key existing_key = poly_get_leaf_key(existing);

        // Find where they diverge using existing utility
        int divergence_depth =
            shamap::find_divergence_depth(key, existing_key, insert_depth + 1);

        // Create new inner node at divergence depth
        auto* divergence_node = new HmapInnerNode(divergence_depth);

        // Add the new leaf
        divergence_node->set_child(
            shamap::select_branch(key, divergence_depth),
            PolyNodePtr::adopt_materialized(new HmapLeafNode(key, data)),
            v2::ChildType::LEAF);

        // Add the existing leaf
        divergence_node->set_child(
            shamap::select_branch(existing_key, divergence_depth),
            existing,
            v2::ChildType::LEAF);

        // Replace the branch in the parent
        insert_parent->set_child(
            terminal_branch,
            PolyNodePtr::adopt_materialized(divergence_node),
            v2::ChildType::INNER);

        // Check mode constraints
        if (mode == shamap::SetMode::UPDATE_ONLY)
        {
            // Item doesn't exist but UPDATE_ONLY was specified
            return shamap::SetResult::FAILED;
        }
        return shamap::SetResult::ADD;
    }
    else
    {
        // Existing is an inner node - this shouldn't happen if pathfinder
        // worked correctly
        throw std::runtime_error("Unexpected inner node at insertion point");
    }
}

}  // namespace catl::hybrid_shamap