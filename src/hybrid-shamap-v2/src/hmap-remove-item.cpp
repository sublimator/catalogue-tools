#include "catl/core/logger.h"
#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-pathfinder.h"
#include "catl/hybrid-shamap-v2/hmap.h"
#include "catl/v2/catl-v2-structs.h"

namespace catl::hybrid_shamap {

bool
Hmap::remove_item(const Key& key)
{
    LOGD("[remove_item] Starting removal for key: ", key.hex());

    if (!root_)
    {
        LOGD("[remove_item] Empty tree, nothing to remove");
        return false;  // Empty tree
    }

    // Find the path to the key
    HmapPathFinder pathfinder(key);
    pathfinder.find_path(root_);

    // Check if we found the key
    if (!pathfinder.found_leaf() || !pathfinder.key_matches())
    {
        LOGD("[remove_item] Key not found: ", key.hex());
        return false;  // Key not found
    }

    LOGD(
        "[remove_item] Found key, materializing path of size ",
        pathfinder.get_path().size());

    // Materialize the path so we can modify it
    pathfinder.materialize_path();

    // Update root if it was materialized
    const auto& path = pathfinder.get_path();
    if (!path.empty())
    {
        assert(
            path[0].first.is_materialized() &&
            "Root should be materialized after materialize_path()");
        root_ = path[0].first;  // Update root to materialized version
    }

    // Verify all nodes in path are properly set up
    for (size_t i = 0; i < path.size(); ++i)
    {
        LOGD(
            "[remove_item] Path[",
            i,
            "] is_materialized=",
            path[i].first.is_materialized(),
            " is_leaf=",
            path[i].first.is_leaf(),
            " is_inner=",
            path[i].first.is_inner(),
            " branch=",
            path[i].second);
        if (i < path.size() - 1)  // All non-leaf nodes should be materialized
        {
            assert(
                path[i].first.is_materialized() &&
                "All inner nodes in path should be materialized");
            assert(
                path[i].first.is_inner() &&
                "Non-terminal path nodes should be inner nodes");
        }
    }

    // Find the parent of the leaf to remove
    HmapInnerNode* parent = nullptr;
    int branch_to_remove = -1;
    size_t leaf_index = 0;

    for (size_t i = 0; i < path.size(); ++i)
    {
        if (i > 0 && path[i].first.is_leaf())
        {
            // Previous node is the parent
            auto& parent_node = path[i - 1].first;
            assert(
                parent_node.is_inner() &&
                "Parent of leaf should be inner node");
            assert(
                parent_node.is_materialized() &&
                "Parent should be materialized");

            if (parent_node.is_inner() && parent_node.is_materialized())
            {
                parent = parent_node.get_materialized<HmapInnerNode>();
                branch_to_remove = path[i].second;
                leaf_index = i;
                LOGD(
                    "[remove_item] Found leaf at path[",
                    i,
                    "], parent at [",
                    i - 1,
                    "], branch=",
                    branch_to_remove);
                break;
            }
        }
    }

    if (!parent || branch_to_remove == -1)
    {
        LOGE("[remove_item] Couldn't find parent of leaf!");
        return false;  // Couldn't find parent
    }

    LOGD(
        "[remove_item] Removing leaf from parent at branch ", branch_to_remove);

    // Remove the leaf
    parent->set_child(
        branch_to_remove, PolyNodePtr::make_empty(), v2::ChildType::EMPTY);

    LOGD("[remove_item] Starting collapse phase");

    // Collapse path - promote single children up the tree
    // Start from the parent and work our way up
    for (int i = static_cast<int>(path.size()) - 2; i >= 0; --i)
    {
        LOGD("[remove_item] Checking collapse at path[", i, "]");

        // After materialize_path(), ALL inner nodes in path should be
        // materialized! (The leaf might still be raw, but we've already removed
        // it)
        if (i < static_cast<int>(leaf_index))
        {
            assert(
                path[i].first.is_materialized() &&
                "Inner path node not materialized after materialize_path()!");
        }

        if (!path[i].first.is_inner())
        {
            LOGD("[remove_item] Path[", i, "] is not inner, skipping");
            continue;  // It's a leaf, skip
        }

        assert(
            path[i].first.is_materialized() &&
            "Inner node must be materialized for collapse");
        auto* inner = path[i].first.get_materialized<HmapInnerNode>();

        // Count children and find the single child if there is one
        PolyNodePtr single_child;
        int child_count = 0;
        int single_child_branch = -1;

        for (int branch = 0; branch < 16; ++branch)
        {
            auto child = inner->get_child(branch);
            if (!child.is_empty())
            {
                child_count++;
                if (child_count == 1)
                {
                    single_child = child;
                    single_child_branch = branch;
                }
                else if (child_count > 1)
                {
                    break;  // More than one child, can't collapse
                }
            }
        }

        LOGD("[remove_item] Path[", i, "] has ", child_count, " children");
        if (child_count == 1)
        {
            LOGD(
                "  Single child at branch ",
                single_child_branch,
                " is_leaf=",
                single_child.is_leaf(),
                " is_materialized=",
                single_child.is_materialized());
        }

        // If this inner node has only one child and it's a leaf, promote it
        if (child_count == 1 && single_child.is_leaf() && i > 0)
        {
            LOGD(
                "[remove_item] Collapsing: promoting single leaf child up from "
                "path[",
                i,
                "]");

            // Get the parent of this inner node
            auto& parent_entry = path[i - 1];
            assert(parent_entry.first.is_inner() && "Parent must be inner");
            assert(
                parent_entry.first.is_materialized() &&
                "Parent must be materialized");

            if (parent_entry.first.is_inner() &&
                parent_entry.first.is_materialized())
            {
                auto* parent_inner =
                    parent_entry.first.get_materialized<HmapInnerNode>();
                int branch_in_parent = path[i].second;

                LOGD(
                    "  Replacing inner at parent's branch ",
                    branch_in_parent,
                    " with leaf (type=",
                    static_cast<int>(single_child.get_type()),
                    ")");

                // Replace this inner node with its single leaf child
                // IMPORTANT: preserve the actual type of the child
                parent_inner->set_child(
                    branch_in_parent, single_child, single_child.get_type());
            }
        }
        else if (child_count == 1 && !single_child.is_leaf())
        {
            LOGD("[remove_item] Single child is inner node, stopping collapse");
            break;  // Don't collapse inner nodes
        }
        else if (child_count > 1)
        {
            LOGD(
                "[remove_item] Multiple children (",
                child_count,
                "), stopping collapse");
            break;
        }
        else if (child_count == 0)
        {
            LOGW(
                "[remove_item] Inner node has NO children after removal! This "
                "shouldn't happen");
        }
    }

    LOGD("[remove_item] Successfully removed key: ", key.hex());
    return true;
}

}  // namespace catl::hybrid_shamap