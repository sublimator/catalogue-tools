#include "catl/shamap/shamap-pathfinder.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-utils.h"

// #define COLLAPSE_PATH_SINGLE_CHILD_INNERS 0

//----------------------------------------------------------
// PathFinder Implementation
//----------------------------------------------------------
PathFinder::PathFinder(
    boost::intrusive_ptr<SHAMapInnerNode>& root,
    const Key& key)
    : targetKey(key)
{
    find_path(root);
}

void
PathFinder::find_path(
    boost::intrusive_ptr<SHAMapInnerNode> root,
    bool regenerateSkippedNodes)
{
    if (!root)
    {
        throw NullNodeException("PathFinder: null root node");
    }

    OLOGI(
        "Starting path finding for key ",
        targetKey.hex(),
        ", regenerateSkippedNodes=",
        regenerateSkippedNodes);

    searchRoot = root;
    foundLeaf = nullptr;
    leafKeyMatches = false;
    terminalBranch = -1;
    inners.clear();
    branches.clear();

    boost::intrusive_ptr<SHAMapInnerNode> currentInner = root;

    // For logging depth progression
    int pathLevel = 0;

    while (true)
    {
        uint8_t currentDepth = currentInner->get_depth();
        int branch = select_branch(targetKey, currentDepth);

        OLOGD(
            "Level ",
            pathLevel,
            ", depth=",
            static_cast<int>(currentDepth),
            ", selected branch=",
            branch);

        boost::intrusive_ptr<SHAMapTreeNode> child =
            currentInner->get_child(branch);

        if (!child)
        {
            // No child at this branch
            OLOGD(
                "Reached null branch at depth ",
                static_cast<int>(currentDepth),
                ", branch ",
                branch);

            terminalBranch = branch;
            inners.push_back(currentInner);
            break;
        }

        if (child->is_leaf())
        {
            // Found a leaf node
            OLOGD(
                "Found leaf at depth ",
                static_cast<int>(currentDepth),
                ", branch ",
                branch);

            terminalBranch = branch;
            inners.push_back(currentInner);
            foundLeaf = boost::static_pointer_cast<SHAMapLeafNode>(child);
            if (foundLeaf->get_item())
            {
                Key leafKey = foundLeaf->get_item()->key();
                leafKeyMatches = (leafKey == targetKey);
                OLOGD(
                    "Leaf key match=",
                    leafKeyMatches,
                    ", leaf key=",
                    leafKey.hex());
            }
            else
            {
                OLOGW("Found leaf node with null item");
                throw NullItemException();
            }
            break;
        }

        // It's an inner node - check for depth skips
        auto childInner = boost::static_pointer_cast<SHAMapInnerNode>(child);
        uint8_t childDepth = childInner->get_depth();
        uint8_t expectedDepth = currentDepth + 1;

        inners.push_back(currentInner);
        branches.push_back(branch);

        // Check if we have skipped inner nodes
        if (childDepth > expectedDepth)
        {
            uint8_t skips = childDepth - expectedDepth;
            OLOGD(
                "Detected depth skip - expected=",
                static_cast<int>(expectedDepth),
                ", actual=",
                static_cast<int>(childDepth),
                ", skips=",
                static_cast<int>(skips));

            if (regenerateSkippedNodes)
            {
                OLOGD(
                    "Regenerating ", static_cast<int>(skips), " skipped nodes");

                // Create the missing inner nodes
                boost::intrusive_ptr<SHAMapInnerNode> lastInner = currentInner;
                for (uint8_t i = 0; i < skips; i++)
                {
                    uint8_t newDepth = expectedDepth + i;
                    int skipBranch = select_branch(targetKey, newDepth);

                    OLOGD(
                        "Creating node at depth ",
                        static_cast<int>(newDepth),
                        ", branch ",
                        skipBranch);

                    // Create new inner node at this depth
                    auto newInner =
                        boost::intrusive_ptr(new SHAMapInnerNode(newDepth));

                    // If CoW is enabled, set the same version and CoW flag
                    if (lastInner->is_cow_enabled())
                    {
                        newInner->enable_cow(true);
                        newInner->set_version(lastInner->get_version());
                        OLOGD(
                            "Set CoW for new node, version=",
                            lastInner->get_version());
                    }

                    // Replace the child with this new inner node
                    lastInner->set_child(branches.back(), newInner);

                    // Add to our path
                    inners.push_back(newInner);
                    branches.push_back(skipBranch);

                    // Set this as the parent for the next level
                    lastInner = newInner;
                }

                // Connect the final regenerated inner to the original child
                int finalBranch = select_branch(targetKey, childDepth - 1);
                OLOGD(
                    "Connecting final regenerated node to original "
                    "child at branch ",
                    finalBranch);
                lastInner->set_child(finalBranch, childInner);
            }
            else
            {
                OLOGD("Skipping node regeneration (flag is false)");
            }
        }

        // Move to the next inner node
        currentInner = childInner;
        pathLevel++;
    }

    OLOGI(
        "Path finding complete, found ",
        inners.size(),
        " inner nodes, leaf=",
        (foundLeaf ? "YES" : "NO"),
        ", matches=",
        leafKeyMatches);
}

// void
// PathFinder::find_path(boost::intrusive_ptr<SHAMapInnerNode> root)
// {
//     if (!root)
//     {
//         throw NullNodeException("PathFinder: null root node");
//     }
//     searchRoot = root;
//     foundLeaf = nullptr;
//     leafKeyMatches = false;
//     terminalBranch = -1;
//     boost::intrusive_ptr<SHAMapInnerNode> currentInner = root;
//     while (true)
//     {
//         int branch = select_branch(targetKey, currentInner->get_depth());
//         boost::intrusive_ptr<SHAMapTreeNode> child =
//             currentInner->get_child(branch);
//         if (!child)
//         {
//             terminalBranch = branch;
//             inners.push_back(currentInner);
//             break;
//         }
//         if (child->is_leaf())
//         {
//             terminalBranch = branch;
//             inners.push_back(currentInner);
//             foundLeaf = boost::static_pointer_cast<SHAMapLeafNode>(child);
//             if (foundLeaf->get_item())
//             {
//                 leafKeyMatches = (foundLeaf->get_item()->key() == targetKey);
//             }
//             else
//             {
//                 throw NullItemException();
//             }
//             break;
//         }
//         inners.push_back(currentInner);
//         branches.push_back(branch);
//         currentInner = boost::static_pointer_cast<SHAMapInnerNode>(child);
//     }
// }

bool
PathFinder::has_leaf() const
{
    return foundLeaf != nullptr;
}

bool
PathFinder::did_leaf_key_match() const
{
    return leafKeyMatches;
}

bool
PathFinder::ended_at_null_branch() const
{
    return foundLeaf == nullptr && terminalBranch != -1;
}

boost::intrusive_ptr<const SHAMapLeafNode>
PathFinder::get_leaf() const
{
    return foundLeaf;
}

boost::intrusive_ptr<SHAMapLeafNode>
PathFinder::get_leaf_mutable()
{
    return foundLeaf;
}

boost::intrusive_ptr<SHAMapInnerNode>
PathFinder::get_parent_of_terminal()
{
    return inners.empty() ? nullptr : inners.back();
}

boost::intrusive_ptr<const SHAMapInnerNode>
PathFinder::get_parent_of_terminal() const
{
    return inners.empty() ? nullptr : inners.back();
}

int
PathFinder::get_terminal_branch() const
{
    return terminalBranch;
}

void
PathFinder::dirty_path() const
{
    for (auto& inner : inners)
    {
        inner->invalidate_hash();
    }
}

#ifndef COLLAPSE_PATH_SINGLE_CHILD_INNERS
void
PathFinder::collapse_path()
{
    if (inners.size() <= 1)
        return;
    boost::intrusive_ptr<SHAMapLeafNode> onlyChild = nullptr;
    auto innermost = inners.back();
    onlyChild = innermost->get_only_child_leaf();
    for (int i = static_cast<int>(inners.size()) - 2; i >= 0; --i)
    {
        auto inner = inners[i];
        int branch = branches[i];
        if (onlyChild)
        {
            inner->set_child(branch, onlyChild);
        }
        onlyChild = inner->get_only_child_leaf();
        if (!onlyChild)
            break;
    }
}
#endif

#ifdef COLLAPSE_PATH_SINGLE_CHILD_INNERS
void
PathFinder::collapse_path()
{
    // We need at least a node and its parent in the path to potentially
    // collapse the node. The root node (index 0) cannot be collapsed *by* a
    // parent.
    if (inners.empty())
    {
        LOGD(
            "PathFinder::collapse_path: No inner nodes in path, nothing to "
            "collapse");
        return;
    }

    LOGI(
        "PathFinder::collapse_path: Starting collapse for key ",
        targetKey.hex(),
        ", path length=",
        inners.size());

    bool needs_invalidation = false;

    // Iterate upwards from the parent of the terminal position towards the
    // root. Stop *before* index 0, as the root itself cannot be removed by its
    // parent.
    for (int i = static_cast<int>(inners.size()) - 1; i > 0; --i)
    {
        auto current_inner = inners[i];
        auto parent_inner =
            inners[i - 1];  // Parent is guaranteed to exist since i > 0
        int branch_in_parent =
            branches[i - 1];  // Branch in parent leading to current_inner

        LOGD(
            "PathFinder::collapse_path: Checking node at index ",
            i,
            ", depth=",
            static_cast<int>(current_inner->get_depth()),
            ", parent depth=",
            static_cast<int>(parent_inner->get_depth()),
            ", branch in parent=",
            branch_in_parent);

        int child_count = current_inner->get_branch_count();
        LOGD("PathFinder::collapse_path: Node has ", child_count, " children");

        if (child_count == 1)
        {
            // Node has exactly one child - collapse it.
            boost::intrusive_ptr<SHAMapTreeNode> the_only_child = nullptr;

            // Find the single child using the iterator for efficiency
            auto it = current_inner->children_->begin();
            if (it != current_inner->children_->end())
            {
                the_only_child = *it;
                LOGD(
                    "PathFinder::collapse_path: Found single child at branch ",
                    it.branch(),
                    ", is_leaf=",
                    (the_only_child->is_leaf() ? "YES" : "NO"),
                    ", child hash=",
                    the_only_child->get_hash().hex());
            }
            else
            {
                LOGW(
                    "PathFinder::collapse_path: Iterator didn't find child "
                    "despite child_count=1");
            }

            if (the_only_child)  // Should always be true if count is 1
            {
                // Perform the collapse: Parent points directly to the
                // grandchild
                LOGI(
                    "PathFinder::collapse_path: Collapsing inner node (depth ",
                    static_cast<int>(current_inner->get_depth()),
                    ") under parent (depth ",
                    static_cast<int>(parent_inner->get_depth()),
                    ", branch ",
                    branch_in_parent,
                    ") - linking parent directly to child node");

                // The child keeps its original depth. This creates the "skip".
                // Log the hash of the parent before and after
                Hash256 parentHashBefore = parent_inner->hashValid
                    ? parent_inner->hash
                    : Hash256::zero();

                parent_inner->set_child(branch_in_parent, the_only_child);
                parent_inner
                    ->invalidate_hash();  // Parent's hash is now invalid

                LOGD(
                    "PathFinder::collapse_path: Parent hash before collapse: ",
                    parentHashBefore.hex());
                LOGD(
                    "PathFinder::collapse_path: New skipped depth difference: ",
                    (the_only_child->is_inner()
                         ? std::to_string(
                               boost::static_pointer_cast<SHAMapInnerNode>(
                                   the_only_child)
                                   ->get_depth() -
                               parent_inner->get_depth())
                         : "N/A (leaf)"));

                needs_invalidation = true;  // Mark that *some* change happened

                // current_inner is now bypassed. Loop continues to check
                // parent_inner.
            }
            else
            {
                LOGE(
                    "PathFinder::collapse_path: Consistency error: Inner node "
                    "(depth ",
                    static_cast<int>(current_inner->get_depth()),
                    ") reported 1 child but child pointer is null");
                // This indicates a bug elsewhere, possibly in NodeChildren or
                // set_child
            }
        }
        else if (child_count == 0)
        {
            // Node has become completely empty - remove it from the parent.
            LOGI(
                "PathFinder::collapse_path: Removing empty inner node (depth ",
                static_cast<int>(current_inner->get_depth()),
                ") from parent (depth ",
                static_cast<int>(parent_inner->get_depth()),
                ", branch ",
                branch_in_parent,
                ")");

            Hash256 parentHashBefore =
                parent_inner->hashValid ? parent_inner->hash : Hash256::zero();

            parent_inner->set_child(branch_in_parent, nullptr);
            parent_inner->invalidate_hash();

            LOGD(
                "PathFinder::collapse_path: Parent hash before removal: ",
                parentHashBefore.hex());

            needs_invalidation = true;  // Mark that *some* change happened
        }
        else
        {
            LOGD(
                "PathFinder::collapse_path: No collapse needed for node with ",
                child_count,
                " children");
        }
    }

    LOGI(
        "PathFinder::collapse_path: Collapse process complete, "
        "needs_invalidation=",
        (needs_invalidation ? "YES" : "NO"));

    // Call dirty_path() to ensure all nodes on the original path are marked
    // invalid.
    dirty_path();

    // Log hash invalidation
    LOGD(
        "PathFinder::collapse_path: Called dirty_path() to invalidate all "
        "hashes in path");
    for (size_t i = 0; i < inners.size(); i++)
    {
        LOGD(
            "PathFinder::collapse_path: Node at index ",
            i,
            ", depth=",
            static_cast<int>(inners[i]->get_depth()),
            ", hashValid=",
            (inners[i]->hashValid ? "YES" : "NO"));
    }
}
#endif

bool
PathFinder::maybe_copy_on_write() const
{
    return !inners.empty() && inners.back()->is_cow_enabled();
}

boost::intrusive_ptr<SHAMapInnerNode>
PathFinder::dirty_or_copy_inners(int targetVersion)
{
    if (inners.empty())
    {
        LOGW("No inner nodes in path to apply CoW");
        return nullptr;
    }

    // Start from the root and work downward
    // Remove the unused variable

    for (size_t i = 0; i < inners.size(); ++i)
    {
        auto& currentInner = inners[i];

        // Skip if already at target version
        if (currentInner->get_version() == targetVersion)
        {
            LOGD(
                "Node at index ",
                i,
                " already at target version ",
                targetVersion);
            continue;
        }

        // Skip nodes that don't have CoW enabled
        if (!currentInner->is_cow_enabled())
        {
            // Just update version
            LOGD(
                "Node at index ",
                i,
                " has CoW disabled, updating version from ",
                currentInner->get_version(),
                " to ",
                targetVersion);
            currentInner->set_version(targetVersion);
            continue;
        }

        // Need to create a copy (CoW)
        LOGD(
            "Creating CoW copy of node at index ",
            i,
            " version ",
            currentInner->get_version(),
            " to version ",
            targetVersion);

        // Create copy with new version
        auto copy = currentInner->copy(targetVersion);

        // If this is the root, update the search root
        if (i == 0)
        {
            searchRoot = copy;
        }

        // If not the root, update parent's child pointer to point to this copy
        if (i > 0)
        {
            const auto& parent = inners[i - 1];
            int branch = branches[i - 1];
            LOGD(
                "Updating parent at depth ",
                parent->get_depth(),
                " branch ",
                branch,
                " to point to new copy");
            parent->set_child(branch, copy);
        }

        // Replace in our path vector
        inners[i] = copy;
    }

    // Return the innermost node for further operations
    return inners.back();
}

boost::intrusive_ptr<SHAMapLeafNode>
PathFinder::invalidated_possibly_copied_leaf_for_updating(int targetVersion)
{
    if (!leafKeyMatches)
    {
        throw SHAMapException("Cannot update leaf - key mismatch");
    }

    // Make sure we've handled the inner nodes first
    auto terminal = dirty_or_copy_inners(targetVersion);
    if (!terminal)
    {
        throw SHAMapException("Failed to prepare path for leaf update");
    }

    boost::intrusive_ptr<SHAMapLeafNode> theLeaf = foundLeaf;

    // Check if we need to copy the leaf
    if (foundLeaf->get_version() != targetVersion)
    {
        theLeaf = foundLeaf->copy();
        theLeaf->set_version(targetVersion);
        terminal->set_child(terminalBranch, theLeaf);
        foundLeaf = theLeaf;  // Update our reference
    }

    theLeaf->invalidate_hash();
    return theLeaf;
}

LogPartition PathFinder::log_partition_{"PathFinder", LogLevel::DEBUG};
