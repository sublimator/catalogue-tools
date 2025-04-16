#include "catl/shamap/shamap-pathfinder.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-impl.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-utils.h"

namespace {
// Anonymous namespace - only visible in this file
void
check_no_existing_skips(
    const std::vector<boost::intrusive_ptr<SHAMapInnerNode>>& inners)
{
    if (inners.size() <= 1)
    {
        return;  // No skips possible with 0 or 1 node
    }

    for (size_t i = 0; i < inners.size() - 1; i++)
    {
        uint8_t currentDepth = inners[i]->get_depth();
        uint8_t nextDepth = inners[i + 1]->get_depth();

        // Check if depths are not sequential (should differ by exactly 1)
        if (nextDepth - currentDepth > 1)
        {
            std::ostringstream oss;
            oss << "INVARIANT VIOLATION: Depth skip detected in path before "
                   "collapse: "
                << "node at index " << i << " has depth "
                << static_cast<int>(currentDepth)
                << " followed by node at depth " << static_cast<int>(nextDepth)
                << " (skipped " << (nextDepth - currentDepth - 1) << " levels)";

            LOGE("PathFinder", oss.str());
            throw std::runtime_error(oss.str());
        }
    }
}
}  // anonymous namespace

//----------------------------------------------------------
// PathFinder Implementation
//----------------------------------------------------------
PathFinder::PathFinder(
    boost::intrusive_ptr<SHAMapInnerNode>& root,
    const Key& key,
    SHAMapOptions options)
    : targetKey(key), options_(options)
{
    find_path(root);
}

void
PathFinder::update_path()
{
    // Check if we need to update the path
    find_path(searchRoot, false);
}

// --- THIS IS THE MODIFIED FUNCTION (Attempt 2) ---
void
PathFinder::find_path(
    boost::intrusive_ptr<SHAMapInnerNode> root,
    bool regenerateSkippedNodes)  // Flag controls regeneration
{
    if (!root)
    {
        // This case should ideally be caught before calling, but double-check
        throw NullNodeException("PathFinder::find_path called with null root");
    }

    OLOGI(  // Use OLOGI from log-macros.h
        "Starting path finding for key ",
        targetKey.hex(),
        ", regenerateSkippedNodes=",
        regenerateSkippedNodes);

    // Reset state for a fresh search
    searchRoot = root;  // Keep track of the root used for this search
    foundLeaf = nullptr;
    leafKeyMatches = false;
    terminalBranch = -1;
    inners.clear();
    branches.clear();

    boost::intrusive_ptr<SHAMapInnerNode> currentInner = root;

    // For logging depth progression
    int pathLevel = 0;

    while (true)  // Main descent loop
    {
        // Ensure currentInner is valid before proceeding in the loop
        if (!currentInner)
        {
            throw NullNodeException(
                "PathFinder: encountered null currentInner during traversal");
        }

        uint8_t currentDepth = currentInner->get_depth();
        int branch = select_branch(targetKey, currentDepth);

        OLOGD(  // Use OLOGD from log-macros.h
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
            // No child at this branch - path ends here
            OLOGD(
                "Reached null branch at depth ",
                static_cast<int>(currentDepth),
                ", branch ",
                branch);

            terminalBranch = branch;
            inners.push_back(
                currentInner);  // Record the parent of the null branch
            break;  // Path finding terminates: Found end of path (null)
        }

        if (child->is_leaf())
        {
            // Found a leaf node - path ends here
            OLOGD(
                "Found leaf at depth ",
                static_cast<int>(currentDepth),
                ", branch ",
                branch);
            terminalBranch = branch;
            inners.push_back(currentInner);  // Record the parent of the leaf
            foundLeaf = boost::static_pointer_cast<SHAMapLeafNode>(child);
            if (!foundLeaf)
            {
                throw SHAMapException(
                    "PathFinder: Failed to cast child to SHAMapLeafNode");
            }
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
                OLOGW("Found leaf node with null item");  // Use OLOGW
                throw NullItemException();  // Leaf node must have an item
            }
            break;  // Path finding terminates: Found leaf
        }

        // Child is an inner node
        auto childInner = boost::static_pointer_cast<SHAMapInnerNode>(child);
        if (!childInner)
        {
            throw SHAMapException(
                "PathFinder: Failed to cast child to SHAMapInnerNode");
        }

        uint8_t childDepth = childInner->get_depth();
        uint8_t expectedDepth = currentDepth + 1;

        // Record current node and branch *before* handling potential
        // skips/regeneration
        inners.push_back(currentInner);
        branches.push_back(branch);

        // Check for depth skips
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

                // Create the missing intermediate inner nodes
                boost::intrusive_ptr<SHAMapInnerNode> lastInner =
                    currentInner;  // Parent of the first node to create
                if (branches.empty())
                {
                    // This should not happen if we detected a skip, as branches
                    // should contain at least the branch from root
                    throw SHAMapException(
                        "PathFinder: branches vector empty during "
                        "regeneration");
                }
                int branchIntoRegen =
                    branches.back();  // Branch from the original parent into
                                      // the regenerated path

                for (uint8_t i = 0; i < skips; i++)
                {
                    uint8_t newDepth = expectedDepth + i;
                    // Branch needed *out* of the node being created (following
                    // targetKey)
                    int skipBranch = select_branch(targetKey, newDepth);

                    OLOGD(
                        "Creating node at depth ",
                        static_cast<int>(newDepth),
                        ", branch out=",
                        skipBranch);

                    auto newInner =
                        boost::intrusive_ptr(new SHAMapInnerNode(newDepth));
                    if (!newInner)
                    {
                        throw std::runtime_error(
                            "Failed to allocate SHAMapInnerNode during "
                            "regeneration");
                    }

                    // If CoW is enabled, propagate properties
                    if (lastInner && lastInner->is_cow_enabled())
                    {
                        newInner->enable_cow(true);
                        newInner->set_version(lastInner->get_version());
                        OLOGD(
                            "Set CoW for new node, version=",
                            lastInner->get_version());
                    }

                    // Link the parent (lastInner) to the newly created node
                    // (newInner).
                    lastInner->set_child(branchIntoRegen, newInner);
                    lastInner->invalidate_hash();  // Invalidate parent hash

                    // Update PathFinder's internal state (add new node to path)
                    // Note: We push onto the *end* of the vectors, maintaining
                    // the path order
                    inners.push_back(newInner);
                    branches.push_back(
                        skipBranch);  // Store branch required *out* of newInner

                    // This new node becomes the parent for the next iteration
                    // *within this loop*
                    lastInner = newInner;
                    // The branch *into* the next regenerated node will be
                    // `skipBranch`
                    branchIntoRegen = skipBranch;  // Update branch for the next
                                                   // parent->child link
                }                                  // End regen loop

                // --- FIX #1 (Linking original child to last regenerated node)
                // ---
                int finalBranch =
                    -1;  // The branch index within lastInner for childInner
                auto representativeLeaf =
                    childInner->first_leaf(childInner);  // Use instance method

                if (representativeLeaf && representativeLeaf->get_item())
                {
                    const Key& representativeKey =
                        representativeLeaf->get_item()->key();
                    // Calculate the branch index at the depth of lastInner
                    // (childDepth - 1) using the key from the actual subtree.
                    finalBranch =
                        select_branch(representativeKey, childDepth - 1);
                    OLOGD(
                        "Regen linking: Found key ",
                        representativeKey.hex().substr(0, 16),
                        " in childInner (depth ",
                        static_cast<int>(childDepth),
                        "). Correct branch from lastInner (depth ",
                        static_cast<int>(lastInner->get_depth()),
                        ") is ",
                        finalBranch);
                }
                else
                {
                    // If no leaf found, the subtree structure might be invalid
                    // or empty.
                    OLOGE(
                        "Regen linking: CRITICAL - Could not find "
                        "representative leaf in childInner (depth ",
                        static_cast<int>(childDepth),
                        ") subtree. Cannot determine correct branch.");
                    throw SHAMapException(
                        "PathFinder regeneration error: Cannot determine "
                        "correct branch "
                        "for child subtree, no leaves found.");
                }

                // Perform the connection using the determined branch
                if (!lastInner)
                {
                    throw NullNodeException(
                        "PathFinder: lastInner became null before final link "
                        "in regeneration");
                }
                lastInner->set_child(finalBranch, childInner);
                lastInner->invalidate_hash();  // Invalidate hash of the node we
                                               // just modified
                // --- END FIX #1 ---

                // --- FIX #2 (Continuing descent) ---
                // After regenerating, continue the main loop's descent
                // starting from the *last regenerated node* (lastInner), NOT
                // the original childInner.
                currentInner = lastInner;
                // Update pathLevel to reflect the depth we are now at
                pathLevel = static_cast<int>(lastInner->get_depth());
                // Use 'continue' to restart the while loop from the top with
                // the correct currentInner
                OLOGD(
                    "Regeneration complete. Continuing descent from depth ",
                    pathLevel);
                continue;  // Skip the default descent step below for this
                           // iteration
                // --- END FIX #2 ---

            }  // End if (regenerateSkippedNodes)
            else
            {
                OLOGD("Skipping node regeneration (flag is false)");
                // If not regenerating, fall through to the default descent
                // using childInner
            }
        }  // End if skip detected

        // Default action (no skip detected, or skip detected but not
        // regenerated): Continue descent into the child node found.
        currentInner = childInner;
        pathLevel++;

    }  // End while(true) loop

    // Log final state after path finding
    static const auto logDepthSkipsAfterFind =
        [this](const std::vector<boost::intrusive_ptr<SHAMapInnerNode>>&
                   path_inners) {
            if (path_inners.size() <= 1)
                return false;
            bool found = false;
            for (size_t i = 0; i < path_inners.size() - 1; i++)
            {
                if (!path_inners[i] || !path_inners[i + 1])
                    continue;  // Skip nulls
                uint8_t currentDepth = path_inners[i]->get_depth();
                uint8_t nextDepth = path_inners[i + 1]->get_depth();
                if (nextDepth - currentDepth > 1)
                {
                    OLOGD(
                        "PathFinder state: Skip found between index ",
                        i,
                        " (depth ",
                        static_cast<int>(currentDepth),
                        ") and ",
                        i + 1,
                        " (depth ",
                        static_cast<int>(nextDepth),
                        ")");
                    found = true;
                }
            }
            return found;
        };

    OLOGD(
        "Path finding complete, found ",
        inners.size(),
        " inner nodes in path, leaf=",
        (foundLeaf ? "YES" : "NO"),
        ", matches=",
        leafKeyMatches,
        ", null_branch_end=",
        ended_at_null_branch(),  // Use helper method
        ", skips_in_path=",      // Log if skips remain *in the path stored*
        logDepthSkipsAfterFind(inners));  // Call lambda
}
// --- END OF MODIFIED FUNCTION ---
// void
// PathFinder::find_path(
//     boost::intrusive_ptr<SHAMapInnerNode> root,
//     bool regenerateSkippedNodes)
// {
//     if (!root)
//     {
//         throw NullNodeException("PathFinder: null root node");
//     }
//
//     OLOGI(
//         "Starting path finding for key ",
//         targetKey.hex(),
//         ", regenerateSkippedNodes=",
//         regenerateSkippedNodes);
//
//     searchRoot = root;
//     foundLeaf = nullptr;
//     leafKeyMatches = false;
//     terminalBranch = -1;
//     inners.clear();
//     branches.clear();
//
//     boost::intrusive_ptr<SHAMapInnerNode> currentInner = root;
//
//     // For logging depth progression
//     int pathLevel = 0;
//
//     while (true)
//     {
//         uint8_t currentDepth = currentInner->get_depth();
//         int branch = select_branch(targetKey, currentDepth);
//
//         OLOGD(
//             "Level ",
//             pathLevel,
//             ", depth=",
//             static_cast<int>(currentDepth),
//             ", selected branch=",
//             branch);
//
//         boost::intrusive_ptr<SHAMapTreeNode> child =
//             currentInner->get_child(branch);
//
//         if (!child)
//         {
//             // No child at this branch
//             OLOGD(
//                 "Reached null branch at depth ",
//                 static_cast<int>(currentDepth),
//                 ", branch ",
//                 branch);
//
//             terminalBranch = branch;
//             inners.push_back(currentInner);
//             break;
//         }
//
//         if (child->is_leaf())
//         {
//             // Found a leaf node
//             OLOGD(
//                 "Found leaf at depth ",
//                 static_cast<int>(currentDepth),
//                 ", branch ",
//                 branch);
//
//             terminalBranch = branch;
//             inners.push_back(currentInner);
//             foundLeaf = boost::static_pointer_cast<SHAMapLeafNode>(child);
//             if (foundLeaf->get_item())
//             {
//                 Key leafKey = foundLeaf->get_item()->key();
//                 leafKeyMatches = (leafKey == targetKey);
//                 OLOGD(
//                     "Leaf key match=",
//                     leafKeyMatches,
//                     ", leaf key=",
//                     leafKey.hex());
//             }
//             else
//             {
//                 OLOGW("Found leaf node with null item");
//                 throw NullItemException();
//             }
//             break;
//         }
//
//         // It's an inner node - check for depth skips
//         auto childInner = boost::static_pointer_cast<SHAMapInnerNode>(child);
//         uint8_t childDepth = childInner->get_depth();
//         uint8_t expectedDepth = currentDepth + 1;
//
//         inners.push_back(currentInner);
//         branches.push_back(branch);
//
//         // Check if we have skipped inner nodes
//         if (childDepth > expectedDepth)
//         {
//             uint8_t skips = childDepth - expectedDepth;
//             OLOGD(
//                 "Detected depth skip - expected=",
//                 static_cast<int>(expectedDepth),
//                 ", actual=",
//                 static_cast<int>(childDepth),
//                 ", skips=",
//                 static_cast<int>(skips));
//
//             if (regenerateSkippedNodes)
//             {
//                 OLOGD(
//                     "Regenerating ", static_cast<int>(skips), " skipped
//                     nodes");
//
//                 // Create the missing inner nodes
//                 boost::intrusive_ptr<SHAMapInnerNode> lastInner =
//                 currentInner; for (uint8_t i = 0; i < skips; i++)
//                 {
//                     uint8_t newDepth = expectedDepth + i;
//                     int skipBranch = select_branch(targetKey, newDepth);
//
//                     OLOGD(
//                         "Creating node at depth ",
//                         static_cast<int>(newDepth),
//                         ", branch ",
//                         skipBranch);
//
//                     // Create new inner node at this depth
//                     auto newInner =
//                         boost::intrusive_ptr(new SHAMapInnerNode(newDepth));
//
//                     // If CoW is enabled, set the same version and CoW flag
//                     if (lastInner->is_cow_enabled())
//                     {
//                         newInner->enable_cow(true);
//                         newInner->set_version(lastInner->get_version());
//                         OLOGD(
//                             "Set CoW for new node, version=",
//                             lastInner->get_version());
//                     }
//
//                     // Replace the child with this new inner node
//                     lastInner->set_child(branches.back(), newInner);
//
//                     // Add to our path
//                     inners.push_back(newInner);
//                     branches.push_back(skipBranch);
//
//                     // Set this as the parent for the next level
//                     lastInner = newInner;
//                 }
//
//                 // Connect the final regenerated inner to the original child
//                 int finalBranch = select_branch(targetKey, childDepth - 1);
//                 OLOGD(
//                     "Connecting final regenerated node to original "
//                     "child at branch ",
//                     finalBranch);
//                 lastInner->set_child(finalBranch, childInner);
//             }
//             else
//             {
//                 OLOGD("Skipping node regeneration (flag is false)");
//             }
//         }
//
//         // Move to the next inner node
//         currentInner = childInner;
//         pathLevel++;
//     }
//
//     static const auto logDepthSkips =
//         [this](
//             const std::vector<boost::intrusive_ptr<SHAMapInnerNode>>& inners)
//             { if (inners.size() <= 1)
//             {
//                 return false;  // No skips possible with 0 or 1 node
//             }
//
//             auto found = false;
//
//             for (size_t i = 0; i < inners.size() - 1; i++)
//             {
//                 uint8_t currentDepth = inners[i]->get_depth();
//                 uint8_t nextDepth = inners[i + 1]->get_depth();
//
//                 // Check if depths are not sequential (should differ by
//                 exactly
//                 // 1)
//                 if (nextDepth - currentDepth > 1)
//                 {
//                     OLOGD(
//                         "Depth skip detected: node at index ",
//                         i,
//                         " (depth ",
//                         static_cast<int>(currentDepth),
//                         ") -> node at index ",
//                         i + 1,
//                         " (depth ",
//                         static_cast<int>(nextDepth),
//                         ") - skipped ",
//                         (nextDepth - currentDepth - 1),
//                         " levels");
//                     found = true;
//                 }
//             }
//             return found;
//         };
//
//     OLOGD(
//         "Path finding complete, found ",
//         inners.size(),
//         " inner nodes, leaf=",
//         (foundLeaf ? "YES" : "NO"),
//         ", matches=",
//         leafKeyMatches,
//         ", skipped=",
//         logDepthSkips(inners));
// }

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

void
PathFinder::collapse_path()
{
    if (options_.collapse_path_single_child_inners)
    {
        return collapse_path_inners();
    }
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

void
PathFinder::collapse_path_inners()
{
    // We need at least a node and its parent in the path to potentially
    // collapse the node. The root node (index 0) cannot be collapsed *by* a
    // parent.
    if (inners.empty())
    {
        OLOGD("No inner nodes in path, nothing to collapse");
        return;
    }

#if PATH_FINDER_CHECK_INVARIANTS
    check_no_existing_skips(inners);
#endif

    OLOGI(
        "Starting collapse for key ",
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

        OLOGD(
            "Checking node at index ",
            i,
            ", depth=",
            static_cast<int>(current_inner->get_depth()),
            ", parent depth=",
            static_cast<int>(parent_inner->get_depth()),
            ", branch in parent=",
            branch_in_parent);

        int child_count = current_inner->get_branch_count();
        OLOGD("Node has ", child_count, " children");

        // Log branch mask for more detailed debugging
        OLOGD(
            "Node branch mask: 0x",
            std::hex,
            current_inner->get_branch_mask(),
            std::dec);

        if (child_count == 1)
        {
            // Node has exactly one child - collapse it.
            boost::intrusive_ptr<SHAMapTreeNode> the_only_child = nullptr;

            // Find the single child using the iterator for efficiency
            auto it = current_inner->children_->begin();
            if (it != current_inner->children_->end())
            {
                the_only_child = *it;
                OLOGD(
                    "Found single child at branch ",
                    it.branch(),
                    ", is_leaf=",
                    (the_only_child->is_leaf() ? "YES" : "NO"));

                // Add extra info about child node type
                if (the_only_child->is_leaf())
                {
                    OLOGD("Child is a leaf node");
                }
                else
                {
                    OLOGD(
                        "Child is an inner node at depth ",
                        static_cast<int>(
                            boost::static_pointer_cast<SHAMapInnerNode>(
                                the_only_child)
                                ->get_depth()),
                        " with ",
                        boost::static_pointer_cast<SHAMapInnerNode>(
                            the_only_child)
                            ->get_branch_count(),
                        " branches");
                }
            }
            else
            {
                OLOGW("Iterator didn't find child despite child_count=1");
            }

            if (the_only_child)  // Should always be true if count is 1
            {
                // Perform the collapse: Parent points directly to the
                // grandchild
                OLOGI(
                    "Collapsing inner node (depth ",
                    static_cast<int>(current_inner->get_depth()),
                    ") under parent (depth ",
                    static_cast<int>(parent_inner->get_depth()),
                    ", branch ",
                    branch_in_parent,
                    ") - linking parent directly to child node");

                // The child keeps its original depth. This creates the "skip".
                // Log the hash of the parent before and after
                // Hash256 parentHashBefore = parent_inner->hashValid
                //     ? parent_inner->hash
                //     : Hash256::zero();

                // Check CoW status before modifying
                if (parent_inner->is_cow_enabled())
                {
                    OLOGD(
                        "Parent has CoW enabled with version ",
                        parent_inner->get_version());

                    if (the_only_child->is_inner())
                    {
                        auto child_inner =
                            boost::static_pointer_cast<SHAMapInnerNode>(
                                the_only_child);
                        if (child_inner->is_cow_enabled())
                        {
                            OLOGD(
                                "Child also has CoW enabled with version ",
                                child_inner->get_version());
                        }
                        else
                        {
                            OLOGD("Child doesn't have CoW enabled yet");
                        }
                    }
                }

                parent_inner->set_child(branch_in_parent, the_only_child);
                parent_inner
                    ->invalidate_hash();  // Parent's hash is now invalid

                // OLOGD(
                //     "Parent hash before collapse: ",
                //     parentHashBefore.hex());

                // Log depth difference which creates a "skip"
                if (the_only_child->is_inner())
                {
                    int depth_diff =
                        boost::static_pointer_cast<SHAMapInnerNode>(
                            the_only_child)
                            ->get_depth() -
                        parent_inner->get_depth();

                    OLOGD(
                        "New skipped depth difference: ",
                        depth_diff,
                        " (parent: ",
                        static_cast<int>(parent_inner->get_depth()),
                        ", child: ",
                        static_cast<int>(
                            boost::static_pointer_cast<SHAMapInnerNode>(
                                the_only_child)
                                ->get_depth()),
                        ")");
                }
                else
                {
                    OLOGD("New child is a leaf node (no depth skipping)");
                }

                needs_invalidation = true;  // Mark that *some* change happened

                // current_inner is now bypassed. Loop continues to check
                // parent_inner.
            }
            else
            {
                OLOGE(
                    "Consistency error: Inner node (depth ",
                    static_cast<int>(current_inner->get_depth()),
                    ") reported 1 child but child pointer is null");
                // This indicates a bug elsewhere, possibly in NodeChildren or
                // set_child
            }
        }
        else if (child_count == 0)
        {
            // Node has become completely empty - remove it from the parent.
            OLOGI(
                "Removing empty inner node (depth ",
                static_cast<int>(current_inner->get_depth()),
                ") from parent (depth ",
                static_cast<int>(parent_inner->get_depth()),
                ", branch ",
                branch_in_parent,
                ")");

            Hash256 parentHashBefore =
                parent_inner->hashValid ? parent_inner->hash : Hash256::zero();

            // Check CoW status before modifying
            if (parent_inner->is_cow_enabled())
            {
                OLOGD(
                    "Parent has CoW enabled with version ",
                    parent_inner->get_version(),
                    " - setting child to nullptr");
            }

            parent_inner->set_child(branch_in_parent, nullptr);
            parent_inner->invalidate_hash();

            OLOGD("Parent hash before removal: ", parentHashBefore.hex());

            needs_invalidation = true;  // Mark that *some* change happened
        }
        else
        {
            OLOGD(
                "No collapse needed for node with ", child_count, " children");
        }
    }

    OLOGI(
        "Collapse process complete, needs_invalidation=",
        (needs_invalidation ? "YES" : "NO"));

    // Call dirty_path() to ensure all nodes on the original path are marked
    // invalid.
    dirty_path();

    // Log hash invalidation
    OLOGD("Called dirty_path() to invalidate all hashes in path");

    // Log the state of each node in the path after collapse
    for (size_t i = 0; i < inners.size(); i++)
    {
        auto node = inners[i];
        OLOGD(
            "Node at index ",
            i,
            ", depth=",
            static_cast<int>(node->get_depth()),
            ", hashValid=",
            (node->hashValid ? "YES" : "NO"),
            ", branchCount=",
            node->get_branch_count(),
            ", branchMask=0x",
            std::hex,
            node->get_branch_mask(),
            std::dec);
    }
}

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
