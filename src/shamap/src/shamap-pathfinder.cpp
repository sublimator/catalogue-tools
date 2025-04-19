#include "catl/shamap/shamap-pathfinder.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-impl.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-utils.h"

//----------------------------------------------------------
// PathFinder Implementation
//----------------------------------------------------------
PathFinder::PathFinder(
    boost::intrusive_ptr<SHAMapInnerNode>& root,
    const Key& key,
    SHAMapOptions options)
    : target_key_(key), options_(options)
{
    find_path(root);
}

void
PathFinder::update_path()
{
    // Check if we need to update the path
    find_path(search_root_);
}

std::pair<bool, int>
key_belongs_in_inner(
    const boost::intrusive_ptr<SHAMapInnerNode>& inner,
    const Key& key,
    int start_depth)
{
    auto end_depth = inner->get_depth();
    auto rep_key = inner->first_leaf(inner)->get_item()->key();

    for (int depth = start_depth; depth <= end_depth; depth++)
    {
        int branch = select_branch(key, depth);
        if (branch != select_branch(rep_key, depth))
        {
            return {false, depth};  // Return false and the divergence depth
        }
    }
    return {true, -1};  // Key belongs in inner, no divergence found
}

void
PathFinder::find_path(const boost::intrusive_ptr<SHAMapInnerNode>& root)
{
    if (!root)
    {
        throw NullNodeException("PathFinder: null root node");
    }
    search_root_ = root;
    found_leaf_ = nullptr;
    leaf_key_matches_ = false;
    terminal_branch_ = -1;
    boost::intrusive_ptr<SHAMapInnerNode> current_inner = root;
    while (true)
    {
        int branch = select_branch(target_key_, current_inner->get_depth());
        boost::intrusive_ptr<SHAMapTreeNode> child =
            current_inner->get_child(branch);
        if (!child)
        {
            terminal_branch_ = branch;
            inners.push_back(current_inner);
            break;
        }
        if (child->is_leaf())
        {
            terminal_branch_ = branch;
            inners.push_back(current_inner);
            found_leaf_ = boost::static_pointer_cast<SHAMapLeafNode>(child);
            if (found_leaf_->get_item())
            {
                leaf_key_matches_ =
                    (found_leaf_->get_item()->key() == target_key_);
            }
            else
            {
                throw NullItemException();
            }
            break;
        }
        if (child->is_inner())
        {
            auto inner_child =
                boost::static_pointer_cast<SHAMapInnerNode>(child);
            if (inner_child->get_depth() > current_inner->get_depth())
            {
                // We've found a skipped inner node
                // TODO: handle skipped inner nodes
            }
        }
        inners.push_back(current_inner);
        branches_.push_back(branch);
        current_inner = boost::static_pointer_cast<SHAMapInnerNode>(child);
    }
}

bool
PathFinder::has_leaf() const
{
    return found_leaf_ != nullptr;
}

bool
PathFinder::did_leaf_key_match() const
{
    return leaf_key_matches_;
}

bool
PathFinder::ended_at_null_branch() const
{
    return found_leaf_ == nullptr && terminal_branch_ != -1;
}

boost::intrusive_ptr<const SHAMapLeafNode>
PathFinder::get_leaf() const
{
    return found_leaf_;
}

boost::intrusive_ptr<SHAMapLeafNode>
PathFinder::get_leaf_mutable()
{
    return found_leaf_;
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
    return terminal_branch_;
}

void
PathFinder::dirty_path() const
{
    for (auto& inner : inners)
    {
        inner->invalidate_hash();
    }
}

bool
PathFinder::collapse_path_single_leaf_child()
{
    if (inners.size() <= 1)
        return true;
    boost::intrusive_ptr<SHAMapLeafNode> onlyChild = nullptr;
    auto innermost = inners.back();
    onlyChild = innermost->get_only_child_leaf();
    for (int i = static_cast<int>(inners.size()) - 2; i >= 0; --i)
    {
        auto inner = inners[i];
        int branch = branches_[i];
        if (onlyChild)
        {
            inner->set_child(branch, onlyChild);
        }
        onlyChild = inner->get_only_child_leaf();
        if (!onlyChild)
            break;
    }
    return false;
}

void
PathFinder::collapse_path()
{
    // if (options_.tree_collapse_impl == TreeCollapseImpl::leafs_and_inners)
    // {
    //     return collapse_path_inners();
    // }
    collapse_path_single_leaf_child();
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

    OLOGD(
        "Starting collapse for key ",
        target_key_.hex(),
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
            branches_[i - 1];  // Branch in parent leading to current_inner

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
                OLOGD(
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

            Hash256 parentHashBefore = parent_inner->hash_valid_
                ? parent_inner->hash
                : Hash256::zero();

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

    OLOGD(
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
            (node->hash_valid_ ? "YES" : "NO"),
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
            search_root_ = copy;
        }

        // If not the root, update parent's child pointer to point to this copy
        if (i > 0)
        {
            const auto& parent = inners[i - 1];
            int branch = branches_[i - 1];
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
    if (!leaf_key_matches_)
    {
        throw SHAMapException("Cannot update leaf - key mismatch");
    }

    // Make sure we've handled the inner nodes first
    auto terminal = dirty_or_copy_inners(targetVersion);
    if (!terminal)
    {
        throw SHAMapException("Failed to prepare path for leaf update");
    }

    boost::intrusive_ptr<SHAMapLeafNode> theLeaf = found_leaf_;

    // Check if we need to copy the leaf
    if (found_leaf_->get_version() != targetVersion)
    {
        theLeaf = found_leaf_->copy();
        theLeaf->set_version(targetVersion);
        terminal->set_child(terminal_branch_, theLeaf);
        found_leaf_ = theLeaf;  // Update our reference
    }

    theLeaf->invalidate_hash();
    return theLeaf;
}

LogPartition PathFinder::log_partition_{"PathFinder", LogLevel::DEBUG};
