#include "catl/shamap/shamap-pathfinder.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap-options.h"
#include "catl/shamap/shamap-treenode.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-utils.h"
#include <cstddef>
#include <stdexcept>
#include <utility>

namespace catl::shamap {
//----------------------------------------------------------
// PathFinderT Implementation
//----------------------------------------------------------

// Helper function template declaration
template <typename Traits>
std::pair<bool, int>
key_belongs_in_inner(
    const boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& inner,
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

template <typename Traits>
PathFinderT<Traits>::PathFinderT(
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& root,
    const Key& key,
    SHAMapOptions options)
    : target_key_(key), options_(options)
{
    search_root_ = root;
}

template <typename Traits>
void
PathFinderT<Traits>::find_path()
{
    auto root = search_root_;
    if (!root)
    {
        throw NullNodeException("PathFinder: null root node");
    }
    if (root->get_depth() != 0)
    {
        throw InvalidDepthException(root->get_depth(), 0);
    }
    found_leaf_ = nullptr;
    leaf_key_matches_ = false;
    terminal_branch_ = -1;
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>> current_inner = root;
    while (true)
    {
        int branch = select_branch(target_key_, current_inner->get_depth());
        boost::intrusive_ptr<SHAMapTreeNodeT<Traits>> child =
            current_inner->get_child(branch);
        if (!child)
        {
            terminal_branch_ = branch;
            inners_.push_back(current_inner);
            break;
        }
        if (child->is_leaf())
        {
            terminal_branch_ = branch;
            inners_.push_back(current_inner);
            found_leaf_ =
                boost::static_pointer_cast<SHAMapLeafNodeT<Traits>>(child);
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

        inners_.push_back(current_inner);
        branches_.push_back(branch);

        if (child->is_inner() &&
            options_.tree_collapse_impl == TreeCollapseImpl::leafs_and_inners)
        {
            auto inner_child =
                boost::static_pointer_cast<SHAMapInnerNodeT<Traits>>(child);
            if (inner_child->get_depth() > current_inner->get_depth() + 1)
            {
                auto [belongs, divergence_depth] = key_belongs_in_inner<Traits>(
                    inner_child, target_key_, current_inner->get_depth());
                if (divergence_depth != inner_child->get_depth())
                {
                    if (!belongs)
                    {
                        OLOGD(
                            "Found divergence at depth ",
                            divergence_depth,
                            " current inner depth: ",
                            current_inner->get_depth_int(),
                            " inner child depth: ",
                            inner_child->get_depth_int());
                        divergence_depth_ = divergence_depth;
                        // We need to save this to relink it later
                        diverged_inner_ = inner_child;
                        break;
                    }
                }
            }
        }
        current_inner =
            boost::static_pointer_cast<SHAMapInnerNodeT<Traits>>(child);
    }
}

template <typename Traits>
bool
PathFinderT<Traits>::has_leaf() const
{
    return found_leaf_ != nullptr;
}

template <typename Traits>
bool
PathFinderT<Traits>::did_leaf_key_match() const
{
    return leaf_key_matches_;
}

template <typename Traits>
bool
PathFinderT<Traits>::ended_at_null_branch() const
{
    return found_leaf_ == nullptr && terminal_branch_ != -1;
}

template <typename Traits>
boost::intrusive_ptr<const SHAMapLeafNodeT<Traits>>
PathFinderT<Traits>::get_leaf() const
{
    return found_leaf_;
}

template <typename Traits>
boost::intrusive_ptr<SHAMapLeafNodeT<Traits>>
PathFinderT<Traits>::get_leaf_mutable()
{
    return found_leaf_;
}

template <typename Traits>
boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
PathFinderT<Traits>::get_parent_of_terminal()
{
    return inners_.empty() ? nullptr : inners_.back();
}

template <typename Traits>
boost::intrusive_ptr<const SHAMapInnerNodeT<Traits>>
PathFinderT<Traits>::get_parent_of_terminal() const
{
    return inners_.empty() ? nullptr : inners_.back();
}

template <typename Traits>
int
PathFinderT<Traits>::get_terminal_branch() const
{
    return terminal_branch_;
}

template <typename Traits>
void
PathFinderT<Traits>::dirty_path() const
{
    for (auto& inner : inners_)
    {
        inner->invalidate_hash();
    }
}

template <typename Traits>
bool
PathFinderT<Traits>::collapse_path_single_leaf_child()
{
    if (inners_.size() <= 1)
        return true;
    boost::intrusive_ptr<SHAMapLeafNodeT<Traits>> only_child = nullptr;
    auto innermost = inners_.back();
    only_child = innermost->get_only_child_leaf();
    for (int i = static_cast<int>(inners_.size()) - 2; i >= 0; --i)
    {
        auto inner = inners_[i];
        int branch = branches_[i];
        if (only_child)
        {
            inner->set_child(branch, only_child);
        }
        only_child = inner->get_only_child_leaf();
        if (!only_child)
            break;
    }
    return false;
}

template <typename Traits>
void
PathFinderT<Traits>::collapse_path()
{
    collapse_path_single_leaf_child();
}

template <typename Traits>
boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
PathFinderT<Traits>::dirty_or_copy_inners(int target_version)
{
    if (inners_.empty())
    {
        LOGW("No inner nodes in path to apply CoW");
        return nullptr;
    }

    // Start from the root and work downward
    for (size_t i = 0; i < inners_.size(); ++i)
    {
        auto& current_inner = inners_[i];

        // Skip if already at target version
        if (current_inner->get_version() == target_version)
        {
            LOGD(
                "Node at index ",
                i,
                " already at target version ",
                target_version);
            continue;
        }

        // Handle nodes that don't have CoW enabled
        if (!current_inner->is_cow_enabled())
        {
            // CRITICAL FIX: We must copy nodes that were created without CoW
            // to avoid shared state corruption between snapshots
            LOGD(
                "Node at index ",
                i,
                " has CoW disabled, creating copy from version ",
                current_inner->get_version(),
                " to version ",
                target_version);

            // Create a copy and enable CoW on it
            auto copy = current_inner->copy(
                target_version, i > 0 ? inners_[i - 1].get() : nullptr);
            copy->enable_cow(true);

            // If this is the root, update the search root
            if (i == 0)
            {
                search_root_ = copy;
            }

            // If not the root, update parent's child pointer to point to this
            // copy
            if (i > 0)
            {
                const auto& parent = inners_[i - 1];
                int branch = branches_[i - 1];
                LOGD(
                    "Updating parent at depth ",
                    parent->get_depth(),
                    " branch ",
                    branch,
                    " to point to new copy (non-CoW node)");
                parent->set_child(branch, copy);
            }

            // Replace in our path vector
            inners_[i] = copy;
            continue;
        }

        // Need to create a copy (CoW)
        LOGD(
            "Creating CoW copy of node at index ",
            i,
            " version ",
            current_inner->get_version(),
            " to version ",
            target_version);

        // Create copy with new version
        auto copy = current_inner->copy(
            target_version, i > 0 ? inners_[i - 1].get() : nullptr);

        // If this is the root, update the search root
        if (i == 0)
        {
            search_root_ = copy;
        }

        // If not the root, update parent's child pointer to point to this copy
        if (i > 0)
        {
            const auto& parent = inners_[i - 1];
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
        inners_[i] = copy;
    }

    // Return the innermost node for further operations
    return inners_.back();
}

template <typename Traits>
boost::intrusive_ptr<SHAMapLeafNodeT<Traits>>
PathFinderT<Traits>::invalidated_possibly_copied_leaf_for_updating(
    int targetVersion)
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

    boost::intrusive_ptr<SHAMapLeafNodeT<Traits>> theLeaf = found_leaf_;

    // Check if we need to copy the leaf
    if (found_leaf_->get_version() != targetVersion)
    {
        theLeaf = found_leaf_->copy(targetVersion, terminal.get());
        terminal->set_child(terminal_branch_, theLeaf);
        found_leaf_ = theLeaf;  // Update our reference
    }

    theLeaf->invalidate_hash();
    return theLeaf;
}

template <typename Traits>
void
PathFinderT<Traits>::add_node_at_divergence()
{
    if (divergence_depth_ == -1)
    {
        return;
    }

    auto divergence_depth = divergence_depth_;
    auto inner = inners_.back();
    if (divergence_depth == diverged_inner_->get_depth())
    {
        throw std::runtime_error("Cannot add node at divergence depth");
        return;
    }

    auto new_inner = inner->make_child(divergence_depth);

    // Get the common branch at the parent level
    int common_branch = inner->select_branch_for_depth(target_key_);

    // Get a representative key from the existing subtree
    auto existing_key =
        diverged_inner_->first_leaf(diverged_inner_)->get_item()->key();

    // Determine branch for each key at the divergence depth
    int existing_branch = select_branch(existing_key, divergence_depth);
    int new_branch = select_branch(target_key_, divergence_depth);

    // Place existing subtree under its branch
    new_inner->set_child(existing_branch, diverged_inner_);

    // Link new inner node to parent
    inner->set_child(common_branch, new_inner);

    // Set the terminal branch for the new item
    terminal_branch_ = new_branch;
    branches_.push_back(new_branch);
    inners_.push_back(new_inner);
}

// Explicit template instantiations for default traits
template class PathFinderT<DefaultNodeTraits>;

}  // namespace catl::shamap