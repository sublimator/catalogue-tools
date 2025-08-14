#include "catl/core/logger.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>

namespace catl::shamap {
template <typename Traits>
void
SHAMapT<Traits>::collapse_tree()
{
    if (root)
    {
        collapse_inner_node(root);
    }
}

template <typename Traits>
void
SHAMapT<Traits>::collapse_inner_node(
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& node)
{
    if (!node)
    {
        return;
    }

    // Never collapse the root node (depth 0)
    bool is_root = (node->get_depth() == 0);

    // Check if we need to make a CoW copy before modification
    if (this->cow_enabled_ && node->is_cow_enabled() &&
        node->get_version() != this->current_version_)
    {
        // Create a copy with the current version
        auto node_copy = node->copy(this->current_version_, nullptr);
        // Important: update the reference passed to this function
        node = node_copy;
        // No need to rechain as the caller will update its reference to node
    }

    // First, recursively process all child nodes
    for (int i = 0; i < 16; i++)
    {
        if (node->has_child(i))
        {
            auto child = node->get_child(i);
            if (child && child->is_inner())
            {
                auto inner_child =
                    boost::static_pointer_cast<SHAMapInnerNodeT<Traits>>(child);

                // Recursively process the child
                collapse_inner_node(inner_child);

                // The child reference might have been updated during recursion
                // so we always update the parent's reference to the child
                node->set_child(i, inner_child);
            }
        }
    }

    // Skip further processing for root node to preserve its depth
    if (is_root)
    {
        return;
    }

    // After processing all children, look for a single inner child
    if (boost::intrusive_ptr<SHAMapInnerNodeT<Traits>> single_inner_child =
            find_only_single_inner_child(node))
    {
        OLOGD(
            "Collapsing node at depth ",
            static_cast<int>(node->get_depth()),
            " with single inner child at depth ",
            static_cast<int>(single_inner_child->get_depth()));

        // If using CoW support, handle versioning before modifications
        if (this->cow_enabled_)
        {
            // If the single inner child needs to be copied for versioning
            if (single_inner_child->is_cow_enabled() &&
                single_inner_child->get_version() != this->current_version_)
            {
                auto child_copy = single_inner_child->copy(
                    this->current_version_, node.get());
                // Update our reference to this child
                single_inner_child = child_copy;
                // Note: We don't need to rechain this reference since we're
                // going to collapse its children into 'node' anyway
            }

            // Set version before structural modifications
            node->set_version(this->current_version_);
            node->enable_cow(true);
        }

        // Copy all branches from the child to this node
        for (int i = 0; i < 16; i++)
        {
            // Remove existing branch
            node->set_child(i, nullptr);

            // Set branch from child if it exists
            if (single_inner_child->has_child(i))
            {
                auto child = single_inner_child->get_child(i);

                // If using CoW, ensure child versions are compatible
                if (this->cow_enabled_ && child)
                {
                    if (child->is_inner())
                    {
                        auto inner_child = boost::static_pointer_cast<
                            SHAMapInnerNodeT<Traits>>(child);
                        if (inner_child->is_cow_enabled() &&
                            inner_child->get_version() !=
                                this->current_version_)
                        {
                            // Create a version-compatible copy
                            auto child_copy = inner_child->copy(
                                this->current_version_, node.get());
                            // Update this node to point to the new copy
                            node->set_child(i, child_copy);
                            // No additional rechaining needed since we just
                            // updated the parent->child link
                            continue;
                        }
                    }
                    else if (child->is_leaf())
                    {
                        auto leaf_child =
                            boost::static_pointer_cast<SHAMapLeafNodeT<Traits>>(
                                child);
                        if (leaf_child->get_version() != this->current_version_)
                        {
                            // Copy leaf nodes too if needed
                            auto leaf_copy = leaf_child->copy(
                                this->current_version_, node.get());
                            node->set_child(i, leaf_copy);
                            continue;
                        }
                    }
                }

                // If we get here, either CoW isn't enabled or the child version
                // is compatible
                node->set_child(i, child);
            }
        }

        // Update depth using direct access since no setter is visible
        node->set_depth(single_inner_child->get_depth());

        // Invalidate the hash since we've changed the structure
        node->invalidate_hash();
    }
}

template <typename Traits>
boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
SHAMapT<Traits>::find_only_single_inner_child(
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& node)
{
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>> single_inner_child = nullptr;
    int inner_count = 0;

    for (int i = 0; i < 16; i++)
    {
        if (node->has_child(i))
        {
            if (auto child = node->get_child(i))
            {
                if (child->is_inner())
                {
                    inner_count++;
                    if (inner_count == 1)
                    {
                        single_inner_child = boost::static_pointer_cast<
                            SHAMapInnerNodeT<Traits>>(child);
                    }
                    else
                    {
                        return nullptr;  // More than one inner child, don't
                        // collapse
                    }
                }
                else if (child->is_leaf())
                {
                    return nullptr;  // Contains a leaf node, don't collapse
                }
            }
        }
    }

    return single_inner_child;
}
// Explicit template instantiation for default traits
template void
SHAMapT<DefaultNodeTraits>::collapse_tree();
template void
SHAMapT<DefaultNodeTraits>::collapse_inner_node(
    boost::intrusive_ptr<SHAMapInnerNodeT<DefaultNodeTraits>>& node);
template boost::intrusive_ptr<SHAMapInnerNodeT<DefaultNodeTraits>>
SHAMapT<DefaultNodeTraits>::find_only_single_inner_child(
    boost::intrusive_ptr<SHAMapInnerNodeT<DefaultNodeTraits>>& node);

}  // namespace catl::shamap