#include "catl/core/logger.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>

void
SHAMap::collapse_tree()
{
    if (root)
    {
        collapse_inner_node(root);
    }
}

void
SHAMap::collapse_inner_node(boost::intrusive_ptr<SHAMapInnerNode>& node)
{
    if (!node)
    {
        return;
    }

    // Never collapse the root node (depth 0)
    bool is_root = (node->get_depth() == 0);

    // Check if we need to make a CoW copy before modification
    if (cow_enabled_ && node->is_cow_enabled() &&
        node->get_version() != current_version_)
    {
        // Create a copy with the current version
        auto node_copy = node->copy(current_version_);
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
                auto innerChild =
                    boost::static_pointer_cast<SHAMapInnerNode>(child);

                // Recursively process the child
                collapse_inner_node(innerChild);

                // The child reference might have been updated during recursion
                // so we always update the parent's reference to the child
                node->set_child(i, innerChild);
            }
        }
    }

    // Skip further processing for root node to preserve its depth
    if (is_root)
    {
        return;
    }

    // After processing all children, look for a single inner child
    if (boost::intrusive_ptr<SHAMapInnerNode> singleInnerChild =
            find_only_single_inner_child(node))
    {
        OLOGD(
            "Collapsing node at depth ",
            static_cast<int>(node->get_depth()),
            " with single inner child at depth ",
            static_cast<int>(singleInnerChild->get_depth()));

        // If using CoW support, handle versioning before modifications
        if (cow_enabled_)
        {
            // If the single inner child needs to be copied for versioning
            if (singleInnerChild->is_cow_enabled() &&
                singleInnerChild->get_version() != current_version_)
            {
                auto childCopy = singleInnerChild->copy(current_version_);
                // Update our reference to this child
                singleInnerChild = childCopy;
                // Note: We don't need to rechain this reference since we're
                // going to collapse its children into 'node' anyway
            }

            // Set version before structural modifications
            node->set_version(current_version_);
            node->enable_cow(true);
        }

        // Copy all branches from the child to this node
        for (int i = 0; i < 16; i++)
        {
            // Remove existing branch
            node->set_child(i, nullptr);

            // Set branch from child if it exists
            if (singleInnerChild->has_child(i))
            {
                auto child = singleInnerChild->get_child(i);

                // If using CoW, ensure child versions are compatible
                if (cow_enabled_ && child)
                {
                    if (child->is_inner())
                    {
                        auto innerChild =
                            boost::static_pointer_cast<SHAMapInnerNode>(child);
                        if (innerChild->is_cow_enabled() &&
                            innerChild->get_version() != current_version_)
                        {
                            // Create a version-compatible copy
                            auto childCopy = innerChild->copy(current_version_);
                            // Update this node to point to the new copy
                            node->set_child(i, childCopy);
                            // No additional rechaining needed since we just
                            // updated the parent->child link
                            continue;
                        }
                    }
                    else if (child->is_leaf())
                    {
                        auto leafChild =
                            boost::static_pointer_cast<SHAMapLeafNode>(child);
                        if (leafChild->get_version() != current_version_)
                        {
                            // Copy leaf nodes too if needed
                            auto leafCopy = leafChild->copy();
                            leafCopy->set_version(current_version_);
                            node->set_child(i, leafCopy);
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
        node->set_depth(singleInnerChild->get_depth());

        // Invalidate the hash since we've changed the structure
        node->invalidate_hash();
    }
}

boost::intrusive_ptr<SHAMapInnerNode>
SHAMap::find_only_single_inner_child(
    boost::intrusive_ptr<SHAMapInnerNode>& node)
{
    boost::intrusive_ptr<SHAMapInnerNode> singleInnerChild = nullptr;
    int innerCount = 0;

    for (int i = 0; i < 16; i++)
    {
        if (node->has_child(i))
        {
            if (auto child = node->get_child(i))
            {
                if (child->is_inner())
                {
                    innerCount++;
                    if (innerCount == 1)
                    {
                        singleInnerChild =
                            boost::static_pointer_cast<SHAMapInnerNode>(child);
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

    return singleInnerChild;
}
