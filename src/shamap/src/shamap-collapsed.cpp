#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-utils.h"
#include "catl/shamap/shamap.h"

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
                collapse_inner_node(innerChild);
                // Update the child reference in case it changed
                node->set_child(i, innerChild);
            }
        }
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
        // throw std::runtime_error("Not implemented");

        // Copy all branches from the child to this node
        for (int i = 0; i < 16; i++)
        {
            // Remove existing branch
            node->set_child(i, nullptr);

            // Set branch from child if it exists
            if (singleInnerChild->has_child(i))
            {
                node->set_child(i, singleInnerChild->get_child(i));
            }
        }

        // Update depth and other properties
        node->depth_ = singleInnerChild->get_depth();

        // If using CoW support, copy relevant properties
        if (cow_enabled_)
        {
            node->set_version(singleInnerChild->get_version());
            node->enable_cow(singleInnerChild->is_cow_enabled());
        }

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
