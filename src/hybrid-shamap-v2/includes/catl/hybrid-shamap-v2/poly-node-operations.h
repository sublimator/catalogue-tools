#pragma once

#include "catl/core/types.h"
#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-leafnode.h"
#include "catl/hybrid-shamap-v2/poly-node-ptr.h"
#include "catl/v2/catl-v2-memtree.h"
#include <stdexcept>

namespace catl::hybrid_shamap {

/**
 * Free functions for operating on PolyNodePtr
 * These keep the PolyNodePtr interface clean and focused on being a smart
 * pointer
 */

/**
 * Get the key from a leaf node
 * @param node The node to get the key from
 * @return The leaf's key
 * @throws std::runtime_error if this is not a leaf node
 */
inline Key
poly_get_leaf_key(const PolyNodePtr& node)
{
    if (!node.is_leaf())
    {
        throw std::runtime_error("poly_get_leaf_key() called on non-leaf node");
    }

    if (node.is_materialized())
    {
        auto* leaf = node.get_materialized<HmapLeafNode>();
        return leaf->get_key();
    }
    else
    {
        auto header = node.get_memptr<v2::LeafHeader>();
        return Key(header->key.data());
    }
}

/**
 * Get the first leaf from a PolyNodePtr subtree
 * @param node The node to search from
 * @return PolyNodePtr to the first leaf
 * @throws std::runtime_error if no leaf found
 */
inline PolyNodePtr
poly_first_leaf(const PolyNodePtr& node)
{
    if (node.is_empty())
        throw std::runtime_error("Cannot get first leaf from empty node");

    if (node.is_leaf())
        return node;

    if (node.is_inner())
    {
        if (node.is_materialized())
        {
            auto* inner = node.get_materialized<HmapInnerNode>();
            return inner->first_leaf();
        }
        else
        {
            v2::InnerNodeView view =
                v2::MemTreeOps::get_inner_node(node.get_raw_memory());
            auto leaf_view = v2::MemTreeOps::first_leaf_depth_first(view);
            return PolyNodePtr::make_raw_memory(
                leaf_view.header_ptr.raw(), v2::ChildType::LEAF);
        }
    }

    throw std::runtime_error("Cannot get first leaf from placeholder node");
}

/**
 * Get the first leaf key from a PolyNodePtr subtree
 * @param node The node to search from
 * @return Key of the first leaf
 * @throws std::runtime_error if no leaf found
 */
inline Key
poly_first_leaf_key(const PolyNodePtr& node)
{
    return poly_get_leaf_key(poly_first_leaf(node));
}

}  // namespace catl::hybrid_shamap