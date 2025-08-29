#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-node.h"
#include "catl/hybrid-shamap-v2/poly-node-operations.h"
#include "catl/v2/catl-v2-memtree.h"

namespace catl::hybrid_shamap {

HmapInnerNode::~HmapInnerNode()
{
    // Release references to all materialized children
    for (int i = 0; i < 16; ++i)
    {
        if (is_child_materialized(i) && children_[i] != nullptr)
        {
            // Decrement reference count
            auto* node = static_cast<HMapNode*>(children_[i]);
            intrusive_ptr_release(node);
        }
    }
}

void
HmapInnerNode::set_child(int branch, const PolyNodePtr& ref, v2::ChildType type)
{
    assert(branch >= 0 && branch < 16);

    // Release old child if it was materialized
    if (is_child_materialized(branch) && children_[branch] != nullptr)
    {
        auto* old_node = static_cast<HMapNode*>(children_[branch]);
        intrusive_ptr_release(old_node);
    }

    // Extract raw pointer and store it
    children_[branch] = ref.get_raw_ptr();

    // Add reference to new child if it's materialized
    if (ref.is_materialized() && ref.get_raw_ptr() != nullptr)
    {
        auto* new_node = static_cast<HMapNode*>(ref.get_raw_ptr());
        intrusive_ptr_add_ref(new_node);
    }

    // Update child type
    uint32_t type_mask = ~(0x3u << (branch * 2));
    child_types_ = (child_types_ & type_mask) |
        (static_cast<uint32_t>(type) << (branch * 2));

    // Update materialized bit
    if (ref.is_materialized())
    {
        materialized_mask_ |= (1u << branch);
    }
    else
    {
        materialized_mask_ &= ~(1u << branch);
    }

    invalidate_hash();  // Invalidate cached hash
}

int
HmapInnerNode::count_children() const
{
    int count = 0;
    for (const auto& child : children_)
    {
        if (child)
            count++;
    }
    return count;
}

PolyNodePtr
HmapInnerNode::first_leaf() const
{
    // Check each branch in order
    for (int i = 0; i < 16; ++i)
    {
        auto child = get_child(i);
        if (child.is_empty())
            continue;

        if (child.is_leaf())
        {
            return child;  // Found a leaf!
        }

        // It's an inner node
        if (child.is_materialized())
        {
            // Recurse into materialized inner node
            auto* inner_child = child.get_materialized<HmapInnerNode>();
            return inner_child->first_leaf();
        }
        else
        {
            // Use MemTreeOps for mmap inner node
            v2::InnerNodeView view =
                v2::MemTreeOps::get_inner_node(child.get_raw_memory());
            auto leaf_view = v2::MemTreeOps::first_leaf_depth_first(view);

            // Convert LeafView to PolyNodePtr
            // The leaf_view has the raw pointer in header_ptr
            return PolyNodePtr::wrap_raw_memory(
                leaf_view.header_ptr.raw(), v2::ChildType::LEAF);
        }
    }

    throw std::runtime_error("No leaf found in inner node - malformed tree");
}

Key
HmapInnerNode::first_leaf_key() const
{
    return poly_get_leaf_key(first_leaf());
}

}  // namespace catl::hybrid_shamap