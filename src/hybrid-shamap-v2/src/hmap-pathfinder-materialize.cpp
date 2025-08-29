#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-leafnode.h"
#include "catl/hybrid-shamap-v2/hmap-pathfinder.h"
#include "catl/v2/catl-v2-memtree.h"

namespace catl::hybrid_shamap {

void
HmapPathFinder::materialize_path()
{
    for (size_t i = 0; i < path_.size(); ++i)
    {
        auto& [node_ptr, branch_taken] = path_[i];

        if (node_ptr.is_raw_memory())
        {
            // Need to materialize this node
            const uint8_t* raw = node_ptr.get_raw_memory();

            // Determine node type from parent's child_types or context
            bool is_leaf = false;
            if (i > 0)
            {
                // Get type from parent's child_types
                auto& [parent_ptr, _] = path_[i - 1];
                if (parent_ptr.is_materialized())
                {
                    auto* parent_inner =
                        parent_ptr.get_materialized<HmapInnerNode>();
                    is_leaf =
                        (parent_inner->get_child_type(branch_taken) ==
                         v2::ChildType::LEAF);
                }
                else
                {
                    // Parent is still raw, check its header
                    auto header = parent_ptr.get_memptr<v2::InnerNodeHeader>();
                    const auto& header_val = *header;
                    is_leaf =
                        (header_val.get_child_type(branch_taken) ==
                         v2::ChildType::LEAF);
                }
            }
            else if (i == path_.size() - 1 && found_leaf_)
            {
                // Last node in path and we found a leaf
                is_leaf = true;
            }

            auto materialized = materialize_raw_node(raw, is_leaf);

            // Update this entry in the path using proper ref counting
            node_ptr = PolyNodePtr::from_intrusive(materialized);

            // Update parent's child pointer if not root
            if (i > 0)
            {
                auto& [parent_ptr, _] = path_[i - 1];
                assert(parent_ptr.is_materialized());
                auto* parent_inner =
                    parent_ptr.get_materialized<HmapInnerNode>();
                // Determine child type based on whether it's a leaf
                v2::ChildType child_type =
                    is_leaf ? v2::ChildType::LEAF : v2::ChildType::INNER;
                parent_inner->set_child(branch_taken, node_ptr, child_type);
            }
        }
    }
}

boost::intrusive_ptr<HMapNode>
HmapPathFinder::materialize_raw_node(const uint8_t* raw, bool is_leaf)
{
    if (is_leaf)
    {
        // Materialize as leaf
        v2::MemPtr<v2::LeafHeader> leaf_header_ptr(raw);
        const auto& header = *leaf_header_ptr;

        Key key(header.key.data());
        Slice data(raw + sizeof(v2::LeafHeader), header.data_size());

        return {new HmapLeafNode(key, data)};
    }
    else
    {
        // Materialize as inner
        v2::MemPtr<v2::InnerNodeHeader> inner_ptr(raw);
        const auto& header = *inner_ptr;

        auto* inner = new HmapInnerNode(header.get_depth());
        boost::intrusive_ptr<HMapNode> inner_ptr_managed(inner);

        // Copy children (both types and pointers) from mmap header
        const v2::InnerNodeView view{inner_ptr};
        const auto offsets = view.get_sparse_offsets();

        for (int i = 0; i < 16; ++i)
        {
            v2::ChildType child_type = header.get_child_type(i);
            if (child_type != v2::ChildType::EMPTY)
            {
                const uint8_t* child_raw = offsets.get_child_ptr(i);
                inner->set_child(
                    i,
                    PolyNodePtr::make_raw_memory(child_raw, child_type),
                    child_type);
            }
        }

        return inner_ptr_managed;
    }
}

}  // namespace catl::hybrid_shamap