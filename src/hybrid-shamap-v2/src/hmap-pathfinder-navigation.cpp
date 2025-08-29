#include "catl/core/logger.h"
#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-pathfinder.h"
#include "catl/hybrid-shamap-v2/poly-node-operations.h"
#include "catl/shamap/shamap-utils.h"
#include "catl/v2/catl-v2-memtree.h"
#include <cstring>

namespace catl::hybrid_shamap {

bool
HmapPathFinder::navigate_raw_inner(PolyNodePtr& current, int& depth)
{
    const uint8_t* raw = current.get_raw_memory();

    // Debug: validate the pointer
    if (!raw || reinterpret_cast<uintptr_t>(raw) < 0x1000)
    {
        LOGE("Invalid raw pointer in navigate_raw_inner: ", raw);
        throw std::runtime_error("Invalid raw pointer in tree navigation");
    }

    // Check for suspicious addresses (like our crash address)
    if (reinterpret_cast<uintptr_t>(raw) > 0x700000000000)
    {
        LOGE("Suspicious raw pointer (too large): ", raw);
        LOGE("  This indicates corrupt node data or wrong node type");
        throw std::runtime_error("Corrupt raw pointer detected");
    }

    v2::InnerNodeView view{v2::MemPtr<v2::InnerNodeHeader>(raw)};

    const auto& header = *view.header_ptr;
    depth = header.get_depth();

    // Validate depth
    if (depth < 0 || depth >= 64)
    {
        LOGE("Invalid depth in inner node: ", depth);
        LOGE("  Raw pointer: ", raw);
        LOGE("  This likely means we're reading wrong data type as inner node");
        throw std::runtime_error("Invalid depth in inner node");
    }

    int branch = shamap::select_branch(target_key_, depth);
    auto child_type = header.get_child_type(branch);

    if (child_type == v2::ChildType::EMPTY)
    {
        // Empty branch
        terminal_branch_ = branch;
        return false;
    }

    const uint8_t* child_ptr = view.get_child_ptr(branch);

    // Validate child pointer
    if (!child_ptr || reinterpret_cast<uintptr_t>(child_ptr) < 0x1000)
    {
        LOGE("Invalid child pointer from sparse offsets");
        LOGE("  Branch: ", branch, " Type: ", static_cast<int>(child_type));
        throw std::runtime_error("Invalid child pointer");
    }

    if (reinterpret_cast<uintptr_t>(child_ptr) > 0x700000000000)
    {
        LOGE("Suspicious child pointer (too large): ", child_ptr);
        LOGE("  Parent depth: ", depth, " branch: ", branch);
        throw std::runtime_error("Corrupt child pointer detected");
    }

    PolyNodePtr child = PolyNodePtr::wrap_raw_memory(child_ptr, child_type);

    if (child_type == v2::ChildType::LEAF)
    {
        // It's a leaf - check the key
        const v2::MemPtr<v2::LeafHeader> leaf_header_ptr(child_ptr);
        const auto& leaf_header = *leaf_header_ptr;

        found_leaf_ = child;
        key_matches_ =
            (std::memcmp(leaf_header.key.data(), target_key_.data(), 32) == 0);

        path_.emplace_back(child, branch);
        terminal_branch_ = branch;
        return false;  // Stop here
    }
    else
    {
        // It's another inner node - check for collapse
        v2::InnerNodeView child_view{
            v2::MemPtr<v2::InnerNodeHeader>(child_ptr)};
        int child_depth = child_view.header_ptr->get_depth();
        int expected_depth = depth + 1;

        if (child_depth > expected_depth)
        {
            // Collapsed tree - check if key belongs
            auto [belongs, div_depth] =
                key_belongs_in_inner(child, target_key_, depth);

            if (!belongs)
            {
                LOGD(
                    "Found divergence in raw inner at depth ",
                    div_depth,
                    " current depth: ",
                    depth,
                    " child depth: ",
                    child_depth);
                divergence_depth_ = div_depth;
                diverged_inner_ = child;
                terminal_branch_ = branch;
                return false;
            }
        }

        path_.emplace_back(child, branch);
        current = child;
        depth++;
        return true;  // Continue
    }
}

std::pair<bool, int>
HmapPathFinder::key_belongs_in_inner(
    const PolyNodePtr& inner,
    const Key& key,
    int start_depth)
{
    // Get the actual depth of the inner node
    int end_depth = 0;
    if (inner.is_materialized())
    {
        auto* inner_node = inner.get_materialized<HmapInnerNode>();
        end_depth = inner_node->get_depth();
    }
    else
    {
        auto header = inner.get_memptr<v2::InnerNodeHeader>();
        end_depth = header->get_depth();
    }

    // Get representative key from the inner's first leaf
    Key rep_key = poly_first_leaf_key(inner);

    // Check each depth level to see if keys diverge
    for (int depth = start_depth; depth <= end_depth; depth++)
    {
        int branch = shamap::select_branch(key, depth);
        if (branch != shamap::select_branch(rep_key, depth))
        {
            return {false, depth};  // Keys diverge at this depth
        }
    }

    return {true, -1};  // Key belongs in this inner node
}

void
HmapPathFinder::add_node_at_divergence()
{
    if (divergence_depth_ == -1 || !diverged_inner_)
    {
        return;  // No divergence to handle
    }

    if (path_.empty())
    {
        throw std::runtime_error(
            "Cannot add node at divergence with empty path");
    }

    // Get the parent inner node (last inner in path)
    PolyNodePtr parent_inner;
    int parent_branch = -1;

    for (auto it = path_.rbegin(); it != path_.rend(); ++it)
    {
        if (it->first.is_inner())
        {
            parent_inner = it->first;
            if (it != path_.rbegin())
            {
                auto prev = it;
                --prev;
                parent_branch = prev->second;
            }
            break;
        }
    }

    if (!parent_inner || !parent_inner.is_materialized())
    {
        throw std::runtime_error(
            "Parent must be materialized to add divergence node");
    }

    auto* parent = parent_inner.get_materialized<HmapInnerNode>();

    // Create new inner node at divergence depth
    auto* divergence_node = new HmapInnerNode(divergence_depth_);

    // Get the branch where target key goes at divergence depth
    int new_branch = shamap::select_branch(target_key_, divergence_depth_);

    // Get representative key from existing subtree
    Key existing_key = poly_first_leaf_key(diverged_inner_);
    int existing_branch =
        shamap::select_branch(existing_key, divergence_depth_);

    // Place existing subtree under its branch
    divergence_node->set_child(
        existing_branch, diverged_inner_, diverged_inner_.get_type());

    // Link new inner node to parent
    parent->set_child(
        parent_branch,
        PolyNodePtr::adopt_materialized(divergence_node),
        v2::ChildType::INNER);

    // Update path with new inner node
    path_.emplace_back(
        PolyNodePtr::adopt_materialized(divergence_node), parent_branch);

    // Set terminal branch for insertion
    terminal_branch_ = new_branch;
}

}  // namespace catl::hybrid_shamap