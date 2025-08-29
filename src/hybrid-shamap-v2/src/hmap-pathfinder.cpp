#include "catl/hybrid-shamap-v2/hmap-pathfinder.h"
#include "catl/core/logger.h"
#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-leafnode.h"
#include "catl/hybrid-shamap-v2/hmap-node.h"
#include "catl/hybrid-shamap-v2/poly-node-operations.h"
#include "catl/shamap/shamap-utils.h"
#include "catl/v2/catl-v2-memtree.h"
#include <cstring>
#include <sstream>

namespace catl::hybrid_shamap {

void
HmapPathFinder::find_path(const PolyNodePtr& root)
{
    path_.clear();
    found_leaf_ = PolyNodePtr::make_empty();
    key_matches_ = false;

    LOGD("[HmapPathFinder] Starting path finding for key: ", target_key_.hex());
    LOGD(
        "  Root is raw: ",
        root.is_raw_memory(),
        " materialized: ",
        root.is_materialized());

    // Start at root
    path_.emplace_back(root, -1);

    PolyNodePtr current = root;
    int depth = 0;
    int iteration = 0;

    while (current)
    {
        if (++iteration > 64)
        {
            LOGE("Path finding exceeded maximum depth!");
            throw std::runtime_error("Path finding exceeded maximum depth");
        }

        LOGD("  Iteration ", iteration, " at depth ", depth);

        if (current.is_raw_memory())
        {
            LOGD("    Navigating raw memory node...");
            // Navigate through raw memory node
            if (!navigate_raw_inner(current, depth))
            {
                break;  // Hit leaf or empty
            }
        }
        else
        {
            // Navigate through materialized node
            assert(current.is_materialized());
            HMapNode* node = current.get_materialized_base();

            if (node->get_type() == v2::ChildType::LEAF)
            {
                // Found a leaf
                const auto* leaf = current.get_materialized<HmapLeafNode>();
                found_leaf_ = current;
                key_matches_ = (leaf->get_key() == target_key_);
                break;
            }
            else if (node->get_type() == v2::ChildType::PLACEHOLDER)
            {
                // Can't navigate through placeholder yet
                // Would need to fetch the actual node
                throw std::runtime_error(
                    "Cannot navigate through placeholder nodes yet");
            }
            else
            {
                // It's an inner node
                const auto* inner = current.get_materialized<HmapInnerNode>();
                depth = inner->get_depth();

                // Check for collapsed tree (divergence detection)
                int expected_depth = depth + 1;

                int branch = shamap::select_branch(target_key_, depth);
                PolyNodePtr child = inner->get_child(branch);

                if (!child)
                {
                    // Empty branch
                    terminal_branch_ = branch;
                    break;
                }

                // Check if child is a collapsed inner node
                if (child.is_inner() && child.is_materialized())
                {
                    auto* inner_child = child.get_materialized<HmapInnerNode>();
                    int child_depth = inner_child->get_depth();

                    if (child_depth > expected_depth)
                    {
                        // We have a collapsed section - check if key belongs
                        auto [belongs, div_depth] =
                            key_belongs_in_inner(child, target_key_, depth);

                        if (!belongs)
                        {
                            LOGD(
                                "Found divergence at depth ",
                                div_depth,
                                " current inner depth: ",
                                depth,
                                " inner child depth: ",
                                child_depth);
                            divergence_depth_ = div_depth;
                            diverged_inner_ = child;
                            terminal_branch_ = branch;
                            break;
                        }
                    }
                }

                // Check if we've reached a leaf
                if (child.is_leaf())
                {
                    // Found a leaf - check if it matches our key
                    if (child.is_materialized())
                    {
                        auto* leaf = child.get_materialized<HmapLeafNode>();
                        found_leaf_ = child;
                        key_matches_ = (leaf->get_key() == target_key_);
                    }
                    else
                    {
                        // Raw memory leaf
                        found_leaf_ = child;
                        key_matches_ =
                            (std::memcmp(
                                 child.get_memptr<v2::LeafHeader>()->key.data(),
                                 target_key_.data(),
                                 32) == 0);
                    }
                    path_.emplace_back(child, branch);
                    terminal_branch_ = branch;
                    break;  // Stop navigation at leaf
                }

                // It's an inner node, continue navigation
                path_.emplace_back(child, branch);
                current = child;
                depth++;  // Move to next level
            }
        }
    }
}

void
HmapPathFinder::debug_path() const
{
    LOGD("Path to key ", target_key_.hex());
    for (size_t i = 0; i < path_.size(); ++i)
    {
        const auto& [node_ptr, branch] = path_[i];
        std::stringstream ss;
        ss << "  [" << i << "] ";
        if (branch >= 0)
        {
            ss << "branch " << branch << " -> ";
        }

        // Print node info and hash
        if (node_ptr.is_raw_memory())
        {
            ss << "RAW_MEMORY @ " << node_ptr.get_raw_ptr();

            // Print hash from mmap header
            const uint8_t* raw = node_ptr.get_raw_memory();
            if (node_ptr.is_inner())
            {
                v2::MemPtr<v2::InnerNodeHeader> header(raw);
                const auto& h = *header;
                ss << " depth=" << (int)h.get_depth();
                ss << " hash=";
                for (int j = 0; j < 8; ++j)
                {  // First 8 bytes
                    char buf[3];
                    sprintf(buf, "%02x", h.hash[j]);
                    ss << buf;
                }
                ss << "...";
            }
            else if (node_ptr.is_leaf())
            {
                v2::MemPtr<v2::LeafHeader> header(raw);
                const auto& h = *header;
                ss << " hash=";
                for (int j = 0; j < 8; ++j)
                {  // First 8 bytes
                    char buf[3];
                    sprintf(buf, "%02x", h.hash[j]);
                    ss << buf;
                }
                ss << "...";
            }
        }
        else
        {
            auto* node = node_ptr.get_materialized_base();
            ss << "MATERIALIZED " << node->describe();

            // Print hash if valid
            if (node->get_type() == v2::ChildType::INNER)
            {
                ss << " hash=" << node->get_hash().hex().substr(0, 16) << "...";
            }
            else if (node->get_type() == v2::ChildType::LEAF)
            {
                ss << " hash=" << node->get_hash().hex().substr(0, 16) << "...";
            }
        }
        LOGD(ss.str());
    }
    if (found_leaf_)
    {
        LOGD(
            "  Found leaf, key ",
            (key_matches_ ? "MATCHES" : "does NOT match"));
    }
    else
    {
        LOGD("  No leaf found");
    }
}

}  // namespace catl::hybrid_shamap