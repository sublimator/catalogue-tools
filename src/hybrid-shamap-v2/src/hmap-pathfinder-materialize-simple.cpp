#include "catl/core/logger.h"
#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-leafnode.h"
#include "catl/hybrid-shamap-v2/hmap-pathfinder.h"
#include "catl/hybrid-shamap-v2/poly-node-operations.h"
#include "catl/shamap/shamap-utils.h"
#include "catl/v2/catl-v2-memtree.h"

namespace catl::hybrid_shamap {

PolyNodePtr
HmapPathFinder::materialize_path_for_key(
    const PolyNodePtr& root,
    const Key& key,
    int max_depth)
{
    if (!root || root.is_empty())
    {
        return root;  // Nothing to materialize
    }

    // If root needs materialization
    PolyNodePtr current = root;
    if (root.is_raw_memory())
    {
        // Materialize root
        const uint8_t* raw = root.get_raw_memory();
        bool is_leaf = root.is_leaf();

        LOGD(
            "[materialize_path_for_key] Materializing root for key ",
            key.hex().substr(0, 16),
            "...");

        current = PolyNodePtr::adopt_materialized(
            materialize_raw_node_unmanaged(raw, is_leaf));
    }

    // If root is a leaf or we've reached max depth, we're done
    if (current.is_leaf() || max_depth == 0)
    {
        return current;
    }

    // Navigate down the tree, materializing as we go
    PolyNodePtr result_root = current;
    int current_depth = 0;

    while (current.is_inner() && (max_depth == -1 || current_depth < max_depth))
    {
        auto* inner = current.get_materialized<HmapInnerNode>();
        int inner_depth = inner->get_depth();
        int branch = shamap::select_branch(key, inner_depth);

        LOGD(
            "[materialize_path_for_key] At depth ",
            inner_depth,
            " for key ",
            key.hex().substr(0, 16),
            "..., taking branch ",
            branch);

        PolyNodePtr child = inner->get_child(branch);
        if (!child || child.is_empty())
        {
            break;  // No child at this branch
        }

        // Check if child needs materialization
        if (child.is_raw_memory())
        {
            const uint8_t* raw = child.get_raw_memory();
            bool is_leaf = child.is_leaf();

            LOGD(
                "[materialize_path_for_key] Materializing child at branch ",
                branch,
                " (",
                is_leaf ? "LEAF" : "INNER",
                ") for key ",
                key.hex().substr(0, 16),
                "...");

            // Store the original hash from mmap before materializing
            Hash256 original_hash = child.get_hash();

            // Materialize the child
            PolyNodePtr materialized_child = PolyNodePtr::adopt_materialized(
                materialize_raw_node_unmanaged(raw, is_leaf));

            // Sanity check: the materialized node should produce the same hash
            Hash256 materialized_hash = materialized_child.get_hash();
            if (original_hash != materialized_hash)
            {
                LOGE(
                    "[materialize_path_for_key] HASH MISMATCH after "
                    "materialization!");
                LOGE("  Key: ", key.hex().substr(0, 16), "...");
                LOGE("  Branch: ", branch, " at depth ", inner_depth);
                LOGE("  Original (mmap) hash:      ", original_hash.hex());
                LOGE("  Materialized (heap) hash:  ", materialized_hash.hex());
            }

            // Update parent's pointer to the materialized child
            inner->set_child(branch, materialized_child, child.get_type());

            child = materialized_child;
        }

        // Check for collapsed section (depth skip)
        if (child.is_inner())
        {
            auto* child_inner = child.get_materialized<HmapInnerNode>();
            int child_depth = child_inner->get_depth();

            // If there's a depth skip, we have a collapsed section
            if (child_depth > inner_depth + 1)
            {
                LOGD(
                    "[materialize_path_for_key] Found collapsed section: "
                    "parent depth ",
                    inner_depth,
                    ", child depth ",
                    child_depth,
                    " (skipping ",
                    child_depth - inner_depth - 1,
                    " levels)");

                // In a collapsed section, we need to check if the key
                // follows the collapsed path or diverges
                Key rep_key = poly_first_leaf_key(child);

                // Check each intermediate depth
                for (int d = inner_depth + 1; d < child_depth; ++d)
                {
                    if (shamap::select_branch(key, d) !=
                        shamap::select_branch(rep_key, d))
                    {
                        // Key diverges from collapsed path
                        // In simple materialization, we just stop here
                        // We don't create new nodes or modify structure
                        LOGD(
                            "[materialize_path_for_key] Key ",
                            key.hex().substr(0, 16),
                            "... diverges at depth ",
                            d,
                            " in collapsed section (parent depth ",
                            inner->get_depth(),
                            ", child depth ",
                            child_depth,
                            "), stopping materialization");
                        return result_root;
                    }
                }
            }
        }

        // Move to the child
        current = child;
        current_depth++;

        // If we've reached a leaf, we're done
        if (current.is_leaf())
        {
            break;
        }
    }

    return result_root;
}

}  // namespace catl::hybrid_shamap