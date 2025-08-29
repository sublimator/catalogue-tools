#pragma once

#include "catl/core/logger.h"
#include "catl/core/types.h"
#include "catl/hybrid-shamap-v2/hybrid-shamap-forwards.h"
#include "catl/hybrid-shamap-v2/poly-node-ptr.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <utility>
#include <vector>

namespace catl::hybrid_shamap {

/**
 * PathFinder for navigating hybrid SHAMap trees
 * Can traverse through both RAW_MEMORY (mmap) and MATERIALIZED (heap) nodes
 */
class HmapPathFinder
{
private:
    Key target_key_;

    // Path we've traversed: pair of (node_ptr, branch_taken_to_get_here)
    // First element has branch=-1 (root)
    std::vector<std::pair<PolyNodePtr, int>> path_;

    // Terminal node we found (if any)
    PolyNodePtr found_leaf_;
    bool key_matches_ = false;

    // For handling collapsed trees and divergence
    int divergence_depth_ = -1;
    PolyNodePtr diverged_inner_;
    int terminal_branch_ = -1;

public:
    explicit HmapPathFinder(const Key& key) : target_key_(key)
    {
    }

    /**
     * Find path to target key starting from root
     * Root can be either RAW_MEMORY or MATERIALIZED
     */
    void
    find_path(const PolyNodePtr& root);

    /**
     * Materialize the path for modification
     * Converts RAW_MEMORY nodes to MATERIALIZED along the path
     */
    void
    materialize_path();

    // Getters
    [[nodiscard]] bool
    found_leaf() const
    {
        return static_cast<bool>(found_leaf_);
    }
    [[nodiscard]] bool
    key_matches() const
    {
        return key_matches_;
    }
    [[nodiscard]] PolyNodePtr
    get_found_leaf() const
    {
        return found_leaf_;
    }
    [[nodiscard]] const std::vector<std::pair<PolyNodePtr, int>>&
    get_path() const
    {
        return path_;
    }
    [[nodiscard]] int
    get_terminal_branch() const
    {
        return terminal_branch_;
    }
    [[nodiscard]] int
    get_divergence_depth() const
    {
        return divergence_depth_;
    }
    [[nodiscard]] bool
    has_divergence() const
    {
        return divergence_depth_ != -1;
    }

    // Debug helper
    void
    debug_path() const;

    /**
     * Add a new inner node at the divergence point
     * Used when inserting into a collapsed tree where paths diverge
     */
    void
    add_node_at_divergence();

private:
    /**
     * Navigate through a raw memory inner node
     * Returns true if we should continue, false if we hit a leaf/empty
     */
    bool
    navigate_raw_inner(PolyNodePtr& current, int& depth);

    /**
     * Materialize a raw node (convert from mmap to heap)
     * Returns intrusive_ptr for proper memory management
     */
    static boost::intrusive_ptr<HMapNode>
    materialize_raw_node(const uint8_t* raw, bool is_leaf);

    /**
     * Check if a key belongs in a collapsed inner node
     * Returns: {belongs, divergence_depth}
     * - belongs: true if key follows same path through collapsed levels
     * - divergence_depth: depth where paths diverge (or -1 if belongs)
     */
    std::pair<bool, int>
    key_belongs_in_inner(
        const PolyNodePtr& inner,
        const Key& key,
        int start_depth);
};

}  // namespace catl::hybrid_shamap