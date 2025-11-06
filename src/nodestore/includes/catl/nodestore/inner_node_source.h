#pragma once

#include "catl/core/types.h"
#include <concepts>
#include <cstdint>

namespace catl::nodestore {

/**
 * Concept for types that can be directly compressed as inner nodes.
 *
 * This enables zero-copy compression: instead of serializing to 525 bytes,
 * parsing back out, and re-encoding, we read the data directly from the
 * source structure and encode it in one pass.
 *
 * The get_node_source_* naming convention is intentionally verbose to:
 * - Avoid naming collisions with existing methods
 * - Make it obvious these are for the compression concept
 * - Stand out clearly when implementing the concept
 */
template <typename T>
concept inner_node_source = requires(T const& node, int branch)
{
    /**
     * Direct access to child hash by branch index (0-15).
     * Must return Hash256 or reference to Hash256.
     */
    {
        node.get_node_source_child_hash(branch)
    }
    ->std::convertible_to<Hash256 const&>;

    /**
     * Bitmask where bit N set means branch N is populated.
     * Enables single atomic read + __builtin_popcount() for branch count.
     */
    {
        node.get_node_source_branch_mask()
    }
    ->std::convertible_to<uint16_t>;

    /**
     * Hash of the node itself.
     * Used as the storage key (not stored in the blob to avoid duplication).
     */
    {
        node.get_node_source_hash()
    }
    ->std::convertible_to<Hash256 const&>;
};

}  // namespace catl::nodestore
