#pragma once

#include "catl/experiments/shamap-custom-traits.h"
#include <array>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>

namespace catl::experiments {

//----------------------------------------------------------
// Core Binary Format Structures
//----------------------------------------------------------

/**
 * Child type encoding for inner nodes (2 bits per child)
 */
enum class ChildType : std::uint8_t {
    EMPTY = 0,  // No child at this branch
    INNER = 1,  // Points to another inner node
    LEAF = 2,   // Points to a leaf node
    RFU = 3     // Reserved for future use
};

/**
 * Compact depth and flags representation
 *
 * Using 2 bytes provides:
 * - 6 bits for depth (supports 0-63)
 * - 10 bits reserved for future features
 * - Word alignment efficiency
 */
struct DepthAndFlags
{
    std::uint16_t depth : 6;  // Current depth in tree
    std::uint16_t rfu : 10;   // Reserved for future use
};

/**
 * Compact inner node header
 * Total size: 6 bytes
 */
struct InnerNodeHeader
{
    union
    {
        std::uint16_t depth_plus;  // Raw access for serialization
        DepthAndFlags bits;        // Structured field access
    };
    std::uint32_t child_types;  // 2 bits × 16 children = 32 bits

    // Helper to get/set child type
    ChildType
    get_child_type(int branch) const
    {
        return static_cast<ChildType>((child_types >> (branch * 2)) & 0x3);
    }

    void
    set_child_type(int branch, ChildType type)
    {
        std::uint32_t mask = ~(0x3u << (branch * 2));
        child_types = (child_types & mask) |
            (static_cast<std::uint32_t>(type) << (branch * 2));
    }

    // Count non-empty children
    int
    count_children() const
    {
        int count = 0;
        for (int i = 0; i < 16; ++i)
        {
            if (get_child_type(i) != ChildType::EMPTY)
            {
                count++;
            }
        }
        return count;
    }
};

/**
 * File header for serialized trees
 */
struct SerializedTreeHeader
{
    std::array<char, 4> magic = {'S', 'I', 'N', 'R'};  // Serialized INneR
    std::uint32_t version = 1;
    std::uint64_t root_offset = 0;           // Offset to root inner node
    std::uint64_t total_inners = 0;          // Total inner nodes
    std::uint64_t total_leaves = 0;          // Total leaf nodes
    std::uint64_t bookmark_offset = 0;       // Offset to bookmark table
    std::array<std::uint8_t, 32> root_hash;  // Root hash for verification
};

/**
 * Bookmark entry for parallel loading
 * Marks the start of each depth=1 subtree
 */
struct BookmarkEntry
{
    std::uint8_t branch;         // Which branch at depth 0 (0-15)
    std::uint64_t offset;        // File offset to this subtree
    std::uint64_t subtree_size;  // Size of serialized subtree
};

/**
 * Compressed leaf header
 * Used when leaf data is compressed with zstd
 */
struct CompressedLeafHeader
{
    std::uint32_t uncompressed_size;
    std::uint32_t compressed_size;
    // Followed by compressed data
};

/**
 * Build child types bitmap from a SHAMapInnerNodeS
 */
inline std::uint32_t
build_child_types(const boost::intrusive_ptr<SHAMapInnerNodeS>& inner)
{
    std::uint32_t child_types = 0;

    for (int i = 0; i < 16; ++i)
    {
        auto child = inner->get_child(i);
        ChildType type;

        if (!child)
        {
            type = ChildType::EMPTY;
        }
        else if (child->is_inner())
        {
            type = ChildType::INNER;
        }
        else
        {
            type = ChildType::LEAF;
        }

        // Set 2 bits for this branch
        child_types |= (static_cast<std::uint32_t>(type) << (i * 2));
    }

    return child_types;
}

}  // namespace catl::experiments