#pragma once

#include <cstddef>

namespace catl::nodestore {

/**
 * Compression types used in nodeobject encoding.
 *
 * The nodeobject format starts with a varint indicating the compression type,
 * followed by type-specific payload.
 */
enum class compression_type : std::size_t {
    uncompressed = 0,           // Raw data (no longer used, always compress)
    lz4 = 1,                    // LZ4 compressed data
    inner_node_compressed = 2,  // v1 inner node with bitmask (sparse)
    inner_node_full = 3         // v1 inner node with all 16 hashes
};

/**
 * Convert compression_type to string for display/logging
 */
inline const char*
compression_type_to_string(compression_type type)
{
    switch (type)
    {
        case compression_type::uncompressed:
            return "uncompressed";
        case compression_type::lz4:
            return "lz4";
        case compression_type::inner_node_compressed:
            return "inner_node_compressed";
        case compression_type::inner_node_full:
            return "inner_node_full";
        default:
            return "unknown";
    }
}

// Format size constants
namespace format {

// Inner node v1 format sizes
constexpr std::size_t INNER_NODE_V1_SIZE =
    525;  // Full decoded size (9 header + 4 prefix + 512 hashes)
constexpr std::size_t INNER_NODE_HASH_ARRAY_SIZE = 512;  // 16 * 32 bytes
constexpr std::size_t INNER_NODE_HASH_SIZE = 32;         // Single hash size
constexpr std::size_t INNER_NODE_BRANCH_COUNT = 16;      // Number of branches

// Nodeobject header sizes
constexpr std::size_t NODEOBJECT_HEADER_SIZE =
    9;  // 8 unused bytes + 1 type byte

}  // namespace format

}  // namespace catl::nodestore
