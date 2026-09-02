#pragma once

#include <cstddef>
#include <cstdint>

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
    inner_node_full = 3,        // v1 inner node with all 16 hashes
    inner_node_augmented = 4    // sparse inner + one .dat offset per child
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
        case compression_type::inner_node_augmented:
            return "inner_node_augmented";
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

// Augmented inner (codec 4): a .dat record offset per populated child,
// 48 bits big-endian, the width of NuDB's own bucket entries.
constexpr std::size_t INNER_NODE_CHILD_OFFSET_SIZE = 6;
constexpr std::uint64_t INNER_NODE_CHILD_OFFSET_MAX =
    (std::uint64_t{1} << 48) - 1;

// Prelude of the DECODED form only: 8 unused bytes + 1 type byte.
// It is NOT present in front of the on-disk value; the on-disk value starts
// with the codec varint (see node_blob.h).
constexpr std::size_t NODEOBJECT_HEADER_SIZE = 9;

}  // namespace format

}  // namespace catl::nodestore
