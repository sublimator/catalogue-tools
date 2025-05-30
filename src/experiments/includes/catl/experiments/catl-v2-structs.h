#pragma once

#include "catl/common/ledger-info.h"
#include "catl/experiments/shamap-custom-traits.h"
#include <array>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>

namespace catl::experiments {

/**
 * CATL v2 File Format Layout
 * =========================
 *
 * [CatlV2Header]                    // 48 bytes
 *   - magic: 'CAT2'                 // 4 bytes
 *   - version: 1                    // 4 bytes
 *   - ledger_count                  // 8 bytes
 *   - first_ledger_seq              // 8 bytes
 *   - last_ledger_seq               // 8 bytes
 *   - ledger_index_offset           // 8 bytes (points to index at EOF)
 *
 * [Ledger 0]
 *   [LedgerInfo]                    // 118 bytes (canonical format)
 *     - seq                         // 4 bytes
 *     - drops                       // 8 bytes
 *     - parent_hash                 // 32 bytes
 *     - tx_hash                     // 32 bytes
 *     - account_hash                // 32 bytes
 *     - parent_close_time           // 4 bytes
 *     - close_time                  // 4 bytes
 *     - close_time_resolution       // 1 byte
 *     - close_flags                 // 1 byte
 *     - hash                        // 32 bytes
 *
 *   [TreesHeader]                   // 16 bytes
 *     - state_tree_size             // 8 bytes
 *     - tx_tree_size                // 8 bytes
 *
 *   [State Tree]
 *     [InnerNodeHeader]             // 6 bytes
 *       - depth                     // 2 bytes (6 bits used)
 *       - child_types               // 4 bytes (2 bits × 16 children)
 *     [Child Offsets]               // 8 bytes × N non-empty children
 *     ... (depth-first traversal)
 *
 *     [LeafHeader]                  // 36 bytes
 *       - key                       // 32 bytes
 *       - size_and_flags            // 4 bytes
 *         - bits 0-23: data size
 *         - bits 24-27: compression type
 *         - bits 28-31: reserved
 *     [Leaf Data]                   // Variable length
 *
 *   [Transaction Tree]              // Same structure as State Tree
 *     ...
 *
 * [Ledger 1]
 *   ... (only changed nodes written due to CoW)
 *
 * ... more ledgers ...
 *
 * [Ledger Index]                    // At EOF for easy appending
 *   [LedgerIndexEntry 0]            // 28 bytes
 *     - sequence                    // 4 bytes
 *     - header_offset               // 8 bytes
 *     - state_tree_offset           // 8 bytes
 *     - tx_tree_offset              // 8 bytes
 *   [LedgerIndexEntry 1]
 *   ... (one per ledger)
 *
 * Key Features:
 * - Structural sharing: unchanged nodes reference existing offsets
 * - Depth-first layout: optimizes cache locality
 * - Parallel-friendly: inner node child offsets enable concurrent processing
 * - Compression-ready: leaf headers support multiple compression schemes
 */

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
 * CATL v2 file header
 *
 * This format stores multiple ledgers with their canonical headers
 * and serialized state/transaction trees
 */
struct CatlV2Header
{
    std::array<char, 4> magic = {'C', 'A', 'T', '2'};  // CATL v2
    std::uint32_t version = 1;
    std::uint64_t ledger_count = 0;         // Number of ledgers in file
    std::uint64_t first_ledger_seq = 0;     // Sequence of first ledger
    std::uint64_t last_ledger_seq = 0;      // Sequence of last ledger
    std::uint64_t ledger_index_offset = 0;  // Offset to ledger index
};

/**
 * Entry in the ledger index
 */
struct LedgerIndexEntry
{
    std::uint32_t sequence;           // Ledger sequence number
    std::uint64_t header_offset;      // Offset to LedgerInfo
    std::uint64_t state_tree_offset;  // Offset to state tree root
    std::uint64_t tx_tree_offset;     // Offset to tx tree root (0 if none)
};

/**
 * Tree size header written after each LedgerInfo
 *
 * This allows readers to skip entire trees without parsing them
 */
struct TreesHeader
{
    std::uint64_t state_tree_size;  // Size of state tree in bytes
    std::uint64_t tx_tree_size;     // Size of tx tree in bytes
};

/**
 * Compression type for future extensibility
 */
enum class CompressionType : std::uint8_t {
    NONE = 0,
    ZSTD = 1,
};

/**
 * Unified leaf header for all leaf nodes
 * Total size: 36 bytes (good alignment)
 */
struct LeafHeader
{
    std::array<std::uint8_t, 32> key;  // 32 bytes
    std::uint32_t size_and_flags;      // 4 bytes packed:
                                       // Bits 0-23: data size (up to 16MB)
                                       // Bits 24-27: compression type
                                       // Bits 28-31: reserved

    // Helper methods
    CompressionType
    compression_type() const
    {
        return static_cast<CompressionType>((size_and_flags >> 24) & 0x0F);
    }

    bool
    is_compressed() const
    {
        return compression_type() != CompressionType::NONE;
    }

    std::uint32_t
    data_size() const
    {
        return size_and_flags & 0x00FFFFFF;  // lower 24 bits
    }

    void
    set_compression_type(CompressionType type)
    {
        size_and_flags = (size_and_flags & 0xF0FFFFFF) |
            (static_cast<std::uint32_t>(type) << 24);
    }

    void
    set_data_size(std::uint32_t size)
    {
        if (size > 0x00FFFFFF)
        {
            throw std::overflow_error("Leaf data size exceeds 16MB");
        }
        size_and_flags = (size_and_flags & 0xFF000000) | size;
    }
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