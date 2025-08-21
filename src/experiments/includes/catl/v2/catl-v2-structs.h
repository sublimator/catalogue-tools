#pragma once

#include "catl/common/ledger-info.h"
#include "shamap-custom-traits.h"
#include <array>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cassert>
#include <cstdint>
#include <cstring>  // for std::memcpy
#include <limits>
#include <optional>

namespace catl::v2 {

/**
 * Type alias for self-relative offsets
 * Each offset is relative to its own slot position in the file:
 * absolute_offset = slot_position + relative_offset
 */
using rel_off_t = std::int64_t;  // self-relative, signed 64-bit offsets
static_assert(sizeof(rel_off_t) == 8, "rel_off_t must be 8 bytes");

/**
 * Helper to convert relative offset to absolute offset
 * @param slot File position of the offset slot
 * @param rel Relative offset value
 * @return Absolute file offset
 */
inline std::uint64_t
abs_from_rel(std::uint64_t slot, rel_off_t rel)
{
    // Defensive checks (compiled out in release with NDEBUG)
    // File sizes >> 2^63-1 are not a realistic target for this format
    assert(
        slot <=
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()));
    std::int64_t a = static_cast<std::int64_t>(slot) + rel;
    assert(a >= 0);
    return static_cast<std::uint64_t>(a);
}

/**
 * Helper to convert absolute offset to relative offset
 * @param abs Absolute file offset
 * @param slot File position of the offset slot
 * @return Relative offset value
 */
inline rel_off_t
rel_from_abs(std::uint64_t abs, std::uint64_t slot)
{
    assert(
        abs <=
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()));
    assert(
        slot <=
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()));
    return static_cast<rel_off_t>(
        static_cast<std::int64_t>(abs) - static_cast<std::int64_t>(slot));
}

/**
 * Helper to calculate slot position in offset array
 * @param base Base address of offset array
 * @param index Index of the slot (0-based)
 * @return File position of the slot
 */
inline std::uint64_t
slot_from_index(std::uint64_t base, int index)
{
    assert(index >= 0);
    return base + static_cast<std::uint64_t>(index) * sizeof(rel_off_t);
}

/**
 * Helper to load a relative offset from unaligned memory
 * @param base Base address of offset array
 * @param index Index of the offset to load (0-based)
 * @return The loaded relative offset
 */
inline rel_off_t
load_rel(const uint8_t* base, int index)
{
    assert(index >= 0);
    rel_off_t rel{};
    std::memcpy(
        &rel,
        base + static_cast<std::size_t>(index) * sizeof(rel_off_t),
        sizeof(rel));
    return rel;
}

/**
 * Safe loading of POD types from memory-mapped data.
 * This avoids undefined behavior from reinterpret_cast on potentially
 * misaligned pointers and ensures proper object lifetime.
 * 
 * @tparam T The trivially copyable type to load
 * @param base Base pointer to the memory-mapped data
 * @param offset Byte offset from base
 * @param file_size Total size of the memory-mapped file (for bounds checking)
 * @return Copy of the object at the specified location
 * @throws std::runtime_error if reading past end of file
 */
template <typename T>
inline T
load_pod(const uint8_t* base, size_t offset, size_t file_size)
{
    static_assert(
        std::is_trivially_copyable_v<T>,
        "T must be trivially copyable");
    
    if (offset + sizeof(T) > file_size)
    {
        throw std::runtime_error("read past end of file");
    }
    
    T out;
    std::memcpy(&out, base + offset, sizeof(T));
    return out;
}

/**
 * CATL v2 File Format Layout
 * =========================
 *
 * [CatlV2Header]                    // 52 bytes
 *   - magic: 'CAT2'                 // 4 bytes
 *   - version: 1                    // 4 bytes
 *   - network_id                    // 4 bytes (0=XRPL, 21337=Xahau)
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
 *     [Child Offsets]               // rel_off_t (8 bytes) × N non-empty
 * children
 *                                   // Each entry is self-relative to its own
 * slot:
 *                                   // abs_child = slot_file_offset + rel_off
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
 * Total size: 8 bytes (was 6, added overlay_mask for alignment and future use)
 *
 * Field ordering is important to avoid padding:
 *   child_types (4 bytes) at offset 0
 *   depth union (2 bytes) at offset 4
 *   overlay_mask (2 bytes) at offset 6
 * Total: 8 bytes with no padding
 */
#pragma pack(push, 1)  // Ensure no padding between fields
struct InnerNodeHeader
{
    std::uint32_t child_types;  // 2 bits × 16 children = 32 bits (offset 0)
    union
    {
        std::uint16_t depth_plus;  // Raw access for serialization (offset 4)
        DepthAndFlags bits;        // Structured field access
    };
    std::uint16_t
        overlay_mask;  // 16 bits: which branches are overridden (offset 6)
                       // 0 => no overlay (current experimental format)
                       //
                       // Future overlay layout when overlay_mask != 0:
                       //   [InnerNodeHeader (8 bytes)]
                       //   [rel_off_t base_rel]
                       //   [rel_off_t × popcount(overlay_mask) for changed
                       //   branches
                       //        in increasing branch order]
                       //
                       // Semantics: child_types describes the POST-overlay
                       // node. For branch b:
                       //   if (overlay_mask bit b) use the next overlay entry,
                       //   else resolve from base_rel's inner.

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
#pragma pack(pop)  // Restore default alignment
static_assert(sizeof(InnerNodeHeader) == 8, "InnerNodeHeader must be 8 bytes");

/**
 * Lightweight iterator for non-empty children in sparse offset array
 *
 * Designed for maximum performance - no virtual functions, minimal state.
 * Only iterates over branches that actually have children.
 * Converts self-relative offsets to absolute offsets on-the-fly.
 */
struct ChildIterator
{
    const InnerNodeHeader* header;
    const std::uint8_t* rel_base;     // Byte pointer to relative offset array
    std::uint64_t offsets_file_base;  // File offset of the FIRST slot
    uint32_t remaining_mask;          // Bitmask of remaining children to visit
    int offset_index;                 // Current index in sparse offset array

    ChildIterator(
        const InnerNodeHeader* h,
        const uint8_t* offset_data,
        std::uint64_t offsets_file_base_)
        : header(h)
        , rel_base(offset_data)
        , offsets_file_base(offsets_file_base_)
        , remaining_mask(0)
        , offset_index(0)
    {
        // Overlay not implemented in the reader path yet
        assert(h->overlay_mask == 0 && "overlay not implemented in iterator");

        // Build initial mask of non-empty children
        for (int i = 0; i < 16; ++i)
        {
            if (header->get_child_type(i) != ChildType::EMPTY)
            {
                remaining_mask |= (1u << i);
            }
        }
    }

    struct Child
    {
        int branch;
        ChildType type;
        std::uint64_t offset;
    };

    // Check if more children available
    inline bool
    has_next() const
    {
        return remaining_mask != 0;
    }

    // Get next child - caller must check has_next() first
    inline Child
    next()
    {
        // Find next set bit (next non-empty branch)
        int branch = __builtin_ctz(remaining_mask);  // Count trailing zeros

        // Load relative offset safely (unaligned-friendly)
        rel_off_t rel = load_rel(rel_base, offset_index);

        // Calculate the file position of this slot
        std::uint64_t slot = slot_from_index(offsets_file_base, offset_index);

        Child child;
        child.branch = branch;
        child.type = header->get_child_type(branch);
        // Convert relative to absolute: abs = slot + rel
        child.offset = abs_from_rel(slot, rel);

        // Clear this bit from remaining mask
        remaining_mask &= ~(1u << branch);
        ++offset_index;

        return child;
    }
};

/**
 * CATL v2 file header
 *
 * This format stores multiple ledgers with their canonical headers
 * and serialized state/transaction trees
 */
#pragma pack(push, 1)  // Ensure consistent binary layout
struct CatlV2Header
{
    std::array<char, 4> magic = {'C', 'A', 'T', '2'};  // CATL v2
    std::uint32_t version =
        1;  // Currently experimental - no version handling yet
            // Will be used for compatibility when out of experimental
    std::uint32_t network_id = 0;  // Network ID (0=XRPL, 21337=Xahau)
    std::uint32_t endianness =
        0x01020304;  // Endianness marker (little=0x04030201, big=0x01020304)
    std::uint64_t ledger_count = 0;         // Number of ledgers in file
    std::uint64_t first_ledger_seq = 0;     // Sequence of first ledger
    std::uint64_t last_ledger_seq = 0;      // Sequence of last ledger
    std::uint64_t ledger_index_offset = 0;  // Offset to ledger index
};
#pragma pack(pop)  // Restore default alignment
static_assert(sizeof(CatlV2Header) == 48, "CatlV2Header must be 48 bytes");

/**
 * Get the host system's endianness marker
 * @return 0x01020304 for big endian, 0x04030201 for little endian
 */
inline std::uint32_t
get_host_endianness()
{
    const std::uint32_t test = 0x01020304;
    const std::uint8_t* bytes = reinterpret_cast<const std::uint8_t*>(&test);
    return bytes[0] == 0x04 ? 0x04030201 : 0x01020304;
}

/**
 * Entry in the ledger index
 */
#pragma pack(push, 1)  // Ensure consistent binary layout
struct LedgerIndexEntry
{
    std::uint32_t sequence;           // Ledger sequence number
    std::uint64_t header_offset;      // Offset to LedgerInfo
    std::uint64_t state_tree_offset;  // Offset to state tree root
    std::uint64_t tx_tree_offset;     // Offset to tx tree root (0 if none)
};
#pragma pack(pop)  // Restore default alignment
static_assert(
    sizeof(LedgerIndexEntry) == 28,
    "LedgerIndexEntry must be 28 bytes");

/**
 * Tree size header written after each LedgerInfo
 *
 * This allows readers to skip entire trees without parsing them
 */
#pragma pack(push, 1)  // Ensure consistent binary layout
struct TreesHeader
{
    std::uint64_t state_tree_size;  // Size of state tree in bytes
    std::uint64_t tx_tree_size;     // Size of tx tree in bytes
};
#pragma pack(pop)  // Restore default alignment
static_assert(sizeof(TreesHeader) == 16, "TreesHeader must be 16 bytes");

/**
 * Compression type for future extensibility
 */
enum class CompressionType : std::uint8_t {
    NONE = 0,
    ZSTD = 1,
};

/**
 * Unified leaf header for all leaf nodes
 * Total size: 36 bytes (32 + 4, packed)
 */
#pragma pack(push, 1)  // Ensure consistent binary layout
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
#pragma pack(pop)  // Restore default alignment
static_assert(sizeof(LeafHeader) == 36, "LeafHeader must be 36 bytes");

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

}  // namespace catl::v2