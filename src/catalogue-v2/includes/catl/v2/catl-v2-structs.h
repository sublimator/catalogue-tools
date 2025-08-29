#pragma once

#include "catl/common/ledger-info.h"
#include "catl/core/bit-utils.h"
#include "catl/core/logger.h"
#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>  // for std::memcpy
#include <limits>
#include <optional>

namespace catl::v2 {

/**
 * CATL v2 File Format Layout
 * =========================
 *
 * Portability Status:
 * ------------------
 * This format has been tested on:
 *   - ARM64 (Apple M2 Mac)
 *   - x86_64 (Linux via Docker)
 *
 * The combination of #pragma pack(1) and comprehensive static_assert checks
 * ensures consistent binary layout across these platforms. All structs are
 * trivially copyable with exact offsets verified at compile time.
 *
 * Current approach:
 *   - No compiler-specific bitfields (replaced with portable getters/setters)
 *   - Explicit bit manipulation for sub-byte fields
 *   - Both platforms tested are little-endian
 *
 * Future considerations:
 *   - May add explicit endianness conversion (currently files are created in
 * host endianness)
 *   - The endianness field in the header allows detecting mismatch but no
 * conversion yet
 *   - For now, keeping it nimble for experimental R&D
 *   - The pack(1) + static_assert approach works well in practice
 *   - Enable safe loading (memcpy) if targeting exotic architectures
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
 *     [InnerNodeHeader]             // 40 bytes
 *       - child_types               // 4 bytes (2 bits × 16 children)
 *       - depth                     // 2 bytes (6 bits used)
 *       - overlay_mask              // 2 bytes
 *       - hash                      // 32 bytes (perma-cached, first 256 bits
 * of SHA512) [Child Offsets]               // rel_off_t (8 bytes) × N non-empty
 * children
 *                                   // Each entry is self-relative to its own
 * slot:
 *                                   // abs_child = slot_file_offset + rel_off
 *     ... (depth-first traversal)
 *
 *     [LeafHeader]                  // 68 bytes
 *       - key                       // 32 bytes
 *       - hash                      // 32 bytes (perma-cached, first 256 bits
 * of SHA512)
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
 * Type aliases for different offset types
 */
using abs_off_t = std::uint64_t;  // absolute file offsets (from start of file)
using rel_off_t = std::int64_t;   // self-relative, signed 64-bit offsets

static_assert(sizeof(abs_off_t) == 8, "abs_off_t must be 8 bytes");
static_assert(sizeof(rel_off_t) == 8, "rel_off_t must be 8 bytes");

/**
 * Child type encoding for inner nodes (2 bits per child)
 */
enum class ChildType : std::uint8_t {
    EMPTY = 0,       // No child at this branch
    INNER = 1,       // Points to another inner node
    LEAF = 2,        // Points to a leaf node
    PLACEHOLDER = 3  // Reserved for future use
};

/**
 * Compact inner node header with embedded perma-cached hash
 * Total size: 40 bytes
 *
 * Field ordering is important to avoid padding:
 *   child_types (4 bytes) at offset 0
 *   depth_plus (2 bytes) at offset 4 - bits 0-5: depth, bits 6-15: reserved
 *   overlay_mask (2 bytes) at offset 6
 *   hash (32 bytes) at offset 8 - perma-cached hash (first 256 bits of SHA512)
 * Total: 40 bytes with no padding
 */
#pragma pack(push, 1)  // Ensure no padding between fields
struct InnerNodeHeader
{
    std::uint32_t child_types;  // 2 bits × 16 children = 32 bits (offset 0)
    std::uint16_t depth_plus;   // bits 0-5: depth (0-63), bits 6-15: reserved
    std::uint16_t
        overlay_mask;  // 16 bits: which branches are overridden (offset 6)
                       // 0 => no overlay (current experimental format)
                       //
                       // Future overlay layout when overlay_mask != 0:
                       //   [InnerNodeHeader (40 bytes)]
                       //   [rel_off_t base_rel]
                       //   [rel_off_t × popcount(overlay_mask) for changed
                       //   branches
                       //        in increasing branch order]
                       //
                       // Semantics: child_types describes the POST-overlay
                       // node. For branch b:
                       //   if (overlay_mask bit b) use the next overlay entry,
                       //   else resolve from base_rel's inner.
    std::array<std::uint8_t, 32>
        hash;  // Perma-cached hash: first 256 bits of SHA512 (offset 8)

    // Portable depth accessors (bits 0-5 of depth_plus)
    inline std::uint8_t
    get_depth() const
    {
        return static_cast<std::uint8_t>(
            depth_plus & 0x3F);  // Extract lower 6 bits
    }

    inline void
    set_depth(std::uint8_t depth)
    {
        assert(depth <= 63);  // Max value for 6 bits
        depth_plus = (depth_plus & 0xFFC0) | (depth & 0x3F);
    }

    // Reserved field accessors (bits 6-15 of depth_plus)
    inline std::uint16_t
    get_rfu() const
    {
        return depth_plus >> 6;  // Extract upper 10 bits
    }

    inline void
    set_rfu(std::uint16_t rfu)
    {
        assert(rfu <= 1023);  // Max value for 10 bits
        depth_plus = (depth_plus & 0x003F) | ((rfu & 0x3FF) << 6);
    }

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

    // Get perma-cached hash as Slice (zero-copy, read-only from mmap)
    Slice
    get_hash() const
    {
        return Slice(hash.data(), 32);
    }
};
#pragma pack(pop)  // Restore default alignment
static_assert(
    sizeof(InnerNodeHeader) == 40,
    "InnerNodeHeader must be 40 bytes");

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
    std::uint64_t ledger_count = 0;      // Number of ledgers in file
    std::uint64_t first_ledger_seq = 0;  // Sequence of first ledger
    std::uint64_t last_ledger_seq = 0;   // Sequence of last ledger
    abs_off_t ledger_index_offset = 0;   // Offset to ledger index
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
    std::uint32_t sequence;       // Ledger sequence number
    abs_off_t header_offset;      // Offset to LedgerInfo
    abs_off_t state_tree_offset;  // Offset to state tree root
    abs_off_t tx_tree_offset;     // Offset to tx tree root (0 if none)
};
#pragma pack(pop)  // Restore default alignment
static_assert(
    sizeof(LedgerIndexEntry) == 28,
    "LedgerIndexEntry must be 28 bytes");

/**
 * View that wraps a LedgerIndexEntry and provides lazy pointer conversion
 *
 * This provides a clean interface that automatically converts file offsets
 * to memory pointers on demand, avoiding the need to store converted pointers.
 */
class LedgerIndexEntryView
{
private:
    const LedgerIndexEntry* entry_;
    const uint8_t* file_base_;

public:
    LedgerIndexEntryView(
        const LedgerIndexEntry* entry,
        const uint8_t* file_base)
        : entry_(entry), file_base_(file_base)
    {
    }

    // Direct access to sequence number (no conversion needed)
    [[nodiscard]] uint32_t
    sequence() const
    {
        return entry_->sequence;
    }

    // Lazy conversions to pointers
    [[nodiscard]] const uint8_t*
    header_ptr() const
    {
        return file_base_ + entry_->header_offset;
    }

    [[nodiscard]] const uint8_t*
    state_tree_ptr() const
    {
        return file_base_ + entry_->state_tree_offset;
    }

    [[nodiscard]] const uint8_t*
    tx_tree_ptr() const
    {
        return entry_->tx_tree_offset ? file_base_ + entry_->tx_tree_offset
                                      : nullptr;
    }

    // Check if transaction tree exists
    [[nodiscard]] bool
    has_tx_tree() const
    {
        return entry_->tx_tree_offset != 0;
    }
};

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
 * Unified leaf header with perma-cached hash
 * Total size: 68 bytes (32 key + 32 hash + 4 flags, packed)
 *
 * Why we need the perma-cached hash:
 * - Without it, computing merkle hashes requires traversing entire mmap'd
 * subtrees
 * - This defeats lazy loading - you'd materialize the whole tree just for
 * hashing
 * - With perma-cached hashes: instant O(1) hash access for any node
 * - Enables efficient merkle proofs and incremental updates
 * - After one-time bulk verification, these hashes are trusted forever
 */
#pragma pack(push, 1)  // Ensure consistent binary layout
struct LeafHeader
{
    std::array<std::uint8_t, 32> key;  // 32 bytes
    std::array<std::uint8_t, 32>
        hash;  // 32 bytes - perma-cached hash (first 256 bits of SHA512)
    std::uint32_t size_and_flags;  // 4 bytes packed:
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

    // Get perma-cached hash as Slice (zero-copy, read-only from mmap)
    Slice
    get_hash() const
    {
        return Slice(hash.data(), 32);
    }
};
#pragma pack(pop)  // Restore default alignment
static_assert(sizeof(LeafHeader) == 68, "LeafHeader must be 68 bytes");

//----------------------------------------------------------
// Wire Format Static Assertions
// These ensure binary compatibility across platforms
//----------------------------------------------------------

// rel_off_t type guarantees
static_assert(sizeof(rel_off_t) == 8, "rel_off_t must be 8 bytes");
static_assert(std::is_signed_v<rel_off_t>, "rel_off_t must be signed");
static_assert(
    std::is_same_v<rel_off_t, std::int64_t>,
    "rel_off_t must be int64_t");

// CatlV2Header layout guarantees
static_assert(
    std::is_trivially_copyable_v<CatlV2Header>,
    "CatlV2Header must be trivially copyable");
static_assert(
    std::is_standard_layout_v<CatlV2Header>,
    "CatlV2Header must be standard layout");
static_assert(sizeof(CatlV2Header) == 48, "CatlV2Header must be 48 bytes");
static_assert(alignof(CatlV2Header) == 1, "CatlV2Header must be packed");
static_assert(offsetof(CatlV2Header, magic) == 0, "magic at offset 0");
static_assert(offsetof(CatlV2Header, version) == 4, "version at offset 4");
static_assert(
    offsetof(CatlV2Header, network_id) == 8,
    "network_id at offset 8");
static_assert(
    offsetof(CatlV2Header, endianness) == 12,
    "endianness at offset 12");
static_assert(
    offsetof(CatlV2Header, ledger_count) == 16,
    "ledger_count at offset 16");
static_assert(
    offsetof(CatlV2Header, first_ledger_seq) == 24,
    "first_ledger_seq at offset 24");
static_assert(
    offsetof(CatlV2Header, last_ledger_seq) == 32,
    "last_ledger_seq at offset 32");
static_assert(
    offsetof(CatlV2Header, ledger_index_offset) == 40,
    "ledger_index_offset at offset 40");

// InnerNodeHeader layout guarantees
static_assert(
    std::is_trivially_copyable_v<InnerNodeHeader>,
    "InnerNodeHeader must be trivially copyable");
static_assert(
    std::is_standard_layout_v<InnerNodeHeader>,
    "InnerNodeHeader must be standard layout");
static_assert(
    sizeof(InnerNodeHeader) == 40,
    "InnerNodeHeader must be 40 bytes");
static_assert(alignof(InnerNodeHeader) == 1, "InnerNodeHeader must be packed");
static_assert(
    offsetof(InnerNodeHeader, child_types) == 0,
    "child_types at offset 0");
static_assert(
    offsetof(InnerNodeHeader, depth_plus) == 4,
    "depth_plus at offset 4");
static_assert(
    offsetof(InnerNodeHeader, overlay_mask) == 6,
    "overlay_mask at offset 6");
static_assert(offsetof(InnerNodeHeader, hash) == 8, "hash at offset 8");

// LeafHeader layout guarantees
static_assert(
    std::is_trivially_copyable_v<LeafHeader>,
    "LeafHeader must be trivially copyable");
static_assert(
    std::is_standard_layout_v<LeafHeader>,
    "LeafHeader must be standard layout");
static_assert(sizeof(LeafHeader) == 68, "LeafHeader must be 68 bytes");
static_assert(alignof(LeafHeader) == 1, "LeafHeader must be packed");
static_assert(offsetof(LeafHeader, key) == 0, "key at offset 0");
static_assert(offsetof(LeafHeader, hash) == 32, "hash at offset 32");
static_assert(
    offsetof(LeafHeader, size_and_flags) == 64,
    "size_and_flags at offset 64");

// LedgerIndexEntry layout guarantees
static_assert(
    std::is_trivially_copyable_v<LedgerIndexEntry>,
    "LedgerIndexEntry must be trivially copyable");
static_assert(
    std::is_standard_layout_v<LedgerIndexEntry>,
    "LedgerIndexEntry must be standard layout");
static_assert(
    sizeof(LedgerIndexEntry) == 28,
    "LedgerIndexEntry must be 28 bytes");
static_assert(
    alignof(LedgerIndexEntry) == 1,
    "LedgerIndexEntry must be packed");
static_assert(
    offsetof(LedgerIndexEntry, sequence) == 0,
    "sequence at offset 0");
static_assert(
    offsetof(LedgerIndexEntry, header_offset) == 4,
    "header_offset at offset 4");
static_assert(
    offsetof(LedgerIndexEntry, state_tree_offset) == 12,
    "state_tree_offset at offset 12");
static_assert(
    offsetof(LedgerIndexEntry, tx_tree_offset) == 20,
    "tx_tree_offset at offset 20");

// TreesHeader layout guarantees
static_assert(
    std::is_trivially_copyable_v<TreesHeader>,
    "TreesHeader must be trivially copyable");
static_assert(
    std::is_standard_layout_v<TreesHeader>,
    "TreesHeader must be standard layout");
static_assert(sizeof(TreesHeader) == 16, "TreesHeader must be 16 bytes");
static_assert(alignof(TreesHeader) == 1, "TreesHeader must be packed");
static_assert(
    offsetof(TreesHeader, state_tree_size) == 0,
    "state_tree_size at offset 0");
static_assert(
    offsetof(TreesHeader, tx_tree_size) == 8,
    "tx_tree_size at offset 8");

}  // namespace catl::v2