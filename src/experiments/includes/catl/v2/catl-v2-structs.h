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
 * MemPtr - A typed pointer wrapper for memory-mapped data
 *
 * This class provides a thin (8-byte) wrapper around pointers into mmap'd
 * memory. It documents ownership semantics (the data is owned by the mapped
 * file) and provides safe/unsafe access patterns based on compile-time
 * configuration.
 *
 * Key design principles:
 * - Same size as a raw pointer (8 bytes)
 * - get() returns a VALUE (not pointer) for stack-based usage
 * - Respects CATL_UNSAFE_POD_LOADS for performance vs portability
 * - Makes memory-mapped pointer semantics explicit in the type system
 *
 * Usage:
 *   MemPtr<InnerNodeHeader> header_ptr(mmap_data);
 *   // ... pass header_ptr around (cheap, 8 bytes) ...
 *   auto header = header_ptr.get();  // Get value on stack when needed
 *   auto depth = header.get_depth(); // Use the value
 */
template <typename T>
class MemPtr
{
private:
    const uint8_t* ptr_;

public:
    // Constructors
    explicit MemPtr(const uint8_t* p) : ptr_(p)
    {
    }
    explicit MemPtr(const void* p) : ptr_(static_cast<const uint8_t*>(p))
    {
    }
    MemPtr() : ptr_(nullptr)
    {
    }

    // Copy and move semantics (trivial, it's just a pointer)
    MemPtr(const MemPtr&) = default;
    MemPtr&
    operator=(const MemPtr&) = default;
    MemPtr(MemPtr&&) = default;
    MemPtr&
    operator=(MemPtr&&) = default;

    /**
     * Get the value pointed to, safely handling alignment.
     *
     * In CATL_UNSAFE_POD_LOADS mode: returns const reference (zero-copy!)
     * In safe mode: returns by value (copy for alignment safety)
     *
     * This allows zero-copy access in unsafe mode while maintaining
     * safety in portable mode.
     *
     * @return Reference (unsafe mode) or copy (safe mode)
     */
#ifdef CATL_UNSAFE_POD_LOADS
    [[nodiscard]] const T&
    get() const
    {
        static_assert(
            std::is_trivially_copyable_v<T>, "T must be trivially copyable");
        assert(ptr_ != nullptr);
        // Fast path: direct cast, return reference (zero-copy!)
        return *reinterpret_cast<const T*>(ptr_);
    }
#else
    [[nodiscard]] T
    get() const
    {
        static_assert(
            std::is_trivially_copyable_v<T>, "T must be trivially copyable");
        assert(ptr_ != nullptr);
        // Safe path: memcpy (portable, compiler optimizes for small types)
        T out;
        std::memcpy(&out, ptr_, sizeof(T));
        return out;
    }
#endif

    /**
     * Get the raw byte pointer
     * Useful for pointer arithmetic or passing to functions that need uint8_t*
     */
    [[nodiscard]] const uint8_t*
    raw() const
    {
        return ptr_;
    }

    /**
     * Check if the pointer is null
     */
    [[nodiscard]] bool
    is_null() const
    {
        return ptr_ == nullptr;
    }
    explicit operator bool() const
    {
        return ptr_ != nullptr;
    }

    /**
     * Offset the pointer by a number of bytes
     */
    [[nodiscard]] MemPtr<T>
    offset(std::ptrdiff_t bytes) const
    {
        return MemPtr<T>(ptr_ + bytes);
    }

    /**
     * Cast to a different type (for reinterpreting memory)
     */
    template <typename U>
    MemPtr<U>
    cast() const
    {
        return MemPtr<U>(ptr_);
    }
};

// Ensure MemPtr is truly just a pointer (8 bytes on 64-bit systems)
static_assert(
    sizeof(MemPtr<int>) == sizeof(void*),
    "MemPtr must be same size as a pointer");

/**
 * Helper to convert relative offset to absolute offset
 * @param slot File position of the offset slot
 * @param rel Relative offset value
 * @return Absolute file offset
 *
 * TODO: This is overcomplicated - when working with mmap'd memory,
 * self-relative offsets work directly with pointer arithmetic:
 *   uint8_t* absolute_ptr = slot_ptr + relative_offset;
 * The abstraction to "file offsets" actually obscures the simplicity!
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
 *
 * TODO: Similarly overcomplicated - with pointers it's just:
 *   rel_off_t rel = target_ptr - slot_ptr;
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
 *
 * TODO: When working with pointers, this is just:
 *   uint8_t* slot_ptr = base_ptr + index * sizeof(rel_off_t);
 * No need for the uint64_t abstraction!
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
 * Define CATL_UNSAFE_POD_LOADS to use direct reinterpret_cast for
 * performance on platforms where alignment is guaranteed.
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
        std::is_trivially_copyable_v<T>, "T must be trivially copyable");

    if (offset + sizeof(T) > file_size)
    {
        throw std::runtime_error("read past end of file");
    }

#ifdef CATL_UNSAFE_POD_LOADS
    // Fast path: direct cast (platform-specific, may be unsafe)
    return *reinterpret_cast<const T*>(base + offset);
#else
    // Safe path: memcpy (portable, compiler optimizes for small types)
    T out;
    std::memcpy(&out, base + offset, sizeof(T));
    return out;
#endif
}

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
 *   - Safe POD loading available via CATL_UNSAFE_POD_LOADS flag
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
 * Compact inner node header
 * Total size: 8 bytes
 *
 * Field ordering is important to avoid padding:
 *   child_types (4 bytes) at offset 0
 *   depth_plus (2 bytes) at offset 4 - bits 0-5: depth, bits 6-15: reserved
 *   overlay_mask (2 bytes) at offset 6
 * Total: 8 bytes with no padding
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
    MemPtr<InnerNodeHeader> header;   // Memory-mapped header pointer
    const std::uint8_t* rel_base;     // Byte pointer to relative offset array
    std::uint64_t offsets_file_base;  // File offset of the FIRST slot
    uint32_t remaining_mask;          // Bitmask of remaining children to visit
    int offset_index;                 // Current index in sparse offset array

    ChildIterator(
        MemPtr<InnerNodeHeader> h,
        const uint8_t* offset_data,
        std::uint64_t offsets_file_base_)
        : header(h)
        , rel_base(offset_data)
        , offsets_file_base(offsets_file_base_)
        , remaining_mask(0)
        , offset_index(0)
    {
        auto header_val = header.get();
        // Overlay not implemented in the reader path yet
        assert(
            header_val.overlay_mask == 0 &&
            "overlay not implemented in iterator");

        // Build initial mask of non-empty children
        for (int i = 0; i < 16; ++i)
        {
            if (header_val.get_child_type(i) != ChildType::EMPTY)
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

        auto header_val = header.get();
        Child child;
        child.branch = branch;
        child.type = header_val.get_child_type(branch);
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
static_assert(sizeof(InnerNodeHeader) == 8, "InnerNodeHeader must be 8 bytes");
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

// LeafHeader layout guarantees
static_assert(
    std::is_trivially_copyable_v<LeafHeader>,
    "LeafHeader must be trivially copyable");
static_assert(
    std::is_standard_layout_v<LeafHeader>,
    "LeafHeader must be standard layout");
static_assert(sizeof(LeafHeader) == 36, "LeafHeader must be 36 bytes");
static_assert(alignof(LeafHeader) == 1, "LeafHeader must be packed");
static_assert(offsetof(LeafHeader, key) == 0, "key at offset 0");
static_assert(
    offsetof(LeafHeader, size_and_flags) == 32,
    "size_and_flags at offset 32");

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