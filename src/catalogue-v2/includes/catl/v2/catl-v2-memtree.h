#pragma once

/**
 * Memory-mapped tree operations for CATL v2 format
 *
 * Provides lightweight views and operations for navigating v2 trees
 * directly in mmap'd memory without copying or materializing nodes.
 */

#include "catl/core/types.h"
#include "catl/shamap/shamap-utils.h"
#include "catl/v2/catl-v2-structs.h"
#include <cstring>
#include <stdexcept>

namespace catl::v2 {
// Log partition for v2 structs debugging
inline LogPartition&
get_v2_memtree_log_partition()
{
    static LogPartition partition("v2-memtree", LogLevel::NONE);
    return partition;
}

/**
 * Helper functions for self-relative offset conversion
 * Used by: catl-v2-writer.h serialize_tree() method
 *
 * These convert between absolute file offsets (used during writing)
 * and self-relative offsets (stored in the file format)
 */
inline abs_off_t
slot_from_index(abs_off_t base_offset, size_t index)
{
    return base_offset + index * sizeof(rel_off_t);
}

inline rel_off_t
rel_from_abs(abs_off_t target_offset, abs_off_t slot_offset)
{
    return static_cast<rel_off_t>(target_offset - slot_offset);
}

/**
 * This codebase uses memory pointers (const uint8_t*) exclusively for
 * navigation. File offsets are not used - everything is direct pointer
 * arithmetic.
 *
 * Self-relative offsets work simply:
 *   child_ptr = slot_ptr + relative_offset
 *
 * This enables:
 * - Multiple mmap files (each with their own base pointer)
 * - Simpler code (no offset/pointer conversions)
 * - Better performance (direct pointer access)
 * Resolve a self-relative offset to get the actual pointer
 *
 * Self-relative offsets are stored relative to their own storage location.
 * This function loads the offset and adds it to the slot address to get
 * the final pointer.
 *
 * @param offsets_array Base address of the offset array
 * @param index Index of the offset to resolve (0-based)
 * @return Pointer to the target location
 */
inline const uint8_t*
resolve_self_relative(const uint8_t* offsets_array, int index)
{
    assert(index >= 0);
    const uint8_t* slot =
        offsets_array + static_cast<std::size_t>(index) * sizeof(rel_off_t);

    auto& log = get_v2_memtree_log_partition();
    if (log.should_log(LogLevel::DEBUG))
    {
        Logger::log(
            LogLevel::DEBUG,
            "[v2-structs] resolve_self_relative: index=",
            index,
            ", offsets_array=",
            static_cast<const void*>(offsets_array),
            ", slot=",
            static_cast<const void*>(slot));
    }

    rel_off_t offset{};
    std::memcpy(&offset, slot, sizeof(offset));

    if (log.should_log(LogLevel::DEBUG))
    {
        Logger::log(LogLevel::DEBUG, "[v2-structs]   loaded offset=", offset);
    }

    const uint8_t* result = slot + offset;

    if (log.should_log(LogLevel::DEBUG))
    {
        Logger::log(
            LogLevel::DEBUG,
            "[v2-structs]   result ptr=",
            static_cast<const void*>(result));
    }

    return slot + offset;  // Self-relative: slot + offset_from_slot
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
 * @return Reference (UNSAFE mode) or copy (safe mode) of the object
 * @throws std::runtime_error if reading past end of file
 */
template <typename T>
#ifdef CATL_UNSAFE_POD_LOADS
inline const T&
load_pod(const uint8_t* base, size_t offset, size_t file_size)
{
    static_assert(
        std::is_trivially_copyable_v<T>, "T must be trivially copyable");

    if (offset + sizeof(T) > file_size)
    {
        throw std::runtime_error("read past end of file");
    }

    // Fast path: direct cast, return reference (zero-copy!)
    return *reinterpret_cast<const T*>(base + offset);
}
#else
inline T
load_pod(const uint8_t* base, size_t offset, size_t file_size)
{
    static_assert(
        std::is_trivially_copyable_v<T>, "T must be trivially copyable");

    if (offset + sizeof(T) > file_size)
    {
        throw std::runtime_error("read past end of file");
    }

    // Safe path: memcpy (portable, compiler optimizes for small types)
    T out;
    std::memcpy(&out, base + offset, sizeof(T));
    return out;
}
#endif

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
 *   auto header = header_ptr.get_uncopyable();  // Get value on stack when
 * needed auto depth = header.get_depth(); // Use the value
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
    get_uncopyable() const
    {
        static_assert(
            std::is_trivially_copyable_v<T>, "T must be trivially copyable");
        assert(ptr_ != nullptr);
        // Fast path: direct cast, return reference (zero-copy!)
        return *reinterpret_cast<const T*>(ptr_);
    }
#else
    [[nodiscard]] T
    get_uncopyable() const
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
 * Sparse child offset array accessor
 *
 * Provides efficient access to child offsets in a sparse array where only
 * non-empty children have offsets stored. Uses popcount for O(1) indexing.
 *
 * This is specifically designed for our 16-branch merkle tree with
 * 2-bits-per-branch encoding.
 */
class SparseChildOffsets
{
private:
    const uint8_t* base_;   // First offset location in memory
    uint32_t child_types_;  // 2-bits-per-branch mask from header

public:
    SparseChildOffsets(const uint8_t* offset_base, uint32_t child_types)
        : base_(offset_base), child_types_(child_types)
    {
    }

    /**
     * Check if a branch has a child
     */
    [[nodiscard]] bool
    has_child(int branch) const
    {
        assert(branch >= 0 && branch < 16);
        return ((child_types_ >> (branch * 2)) & 0x3) != 0;
    }

    /**
     * Get the child type for a branch
     */
    [[nodiscard]] ChildType
    get_child_type(int branch) const
    {
        assert(branch >= 0 && branch < 16);
        return static_cast<ChildType>((child_types_ >> (branch * 2)) & 0x3);
    }

    /**
     * Get the sparse array index for a branch
     * Returns -1 if the branch has no child
     */
    [[nodiscard]] int
    get_sparse_index(int branch) const
    {
        if (!has_child(branch))
            return -1;

        // Count non-empty children before this branch
        uint32_t mask = 0;
        for (int i = 0; i < branch; ++i)
        {
            if (((child_types_ >> (i * 2)) & 0x3) != 0)
            {
                mask |= (1u << i);
            }
        }
        return catl::core::popcount(mask);
    }

    /**
     * Get a MemPtr to the offset slot for a branch
     * Returns null MemPtr if branch has no child
     */
    [[nodiscard]] MemPtr<rel_off_t>
    get_offset_ptr(int branch) const
    {
        int index = get_sparse_index(branch);
        if (index < 0)
            return MemPtr<rel_off_t>();

        return MemPtr<rel_off_t>(base_ + index * sizeof(rel_off_t));
    }

    /**
     * Get the absolute child pointer for a branch
     * Returns nullptr if branch has no child
     */
    [[nodiscard]] const uint8_t*
    get_child_ptr(int branch) const
    {
        int index = get_sparse_index(branch);
        if (index < 0)
            return nullptr;

        // Resolve self-relative offset to get child pointer
        return resolve_self_relative(base_, index);
    }

    /**
     * Count total non-empty children
     */
    [[nodiscard]] int
    count_children() const
    {
        int count = 0;
        for (int i = 0; i < 16; ++i)
        {
            if (((child_types_ >> (i * 2)) & 0x3) != 0)
            {
                count++;
            }
        }
        return count;
    }
};

/**
 * Lightweight iterator for non-empty children in sparse offset array
 *
 * Designed for maximum performance - no virtual functions, minimal state.
 * Only iterates over branches that actually have children.
 * Converts self-relative offsets to absolute offsets on-the-fly.
 */
struct ChildIterator
{
    MemPtr<InnerNodeHeader> header;     // Memory-mapped header pointer
    const std::uint8_t* offsets_start;  // Byte pointer to relative offset array
    uint32_t remaining_mask;  // Bitmask of remaining children to visit
    int offset_index;         // Current index in sparse offset array

    // Static log partition getter for OLOGD support
    static LogPartition&
    get_log_partition()
    {
        return get_v2_memtree_log_partition();
    }

    ChildIterator(MemPtr<InnerNodeHeader> h, const uint8_t* offset_data)
        : header(h)
        , offsets_start(offset_data)
        , remaining_mask(0)
        , offset_index(0)
    {
        // Build initial mask of non-empty children
        const auto& header_val = header.get_uncopyable();

        // Overlay not implemented in the reader path yet
        assert(
            header_val.overlay_mask == 0 &&
            "overlay not implemented in iterator");
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
        const uint8_t* ptr;  // Direct memory pointer to child
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
        OLOGD(
            "next() called: remaining_mask=0x",
            std::hex,
            remaining_mask,
            std::dec,
            ", offset_index=",
            offset_index);

        // Find next set bit (next non-empty branch)
        int branch = catl::core::ctz(remaining_mask);  // Count trailing zeros

        OLOGD("  ctz returned branch=", branch);

        // DEBUG
        if (branch >= 16)
        {
            throw std::runtime_error(
                "ChildIterator: Invalid branch " + std::to_string(branch) +
                " from remaining_mask=0x" + std::to_string(remaining_mask));
        }

        // Bounds check
        if (offset_index >= 16)
        {
            throw std::runtime_error(
                "ChildIterator offset_index out of bounds: " +
                std::to_string(offset_index));
        }

        OLOGD(
            "  About to call resolve_self_relative with offsets_start=",
            static_cast<const void*>(offsets_start),
            ", offset_index=",
            offset_index);

        // Resolve self-relative offset to get child pointer
        const uint8_t* child_ptr =
            resolve_self_relative(offsets_start, offset_index);

        OLOGD(
            "  resolve_self_relative returned child_ptr=",
            static_cast<const void*>(child_ptr));

        // Use get_type to ensure we get the right type (ref in unsafe, value in
        // safe)
        const auto& header_val = header.get_uncopyable();

        Child child;
        child.branch = branch;
        child.type = header_val.get_child_type(branch);
        child.ptr = child_ptr;

        OLOGD(
            "  Created child struct: branch=",
            child.branch,
            ", type=",
            static_cast<int>(child.type),
            ", ptr=",
            static_cast<const void*>(child.ptr));

        // Clear this bit from remaining mask
        remaining_mask &= ~(1u << branch);
        ++offset_index;

        OLOGD("  About to return child");
        return child;
    }
};

/**
 * Lightweight view of a leaf node in mmap'd memory
 */
struct LeafView
{
    MemPtr<LeafHeader> header;
    Key key;
    Slice data;

    bool
    eq(const LeafView& other) const
    {
        return header.raw() == other.header.raw() ||
            get_hash().eq(other.get_hash());
    }

    Slice
    get_hash() const
    {
        return header.get_uncopyable().get_hash();
    }
};

/**
 * Lightweight view of an inner node in mmap'd memory
 */
struct InnerNodeView
{
    MemPtr<v2::InnerNodeHeader> header;  // Points directly into mmap

    // Get a child iterator on demand
    [[nodiscard]] v2::ChildIterator
    get_child_iter() const
    {
        const auto* offsets_data =
            header.offset(sizeof(v2::InnerNodeHeader)).raw();
        // Simple: just pass the header and offset array pointer
        return {header, offsets_data};
    }

    // Get child type for branch i (EMPTY, INNER, or LEAF)
    [[nodiscard]] v2::ChildType
    get_child_type(int branch) const
    {
        if (branch < 0 || branch >= 16)
        {
            throw std::out_of_range(
                "Branch index " + std::to_string(branch) +
                " out of range [0,16)");
        }
        const auto& header_val = header.get_uncopyable();
        return header_val.get_child_type(branch);
    }

    // Get pointer to child at branch i using SparseChildOffsets
    [[nodiscard]] const uint8_t*
    get_child_ptr(int branch) const
    {
        if (branch < 0 || branch >= 16)
        {
            throw std::out_of_range(
                "Branch index " + std::to_string(branch) +
                " out of range [0,16)");
        }

        // Get the child pointer (will return nullptr if empty)
        const uint8_t* child_ptr = get_sparse_offsets().get_child_ptr(branch);
        if (!child_ptr)
        {
            throw std::runtime_error(
                "No child at branch " + std::to_string(branch));
        }

        return child_ptr;
    }

    // Get a SparseChildOffsets accessor for this node
    [[nodiscard]] v2::SparseChildOffsets
    get_sparse_offsets() const
    {
        const auto& header_val = header.get_uncopyable();
        const uint8_t* offsets_base =
            header.offset(sizeof(v2::InnerNodeHeader)).raw();
        return {offsets_base, header_val.child_types};
    }
};

/**
 * Static operations for navigating memory-mapped v2 trees
 */
class MemTreeOps
{
public:
    /**
     * Get an inner node view from a pointer
     * Returns lightweight view with pointer into mmap data
     */
    [[nodiscard]] static InnerNodeView
    get_inner_node(const uint8_t* ptr)
    {
        return InnerNodeView{(v2::MemPtr<v2::InnerNodeHeader>(ptr))};
    }

    // In catl/v2/catl-v2-memtree.h, inside class MemTreeOps
    static Slice
    get_leaf_hash(const InnerNodeView& parent, int branch)
    {
        // Sanity: must be a leaf at this branch
        auto ct = parent.get_child_type(branch);
        if (ct != v2::ChildType::LEAF)
            throw std::runtime_error("get_leaf_hash: not a leaf");
        const uint8_t* leaf_ptr = parent.get_child_ptr(branch);
        v2::MemPtr<v2::LeafHeader> leaf_header_ptr(leaf_ptr);
        const auto& hdr = leaf_header_ptr.get_uncopyable();
        return hdr.get_hash();  // 32-byte slice
    }

    /**
     * Get an inner child from a node view
     */
    [[nodiscard]] static InnerNodeView
    get_inner_child(const InnerNodeView& parent, int branch)
    {
        auto child_type = parent.get_child_type(branch);
        if (child_type != v2::ChildType::INNER)
        {
            if (child_type == v2::ChildType::EMPTY)
            {
                throw std::runtime_error(
                    "No child at branch " + std::to_string(branch));
            }
            throw std::runtime_error(
                "Child at branch " + std::to_string(branch) +
                " is a leaf, not an inner node");
        }
        return get_inner_node(parent.get_child_ptr(branch));
    }

    /**
     * Get a leaf child from a node view
     */
    [[nodiscard]] static LeafView
    get_leaf_child(const InnerNodeView& parent, int branch)
    {
        auto child_type = parent.get_child_type(branch);
        if (child_type != v2::ChildType::LEAF)
        {
            if (child_type == v2::ChildType::EMPTY)
            {
                throw std::runtime_error(
                    "No child at branch " + std::to_string(branch));
            }
            throw std::runtime_error(
                "Child at branch " + std::to_string(branch) +
                " is an inner node, not a leaf");
        }

        const uint8_t* leaf_ptr = parent.get_child_ptr(branch);

        // Load leaf header using MemPtr
        v2::MemPtr<v2::LeafHeader> leaf_header_ptr(leaf_ptr);
        const auto& leaf_header =
            leaf_header_ptr.get_uncopyable();  // Force ref binding

        return LeafView{
            leaf_header_ptr,
            Key(leaf_header.key.data()),  // Now safe - ref in UNSAFE mode!
            Slice(
                leaf_header_ptr.offset(sizeof(v2::LeafHeader)).raw(),
                leaf_header.data_size())};
    }

    /**
     * Lookup a key in the state tree starting from a given inner node
     * @param root The root node to start from
     * @param key The key to search for
     * @return LeafView if found
     * @throws std::runtime_error if key not found
     */
    [[nodiscard]] static LeafView
    lookup_key(const InnerNodeView& root, const Key& key)
    {
        auto leaf_view = lookup_key_optional(root, key);
        if (!leaf_view)
        {
            throw std::runtime_error("Key not found");
        }
        return *leaf_view;
    }

    /**
     * Lookup a key in the state tree starting from a given inner node
     * @param root The root node to start from
     * @param key The key to search for
     * @return std::optional<LeafView>
     */
    [[nodiscard]] static std::optional<LeafView>
    lookup_key_optional(const InnerNodeView& root, const Key& key)
    {
        InnerNodeView current = root;
        const auto& root_header = root.header.get_uncopyable();
        int depth = root_header.get_depth();

        // Walk down the tree following the key nibbles
        while (true)
        {
            // Use shamap utility to extract nibble at current depth
            int nibble = shamap::select_branch(key, depth);

            // Check child type
            auto child_type = current.get_child_type(nibble);
            if (child_type == v2::ChildType::EMPTY)
            {
                return std::nullopt;
            }

            if (child_type == v2::ChildType::LEAF)
            {
                // Found a leaf, verify it's the right key
                auto leaf = get_leaf_child(current, nibble);
                if (std::memcmp(leaf.key.data(), key.data(), 32) == 0)
                {
                    return leaf;
                }
                return std::nullopt;
            }

            // It's an inner node, continue traversing
            current = get_inner_child(current, nibble);
            const auto& current_header = current.header.get_uncopyable();
            depth = current_header.get_depth();
        }
    }

    /**
     * Find the first leaf in depth-first order starting from given node
     *
     * Note: Uses recursion which is optimal here because:
     * - Max depth is bounded by key size (64 nibbles = 64 levels max)
     * - Stack usage is tiny (~8KB worst case vs 8MB default stack)
     * - CPU call stack is faster than heap-allocated stack (better cache
     * locality)
     * - Code is cleaner and compiler can optimize
     *
     * @param node The inner node to start from
     * @return LeafView of the first leaf found
     * @throws std::runtime_error if no leaf found (malformed tree)
     */
    [[nodiscard]] static LeafView
    first_leaf_depth_first(const InnerNodeView& node)
    {
        // Check each branch in order
        for (int i = 0; i < 16; ++i)
        {
            auto child_type = node.get_child_type(i);
            if (child_type == v2::ChildType::EMPTY)
            {
                continue;  // Skip empty branches
            }

            if (child_type == v2::ChildType::LEAF)
            {
                // Found a leaf!
                return get_leaf_child(node, i);
            }

            // It's an inner node, recurse
            auto inner_child = get_inner_child(node, i);
            // Recursive call will throw if no leaf found in subtree
            return first_leaf_depth_first(inner_child);
        }

        // No children at all or all empty - malformed tree
        throw std::runtime_error("No leaf found - malformed tree");
    }

    /**
     * Walk all leaf nodes in a tree, calling callback for each
     *
     * Uses iterative traversal with explicit stack to visit all leaves.
     * Callback can return false to stop traversal early.
     *
     * @param root Root node to start from
     * @param callback Function (Key, Slice) -> bool, return false to stop
     * @return Number of leaves visited
     */
    template <typename Callback>
    static size_t
    walk_leaves(const InnerNodeView& root, Callback&& callback)
    {
        struct StackEntry
        {
            InnerNodeView node;
            int next_branch;  // Next branch to explore (0-15)
        };

        // Use fixed-size stack (max tree depth is 64)
        StackEntry stack[64];
        int stack_top = 0;

        // Push root
        stack[stack_top++] = {root, 0};

        size_t leaves_visited = 0;

        while (stack_top > 0)
        {
            auto& entry = stack[stack_top - 1];

            // Find next non-empty child
            bool found_child = false;
            for (int i = entry.next_branch; i < 16; ++i)
            {
                auto child_type = entry.node.get_child_type(i);
                if (child_type != v2::ChildType::EMPTY)
                {
                    entry.next_branch =
                        i + 1;  // Next time, start from next branch
                    found_child = true;

                    if (child_type == v2::ChildType::LEAF)
                    {
                        // Process leaf
                        auto leaf = get_leaf_child(entry.node, i);
                        leaves_visited++;

                        // Call callback - stop if it returns false
                        if (!callback(leaf.key, leaf.data))
                        {
                            return leaves_visited;
                        }
                    }
                    else
                    {
                        // It's an inner node - push onto stack
                        if (stack_top >= 64)
                        {
                            throw std::runtime_error("Tree depth exceeds 64");
                        }
                        auto inner_child = get_inner_child(entry.node, i);
                        stack[stack_top++] = {inner_child, 0};
                    }
                    break;
                }
            }

            // If no more children at this level, pop
            if (!found_child)
            {
                stack_top--;
            }
        }

        return leaves_visited;
    }

    /**
     * Walk all leaf nodes starting from a raw pointer
     * Convenience method that creates the view first
     *
     * @param root_ptr Pointer to root inner node
     * @param callback Function (Key, Slice) -> bool, return false to stop
     * @return Number of leaves visited
     */
    template <typename Callback>
    static size_t
    walk_leaves_from_ptr(const uint8_t* root_ptr, Callback&& callback)
    {
        auto root = get_inner_node(root_ptr);
        return walk_leaves(root, std::forward<Callback>(callback));
    }
};
}  // namespace catl::v2
