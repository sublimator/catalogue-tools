#pragma once

#include "catl/core/types.h"
#include "catl/hybrid-shamap-v2/hybrid-shamap-forwards.h"
#include "catl/v2/catl-v2-memtree.h"  // For MemPtr
#include "catl/v2/catl-v2-structs.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cassert>
#include <cstdint>

namespace catl::hybrid_shamap {

/**
 * PolyNodePtr - A polymorphic smart pointer to nodes that can be either:
 * - In mmap memory (no ref counting, non-owning)
 * - In heap memory (with ref counting, shared ownership)
 *
 * Acts like a shared_ptr for materialized nodes, and a raw pointer for mmap
 * nodes. It explicitly stores metadata since we can't use bit-tagging with
 * unaligned pointers from #pragma pack(1).
 *
 * Size: 16 bytes (8 bytes pointer + 4 bytes type enum + 1 byte bool + 3 bytes
 * padding)
 *
 * ## Core Design Philosophy:
 * This class bridges the gap between two worlds:
 * 1. MMAP nodes from CATL files (read-only, no ownership)
 * 2. Heap nodes created at runtime (mutable, ref-counted)
 *
 * ## Ownership Semantics:
 * - MMAP nodes: No ownership, just a view into mapped memory
 * - Heap nodes: Shared ownership via intrusive reference counting
 *
 * ## Common Usage Patterns:
 *
 * ### Tree Navigation (HmapPathFinder::find_path):
 * ```cpp
 * PolyNodePtr current = root;
 * // Navigate through mixed mmap/heap nodes transparently
 * if (current.is_raw_memory()) {
 *     // Handle mmap node
 * } else {
 *     // Handle heap node
 * }
 * ```
 *
 * ### Node Creation (Hmap::set_item):
 * ```cpp
 * // Create new node and transfer ownership to PolyNodePtr
 * auto* node = new HmapInnerNode(depth);
 * PolyNodePtr ptr = PolyNodePtr::adopt_materialized(node);
 * ```
 *
 * ### Child Management (HmapInnerNode):
 * - Stores raw void* pointers internally for compactness
 * - Creates PolyNodePtr lazily in get_child()
 * - Manually manages ref counts in set_child() and destructor
 *
 * ## Reference Counting Rules:
 * - Constructor with materialized=true: INCREMENTS ref count
 * - Copy constructor: INCREMENTS if materialized
 * - Move constructor: TRANSFERS ownership (no increment)
 * - Destructor: DECREMENTS if materialized
 * - adopt_materialized(): INCREMENTS (takes ownership)
 * - wrap_uncounted(): DOES NOT increment (advanced use)
 *
 * ## Thread Safety:
 * - Reference counting is atomic
 * - Node access is NOT thread-safe (external synchronization required)
 */
class PolyNodePtr
{
private:
    void* ptr_;           // Raw pointer (could point anywhere)
    v2::ChildType type_;  // What type of node (empty/inner/leaf/hash)
    bool materialized_;   // true = heap (needs ref counting), false = mmap

    /**
     * Helper to increment ref count if materialized
     *
     * Called by:
     * - Constructor when materialized=true
     * - Copy constructor
     * - Copy assignment (for new reference)
     *
     * No-op for mmap nodes or empty pointers.
     * Uses intrusive_ptr_add_ref() which atomically increments.
     */
    void
    add_ref() const;

    /**
     * Helper to decrement ref count if materialized
     *
     * Called by:
     * - Destructor
     * - Copy/move assignment (for old reference)
     *
     * No-op for mmap nodes or empty pointers.
     * Uses intrusive_ptr_release() which atomically decrements
     * and deletes the node if ref count reaches zero.
     */
    void
    release() const;

public:
    // Default constructor - creates empty reference
    PolyNodePtr()
        : ptr_(nullptr), type_(v2::ChildType::EMPTY), materialized_(false)
    {
    }

    /**
     * Constructor with all fields - THE CORE CONSTRUCTOR
     *
     * @param p Raw pointer to the node (mmap or heap)
     * @param t Type of node (INNER, LEAF, PLACEHOLDER, EMPTY)
     * @param m Whether this is materialized (heap) or not (mmap)
     *
     * CRITICAL: If m=true, this WILL increment the reference count!
     * This is the source of truth for ref counting behavior.
     *
     * Used by:
     * - adopt_materialized() with m=true to take ownership
     * - HmapInnerNode::get_child() to create temporary views
     * - from_intrusive() to convert from intrusive_ptr
     */
    PolyNodePtr(void* p, v2::ChildType t, bool m);

    /**
     * Copy constructor - Creates a new reference to the same node
     *
     * If the node is materialized (heap), increments ref count.
     * If the node is mmap, just copies the pointer (no ownership).
     *
     * Common in:
     * - Returning PolyNodePtr from functions
     * - Storing in containers
     * - Path traversal in HmapPathFinder
     */
    PolyNodePtr(const PolyNodePtr& other);

    /**
     * Move constructor - Transfers ownership without ref count change
     *
     * The source becomes empty after the move.
     * Efficient for returning from functions and temporary objects.
     *
     * Used heavily in:
     * - Factory methods returning new PolyNodePtr instances
     * - Path vector operations in HmapPathFinder
     */
    PolyNodePtr(PolyNodePtr&& other) noexcept;

    /**
     * Destructor - Releases reference if materialized
     *
     * For materialized nodes: decrements ref count, potentially deleting
     * For mmap nodes: no-op (no ownership)
     */
    ~PolyNodePtr();

    /**
     * Copy assignment - Replaces current reference with new one
     *
     * Releases old reference (if any), acquires new reference.
     * Handles self-assignment correctly.
     *
     * Used in:
     * - Hmap::set_root() to update root node
     * - Path updates during materialization
     */
    PolyNodePtr&
    operator=(const PolyNodePtr& other);

    /**
     * Move assignment - Transfers ownership from another PolyNodePtr
     *
     * Releases old reference, takes ownership from source.
     * Source becomes empty after the move.
     */
    PolyNodePtr&
    operator=(PolyNodePtr&& other) noexcept;

    // ==================== FACTORY METHODS ====================

    /**
     * Creates a PolyNodePtr for an mmap (raw memory) node
     *
     * @param p Pointer into mmap'd CATL file
     * @param type Node type (usually determined from parent or header)
     *
     * NO REFERENCE COUNTING - This is just a non-owning view!
     *
     * Used extensively in:
     * - HmapPathFinder::navigate_raw_inner() when traversing mmap nodes
     * - HmapInnerNode::materialize_raw_node() to wrap child pointers
     * - poly_first_leaf() to return mmap leaf nodes
     * - TreeWalker for read-only traversal
     *
     * Example:
     * ```cpp
     * const uint8_t* child_ptr = view.get_child_ptr(branch);
     * PolyNodePtr child = PolyNodePtr::wrap_raw_memory(child_ptr, child_type);
     * ```
     */
    static PolyNodePtr
    wrap_raw_memory(const void* p, v2::ChildType type = v2::ChildType::INNER)
    {
        // Note: does NOT call add_ref since it's not materialized
        PolyNodePtr tp;
        tp.ptr_ = const_cast<void*>(p);
        tp.type_ = type;
        tp.materialized_ = false;
        return tp;
    }

    /**
     * Creates an empty/null PolyNodePtr
     *
     * Used for:
     * - Default initialization
     * - Representing empty tree branches
     * - Clearing references
     *
     * Example in HmapPathFinder:
     * ```cpp
     * found_leaf_ = PolyNodePtr::make_empty();  // Clear any previous result
     * ```
     */
    static PolyNodePtr
    make_empty()
    {
        return {};  // null/empty reference
    }

    /**
     * RECOMMENDED: Takes ownership of a newly created node
     *
     * @param node Newly allocated node (typically from 'new')
     * @return PolyNodePtr that owns the node (ref count incremented)
     *
     * This is THE PREFERRED way to create PolyNodePtr for new nodes!
     * - Auto-detects type from node->get_type()
     * - Increments ref count (takes ownership)
     * - Clear semantics: "adopt" = take ownership
     *
     * Used throughout for node creation:
     * - Hmap::set_item(): `adopt_materialized(new HmapLeafNode(key, data))`
     * - Hmap constructor: `adopt_materialized(new HmapInnerNode(0))`
     * - HmapPathFinder::add_node_at_divergence(): for new divergence nodes
     *
     * Example:
     * ```cpp
     * root_ = PolyNodePtr::adopt_materialized(new HmapInnerNode(0));
     * ```
     */
    static PolyNodePtr
    adopt_materialized(HMapNode* node);

    // ==================== ACCESSORS ====================

    /**
     * Get the raw pointer value (for debugging/comparison)
     *
     * Returns the actual pointer regardless of type.
     * Mainly used for pointer equality comparisons.
     */
    [[nodiscard]] void*
    get_raw_ptr() const
    {
        return ptr_;
    }

    /**
     * Get pointer as raw bytes for mmap nodes
     *
     * @return Pointer suitable for reading mmap'd CATL data
     *
     * ASSERTS if called on materialized node!
     *
     * Used by:
     * - HmapPathFinder::navigate_raw_inner() to read mmap headers
     * - TreeWalker for traversing mmap nodes
     * - poly_first_leaf() to navigate mmap inner nodes
     */
    [[nodiscard]] const uint8_t*
    get_raw_memory() const
    {
        assert(!materialized_);  // Should be mmap memory
        return static_cast<const uint8_t*>(ptr_);
    }

    /**
     * Get base pointer for materialized nodes
     *
     * @return HMapNode* base pointer for heap nodes
     *
     * ASSERTS if called on mmap node!
     *
     * Used by:
     * - add_ref() and release() for reference counting
     * - Type checking in navigation code
     * - Debug/logging to call describe()
     */
    [[nodiscard]] HMapNode*
    get_materialized_base() const
    {
        assert(materialized_);  // Should be heap memory
        return static_cast<HMapNode*>(ptr_);
    }

    /**
     * Get the node type (INNER, LEAF, PLACEHOLDER, or EMPTY)
     *
     * This is stored explicitly since we can't use virtual functions
     * on mmap nodes and can't use bit-tagging with unaligned pointers.
     *
     * Used everywhere for node type dispatch.
     */
    [[nodiscard]] v2::ChildType
    get_type() const
    {
        return type_;
    }

    // Type checks (inline for performance)
    [[nodiscard]] bool
    is_raw_memory() const
    {
        return !materialized_ && ptr_ != nullptr;
    }
    [[nodiscard]] bool
    is_materialized() const
    {
        return materialized_;
    }
    [[nodiscard]] bool
    is_empty() const
    {
        return ptr_ == nullptr || type_ == v2::ChildType::EMPTY;
    }
    [[nodiscard]] bool
    is_inner() const
    {
        return type_ == v2::ChildType::INNER;
    }
    [[nodiscard]] bool
    is_leaf() const
    {
        return type_ == v2::ChildType::LEAF;
    }
    [[nodiscard]] bool
    is_placeholder() const
    {
        return type_ == v2::ChildType::PLACEHOLDER;
    }

    // Equality (based on pointer only, not metadata)
    bool
    operator==(const PolyNodePtr& other) const
    {
        return ptr_ == other.ptr_;
    }
    bool
    operator!=(const PolyNodePtr& other) const
    {
        return ptr_ != other.ptr_;
    }

    // Bool conversion
    explicit operator bool() const
    {
        return !is_empty();
    }

    // ==================== TEMPLATE ACCESSORS ====================

    /**
     * Get a MemPtr of specified type for reading mmap node headers
     *
     * @tparam T The header type (v2::InnerNodeHeader or v2::LeafHeader)
     * @return MemPtr<T> for reading packed CATL structures
     *
     * CRITICAL: Only valid for mmap nodes (asserts if materialized)!
     *
     * The MemPtr allows safe reading of #pragma pack(1) structures
     * that may be unaligned in memory.
     *
     * Used extensively for mmap node access:
     * - Reading InnerNodeHeader to get depth, child types, sparse offsets
     * - Reading LeafHeader to get key and data size
     * - Accessing perma-cached hashes from mmap headers
     *
     * Example:
     * ```cpp
     * auto header = node.get_memptr<v2::InnerNodeHeader>();
     * int depth = header->get_depth();
     * ```
     */
    template <typename T>
    [[nodiscard]] v2::MemPtr<T>
    get_memptr() const
    {
        assert(!materialized_);  // Should only be used for mmap nodes
        assert(ptr_ != nullptr);
        return v2::MemPtr<T>(static_cast<const uint8_t*>(ptr_));
    }

    /**
     * Get typed pointer to a specific materialized node class
     *
     * @tparam T The node class (HmapInnerNode, HmapLeafNode, HmapPlaceholder)
     * @return T* pointer to the specific node type
     *
     * CRITICAL: Only valid for heap nodes (asserts if mmap)!
     *
     * This does a static_cast, so caller must ensure the type is correct
     * (usually by checking is_inner(), is_leaf(), etc. first).
     *
     * Used throughout for accessing heap node functionality:
     * - HmapInnerNode: get_depth(), get_child(), set_child(), first_leaf_key()
     * - HmapLeafNode: get_key(), get_data()
     * - HmapPlaceholder: get_hash()
     *
     * Example:
     * ```cpp
     * if (node.is_inner() && node.is_materialized()) {
     *     auto* inner = node.get_materialized<HmapInnerNode>();
     *     int depth = inner->get_depth();
     * }
     * ```
     */
    template <typename T>
    [[nodiscard]] T*
    get_materialized() const
    {
        assert(materialized_);  // Should only be used for heap nodes
        assert(ptr_ != nullptr);
        return static_cast<T*>(ptr_);
    }

    // ==================== HASH OPERATIONS ====================

    /**
     * Copy hash to a buffer (works for all node types)
     *
     * @param dest Buffer to copy 32 bytes to
     *
     * Works for both mmap and heap nodes:
     * - Mmap: Reads perma-cached hash from header
     * - Heap: Calls get_hash() which may trigger update_hash()
     *
     * Used by parent nodes when computing their own hashes.
     */
    void
    copy_hash_to(uint8_t* dest) const;

    /**
     * Get hash as Hash256 object
     *
     * @return Copy of the node's hash
     *
     * Convenience method that allocates a Hash256.
     * For performance-critical code, use copy_hash_to() instead.
     *
     * Used in:
     * - HmapInnerNode::update_hash() for child hashes
     * - Hmap::get_root_hash() for the tree's root hash
     * - compute_synthetic_hash() for collapsed tree support
     */
    [[nodiscard]] Hash256
    get_hash() const;
};

}  // namespace catl::hybrid_shamap