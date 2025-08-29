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
 * Size: 16 bytes (same as std::shared_ptr)
 *
 * Usage: PolyNodePtr instances typically have short lifetimes - created
 * temporarily during traversal/manipulation then destroyed. The main exceptions
 * are class members like Hmap::root_ which hold long-term references to the
 * tree structure.
 */
class PolyNodePtr
{
private:
    void* ptr_;           // Raw pointer (could point anywhere)
    v2::ChildType type_;  // What type of node (empty/inner/leaf/hash)
    bool materialized_;   // true = heap (needs ref counting), false = mmap

    // Helper to increment ref count if materialized
    void
    add_ref() const;

    // Helper to decrement ref count if materialized
    void
    release() const;

public:
    // Default constructor - creates empty reference
    PolyNodePtr()
        : ptr_(nullptr), type_(v2::ChildType::EMPTY), materialized_(false)
    {
    }

    // Constructor with all fields
    PolyNodePtr(void* p, v2::ChildType t, bool m);

    // Copy constructor - increments ref count if materialized
    PolyNodePtr(const PolyNodePtr& other);

    // Move constructor - takes ownership, no ref count change
    PolyNodePtr(PolyNodePtr&& other) noexcept;

    // Destructor - decrements ref count if materialized
    ~PolyNodePtr();

    // Copy assignment
    PolyNodePtr&
    operator=(const PolyNodePtr& other);

    // Move assignment
    PolyNodePtr&
    operator=(PolyNodePtr&& other) noexcept;

    // Factory methods
    static PolyNodePtr
    make_raw_memory(const void* p, v2::ChildType type = v2::ChildType::INNER)
    {
        // Note: does NOT call add_ref since it's not materialized
        PolyNodePtr tp;
        tp.ptr_ = const_cast<void*>(p);
        tp.type_ = type;
        tp.materialized_ = false;
        return tp;
    }

    static PolyNodePtr
    make_empty()
    {
        return {};  // null/empty reference
    }

    // New factory methods with clearer semantics
    static PolyNodePtr
    adopt_materialized(HMapNode* node);
    static PolyNodePtr
    wrap_uncounted(HMapNode* p, v2::ChildType type);

    // Factory from intrusive_ptr - takes a ref-counted pointer
    static PolyNodePtr
    from_intrusive(const boost::intrusive_ptr<HMapNode>& p);

    // Convert to intrusive_ptr (only valid for MATERIALIZED)
    [[nodiscard]] boost::intrusive_ptr<HMapNode>
    to_intrusive() const;

    // Accessors (inline for performance)
    [[nodiscard]] void*
    get_raw_ptr() const
    {
        return ptr_;
    }

    [[nodiscard]] const uint8_t*
    get_raw_memory() const
    {
        assert(!materialized_);  // Should be mmap memory
        return static_cast<const uint8_t*>(ptr_);
    }

    [[nodiscard]] HMapNode*
    get_materialized_base() const
    {
        assert(materialized_);  // Should be heap memory
        return static_cast<HMapNode*>(ptr_);
    }

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

    /**
     * Get a MemPtr of specified type pointing to raw memory
     * Only valid for non-materialized (mmap) nodes
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
     * Get typed pointer to materialized node
     * Only valid for materialized (heap) nodes
     */
    template <typename T>
    [[nodiscard]] T*
    get_materialized() const
    {
        assert(materialized_);  // Should only be used for heap nodes
        assert(ptr_ != nullptr);
        return static_cast<T*>(ptr_);
    }

    /**
     * Copy hash to a buffer (works for all node types)
     * @param dest Buffer to copy 32 bytes to
     */
    void
    copy_hash_to(uint8_t* dest) const;

    /**
     * Get hash as Hash256 (involves a copy)
     */
    [[nodiscard]] Hash256
    get_hash() const;
};

}  // namespace catl::hybrid_shamap