#pragma once

#include "catl/core/types.h"
#include <atomic>

#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-options.h"  // NO LOGGING INSIDE IMPLEMENTATIONS
#include "catl/shamap/shamap-traits.h"

namespace catl::shamap {

// Forward declaration
template <typename Traits = DefaultNodeTraits>
class SHAMapTreeNodeT;

/**
 * Abstract base class for SHAMap tree nodes
 */
template <typename Traits>
class SHAMapTreeNodeT : public Traits
{
protected:
    mutable std::atomic<int> ref_count_{0};

public:
    virtual ~SHAMapTreeNodeT() = default;
    virtual void
    invalidate_hash() = 0;
    virtual bool
    is_leaf() const = 0;
    virtual bool
    is_inner() const = 0;
    virtual Hash256
    get_hash(const SHAMapOptions& options) = 0;

    virtual Hash256
    valid_hash_or_throw() const = 0;

    // Get the serialized size of this node
    virtual size_t
    serialized_size() const = 0;

    // Serialize node to buffer for storage
    // Returns number of bytes written
    virtual size_t
    write_to_buffer(uint8_t* ptr) const = 0;

    // friend declarations needed for boost::intrusive_ptr
    template <typename T>
    friend void
    intrusive_ptr_add_ref(const SHAMapTreeNodeT<T>* p);

    template <typename T>
    friend void
    intrusive_ptr_release(const SHAMapTreeNodeT<T>* p);
};

// Type alias for backward compatibility
using SHAMapTreeNode = SHAMapTreeNodeT<DefaultNodeTraits>;

// Template functions for intrusive_ptr support
template <typename Traits>
void
intrusive_ptr_add_ref(const SHAMapTreeNodeT<Traits>* p)
{
    p->ref_count_.fetch_add(1, std::memory_order_relaxed);
}

template <typename Traits>
void
intrusive_ptr_release(const SHAMapTreeNodeT<Traits>* p)
{
    // acq_rel on the RMW rather than release + a standalone acquire fence:
    // same ordering guarantee for the delete, but ThreadSanitizer models the
    // RMW's acquire and does NOT model standalone atomic_thread_fence, so this
    // stays race-clean under TSan (shared nodes are released concurrently by
    // the builder and hasher threads of a CoW snapshot).
    if (p->ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1)
    {
        delete p;
    }
}

}  // namespace catl::shamap
