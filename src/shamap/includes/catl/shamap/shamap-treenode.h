#pragma once

#include "catl/core/types.h"
#include <atomic>

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
    Hash256 hash;
    bool hash_valid_ = false;
    mutable std::atomic<int> ref_count_{0};

public:
    virtual ~SHAMapTreeNodeT() = default;
    void
    invalidate_hash();
    virtual bool
    is_leaf() const = 0;
    virtual bool
    is_inner() const = 0;
    virtual void
    update_hash(
        const SHAMapOptions& options) = 0;  // NO LOGGING INSIDE IMPLEMENTATIONS
    const Hash256&
    get_hash(const SHAMapOptions& options);

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
    if (p->ref_count_.fetch_sub(1, std::memory_order_release) == 1)
    {
        std::atomic_thread_fence(std::memory_order_acquire);
        delete p;
    }
}

}  // namespace catl::shamap