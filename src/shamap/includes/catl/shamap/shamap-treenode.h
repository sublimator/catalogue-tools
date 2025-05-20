#pragma once

#include "catl/core/types.h"
#include <atomic>

#include "catl/shamap/shamap-options.h"  // NO LOGGING INSIDE IMPLEMENTATIONS

namespace catl::shamap {
/**
 * Abstract base class for SHAMap tree nodes
 */
class SHAMapTreeNode
{
protected:
    Hash256 hash;
    bool hash_valid_ = false;
    mutable std::atomic<int> ref_count_{0};

public:
    virtual ~SHAMapTreeNode() = default;
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
    friend void
    intrusive_ptr_add_ref(const SHAMapTreeNode* p);
    friend void
    intrusive_ptr_release(const SHAMapTreeNode* p);
};
}  // namespace catl::shamap