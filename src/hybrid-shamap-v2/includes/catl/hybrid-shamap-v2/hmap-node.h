#pragma once

#include "catl/core/types.h"
#include <atomic>
#include <string>

#include "catl/v2/catl-v2-structs.h"

namespace catl::hybrid_shamap {

/**
 * Base class for all hybrid map nodes
 * The node type tells us WHAT the node is (Inner/Leaf/Placeholder)
 * while the PolyNodePtr tells us WHERE it lives (mmap vs heap)
 *
 * Supports boost::intrusive_ptr for reference counting of heap nodes.
 */
class HMapNode
{
private:
    mutable std::atomic<int> ref_count_{0};

protected:
    Hash256 hash_;
    bool hash_valid_ = false;

public:
    virtual ~HMapNode() = default;
    virtual v2::ChildType
    get_type() const = 0;

    // Hash support
    virtual void
    update_hash() = 0;

    const Hash256&
    get_hash()
    {
        if (!hash_valid_)
        {
            update_hash();
            hash_valid_ = true;
        }
        return hash_;
    }

    void
    invalidate_hash()
    {
        hash_valid_ = false;
    }

    // For debugging
    virtual std::string
    describe() const = 0;

    // Friend functions for intrusive_ptr support
    friend void
    intrusive_ptr_add_ref(const HMapNode* p)
    {
        p->ref_count_.fetch_add(1, std::memory_order_relaxed);
    }

    friend void
    intrusive_ptr_release(const HMapNode* p)
    {
        // acq_rel RMW instead of release + a standalone acquire fence: same
        // ordering, but TSan models the RMW acquire and not standalone fences.
        // (hybrid-shamap-v2 is single-threaded today; converted for consistency
        // with the SHAMap refcounts.)
        if (p->ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            delete p;
        }
    }
};

}  // namespace catl::hybrid_shamap