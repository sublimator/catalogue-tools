#pragma once

#include "catl/core/types.h"
#include <atomic>
#include <string>

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
    enum class Type : uint8_t { INNER, LEAF, PLACEHOLDER };

    virtual ~HMapNode() = default;
    virtual Type
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
        if (p->ref_count_.fetch_sub(1, std::memory_order_release) == 1)
        {
            std::atomic_thread_fence(std::memory_order_acquire);
            delete p;
        }
    }
};

}  // namespace catl::hybrid_shamap