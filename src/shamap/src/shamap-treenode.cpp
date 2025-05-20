#include "catl/shamap/shamap-treenode.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap-options.h"
#include <atomic>

namespace catl::shamap {
//----------------------------------------------------------
// SHAMapTreeNode Implementation
//----------------------------------------------------------

void
intrusive_ptr_add_ref(const SHAMapTreeNode* p)
{
    p->ref_count_.fetch_add(1, std::memory_order_relaxed);
}

void
intrusive_ptr_release(const SHAMapTreeNode* p)
{
    if (p->ref_count_.fetch_sub(1, std::memory_order_release) == 1)
    {
        std::atomic_thread_fence(std::memory_order_acquire);
        delete p;
    }
}

void
SHAMapTreeNode::invalidate_hash()
{
    hash_valid_ = false;
    hash = Hash256::zero();
}

const Hash256&
SHAMapTreeNode::get_hash(SHAMapOptions const& options)
{
    if (!hash_valid_)
    {
        update_hash(options);
        hash_valid_ = true;
    }
    return hash;
}
}  // namespace catl::shamap