#include "catl/shamap/shamap-treenode.h"

//----------------------------------------------------------
// SHAMapTreeNode Implementation
//----------------------------------------------------------

void
intrusive_ptr_add_ref(const SHAMapTreeNode* p)
{
    p->refCount_.fetch_add(1, std::memory_order_relaxed);
}

void
intrusive_ptr_release(const SHAMapTreeNode* p)
{
    if (p->refCount_.fetch_sub(1, std::memory_order_release) == 1)
    {
        std::atomic_thread_fence(std::memory_order_acquire);
        delete p;
    }
}

void
SHAMapTreeNode::invalidate_hash()
{
    hashValid = false;
}

const Hash256&
SHAMapTreeNode::get_hash(SHAMapOptions const& options)
{
    if (!hashValid)
    {
        update_hash(options);
        hashValid = true;
    }
    return hash;
}
