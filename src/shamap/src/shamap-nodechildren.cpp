#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "catl/shamap/shamap-nodechildren.h"
#include "catl/shamap/shamap-treenode.h"
#include <memory>
#include <stdexcept>

namespace catl::shamap {
//----------------------------------------------------------
// NodeChildrenT Implementation
//----------------------------------------------------------

template <typename Traits>
NodeChildrenT<Traits>::NodeChildrenT() : capacity_(16), canonicalized_(false)
{
    // Allocate full array of 16 slots
    children_ = new boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>[16]();

    // Initialize branch mapping for direct indexing
    for (int i = 0; i < 16; i++)
    {
        branch_to_index_[i] = i;
    }
}

template <typename Traits>
NodeChildrenT<Traits>::~NodeChildrenT()
{
    delete[] children_;
}

template <typename Traits>
boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>
NodeChildrenT<Traits>::get_child(int branch) const
{
    if (branch < 0 || branch >= 16)
        return nullptr;

    if (!(branch_mask_ & (1 << branch)))
        return nullptr;

    return children_[canonicalized_ ? branch_to_index_[branch] : branch];
}

template <typename Traits>
void
NodeChildrenT<Traits>::set_child(
    int branch,
    boost::intrusive_ptr<SHAMapTreeNodeT<Traits>> child)
{
    if (branch < 0 || branch >= 16)
        return;

    if (canonicalized_)
    {
        // IMPORTANT: Canonicalized nodes are immutable!
        // This should never happen if used correctly
        throw std::runtime_error("Attempted to modify a canonicalized node");
    }

    // Only non-canonicalized nodes can be modified
    if (child)
    {
        children_[branch] = child;
        branch_mask_ |= (1 << branch);
    }
    else if (branch_mask_ & (1 << branch))
    {
        children_[branch] = nullptr;
        branch_mask_ &= ~(1 << branch);
    }
}

template <typename Traits>
void
NodeChildrenT<Traits>::canonicalize()
{
    if (canonicalized_ || branch_mask_ == 0)
        return;

    int child_count = __builtin_popcount(branch_mask_);

    // No need to canonicalize if nearly full
    if (child_count >= 14)
        return;

    // Create optimally sized array
    auto new_children =
        new boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>[child_count];

    // Initialize lookup table (all -1)
    for (int i = 0; i < 16; i++)
    {
        branch_to_index_[i] = -1;
    }

    // Copy only non-null children
    int new_index = 0;
    for (int i = 0; i < 16; i++)
    {
        if (branch_mask_ & (1 << i))
        {
            new_children[new_index] = children_[i];
            branch_to_index_[i] = new_index++;
        }
    }

    // Replace storage
    delete[] children_;
    children_ = new_children;
    capacity_ = child_count;
    canonicalized_ = true;
}

template <typename Traits>
std::unique_ptr<NodeChildrenT<Traits>>
NodeChildrenT<Traits>::copy() const
{
    auto new_children = std::make_unique<NodeChildrenT<Traits>>();

    // Copy branch mask
    new_children->branch_mask_ = branch_mask_;

    // Always create a full non-canonicalized copy
    for (int i = 0; i < 16; i++)
    {
        if (branch_mask_ & (1 << i))
        {
            if (canonicalized_)
            {
                new_children->children_[i] = children_[branch_to_index_[i]];
            }
            else
            {
                new_children->children_[i] = children_[i];
            }
        }
    }

    // Never copy the canonicalized state!
    new_children->canonicalized_ = false;

    return new_children;
}

template <typename Traits>
const boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>&
NodeChildrenT<Traits>::operator[](int branch) const
{
    static boost::intrusive_ptr<SHAMapTreeNodeT<Traits>> nullPtr;

    if (branch < 0 || branch >= 16 || !(branch_mask_ & (1 << branch)))
        return nullPtr;

    return children_[canonicalized_ ? branch_to_index_[branch] : branch];
}

// Explicit template instantiations for default traits
template class NodeChildrenT<DefaultNodeTraits>;

}  // namespace catl::shamap
