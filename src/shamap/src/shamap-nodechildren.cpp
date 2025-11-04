#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "catl/shamap/shamap-nodechildren.h"
#include "catl/shamap/shamap-treenode.h"
#include "catl/core/logger.h"
#include <memory>
#include <stdexcept>

namespace catl::shamap {

// External destructor logging partition
extern LogPartition destructor_log;

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

// Private constructor that allocates exactly capacity slots
template <typename Traits>
NodeChildrenT<Traits>::NodeChildrenT(uint8_t cap)
    : capacity_(cap), canonicalized_(true)  // Always canonicalized when using this ctor
{
    if (cap == 0 || cap > 16)
        throw std::invalid_argument("Invalid capacity for NodeChildrenT");

    // Allocate exact size array
    children_ = new boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>[cap]();

    // Initialize branch_to_index_ to all -1 (will be set by canonicalize())
    for (int i = 0; i < 16; i++)
    {
        branch_to_index_[i] = -1;
    }
}

template <typename Traits>
NodeChildrenT<Traits>::~NodeChildrenT()
{
    PLOGD(destructor_log, "~NodeChildrenT: count=", get_child_count(),
          ", canonical=", canonicalized_,
          ", refcount=", ref_count_.load(),
          ", capacity=", static_cast<int>(capacity_));
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
boost::intrusive_ptr<NodeChildrenT<Traits>>
NodeChildrenT<Traits>::canonicalize() const
{
    if (canonicalized_ || branch_mask_ == 0)
        return nullptr;  // Already canonical or empty

    int child_count = __builtin_popcount(branch_mask_);

    // No need to canonicalize if nearly full
    if (child_count >= 14)
        return nullptr;

    // Create NEW canonicalized NodeChildrenT with exact capacity
    auto result = boost::intrusive_ptr<NodeChildrenT<Traits>>(
        new NodeChildrenT<Traits>(child_count));  // Uses private constructor!

    result->branch_mask_ = branch_mask_;

    // Copy only non-null children
    int new_index = 0;
    for (int i = 0; i < 16; i++)
    {
        if (branch_mask_ & (1 << i))
        {
            result->children_[new_index] = children_[i];
            result->branch_to_index_[i] = new_index++;
        }
    }

    return result;
}

template <typename Traits>
boost::intrusive_ptr<NodeChildrenT<Traits>>
NodeChildrenT<Traits>::copy() const
{
    auto new_children = boost::intrusive_ptr<NodeChildrenT<Traits>>(
        new NodeChildrenT<Traits>());

    // Copy branch mask
    new_children->branch_mask_ = branch_mask_;

    // Always create a full non-canonicalized copy
    for (int i = 0; i < 16; i++)
    {
        if (branch_mask_ & (1 << i))
        {
            if (canonicalized_)
            {
                // For canonicalized nodes, use the mapping
                int index = branch_to_index_[i];
                if (index >= 0 && index < capacity_ && children_)
                {
                    new_children->children_[i] = children_[index];
                }
                else
                {
                    // Invalid index - this shouldn't happen but be defensive
                    continue;
                }
            }
            else
            {
                if (children_)
                {
                    new_children->children_[i] = children_[i];
                }
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
