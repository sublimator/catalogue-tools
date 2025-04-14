#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "hasher/shamap/shamap-nodechildren.h"
#include "hasher/shamap/shamap-treenode.h"

//----------------------------------------------------------
// NodeChildren Implementation
//----------------------------------------------------------

NodeChildren::NodeChildren() : capacity_(16), canonicalized_(false)
{
    // Allocate full array of 16 slots
    children_ = new boost::intrusive_ptr<SHAMapTreeNode>[16]();

    // Initialize branch mapping for direct indexing
    for (int i = 0; i < 16; i++)
    {
        branchToIndex_[i] = i;
    }
}

NodeChildren::~NodeChildren()
{
    delete[] children_;
}

boost::intrusive_ptr<SHAMapTreeNode>
NodeChildren::getChild(int branch) const
{
    if (branch < 0 || branch >= 16)
        return nullptr;

    if (!(branchMask_ & (1 << branch)))
        return nullptr;

    return children_[canonicalized_ ? branchToIndex_[branch] : branch];
}

void
NodeChildren::setChild(int branch, boost::intrusive_ptr<SHAMapTreeNode> child)
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
        branchMask_ |= (1 << branch);
    }
    else if (branchMask_ & (1 << branch))
    {
        children_[branch] = nullptr;
        branchMask_ &= ~(1 << branch);
    }
}

void
NodeChildren::canonicalize()
{
    if (canonicalized_ || branchMask_ == 0)
        return;

    int childCount = __builtin_popcount(branchMask_);

    // No need to canonicalize if nearly full
    if (childCount >= 14)
        return;

    // Create optimally sized array
    auto newChildren = new boost::intrusive_ptr<SHAMapTreeNode>[childCount];

    // Initialize lookup table (all -1)
    for (int i = 0; i < 16; i++)
    {
        branchToIndex_[i] = -1;
    }

    // Copy only non-null children
    int newIndex = 0;
    for (int i = 0; i < 16; i++)
    {
        if (branchMask_ & (1 << i))
        {
            newChildren[newIndex] = children_[i];
            branchToIndex_[i] = newIndex++;
        }
    }

    // Replace storage
    delete[] children_;
    children_ = newChildren;
    capacity_ = childCount;
    canonicalized_ = true;
}

std::unique_ptr<NodeChildren>
NodeChildren::copy() const
{
    auto newChildren = std::make_unique<NodeChildren>();

    // Copy branch mask
    newChildren->branchMask_ = branchMask_;

    // Always create a full non-canonicalized copy
    for (int i = 0; i < 16; i++)
    {
        if (branchMask_ & (1 << i))
        {
            if (canonicalized_)
            {
                newChildren->children_[i] = children_[branchToIndex_[i]];
            }
            else
            {
                newChildren->children_[i] = children_[i];
            }
        }
    }

    // Never copy the canonicalized state!
    newChildren->canonicalized_ = false;

    return newChildren;
}

const boost::intrusive_ptr<SHAMapTreeNode>&
NodeChildren::operator[](int branch) const
{
    static boost::intrusive_ptr<SHAMapTreeNode> nullPtr;

    if (branch < 0 || branch >= 16 || !(branchMask_ & (1 << branch)))
        return nullPtr;

    return children_[canonicalized_ ? branchToIndex_[branch] : branch];
}
