#pragma once

#include "hasher/shamap/shamap-treenode.h"
#include <boost/intrusive_ptr.hpp>

/**
 * Memory-optimized container for SHAMapInnerNode children with iteration
 * support
 */
class NodeChildren
{
private:
    boost::intrusive_ptr<SHAMapTreeNode>* children_;  // Dynamic array
    uint16_t branchMask_ = 0;         // Bit mask of active branches
    uint8_t capacity_ = 0;            // Actual allocation size
    bool canonicalized_ = false;      // Has this been optimized?
    int8_t branchToIndex_[16] = {0};  // Maps branch to array index

public:
    // Iterator class for iterating through valid children
    class iterator
    {
    private:
        NodeChildren const* container_;
        int currentBranch_;

        // Find next valid branch
        void
        findNextValid()
        {
            while (currentBranch_ < 16 &&
                   !(container_->branchMask_ & (1 << currentBranch_)))
            {
                ++currentBranch_;
            }
        }

    public:
        // Standard iterator type definitions
        using iterator_category = std::forward_iterator_tag;
        using value_type = boost::intrusive_ptr<SHAMapTreeNode>;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = const value_type&;

        iterator(NodeChildren const* container, int branch)
            : container_(container), currentBranch_(branch)
        {
            findNextValid();
        }

        reference
        operator*() const
        {
            if (container_->canonicalized_)
            {
                return container_
                    ->children_[container_->branchToIndex_[currentBranch_]];
            }
            else
            {
                return container_->children_[currentBranch_];
            }
        }

        pointer
        operator->() const
        {
            if (container_->canonicalized_)
            {
                return &container_->children_
                            [container_->branchToIndex_[currentBranch_]];
            }
            else
            {
                return &container_->children_[currentBranch_];
            }
        }

        iterator&
        operator++()
        {
            ++currentBranch_;
            findNextValid();
            return *this;
        }

        iterator
        operator++(int)
        {
            iterator temp = *this;
            ++(*this);
            return temp;
        }

        bool
        operator==(const iterator& other) const
        {
            return container_ == other.container_ &&
                currentBranch_ == other.currentBranch_;
        }

        bool
        operator!=(const iterator& other) const
        {
            return !(*this == other);
        }

        // Get the current branch index (useful for knowing which child)
        int
        branch() const
        {
            return currentBranch_;
        }
    };

    // Constructor - always starts with full 16 slots
    NodeChildren();
    ~NodeChildren();

    // Core operations
    boost::intrusive_ptr<SHAMapTreeNode>
    getChild(int branch) const;
    void
    setChild(int branch, boost::intrusive_ptr<SHAMapTreeNode> child);
    bool
    hasChild(int branch) const
    {
        return (branchMask_ & (1 << branch)) != 0;
    }
    int
    getChildCount() const
    {
        return __builtin_popcount(branchMask_);
    }
    uint16_t
    getBranchMask() const
    {
        return branchMask_;
    }

    // Memory optimization
    void
    canonicalize();
    bool
    isCanonical() const
    {
        return canonicalized_;
    }

    // For Copy-on-Write
    std::unique_ptr<NodeChildren>
    copy() const;

    // No copying
    NodeChildren(const NodeChildren&) = delete;
    NodeChildren&
    operator=(const NodeChildren&) = delete;

    // Iteration support - iterates only through non-empty children
    iterator
    begin() const
    {
        return iterator(this, 0);
    }
    iterator
    end() const
    {
        return iterator(this, 16);
    }

    // Array-like access (both const and non-const versions)
    const boost::intrusive_ptr<SHAMapTreeNode>&
    operator[](int branch) const;
    boost::intrusive_ptr<SHAMapTreeNode>&
    operator[](int branch);
};