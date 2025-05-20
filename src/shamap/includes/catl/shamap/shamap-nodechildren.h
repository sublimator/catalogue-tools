#pragma once

#include "catl/shamap/shamap-treenode.h"
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>
#include <memory>

namespace catl::shamap {
/**
 * Memory-optimized container for SHAMapInnerNode children with iteration
 * support
 */
class NodeChildren
{
private:
    boost::intrusive_ptr<SHAMapTreeNode>* children_;  // Dynamic array
    uint16_t branch_mask_ = 0;          // Bit mask of active branches
    uint8_t capacity_ = 0;              // Actual allocation size
    bool canonicalized_ = false;        // Has this been optimized?
    int8_t branch_to_index_[16] = {0};  // Maps branch to array index

public:
    // Iterator class for iterating through valid children
    class iterator
    {
    private:
        NodeChildren const* container_;
        int current_branch_;

        // Find next valid branch
        void
        findNextValid()
        {
            while (current_branch_ < 16 &&
                   !(container_->branch_mask_ & (1 << current_branch_)))
            {
                ++current_branch_;
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
            : container_(container), current_branch_(branch)
        {
            findNextValid();
        }

        reference
        operator*() const
        {
            if (container_->canonicalized_)
            {
                return container_
                    ->children_[container_->branch_to_index_[current_branch_]];
            }
            else
            {
                return container_->children_[current_branch_];
            }
        }

        pointer
        operator->() const
        {
            if (container_->canonicalized_)
            {
                return &container_->children_
                            [container_->branch_to_index_[current_branch_]];
            }
            else
            {
                return &container_->children_[current_branch_];
            }
        }

        iterator&
        operator++()
        {
            ++current_branch_;
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
                current_branch_ == other.current_branch_;
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
            return current_branch_;
        }
    };

    // Constructor - always starts with full 16 slots
    NodeChildren();
    ~NodeChildren();

    // Core operations
    boost::intrusive_ptr<SHAMapTreeNode>
    get_child(int branch) const;
    void
    set_child(int branch, boost::intrusive_ptr<SHAMapTreeNode> child);
    bool
    has_child(int branch) const
    {
        return (branch_mask_ & (1 << branch)) != 0;
    }
    int
    get_child_count() const
    {
        return __builtin_popcount(branch_mask_);
    }
    uint16_t
    get_branch_mask() const
    {
        return branch_mask_;
    }

    // Memory optimization
    void
    canonicalize();
    bool
    is_canonical() const
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

    const boost::intrusive_ptr<SHAMapTreeNode>&
    operator[](int branch) const;
};
}  // namespace catl::shamap