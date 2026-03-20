#pragma once

#include "catl/shamap/shamap-traits.h"
#include "catl/shamap/shamap-treenode.h"
#include <atomic>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <variant>

namespace catl::shamap {
/**
 * Memory-optimized container for SHAMapInnerNode children with iteration
 * support. Uses intrusive reference counting for thread-safe shared ownership.
 */
template <typename Traits = DefaultNodeTraits>
class NodeChildrenT
{
private:
    boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>* children_;  // Dynamic array
    uint16_t branch_mask_ = 0;          // Bit mask of active branches
    uint8_t capacity_ = 0;              // Actual allocation size
    bool canonicalized_ = false;        // Has this been optimized?
    int8_t branch_to_index_[16] = {0};  // Maps branch to array index

    // Placeholder hash storage — zero bytes for DefaultNodeTraits,
    // full PlaceholderHashes for AbbreviatedTreeTraits.
    [[no_unique_address]]
    std::conditional_t<
        has_placeholders_v<Traits>,
        PlaceholderHashes,
        std::monostate> placeholders_{};

    // Intrusive reference counting for thread-safe shared ownership
    mutable std::atomic<int> ref_count_{0};

    // Private constructor for canonicalize() - allocates exactly capacity slots
    explicit NodeChildrenT(uint8_t capacity);

public:
    // Iterator class for iterating through valid children
    class iterator
    {
    private:
        NodeChildrenT const* container_;
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
        using value_type = boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = const value_type&;

        iterator(NodeChildrenT const* container, int branch)
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
    NodeChildrenT();
    ~NodeChildrenT();

    // Core operations
    boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>
    get_child(int branch) const;
    void
    set_child(int branch, boost::intrusive_ptr<SHAMapTreeNodeT<Traits>> child);
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

    // ── Placeholder support (compile-time gated by Traits) ─────
    //
    // Placeholders are precomputed subtree hashes stored alongside the
    // children array. They represent pruned subtrees in abbreviated trees.
    // A branch is in exactly one of three states:
    //   - branch_mask_ set          → real child (use child->get_hash())
    //   - placeholder_mask_ set     → no child, use stored hash
    //   - neither                   → empty (hash = zero)
    //
    // Real children always supersede placeholders:
    //   - set_child() clears any placeholder on that branch
    //   - set_placeholder() is a no-op if a real child exists
    //
    // The placeholder storage is zero-cost for DefaultNodeTraits
    // (std::monostate via [[no_unique_address]]).

    /// Does this branch have a placeholder hash?
    bool
    has_placeholder(int branch) const
    {
        if constexpr (has_placeholders_v<Traits>)
            return placeholders_.has(branch);
        else
            return false;
    }

    /// Set a placeholder hash for a branch. No-op if a real child exists.
    void
    set_placeholder(int branch, Hash256 const& hash)
    {
        if constexpr (has_placeholders_v<Traits>)
        {
            if (has_child(branch))
                return;  // real child wins
            placeholders_.set(branch, hash);
        }
    }

    /// Clear a placeholder hash for a branch.
    void
    clear_placeholder(int branch)
    {
        if constexpr (has_placeholders_v<Traits>)
            placeholders_.clear(branch);
    }

    /// Get the placeholder hash for a branch.
    Hash256 const&
    get_placeholder(int branch) const
    {
        if constexpr (has_placeholders_v<Traits>)
            return placeholders_.get(branch);
        else
            return Hash256::zero();
    }

    /// Placeholder mask — which branches have placeholder hashes.
    uint16_t
    get_placeholder_mask() const
    {
        if constexpr (has_placeholders_v<Traits>)
            return placeholders_.mask;
        else
            return 0;
    }

    // Memory optimization - returns new canonicalized object or nullptr
    boost::intrusive_ptr<NodeChildrenT<Traits>>
    canonicalize() const;
    bool
    is_canonical() const
    {
        return canonicalized_;
    }

    // Check if this NodeChildren is shared (ref_count > 1)
    bool
    is_shared() const
    {
        return ref_count_.load(std::memory_order_acquire) > 1;
    }

    // For Copy-on-Write
    boost::intrusive_ptr<NodeChildrenT<Traits>>
    copy() const;

    // No copying
    NodeChildrenT(const NodeChildrenT&) = delete;
    NodeChildrenT&
    operator=(const NodeChildrenT&) = delete;

    // Intrusive reference counting support
    friend void
    intrusive_ptr_add_ref(const NodeChildrenT<Traits>* p)
    {
        p->ref_count_.fetch_add(1, std::memory_order_relaxed);
    }

    friend void
    intrusive_ptr_release(const NodeChildrenT<Traits>* p)
    {
        if (p->ref_count_.fetch_sub(1, std::memory_order_release) == 1)
        {
            std::atomic_thread_fence(std::memory_order_acquire);
            delete p;
        }
    }

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

    const boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>&
    operator[](int branch) const;
};

// Type alias for backward compatibility
using NodeChildren = NodeChildrenT<DefaultNodeTraits>;

}  // namespace catl::shamap