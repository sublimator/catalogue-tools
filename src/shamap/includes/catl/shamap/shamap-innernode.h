#pragma once

#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-nodechildren.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-options.h"
#include "catl/shamap/shamap-treenode.h"
#include <atomic>
#include <boost/json/object.hpp>
#include <memory>

#include "catl/core/logger.h"
#include "shamap-utils.h"

namespace catl::shamap {

template <typename Traits>
class SHAMapT;

template <typename Traits>
class PathFinderT;

// Forward declaration for the NodeChildrenT
template <typename Traits>
class NodeChildrenT;

/**
 * Inner (branch) node in the SHAMap tree
 */
template <typename Traits = DefaultNodeTraits>
class SHAMapInnerNodeT : public SHAMapTreeNodeT<Traits>
{
private:
    NodeChildrenT<Traits>* children_;  // Plain pointer - spinlock protects it
    mutable std::atomic_flag children_lock_ =
        ATOMIC_FLAG_INIT;  // Simple spinlock
    uint8_t depth_ = 0;
    static LogPartition log_partition_;
    // CoW support
    int version_{0};  // TODO: make atomic or have clear reason not to
    bool do_cow_ = false;

protected:
    // Thread-safe helpers for children access using spinlock
    // OWNERSHIP MODEL: SHAMapInnerNode OWNS one reference to its children_

    boost::intrusive_ptr<NodeChildrenT<Traits>>
    get_children() const
    {
        // Acquire spinlock
        while (children_lock_.test_and_set(std::memory_order_acquire))
        {
            // Spin - could add pause instruction for better performance
        }

        // Now safe to access plain pointer - spinlock protects it
        auto* ptr = children_;  // Plain load
        if (ptr)
        {
            intrusive_ptr_add_ref(ptr);  // Add caller's reference
        }

        // Release spinlock
        children_lock_.clear(std::memory_order_release);

        // Return with 'false' to not add another reference
        return boost::intrusive_ptr<NodeChildrenT<Traits>>(ptr, false);
    }

    void
    set_children(
        const boost::intrusive_ptr<NodeChildrenT<Traits>>& new_children)
    {
        auto* new_ptr = new_children.get();

        // Add OUR ownership reference to the new children
        if (new_ptr)
        {
            intrusive_ptr_add_ref(new_ptr);
        }

        // Acquire spinlock
        while (children_lock_.test_and_set(std::memory_order_acquire))
        {
            // Spin
        }

        // Simple pointer swap while holding lock
        auto* old_ptr = children_;
        children_ = new_ptr;  // Plain assignment

        // Release spinlock
        children_lock_.clear(std::memory_order_release);

        // Release OUR ownership reference to the old children (outside lock)
        if (old_ptr)
        {
            intrusive_ptr_release(old_ptr);
        }
    }

public:
    explicit SHAMapInnerNodeT(uint8_t nodeDepth = 0);

    SHAMapInnerNodeT(bool isCopy, uint8_t nodeDepth, int initialVersion);

    ~SHAMapInnerNodeT();

    bool
    is_leaf() const override;

    bool
    is_inner() const override;

    uint8_t
    get_depth() const;

    void
    set_depth(uint8_t depth);

    // Useful for debugging without static_cast<int> calls everywhere
    int
    get_depth_int() const;

    bool
    set_child(
        int branch,
        boost::intrusive_ptr<SHAMapTreeNodeT<Traits>> const& child);

    boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>
    get_child(int branch) const;

    bool
    has_child(int branch) const;

    int
    get_branch_count() const;

    uint16_t
    get_branch_mask() const;

    boost::intrusive_ptr<SHAMapLeafNodeT<Traits>>
    get_only_child_leaf() const;

    boost::intrusive_ptr<SHAMapLeafNodeT<Traits>>
    first_leaf(
        const boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& inner) const;

    boost::json::object
    trie_json(TrieJsonOptions options, SHAMapOptions const& shamap_options)
        const;

    void
    invalidate_hash_recursive();

    static LogPartition&
    get_log_partition()
    {
        return log_partition_;
    }

    void
    set_version(int v)
    {
        version_ = v;  //.store(v, std::memory_order_release);
    }

    void
    enable_cow(bool enable)
    {
        do_cow_ = enable;
    }

    int
    get_version() const
    {
        return version_;  // .load(std::memory_order_acquire);
    }

    // Get serialized size - inner nodes are 512 bytes (16 * 32), or 0 if empty
    size_t
    serialized_size() const override
    {
        // Empty inner nodes (no children) don't need to be stored
        // They all hash to the same zero hash
        if (get_branch_count() == 0)
        {
            return 0;
        }
        return 512;  // 16 hashes * 32 bytes each
    }

    // Serialize inner node to buffer for storage
    // Format: 16 concatenated hashes (32 bytes each), using zero hash for empty
    // children Returns number of bytes written (always 512)
    size_t
    write_to_buffer(uint8_t* ptr) const override;

    // nodestore::inner_node_source concept methods
    // These enable direct concept-based compression, eliminating the
    // serialize→compress step for inner nodes

    /**
     * Get child hash by branch index for nodestore compression.
     * Returns child's hash if present, or zero hash if no child.
     * Required by inner_node_source concept.
     */
    Hash256 const&
    get_node_source_child_hash(int branch) const;

    /**
     * Get bitmask of populated branches for nodestore compression.
     * Required by inner_node_source concept.
     *
     * IMPORTANT: Converts from SHAMap internal format to rippled/xahaud
     * canonical format!
     * - SHAMap internal: branch i = bit i
     * - Rippled/xahaud canonical: branch i = bit (15 - i)
     *
     * This matches the reference implementation in rippled where the serializer
     * iterates: for (unsigned bit = 0x8000; bit; bit >>= 1)  // bits 15, 14,
     * 13, ..., 0 read hash for branch 0, 1, 2, ..., 15
     */
    uint16_t
    get_node_source_branch_mask() const
    {
        uint16_t shamap_mask = get_branch_mask();
        uint16_t canonical_mask = 0;

        // Convert: branch i (bit i in SHAMap) → bit (15-i) in canonical format
        for (int i = 0; i < 16; ++i)
        {
            if (shamap_mask & (1 << i))
                canonical_mask |= (1 << (15 - i));
        }

        return canonical_mask;
    }

    /**
     * Get this node's hash for use as nodestore key.
     * Required by inner_node_source concept.
     */
    Hash256 const&
    get_node_source_hash() const
    {
        return this->valid_hash_or_throw();
    }

protected:
    template <typename T>
    friend class PathFinderT;

    template <typename T>
    friend class SHAMapT;

    void
    update_hash_reference(const SHAMapOptions& options);

    void
    update_hash_collapsed(const SHAMapOptions& options);

    void
    update_hash(const SHAMapOptions& options) override
    {
        if (options.tree_collapse_impl == TreeCollapseImpl::leafs_only &&
            options.reference_hash_impl !=
                ReferenceHashImpl::use_synthetic_inners)
        {
            update_hash_reference(options);
        }
        else
        {
            update_hash_collapsed(options);
        }
    }

    Hash256
    compute_skipped_hash_stack(
        const SHAMapOptions& options,
        const boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& inner,
        const Key& index,
        int round,
        int skips) const;

    Hash256
    compute_skipped_hash_recursive(
        const SHAMapOptions& options,
        const boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& inner,
        const Key& index,
        int round,
        int skips) const;

    // CoW support - only accessible to friends

    bool
    is_cow_enabled() const
    {
        return do_cow_;
    }

    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
    copy(int newVersion, SHAMapInnerNodeT<Traits>* parent = nullptr) const;

    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
    make_child(int depth) const;

    int
    select_branch_for_depth(const Key& key) const
    {
        return select_branch(key, depth_);
    }
};

// Type alias for backward compatibility
using SHAMapInnerNode = SHAMapInnerNodeT<DefaultNodeTraits>;

// Define the static log partition declaration for all template instantiations
template <typename Traits>
LogPartition SHAMapInnerNodeT<Traits>::log_partition_("SHAMapInnerNode");

}  // namespace catl::shamap