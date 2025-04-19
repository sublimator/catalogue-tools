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

/**
 * Inner (branch) node in the SHAMap tree
 */
class SHAMapInnerNode : public SHAMapTreeNode
{
private:
    std::unique_ptr<NodeChildren> children_;
    uint8_t depth_ = 0;
    static LogPartition log_partition_;
    // CoW support
    int version_{0};  // TODO: make atomic or have clear reason not to
    bool do_cow_ = false;

public:
    explicit SHAMapInnerNode(uint8_t nodeDepth = 0);

    SHAMapInnerNode(bool isCopy, uint8_t nodeDepth, int initialVersion);

    bool
    is_leaf() const override;

    bool
    is_inner() const override;

    uint8_t
    get_depth() const;

    // Useful for debugging without static_cast<int> calls everywhere
    int
    get_depth_int() const;

    bool
    set_child(int branch, boost::intrusive_ptr<SHAMapTreeNode> const& child);

    boost::intrusive_ptr<SHAMapTreeNode>

    get_child(int branch) const;

    bool
    has_child(int branch) const;

    int
    get_branch_count() const;

    uint16_t
    get_branch_mask() const;

    boost::intrusive_ptr<SHAMapLeafNode>
    get_only_child_leaf() const;

    boost::intrusive_ptr<SHAMapLeafNode>
    first_leaf(const boost::intrusive_ptr<SHAMapInnerNode>& inner) const;

    boost::json::object
    trie_json(TrieJsonOptions options, SHAMapOptions const& shamap_options)
        const;

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

protected:
    friend class PathFinder;
    friend class SHAMap;

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
        const boost::intrusive_ptr<SHAMapInnerNode>& inner,
        const Key& index,
        int round,
        int skips) const;

    Hash256
    compute_skipped_hash_recursive(
        const SHAMapOptions& options,
        const boost::intrusive_ptr<SHAMapInnerNode>& inner,
        const Key& index,
        int round,
        int skips) const;

    // CoW support - only accessible to friends
    int
    get_version() const
    {
        return version_;  // .load(std::memory_order_acquire);
    }

    bool
    is_cow_enabled() const
    {
        return do_cow_;
    }

    boost::intrusive_ptr<SHAMapInnerNode>
    copy(int newVersion) const;

    boost::intrusive_ptr<SHAMapInnerNode>
    make_child(int depth) const;

    int
    select_branch_for_depth(const Key& key) const
    {
        return select_branch(key, depth_);
    }
};
