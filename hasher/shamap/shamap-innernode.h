#pragma once

#include "hasher/shamap/shamap-leafnode.h"
#include "hasher/shamap/shamap-nodechildren.h"
#include "hasher/shamap/shamap-nodetype.h"
#include "hasher/shamap/shamap-treenode.h"
#include <atomic>
#include <memory>

/**
 * Inner (branch) node in the SHAMap tree
 */
class SHAMapInnerNode : public SHAMapTreeNode
{
private:
    std::unique_ptr<NodeChildren> children_;
    uint8_t depth_ = 0;

    // CoW support
    int version{0};  // TODO: make atomic or have clear reason not to
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
    void
    update_hash() override;
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

protected:
    friend class PathFinder;
    friend class SHAMap;

    // CoW support - only accessible to friends
    int
    get_version() const
    {
        return version;  // .load(std::memory_order_acquire);
    }
    void
    set_version(int v)
    {
        version = v;  //.store(v, std::memory_order_release);
    }
    bool
    is_cow_enabled() const
    {
        return do_cow_;
    }
    void
    enable_cow(bool enable)
    {
        do_cow_ = enable;
    }
    boost::intrusive_ptr<SHAMapInnerNode>
    copy(int newVersion) const;
};