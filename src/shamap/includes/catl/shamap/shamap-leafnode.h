#pragma once

#include "catl/core/types.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-treenode.h"
#include <boost/intrusive_ptr.hpp>

/**
 * Leaf node in the SHAMap tree
 */
class SHAMapLeafNode : public SHAMapTreeNode
{
private:
    boost::intrusive_ptr<MmapItem> item;
    SHAMapNodeType type;
    int version = -1;  // Version for CoW tracking

public:
    SHAMapLeafNode(boost::intrusive_ptr<MmapItem> i, SHAMapNodeType t);
    bool
    is_leaf() const override;
    bool
    is_inner() const override;
    void
    update_hash() override;
    boost::intrusive_ptr<MmapItem>
    get_item() const;
    SHAMapNodeType
    get_type() const;

protected:
    friend class PathFinder;
    friend class SHAMap;

    // CoW support - only accessible to friends
    boost::intrusive_ptr<SHAMapLeafNode>
    copy() const;
    int
    get_version() const
    {
        return version;
    }
    void
    set_version(int v)
    {
        version = v;
    }
};
