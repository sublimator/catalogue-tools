#pragma once

#include "catl/core/types.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-treenode.h"
#include <boost/intrusive_ptr.hpp>

namespace catl::shamap {

template <typename Traits>
class SHAMapT;

template <typename Traits>
class PathFinderT;

/**
 * Leaf node in the SHAMap tree
 */
template <typename Traits = DefaultNodeTraits>
class SHAMapLeafNodeT : public SHAMapTreeNodeT<Traits>
{
private:
    boost::intrusive_ptr<MmapItem> item;
    SHAMapNodeType type;
    int version = -1;  // Version for CoW tracking

public:
    SHAMapLeafNodeT(boost::intrusive_ptr<MmapItem> i, SHAMapNodeType t);

    bool
    is_leaf() const override;

    bool
    is_inner() const override;

    void
    update_hash(SHAMapOptions const& options) override;

    boost::intrusive_ptr<MmapItem>
    get_item() const;

    SHAMapNodeType
    get_type() const;

    void
    set_version(int v)
    {
        version = v;
    }

    int
    get_version() const
    {
        return version;
    }

protected:
    template <typename T>
    friend class PathFinderT;

    template <typename T>
    friend class SHAMapT;

    // CoW support - only accessible to friends
    boost::intrusive_ptr<SHAMapLeafNodeT<Traits>>
    copy(int newVersion) const;
};

// Type alias for backward compatibility
using SHAMapLeafNode = SHAMapLeafNodeT<DefaultNodeTraits>;

}  // namespace catl::shamap
