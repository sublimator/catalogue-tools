#include "catl/hybrid-shamap-v2/hmap.h"
#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-node.h"

namespace catl::hybrid_shamap {

void
Hmap::set_root_materialized(HmapInnerNode* node)
{
    root_ = PolyNodePtr::adopt_materialized(node);
}

Hash256
Hmap::get_root_hash() const
{
    if (!root_)
    {
        return Hash256::zero();
    }
    return root_.get_hash();
}

}  // namespace catl::hybrid_shamap