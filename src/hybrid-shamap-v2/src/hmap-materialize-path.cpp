#include "catl/core/logger.h"
#include "catl/hybrid-shamap-v2/hmap-pathfinder.h"
#include "catl/hybrid-shamap-v2/hmap.h"

namespace catl::hybrid_shamap {

bool
Hmap::materialize_path(const Key& key)
{
    LOGD("[materialize_path] Materializing path for key: ", key.hex());

    if (!root_)
    {
        LOGD("[materialize_path] Empty tree");
        return false;
    }

    // Use the simple static materialization method
    // This only converts mmap nodes to heap nodes, no structural changes
    auto materialized_root =
        HmapPathFinder::materialize_path_for_key(root_, key, -1);

    // Update root if it changed
    if (materialized_root != root_)
    {
        LOGD(
            "[materialize_path] Root changed for key ",
            key.hex().substr(0, 16),
            "... - updating root pointer");
        root_ = materialized_root;
    }

    LOGD("[materialize_path] Path materialization complete");
    return true;
}

}  // namespace catl::hybrid_shamap