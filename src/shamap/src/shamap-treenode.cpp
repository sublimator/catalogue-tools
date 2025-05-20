#include "catl/shamap/shamap-treenode.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap-options.h"
#include <atomic>

namespace catl::shamap {
//----------------------------------------------------------
// SHAMapTreeNode Implementation
//----------------------------------------------------------

// Template implementations are now in the header file,
// but we need to implement the methods defined outside the class

template <typename Traits>
void
SHAMapTreeNodeT<Traits>::invalidate_hash()
{
    hash_valid_ = false;
    hash = Hash256::zero();
}

template <typename Traits>
const Hash256&
SHAMapTreeNodeT<Traits>::get_hash(SHAMapOptions const& options)
{
    if (!hash_valid_)
    {
        update_hash(options);
        hash_valid_ = true;
    }
    return hash;
}

// Explicit template instantiations for default traits
template class SHAMapTreeNodeT<DefaultNodeTraits>;

}  // namespace catl::shamap