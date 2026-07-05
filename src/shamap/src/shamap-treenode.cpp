#include "catl/shamap/shamap-treenode.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap-options.h"
#include <atomic>

namespace catl::shamap {
//----------------------------------------------------------
// SHAMapTreeNode Implementation
//----------------------------------------------------------

// Explicit template instantiations for default traits
template class SHAMapTreeNodeT<DefaultNodeTraits>;

}  // namespace catl::shamap
