// This file exists solely to compile the shamap-serialized header
// as a precompiled unit that can be linked into tests
#include "catl/v2/shamap-serialized.h"

// The header already contains INSTANTIATE_SHAMAP_NODE_TRAITS(SerializedNode)
// which provides the explicit instantiation