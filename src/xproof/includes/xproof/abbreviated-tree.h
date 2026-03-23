#pragma once

// Instantiate SHAMap templates for AbbreviatedTreeTraits.
// Include this once in exactly one .cpp file that uses abbreviated trees.

#include <catl/shamap/shamap-header-only.h>
#include <catl/shamap/shamap.h>

INSTANTIATE_SHAMAP_NODE_TRAITS(catl::shamap::AbbreviatedTreeTraits);
