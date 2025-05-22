#pragma once

// Include the header-only version of shamap and reader so can instantiate the
// templates
#include "catl/shamap/shamap-header-only.h"
#include "catl/v1/catl-v1-reader-header-only.h"

// Custom traits definition for experiments
struct SerializedNode
{
    // offset of the node in the file, 0 is never going to be used as the header
    // will take that, so we can just use 0 a special value
    uint64_t node_offset = 0;
    bool processed = false;
};

// Instantiate all templates with custom traits
INSTANTIATE_SHAMAP_NODE_TRAITS(SerializedNode);
INSTANTIATE_READER_SHAMAP_NODE_TRAITS(SerializedNode);

// Define aliases for easier typing
using SHAMapS = catl::shamap::SHAMapT<SerializedNode>;
using SHAMapInnerNodeS = catl::shamap::SHAMapInnerNodeT<SerializedNode>;
using SHAMapLeafNodeS = catl::shamap::SHAMapLeafNodeT<SerializedNode>;
using SHAMapTreeNodeS = catl::shamap::SHAMapTreeNodeT<SerializedNode>;
