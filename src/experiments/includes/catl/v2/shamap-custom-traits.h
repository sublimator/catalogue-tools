#pragma once

// Include the header-only version of shamap and reader so can instantiate the
// templates
#include "catl/shamap/shamap-header-only.h"
#include "catl/v1/catl-v1-reader-header-only.h"

/**
 * Custom traits for serialized SHAMap nodes that enable on-disk structural
 * sharing
 *
 * The SerializedNode traits work with Copy-on-Write (CoW) to track which nodes
 * have been written to disk:
 *
 * 1. When a node is created (via CoW or initial construction), it gets default
 * values:
 *    - node_offset = 0 (not yet written to disk)
 *    - processed = false (needs to be serialized)
 *
 * 2. When serializing a tree:
 *    - Nodes with processed=false are written to disk
 *    - After writing, processed=true and node_offset=file_position
 *    - Nodes with processed=true are skipped (already on disk)
 *
 * 3. During Copy-on-Write operations:
 *    - map.snapshot() marks the current state as immutable
 *    - Any modification creates a new path from root to modified leaf
 *    - All nodes on this path are NEW objects with processed=false
 *    - Unchanged subtrees keep their processed=true nodes
 *
 * This enables incremental serialization where each snapshot only writes
 * its changes, with unchanged nodes referenced by their disk offsets.
 */
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
