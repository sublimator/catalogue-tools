#pragma once

/**
 * Main include file for hybrid-shamap-v2
 * This is a convenience header that includes all the components
 */

// Core components
#include "catl/hybrid-shamap-v2/hmap-node.h"
#include "catl/hybrid-shamap-v2/poly-node-ptr.h"

// Node types
#include "catl/hybrid-shamap-v2/hmap-innernode.h"
#include "catl/hybrid-shamap-v2/hmap-leafnode.h"
#include "catl/hybrid-shamap-v2/hmap-placeholder.h"

// Operations
#include "catl/hybrid-shamap-v2/poly-node-operations.h"

// Navigation
#include "catl/hybrid-shamap-v2/hmap-pathfinder.h"

// Main tree class
#include "catl/hybrid-shamap-v2/hmap.h"

// Re-export commonly used types for convenience
namespace catl::hybrid_shamap {
using catl::shamap::SetMode;
using catl::shamap::SetResult;
}  // namespace catl::hybrid_shamap