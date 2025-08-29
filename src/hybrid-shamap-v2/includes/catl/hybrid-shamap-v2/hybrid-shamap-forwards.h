#pragma once

/**
 * Forward declarations for hybrid-shamap-v2
 * Include this file when you only need to declare pointers/references
 * to these types without needing their full definitions.
 */

// Forward declare shamap types
namespace catl::shamap {
enum class SetMode;
enum class SetResult;
}  // namespace catl::shamap

namespace catl::hybrid_shamap {

// Core classes
class PolyNodePtr;
class HMapNode;
class HmapInnerNode;
class HmapLeafNode;
class HmapPlaceholder;
class HmapPathFinder;
class Hmap;

// Type aliases that might be needed
using SetMode = catl::shamap::SetMode;
using SetResult = catl::shamap::SetResult;

}  // namespace catl::hybrid_shamap