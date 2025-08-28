#pragma once

/**
 * Bundled header for SHAMap with SerializedNode traits
 * This includes all SHAMap implementation and instantiates it for
 * SerializedNode Used as a precompiled header by utils-v2 and other v2 tools
 */

// Include the traits definition
#include "catl/v2/shamap-custom-traits.h"

// Include the header-only SHAMap implementation (includes all .cpp files)
#include "catl/shamap/shamap-header-only.h"

// Include the v1 reader header-only implementation (includes reader .cpp files)
#include "catl/v1/catl-v1-reader-header-only.h"

// Instantiate all templates with SerializedNode traits
// This generates the actual code for these specific template instantiations
INSTANTIATE_SHAMAP_NODE_TRAITS(SerializedNode);
INSTANTIATE_READER_SHAMAP_NODE_TRAITS(SerializedNode);