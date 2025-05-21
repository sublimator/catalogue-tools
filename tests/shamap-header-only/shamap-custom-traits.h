#pragma once

// Include the header-only version of shamap
#include "catl/shamap/shamap-header-only.h"

// Custom traits definition
struct CustomTestTraits
{
    uint64_t node_offset = 1337;
};

// Instantiate all templates with custom traits
INSTANTIATE_CUSTOM_TRAIT(CustomTestTraits);

// Define aliases for easier typing
using CustomSHAMap = catl::shamap::SHAMapT<CustomTestTraits>;
