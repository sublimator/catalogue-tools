#pragma once

#include "catl/core/types.h"

namespace catl::shamap {
/**
 * Helper function to select a branch based on a key and depth
 */
int
select_branch(const Key& key, int depth);

int
find_divergence_depth(const Key& k1, const Key& k2, int start_depth);
}  // namespace catl::shamap