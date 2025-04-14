#pragma once

#include "hasher/core-types.h"

/**
 * Helper function to select a branch based on a key and depth
 */
int
select_branch(const Key& key, int depth);
