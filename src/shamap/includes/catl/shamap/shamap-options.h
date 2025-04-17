#pragma once
#include "shamap-impl.h"
#include <optional>

enum class SetResult {
    FAILED = 0,  // Operation failed
    ADD = 1,     // New item was added
    UPDATE = 2,  // Existing item was updated
};

enum class SetMode {
    ADD_ONLY,      // Fail if the item already exists
    UPDATE_ONLY,   // Fail if the item doesn't exist
    ADD_OR_UPDATE  // Allow either adding or updating
};

struct TrieJsonOptions
{
    bool key_as_hash = false;
    bool pretty = true;
};

enum class SHAMapHashImpl {
    reference,
    synthetic_inners,
};

enum class SyntheticInnersHashImpl { recursive_simple, stack_performant };

enum class SkippedInnersHashImpl { recursive_simple, stack_performant };

enum class TreeCollapseImpl { leafs_only, leafs_and_inners };

struct SHAMapOptions
{
    TreeCollapseImpl tree_collapse_impl = TreeCollapseImpl::leafs_only;
    SHAMapHashImpl hash_impl = SHAMapHashImpl::reference;
    std::optional<SyntheticInnersHashImpl> synthetic_inners_hash_impl;
    std::optional<SkippedInnersHashImpl> skipped_inners_hash_impl;
};

/**
 *
 * @param options options to validate
 * @throws std::runtime_error if options are invalid
 *
 * Validates the options passed to the SHAMap constructor.
 * Throws an exception if combinations of the options are invalid.
 */
void
validate_options(const SHAMapOptions& options);
