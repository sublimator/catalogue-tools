#pragma once
#include <optional>

namespace catl::shamap {
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

enum class ReferenceHashImpl {
    recursive_simple,
    stack_performant,
    use_synthetic_inners,  // handles a normal tree using collapse_tree
};

enum class SyntheticInnersHashImpl { recursive_simple, stack_performant };

enum class SkippedInnersHashImpl { recursive_simple, stack_performant };

enum class TreeCollapseImpl { leafs_only, leafs_and_inners };

struct SHAMapOptions
{
    TreeCollapseImpl tree_collapse_impl = TreeCollapseImpl::leafs_and_inners;
    ReferenceHashImpl reference_hash_impl = ReferenceHashImpl::recursive_simple;
    SyntheticInnersHashImpl synthetic_inners_hash_impl =
        SyntheticInnersHashImpl::stack_performant;
    SkippedInnersHashImpl skipped_inners_hash_impl =
        SkippedInnersHashImpl::stack_performant;
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
}  // namespace catl::shamap