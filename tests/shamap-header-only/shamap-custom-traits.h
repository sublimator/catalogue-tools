#pragma once

// Include the header-only version of shamap
#include "catl/shamap/shamap-header-only.h"
#include <atomic>

// Forward declarations for type aliases
struct CustomTestTraits;
struct HookTestTraits;

// Type aliases for test traits
using CustomInnerNode = catl::shamap::SHAMapInnerNodeT<CustomTestTraits>;
using CustomLeafNode = catl::shamap::SHAMapLeafNodeT<CustomTestTraits>;
using HookInnerNode = catl::shamap::SHAMapInnerNodeT<HookTestTraits>;
using HookLeafNode = catl::shamap::SHAMapLeafNodeT<HookTestTraits>;

// Custom traits definition - no hooks
struct CustomTestTraits
{
    uint64_t node_offset = 1337;
};

// Global counters for testing hook invocations
namespace test_counters {
inline std::atomic<int> inner_copies{0};
inline std::atomic<int> leaf_copies{0};
inline std::atomic<int> last_source_offset{0};
inline std::atomic<int> last_copy_version{0};

inline void
reset()
{
    inner_copies = 0;
    leaf_copies = 0;
    last_source_offset = 0;
    last_copy_version = 0;
}
}  // namespace test_counters

// Custom traits with CoW hooks for testing
struct HookTestTraits
{
    uint64_t node_offset = 0;
    bool processed = false;

    // CoW hook for inner nodes
    void
    on_inner_node_copied(HookInnerNode* this_copy, const HookInnerNode* source)
    {
        test_counters::inner_copies++;

        // Reset state for the new copy
        this->processed = false;

        // Track what we copied from (for testing)
        test_counters::last_source_offset = source->node_offset;
        test_counters::last_copy_version = this_copy->get_version();
    }

    // CoW hook for leaf nodes
    void
    on_leaf_node_copied(HookLeafNode* this_copy, const HookLeafNode* source)
    {
        test_counters::leaf_copies++;

        // Reset state for the new copy
        this->processed = false;

        // Track what we copied from (for testing)
        test_counters::last_source_offset = source->node_offset;
        test_counters::last_copy_version = this_copy->get_version();
    }
};

// Instantiate all templates with custom traits
INSTANTIATE_SHAMAP_NODE_TRAITS(CustomTestTraits);
INSTANTIATE_SHAMAP_NODE_TRAITS(HookTestTraits);

// Define aliases for easier typing
using CustomSHAMap = catl::shamap::SHAMapT<CustomTestTraits>;
using HookSHAMap = catl::shamap::SHAMapT<HookTestTraits>;
