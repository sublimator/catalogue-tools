#pragma once

#include "catl/core/types.h"
#include <array>
#include <cstdint>
#include <type_traits>
#include <variant>

namespace catl::shamap {

/**
 * Default empty traits for SHAMap nodes.
 * No placeholder support — zero overhead.
 */
struct DefaultNodeTraits
{
    static constexpr bool supports_placeholders = false;
};

/**
 * Traits for abbreviated/proof trees.
 * Enables placeholder hash storage on inner node children.
 */
struct AbbreviatedTreeTraits
{
    static constexpr bool supports_placeholders = true;
};

/**
 * Placeholder hash storage for inner node children.
 * Sparse array of precomputed subtree hashes for pruned branches.
 * Only compiled into NodeChildrenT when Traits::supports_placeholders is true.
 */
struct PlaceholderHashes
{
    uint16_t mask = 0;  // which branches have placeholder hashes
    std::array<Hash256, 16>
        hashes{};  // hash per branch (only valid if mask bit set)

    bool
    has(int branch) const
    {
        return (mask & (1 << branch)) != 0;
    }

    void
    set(int branch, Hash256 const& hash)
    {
        hashes[branch] = hash;
        mask |= (1 << branch);
    }

    void
    clear(int branch)
    {
        mask &= ~(1 << branch);
    }

    Hash256 const&
    get(int branch) const
    {
        return hashes[branch];
    }
};

/// Helper to detect placeholder support at compile time.
template <typename Traits>
inline constexpr bool has_placeholders_v = Traits::supports_placeholders;

}  // namespace catl::shamap