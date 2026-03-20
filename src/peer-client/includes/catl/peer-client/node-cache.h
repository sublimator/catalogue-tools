#pragma once

#include <catl/core/types.h>

#include <cstdint>
#include <map>
#include <optional>
#include <span>
#include <vector>

namespace catl::peer_client {

/**
 * Simple in-memory cache for SHAMap node data, keyed by content hash.
 *
 * Unbounded for now. Nodes are immutable (content-addressed by hash),
 * so cached data never goes stale.
 *
 * TODO: LRU eviction, size cap, stats
 */
class NodeCache
{
    std::map<Hash256, std::vector<uint8_t>> cache_;

public:
    /// Store node data by its content hash.
    void
    put(Hash256 const& hash, std::span<const uint8_t> data)
    {
        cache_.try_emplace(hash, data.begin(), data.end());
    }

    /// Retrieve node data by hash. Returns empty span if not cached.
    std::span<const uint8_t>
    get(Hash256 const& hash) const
    {
        auto it = cache_.find(hash);
        if (it == cache_.end())
            return {};
        return {it->second.data(), it->second.size()};
    }

    /// Check if a hash is cached.
    bool
    has(Hash256 const& hash) const
    {
        return cache_.contains(hash);
    }

    /// Number of cached nodes.
    size_t
    size() const
    {
        return cache_.size();
    }

    /// Total bytes stored (node data only, not keys).
    size_t
    bytes() const
    {
        size_t total = 0;
        for (auto const& [_, data] : cache_)
            total += data.size();
        return total;
    }

    /// Clear all cached data.
    void
    clear()
    {
        cache_.clear();
    }
};

}  // namespace catl::peer_client
