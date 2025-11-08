#pragma once

#include "catl/core/types.h"
#include "catl/nodestore/node_blob.h"
#include <optional>

namespace catl::nodestore {

/**
 * Simple backend interface for nodestore operations.
 * Implementations can wrap NuDB, RocksDB, or any other KV store.
 */
class Backend
{
public:
    virtual ~Backend() = default;

    /**
     * Fetch a node by its hash.
     * Returns nullopt if key not found.
     *
     * @param key The hash of the node to fetch
     * @return Compressed node_blob if found, nullopt otherwise
     */
    virtual std::optional<node_blob>
    get(Hash256 const& key) = 0;

    /**
     * Store a node by its hash.
     *
     * @param key The hash of the node (used as key)
     * @param blob The compressed node_blob to store
     */
    virtual void
    store(Hash256 const& key, node_blob const& blob) = 0;
};

}  // namespace catl::nodestore
