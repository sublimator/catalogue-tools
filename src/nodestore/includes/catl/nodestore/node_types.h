#pragma once

#include <cstdint>

namespace catl::nodestore {

/**
 * The types of node objects stored in the nodestore.
 *
 * Regular "hot" types are cached in memory, while "pinned" types
 * bypass the cache when stored.
 */
enum class node_type : std::uint32_t {
    hot_unknown = 0,
    hot_ledger = 1,
    hot_account_node = 3,
    hot_transaction_node = 4,
    hot_dummy = 512,  // an invalid or missing object

    // Uncached variants - these bypass the cache when stored
    pinned_account_node = 1003,
    pinned_transaction_node = 1004,
    pinned_ledger = 1005
};

/**
 * Convert node_type to string for display/logging
 */
inline const char*
node_type_to_string(node_type type)
{
    switch (type)
    {
        case node_type::hot_unknown:
            return "hot_unknown";
        case node_type::hot_ledger:
            return "hot_ledger";
        case node_type::hot_account_node:
            return "hot_account_node";
        case node_type::hot_transaction_node:
            return "hot_transaction_node";
        case node_type::hot_dummy:
            return "hot_dummy";
        case node_type::pinned_account_node:
            return "pinned_account_node";
        case node_type::pinned_transaction_node:
            return "pinned_transaction_node";
        case node_type::pinned_ledger:
            return "pinned_ledger";
        default:
            return "unknown";
    }
}

}  // namespace catl::nodestore
