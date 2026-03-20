#pragma once

#include "catl/core/types.h"
#include "catl/xdata/protocol.h"
#include <boost/json.hpp>

namespace catl::xdata::json {

/// Options for parse_leaf.
struct ParseLeafOptions
{
    /// If true, skip first 4 bytes (hash prefix) before parsing.
    bool includes_prefix = true;
    /// If true, add a "blob" field with hex-encoded raw item data
    /// (excluding prefix and key). Enables round-trip verification.
    bool include_blob = false;
};

/**
 * Parse a single leaf node (account state/SLE) to JSON.
 *
 * Leaf format: [optional 4-byte prefix] + item data + 32-byte key
 * This function skips the prefix and trailing key, parsing only the item data.
 *
 * @param data The raw leaf node data
 * @param protocol Protocol definitions for parsing
 * @param opts Parsing options
 * @return Parsed JSON object
 * @throws std::runtime_error if data is malformed
 */
boost::json::value
parse_leaf(
    Slice const& data,
    Protocol const& protocol,
    ParseLeafOptions opts = {});

/// Backward-compatible overload.
inline boost::json::value
parse_leaf(
    Slice const& data,
    Protocol const& protocol,
    bool includes_prefix)
{
    return parse_leaf(data, protocol, {includes_prefix, false});
}

}  // namespace catl::xdata::json
