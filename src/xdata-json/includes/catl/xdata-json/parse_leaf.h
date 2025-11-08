#pragma once

#include "catl/core/types.h"
#include "catl/xdata/protocol.h"
#include <boost/json.hpp>

namespace catl::xdata::json {

/**
 * Parse a single leaf node (account state/SLE) to JSON.
 *
 * Leaf format: 4-byte prefix + item data + 32-byte key
 * This function skips the prefix and trailing key, parsing only the item data.
 *
 * @param data The raw leaf node data (with prefix and key)
 * @param protocol Protocol definitions for parsing
 * @return Parsed JSON object
 * @throws std::runtime_error if data is malformed
 */
boost::json::value
parse_leaf(Slice const& data, Protocol const& protocol);

}  // namespace catl::xdata::json
