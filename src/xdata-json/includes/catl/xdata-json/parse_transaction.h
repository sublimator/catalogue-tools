#pragma once

#include "catl/core/types.h"
#include "catl/xdata/protocol.h"
#include <boost/json.hpp>

namespace catl::xdata::json {

/**
 * Parse a transaction leaf node with metadata to JSON.
 *
 * Transaction format: 4-byte prefix + VL-encoded tx + VL-encoded metadata +
 * 32-byte key Returns: {"tx": {...}, "meta": {...}}
 *
 * @param data The raw transaction node data
 * @param protocol Protocol definitions for parsing
 * @return Parsed JSON object with "tx" and "meta" fields
 * @throws std::runtime_error if data is malformed
 */
boost::json::value
parse_transaction(Slice const& data, Protocol const& protocol);

/**
 * Parse a transaction set leaf node (no metadata, no VL encoding).
 *
 * Wire format: raw tx STObject (no prefix)
 * Prefixed format: 4-byte prefix + raw tx STObject
 *
 * @param data The raw transaction set leaf node data
 * @param protocol Protocol definitions for parsing
 * @param includes_prefix If true, skip first 4 bytes
 * (HashPrefix::transactionID)
 * @return Parsed JSON object (just the transaction)
 * @throws std::runtime_error if data is malformed
 */
boost::json::value
parse_txset_transaction(
    Slice const& data,
    Protocol const& protocol,
    bool includes_prefix = false);

}  // namespace catl::xdata::json
