#pragma once

#include "catl/core/types.h"
#include "catl/xdata/protocol.h"
#include <boost/json.hpp>

namespace catl::xdata {

/**
 * Parse a transaction leaf node with metadata to JSON.
 *
 * Nodestore format: [4-byte prefix][VL tx][VL meta][32-byte key]
 * Wire format:      [VL tx][VL meta][32-byte key]
 *
 * Returns: {"hash": "...", "tx": {...}, "meta": {...}}
 *
 * @param data The raw transaction node data
 * @param protocol Protocol definitions for parsing
 * @param includes_prefix If true (default), skip first 4 bytes (hash prefix)
 * @return Parsed JSON object with "hash", "tx" and "meta" fields
 * @throws std::runtime_error if data is malformed
 */
/// Options for parse_transaction.
struct ParseTransactionOptions
{
    bool includes_prefix = true;
    /// If true, add a "blob" field with hex-encoded raw item data
    /// (VL tx + VL meta, excluding prefix and key).
    bool include_blob = false;
};

boost::json::value
parse_transaction(
    Slice const& data,
    Protocol const& protocol,
    ParseTransactionOptions opts = {});

/// Backward-compatible overload.
inline boost::json::value
parse_transaction(
    Slice const& data,
    Protocol const& protocol,
    bool includes_prefix)
{
    return parse_transaction(data, protocol, {includes_prefix, false});
}

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

}  // namespace catl::xdata
