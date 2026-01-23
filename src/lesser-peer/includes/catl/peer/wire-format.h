#pragma once

#include <catl/core/types.h>
#include <cstdint>
#include <vector>

namespace catl::peer {

/**
 * SHAMap node wire format types
 *
 * These match rippled's wire-protocol identifiers used during serialization.
 */
enum class SHAMapWireType : uint8_t {
    Transaction = 0,   // Leaf node containing a transaction
    AccountState = 1,  // Leaf node containing account state
    Inner = 2,         // Uncompressed inner node (12+ children, all 16 hashes)
    CompressedInner = 3,     // Compressed inner node (< 12 children)
    TransactionWithMeta = 4  // Transaction with metadata (ledger data)
};

/**
 * Represents a child reference in a compressed inner node
 */
struct InnerNodeChild
{
    Hash256 hash;    // 32-byte hash of child node
    uint8_t branch;  // Branch number (0-15)
};

/**
 * Parse a compressed inner node from wire format
 *
 * Format: [hash1][branch1][hash2][branch2]...[hashN][branchN][wireType]
 *
 * @param data Pointer to node data
 * @param size Size of node data in bytes
 * @return Vector of child references, or empty if parse failed
 */
std::vector<InnerNodeChild>
parse_compressed_inner_node(const uint8_t* data, size_t size);

/**
 * Parse an uncompressed inner node from wire format
 *
 * Used when inner node has 12+ children. All 16 child hashes are stored
 * consecutively. Empty branches have zero hashes.
 *
 * Format: [hash0][hash1]...[hash15][wireType] = 513 bytes
 *
 * @param data Pointer to node data
 * @param size Size of node data in bytes (must be 513)
 * @return Vector of child references (only non-zero hashes), or empty if failed
 */
std::vector<InnerNodeChild>
parse_uncompressed_inner_node(const uint8_t* data, size_t size);

/**
 * Parse a transaction leaf node from wire format
 *
 * Format: [transaction_data][wireType=0]
 *
 * @param data Pointer to node data
 * @param size Size of node data in bytes
 * @return Slice containing transaction data (without wire type byte),
 *         or empty slice if parse failed
 */
Slice
parse_transaction_leaf_node(const uint8_t* data, size_t size);

/**
 * Determine the wire type of a node
 *
 * @param data Pointer to node data
 * @param size Size of node data in bytes
 * @return Wire type, or std::nullopt if invalid
 */
std::optional<SHAMapWireType>
get_wire_type(const uint8_t* data, size_t size);

}  // namespace catl::peer
