#pragma once

#include "catl/core/types.h"
#include <cstdint>
#include <optional>
#include <string>

namespace catl::common {

/**
 * LedgerInfo - Canonical representation of Ripple/Xahau ledger information
 *
 * This structure matches the canonical binary serialization format used in the
 * Ripple/Xahau network protocol. This is the standard format for ledger headers
 * as they appear on the network, in contrast to format-specific storage
 * representations (e.g., the CATL v1 format which uses a different layout).
 *
 * IMPORTANT: This structure represents data in HOST byte order. The actual
 * network protocol uses big-endian (network byte order) for multi-byte fields.
 * The LedgerInfoView class handles the conversion from network to host byte
 * order.
 */
#pragma pack(push, 1)
struct LedgerInfo  // NOLINT(*-pro-type-member-init)
{
    // The sequence number of this ledger
    uint32_t seq;

    // The total number of drops in existence
    uint64_t drops;

    // Hash of the previous ledger
    Hash256 parent_hash;

    // Hash of the transaction tree
    Hash256 tx_hash;

    // Hash of the state tree
    Hash256 account_hash;

    // When the previous ledger closed (Ripple epoch time)
    uint32_t parent_close_time;

    // When this ledger closed (Ripple epoch time)
    uint32_t close_time;

    // Resolution of close time (seconds)
    uint8_t close_time_resolution;

    // Flags indicating how this ledger closed
    uint8_t close_flags;

    // Hash of this ledger (computed from the above fields)
    // Optional because ledger headers can be serialized with or without hash
    std::optional<Hash256> hash;

    /**
     * Convert to human-readable string representation
     */
    std::string
    to_string() const;
};
#pragma pack(pop)

/**
 * LedgerInfoView - Zero-copy view into canonically serialized ledger
 * information
 *
 * This class provides a zero-copy view into binary data that follows the
 * canonical Ripple/Xahau network serialization format for ledger headers. This
 * format is:
 *
 * - seq:                  32 bits (big-endian)
 * - drops:                64 bits (big-endian)
 * - parent_hash:         256 bits (byte array, no endianness)
 * - tx_hash:             256 bits (byte array, no endianness)
 * - account_hash:        256 bits (byte array, no endianness)
 * - parent_close_time:    32 bits (big-endian)
 * - close_time:           32 bits (big-endian)
 * - close_time_resolution: 8 bits (single byte, no endianness)
 * - close_flags:           8 bits (single byte, no endianness)
 * - hash:                 256 bits (byte array, no endianness)
 *
 * This class automatically converts multi-byte integer fields from network
 * byte order (big-endian) to host byte order when reading.
 */
class LedgerInfoView
{
public:
    // Size constants for ledger header serialization
    static constexpr size_t HEADER_SIZE_WITHOUT_HASH = 118;
    static constexpr size_t HEADER_SIZE_WITH_HASH = 150;

private:
    const uint8_t* data;  // Raw pointer to header data
    size_t size_;         // Size of the header data

public:
    /**
     * Constructor with raw data pointer
     *
     * @param header_data Pointer to the start of the ledger header data
     * @param size Size of the header data (defaults to full size with hash)
     */
    explicit LedgerInfoView(
        const uint8_t* header_data,
        size_t size = HEADER_SIZE_WITH_HASH);

    /**
     * Get the sequence number
     */
    uint32_t
    seq() const;

    /**
     * Get the drops amount
     */
    uint64_t
    drops() const;

    /**
     * Get the parent ledger hash
     */
    Hash256
    parent_hash() const;

    /**
     * Get the transaction tree hash
     */
    Hash256
    tx_hash() const;

    /**
     * Get the account state tree hash
     */
    Hash256
    account_hash() const;

    /**
     * Get the parent close time
     */
    uint32_t
    parent_close_time() const;

    /**
     * Get the close time
     */
    uint32_t
    close_time() const;

    /**
     * Get the close time resolution
     */
    uint8_t
    close_time_resolution() const;

    /**
     * Get the close flags
     */
    uint8_t
    close_flags() const;

    /**
     * Get the ledger hash (if present)
     *
     * @return The ledger hash if the header includes it, std::nullopt otherwise
     */
    std::optional<Hash256>
    hash() const;

    /**
     * Convert to a LedgerInfo structure
     */
    LedgerInfo
    to_ledger_info() const;

    /**
     * Convert to human-readable string representation
     */
    std::string
    to_string() const;
};

}  // namespace catl::common