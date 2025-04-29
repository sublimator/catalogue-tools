#pragma once

#include "catl/core/types.h"
#include <cstdint>
#include <string>

namespace catl::common {

/**
 * LedgerInfo - Canonical representation of Ripple/Xahau ledger information
 *
 * This structure matches the canonical binary serialization format used in the
 * Ripple/Xahau network protocol. This is the standard format for ledger headers
 * as they appear on the network, in contrast to format-specific storage
 * representations (e.g., the CATL v1 format which uses a different layout).
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
    Hash256 hash;

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
 * - seq:                  32 bits
 * - drops:                64 bits
 * - parent_hash:         256 bits
 * - tx_hash:             256 bits
 * - account_hash:        256 bits
 * - parent_close_time:    32 bits
 * - close_time:           32 bits
 * - close_time_resolution: 8 bits
 * - close_flags:           8 bits
 * - hash:                 256 bits
 */
class LedgerInfoView
{
private:
    const uint8_t* data;  // Raw pointer to header data

public:
    /**
     * Constructor with raw data pointer
     *
     * @param header_data Pointer to the start of the ledger header data
     */
    explicit LedgerInfoView(const uint8_t* header_data);

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
     * Get the ledger hash
     *
     */
    Hash256
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