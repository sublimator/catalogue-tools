#pragma once

#include "catl/core/types.h"
#include "catl/v1/catl-v1-structs.h"
#include <cstdint>
#include <string>

namespace catl::v1 {

/**
 * LedgerInfoV1View - Zero-copy view into v1 ledger headers
 *
 * This provides a view into the v1-specific binary layout of ledger information
 * in CATL files. Note that this layout does not match the canonical network
 * serialization format.
 */
class LedgerInfoView
{
private:
    const uint8_t* data;  // Raw pointer to header data

public:
    // Constructor with raw data pointer
    explicit LedgerInfoView(const uint8_t* headerData);

    // Core accessor methods
    uint32_t
    sequence() const;

    Hash256
    hash() const;

    Hash256
    parent_hash() const;

    Hash256
    transaction_hash() const;

    Hash256
    account_hash() const;

    uint32_t
    close_time() const;

    uint64_t
    drops() const;

    uint8_t
    close_flags() const;

    // Utility method for debugging/logging
    std::string
    to_string() const;
};

}  // namespace catl::v1