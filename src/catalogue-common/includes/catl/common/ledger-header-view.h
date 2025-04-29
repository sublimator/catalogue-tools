#pragma once

#include "catl/core/types.h"
#include <cstdint>
#include <string>

namespace catl::common {

/**
 * LedgerHeaderView - Zero-copy view into ledger headers
 */
class LedgerHeaderView
{
private:
    const uint8_t* data;  // Raw pointer to header data

public:
    // Constructor with raw data pointer
    explicit LedgerHeaderView(const uint8_t* headerData);

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

}  // namespace catl::common