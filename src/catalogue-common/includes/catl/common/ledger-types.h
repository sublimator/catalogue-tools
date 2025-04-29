#pragma once

#include <array>
#include <cstdint>

namespace catl::common {

/**
 * Common ledger structures used across different catalogue formats
 */
#pragma pack(push, 1)
struct LedgerInfo
{
    uint32_t sequence;
    uint8_t hash[32];
    uint8_t txHash[32];
    uint8_t accountHash[32];
    uint8_t parentHash[32];
    uint64_t drops;
    uint32_t closeFlags;
    uint32_t closeTimeResolution;
    uint64_t closeTime;
    uint64_t parentCloseTime;
};
#pragma pack(pop)

}  // namespace catl::common