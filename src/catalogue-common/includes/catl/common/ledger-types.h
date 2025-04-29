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
    uint8_t tx_hash[32];
    uint8_t account_hash[32];
    uint8_t parent_hash[32];
    uint64_t drops;
    uint32_t close_flags;
    uint32_t close_time_resolution;
    uint64_t close_time;
    uint64_t parent_close_time;
};
#pragma pack(pop)

}  // namespace catl::common

// struct LedgerHeader
// {
//     uint32_t sequence;
//     std::array<uint8_t, 32> hash;
//     std::array<uint8_t, 32> parent_hash;
//     std::array<uint8_t, 32> account_hash;
//     std::array<uint8_t, 32> tx_hash;
//     uint64_t drops;
//     int32_t close_flags;
//     uint32_t close_time_resolution;
//     uint64_t close_time;
//     uint64_t parent_close_time;
// }