#pragma once

#include <array>
#include <cstdint>

#include "catl/common/catalogue-types.h"

namespace catl::v1 {

// Additional v1-specific constants
static constexpr uint16_t CATALOGUE_RESERVED_MASK = 0xF000;
static constexpr uint16_t BASE_CATALOGUE_VERSION = 1;

// Re-export constants from common for backward compatibility
static constexpr uint32_t CATL_MAGIC = catl::common::CATL_MAGIC;
static constexpr uint16_t CATALOGUE_VERSION_MASK =
    catl::common::CATALOGUE_VERSION_MASK;
static constexpr uint16_t CATALOGUE_COMPRESS_LEVEL_MASK =
    catl::common::CATALOGUE_COMPRESS_LEVEL_MASK;

// Use the common header type
using CatlHeader = catl::common::CATLHeader;

// The v1-specific structure for ledger information in CATL files
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

// SHAMap node types
enum SHAMapNodeType : uint8_t {
    tnINNER = 1,
    tnTRANSACTION_NM = 2,
    tnTRANSACTION_MD = 3,
    tnACCOUNT_STATE = 4,
    tnREMOVE = 254,
    tnTERMINAL = 255
};

}  // namespace catl::v1
