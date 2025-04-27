#pragma once

#include <array>
#include <cstdint>

namespace catl::v1 {

// Constants for CATL files
static constexpr uint32_t CATL_MAGIC = 0x4C544143UL;  // "CATL" in LE
static constexpr uint16_t CATALOGUE_VERSION_MASK = 0x00FF;
static constexpr uint16_t CATALOGUE_COMPRESS_LEVEL_MASK = 0x0F00;
static constexpr uint16_t CATALOGUE_RESERVED_MASK = 0xF000;
static constexpr uint16_t BASE_CATALOGUE_VERSION = 1;

// The updated header structure with hash and filesize
#pragma pack(push, 1)
struct CatlHeader  // NOLINT(*-pro-type-member-init)
{
    uint32_t magic = CATL_MAGIC;
    uint32_t min_ledger;
    uint32_t max_ledger;
    uint16_t version;
    uint16_t network_id;
    uint64_t filesize = 0;
    std::array<uint8_t, 64> hash = {};
};
#pragma pack(pop)

#pragma pack(push, 1)
struct LedgerHeader
{
    uint32_t sequence;
    std::array<uint8_t, 32> hash;
    std::array<uint8_t, 32> parent_hash;
    std::array<uint8_t, 32> account_hash;
    std::array<uint8_t, 32> tx_hash;
    uint64_t drops;
    int32_t close_flags;
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
