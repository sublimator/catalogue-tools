#pragma once

#include <array>
#include <string>

/**
 * Constants and types for CATL (Catalogue) file format
 */

// Magic number and parsing masks
static constexpr uint32_t CATL = 0x4C544143UL;  // "CATL" in LE
static constexpr uint16_t CATALOGUE_VERSION_MASK = 0x00FF;
static constexpr uint16_t CATALOGUE_COMPRESS_LEVEL_MASK = 0x0F00;

/**
 * Header structures for CATL file format
 */
#pragma pack(push, 1)
struct CATLHeader
{
    uint32_t magic;
    uint32_t min_ledger;
    uint32_t max_ledger;
    uint16_t version;
    uint16_t network_id;
    uint64_t filesize;
    std::array<uint8_t, 64>
        hash;  // Note: This hash is usually unused/zero in practice
};

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
