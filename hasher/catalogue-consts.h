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
 * Create a hash prefix from three characters
 */
constexpr std::uint32_t
make_hash_prefix(char a, char b, char c)
{
    return (static_cast<std::uint32_t>(a) << 24) +
        (static_cast<std::uint32_t>(b) << 16) +
        (static_cast<std::uint32_t>(c) << 8);
}

/**
 * Hash prefixes from rippled
 */
namespace HashPrefix {
// TODO: just use std::uint32_t enum but need to handle endian flip
// when passing to hasher
constexpr std::array<unsigned char, 4> txNode = {'S', 'N', 'D', 0x00};
constexpr std::array<unsigned char, 4> leafNode = {'M', 'L', 'N', 0x00};
constexpr std::array<unsigned char, 4> innerNode = {'M', 'I', 'N', 0x00};
}  // namespace HashPrefix

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
