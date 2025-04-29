#pragma once

#include <array>
#include <cstdint>

namespace catl::common {
/**
 * Common ledger structures used across different catalogue formats
 */
#pragma pack(push, 1)

/***
 * TODO: this should really match the canonical ledger header format per this
 *
 *LedgerInfo
 *  deserializeHeader(Slice data, bool hasHash)
 *  {
 *      SerialIter sit(data.data(), data.size());
 *
 *      LedgerInfo info;
 *
 *      info.seq = sit.get32();
 *      info.drops = sit.get64();
 *      info.parentHash = sit.get256();
 *      info.txHash = sit.get256();
 *      info.accountHash = sit.get256();
 *      info.parentCloseTime =
 *          NetClock::time_point{NetClock::duration{sit.get32()}};
 *      info.closeTime = NetClock::time_point{NetClock::duration{sit.get32()}};
 *      info.closeTimeResolution = NetClock::duration{sit.get8()};
 *      info.closeFlags = sit.get8();
 *
 *      if (hasHash)
 *          info.hash = sit.get256();
 *
 *      return info;
 *  }
 */
struct LedgerInfoV1
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
