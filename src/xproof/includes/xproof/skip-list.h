#pragma once

#include <catl/core/types.h>
#include <catl/crypto/sha512-half-hasher.h>

#include <cstdint>

namespace xproof {

/// Short skip list key: SHA512Half(be16('s'))
/// The LedgerHashes SLE that contains the last 256 ledger hashes.
/// Every ledger writes to this same key.
inline Hash256
skip_list_key()
{
    catl::crypto::Sha512HalfHasher h;
    uint16_t ns = 0x0073;  // 's' as big-endian uint16
    uint8_t buf[2] = {
        static_cast<uint8_t>((ns >> 8) & 0xFF),
        static_cast<uint8_t>(ns & 0xFF)};
    h.update(buf, 2);
    return h.finalize();
}

/// Long skip list key: SHA512Half(be16('s'), be32(seq >> 16))
/// Contains hashes of flag ledgers (every 256th) within a 65536-ledger range.
/// There are 2^16 of these, each covering a range of 65536 ledgers.
inline Hash256
skip_list_key(uint32_t ledger_seq)
{
    catl::crypto::Sha512HalfHasher h;
    uint16_t ns = 0x0073;  // 's'
    uint32_t group = ledger_seq >> 16;
    uint8_t buf[6] = {
        static_cast<uint8_t>((ns >> 8) & 0xFF),
        static_cast<uint8_t>(ns & 0xFF),
        static_cast<uint8_t>((group >> 24) & 0xFF),
        static_cast<uint8_t>((group >> 16) & 0xFF),
        static_cast<uint8_t>((group >> 8) & 0xFF),
        static_cast<uint8_t>(group & 0xFF),
    };
    h.update(buf, 6);
    return h.finalize();
}

}  // namespace xproof
