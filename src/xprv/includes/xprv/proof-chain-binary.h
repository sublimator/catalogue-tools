#pragma once

// XPRV — Compact binary proof chain format.
//
// File layout:
//   [magic: 4 bytes "XPRV"]
//   [version: 1 byte, XPRV_FORMAT_VERSION — see src/xprv/VERSION]
//   [flags: 1 byte]
//     bit 0: zlib compressed body
//   [network_id: 4 bytes big-endian]  (added in v2)
//
// Endianness: every multi-byte integer in the binary form is big-endian,
// matching XRPL canonical serialization (spec §3.1). The comment previously
// said LE; the code has always used read_u32_be.
//   [body: concatenated TLV records]
//
// TLV record: [type: 1 byte][length: LEB128 varint][payload]
//
// TLV types:
//   0x01  Anchor core    — ledger hash, publisher key, validation signatures
//   0x02  Ledger header  — fixed 118-byte canonical layout
//   0x03  Map proof (tx) — binary trie (2-bit branch headers, depth-first)
//   0x04  Map proof (state)
//   0x05  Anchor UNL     — publisher manifest, blob signature, decomposed VL
//
// TLV ordering is significant for progressive verification:
//
//   [0x01 anchor core]       ← verifier can check signatures against cached UNL
//   [0x02 header]            ← authenticates tree roots
//   [0x03/0x04 map proofs]   ← proves data exists in the ledger
//   [0x02 header] ...        ← skip list hops (if historical)
//   [0x05 anchor UNL]        ← LAST and OPTIONAL: omitted when the verifier
//                            already holds trust for this publisher (cached
//                            UNL, or an anchor pinned on-chain). See spec §3.3.
//
// This ordering enables streaming/progressive verification:
//   - For animated QR codes, the first few frames carry the payment data
//     and signatures. A verifier with a cached UNL can confirm the payment
//     before all frames are scanned.
//   - For BLE/NFC, a push parser can fire callbacks as TLVs arrive,
//     allowing early quorum detection.
//   - Zlib compression is optional (flag bit 0). When enabled, the entire
//     body is compressed as one stream. For progressive use cases, leave
//     compression off or use per-section compression (future).
//
// The anchor is split into two TLV records so the ~15KB VL blob doesn't
// block the ~4KB of signatures that the verifier actually needs first.
//
// Future directions:
//   - Push parser: feed() chunks, fire callbacks per TLV. The framing
//     already supports this — just need to buffer partial records.
//   - Per-section compression (zstd streaming, or indexed gzip) for
//     progressive decoding with compression.
//   - UNL hash in anchor core so verifier can check cache hit without
//     reading UNL blob (currently it just tries its cached keys).

#include "proof-chain.h"

#include <catl/xdata/protocol.h>

#include <cstdint>
#include <span>
#include <vector>

namespace xprv {

/// Binary proof magic bytes.
inline constexpr uint8_t XPRV_MAGIC[4] = {'X', 'P', 'R', 'V'};

/// Options for binary serialization.
struct BinaryOptions
{
    bool compress = false;   // zlib compress the TLV body
    int compress_level = 9;  // 1=fast, 9=best, 0=none (zlib levels)

    /// Required only to serialize a chain whose tries exist as JSON but not as
    /// binary — i.e. one decoded with from_json() rather than produced by the
    /// builder. Leaf bytes must be re-serialized through the XRPL codec, which
    /// needs the protocol definitions. Null is fine for builder-produced chains,
    /// which already carry trie_binary.
    catl::xdata::Protocol const* protocol = nullptr;
};

/// Serialize a ProofChain to binary format with file header.
std::vector<uint8_t>
to_binary(ProofChain const& chain, BinaryOptions const& opts = {});

/// Deserialize binary data (with file header) into a ProofChain.
ProofChain
from_binary(std::span<const uint8_t> data);

}  // namespace xprv
