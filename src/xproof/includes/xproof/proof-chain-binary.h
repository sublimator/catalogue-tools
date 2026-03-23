#pragma once

// Binary TLV encoding/decoding for proof chains.
//
// File format:
//   [magic: 4 bytes "XPRF"]
//   [version: 1 byte, currently 0x01]
//   [flags: 1 byte]
//     bit 0: zlib compressed
//   [body: TLV records, optionally zlib-compressed]
//
// See xpop-2-py SPEC.md section 3 for TLV record format.

#include "proof-chain.h"

#include <cstdint>
#include <span>
#include <vector>

namespace xproof {

/// Options for binary serialization.
struct BinaryOptions
{
    bool compress = false;   // zlib compress the TLV body
    int compress_level = 9;  // 1=fast, 9=best, 0=none (zlib levels)
};

/// Serialize a ProofChain to binary format with file header.
std::vector<uint8_t>
to_binary(ProofChain const& chain, BinaryOptions const& opts = {});

/// Deserialize binary data (with file header) into a ProofChain.
ProofChain
from_binary(std::span<const uint8_t> data);

}  // namespace xproof
