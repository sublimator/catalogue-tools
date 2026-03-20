#pragma once

#include <cstdint>
#include <span>

namespace catl::crypto {

/// Key type detection from the first byte of a 33-byte public key.
enum class KeyType { secp256k1, ed25519, unknown };

/// Detect key type: 0xED = Ed25519, 0x02/0x03 = secp256k1.
KeyType
detect_key_type(std::span<const uint8_t> public_key);

/// Verify a secp256k1 ECDSA signature (DER encoded).
/// @param public_key 33-byte compressed secp256k1 public key
/// @param signature  DER-encoded ECDSA signature (variable length)
/// @param hash       32-byte message hash (already SHA512Half'ed)
bool
verify_secp256k1(
    std::span<const uint8_t> public_key,
    std::span<const uint8_t> signature,
    std::span<const uint8_t> hash);

/// Verify an Ed25519 signature.
/// @param public_key 32-byte raw Ed25519 public key (NOT 0xED-prefixed)
/// @param signature  64-byte Ed25519 signature
/// @param message    raw message bytes (NOT pre-hashed)
bool
verify_ed25519(
    std::span<const uint8_t> public_key,
    std::span<const uint8_t> signature,
    std::span<const uint8_t> message);

/// Convenience: verify a signature given a 33-byte XRPL public key
/// (auto-detects Ed25519 vs secp256k1).
/// For secp256k1: `data` must be a 32-byte hash.
/// For Ed25519:   `data` is the raw message (will NOT be pre-hashed).
bool
verify(
    std::span<const uint8_t> public_key_33,
    std::span<const uint8_t> signature,
    std::span<const uint8_t> data);

}  // namespace catl::crypto
