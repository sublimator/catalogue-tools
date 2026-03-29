#include "catl/crypto/sig-verify.h"

#include <secp256k1.h>
#include <sodium.h>

#include <mutex>

namespace catl::crypto {

// Lazily-initialized singleton secp256k1 context for verification.
static secp256k1_context*
get_verify_context()
{
    static secp256k1_context* ctx = [] {
        if (sodium_init() < 0)
            return static_cast<secp256k1_context*>(nullptr);
        auto* c = secp256k1_context_create(SECP256K1_CONTEXT_VERIFY);
        return c;
    }();
    return ctx;
}

KeyType
detect_key_type(std::span<const uint8_t> public_key)
{
    if (public_key.size() < 1)
        return KeyType::unknown;
    if (public_key[0] == 0xED)
        return KeyType::ed25519;
    if (public_key[0] == 0x02 || public_key[0] == 0x03)
        return KeyType::secp256k1;
    return KeyType::unknown;
}

bool
verify_secp256k1(
    std::span<const uint8_t> public_key,
    std::span<const uint8_t> signature,
    std::span<const uint8_t> hash)
{
    if (public_key.size() != 33 || hash.size() != 32)
        return false;

    auto* ctx = get_verify_context();
    if (!ctx)
        return false;

    // Parse the public key
    secp256k1_pubkey pubkey;
    if (!secp256k1_ec_pubkey_parse(
            ctx, &pubkey, public_key.data(), public_key.size()))
        return false;

    // Parse the DER-encoded signature
    secp256k1_ecdsa_signature sig;
    if (!secp256k1_ecdsa_signature_parse_der(
            ctx, &sig, signature.data(), signature.size()))
        return false;

    // Verify
    return secp256k1_ecdsa_verify(ctx, &sig, hash.data(), &pubkey) == 1;
}

bool
verify_ed25519(
    std::span<const uint8_t> public_key,
    std::span<const uint8_t> signature,
    std::span<const uint8_t> message)
{
    if (public_key.size() != 32 || signature.size() != 64)
        return false;

    // libsodium: returns 0 on success
    return crypto_sign_verify_detached(
               signature.data(),
               message.data(),
               message.size(),
               public_key.data()) == 0;
}

bool
verify_message(
    std::span<const uint8_t> public_key_33,
    std::span<const uint8_t> signature,
    std::span<const uint8_t> message)
{
    auto type = detect_key_type(public_key_33);
    switch (type)
    {
        case KeyType::secp256k1:
        {
            // secp256k1 verifies against SHA512-Half of the message
            unsigned char full_hash[64];
            crypto_hash_sha512(full_hash, message.data(), message.size());
            return verify_secp256k1(
                public_key_33, signature,
                std::span<const uint8_t>(full_hash, 32));
        }
        case KeyType::ed25519:
            return verify_ed25519(
                public_key_33.subspan(1, 32), signature, message);
        default:
            return false;
    }
}

}  // namespace catl::crypto
