// Verifies the session-signature *verification* path used by
// peer_connection::verify_peer_session_signature (sec #0053).
//
// The production verifier can't be unit-tested directly (it needs a live TLS
// handshake), so this test reproduces its exact crypto pipeline: a signature
// produced by crypto_utils::create_session_signature — the same call both this
// client and a real rippled peer make — must verify via verify_secp256k1
// against the shared cookie, and must FAIL on any tamper. This pins the one
// subtlety that would silently break the verifier: the signature is over the
// 32-byte cookie *directly* (it is already a digest), not over its hash.

#include <catl/base58/base58.h>
#include <catl/crypto/sig-verify.h>
#include <catl/peer-client/crypto-utils.h>

#include <gtest/gtest.h>
#include <sodium.h>

#include <array>
#include <cstdint>
#include <span>
#include <vector>

namespace catl::peer_client::test {

namespace {

// Decode a base64 (original variant) session signature to DER bytes, exactly
// as verify_peer_session_signature does.
std::vector<std::uint8_t>
b64_to_der(std::string const& b64)
{
    std::array<std::uint8_t, 128> der{};
    std::size_t der_len = 0;
    int rc = sodium_base642bin(
        der.data(),
        der.size(),
        b64.data(),
        b64.size(),
        nullptr,
        &der_len,
        nullptr,
        sodium_base64_VARIANT_ORIGINAL);
    EXPECT_EQ(rc, 0) << "signature should be valid base64";
    return {der.data(), der.data() + der_len};
}

// A deterministic, non-trivial 32-byte cookie stand-in.
std::array<std::uint8_t, 32>
sample_cookie()
{
    std::array<std::uint8_t, 32> c{};
    for (std::size_t i = 0; i < c.size(); ++i)
        c[i] = static_cast<std::uint8_t>(0xA0 ^ (i * 7 + 1));
    return c;
}

}  // namespace

class SessionSignatureTest : public ::testing::Test
{
protected:
    crypto_utils crypto_;
};

TEST_F(SessionSignatureTest, RoundTripVerifies)
{
    auto keys = crypto_.generate_node_keys();
    auto cookie = sample_cookie();

    auto sig_b64 = crypto_.create_session_signature(keys.secret_key, cookie);
    auto der = b64_to_der(sig_b64);

    // The advertised Public-Key travels as base58; decode it back to 33 bytes,
    // mirroring the wire path the verifier sees.
    auto pub = base58::decode_node_public(keys.public_key_b58);
    ASSERT_TRUE(pub.has_value());
    ASSERT_EQ(pub->size(), 33u);

    EXPECT_TRUE(catl::crypto::verify_secp256k1(
        *pub,
        std::span<const std::uint8_t>(der.data(), der.size()),
        cookie));
}

TEST_F(SessionSignatureTest, TamperedCookieFails)
{
    auto keys = crypto_.generate_node_keys();
    auto cookie = sample_cookie();
    auto sig_b64 = crypto_.create_session_signature(keys.secret_key, cookie);
    auto der = b64_to_der(sig_b64);

    auto pub = base58::decode_node_public(keys.public_key_b58);
    ASSERT_TRUE(pub.has_value());

    // A peer that signed a *different* cookie (e.g. a replayed signature from
    // another session) must not verify against ours.
    cookie[0] ^= 0x01;
    EXPECT_FALSE(catl::crypto::verify_secp256k1(
        *pub,
        std::span<const std::uint8_t>(der.data(), der.size()),
        cookie));
}

TEST_F(SessionSignatureTest, WrongKeyFails)
{
    auto signer = crypto_.generate_node_keys();
    auto impostor = crypto_.generate_node_keys();
    auto cookie = sample_cookie();

    auto sig_b64 = crypto_.create_session_signature(signer.secret_key, cookie);
    auto der = b64_to_der(sig_b64);

    // A peer advertising someone else's Public-Key while signing with its own
    // key (or vice versa) must fail — this is the impersonation case.
    auto impostor_pub = base58::decode_node_public(impostor.public_key_b58);
    ASSERT_TRUE(impostor_pub.has_value());

    EXPECT_FALSE(catl::crypto::verify_secp256k1(
        *impostor_pub,
        std::span<const std::uint8_t>(der.data(), der.size()),
        cookie));
}

TEST_F(SessionSignatureTest, HashingTheCookieWouldFail)
{
    // Guard against a regression where someone "fixes" the verifier to use
    // verify_message (which SHA512-halves the message). The cookie is already
    // a digest and is signed directly, so hashing it must NOT verify.
    auto keys = crypto_.generate_node_keys();
    auto cookie = sample_cookie();
    auto sig_b64 = crypto_.create_session_signature(keys.secret_key, cookie);
    auto der = b64_to_der(sig_b64);

    auto pub = base58::decode_node_public(keys.public_key_b58);
    ASSERT_TRUE(pub.has_value());

    EXPECT_FALSE(catl::crypto::verify_message(
        *pub,
        std::span<const std::uint8_t>(der.data(), der.size()),
        cookie))
        << "verify_message hashes the message; the cookie is pre-hashed";
}

}  // namespace catl::peer_client::test
