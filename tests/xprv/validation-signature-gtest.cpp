// Oracle test for ValidationCollector::verify_validation_signature (sec #0053).
//
// The fixture proof.json carries 32 real STValidations captured from the live
// network, keyed by signing key. Every one MUST verify — this pins that the
// signing-serialization reconstruction ("VAL\0" || object-without-sfSignature)
// and the verify_message dispatch are correct. If this ever regresses, the
// production on_packet path would silently drop every real validation and break
// quorum, so the green bar here is what makes drop-on-failure safe to enable.

#include "xprv/proof-chain-json.h"
#include "xprv/proof-chain.h"
#include "xprv/validation-collector.h"

#include <catl/xdata/protocol.h>

#include <boost/json.hpp>
#include <gtest/gtest.h>
#include <sodium.h>

#include <cstdint>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

xprv::AnchorData
load_anchor()
{
    std::string const path =
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/proof.json";
    std::ifstream input(path);
    if (!input.is_open())
        throw std::runtime_error("proof.json not found at " + path);
    std::stringstream buffer;
    buffer << input.rdbuf();
    auto const chain = xprv::from_json(boost::json::parse(buffer.str()));
    if (chain.steps.empty() ||
        !std::holds_alternative<xprv::AnchorData>(chain.steps.front()))
        throw std::runtime_error("proof.json does not start with an anchor");
    return std::get<xprv::AnchorData>(chain.steps.front());
}

}  // namespace

using catl::xdata::Protocol;
using xprv::ValidationCollector;

TEST(ValidationSignature, AllFixtureValidationsVerify)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    auto const anchor = load_anchor();
    ASSERT_FALSE(anchor.validations.empty())
        << "fixture should carry captured validations";

    std::size_t checked = 0;
    for (auto const& [key_hex, raw] : anchor.validations)
    {
        EXPECT_TRUE(
            ValidationCollector::verify_validation_signature(raw, protocol))
            << "real captured validation must verify; signing key " << key_hex;
        ++checked;
    }
    EXPECT_GT(checked, 0u);
}

TEST(ValidationSignature, TamperedBodyFails)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    auto const anchor = load_anchor();
    ASSERT_FALSE(anchor.validations.empty());

    auto raw = anchor.validations.begin()->second;
    ASSERT_GT(raw.size(), 8u);
    ASSERT_TRUE(ValidationCollector::verify_validation_signature(raw, protocol));

    // Flip a byte near the front — a signed field (flags/seq region), well
    // ahead of the trailing sfSignature. Either the hash changes (verify
    // fails) or the parse breaks (returns false); both are a rejection.
    raw[5] ^= 0x01;
    EXPECT_FALSE(
        ValidationCollector::verify_validation_signature(raw, protocol));
}

TEST(ValidationSignature, TamperedSignatureFails)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    auto const anchor = load_anchor();
    auto raw = anchor.validations.begin()->second;
    ASSERT_FALSE(raw.empty());
    ASSERT_TRUE(ValidationCollector::verify_validation_signature(raw, protocol));

    // The sfSignature is the last field in canonical order; flip a byte inside
    // its value.
    raw.back() ^= 0x01;
    EXPECT_FALSE(
        ValidationCollector::verify_validation_signature(raw, protocol));
}

TEST(ValidationSignature, EmptyOrGarbageFails)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    EXPECT_FALSE(
        ValidationCollector::verify_validation_signature({}, protocol));
    std::vector<std::uint8_t> garbage(64, 0xFF);
    EXPECT_FALSE(
        ValidationCollector::verify_validation_signature(garbage, protocol));
}

// extract_stvalidation parses an attacker-controlled protobuf length prefix
// (TMValidation is gossiped pre-auth). A crafted length must never overflow the
// bound and construct an inverted pointer range (sec #0060) — the old
// hand-rolled varint crashed a worker thread on this input.
TEST(ExtractStValidation, RejectsOverflowingLength)
{
    // 0x0A tag + a 10-byte LEB128 length that decodes to a near-2^64 value;
    // the old `pos + length` check wrapped and let this through.
    std::vector<std::uint8_t> overflow{
        0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F};
    EXPECT_TRUE(ValidationCollector::extract_stvalidation(overflow).empty());

    // A varint with too many continuation bytes (shift >= 64): the decoder
    // throws, extract_stvalidation catches it and returns empty.
    std::vector<std::uint8_t> overlong(12, 0xFF);
    overlong[0] = 0x0A;
    EXPECT_TRUE(ValidationCollector::extract_stvalidation(overlong).empty());
}

TEST(ExtractStValidation, RejectsTruncatedOrTooShort)
{
    // length=100 declared but only 10 body bytes present.
    std::vector<std::uint8_t> truncated{0x0A, 100};
    truncated.resize(2 + 10, 0x00);
    EXPECT_TRUE(ValidationCollector::extract_stvalidation(truncated).empty());

    // length=5 is below the 50-byte minimum STValidation size.
    std::vector<std::uint8_t> too_short{0x0A, 5, 1, 2, 3, 4, 5};
    EXPECT_TRUE(ValidationCollector::extract_stvalidation(too_short).empty());
}

TEST(ExtractStValidation, AcceptsWellFormed)
{
    // 0x0A tag + single-byte length 50 + exactly 50 body bytes.
    std::vector<std::uint8_t> data{0x0A, 50};
    for (int i = 0; i < 50; ++i)
        data.push_back(static_cast<std::uint8_t>(i));
    auto body = ValidationCollector::extract_stvalidation(data);
    ASSERT_EQ(body.size(), 50u);
    EXPECT_EQ(body.front(), 0u);
    EXPECT_EQ(body.back(), 49u);
}

// All captured fixtures are secp256k1, so the ed25519 verify path (raw message,
// no SHA512-Half) is otherwise unexercised — yet on_packet DROPS on sig-fail
// (#0053), so a broken ed25519 path would silently discard every ed25519
// validator's validations (quorum harm). Synthesize an ed25519 STValidation and
// prove the live verify path accepts it (sec #0061).
TEST(ValidationSignature, Ed25519Verifies)
{
    ASSERT_GE(sodium_init(), 0);

    unsigned char pk[crypto_sign_ed25519_PUBLICKEYBYTES];  // 32
    unsigned char sk[crypto_sign_ed25519_SECRETKEYBYTES];  // 64
    ASSERT_EQ(crypto_sign_ed25519_keypair(pk, sk), 0);

    // Minimal STValidation body, canonical field order, WITHOUT sfSignature:
    //   sfLedgerSequence (UInt32, type 2 / nth 6) header 0x26
    //   sfSigningPubKey  (Blob,  type 7 / nth 3) header 0x73, 0xED-prefixed key
    std::vector<std::uint8_t> body;
    body.push_back(0x26);
    std::uint32_t const seq = 12345;
    body.push_back((seq >> 24) & 0xFF);
    body.push_back((seq >> 16) & 0xFF);
    body.push_back((seq >> 8) & 0xFF);
    body.push_back(seq & 0xFF);
    body.push_back(0x73);
    body.push_back(33);  // VL length (<=192 → single byte)
    body.push_back(0xED);
    body.insert(body.end(), pk, pk + 32);

    // Signing message: "VAL\0" (HashPrefix::validation) || body-without-sig.
    static constexpr std::uint8_t kPrefix[4] = {0x56, 0x41, 0x4C, 0x00};
    std::vector<std::uint8_t> msg;
    msg.insert(msg.end(), kPrefix, kPrefix + 4);
    msg.insert(msg.end(), body.begin(), body.end());

    // Ed25519 signs the raw message — matching verify_message's ed25519 path.
    unsigned char sig[crypto_sign_ed25519_BYTES];  // 64
    unsigned long long siglen = 0;
    ASSERT_EQ(
        crypto_sign_ed25519_detached(sig, &siglen, msg.data(), msg.size(), sk),
        0);
    ASSERT_EQ(siglen, 64u);

    // Full STValidation = body || sfSignature (Blob, type 7 / nth 6) header 0x76
    std::vector<std::uint8_t> stval = body;
    stval.push_back(0x76);
    stval.push_back(64);  // VL length
    stval.insert(stval.end(), sig, sig + 64);

    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    EXPECT_TRUE(
        ValidationCollector::verify_validation_signature(stval, protocol))
        << "synthesized ed25519 STValidation must verify on the live path";

    // Tampering the signature must fail.
    auto bad_sig = stval;
    bad_sig.back() ^= 0x01;
    EXPECT_FALSE(
        ValidationCollector::verify_validation_signature(bad_sig, protocol));

    // Tampering a signed body byte (LedgerSequence low byte at index 4) must
    // fail — proves the body is actually covered by the signature.
    auto bad_body = stval;
    bad_body[4] ^= 0x01;
    EXPECT_FALSE(
        ValidationCollector::verify_validation_signature(bad_body, protocol));
}
