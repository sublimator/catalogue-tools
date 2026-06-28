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
