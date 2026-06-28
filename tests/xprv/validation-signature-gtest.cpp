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
