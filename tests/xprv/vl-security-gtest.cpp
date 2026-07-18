#include "xprv/manifest-utils.h"
#include "xprv/proof-chain-json.h"

#include <catl/core/base64.h>
#include <catl/vl-client/vl-client.h>
#include <catl/xdata/protocol.h>

#include <boost/json.hpp>
#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

namespace {

catl::xdata::Protocol
test_protocol()
{
    return catl::xdata::Protocol::load_embedded_xrpl_protocol();
}

catl::vl::ValidatorList
load_vl_fixture()
{
    std::ifstream input(
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/proof.json");
    if (!input.is_open())
        throw std::runtime_error("proof fixture not found");

    std::stringstream buffer;
    buffer << input.rdbuf();
    auto const chain = xprv::from_json(boost::json::parse(buffer.str()));
    if (chain.steps.empty() ||
        !std::holds_alternative<xprv::AnchorData>(chain.steps.front()))
    {
        throw std::runtime_error("proof fixture has no anchor");
    }

    auto const& anchor = std::get<xprv::AnchorData>(chain.steps.front());
    catl::vl::ValidatorList vl;
    vl.publisher_key_hex = anchor.publisher_key_hex;
    vl.publisher_manifest = catl::vl::parse_manifest(anchor.publisher_manifest);
    vl.blob_bytes = anchor.blob;
    vl.blob_signature = anchor.blob_signature;

    std::string const blob_text(vl.blob_bytes.begin(), vl.blob_bytes.end());
    auto blob_json = boost::json::parse(blob_text);
    auto const& blob = blob_json.as_object();
    vl.sequence = blob.at("sequence").to_number<std::uint32_t>();
    vl.expiration = blob.at("expiration").to_number<std::uint32_t>();
    if (blob.contains("effective"))
        vl.effective = blob.at("effective").to_number<std::uint32_t>();

    for (auto const& entry : blob.at("validators").as_array())
    {
        auto const encoded =
            std::string(entry.as_object().at("manifest").as_string());
        auto const raw = catl::base64_decode(encoded);
        vl.validators.push_back(catl::vl::parse_manifest(raw));
    }
    return vl;
}

catl::vl::ValidatorList
make_freshness_candidate(
    std::uint32_t list_sequence,
    std::uint32_t manifest_sequence,
    std::vector<std::uint8_t> blob = {0x01},
    std::vector<std::uint8_t> manifest = {0x02})
{
    catl::vl::ValidatorList vl;
    vl.sequence = list_sequence;
    vl.effective = 50;
    vl.expiration = 200;
    vl.blob_bytes = std::move(blob);
    vl.publisher_manifest.sequence = manifest_sequence;
    vl.publisher_manifest.raw = std::move(manifest);
    return vl;
}

}  // namespace

TEST(VlSecurity, DecodeHexRejectsMalformedInput)
{
    EXPECT_EQ(
        catl::vl::decode_hex("00aF"), (std::vector<std::uint8_t>{0x00, 0xAF}));
    EXPECT_THROW(catl::vl::decode_hex("0"), std::runtime_error);
    EXPECT_THROW(catl::vl::decode_hex("0Z"), std::runtime_error);
}

TEST(VlSecurity, RegularManifestRequiresCanonicalDelegation)
{
    auto const protocol = test_protocol();
    auto vl = load_vl_fixture();
    ASSERT_TRUE(xprv::verify_manifest(vl.publisher_manifest, protocol));

    auto missing_signature = vl.publisher_manifest;
    missing_signature.signing_signature.clear();
    EXPECT_FALSE(xprv::verify_manifest(missing_signature, protocol));

    auto missing_sequence = vl.publisher_manifest;
    missing_sequence.has_sequence = false;
    EXPECT_FALSE(xprv::verify_manifest(missing_sequence, protocol));

    auto missing_key = vl.publisher_manifest;
    missing_key.signing_public_key.clear();
    EXPECT_FALSE(missing_key.is_revocation());
    EXPECT_FALSE(xprv::verify_manifest(missing_key, protocol));

    auto malformed_revocation = vl.publisher_manifest;
    malformed_revocation.sequence = std::numeric_limits<std::uint32_t>::max();
    EXPECT_TRUE(malformed_revocation.is_revocation());
    EXPECT_FALSE(xprv::verify_manifest(malformed_revocation, protocol));
}

TEST(VlSecurity, FetchedListRequiresCompleteVerifiedContents)
{
    auto const protocol = test_protocol();
    auto vl = load_vl_fixture();
    std::string error;
    ASSERT_TRUE(
        xprv::verify_fetched_vl(vl, vl.publisher_key_hex, protocol, error));

    auto missing_key = vl;
    missing_key.publisher_key_hex.clear();
    EXPECT_FALSE(xprv::verify_fetched_vl(
        missing_key, vl.publisher_key_hex, protocol, error));

    auto empty = vl;
    empty.validators.clear();
    EXPECT_FALSE(
        xprv::verify_fetched_vl(empty, vl.publisher_key_hex, protocol, error));

    auto duplicate = vl;
    duplicate.validators.push_back(duplicate.validators.front());
    EXPECT_FALSE(xprv::verify_fetched_vl(
        duplicate, vl.publisher_key_hex, protocol, error));

    auto invalid = vl;
    invalid.validators.front().signing_signature.clear();
    EXPECT_FALSE(xprv::verify_fetched_vl(
        invalid, vl.publisher_key_hex, protocol, error));
}

TEST(VlSecurity, FreshnessRejectsInactiveAndRolledBackLists)
{
    std::string error;
    auto candidate = make_freshness_candidate(10, 5);
    EXPECT_EQ(
        xprv::assess_vl_freshness(candidate, nullptr, 100, error),
        xprv::VlFreshnessDisposition::accept);

    candidate.expiration = 100;
    EXPECT_EQ(
        xprv::assess_vl_freshness(candidate, nullptr, 100, error),
        xprv::VlFreshnessDisposition::reject);

    candidate = make_freshness_candidate(10, 5);
    candidate.effective = 101;
    EXPECT_EQ(
        xprv::assess_vl_freshness(candidate, nullptr, 100, error),
        xprv::VlFreshnessDisposition::reject);

    auto const current = make_freshness_candidate(10, 5);
    candidate = current;
    EXPECT_EQ(
        xprv::assess_vl_freshness(candidate, &current, 100, error),
        xprv::VlFreshnessDisposition::unchanged);

    candidate = make_freshness_candidate(9, 5);
    EXPECT_EQ(
        xprv::assess_vl_freshness(candidate, &current, 100, error),
        xprv::VlFreshnessDisposition::reject);

    candidate = make_freshness_candidate(11, 4);
    EXPECT_EQ(
        xprv::assess_vl_freshness(candidate, &current, 100, error),
        xprv::VlFreshnessDisposition::reject);
}

TEST(VlSecurity, FreshnessRequiresSequenceForContentChanges)
{
    std::string error;
    auto const current = make_freshness_candidate(10, 5);

    auto candidate = make_freshness_candidate(10, 5, {0x03}, {0x02});
    EXPECT_EQ(
        xprv::assess_vl_freshness(candidate, &current, 100, error),
        xprv::VlFreshnessDisposition::reject);

    candidate = make_freshness_candidate(10, 5, {0x01}, {0x03});
    EXPECT_EQ(
        xprv::assess_vl_freshness(candidate, &current, 100, error),
        xprv::VlFreshnessDisposition::reject);

    candidate = make_freshness_candidate(11, 5, {0x03}, {0x02});
    EXPECT_EQ(
        xprv::assess_vl_freshness(candidate, &current, 100, error),
        xprv::VlFreshnessDisposition::accept);

    candidate = make_freshness_candidate(10, 6, {0x01}, {0x03});
    EXPECT_EQ(
        xprv::assess_vl_freshness(candidate, &current, 100, error),
        xprv::VlFreshnessDisposition::accept);
}
