#include "xprv/manifest-utils.h"
#include "xprv/proof-chain-json.h"

#include <catl/core/base64.h>
#include <catl/crypto/sig-verify.h>
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
    vl.version = 1;
    vl.has_version = true;
    vl.publisher_key_hex = anchor.publisher_key_hex;
    vl.publisher_manifest = catl::vl::parse_manifest(anchor.publisher_manifest);
    vl.blob_bytes = anchor.blob;
    vl.blob_signature = anchor.blob_signature;

    std::string const blob_text(vl.blob_bytes.begin(), vl.blob_bytes.end());
    auto blob_json = boost::json::parse(blob_text);
    auto const& blob = blob_json.as_object();
    vl.sequence = blob.at("sequence").to_number<std::uint32_t>();
    vl.has_sequence = true;
    vl.expiration = blob.at("expiration").to_number<std::uint32_t>();
    vl.has_expiration = true;
    if (blob.contains("effective"))
        vl.effective = blob.at("effective").to_number<std::uint32_t>();

    for (auto const& entry : blob.at("validators").as_array())
    {
        auto const encoded =
            std::string(entry.as_object().at("manifest").as_string());
        auto const raw = catl::base64_decode(encoded);
        vl.validators.push_back(catl::vl::parse_manifest(raw));
    }
    vl.has_validators = true;
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
    vl.version = 1;
    vl.has_version = true;
    vl.sequence = list_sequence;
    vl.has_sequence = true;
    vl.effective = 50;
    vl.expiration = 200;
    vl.has_expiration = true;
    vl.has_validators = true;
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

    auto short_master_key = vl.publisher_manifest;
    short_master_key.master_public_key = {0xED};
    EXPECT_FALSE(xprv::verify_manifest(short_master_key, protocol));

    auto short_signing_key = vl.publisher_manifest;
    short_signing_key.signing_public_key = {0xED};
    EXPECT_FALSE(xprv::verify_manifest(short_signing_key, protocol));

    EXPECT_FALSE(catl::crypto::verify_message(
        std::vector<std::uint8_t>{0xED},
        std::vector<std::uint8_t>(64),
        std::vector<std::uint8_t>{0x01}));

    auto unsupported_version = vl.publisher_manifest.raw;
    unsupported_version.insert(
        unsupported_version.begin(), {0x10, 0x10, 0x00, 0x01});
    EXPECT_THROW(
        catl::vl::parse_manifest(unsupported_version), std::runtime_error);

    auto explicit_version_zero = vl.publisher_manifest.raw;
    explicit_version_zero.insert(
        explicit_version_zero.begin(), {0x10, 0x10, 0x00, 0x00});
    auto const parsed_version_zero =
        catl::vl::parse_manifest(explicit_version_zero);
    EXPECT_TRUE(parsed_version_zero.has_version);
    EXPECT_EQ(parsed_version_zero.version, 0u);

    auto unexpected_field = vl.publisher_manifest.raw;
    unexpected_field.insert(
        unexpected_field.begin(), {0x22, 0x00, 0x00, 0x00, 0x00});
    EXPECT_THROW(
        catl::vl::parse_manifest(unexpected_field), std::runtime_error);

    auto duplicate_sequence = vl.publisher_manifest.raw;
    duplicate_sequence.insert(
        duplicate_sequence.begin(),
        {0x24,
         static_cast<std::uint8_t>(vl.publisher_manifest.sequence >> 24),
         static_cast<std::uint8_t>(vl.publisher_manifest.sequence >> 16),
         static_cast<std::uint8_t>(vl.publisher_manifest.sequence >> 8),
         static_cast<std::uint8_t>(vl.publisher_manifest.sequence)});
    EXPECT_THROW(
        catl::vl::parse_manifest(duplicate_sequence), std::runtime_error);

    auto trailing_object_end = vl.publisher_manifest.raw;
    trailing_object_end.push_back(0xE1);
    EXPECT_THROW(
        catl::vl::parse_manifest(trailing_object_end), std::runtime_error);
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

    auto unsupported_version = vl;
    unsupported_version.version = 2;
    EXPECT_FALSE(xprv::verify_fetched_vl(
        unsupported_version, vl.publisher_key_hex, protocol, error));

    auto missing_sequence = vl;
    missing_sequence.has_sequence = false;
    EXPECT_FALSE(xprv::verify_fetched_vl(
        missing_sequence, vl.publisher_key_hex, protocol, error));

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

    auto current_with_validator = current;
    catl::vl::Manifest validator;
    validator.master_public_key = {0x01};
    validator.sequence = 7;
    validator.raw = {0x10};
    current_with_validator.validators.push_back(validator);

    candidate = make_freshness_candidate(11, 5, {0x03}, {0x02});
    validator.sequence = 6;
    candidate.validators.push_back(validator);
    EXPECT_EQ(
        xprv::assess_vl_freshness(
            candidate, &current_with_validator, 100, error),
        xprv::VlFreshnessDisposition::reject);

    validator.sequence = 7;
    validator.raw = {0x11};
    candidate.validators.front() = validator;
    EXPECT_EQ(
        xprv::assess_vl_freshness(
            candidate, &current_with_validator, 100, error),
        xprv::VlFreshnessDisposition::reject);

    xprv::ValidatorManifestHistory history;
    xprv::remember_validator_manifests(current_with_validator, history);

    auto removed = make_freshness_candidate(11, 5, {0x03}, {0x02});
    EXPECT_EQ(
        xprv::assess_vl_freshness(
            removed, &current_with_validator, 100, error, &history),
        xprv::VlFreshnessDisposition::accept);
    xprv::remember_validator_manifests(removed, history);

    auto readded = make_freshness_candidate(12, 5, {0x04}, {0x02});
    validator.sequence = 6;
    validator.raw = {0x09};
    readded.validators.push_back(validator);
    EXPECT_EQ(
        xprv::assess_vl_freshness(readded, &removed, 100, error, &history),
        xprv::VlFreshnessDisposition::reject);
}

// §1.3 rule 3 (D8): a list may carry revoked validators. They stay members
// (counted in N) and never vote; the loader must not refuse the list. These
// are self-signed revocations by synthetic test masters (keypair(900/901) in
// scripts/test-verify-xprv.py).
TEST(VlSecurity, FetchedListAcceptsRevokedValidators)
{
    auto const protocol = test_protocol();
    auto vl = load_vl_fixture();
    std::string error;
    for (auto const* hex :
         {"24FFFFFFFF7121ED0AF92088023084FC4563169B374383A65C00EF57B495F07B7AF6"
          "6C47287194D370124057EFFE8BB8708219F085F16BB9E8A4546847BE8F06F37010"
          "90A9C57F23D30C6014599C6B4ABB1F44A52E49B94CEFF612D6BD6D8092D58D0B7C"
          "74FBBFDF256805",
          "24FFFFFFFF7121ED249C3C61B88F9A4EB3B614C9028DB97F3C186016AFA1C7B68C"
          "9B262BB3900118701240F8A07F067600FB20F950DDEB03CD0CCD142C438234B670"
          "7D2BBC8B41932BDEEFDA6B08FDD0B899F4469E8F653DBD401350D4F85FC8A43611"
          "B4A1207BB549620E"})
    {
        auto const bytes = catl::vl::decode_hex(hex);
        auto revocation = catl::vl::parse_manifest(bytes);
        ASSERT_TRUE(revocation.is_revocation());
        ASSERT_TRUE(xprv::verify_manifest(revocation, protocol));
        vl.validators.push_back(std::move(revocation));
    }
    // Two revocations: neither registers a signing key, so they can't collide.
    EXPECT_TRUE(
        xprv::verify_fetched_vl(vl, vl.publisher_key_hex, protocol, error))
        << error;
}
