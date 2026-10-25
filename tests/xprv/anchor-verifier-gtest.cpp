#include "xprv/anchor-verifier.h"

#include <catl/xdata/protocol.h>

#include <boost/json.hpp>
#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace {

boost::json::object
load_anchor_fixture()
{
    std::ifstream in(
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/proof.json");
    if (!in)
        throw std::runtime_error("cannot open XPRV proof fixture");
    std::stringstream buffer;
    buffer << in.rdbuf();
    auto proof = boost::json::parse(buffer.str()).as_object();
    return proof.at("steps").as_array().front().as_object();
}

constexpr auto xrpl_publisher =
    "ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734";
constexpr auto other_publisher =
    "EDA46E9C39B1389894E690E58914DC1029602870370A0993E5B87C4A24EAF4A8E8";

xprv::AnchorVerifyResult
verify(boost::json::object const& anchor, std::string const& trusted_key)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    std::vector<std::string> narrative;
    return xprv::AnchorVerifier::verify(
        anchor, trusted_key, protocol, narrative);
}

}  // namespace

TEST(AnchorVerifier, AcceptsRealFixture)
{
    EXPECT_TRUE(verify(load_anchor_fixture(), xrpl_publisher).verified);
}

TEST(AnchorVerifier, RejectsPublisherClaimNotBoundToManifest)
{
    auto anchor = load_anchor_fixture();
    anchor.at("unl").as_object()["public_key"] = other_publisher;
    auto const result = verify(anchor, other_publisher);
    EXPECT_FALSE(result.verified);
    EXPECT_EQ(result.error, "publisher manifest master key mismatch");
}

TEST(AnchorVerifier, RejectsValidationMapKeyMismatch)
{
    auto anchor = load_anchor_fixture();
    auto const& original = anchor.at("validations").as_object();
    boost::json::object changed;
    bool replaced = false;
    for (auto const& validation : original)
    {
        if (!replaced)
        {
            changed
                ["ED00000000000000000000000000000000000000000000000000000000000"
                 "00000"] = validation.value();
            replaced = true;
        }
        else
        {
            changed[validation.key()] = validation.value();
        }
    }
    anchor["validations"] = std::move(changed);

    auto const result = verify(anchor, xrpl_publisher);
    EXPECT_FALSE(result.verified);
    EXPECT_EQ(result.error, "validation map key does not match SigningPubKey");
}

namespace {

boost::json::object
load_partial_fixture()
{
    std::ifstream in(
        std::string(PROJECT_ROOT) +
        "tests/xprv/fixture/partial-validations.json");
    if (!in)
        throw std::runtime_error("cannot open partial-validations fixture");
    std::stringstream buffer;
    buffer << in.rdbuf();
    return boost::json::parse(buffer.str()).as_object();
}

}  // namespace

TEST(AnchorVerifier, PartialValidationsDoNotCountTowardQuorum)
{
    auto const fixture = load_partial_fixture();
    auto const key = std::string(fixture.at("trusted_key").as_string());
    auto const& anchors = fixture.at("anchors").as_object();

    auto const all_partial = verify(anchors.at("all-partial").as_object(), key);
    EXPECT_FALSE(all_partial.verified);
    EXPECT_EQ(all_partial.error, "quorum not met");
    EXPECT_EQ(all_partial.validations_verified, 5);
    EXPECT_EQ(all_partial.validations_partial, 5);
    EXPECT_EQ(all_partial.validations_matched_unl, 0);

    auto const three_full =
        verify(anchors.at("three-full-two-partial").as_object(), key);
    EXPECT_FALSE(three_full.verified);
    EXPECT_EQ(three_full.validations_matched_unl, 3);

    auto const four_full =
        verify(anchors.at("four-full-one-partial").as_object(), key);
    EXPECT_TRUE(four_full.verified);
    EXPECT_EQ(four_full.validations_partial, 1);
    EXPECT_EQ(four_full.validations_matched_unl, 4);
}

TEST(AnchorVerifier, RejectsListExpiredAtAnchorCloseTime)
{
    EXPECT_TRUE(xprv::validator_list_active_at(50, 100, 99));
    EXPECT_FALSE(xprv::validator_list_active_at(50, 100, 100));
}

TEST(AnchorVerifier, RejectsListNotYetEffectiveAtAnchorCloseTime)
{
    EXPECT_FALSE(xprv::validator_list_active_at(101, 200, 100));
    EXPECT_TRUE(xprv::validator_list_active_at(100, 200, 100));
}
