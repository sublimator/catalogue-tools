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
