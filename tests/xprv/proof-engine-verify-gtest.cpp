// ProofEngine::verify takes its trust from the deployment's configuration
// for the proof's network, never from built-in defaults.

#include "xprv/network-config.h"
#include "xprv/proof-chain.h"
#include "xprv/proof-engine.h"

#include <catl/xdata/protocol.h>

#include <boost/json.hpp>
#include <gtest/gtest.h>

#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace {

// An anchor-only proof signed by test keys: a publisher that is not the
// built-in one for any network.
struct CustomPublisherProof
{
    std::vector<uint8_t> bytes;
    std::string publisher_key;
};

CustomPublisherProof
custom_publisher_proof()
{
    std::ifstream file(
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/trust-evidence.json");
    std::stringstream text;
    text << file.rdbuf();
    auto const fixture = boost::json::parse(text.str());
    auto const& anchor = fixture.at("cases").at("current");

    boost::json::object proof{
        {"format_version", XPRV_FORMAT_VERSION},
        {"network_id", 0},
        {"steps", boost::json::array{anchor}}};
    auto const json = boost::json::serialize(proof);
    return {
        std::vector<uint8_t>(json.begin(), json.end()),
        std::string(anchor.at("unl").at("public_key").as_string())};
}

}  // namespace

TEST(ProofEngineVerify, UsesTheConfiguredPublisherOfTheProofsNetwork)
{
    auto const proof = custom_publisher_proof();
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto const builtin = xprv::NetworkConfig::for_network(0).publisher_key;
    ASSERT_NE(proof.publisher_key, builtin);

    auto const configured = [&](uint32_t network_id)
        -> std::optional<xprv::ProofEngine::VerifyTrust> {
        if (network_id != 0)
            return std::nullopt;
        return xprv::ProofEngine::VerifyTrust{
            proof.publisher_key, &protocol, nullptr};
    };
    auto const accepted = xprv::ProofEngine::verify(proof.bytes, configured);
    EXPECT_TRUE(accepted.ok) << accepted.error;

    // The same proof under the network's built-in key is another
    // deployment's question, and the answer is no.
    auto const defaults = [&](uint32_t)
        -> std::optional<xprv::ProofEngine::VerifyTrust> {
        return xprv::ProofEngine::VerifyTrust{builtin, &protocol, nullptr};
    };
    auto const refused = xprv::ProofEngine::verify(proof.bytes, defaults);
    EXPECT_FALSE(refused.ok);
    EXPECT_NE(refused.error.find("publisher key mismatch"), std::string::npos)
        << refused.error;
}

TEST(ProofEngineVerify, RefusesANetworkTheDeploymentDoesNotServe)
{
    auto const proof = custom_publisher_proof();
    auto const none =
        [](uint32_t) -> std::optional<xprv::ProofEngine::VerifyTrust> {
        return std::nullopt;
    };
    auto const result = xprv::ProofEngine::verify(proof.bytes, none);
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.error.find("not configured"), std::string::npos)
        << result.error;
}

TEST(ProofEngineVerify, MalformedInputIsARefusalWithAReason)
{
    std::string const junk = R"({"format_version":)";
    std::vector<uint8_t> const bytes(junk.begin(), junk.end());
    auto const none =
        [](uint32_t) -> std::optional<xprv::ProofEngine::VerifyTrust> {
        return std::nullopt;
    };
    auto const result = xprv::ProofEngine::verify(bytes, none);
    EXPECT_FALSE(result.ok);
    EXPECT_FALSE(result.error.empty());
}
