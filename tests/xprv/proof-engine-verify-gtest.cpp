// ProofEngine::verify takes its trust from the deployment's configuration
// for the proof's network, never from built-in defaults.

#include "xprv/hex-utils.h"
#include "xprv/network-config.h"
#include "xprv/proof-chain.h"
#include "xprv/proof-engine.h"
#include "xprv/proof-resolver.h"

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

// ─── the report (§7) ──────────────────────────────────────────────────

namespace {

boost::json::object
positive_vector(std::string const& name)
{
    std::ifstream file(
        std::string(PROJECT_ROOT) + "tests/xprv/vectors/" + name + ".json");
    std::stringstream text;
    text << file.rdbuf();
    return boost::json::parse(text.str()).as_object();
}

std::vector<uint8_t>
json_bytes(boost::json::value const& value)
{
    auto const text = boost::json::serialize(value);
    return {text.begin(), text.end()};
}

xprv::TrustForNetworkFn
mainnet_trust(catl::xdata::Protocol const& protocol, std::string key)
{
    return [&protocol, key](uint32_t network_id)
               -> std::optional<xprv::VerifyTrust> {
        if (network_id != 0)
            return std::nullopt;
        return xprv::VerifyTrust{key, &protocol, nullptr};
    };
}

}  // namespace

// An accepted proof reports what it authenticated: the anchor, each header
// with what supported it, and each leaf with its key, the bytes that were
// hashed and the ledger it belongs to. Both forms report the same.
TEST(VerifyReport, AcceptedProofReportsItsHeadersAndLeaves)
{
    auto const vector = positive_vector("positive-tx-in-anchor-parent");
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto const key =
        std::string(vector.at("trust").at("publisher_key").as_string());
    auto const trust = mainnet_trust(protocol, key);

    auto const from_json = xprv::verify_proof(
        json_bytes(vector.at("form").at("json")), trust);
    ASSERT_TRUE(from_json.verified) << from_json.reason;
    EXPECT_TRUE(from_json.layer.empty());
    EXPECT_GT(from_json.votes, 0);
    EXPECT_GE(from_json.list_size, from_json.votes);
    ASSERT_EQ(from_json.headers.size(), 2u);
    EXPECT_EQ(from_json.headers[0].supported_by, "anchor");
    EXPECT_EQ(from_json.headers[0].ledger_hash, from_json.anchor_hash);
    EXPECT_EQ(from_json.headers[1].supported_by, "parent_hash");
    EXPECT_EQ(
        from_json.headers[1].ledger_hash, from_json.headers[0].header.parent_hash);
    ASSERT_EQ(from_json.leaves.size(), 1u);
    auto const& leaf = from_json.leaves[0];
    EXPECT_TRUE(leaf.is_tx);
    ASSERT_EQ(leaf.ledgers.size(), 1u);
    EXPECT_EQ(leaf.ledgers[0].first, from_json.headers[1].ledger_hash);
    EXPECT_EQ(leaf.ledgers[0].second, from_json.headers[1].header.seq);

    // The recorded leaf is the reported leaf, byte for byte.
    auto const& recorded = vector.at("intermediates")
                               .at("map_proofs")
                               .as_array()
                               .back()
                               .at("leaves")
                               .at(0);
    EXPECT_EQ(
        xprv::upper_hex(leaf.key), std::string(recorded.at("key").as_string()));
    EXPECT_EQ(
        leaf.bytes,
        xprv::from_hex(
            std::string(recorded.at("hashed_bytes_hex").as_string())));

    auto const from_binary = xprv::verify_proof(
        xprv::from_hex(
            std::string(vector.at("form").at("binary_hex").as_string())),
        trust);
    ASSERT_TRUE(from_binary.verified) << from_binary.reason;
    EXPECT_EQ(
        boost::json::serialize(xprv::to_json(from_binary)),
        boost::json::serialize(xprv::to_json(from_json)));
}

// A refusal names its layer and reports nothing as authenticated.
TEST(VerifyReport, RefusalNamesItsLayerAndReportsNoOutputs)
{
    auto const vector = positive_vector("positive-tx-in-anchor-parent");
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto const key =
        std::string(vector.at("trust").at("publisher_key").as_string());
    auto const xahau_key = xprv::NetworkConfig::for_network(21337).publisher_key;

    std::string const junk = R"({"format_version":)";
    auto const parse = xprv::verify_proof(
        std::vector<uint8_t>(junk.begin(), junk.end()),
        mainnet_trust(protocol, key));
    EXPECT_FALSE(parse.verified);
    EXPECT_EQ(parse.layer, "parse");

    // The right proof under another publisher's key.
    auto const trust = xprv::verify_proof(
        json_bytes(vector.at("form").at("json")),
        mainnet_trust(protocol, xahau_key));
    EXPECT_FALSE(trust.verified);
    EXPECT_EQ(trust.layer, "trust");

    // The target header changed: the anchor still verifies, and the header
    // it should support no longer hashes to the parent.
    auto proof = vector.at("form").at("json");
    auto& target = proof.at("steps").as_array().at(2).at("header").as_object();
    target["ledger_index"] = target.at("ledger_index").to_number<uint32_t>() + 1;
    auto const graph =
        xprv::verify_proof(json_bytes(proof), mainnet_trust(protocol, key));
    EXPECT_FALSE(graph.verified);
    EXPECT_EQ(graph.layer, "graph");
    for (auto const* refused : {&parse, &trust, &graph})
    {
        EXPECT_FALSE(refused->reason.empty());
        EXPECT_TRUE(refused->headers.empty());
        EXPECT_TRUE(refused->leaves.empty());
        auto const json = xprv::to_json(*refused);
        EXPECT_EQ(json.size(), 3u) << boost::json::serialize(json);
    }
}
