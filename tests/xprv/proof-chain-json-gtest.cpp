#include "xprv/proof-chain-json.h"

#include <boost/json.hpp>
#include <gtest/gtest.h>

namespace {

boost::json::object
header_proof_json()
{
    xprv::ProofChain chain;
    xprv::HeaderData header;
    header.seq = 1;
    header.drops = 100;
    chain.steps.emplace_back(header);
    return xprv::to_json(chain);
}

boost::json::object&
header_fields(boost::json::object& proof)
{
    return proof.at("steps")
        .as_array()
        .front()
        .as_object()
        .at("header")
        .as_object();
}

}  // namespace

TEST(ProofChainJson, RejectsUnknownOrMalformedSteps)
{
    auto proof = header_proof_json();
    proof.at("steps").as_array().emplace_back(
        boost::json::object{{"type", "unknown"}});
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);

    proof = header_proof_json();
    proof.at("steps").as_array().emplace_back("not-an-object");
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);
}

TEST(ProofChainJson, RejectsMalformedHeaderNumbers)
{
    auto proof = header_proof_json();
    header_fields(proof)["total_coins"] = "100junk";
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);

    proof = header_proof_json();
    header_fields(proof)["close_time_resolution"] = 256;
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);

    proof = header_proof_json();
    header_fields(proof)["close_flags"] = 256;
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);
}

TEST(ProofChainJson, RejectsUnknownTreeAndMissingVersion)
{
    xprv::ProofChain chain;
    xprv::TrieData trie;
    trie.trie_json = boost::json::object{};
    chain.steps.emplace_back(std::move(trie));
    auto proof = xprv::to_json(chain);
    proof.at("steps").as_array().front().as_object()["tree"] = "other";
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);

    proof = header_proof_json();
    proof.erase("format_version");
    EXPECT_THROW(xprv::from_json(proof), std::exception);
}

// 2055 interim rule: JSON leaves are decoded only. A trie whose leaf this
// server can't decode and re-encode exactly has no JSON form; the builder
// records why, and asking for JSON raises JsonFormUnavailable with a hint.

#include "xprv/hex-utils.h"
#include "xprv/proof-builder.h"

#include <catl/core/types.h>

#include <boost/intrusive_ptr.hpp>
#include <catl/xdata/serialize.h>

#include <fstream>
#include <sstream>

namespace {

boost::json::array const&
first_leaf(boost::json::value const& node)
{
    if (node.is_array())
        return node.as_array();
    for (auto const& kv : node.as_object())
    {
        if (kv.value().is_object() || kv.value().is_array())
            return first_leaf(kv.value());
    }
    throw std::runtime_error("trie has no leaf");
}

}  // namespace

TEST(ProofChainJson, TrieWithoutJsonFormRefusesJsonWithHint)
{
    xprv::TrieData trie;
    trie.json_unavailable = "tx leaf AB can't be decoded";
    trie.trie_binary = {0x00};
    try
    {
        xprv::step_to_json(xprv::ChainStep{trie});
        FAIL() << "expected JsonFormUnavailable";
    }
    catch (xprv::JsonFormUnavailable const& e)
    {
        EXPECT_EQ(e.reason(), "tx leaf AB can't be decoded");
        EXPECT_NE(std::string(e.what()).find("format=bin"), std::string::npos);
    }
}

TEST(ProofChainJson, LeafCallbackDecodesRoundTrippingLeaf)
{
    std::ifstream in(
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/proof.json");
    ASSERT_TRUE(in.is_open());
    std::stringstream buffer;
    buffer << in.rdbuf();
    auto const proof = boost::json::parse(buffer.str());
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();

    boost::json::value const* tx_trie = nullptr;
    for (auto const& step : proof.at("steps").as_array())
    {
        if (step.at("type") == "map_proof" && step.at("tree") == "tx")
            tx_trie = &step.at("trie");
    }
    ASSERT_NE(tx_trie, nullptr);
    auto const& leaf = first_leaf(*tx_trie);
    auto const bytes = catl::xdata::serialize_transaction(
        leaf.at(1).as_object(), protocol);
    auto const key = xprv::hash_from_hex(std::string(leaf.at(0).as_string()));
    boost::intrusive_ptr<MmapItem> const item(
        OwnedItem::create(key, Slice(bytes.data(), bytes.size())));

    auto const callback = xprv::make_proof_leaf_callback(protocol, true);
    auto const rendered = callback(*item);
    auto const reencoded = catl::xdata::serialize_transaction(
        rendered.as_array().at(1).as_object(), protocol);
    EXPECT_EQ(reencoded, bytes);
}

TEST(ProofChainJson, LeafCallbackRefusesUndecodableLeaf)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    std::vector<uint8_t> garbage{0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x02};
    auto const key = xprv::hash_from_hex(std::string(64, 'A'));
    boost::intrusive_ptr<MmapItem> const item(
        OwnedItem::create(key, Slice(garbage.data(), garbage.size())));

    auto const callback = xprv::make_proof_leaf_callback(protocol, false);
    EXPECT_THROW(callback(*item), xprv::JsonFormUnavailable);
}
