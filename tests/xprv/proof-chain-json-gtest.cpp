#include "xprv/proof-chain-json.h"
#include "xprv/hex-utils.h"
#include "xprv/network-config.h"

#include <catl/shamap/binary-trie.h>
#include <catl/xdata/serialize.h>

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


namespace {

std::vector<uint8_t>
one_leaf_trie(int branch, Hash256 const& key, std::vector<uint8_t> const& data)
{
    uint32_t header = 0;
    catl::shamap::set_branch_type(
        header, branch, catl::shamap::BranchType::leaf);
    std::vector<uint8_t> out;
    catl::shamap::write_u32_le(out, header);
    out.insert(out.end(), key.data(), key.data() + 32);
    catl::shamap::leb128_encode(out, data.size());
    out.insert(out.end(), data.begin(), data.end());
    return out;
}

xprv::ProofChain
chain_with_binary_trie(
    uint32_t network_id,
    xprv::TrieData::TreeType tree,
    std::vector<uint8_t> binary)
{
    xprv::ProofChain chain;
    chain.network_id = network_id;
    xprv::TrieData trie;
    trie.tree = tree;
    trie.trie_binary = std::move(binary);
    chain.steps.emplace_back(std::move(trie));
    return chain;
}

boost::json::object
ledger_hashes_sle()
{
    boost::json::object sle;
    sle["LedgerEntryType"] = "LedgerHashes";
    sle["Flags"] = 0;
    sle["LastLedgerSequence"] = 1;
    boost::json::array hashes;
    hashes.emplace_back(std::string(64, 'A'));
    sle["Hashes"] = std::move(hashes);
    return sle;
}

}  // namespace

TEST(ProofChainJson, BinaryTrieDecodesToObjectLeaf)
{
    auto const protocol = xprv::NetworkConfig::for_network(0).load_protocol();
    auto const bytes = catl::xdata::serialize_leaf(ledger_hashes_sle(), protocol);
    auto const key = xprv::hash_from_hex(std::string(64, 'A'));
    auto chain = chain_with_binary_trie(
        0,
        xprv::TrieData::TreeType::state,
        one_leaf_trie(10, key, bytes));

    auto const proof = xprv::to_json(chain);
    auto const& trie = proof.at("steps")
                           .as_array()
                           .front()
                           .as_object()
                           .at("trie")
                           .as_object();
    EXPECT_EQ(trie.at("__depth__").as_int64(), 0);
    auto const& leaf = trie.at("A").as_array();
    EXPECT_EQ(std::string(leaf.at(0).as_string()), xprv::upper_hex(key));
    EXPECT_TRUE(leaf.at(1).is_object());
    EXPECT_FALSE(leaf.at(1).is_string());
    EXPECT_EQ(
        std::string(leaf.at(1).as_object().at("LedgerEntryType").as_string()),
        "LedgerHashes");
    EXPECT_FALSE(leaf.at(1).as_object().contains("index"));
    EXPECT_FALSE(leaf.at(1).as_object().contains("blob"));
}

TEST(ProofChainJson, BinaryTrieUndecodableLeafFails)
{
    auto const key = xprv::hash_from_hex(std::string(64, 'B'));
    std::vector<uint8_t> junk{0xFF, 0x01};
    auto chain = chain_with_binary_trie(
        0, xprv::TrieData::TreeType::state, one_leaf_trie(11, key, junk));
    try
    {
        xprv::to_json(chain);
        FAIL() << "undecodable leaf was written";
    }
    catch (std::exception const& e)
    {
        auto const message = std::string(e.what());
        EXPECT_NE(message.find("did not decode"), std::string::npos) << message;
        EXPECT_EQ(message.find("did not re-encode"), std::string::npos) << message;
    }
}

TEST(ProofChainJson, BinaryTrieRoundTripMismatchFails)
{
    auto const protocol = xprv::NetworkConfig::for_network(0).load_protocol();
    auto bytes = catl::xdata::serialize_leaf(ledger_hashes_sle(), protocol);
    bytes.push_back(0x00);
    auto const key = xprv::hash_from_hex(std::string(64, 'C'));
    auto chain = chain_with_binary_trie(
        0, xprv::TrieData::TreeType::state, one_leaf_trie(12, key, bytes));
    try
    {
        xprv::to_json(chain);
        FAIL() << "non-canonical leaf was written";
    }
    catch (std::exception const& e)
    {
        auto const message = std::string(e.what());
        EXPECT_NE(message.find("did not re-encode"), std::string::npos) << message;
    }
}

TEST(ProofChainJson, ExistingJsonTrieIsLeftAlone)
{
    xprv::ProofChain chain;
    xprv::TrieData trie;
    trie.tree = xprv::TrieData::TreeType::state;
    trie.trie_json = boost::json::object{{"__depth__", 0}};
    trie.trie_binary = {0xFF, 0xFF, 0xFF, 0xFF};
    chain.steps.emplace_back(std::move(trie));
    auto const proof = xprv::to_json(chain);
    auto const& node = proof.at("steps")
                           .as_array()
                           .front()
                           .as_object()
                           .at("trie")
                           .as_object();
    EXPECT_EQ(node.at("__depth__").as_int64(), 0);
    EXPECT_EQ(node.size(), 1u);
}

TEST(ProofChainJson, XahauBinaryLeafUsesXahauProtocol)
{
    auto const xahau = xprv::NetworkConfig::for_network(21337).load_protocol();
    boost::json::object tx;
    tx["TransactionType"] = "Invoke";
    tx["Account"] = "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh";
    tx["Fee"] = "12";
    tx["Sequence"] = 1;
    boost::json::object meta;
    meta["TransactionIndex"] = 0;
    meta["TransactionResult"] = "tesSUCCESS";
    boost::json::object leaf;
    leaf["tx"] = tx;
    leaf["meta"] = meta;
    auto const bytes = catl::xdata::serialize_transaction(leaf, xahau);

    auto const key = xprv::hash_from_hex(std::string(64, 'D'));
    auto const binary = one_leaf_trie(13, key, bytes);

    auto tx_type = [](xprv::ProofChain& chain) -> boost::json::value {
        auto const proof = xprv::to_json(chain);
        return proof.at("steps")
            .as_array()
            .front()
            .as_object()
            .at("trie")
            .as_object()
            .at("D")
            .as_array()
            .at(1)
            .as_object()
            .at("tx")
            .as_object()
            .at("TransactionType");
    };

    // XRPL definitions have no Invoke name. The same bytes still re-encode,
    // and the type comes back as its numeric code rather than the name.
    auto xrpl_chain = chain_with_binary_trie(
        0, xprv::TrieData::TreeType::tx, binary);
    auto const xrpl_type = tx_type(xrpl_chain);
    EXPECT_FALSE(xrpl_type.is_string());
    EXPECT_EQ(xrpl_type.to_number<int>(), 99);

    auto xahau_chain = chain_with_binary_trie(
        21337, xprv::TrieData::TreeType::tx, binary);
    auto const xahau_type = tx_type(xahau_chain);
    EXPECT_EQ(std::string(xahau_type.as_string()), "Invoke");
}

// D3: a JSON anchor may omit its validator list, the counterpart of a binary
// proof without 0x05. The list is bundled whole or not at all.

#include "xprv/anchor-verifier.h"
#include "xprv/proof-chain-binary.h"

namespace {

boost::json::object
list_less_fixture()
{
    std::ifstream in(
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/proof.json");
    std::stringstream buffer;
    buffer << in.rdbuf();
    auto proof = boost::json::parse(buffer.str()).as_object();
    auto& unl = proof.at("steps").as_array().front().at("unl").as_object();
    unl.erase("manifest");
    unl.erase("blob");
    unl.erase("signature");
    return proof;
}

}  // namespace

TEST(ProofChainJson, ListLessAnchorRoundTripsThroughJsonAndBinary)
{
    auto const proof = list_less_fixture();
    auto const chain = xprv::from_json(proof);
    auto const& anchor = std::get<xprv::AnchorData>(chain.steps.front());
    EXPECT_TRUE(anchor.blob.empty());

    auto const json = xprv::to_json(chain);
    auto const& unl =
        json.at("steps").as_array().front().at("unl").as_object();
    EXPECT_EQ(unl.size(), 1u);
    EXPECT_TRUE(unl.contains("public_key"));

    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    xprv::BinaryOptions opts;
    opts.protocol = &protocol;
    auto const bin = xprv::to_binary(chain, opts);
    auto const back = xprv::from_binary(bin);
    EXPECT_EQ(
        xprv::to_json(back).at("steps").as_array().front().at("unl"),
        json.at("steps").as_array().front().at("unl"));
}

TEST(ProofChainJson, PartlyBundledListIsRejected)
{
    auto proof = list_less_fixture();
    proof.at("steps").as_array().front().at("unl").as_object()["blob"] = "AB";
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);
}

TEST(ProofChainJson, UnknownUnlMemberIsRejected)
{
    auto proof = list_less_fixture();
    proof.at("steps").as_array().front().at("unl").as_object()["extra"] = "00";
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);
}

TEST(ProofChainJson, ListLessAnchorNeedsAnExternalList)
{
    auto const proof = list_less_fixture();
    auto const& anchor = proof.at("steps").as_array().front().as_object();
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    std::vector<std::string> narrative;
    auto const result = xprv::AnchorVerifier::verify(
        anchor,
        std::string(anchor.at("unl").at("public_key").as_string()),
        protocol,
        narrative);
    EXPECT_FALSE(result.verified);
    EXPECT_EQ(result.error, "validator list not bundled and no external list");
}

// v7 §2.3: a hex string is not JSON leaf data, at load and at conversion.
namespace {

boost::json::array&
first_leaf_mut(boost::json::value& node)
{
    if (node.is_array())
        return node.as_array();
    for (auto& kv : node.as_object())
    {
        if (kv.value().is_object() || kv.value().is_array())
            return first_leaf_mut(kv.value());
    }
    throw std::runtime_error("trie has no leaf");
}

boost::json::value
hex_leaf_proof()
{
    std::ifstream in(
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/proof.json");
    std::stringstream buffer;
    buffer << in.rdbuf();
    auto proof = boost::json::parse(buffer.str());
    for (auto& step : proof.at("steps").as_array())
    {
        if (step.at("type") == "map_proof" && step.at("tree") == "tx")
        {
            first_leaf_mut(step.at("trie")).at(1) = "ABCDEF";
            break;
        }
    }
    return proof;
}

}  // namespace

TEST(ProofChainJson, HexLeafIsRejectedAtLoad)
{
    auto const text = boost::json::serialize(hex_leaf_proof());
    try
    {
        xprv::from_json_text(text);
        FAIL() << "expected a hex leaf to be rejected";
    }
    catch (std::runtime_error const& e)
    {
        EXPECT_NE(std::string(e.what()).find("not hex"), std::string::npos)
            << e.what();
    }
}

TEST(ProofChainJson, HexLeafIsRejectedByBinaryConversion)
{
    // The loader refuses this already; build the chain in memory so the
    // encoder's own refusal is exercised too.
    auto const proof = hex_leaf_proof();
    xprv::ProofChain chain;
    chain.network_id = 0;
    for (auto const& step : proof.at("steps").as_array())
    {
        if (step.at("type") == "map_proof" && step.at("tree") == "tx")
        {
            xprv::TrieData trie;
            trie.tree = xprv::TrieData::TreeType::tx;
            trie.trie_json = step.at("trie");
            chain.steps.push_back(trie);
        }
    }
    ASSERT_EQ(chain.steps.size(), 1u);
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    xprv::BinaryOptions opts;
    opts.protocol = &protocol;
    EXPECT_THROW(xprv::to_binary(chain, opts), std::runtime_error);
}

// Binary→JSON conversion is bounded like the JSON grammar: no inner node at
// depth 64 or deeper (codex, v7 review).
namespace {

std::vector<uint8_t>
nested_binary_trie(int inner_nodes)
{
    // Each inner node uses branch 0: inner (0b10) for all but the last, whose
    // branch 0 is a hash placeholder (0b11). Header is a u32 little-endian
    // with 2 bits per branch.
    std::vector<uint8_t> out;
    for (int i = 0; i < inner_nodes; ++i)
    {
        uint32_t const header = (i + 1 < inner_nodes) ? 0b10u : 0b11u;
        for (int b = 0; b < 4; ++b)
            out.push_back(static_cast<uint8_t>(header >> (8 * b)));
    }
    out.insert(out.end(), 32, 0xAB);
    return out;
}

xprv::ProofChain
chain_with_binary_trie(std::vector<uint8_t> trie)
{
    xprv::ProofChain chain;
    chain.network_id = 0;
    xprv::TrieData t;
    t.tree = xprv::TrieData::TreeType::state;
    t.trie_binary = std::move(trie);
    chain.steps.push_back(t);
    return chain;
}

}  // namespace

TEST(ProofChainJson, BinaryTrieConversionIsDepthBounded)
{
    EXPECT_NO_THROW(xprv::to_json(chain_with_binary_trie(nested_binary_trie(64))));
    EXPECT_THROW(
        xprv::to_json(chain_with_binary_trie(nested_binary_trie(65))),
        std::runtime_error);
}

// Witness for the builder's exact re-encode check (codex, v7 review F2): a
// leaf that decodes but does not re-encode to its own bytes (here, a trailing
// byte the codec drops) has no JSON form.
TEST(ProofChainJson, LeafCallbackRefusesLeafThatDoesNotReencodeExactly)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto bytes = catl::xdata::serialize_leaf(ledger_hashes_sle(), protocol);
    bytes.push_back(0);
    auto const key = xprv::hash_from_hex(std::string(64, 'A'));
    boost::intrusive_ptr<MmapItem> const item(
        OwnedItem::create(key, Slice(bytes.data(), bytes.size())));
    auto const callback = xprv::make_proof_leaf_callback(protocol, false);
    EXPECT_THROW(callback(*item), xprv::JsonFormUnavailable);
}
