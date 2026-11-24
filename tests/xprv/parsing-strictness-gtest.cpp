// What a proof's verdict cannot show about strict parsing.
//
// The parse rules themselves (trie node grammar, the closed JSON form, integer
// tokens, members written twice, leaf members the codec would skip, the binary
// anchor record) are shared vectors under tests/xprv/vectors, run against both
// verifiers by vectors-gtest.cpp and scripts/xprv-conformance.py.

#include "xprv/hex-utils.h"
#include "xprv/proof-chain-binary.h"
#include "xprv/proof-chain-json.h"

#include <boost/json.hpp>
#include <catl/xdata/protocol.h>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>
#include <stdexcept>
#include <unistd.h>

namespace {

boost::json::object const&
vector_data()
{
    static auto const data = [] {
        std::ifstream file(
            std::filesystem::path(PROJECT_ROOT) /
            "tests/xprv/vectors/positive-tx-skiplist.json");
        std::ostringstream text;
        text << file.rdbuf();
        return boost::json::parse(text.str()).as_object();
    }();
    return data;
}

boost::json::value
proof_json()
{
    return vector_data().at("form").at("json");
}

bool
verify_text(std::string const& text, std::string const& args = {})
{
    auto path = std::filesystem::temp_directory_path() /
        ("xprv-strict-json-" + std::to_string(getpid()) + ".json");
    {
        std::ofstream file(path);
        file << text;
    }
    auto quote = [](std::string const& value) {
        std::string out = "'";
        for (char c : value)
            out += c == '\'' ? "'\\''" : std::string(1, c);
        return out + "'";
    };
    auto command = quote(XPRV_TEST_EXECUTABLE) + " verify " +
        quote(path.string()) + " " + args + " >/dev/null 2>&1";
    auto status = std::system(command.c_str());
    std::filesystem::remove(path);
    return status == 0;
}

boost::json::object
tx_leaf_with(std::string const& name, boost::json::value value)
{
    boost::json::object tx{
        {"TransactionType", "Payment"},
        {"Account", "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"}};
    tx[name] = std::move(value);
    return {
        {"tx", std::move(tx)}, {"meta", {{"TransactionResult", "tesSUCCESS"}}}};
}

bool
leaf_accepted(boost::json::object const& leaf)
{
    static auto const protocol =
        catl::xdata::Protocol::load_embedded_xrpl_protocol();
    try
    {
        xprv::require_codec_members(leaf, true, protocol);
        return true;
    }
    catch (std::runtime_error const&)
    {
        return false;
    }
}

}  // namespace

// §2.3: a structured value's codec reads its members and skips the rest, so
// each variant carries exactly its own. An issue without an issuer is
// native, whatever currency it names.
TEST(ParsingStrictness, StructuredValuesCarryExactlyTheirMembers)
{
    auto const issuer = "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh";
    boost::json::object const iou{
        {"value", "1"}, {"currency", "USD"}, {"issuer", issuer}};
    boost::json::object iou_extra = iou;
    iou_extra["note"] = "x";
    EXPECT_TRUE(leaf_accepted(tx_leaf_with("Amount", "1000000")));
    EXPECT_TRUE(leaf_accepted(tx_leaf_with("Amount", iou)));
    EXPECT_FALSE(leaf_accepted(tx_leaf_with("Amount", iou_extra)));
    EXPECT_FALSE(leaf_accepted(tx_leaf_with(
        "Amount", {{"value", "1"}, {"currency", "XRP"}, {"issuer", issuer}})));

    EXPECT_TRUE(leaf_accepted(tx_leaf_with("Asset", "XRP")));
    EXPECT_TRUE(leaf_accepted(tx_leaf_with("Asset", {{"currency", "XRP"}})));
    EXPECT_TRUE(leaf_accepted(
        tx_leaf_with("Asset", {{"currency", "USD"}, {"issuer", issuer}})));
    EXPECT_FALSE(leaf_accepted(tx_leaf_with("Asset", {{"currency", "USD"}})));
    EXPECT_FALSE(leaf_accepted(tx_leaf_with(
        "Asset", {{"currency", "USD"}, {"issuer", issuer}, {"note", "x"}})));

    boost::json::array paths{boost::json::array{
        boost::json::object{{"currency", "USD"}, {"issuer", issuer}}}};
    EXPECT_TRUE(leaf_accepted(tx_leaf_with("Paths", paths)));
    paths[0].as_array()[0].as_object()["note"] = "x";
    EXPECT_FALSE(leaf_accepted(tx_leaf_with("Paths", paths)));
    EXPECT_FALSE(leaf_accepted(tx_leaf_with(
        "Paths",
        boost::json::array{boost::json::array{boost::json::object{}}})));

    boost::json::object bridge{
        {"LockingChainDoor", issuer},
        {"LockingChainIssue", "XRP"},
        {"IssuingChainDoor", issuer},
        {"IssuingChainIssue", "XRP"}};
    EXPECT_TRUE(leaf_accepted(tx_leaf_with("XChainBridge", bridge)));
    auto bridge_extra = bridge;
    bridge_extra["note"] = "x";
    EXPECT_FALSE(leaf_accepted(tx_leaf_with("XChainBridge", bridge_extra)));
    auto bridge_issue = bridge;
    bridge_issue["IssuingChainIssue"] = {{"currency", "USD"}};
    EXPECT_FALSE(leaf_accepted(tx_leaf_with("XChainBridge", bridge_issue)));

    EXPECT_FALSE(leaf_accepted(
        tx_leaf_with("Amendments", boost::json::array{boost::json::object{}})));
    EXPECT_FALSE(leaf_accepted(tx_leaf_with("Fee", {{"value", "10"}})));
}

// §8: the shared loader caps the text before either parse pass. No vector
// edit writes 16 MiB of JSON text.
TEST(ParsingStrictness, JsonTextCapIsInclusive)
{
    std::string text(xprv::XPRV_MAX_JSON_SIZE, ' ');
    try
    {
        xprv::from_json_text(text);
        FAIL() << "blank text is not a proof";
    }
    catch (std::exception const& e)
    {
        EXPECT_EQ(std::string(e.what()).find("exceeds"), std::string::npos)
            << e.what();
    }
    text.push_back(' ');
    try
    {
        xprv::from_json_text(text);
        FAIL() << "over-cap text was parsed";
    }
    catch (std::exception const& e)
    {
        EXPECT_NE(std::string(e.what()).find("exceeds"), std::string::npos)
            << e.what();
    }
}

// A network implies its built-in publisher key. An explicit key may replace
// it, but a key built in for one network is never trusted for another (§8).
// This is the command's trust configuration; a vector always names its key
// and its network.
TEST(ParsingStrictness, BuiltInKeyIsNotTrustedForAnotherNetwork)
{
    auto const text = boost::json::serialize(proof_json());
    std::string const xrpl =
        "ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734";
    EXPECT_TRUE(verify_text(text));
    EXPECT_TRUE(verify_text(text, xrpl));
    EXPECT_TRUE(verify_text(text, xrpl + " --network 0"));
    EXPECT_FALSE(verify_text(text, xrpl + " --network 21337"));

    // The proof relabelled to match: only the key/network binding refuses it,
    // since the proof's network and the configured one agree.
    auto relabelled = proof_json();
    relabelled.as_object()["network_id"] = 21337;
    EXPECT_FALSE(verify_text(
        boost::json::serialize(relabelled), xrpl + " --network 21337"));
}
