#include "xprv/hex-utils.h"
#include "xprv/proof-chain-binary.h"
#include "xprv/proof-chain-json.h"

#include <algorithm>
#include <boost/json.hpp>
#include <catl/shamap/binary-trie.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>
#include <stdexcept>
#include <unistd.h>
#include <variant>

namespace {
using Bytes = std::vector<uint8_t>;

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

boost::json::object
map_proof(boost::json::value trie)
{
    return {
        {"format_version", XPRV_FORMAT_VERSION},
        {"network_id", 0},
        {"steps",
         boost::json::array{boost::json::object{
             {"type", "map_proof"},
             {"tree", "state"},
             {"trie", std::move(trie)}}}}};
}

bool
verify_text(std::string const& text, std::string const& args = {});

bool
verify_text(std::string const& text, std::string const& args)
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

}  // namespace

TEST(ParsingStrictness, RejectsTrieRootThatIsNotAnInnerNode)
{
    EXPECT_THROW(
        xprv::from_json(map_proof(
            boost::json::array{std::string(64, 'A'), boost::json::object{}})),
        std::runtime_error);
    EXPECT_THROW(
        xprv::from_json(map_proof(boost::json::string(std::string(64, 'A')))),
        std::runtime_error);
}

// A trie may be 64 inner nodes deep (§2.3). The JSON parser must read one
// that deep; the node after that is refused by the trie rule, not by the
// parser's nesting limit.
TEST(ParsingStrictness, ReadsATrieOfFullDepth)
{
    auto const deep_trie_text = [](int inner_nodes) {
        // Every inner node sits on branch 0, so the leaf key is all zeros.
        std::string text =
            R"({"format_version":)" + std::to_string(XPRV_FORMAT_VERSION) +
            R"(,"network_id":0,"steps":[{"type":"map_proof","tree":"state","trie":)";
        for (int i = 0; i < inner_nodes; ++i)
            text += R"({"0":)";
        text += "[\"" + std::string(64, '0') +
            R"(",{"LedgerEntryType":"AccountRoot","Flags":0}])";
        text += std::string(static_cast<size_t>(inner_nodes), '}');
        return text + "}]}";
    };
    EXPECT_NO_THROW(xprv::from_json_text(deep_trie_text(64)));
    try
    {
        xprv::from_json_text(deep_trie_text(65));
        FAIL() << "a trie 65 inner nodes deep was read";
    }
    catch (std::runtime_error const& e)
    {
        EXPECT_NE(std::string(e.what()).find("depth exceeds"), std::string::npos)
            << e.what();
    }
}

TEST(ParsingStrictness, RejectsWrongDiagnosticDepth)
{
    EXPECT_THROW(
        xprv::from_json(map_proof(boost::json::object{{"__depth__", 8}})),
        std::runtime_error);
}

TEST(ParsingStrictness, RejectsNullBranch)
{
    EXPECT_THROW(
        xprv::from_json(map_proof(boost::json::object{{"A", nullptr}})),
        std::runtime_error);
}

TEST(ParsingStrictness, RejectsNumericBranch)
{
    EXPECT_THROW(
        xprv::from_json(map_proof(boost::json::object{{"A", 17}})),
        std::runtime_error);
}

TEST(ParsingStrictness, RejectsMalformedPlaceholder)
{
    EXPECT_THROW(
        xprv::from_json(map_proof(boost::json::object{{"A", "BAD"}})),
        std::runtime_error);
}

TEST(ParsingStrictness, RejectsExtraLeafElement)
{
    EXPECT_THROW(
        xprv::from_json(map_proof(boost::json::object{
            {"A",
             boost::json::array{
                 std::string(64, 'A'), boost::json::object{}, "ignored"}}})),
        std::runtime_error);
}

TEST(ParsingStrictness, RejectsLowercaseValidationKeys)
{
    auto proof = proof_json();
    auto& vals =
        proof.at("steps").as_array().front().at("validations").as_object();
    boost::json::object lower;
    for (auto const& kv : vals)
    {
        auto key = std::string(kv.key());
        std::transform(
            key.begin(), key.end(), key.begin(), [](unsigned char c) {
                return std::tolower(c);
            });
        lower[key] = kv.value();
    }
    vals = std::move(lower);
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);
}

TEST(ParsingStrictness, RejectsDuplicateRawBranchMembers)
{
    auto proof = proof_json();
    auto text = boost::json::serialize(proof);
    ASSERT_TRUE(verify_text(text));
    auto at = text.find("\"trie\":{");
    ASSERT_NE(at, std::string::npos);
    at += std::string("\"trie\":{").size();
    std::string branch;
    for (auto const& step : proof.at("steps").as_array())
    {
        if (step.at("type") != "map_proof")
            continue;
        for (auto const& kv : step.at("trie").as_object())
            if (kv.key().size() == 1)
            {
                branch = "\"" + std::string(kv.key()) + "\"";
                break;
            }
        break;
    }
    ASSERT_FALSE(branch.empty());
    text.insert(at, branch + ":\"" + std::string(64, '0') + "\",");
    EXPECT_FALSE(verify_text(text));
}

TEST(ParsingStrictness, RejectsDuplicateRawValidationMembers)
{
    auto proof = proof_json();
    auto key = std::string(proof.at("steps")
                               .as_array()
                               .front()
                               .at("validations")
                               .as_object()
                               .begin()
                               ->key());
    auto text = boost::json::serialize(proof);
    auto at = text.find("\"validations\":{") +
        std::string("\"validations\":{").size();
    text.insert(at, "\"" + key + "\":\"00\",");
    EXPECT_FALSE(verify_text(text));
}

// The binary anchor record's own rules (record order, an empty record,
// trailing bytes, an empty list) are shared vectors, run against both
// verifiers: tests/xprv/vectors/negative-anchor-*.json.

namespace {

// The first leaf ([key, data]) of the vector's last map proof of `tree`.
boost::json::array&
first_leaf(boost::json::value& proof, std::string_view tree)
{
    boost::json::object* found = nullptr;
    for (auto& step : proof.at("steps").as_array())
        if (step.at("type") == "map_proof" &&
            step.at("tree").as_string() == tree)
            found = &step.as_object();
    if (!found)
        throw std::runtime_error("vector has no such map proof");
    boost::json::value* node = &found->at("trie");
    while (node->is_object())
    {
        boost::json::value* next = nullptr;
        for (auto& kv : node->as_object())
            if (kv.key() != "__depth__" && !kv.value().is_string())
            {
                next = &kv.value();
                break;
            }
        if (!next)
            throw std::runtime_error("map proof has no leaf");
        node = next;
    }
    return node->as_array();
}

}  // namespace

// §2, §9.1: a bare steps array has no version to check.
TEST(ParsingStrictness, RejectsBareStepsArray)
{
    auto const steps = proof_json().at("steps");
    EXPECT_THROW(xprv::from_json(steps), std::runtime_error);
    EXPECT_FALSE(verify_text(boost::json::serialize(steps)));
}

// D12: the JSON form is closed, at every level.
TEST(ParsingStrictness, RejectsUnknownMembersAtEveryLevel)
{
    ASSERT_NO_THROW(xprv::from_json(proof_json()));
    auto const with_extra = [](auto&& pick) {
        auto proof = proof_json();
        pick(proof)["note"] = "not part of the proof";
        return proof;
    };
    auto const steps = [](boost::json::value& p) -> boost::json::array& {
        return p.at("steps").as_array();
    };
    EXPECT_THROW(
        xprv::from_json(
            with_extra([](auto& p) -> auto& { return p.as_object(); })),
        std::runtime_error);
    EXPECT_THROW(
        xprv::from_json(with_extra(
            [&](auto& p) -> auto& { return steps(p).at(0).as_object(); })),
        std::runtime_error);
    EXPECT_THROW(
        xprv::from_json(with_extra(
            [&](auto& p) -> auto& { return steps(p).at(1).as_object(); })),
        std::runtime_error);
    EXPECT_THROW(
        xprv::from_json(with_extra([&](auto& p) -> auto& {
            return steps(p).at(1).at("header").as_object();
        })),
        std::runtime_error);
    EXPECT_THROW(
        xprv::from_json(with_extra(
            [&](auto& p) -> auto& { return steps(p).at(2).as_object(); })),
        std::runtime_error);
}

// D12: an integer field is an integer token; 7.0 is not one.
TEST(ParsingStrictness, RejectsNonIntegerNumbers)
{
    auto version = proof_json();
    version.as_object()["format_version"] = double(XPRV_FORMAT_VERSION);
    EXPECT_THROW(xprv::from_json(version), std::runtime_error);

    auto index = proof_json();
    auto& header = index.at("steps").as_array().at(1).at("header").as_object();
    header["ledger_index"] = double(header.at("ledger_index").as_int64());
    EXPECT_THROW(xprv::from_json(index), std::runtime_error);
}

// §2.3: leaf data is the decoded content and nothing else. The codec skips
// names it doesn't serialize, so each of these would be shown, never hashed.
TEST(ParsingStrictness, RejectsLeafMembersTheCodecWouldSkip)
{
    ASSERT_TRUE(verify_text(boost::json::serialize(proof_json())));
    auto const rejects = [](auto&& mutate) {
        auto proof = proof_json();
        mutate(proof);
        return !verify_text(boost::json::serialize(proof));
    };
    EXPECT_TRUE(rejects([](auto& p) {
        first_leaf(p, "state").at(1).as_object()["DisplayedButNotHashed"] = 1;
    }));
    EXPECT_TRUE(rejects([](auto& p) {
        first_leaf(p, "state").at(1).as_object()["index"] =
            first_leaf(p, "state").at(0);
    }));
    EXPECT_TRUE(rejects([](auto& p) {
        first_leaf(p, "tx").at(1).at("tx").as_object()["hash"] =
            first_leaf(p, "tx").at(0);
    }));
    EXPECT_TRUE(rejects([](auto& p) {
        first_leaf(p, "tx").at(1).at("meta").as_object()["not_a_field"] = "1";
    }));
    EXPECT_TRUE(rejects([](auto& p) {
        first_leaf(p, "tx").at(1).as_object()["hash"] =
            first_leaf(p, "tx").at(0);
    }));
    EXPECT_TRUE(rejects([](auto& p) {
        auto& nodes =
            first_leaf(p, "tx").at(1).at("meta").at("AffectedNodes").as_array();
        auto& wrapper = nodes.at(0).as_object();
        wrapper.begin()->value().as_object()["DisplayedButNotHashed"] = 1;
    }));
}

// A map proof without a leaf, in either form, is a shared vector:
// tests/xprv/vectors/negative-leafless-map*.json.

namespace {

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

    // An issue is an object, as rippled writes it; a bare string is not one.
    EXPECT_FALSE(leaf_accepted(tx_leaf_with("Asset", "XRP")));
    EXPECT_TRUE(leaf_accepted(tx_leaf_with("Asset", {{"currency", "XRP"}})));
    // XAH is the native code on Xahau only. On XRPL it is an issued currency
    // like any other: valid with an issuer, in an amount and in an issue,
    // and not a native issue without one.
    EXPECT_TRUE(leaf_accepted(tx_leaf_with(
        "Amount", {{"value", "1"}, {"currency", "XAH"}, {"issuer", issuer}})));
    EXPECT_TRUE(leaf_accepted(
        tx_leaf_with("Asset", {{"currency", "XAH"}, {"issuer", issuer}})));
    EXPECT_FALSE(leaf_accepted(tx_leaf_with("Asset", {{"currency", "XAH"}})));
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
        {"LockingChainIssue", {{"currency", "XRP"}}},
        {"IssuingChainDoor", issuer},
        {"IssuingChainIssue", {{"currency", "XRP"}}}};
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

// §8: the shared loader caps the text before either parse pass.
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
