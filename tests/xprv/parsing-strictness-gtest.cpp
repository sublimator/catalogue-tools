#include "xprv/hex-utils.h"
#include "xprv/proof-chain-binary.h"
#include "xprv/proof-chain-json.h"

#include <algorithm>
#include <boost/json.hpp>
#include <catl/shamap/binary-trie.h>
#include <catl/xdata/protocol.h>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>
#include <unistd.h>

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

size_t
varint(Bytes const& bytes, size_t& pos)
{
    return catl::shamap::leb128_decode(bytes, pos);
}

struct CoreParts
{
    Bytes prefix, common;
    std::vector<Bytes> names, records;
};

Bytes
mutated_core(unsigned mutation)
{
    auto binary = xprv::from_hex(
        std::string(vector_data().at("form").at("binary_hex").as_string()));
    // The corpus vector is uncompressed; mutations keep every other TLV intact.
    if (binary.at(5) != 0)
        throw std::runtime_error("test vector must be uncompressed");
    size_t pos = 11;
    auto size = varint(binary, pos);
    auto end = pos + size;
    Bytes core(binary.begin() + pos, binary.begin() + end);
    CoreParts parts;
    parts.prefix.assign(core.begin(), core.begin() + 69);
    size_t at = 69;
    auto count = varint(core, at);
    auto common_start = at;
    auto common_size = varint(core, at);
    at += common_size;
    parts.common.assign(core.begin() + common_start, core.begin() + at);
    auto names = varint(core, at);
    for (size_t i = 0; i < names; ++i)
    {
        auto length = varint(core, at);
        parts.names.emplace_back(core.begin() + at, core.begin() + at + length);
        at += length;
    }
    for (size_t i = 0; i < count; ++i)
    {
        auto start = at;
        at += 33;
        auto length = varint(core, at);
        at += length;
        parts.records.emplace_back(core.begin() + start, core.begin() + at);
    }
    if (mutation == 0)
        std::fill(parts.names.at(0).begin(), parts.names.at(0).end(), 'Z');
    else if (mutation == 1)
        std::reverse(parts.names.begin(), parts.names.end());
    else if (mutation == 2)
        std::reverse(parts.records.begin(), parts.records.end());
    else
        parts.records.push_back(parts.records.front());
    core = parts.prefix;
    catl::shamap::leb128_encode(core, parts.records.size());
    core.insert(core.end(), parts.common.begin(), parts.common.end());
    catl::shamap::leb128_encode(core, parts.names.size());
    for (auto const& name : parts.names)
    {
        catl::shamap::leb128_encode(core, name.size());
        core.insert(core.end(), name.begin(), name.end());
    }
    for (auto const& record : parts.records)
        core.insert(core.end(), record.begin(), record.end());
    Bytes result(binary.begin(), binary.begin() + 11);
    catl::shamap::leb128_encode(result, core.size());
    result.insert(result.end(), core.begin(), core.end());
    result.insert(result.end(), binary.begin() + end, binary.end());
    return result;
}
}  // namespace

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

TEST(ParsingStrictness, RejectsUnknownDecompositionName)
{
    EXPECT_THROW(xprv::from_binary(mutated_core(0)), std::runtime_error);
}
TEST(ParsingStrictness, RejectsUnsortedDecompositionNames)
{
    EXPECT_THROW(xprv::from_binary(mutated_core(1)), std::runtime_error);
}
TEST(ParsingStrictness, RejectsUnsortedValidationRecords)
{
    EXPECT_THROW(xprv::from_binary(mutated_core(2)), std::runtime_error);
}
TEST(ParsingStrictness, RejectsDuplicateValidationRecords)
{
    EXPECT_THROW(xprv::from_binary(mutated_core(3)), std::runtime_error);
}

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
        xprv::from_json(with_extra([](auto& p) -> auto& { return p.as_object(); })),
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

// §3.8: a map proof authenticates at least one leaf. Replacing the tx leaf
// with its own recorded leaf hash keeps the root, and so the anchor, intact.
TEST(ParsingStrictness, RejectsMapProofWithoutLeaves)
{
    auto proof = proof_json();
    auto const& recorded =
        vector_data().at("intermediates").at("map_proofs").as_array().back();
    ASSERT_EQ(recorded.at("tree"), "tx");
    ASSERT_EQ(recorded.at("leaves").as_array().size(), 1u);
    auto const leaf_hash = recorded.at("leaves").at(0).at("leaf_hash");

    boost::json::value* node = nullptr;
    for (auto& step : proof.at("steps").as_array())
        if (step.at("type") == "map_proof" && step.at("tree") == "tx")
            node = &step.as_object()["trie"];
    ASSERT_NE(node, nullptr);
    while (node->is_object())
    {
        boost::json::value* next = nullptr;
        for (auto& kv : node->as_object())
            if (kv.key() != "__depth__" && !kv.value().is_string())
                next = &kv.value();
        ASSERT_NE(next, nullptr);
        node = next;
    }
    *node = leaf_hash;
    auto const text = boost::json::serialize(proof);
    EXPECT_FALSE(verify_text(text));

    auto chain = xprv::from_json_text(text);
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    xprv::BinaryOptions opts;
    opts.protocol = &protocol;
    auto const binary = xprv::to_binary(chain, opts);
    auto path = std::filesystem::temp_directory_path() /
        ("xprv-leafless-" + std::to_string(getpid()) + ".xprv.bin");
    {
        std::ofstream file(path, std::ios::binary);
        file.write(
            reinterpret_cast<char const*>(binary.data()),
            static_cast<std::streamsize>(binary.size()));
    }
    auto const status = std::system(
        (std::string("'") + XPRV_TEST_EXECUTABLE + "' verify '" + path.string() +
         "' >/dev/null 2>&1")
            .c_str());
    std::filesystem::remove(path);
    EXPECT_NE(status, 0);
}

namespace {

boost::json::object
tx_leaf_with(std::string const& name, boost::json::value value)
{
    boost::json::object tx{
        {"TransactionType", "Payment"},
        {"Account", "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh"}};
    tx[name] = std::move(value);
    return {{"tx", std::move(tx)}, {"meta", {{"TransactionResult", "tesSUCCESS"}}}};
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
    EXPECT_FALSE(leaf_accepted(
        tx_leaf_with("Paths", boost::json::array{boost::json::array{
                                  boost::json::object{}}})));

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

    EXPECT_FALSE(leaf_accepted(tx_leaf_with(
        "Amendments", boost::json::array{boost::json::object{}})));
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
