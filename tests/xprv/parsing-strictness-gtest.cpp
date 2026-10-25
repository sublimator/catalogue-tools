#include "xprv/hex-utils.h"
#include "xprv/proof-chain-binary.h"
#include "xprv/proof-chain-json.h"

#include <algorithm>
#include <boost/json.hpp>
#include <catl/shamap/binary-trie.h>
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
verify_text(std::string const& text)
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
        quote(path.string()) + " >/dev/null 2>&1";
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
