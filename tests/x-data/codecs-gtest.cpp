#include <boost/json.hpp>
#include <catl/shamap/shamap.h>
#include <catl/xdata/codecs/codecs.h>
#include <catl/xdata/hex.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/serialize.h>
#include <fstream>
#include <gtest/gtest.h>

using namespace catl::xdata;
using namespace catl::xdata::codecs;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static std::string
to_upper_hex(std::span<const uint8_t> data)
{
    static const char hex_chars[] = "0123456789ABCDEF";
    std::string result;
    result.reserve(data.size() * 2);
    for (auto b : data)
    {
        result.push_back(hex_chars[b >> 4]);
        result.push_back(hex_chars[b & 0xF]);
    }
    return result;
}

static boost::json::value
load_json_fixture(std::string const& filename)
{
    std::string path =
        std::string(PROJECT_ROOT) + "/tests/x-data/fixture/" + filename;
    std::ifstream f(path);
    if (!f.good())
    {
        throw std::runtime_error("Cannot find fixture: " + path);
    }
    std::string content(
        (std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    return boost::json::parse(content);
}

// ---------------------------------------------------------------------------
// Fixture data holder (loaded once)
// ---------------------------------------------------------------------------

class FixtureData
{
public:
    static FixtureData const&
    instance()
    {
        static FixtureData data;
        return data;
    }

    boost::json::value fixtures;     // our generated fixtures
    boost::json::value data_driven;  // Nicholas's xrpl.js fixtures
    boost::json::value x_codec;      // X-address fixtures
    Protocol protocol;

private:
    FixtureData()
        : fixtures(load_json_fixture("codec-fixtures.json"))
        , data_driven(load_json_fixture("data-driven-tests.json"))
        , x_codec(load_json_fixture("x-codec-fixtures.json"))
        , protocol(Protocol::load_embedded_xrpl_protocol())
    {
    }
};

// ---------------------------------------------------------------------------
// Parameterized test: STObject encode matches rippled
// ---------------------------------------------------------------------------

struct STObjectFixture
{
    std::string name;
    boost::json::value json;
    std::string expected_hex;
};

class STObjectCodecTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(STObjectCodecTest, EncodeMatchesRippled)
{
    auto const& data = FixtureData::instance();
    auto const& entries = data.fixtures.as_object().at("stobject").as_array();
    auto const& entry = entries[GetParam()].as_object();

    auto const& json_val = entry.at("json");
    auto expected_hex = std::string(entry.at("hex").as_string());

    auto result = serialize_object(json_val.as_object(), data.protocol);
    auto result_hex = to_upper_hex(result);

    EXPECT_EQ(result_hex, expected_hex)
        << "Fixture: " << entry.at("name").as_string();
}

INSTANTIATE_TEST_SUITE_P(
    Fixtures,
    STObjectCodecTest,
    ::testing::Range(
        size_t(0),
        FixtureData::instance()
            .fixtures.as_object()
            .at("stobject")
            .as_array()
            .size()),
    [](auto const& info) -> std::string {
        auto const& entries = FixtureData::instance()
                                  .fixtures.as_object()
                                  .at("stobject")
                                  .as_array();
        return std::string(
            entries[info.param].as_object().at("name").as_string());
    });

// ---------------------------------------------------------------------------
// Parameterized test: Amount encode matches rippled
// ---------------------------------------------------------------------------

class AmountCodecTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(AmountCodecTest, EncodeMatchesRippled)
{
    auto const& data = FixtureData::instance();
    auto const& entries = data.fixtures.as_object().at("amount").as_array();
    auto const& entry = entries[GetParam()].as_object();

    auto const& json_val = entry.at("json");
    auto expected_hex = std::string(entry.at("hex").as_string());

    std::vector<uint8_t> buf;
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);
    AmountCodec::encode(s, json_val);

    auto result_hex = to_upper_hex(buf);
    EXPECT_EQ(result_hex, expected_hex)
        << "Fixture: " << entry.at("name").as_string();
}

INSTANTIATE_TEST_SUITE_P(
    Fixtures,
    AmountCodecTest,
    ::testing::Range(
        size_t(0),
        FixtureData::instance()
            .fixtures.as_object()
            .at("amount")
            .as_array()
            .size()),
    [](auto const& info) -> std::string {
        auto const& entries = FixtureData::instance()
                                  .fixtures.as_object()
                                  .at("amount")
                                  .as_array();
        return std::string(
            entries[info.param].as_object().at("name").as_string());
    });

// ---------------------------------------------------------------------------
// Parameterized test: Currency encode matches rippled
// ---------------------------------------------------------------------------

class CurrencyCodecTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(CurrencyCodecTest, EncodeMatchesRippled)
{
    auto const& data = FixtureData::instance();
    auto const& entries = data.fixtures.as_object().at("currency").as_array();
    auto const& entry = entries[GetParam()].as_object();

    auto const& json_val = entry.at("json");
    auto expected_hex = std::string(entry.at("hex").as_string());

    std::vector<uint8_t> buf;
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);
    CurrencyCodec::encode(s, std::string_view(json_val.as_string()));

    auto result_hex = to_upper_hex(buf);
    EXPECT_EQ(result_hex, expected_hex)
        << "Fixture: " << entry.at("name").as_string();
}

INSTANTIATE_TEST_SUITE_P(
    Fixtures,
    CurrencyCodecTest,
    ::testing::Range(
        size_t(0),
        FixtureData::instance()
            .fixtures.as_object()
            .at("currency")
            .as_array()
            .size()),
    [](auto const& info) -> std::string {
        auto const& entries = FixtureData::instance()
                                  .fixtures.as_object()
                                  .at("currency")
                                  .as_array();
        return std::string(
            entries[info.param].as_object().at("name").as_string());
    });

// ---------------------------------------------------------------------------
// Parameterized test: Issue encode matches rippled
// ---------------------------------------------------------------------------

class IssueCodecTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(IssueCodecTest, EncodeMatchesRippled)
{
    auto const& data = FixtureData::instance();
    auto const& entries = data.fixtures.as_object().at("issue").as_array();
    auto const& entry = entries[GetParam()].as_object();

    auto const& json_val = entry.at("json");
    auto expected_hex = std::string(entry.at("hex").as_string());

    std::vector<uint8_t> buf;
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);
    IssueCodec::encode(s, json_val);

    auto result_hex = to_upper_hex(buf);
    EXPECT_EQ(result_hex, expected_hex)
        << "Fixture: " << entry.at("name").as_string();
}

INSTANTIATE_TEST_SUITE_P(
    Fixtures,
    IssueCodecTest,
    ::testing::Range(
        size_t(0),
        FixtureData::instance()
            .fixtures.as_object()
            .at("issue")
            .as_array()
            .size()),
    [](auto const& info) -> std::string {
        auto const& entries =
            FixtureData::instance().fixtures.as_object().at("issue").as_array();
        return std::string(
            entries[info.param].as_object().at("name").as_string());
    });

// ---------------------------------------------------------------------------
// Unit tests (non-fixture, for specific codec behaviors)
// ---------------------------------------------------------------------------

TEST(CodecUnit, FieldHeader_CommonTypeCommonField)
{
    std::vector<uint8_t> buf;
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);

    // UInt32 (type=2) Sequence (field=4) → single byte 0x24
    s.add_field_header(2, 4);
    ASSERT_EQ(buf.size(), 1u);
    EXPECT_EQ(buf[0], 0x24);
}

TEST(CodecUnit, FieldHeader_UncommonField)
{
    std::vector<uint8_t> buf;
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);

    // UInt32 (type=2) field=16 → two bytes
    s.add_field_header(2, 16);
    ASSERT_EQ(buf.size(), 2u);
    EXPECT_EQ(buf[0], 0x20);  // type=2, field=0 (overflow)
    EXPECT_EQ(buf[1], 16);
}

TEST(CodecUnit, VLPrefix_SingleByte)
{
    std::vector<uint8_t> buf;
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);

    s.add_vl_prefix(100);
    ASSERT_EQ(buf.size(), 1u);
    EXPECT_EQ(buf[0], 100);
}

TEST(CodecUnit, VLPrefix_TwoByte)
{
    std::vector<uint8_t> buf;
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);

    s.add_vl_prefix(200);
    ASSERT_EQ(buf.size(), 2u);
}

TEST(CodecUnit, AddHexStreaming)
{
    std::vector<uint8_t> buf;
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);

    s.add_hex("DEADBEEF");
    ASSERT_EQ(buf.size(), 4u);
    EXPECT_EQ(buf[0], 0xDE);
    EXPECT_EQ(buf[1], 0xAD);
    EXPECT_EQ(buf[2], 0xBE);
    EXPECT_EQ(buf[3], 0xEF);
}

TEST(CodecUnit, EncodedSizeMatchesActual)
{
    auto const& data = FixtureData::instance();
    auto const& entries = data.fixtures.as_object().at("stobject").as_array();

    for (auto const& entry : entries)
    {
        auto const& json_val = entry.as_object().at("json");
        if (!json_val.is_object() || json_val.as_object().empty())
            continue;

        auto result = serialize_object(json_val.as_object(), data.protocol);
        size_t predicted = STObjectCodec::encoded_size(json_val, data.protocol);

        EXPECT_EQ(predicted, result.size())
            << "encoded_size mismatch for: "
            << entry.as_object().at("name").as_string();
    }
}

// ===========================================================================
// Data-driven tests from xrpl.js (ripple-binary-codec)
// ===========================================================================

// ---------------------------------------------------------------------------
// Field header encoding tests
// ---------------------------------------------------------------------------

class DDFieldHeaderTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(DDFieldHeaderTest, FieldHeaderMatchesExpected)
{
    auto const& data = FixtureData::instance();
    auto const& entries =
        data.data_driven.as_object().at("fields_tests").as_array();
    auto const& entry = entries[GetParam()].as_object();

    auto type_code = static_cast<uint16_t>(
        entry.at("type").is_uint64()
            ? entry.at("type").as_uint64()
            : static_cast<uint64_t>(entry.at("type").as_int64()));
    auto nth = static_cast<uint16_t>(
        entry.at("nth_of_type").is_uint64()
            ? entry.at("nth_of_type").as_uint64()
            : static_cast<uint64_t>(entry.at("nth_of_type").as_int64()));
    auto expected_hex = std::string(entry.at("expected_hex").as_string());

    std::vector<uint8_t> buf;
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);
    s.add_field_header(type_code, nth);

    auto result_hex = to_upper_hex(buf);
    EXPECT_EQ(result_hex, expected_hex)
        << "Field: " << entry.at("name").as_string();
}

INSTANTIATE_TEST_SUITE_P(
    DataDriven,
    DDFieldHeaderTest,
    ::testing::Range(
        size_t(0),
        FixtureData::instance()
            .data_driven.as_object()
            .at("fields_tests")
            .as_array()
            .size()),
    [](auto const& info) -> std::string {
        auto const& entries = FixtureData::instance()
                                  .data_driven.as_object()
                                  .at("fields_tests")
                                  .as_array();
        return std::string(
            entries[info.param].as_object().at("name").as_string());
    });

// ---------------------------------------------------------------------------
// Value encoding tests (amounts, etc.)
// ---------------------------------------------------------------------------

class DDValueTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(DDValueTest, ValueMatchesExpected)
{
    auto const& data = FixtureData::instance();
    auto const& entries =
        data.data_driven.as_object().at("values_tests").as_array();
    auto const& entry = entries[GetParam()].as_object();

    // Skip error cases (expected to fail)
    if (entry.contains("error"))
    {
        GTEST_SKIP() << "Error case: " << entry.at("error").as_string();
    }

    auto const& test_json = entry.at("test_json");
    auto expected_hex = std::string(entry.at("expected_hex").as_string());
    auto type_name = std::string(entry.at("type").as_string());

    std::vector<uint8_t> buf;
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);

    if (type_name == "Amount")
    {
        AmountCodec::encode(s, test_json);
    }
    else
    {
        GTEST_SKIP() << "Unhandled type: " << type_name;
    }

    auto result_hex = to_upper_hex(buf);
    EXPECT_EQ(result_hex, expected_hex)
        << "Type: " << type_name
        << " JSON: " << boost::json::serialize(test_json);
}

INSTANTIATE_TEST_SUITE_P(
    DataDriven,
    DDValueTest,
    ::testing::Range(
        size_t(0),
        FixtureData::instance()
            .data_driven.as_object()
            .at("values_tests")
            .as_array()
            .size()),
    [](auto const& info) -> std::string {
        auto const& entries = FixtureData::instance()
                                  .data_driven.as_object()
                                  .at("values_tests")
                                  .as_array();
        auto const& e = entries[info.param].as_object();
        return "val_" + std::to_string(info.param) + "_" +
            std::string(e.at("type").as_string());
    });

// ---------------------------------------------------------------------------
// Whole object encoding tests
// ---------------------------------------------------------------------------

class DDWholeObjectTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(DDWholeObjectTest, EncodeMatchesExpected)
{
    auto const& data = FixtureData::instance();
    auto const& entries =
        data.data_driven.as_object().at("whole_objects").as_array();
    auto const& entry = entries[GetParam()].as_object();

    auto const& tx_json = entry.at("tx_json");
    auto expected_hex =
        std::string(entry.at("blob_with_no_signing").as_string());

    auto result = serialize_object(tx_json.as_object(), data.protocol, false);
    auto result_hex = to_upper_hex(result);

    EXPECT_EQ(result_hex, expected_hex)
        << "Object[" << GetParam() << "] "
        << tx_json.as_object().at("TransactionType").as_string();
}

INSTANTIATE_TEST_SUITE_P(
    DataDriven,
    DDWholeObjectTest,
    ::testing::Range(
        size_t(0),
        FixtureData::instance()
            .data_driven.as_object()
            .at("whole_objects")
            .as_array()
            .size()),
    [](auto const& info) -> std::string {
        auto const& entries = FixtureData::instance()
                                  .data_driven.as_object()
                                  .at("whole_objects")
                                  .as_array();
        auto tx_type = std::string(entries[info.param]
                                       .as_object()
                                       .at("tx_json")
                                       .as_object()
                                       .at("TransactionType")
                                       .as_string());
        return tx_type + "_" + std::to_string(info.param);
    });

// ===========================================================================
// X-address tests: rjson and xjson must produce identical binary
// ===========================================================================

class XAddressTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(XAddressTest, XJsonMatchesRJson)
{
    auto const& data = FixtureData::instance();
    auto const& entries =
        data.x_codec.as_object().at("transactions").as_array();
    auto const& entry = entries[GetParam()].as_object();

    auto const& rjson = entry.at("rjson").as_object();
    auto const& xjson = entry.at("xjson").as_object();

    // rjson encodes without X-address expansion
    auto rjson_bytes = serialize_object(rjson, data.protocol);

    // xjson needs X-address expansion enabled
    auto xproto =
        Protocol::load_embedded_xrpl_protocol({.expand_xaddresses = true});
    auto xjson_bytes = serialize_object(xjson, xproto);

    auto r_hex = to_upper_hex(rjson_bytes);
    auto x_hex = to_upper_hex(xjson_bytes);

    EXPECT_EQ(r_hex, x_hex)
        << "X-address expansion mismatch for fixture " << GetParam();
}

INSTANTIATE_TEST_SUITE_P(
    XCodec,
    XAddressTest,
    ::testing::Range(
        size_t(0),
        FixtureData::instance()
            .x_codec.as_object()
            .at("transactions")
            .as_array()
            .size()),
    [](auto const& info) -> std::string {
        return "xaddr_" + std::to_string(info.param);
    });

// ===========================================================================
// Round-trip tests: JSON → binary → JSON → binary (must match)
// ===========================================================================

class RoundTripTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(RoundTripTest, EncodeDecodeRoundTrip)
{
    auto const& data = FixtureData::instance();
    auto const& entries =
        data.data_driven.as_object().at("whole_objects").as_array();
    auto const& entry = entries[GetParam()].as_object();

    auto const& tx_json = entry.at("tx_json");

    // Encode JSON → binary
    auto bytes1 = serialize_object(tx_json.as_object(), data.protocol);

    // Decode binary → JSON via parser + JsonVisitor
    Slice slice(bytes1.data(), bytes1.size());
    JsonVisitor visitor(data.protocol);
    ParserContext ctx(slice);
    parse_with_visitor(ctx, data.protocol, visitor);
    auto decoded_json = visitor.get_result();

    // Re-encode decoded JSON → binary
    auto bytes2 = serialize_object(decoded_json.as_object(), data.protocol);

    // Must match
    auto hex1 = to_upper_hex(bytes1);
    auto hex2 = to_upper_hex(bytes2);
    EXPECT_EQ(hex1, hex2)
        << "Round-trip mismatch for "
        << tx_json.as_object().at("TransactionType").as_string();
}

INSTANTIATE_TEST_SUITE_P(
    RoundTrip,
    RoundTripTest,
    ::testing::Range(
        size_t(0),
        FixtureData::instance()
            .data_driven.as_object()
            .at("whole_objects")
            .as_array()
            .size()),
    [](auto const& info) -> std::string {
        auto const& entries = FixtureData::instance()
                                  .data_driven.as_object()
                                  .at("whole_objects")
                                  .as_array();
        auto tx_type = std::string(entries[info.param]
                                       .as_object()
                                       .at("tx_json")
                                       .as_object()
                                       .at("TransactionType")
                                       .as_string());
        return "rt_" + tx_type + "_" + std::to_string(info.param);
    });

// ===========================================================================
// Full ledger SHAMap tests: serialize all entries, build SHAMap, verify root
// ===========================================================================

using catl::shamap::DefaultNodeTraits;
using catl::shamap::SHAMapT;
using catl::shamap::tnACCOUNT_STATE;
using catl::shamap::tnTRANSACTION_MD;

static Hash256
hash_from_hex_str(std::string_view hex)
{
    Hash256 h;
    for (size_t i = 0; i < 32; ++i)
    {
        h.data()[i] = hex_byte(hex[i * 2], hex[i * 2 + 1]);
    }
    return h;
}

static void
run_ledger_test(std::string const& fixture_file, Protocol const& protocol)
{
    auto ledger = load_json_fixture(fixture_file);
    auto const& obj = ledger.as_object();

    auto expected_account_hash =
        hash_from_hex_str(obj.at("account_hash").as_string());
    auto expected_tx_hash =
        hash_from_hex_str(obj.at("transaction_hash").as_string());

    // Build account state SHAMap
    {
        SHAMapT<DefaultNodeTraits> map(tnACCOUNT_STATE);
        for (auto const& entry : obj.at("accountState").as_array())
        {
            auto const& sle = entry.as_object();
            auto key = hash_from_hex_str(sle.at("index").as_string());
            auto bytes = serialize_object(sle, protocol);
            Slice s(bytes.data(), bytes.size());
            auto item =
                boost::intrusive_ptr<MmapItem>(OwnedItem::create(key, s));
            map.add_item(item);
        }
        auto computed = map.get_hash();
        EXPECT_EQ(computed, expected_account_hash)
            << "Account state hash mismatch for " << fixture_file;
    }

    // Build TX SHAMap
    {
        SHAMapT<DefaultNodeTraits> map(tnTRANSACTION_MD);
        for (auto const& entry : obj.at("transactions").as_array())
        {
            auto const& tx = entry.as_object();
            auto key = hash_from_hex_str(tx.at("hash").as_string());

            // TX tree item = VL(tx_bytes) + VL(meta_bytes)
            // tx fields are at top level (minus hash/metaData)
            // metaData is a nested object
            boost::json::object tx_only;
            for (auto const& [k, v] : tx)
            {
                if (k != "hash" && k != "metaData")
                    tx_only[k] = v;
            }

            boost::json::object wrapper;
            wrapper["tx"] = tx_only;
            wrapper["meta"] = tx.at("metaData");

            auto bytes = serialize_transaction(wrapper, protocol);
            Slice s(bytes.data(), bytes.size());
            auto item =
                boost::intrusive_ptr<MmapItem>(OwnedItem::create(key, s));
            map.add_item(item);
        }
        auto computed = map.get_hash();
        EXPECT_EQ(computed, expected_tx_hash)
            << "Transaction hash mismatch for " << fixture_file;
    }
}

// ---------------------------------------------------------------------------
// SLE encoding tests — every entry from ledger 38129 vs rippled
// ---------------------------------------------------------------------------

class SLECodecTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(SLECodecTest, EncodeMatchesRippled)
{
    static auto sle_fixtures = load_json_fixture("sle-fixtures.json");
    auto const& data = FixtureData::instance();
    auto const& entries = sle_fixtures.as_array();
    auto const& entry = entries[GetParam()].as_object();

    auto const& json_val = entry.at("json").as_object();
    auto expected_hex = std::string(entry.at("hex").as_string());

    auto result = serialize_object(json_val, data.protocol);
    auto result_hex = to_upper_hex(result);

    EXPECT_EQ(result_hex, expected_hex)
        << "SLE type: " << entry.at("type").as_string()
        << " index: " << json_val.at("index").as_string();
}

INSTANTIATE_TEST_SUITE_P(
    SLE,
    SLECodecTest,
    ::testing::Range(
        size_t(0),
        []() -> size_t {
            static auto f = load_json_fixture("sle-fixtures.json");
            return f.as_array().size();
        }()),
    [](auto const& info) -> std::string {
        static auto f = load_json_fixture("sle-fixtures.json");
        auto const& e = f.as_array()[info.param].as_object();
        return "sle_" + std::string(e.at("type").as_string()) + "_" +
            std::to_string(info.param);
    });

// ===========================================================================
// Codec round-trip fixtures from xrpl.js — {binary, json} pairs
// ===========================================================================

class CodecRoundtripTxTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(CodecRoundtripTxTest, EncodeMatchesBinary)
{
    static auto fixtures = load_json_fixture("codec-roundtrip-fixtures.json");
    auto const& data = FixtureData::instance();
    auto const& entries = fixtures.as_object().at("transactions").as_array();
    auto const& entry = entries[GetParam()].as_object();

    auto const& tx_json = entry.at("json").as_object();
    auto expected_hex = std::string(entry.at("binary").as_string());

    auto result = serialize_object(tx_json, data.protocol);
    auto result_hex = to_upper_hex(result);

    EXPECT_EQ(result_hex, expected_hex)
        << "TX[" << GetParam() << "] "
        << tx_json.at("TransactionType").as_string();
}

INSTANTIATE_TEST_SUITE_P(
    CodecRoundtrip,
    CodecRoundtripTxTest,
    ::testing::Range(
        size_t(0),
        []() -> size_t {
            static auto f = load_json_fixture("codec-roundtrip-fixtures.json");
            return f.as_object().at("transactions").as_array().size();
        }()),
    [](auto const& info) -> std::string {
        static auto f = load_json_fixture("codec-roundtrip-fixtures.json");
        auto const& e = f.as_object()
                            .at("transactions")
                            .as_array()[info.param]
                            .as_object();
        auto tt = std::string(e.at("json").as_object().at("TransactionType").as_string());
        return "crt_" + tt + "_" + std::to_string(info.param);
    });

class CodecRoundtripSLETest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(CodecRoundtripSLETest, EncodeMatchesBinary)
{
    static auto fixtures = load_json_fixture("codec-roundtrip-fixtures.json");
    auto const& data = FixtureData::instance();
    auto const& entries = fixtures.as_object().at("accountState").as_array();
    auto const& entry = entries[GetParam()].as_object();

    auto const& sle_json = entry.at("json").as_object();
    auto expected_hex = std::string(entry.at("binary").as_string());

    auto result = serialize_object(sle_json, data.protocol);
    auto result_hex = to_upper_hex(result);

    EXPECT_EQ(result_hex, expected_hex)
        << "SLE[" << GetParam() << "] "
        << sle_json.at("LedgerEntryType").as_string();
}

INSTANTIATE_TEST_SUITE_P(
    CodecRoundtrip,
    CodecRoundtripSLETest,
    ::testing::Range(
        size_t(0),
        []() -> size_t {
            static auto f = load_json_fixture("codec-roundtrip-fixtures.json");
            return f.as_object().at("accountState").as_array().size();
        }()),
    [](auto const& info) -> std::string {
        static auto f = load_json_fixture("codec-roundtrip-fixtures.json");
        auto const& e = f.as_object()
                            .at("accountState")
                            .as_array()[info.param]
                            .as_object();
        auto let = std::string(e.at("json").as_object().at("LedgerEntryType").as_string());
        return "crs_" + let + "_" + std::to_string(info.param);
    });

TEST(LedgerSHAMap, Ledger38129)
{
    auto const& data = FixtureData::instance();
    run_ledger_test("ledger-full-38129.json", data.protocol);
}

TEST(LedgerSHAMap, Ledger40000)
{
    auto const& data = FixtureData::instance();
    run_ledger_test("ledger-full-40000.json", data.protocol);
}

// ===========================================================================
// Transaction type fixtures from xrpl.js — encode tx_json, compare to binary
// ===========================================================================

class TxTypeTest : public ::testing::TestWithParam<size_t>
{
};

TEST_P(TxTypeTest, EncodeMatchesBinary)
{
    static auto tx_fixtures = load_json_fixture("tx-type-fixtures.json");
    auto const& data = FixtureData::instance();
    auto const& entries = tx_fixtures.as_array();
    auto const& entry = entries[GetParam()].as_object();

    auto const& tx_json = entry.at("tx_json").as_object();
    auto expected_hex = std::string(entry.at("binary").as_string());

    auto result = serialize_object(tx_json, data.protocol);
    auto result_hex = to_upper_hex(result);

    EXPECT_EQ(result_hex, expected_hex)
        << "Fixture: " << entry.at("name").as_string();
}

INSTANTIATE_TEST_SUITE_P(
    TxTypes,
    TxTypeTest,
    ::testing::Range(
        size_t(0),
        []() -> size_t {
            static auto f = load_json_fixture("tx-type-fixtures.json");
            return f.as_array().size();
        }()),
    [](auto const& info) -> std::string {
        static auto f = load_json_fixture("tx-type-fixtures.json");
        auto name = std::string(
            f.as_array()[info.param].as_object().at("name").as_string());
        // GTest doesn't allow dashes in param names
        std::replace(name.begin(), name.end(), '-', '_');
        return name;
    });
