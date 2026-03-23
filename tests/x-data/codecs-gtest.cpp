#include <catl/xdata/codecs/codecs.h>
#include <catl/xdata/hex.h>
#include <catl/xdata/protocol.h>
#include <boost/json.hpp>
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
        (std::istreambuf_iterator<char>(f)),
        std::istreambuf_iterator<char>());
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

    boost::json::value fixtures;       // our generated fixtures
    boost::json::value data_driven;    // Nicholas's xrpl.js fixtures
    Protocol protocol;

private:
    FixtureData()
        : fixtures(load_json_fixture("codec-fixtures.json"))
        , data_driven(load_json_fixture("data-driven-tests.json"))
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
        return std::string(entries[info.param].as_object().at("name").as_string());
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
        return std::string(entries[info.param].as_object().at("name").as_string());
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
        return std::string(entries[info.param].as_object().at("name").as_string());
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
        auto const& entries = FixtureData::instance()
                                  .fixtures.as_object()
                                  .at("issue")
                                  .as_array();
        return std::string(entries[info.param].as_object().at("name").as_string());
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

TEST(CodecUnit, CountingSinkMatchesVector)
{
    auto const& data = FixtureData::instance();
    auto const& entries = data.fixtures.as_object().at("stobject").as_array();

    for (auto const& entry : entries)
    {
        auto const& json_val = entry.as_object().at("json");
        if (!json_val.is_object() || json_val.as_object().empty())
            continue;

        auto result = serialize_object(json_val.as_object(), data.protocol);

        CountingSink counter;
        Serializer<CountingSink> cs(counter);
        STObjectCodec::encode(cs, json_val, data.protocol);

        EXPECT_EQ(counter.count(), result.size())
            << "Counting mismatch for: "
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
    auto expected_hex = std::string(entry.at("blob_with_no_signing").as_string());

    auto result =
        serialize_object(tx_json.as_object(), data.protocol, false);
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
