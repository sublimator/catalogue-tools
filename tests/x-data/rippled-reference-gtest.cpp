// Holds catl::xdata to rippled's own codec.
//
// Every expectation in fixture/rippled-reference-fixtures.json was printed by
// xrpl-codec, rippled's libxrpl on the command line; none is derived by hand
// or from x-data. generate-rippled-reference-fixtures.py regenerates the file.
//
// rippled is the reference for the JSON form of currencies, issues, amounts
// and Number fields. Where rippled refuses an input, x-data refuses it too.
// The few places x-data deliberately does something else are marked in the
// fixture with "xdata" and a reason, and asserted here as such.
//
// The oracle is XRPL only. The Xahau rule (XAH is the native code there) is
// in the XahauNative tests below, matched to the xahaud source by reading.

#include <boost/json.hpp>
#include <catl/xdata/codecs/codecs.h>
#include <catl/xdata/hex.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/serialize.h>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <vector>

using namespace catl::xdata;
using namespace catl::xdata::codecs;

namespace {

boost::json::object const&
reference()
{
    static auto const data = [] {
        std::string const path = std::string(PROJECT_ROOT) +
            "/tests/x-data/fixture/rippled-reference-fixtures.json";
        std::ifstream f(path);
        if (!f.good())
            throw std::runtime_error("Cannot find fixture: " + path);
        std::string content(
            (std::istreambuf_iterator<char>(f)),
            std::istreambuf_iterator<char>());
        return boost::json::parse(content).as_object();
    }();
    return data;
}

boost::json::array const&
section(char const* name)
{
    return reference().at(name).as_array();
}

std::vector<uint8_t>
bytes_of(boost::json::value const& hex)
{
    auto const text = std::string_view(hex.as_string());
    std::vector<uint8_t> out(text.size() / 2);
    for (size_t i = 0; i < out.size(); ++i)
        out[i] = hex_byte(text[i * 2], text[i * 2 + 1]);
    return out;
}

std::string
upper_hex(std::vector<uint8_t> const& bytes)
{
    return hex_encode(bytes.data(), bytes.size());
}

// One encode expectation: the bytes rippled wrote, or a refusal. An entry
// with "xdata" states what x-data does instead of rippled, and why.
template <typename Encode>
void
expect_encodes(boost::json::object const& entry, Encode const& encode)
{
    auto const what = boost::json::serialize(
        entry.contains("json") ? entry.at("json") : entry.at("text"));

    std::vector<uint8_t> buf;
    VectorSink vs(buf);
    Serializer<VectorSink> s(vs);

    bool refuse = entry.contains("refused");
    std::string expected =
        refuse ? std::string() : std::string(entry.at("hex").as_string());
    if (auto const* xdata = entry.if_contains("xdata"))
    {
        refuse = xdata->as_string() == "refuses";
        if (!refuse)
            expected = std::string(xdata->as_string());
    }

    if (refuse)
    {
        EXPECT_THROW(encode(s), EncodeError) << what;
        return;
    }
    ASSERT_NO_THROW(encode(s)) << what;
    EXPECT_EQ(upper_hex(buf), expected) << what;
}

// rippled writes "type" into each path step. x-data, xrpl.js and xrpl-py do
// not, and x-data's encoder takes the type from the members present. The
// comparison is of everything else.
boost::json::value
without_step_type(boost::json::value paths)
{
    for (auto& path : paths.as_array())
        for (auto& step : path.as_array())
            step.as_object().erase("type");
    return paths;
}

}  // namespace

// ─── currency ────────────────────────────────────────────────────────

// Every byte value in each position of a three-character code, the native
// and reserved forms, and hex: to_string(Currency).
TEST(RippledReference, CurrencyText)
{
    for (auto const& e : section("currency_decode"))
    {
        auto const bytes = bytes_of(e.at("hex"));
        auto const got =
            CurrencyCodec::decode(Slice(bytes.data(), bytes.size()));
        EXPECT_EQ(
            std::string(got.as_string()), std::string(e.at("text").as_string()))
            << e.at("hex").as_string();
    }
}

TEST(RippledReference, CurrencyFromText)
{
    for (auto const& e : section("currency_encode"))
    {
        auto const text = std::string(e.at("text").as_string());
        expect_encodes(e.as_object(), [&](auto& s) {
            CurrencyCodec::encode(s, std::string_view(text));
        });
    }
}

// What rippled writes for a currency, it reads back to the same bytes. The
// one exception is rippled's own: noCurrency reads as "1", and "1" is not a
// currency it accepts.
TEST(RippledReference, CurrencyTextReadsBack)
{
    for (auto const& e : section("currency_decode"))
    {
        auto const text = std::string(e.at("text").as_string());
        if (text == "1")
            continue;
        uint8_t raw[20];
        ASSERT_NO_THROW(CurrencyCodec::to_bytes(text, raw)) << text;
        EXPECT_EQ(hex_encode(raw, 20), std::string(e.at("hex").as_string()))
            << text;
    }
}

// ─── issue ───────────────────────────────────────────────────────────

// Issue::setJson: always an object; the native issue has no issuer.
TEST(RippledReference, IssueJson)
{
    for (auto const& e : section("issue_decode"))
    {
        auto const bytes = bytes_of(e.at("hex"));
        EXPECT_EQ(
            IssueCodec::decode(Slice(bytes.data(), bytes.size())), e.at("json"))
            << e.at("hex").as_string();
    }
}

// issueFromJson: a bare string is not an issue, a currency without an issuer
// is native only as the native code, and the native code takes no issuer.
TEST(RippledReference, IssueFromJson)
{
    for (auto const& e : section("issue_encode"))
        expect_encodes(e.as_object(), [&](auto& s) {
            IssueCodec::encode(s, e.at("json"));
        });
}

// ─── amount ──────────────────────────────────────────────────────────

// STAmount::getText across every exponent: both notation cutovers, zero,
// negatives, and mantissas with and without trailing zeros.
TEST(RippledReference, AmountJson)
{
    for (auto const& e : section("amount_decode"))
    {
        auto const bytes = bytes_of(e.at("hex"));
        EXPECT_EQ(
            AmountCodec::decode(Slice(bytes.data(), bytes.size())),
            e.at("json"))
            << e.at("hex").as_string();
    }
}

// amountFromJson: the number grammar, the native code never in an issued
// amount, and rounding to nearest with ties to even.
TEST(RippledReference, AmountFromJson)
{
    for (auto const& e : section("amount_encode"))
        expect_encodes(e.as_object(), [&](auto& s) {
            AmountCodec::encode(s, e.at("json"));
        });
}

// The JSON of an amount encodes back to the bytes it came from, for every
// amount rippled could read. Where rippled reads bytes it would not itself
// write, the fixture says what it writes and x-data writes the same.
TEST(RippledReference, AmountJsonReadsBack)
{
    for (auto const& e : section("amount_decode"))
    {
        std::vector<uint8_t> buf;
        VectorSink vs(buf);
        Serializer<VectorSink> s(vs);
        ASSERT_NO_THROW(AmountCodec::encode(s, e.at("json")))
            << boost::json::serialize(e.at("json"));
        auto const* rewritten = e.as_object().if_contains("rippled_writes");
        EXPECT_EQ(
            upper_hex(buf),
            std::string((rewritten ? *rewritten : e.at("hex")).as_string()));
    }
}

// ─── number ──────────────────────────────────────────────────────────

// to_string(Number) of the value rippled builds from the wire mantissa and
// exponent: normalisation, both cutovers, trailing-zero mantissas.
TEST(RippledReference, NumberText)
{
    for (auto const& e : section("number_decode"))
    {
        auto const bytes = bytes_of(e.at("hex"));
        auto const got = NumberCodec::decode(Slice(bytes.data(), bytes.size()));
        EXPECT_EQ(
            std::string(got.as_string()), std::string(e.at("text").as_string()))
            << e.at("hex").as_string();
    }
}

// numberFromJson: the number grammar, and refusal of a decimal the wire form
// cannot hold exactly.
TEST(RippledReference, NumberFromText)
{
    for (auto const& e : section("number_encode"))
        expect_encodes(e.as_object(), [&](auto& s) {
            NumberCodec::encode(s, e.at("text"));
        });
}

// ─── path set and bridge ─────────────────────────────────────────────

TEST(RippledReference, PathSetJson)
{
    for (auto const& e : section("pathset_decode"))
    {
        auto const bytes = bytes_of(e.at("hex"));
        EXPECT_EQ(
            PathSetCodec::decode(Slice(bytes.data(), bytes.size())),
            without_step_type(e.at("json")))
            << e.at("hex").as_string();
    }
}

TEST(RippledReference, PathSetFromJson)
{
    for (auto const& e : section("pathset_encode"))
        expect_encodes(e.as_object(), [&](auto& s) {
            PathSetCodec::encode(s, e.at("json"));
        });
}

TEST(RippledReference, BridgeJson)
{
    for (auto const& e : section("bridge_decode"))
    {
        auto const bytes = bytes_of(e.at("hex"));
        EXPECT_EQ(
            XChainBridgeCodec::decode(Slice(bytes.data(), bytes.size())),
            e.at("json"))
            << e.at("hex").as_string();
    }
}

TEST(RippledReference, BridgeFromJson)
{
    for (auto const& e : section("bridge_encode"))
        expect_encodes(e.as_object(), [&](auto& s) {
            XChainBridgeCodec::encode(s, e.at("json"));
        });
}

// ─── UInt64: measured, not yet matched ───────────────────────────────
//
// rippled writes an ordinary UInt64 as lowercase hex without padding
// ("OwnerNode": "1"), and as decimal for the four fields it marks base-ten
// (MaximumAmount, OutstandingAmount, MPTAmount, LockedAmount). x-data writes
// 16 uppercase hex digits for all of them, and reads all of them as hex, so
// for the base-ten fields it also reads a different number than rippled does
// from the same text.
//
// Changing that changes what every catl tool prints, so it waits for a
// decision. These tests state rippled's behaviour and are disabled until then;
// run them with --gtest_also_run_disabled_tests to see the gap.

namespace {

boost::json::value
decode_object(std::vector<uint8_t> const& bytes, Protocol const& protocol)
{
    Slice slice(bytes.data(), bytes.size());
    JsonVisitor visitor(protocol, {.ascii_hints = false});
    ParserContext ctx(slice);
    parse_with_visitor(ctx, protocol, visitor);
    return visitor.get_result();
}

}  // namespace

TEST(RippledReference, DISABLED_UInt64Text)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    for (auto const& e : section("uint64_decode"))
    {
        EXPECT_EQ(decode_object(bytes_of(e.at("hex")), protocol), e.at("json"))
            << e.at("hex").as_string();
    }
}

TEST(RippledReference, DISABLED_UInt64FromText)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    for (auto const& e : section("uint64_encode"))
    {
        std::vector<uint8_t> bytes;
        ASSERT_NO_THROW(
            bytes = serialize_object(e.at("json").as_object(), protocol))
            << boost::json::serialize(e.at("json"));
        EXPECT_EQ(upper_hex(bytes), std::string(e.at("hex").as_string()))
            << boost::json::serialize(e.at("json"));
    }
}

// ─── the live XRPL cases ─────────────────────────────────────────────
//
// XAH issued on XRPL by rswh1fvyLqHizBS2awu1vs6QcmwTBd9qiv is an ordinary
// three-character code there. x-data used to read it as native, so a proof of
// any transaction touching it had no JSON form.

namespace {

constexpr char const* kXahIssuer = "rswh1fvyLqHizBS2awu1vs6QcmwTBd9qiv";

}  // namespace

TEST(XrplIssuedXah, OfferTakerGetsRoundTrips)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    EXPECT_EQ(protocol.native_currency(), "XRP");

    boost::json::object offer{
        {"TransactionType", "OfferCreate"},
        {"TakerPays", "1000000"},
        {"TakerGets",
         boost::json::object{
             {"value", "130000"},
             {"currency", "XAH"},
             {"issuer", kXahIssuer}}}};
    auto const bytes = serialize_object(offer, protocol);

    // TakerGets: field header 0x65, then the value rippled writes for it
    // (amount_encode in the reference fixture).
    auto const hex = upper_hex(bytes);
    EXPECT_NE(
        hex.find("65D5C49E57D6354000"
                 "0000000000000000000000005841480000000000"
                 "17A73888B98222E485D927BCDFD63FC3895AD2AE"),
        std::string::npos)
        << hex;
    EXPECT_EQ(decode_object(bytes, protocol), boost::json::value(offer));
}

TEST(XrplIssuedXah, PathStepRoundTrips)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    boost::json::object payment{
        {"TransactionType", "Payment"},
        {"Paths",
         boost::json::array{boost::json::array{boost::json::object{
             {"currency", "XAH"}, {"issuer", kXahIssuer}}}}}};
    auto const bytes = serialize_object(payment, protocol);

    // Paths: field header 0x0112, then the step rippled writes
    // (pathset_encode in the reference fixture).
    auto const hex = upper_hex(bytes);
    EXPECT_NE(
        hex.find("011230"
                 "0000000000000000000000005841480000000000"
                 "17A73888B98222E485D927BCDFD63FC3895AD2AE00"),
        std::string::npos)
        << hex;
    EXPECT_EQ(decode_object(bytes, protocol), boost::json::value(payment));
}

// A native issue in a whole object reads as rippled writes it.
TEST(XrplIssuedXah, NativeIssueInAnObjectIsAnObject)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    boost::json::object amm{
        {"LedgerEntryType", "AMM"},
        {"Asset", boost::json::object{{"currency", "XRP"}}},
        {"Asset2",
         boost::json::object{{"currency", "XAH"}, {"issuer", kXahIssuer}}}};
    auto const bytes = serialize_object(amm, protocol);
    EXPECT_EQ(decode_object(bytes, protocol), boost::json::value(amm));

    // The bare string was x-data's own spelling. rippled refuses it.
    amm["Asset"] = "XRP";
    EXPECT_THROW(serialize_object(amm, protocol), EncodeError);
    // A currency without an issuer is not native unless it is the native code.
    amm["Asset"] = boost::json::object{{"currency", "XAH"}};
    EXPECT_THROW(serialize_object(amm, protocol), EncodeError);
}

// ─── Xahau ───────────────────────────────────────────────────────────
//
// No Xahau oracle is built. These follow the xahaud source by reading:
// systemCurrencyCode is "XAH" (include/xrpl/protocol/SystemParameters.h), and
// to_string(Currency), to_currency and Issue::setJson are otherwise rippled's
// (include/xrpl/protocol/UintTypes.h, src/libxrpl/protocol/Issue.cpp).

TEST(XahauNative, EmbeddedProtocolKnowsItsNativeCode)
{
    EXPECT_EQ(
        Protocol::load_embedded_xahau_protocol().native_currency(), "XAH");
    EXPECT_EQ(Protocol::load_embedded_xrpl_protocol().native_currency(), "XRP");
    EXPECT_EQ(
        Protocol::load_embedded_xrpl_protocol({.network_id = 1})
            .native_currency(),
        "XRP");
}

TEST(XahauNative, ZeroCurrencyIsXah)
{
    uint8_t zero[20] = {};
    uint8_t xrp_code[20] = {};
    xrp_code[12] = 'X';
    xrp_code[13] = 'R';
    xrp_code[14] = 'P';
    uint8_t xah_code[20] = {};
    xah_code[12] = 'X';
    xah_code[13] = 'A';
    xah_code[14] = 'H';

    // The zero currency is the native code of whichever network reads it.
    EXPECT_EQ(CurrencyCodec::to_text(zero, "XAH"), "XAH");
    EXPECT_EQ(CurrencyCodec::to_text(zero, "XRP"), "XRP");
    // The other network's symbol is an ordinary code. A network's own symbol
    // in the three-character form is reserved and reads as hex.
    EXPECT_EQ(CurrencyCodec::to_text(xrp_code, "XAH"), "XRP");
    EXPECT_EQ(
        CurrencyCodec::to_text(xah_code, "XAH"),
        "0000000000000000000000005841480000000000");
    EXPECT_EQ(CurrencyCodec::to_text(xah_code, "XRP"), "XAH");
    EXPECT_EQ(
        CurrencyCodec::to_text(xrp_code, "XRP"),
        "0000000000000000000000005852500000000000");

    uint8_t got[20];
    CurrencyCodec::to_bytes("XAH", got, {}, "XAH");
    EXPECT_TRUE(CurrencyCodec::is_zero(got));
    CurrencyCodec::to_bytes("XRP", got, {}, "XAH");
    EXPECT_EQ(hex_encode(got, 20), hex_encode(xrp_code, 20));
}

TEST(XahauNative, NativeIssueIsAnObjectNamingXah)
{
    uint8_t zero[20] = {};
    EXPECT_EQ(
        IssueCodec::decode(Slice(zero, 20), "XAH"),
        (boost::json::object{{"currency", "XAH"}}));

    auto const encode = [](boost::json::value const& v) {
        std::vector<uint8_t> buf;
        VectorSink vs(buf);
        Serializer<VectorSink> s(vs);
        IssueCodec::encode(s, v, {}, "XAH");
        return buf;
    };
    EXPECT_EQ(
        upper_hex(encode(boost::json::object{{"currency", "XAH"}})),
        std::string(40, '0'));
    // On Xahau XRP is not native, so it needs an issuer.
    EXPECT_THROW(encode(boost::json::object{{"currency", "XRP"}}), EncodeError);
    EXPECT_THROW(
        encode(
            boost::json::object{{"currency", "XAH"}, {"issuer", kXahIssuer}}),
        EncodeError);
    // xahaud's isBadCurrency reserves every case of its own code.
    EXPECT_THROW(
        encode(
            boost::json::object{{"currency", "xah"}, {"issuer", kXahIssuer}}),
        EncodeError);
    EXPECT_EQ(
        upper_hex(encode(
            boost::json::object{{"currency", "XRP"}, {"issuer", kXahIssuer}})),
        "0000000000000000000000005852500000000000"
        "17A73888B98222E485D927BCDFD63FC3895AD2AE");
}

TEST(XahauNative, AmountsAndPathsInAnObject)
{
    auto const protocol = Protocol::load_embedded_xahau_protocol();
    boost::json::object payment{
        {"TransactionType", "Payment"},
        {"Amount",
         boost::json::object{
             {"value", "1"}, {"currency", "XRP"}, {"issuer", kXahIssuer}}},
        {"Paths",
         boost::json::array{boost::json::array{
             boost::json::object{{"currency", "XAH"}},
             boost::json::object{
                 {"currency", "XRP"}, {"issuer", kXahIssuer}}}}}};
    auto const bytes = serialize_object(payment, protocol);
    auto const hex = upper_hex(bytes);

    // The path's XAH step is the zero currency; its XRP step is the code.
    EXPECT_NE(
        hex.find(
            "10" + std::string(40, '0') +
            "300000000000000000000000005852500000000000"),
        std::string::npos)
        << hex;
    EXPECT_EQ(decode_object(bytes, protocol), boost::json::value(payment));

    // XAH is native here: an issued amount cannot name it.
    payment["Amount"] = boost::json::object{
        {"value", "1"}, {"currency", "XAH"}, {"issuer", kXahIssuer}};
    EXPECT_THROW(serialize_object(payment, protocol), EncodeError);
}
