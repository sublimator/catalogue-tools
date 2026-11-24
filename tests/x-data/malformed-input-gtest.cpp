// Bytes a parser is handed are untrusted. Malformed ones are refused with an
// exception; they never crash the process and never leave the cursor outside
// the message.

#include <catl/core/types.h>
#include <catl/xdata/codec-error.h>
#include <catl/xdata/codecs/pathset.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/serializer.h>
#include <catl/xdata/slice-cursor.h>
#include <catl/xdata/slice-visitor.h>

#include <boost/json.hpp>
#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>
#include <vector>

namespace {

using namespace catl::xdata;

struct VisitEverything
{
    int fields = 0;
    void
    visit_field(FieldPath const&, FieldSlice const&)
    {
        ++fields;
    }
};

// Declines every array, so the parser skips them.
struct SkipArrays
{
    bool
    visit_array_start(FieldPath const&, FieldSlice const&)
    {
        return false;
    }
};

// Declines every object, so the parser skips them, and looks at what the
// parser says was skipped.
struct SkipObjects
{
    std::size_t seen = 0;
    bool
    visit_object_start(FieldPath const&, FieldSlice const&)
    {
        return false;
    }
    void
    visit_object_end(FieldPath const&, FieldSlice const& fs)
    {
        seen += fs.data.size();
    }
};

template <typename Visitor>
void
parse(std::vector<std::uint8_t> const& bytes, Protocol const& protocol)
{
    Slice slice(bytes.data(), bytes.size());
    SliceCursor cursor{slice, 0};
    ParserContext ctx{cursor};
    Visitor visitor;
    parse_with_visitor(ctx, protocol, visitor);
}

std::uint32_t
field_code_of(std::vector<std::uint8_t> const& header)
{
    Slice slice(header.data(), header.size());
    SliceCursor cursor{slice, 0};
    return read_field_header(cursor).second;
}

}  // namespace

// An STArray header followed by a field code the definitions do not have.
// The array loop looked the element up and used the result unchecked, so
// these two bytes ended the process (found by Neon's differential run at
// xprv 19a04df5; the same code was in every earlier release).
TEST(MalformedInput, UnknownArrayElementIsRefusedNotDereferenced)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    std::vector<std::uint8_t> const bytes = {0xF6, 0x6F};
    ASSERT_NE(protocol.get_field_by_code(field_code_of({0xF6})), nullptr)
        << "0xF6 should be a known STArray field";
    ASSERT_EQ(protocol.get_field_by_code(field_code_of({0x6F})), nullptr)
        << "0x6F should be a field code the definitions do not have";

    EXPECT_THROW(parse<VisitEverything>(bytes, protocol), ParserError);
    // The skipping path used to stop quietly at the unknown code and carry
    // on from the middle of the array.
    EXPECT_THROW(parse<SkipArrays>(bytes, protocol), ParserError);
}

// A field header that does not parse, inside an array that is being skipped.
TEST(MalformedInput, MalformedHeaderInASkippedArrayIsRefused)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    // 0xF6 opens an array; 0x00 0x00 is a header with a zero type code.
    EXPECT_THROW(
        parse<SkipArrays>({0xF6, 0x00, 0x00}, protocol), ParserError);
}

// A skipped object holding a variable-length field that claims more bytes
// than the message has. Skipping moved the cursor past the end without a
// check, and the parser then handed the visitor a slice of "the object" that
// ran outside the buffer.
TEST(MalformedInput, SkippedFieldCannotRunPastTheEnd)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    // 0xEA sfMemo (STObject), 0x7D sfMemoData (Blob), length 0x20, one byte.
    std::vector<std::uint8_t> const bytes = {0xEA, 0x7D, 0x20, 0xAB};
    EXPECT_THROW(parse<SkipObjects>(bytes, protocol), std::runtime_error);
}

// An XChainBridge whose doors and issues are missing or longer than the
// message. Its size was found by indexing the buffer directly and adding
// lengths read from it, so these read past the end (found by Neon's review
// of x-data at xprv 19a04df5). 0x01 0x19 is the sfXChainBridge header.
TEST(MalformedInput, TruncatedBridgeIsRefused)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    ASSERT_NE(protocol.get_field_by_code(field_code_of({0x01, 0x19})), nullptr)
        << "0x01 0x19 should be a known XChainBridge field";
    for (auto const& bytes : std::vector<std::vector<std::uint8_t>>{
             {0x01, 0x19},              // nothing after the header
             {0x01, 0x19, 0xFF},        // a door of 255 bytes, none present
             {0x01, 0x19, 0x14},        // a door of 20 bytes, none present
             {0x01, 0x19, 0x00},        // an empty door, then no issue
             {0x01, 0x19, 0x00, 0x00},  // an issue cut short
         })
    {
        EXPECT_THROW(parse<VisitEverything>(bytes, protocol), std::runtime_error)
            << bytes.size() << " bytes, visited";
        // The same field inside an object the visitor declines.
        std::vector<std::uint8_t> inside = {0xEA};
        inside.insert(inside.end(), bytes.begin(), bytes.end());
        EXPECT_THROW(parse<SkipObjects>(inside, protocol), std::runtime_error)
            << bytes.size() << " bytes, skipped";
    }
}

// A path set with no path, or a path with no step. rippled's JSON reader
// takes these and writes bytes its own STPathSet reader refuses ("empty
// path"; checked against xrpl-codec at cd322b9811), so they have no
// serialized form. x-data used to write and read them both ways.
TEST(MalformedInput, EmptyPathsAreRefusedBothWays)
{
    auto const protocol = Protocol::load_embedded_xrpl_protocol();
    char const* issuer = "rHb9CJAWyB4rj91VRWn96DkukG4bwdtyTh";
    boost::json::object const step{{"currency", "USD"}, {"issuer", issuer}};
    using boost::json::array;
    using boost::json::object;
    for (auto const& paths : std::vector<boost::json::value>{
             array{},                         // no path
             array{array{}},                  // one path, no step
             array{array{step}, array{}},     // a second path with no step
             array{array{object{}}},          // a step naming nothing
             array{array{object{{"type", 0}}}},
         })
    {
        std::vector<std::uint8_t> out;
        catl::xdata::VectorSink sink(out);
        catl::xdata::Serializer<catl::xdata::VectorSink> s(sink);
        EXPECT_THROW(
            catl::xdata::codecs::PathSetCodec::encode(s, paths), EncodeError)
            << boost::json::serialize(paths);
    }

    // The bytes rippled writes for those, read back.
    std::vector<std::uint8_t> one_step = {0x30};  // currency and issuer
    one_step.insert(one_step.end(), 40, 0x11);
    auto after_one_step = one_step;
    after_one_step.insert(after_one_step.end(), {0xFF, 0x00});
    for (auto const& bytes : std::vector<std::vector<std::uint8_t>>{
             {0x00},          // the end byte alone
             {0xFF, 0x00},    // a separator with nothing before it
             after_one_step,  // a separator with nothing after it
         })
        EXPECT_THROW(
            catl::xdata::codecs::PathSetCodec::decode(
                Slice(bytes.data(), bytes.size())),
            ParserError)
            << bytes.size() << " bytes";

    // And a path set that is fine still reads.
    one_step.push_back(0x00);
    EXPECT_NO_THROW(catl::xdata::codecs::PathSetCodec::decode(
        Slice(one_step.data(), one_step.size())));
    (void)protocol;
}

TEST(MalformedInput, CursorNeverLeavesItsData)
{
    std::vector<std::uint8_t> const bytes = {1, 2, 3, 4};
    Slice slice(bytes.data(), bytes.size());
    SliceCursor cursor{slice, 0};
    cursor.advance(4);
    EXPECT_TRUE(cursor.empty());
    EXPECT_EQ(cursor.remaining_size(), 0u);
    EXPECT_THROW(cursor.advance(1), SliceCursorError);
    EXPECT_THROW(cursor.read_slice(1), SliceCursorError);
    // A length that would wrap the position.
    EXPECT_THROW(
        cursor.read_slice(static_cast<std::size_t>(-1)), SliceCursorError);
    EXPECT_THROW(
        cursor.advance(static_cast<std::size_t>(-1)), SliceCursorError);
    EXPECT_EQ(cursor.pos, 4u);
}
