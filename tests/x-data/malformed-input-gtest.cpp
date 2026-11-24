// Bytes a parser is handed are untrusted. Malformed ones are refused with an
// exception; they never crash the process and never leave the cursor outside
// the message.

#include <catl/core/types.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/slice-cursor.h>
#include <catl/xdata/slice-visitor.h>

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
