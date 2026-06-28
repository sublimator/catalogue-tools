// Verifies that SliceVisitor callbacks are optional (issue #0056): a visitor
// may implement only the callbacks it needs, and the parser supplies defaults
// (containers descend; the rest no-op) for the ones it omits.

#include <catl/xdata/known-fields.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/slice-visitor.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

namespace {

using namespace catl::xdata;

// A visitor with only visit_field is the minimal useful shape.
struct FieldOnly
{
    int count = 0;
    std::uint32_t last_code = 0;
    void
    visit_field(FieldPath const&, FieldSlice const& fs)
    {
        ++count;
        last_code = fs.field->code;
    }
};

// A visitor that only prunes (object_start) is also valid.
struct PruneObjects
{
    bool
    visit_object_start(FieldPath const&, FieldSlice const&)
    {
        return false;
    }
};

struct Nothing
{
};

// Compile-time: any non-empty subset satisfies the concept; an empty type does
// not (a type with zero recognized callbacks is almost certainly a mistake).
static_assert(SliceVisitor<FieldOnly>);
static_assert(SliceVisitor<PruneObjects>);
static_assert(!SliceVisitor<Nothing>);

}  // namespace

// A one-method visitor still receives leaf fields through the parser's default
// dispatch — the four omitted callbacks default cleanly.
TEST(OptionalVisitor, MinimalVisitorReceivesField)
{
    auto const protocol = catl::xdata::Protocol::load_from_file(
        std::string(PROJECT_ROOT) + "src/x-data/definitions/xahau_definitions.json");

    // A single serialized sfFlags (UInt32, field id 0x22) holding 0x00000001.
    std::vector<std::uint8_t> bytes = {0x22, 0x00, 0x00, 0x00, 0x01};
    Slice slice(bytes.data(), bytes.size());
    catl::xdata::SliceCursor cursor{slice, 0};
    catl::xdata::ParserContext ctx{cursor};

    FieldOnly visitor;
    catl::xdata::parse_with_visitor(ctx, protocol, visitor);

    EXPECT_EQ(visitor.count, 1);
    EXPECT_EQ(visitor.last_code, catl::xdata::sf::Flags);
}

// A pathologically deep STObject chain must be rejected at kMaxParseDepth rather
// than recursing until the stack is exhausted (sec #0054 — the depth cap from
// #0051, here pinned against regression on the visitor parse path). Each 0xEA
// byte is an sfMemo (STObject) field header; a chain longer than kMaxParseDepth
// drives the recursion past the cap and must throw ParserError, NOT crash.
TEST(OptionalVisitor, RejectsExcessiveNestingDepth)
{
    auto const protocol = catl::xdata::Protocol::load_from_file(
        std::string(PROJECT_ROOT) +
        "src/x-data/definitions/xahau_definitions.json");

    // kMaxParseDepth + margin nested STObject (sfMemo = 0xEA) headers. No end
    // markers are needed: the depth guard throws before they would be consumed.
    std::vector<std::uint8_t> bytes(
        static_cast<std::size_t>(catl::xdata::kMaxParseDepth) + 5, 0xEA);
    Slice slice(bytes.data(), bytes.size());
    catl::xdata::SliceCursor cursor{slice, 0};
    catl::xdata::ParserContext ctx{cursor};

    FieldOnly visitor;
    EXPECT_THROW(
        catl::xdata::parse_with_visitor(ctx, protocol, visitor),
        catl::xdata::ParserError);
}
