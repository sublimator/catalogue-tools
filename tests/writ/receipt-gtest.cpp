#include <writ/receipt.hpp>

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

namespace {

// The capability model under test: the Tag's constructor IS the mint.
// Only drain_authority can construct drain_fact, therefore only it can
// produce receipt<drain_fact>. Identity is typed payload, not baked fields.
struct drain_authority;

struct drain_fact
{
    enum class disposition : std::uint8_t {
        drained,
        forced_teardown,
        receipt_failed,
    };

    std::string owner;
    std::uint64_t generation = 0;

private:
    friend struct drain_authority;

    drain_fact(std::string owner_, std::uint64_t generation_)
        : owner(std::move(owner_)), generation(generation_)
    {
    }
};

struct drain_authority
{
    static writ::receipt<drain_fact>
    mint(std::string owner, std::uint64_t generation, drain_fact::disposition d)
    {
        return writ::receipt<drain_fact>(
            drain_fact(std::move(owner), generation), d);
    }
};

// Forgery fails to compile: this translation unit is not a friend of
// drain_fact, so it cannot construct the Tag and therefore cannot mint.
static_assert(
    !std::is_constructible_v<drain_fact, std::string, std::uint64_t>,
    "outsiders must not be able to construct the fact");

// A receipt is a fact, not a value: move-only, never copied, never
// conjured empty.
static_assert(!std::is_copy_constructible_v<writ::receipt<drain_fact>>);
static_assert(!std::is_copy_assignable_v<writ::receipt<drain_fact>>);
static_assert(std::is_move_constructible_v<writ::receipt<drain_fact>>);
static_assert(!std::is_default_constructible_v<writ::receipt<drain_fact>>);

}  // namespace

TEST(WritReceipt, CarriesTypedIdentityAndClosedDisposition)
{
    auto r = drain_authority::mint(
        "peer.example:1", 42, drain_fact::disposition::drained);
    EXPECT_EQ(r.fact().owner, "peer.example:1");
    EXPECT_EQ(r.fact().generation, 42u);
    EXPECT_EQ(r.disposition(), drain_fact::disposition::drained);
}

// The first fact is latched; a later publication cannot overwrite history's
// winner.
TEST(WritReceipt, SlotRetainsExactlyOneFact)
{
    writ::receipt_slot<drain_fact> slot;
    EXPECT_FALSE(slot.occupied());

    ASSERT_TRUE(slot.retain(
        drain_authority::mint("a:1", 1, drain_fact::disposition::drained)));
    EXPECT_FALSE(slot.retain(drain_authority::mint(
        "a:1", 1, drain_fact::disposition::forced_teardown)))
        << "a later fact must not overwrite the latched winner";

    auto consumed = slot.consume();
    ASSERT_TRUE(consumed.has_value());
    EXPECT_EQ(consumed->disposition(), drain_fact::disposition::drained);
}

// Exchange-with-empty before returning: re-entry during consumption sees
// absence, so a one-shot fact cannot be double-observed.
TEST(WritReceipt, ConsumeEmptiesTheSlotBeforeReturning)
{
    writ::receipt_slot<drain_fact> slot;
    ASSERT_TRUE(slot.retain(drain_authority::mint(
        "b:2", 9, drain_fact::disposition::receipt_failed)));

    auto first = slot.consume();
    ASSERT_TRUE(first.has_value());
    EXPECT_FALSE(slot.occupied())
        << "the slot must be empty while the consumer still holds the fact";
    EXPECT_FALSE(slot.consume().has_value());

    ASSERT_TRUE(slot.retain(std::move(*first)))
        << "an unconsumed fact may be re-retained by its owner";
}
