#include <writ/receipt.hpp>

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

namespace {

// The capability model under test: the Tag's constructor IS the mint, and
// the Tag is move-only so the mint capability cannot be copied back out of a
// legitimate receipt. Only drain_authority can construct drain_fact,
// therefore only it can produce receipt<drain_fact>. Identity is typed
// payload, not baked fields.
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

    drain_fact(drain_fact const&) = delete;
    drain_fact&
    operator=(drain_fact const&) = delete;
    drain_fact(drain_fact&&) noexcept = default;
    drain_fact&
    operator=(drain_fact&&) noexcept = default;

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

// First-mint forgery fails to compile: this translation unit is not a friend
// of drain_fact, so it cannot construct the Tag and therefore cannot mint.
static_assert(
    !std::is_constructible_v<drain_fact, std::string, std::uint64_t>,
    "outsiders must not be able to construct the fact");

// Round 4's compiled counterexample, landed as the compile-time control it
// demanded: `drain_fact copied = receipt.fact();` followed by re-minting
// through the public receipt constructor. The library now rejects copyable
// Tags outright, so the copy step — the only door — does not compile.
static_assert(
    !std::is_copy_constructible_v<drain_fact>,
    "a copyable Tag could be copied out of fact() and re-minted; the Tag "
    "must be move-only");
static_assert(
    !std::is_constructible_v<
        writ::receipt<drain_fact>,
        drain_fact const&,
        drain_fact::disposition>,
    "a receipt must not be mintable from a merely-observed fact");

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
// winner — and the refusal must not consume the loser, whose obligation
// still has to balance somewhere (the census::retire rule, applied here).
TEST(WritReceipt, SlotRetainsExactlyOneFactAndRefusalPreservesTheLoser)
{
    writ::receipt_slot<drain_fact> slot;
    EXPECT_FALSE(slot.occupied());

    ASSERT_TRUE(slot.retain(
        drain_authority::mint("a:1", 1, drain_fact::disposition::drained)));

    auto late = drain_authority::mint(
        "a:1", 1, drain_fact::disposition::forced_teardown);
    EXPECT_FALSE(slot.retain(std::move(late)))
        << "a later fact must not overwrite the latched winner";
    EXPECT_EQ(late.fact().owner, "a:1")
        << "the refusal must leave the loser's fact intact";
    EXPECT_EQ(late.disposition(), drain_fact::disposition::forced_teardown)
        << "and its disposition unconsumed";

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
