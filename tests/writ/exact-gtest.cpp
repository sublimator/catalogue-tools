#include <writ/exact.hpp>

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <type_traits>

namespace {

struct session
{
    int payload = 0;
};

}  // namespace

// Round 5's blocker, landed as permanent trait controls: a mint is an owner
// capability. A copy would carry the same epoch AND sequence and mint
// identical (epoch, value) generations — duplicated identity authority,
// defeating the epoch repair without ever inventing a generation directly —
// and a default move would leave the source integers intact with the same
// effect. Neither exists.
static_assert(!std::is_copy_constructible_v<writ::mint>);
static_assert(!std::is_copy_assignable_v<writ::mint>);
static_assert(!std::is_move_constructible_v<writ::mint>);
static_assert(!std::is_move_assignable_v<writ::mint>);

TEST(WritExact, MintIsMonotonicAndZeroIsNeverMinted)
{
    writ::mint m;
    auto const first = m.next();
    auto const second = m.next();
    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_EQ(first.value(), 1u);
    EXPECT_EQ(second.value(), 2u);
    EXPECT_LT(first, second);
    EXPECT_EQ(first.epoch(), second.epoch())
        << "one mint is one owner: its generations share its epoch";
    EXPECT_NE(first.epoch(), 0u);
}

// Exhaustion is a sticky refusal, never a wrap: the last representable value
// is minted once, then every later call refuses. Counter honesty is proven
// by observing the counter AFTER the refusals, not by narration.
TEST(WritExact, ExhaustionIsStickyRefusalNotWrap)
{
    auto const max = std::numeric_limits<std::uint64_t>::max();
    writ::mint m(max - 1);

    auto const last = m.next();
    ASSERT_TRUE(last);
    EXPECT_EQ(last.value(), max);

    EXPECT_FALSE(m.next());
    EXPECT_FALSE(m.next()) << "refusal must be sticky";
    EXPECT_EQ(m.last(), max) << "refusal must not move the counter";
}

// The stale-handle counterexample: a ref issued against generation g refuses
// once the owner admits a successor incarnation — and the mint still
// produces honest successors after the refusal.
TEST(WritExact, StaleRefRefusesAfterReplacement)
{
    writ::mint m;
    auto first_object = std::make_shared<session>(session{1});
    writ::incarnation<session> current(first_object, m.next());

    auto const stale = current.ref();
    ASSERT_NE(stale.lock_exact(current), nullptr);

    auto second_object = std::make_shared<session>(session{2});
    current = writ::incarnation<session>(second_object, m.next());

    EXPECT_EQ(stale.lock_exact(current), nullptr)
        << "a handle from the replaced incarnation must refuse";
    ASSERT_NE(current.ref().lock_exact(current), nullptr);

    auto const successor = m.next();
    ASSERT_TRUE(successor);
    EXPECT_EQ(successor.value(), 3u)
        << "refusals must not have disturbed the mint";
}

// Incarnation is not a key: the SAME object re-admitted under a fresh
// generation is a different incarnation, and the old ref refuses even though
// the pointer it names is still the live one.
TEST(WritExact, SameObjectReincarnatedRefusesTheOldRef)
{
    writ::mint m;
    auto object = std::make_shared<session>(session{7});
    writ::incarnation<session> current(object, m.next());
    auto const old_ref = current.ref();

    current = writ::incarnation<session>(object, m.next());

    EXPECT_EQ(old_ref.lock_exact(current), nullptr);
    EXPECT_NE(current.ref().lock_exact(current), nullptr);
}

// Scope-first refusal is uniform: a dead incarnation, a foreign generation,
// and a default-constructed ref all receive the identical nullptr, so
// presence is never leaked through the refusal shape.
TEST(WritExact, RefusalIsUniformAcrossDeadForeignAndEmpty)
{
    writ::mint m;
    writ::exact_ref<session> dead_ref;
    {
        auto short_lived = std::make_shared<session>();
        writ::incarnation<session> gone(short_lived, m.next());
        dead_ref = gone.ref();
    }

    auto object = std::make_shared<session>();
    writ::incarnation<session> current(object, m.next());

    EXPECT_EQ(dead_ref.lock_exact(current), nullptr);
    EXPECT_EQ(writ::exact_ref<session>{}.lock_exact(current), nullptr);

    writ::incarnation<session> empty_owner;
    EXPECT_EQ(current.ref().lock_exact(empty_owner), nullptr)
        << "an empty owner refuses every ref";
}

// The round-4 review's compiled counterexample, landed as the control it
// demanded: two independent owners hold the SAME object at the SAME local
// sequence, and each must refuse the other's ref. Before the epoch fix this
// printed foreign_alias=1; the epoch inside generation is what refuses now.
TEST(WritExact, ForeignOwnerWithCollidingSequenceRefuses)
{
    auto shared = std::make_shared<session>(session{7});
    writ::mint first_mint;
    writ::mint second_mint;
    writ::incarnation<session> ours(shared, first_mint.next());
    writ::incarnation<session> foreign(shared, second_mint.next());

    ASSERT_EQ(ours.gen().value(), foreign.gen().value())
        << "the collision under test: identical object, identical sequence";
    ASSERT_NE(ours.gen().epoch(), foreign.gen().epoch())
        << "distinct owners must occupy distinct epochs";

    EXPECT_EQ(ours.ref().lock_exact(foreign), nullptr)
        << "a foreign owner must refuse our ref";
    EXPECT_EQ(foreign.ref().lock_exact(ours), nullptr)
        << "and we must refuse the foreign owner's ref";
    ASSERT_NE(ours.ref().lock_exact(ours), nullptr);
    ASSERT_NE(foreign.ref().lock_exact(foreign), nullptr);
}

// The review's required sibling case: a recreated owner is a new incarnation
// boundary. The restore seam resumes the SEQUENCE, never the epoch, so a
// predecessor's ref refuses against the restored owner even when the object
// survived and the sequences collide exactly.
TEST(WritExact, RestoredOwnerRefusesPredecessorRefs)
{
    auto object = std::make_shared<session>(session{1});
    writ::mint original;
    writ::incarnation<session> current(object, original.next());
    auto const predecessor = current.ref();
    ASSERT_NE(predecessor.lock_exact(current), nullptr);

    // The owner dies and is restored from its persisted counter, around the
    // same still-living object.
    writ::mint restored(0);
    writ::incarnation<session> successor(object, restored.next());
    ASSERT_EQ(predecessor.gen().value(), successor.gen().value())
        << "the restored sequence collides by construction";

    EXPECT_EQ(predecessor.lock_exact(successor), nullptr)
        << "a recreated owner must refuse every predecessor ref";
    ASSERT_NE(successor.ref().lock_exact(successor), nullptr);
}
