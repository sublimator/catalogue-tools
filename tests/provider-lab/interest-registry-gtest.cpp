#include "interest-registry.h"

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <utility>

using namespace catl::provider_lab;
namespace asio = boost::asio;

namespace {

struct RegistryFixture : ::testing::Test
{
    DeterministicScheduler sched;
    DeterministicScheduler::strand_type strand = sched.make_strand();
    InterestRegistry reg{strand};

    template <typename Fn>
    void
    run(Fn fn)
    {
        asio::post(strand, std::move(fn));
        sched.run_until_idle();
    }
};

using R = InterestRegistry::Refusal;
using J = InterestRegistry::JoinRefusal;

}  // namespace

TEST_F(RegistryFixture, RegistrationRetainsOriginalIdentityAndSponsors)
{
    run([&] {
        auto a = reg.register_interest(/*op*/ 100, /*child*/ 7, /*fetch*/ 1);
        auto b = reg.register_interest(101, 8, 1);
        ASSERT_TRUE(a && b);
        EXPECT_GT(*b, *a) << "interest ids are non-reused";

        auto sponsors = reg.sponsors_of(1);
        ASSERT_TRUE(sponsors.has_value());
        EXPECT_EQ(sponsors->size(), 2u)
            << "both actives sponsor the shared fetch";
    });
}

TEST_F(RegistryFixture, NoRootAdoptionIsStructural)
{
    run([&] {
        auto first = reg.register_interest(100, 7, 1);
        auto second = reg.register_interest(101, 8, 1);
        ASSERT_TRUE(first && second);

        // The FIRST waiter detaches; the shared fetch is still owned
        // by the second — no adopted root, because there is no
        // first-waiter field to adopt: the sponsor set is derived.
        ASSERT_EQ(reg.begin_detach(*first), R::ok);
        auto sponsors = reg.sponsors_of(1);
        ASSERT_TRUE(sponsors.has_value());
        ASSERT_EQ(sponsors->size(), 1u);
        EXPECT_EQ(sponsors->front(), *second);

        // The detached-pending interest is draining under retained
        // authority — visible, owned, never gone.
        auto draining = reg.draining_of(1);
        ASSERT_TRUE(draining.has_value());
        ASSERT_EQ(draining->size(), 1u);
        EXPECT_EQ(draining->front(), *first);
    });
}

TEST_F(RegistryFixture, SoleInterestDetachBothOrders)
{
    // THE control the design was amended for: the sole interest
    // detaches with a submitted use in flight, in both orders, and the
    // row remains through the gap — removed exactly once at the join.
    for (bool detach_first : {true, false})
    {
        DeterministicScheduler s2;
        auto strand2 = s2.make_strand();
        InterestRegistry r2{strand2};
        asio::post(strand2, [&] {
            auto id = r2.register_interest(100, 7, 1);
            ASSERT_TRUE(id.has_value());
            ASSERT_EQ(r2.add_submitted_use(*id, 500), R::ok);

            if (detach_first)
            {
                ASSERT_EQ(r2.begin_detach(*id), R::ok);
                EXPECT_TRUE(r2.interest_present(*id).value_or(false))
                    << "detach removes nothing";
                ASSERT_EQ(r2.use_terminal(*id, 500), R::ok);
            }
            else
            {
                ASSERT_EQ(r2.use_terminal(*id, 500), R::ok)
                    << "use completion is legal while active";
                ASSERT_EQ(r2.begin_detach(*id), R::ok);
            }
            EXPECT_TRUE(r2.interest_present(*id).value_or(false))
                << "the row survives the gap: zero active interests is "
                   "draining under retained authority, never ownerless";
            EXPECT_EQ(r2.sponsors_of(1)->size(), 0u);
            EXPECT_EQ(r2.draining_of(1)->size(), 1u);

            ASSERT_EQ(r2.ack_revoke(*id), R::ok);
            ASSERT_EQ(r2.ack_unsubscribe(*id), R::ok);

            auto join = r2.terminal_join(*id);
            ASSERT_EQ(join.refusal, J::ok);
            ASSERT_TRUE(join.ack.has_value());
            EXPECT_EQ(join.ack->interest(), *id);
            EXPECT_EQ(join.ack->operation(), 100u);
            EXPECT_EQ(join.ack->child(), 7u)
                << "the join emits the exact ORIGINAL child terminal ack";

            EXPECT_FALSE(r2.interest_present(*id).value_or(true));
            EXPECT_EQ(r2.terminal_join(*id).refusal, J::unknown_interest)
                << "removed exactly once";
        });
        s2.run_until_idle();
    }
}

TEST_F(RegistryFixture, DetachSealsNewSponsorshipExistingWorkContinues)
{
    run([&] {
        auto id = reg.register_interest(100, 7, 1);
        ASSERT_EQ(reg.add_submitted_use(*id, 500), R::ok);
        ASSERT_EQ(reg.begin_detach(*id), R::ok);

        EXPECT_EQ(reg.add_submitted_use(*id, 501), R::not_active)
            << "detach seals NEW sponsorship";
        EXPECT_EQ(reg.add_delivery_obligation(*id, 600), R::not_active);
        EXPECT_EQ(reg.begin_detach(*id), R::not_active)
            << "detach is not repeatable";

        EXPECT_EQ(reg.use_terminal(*id, 500), R::ok)
            << "existing submitted work drains normally";
    });
}

TEST_F(RegistryFixture, JoinRefusesWhileAnyObligationRemainsOpen)
{
    run([&] {
        auto id = reg.register_interest(100, 7, 1);
        ASSERT_EQ(reg.add_submitted_use(*id, 500), R::ok);
        ASSERT_EQ(reg.add_delivery_obligation(*id, 600), R::ok);

        EXPECT_EQ(reg.terminal_join(*id).refusal, J::not_detach_pending)
            << "an active interest cannot join";
        ASSERT_EQ(reg.begin_detach(*id), R::ok);

        // Each obligation left open alone blocks the join; the row
        // stays through every refusal.
        EXPECT_EQ(reg.terminal_join(*id).refusal, J::obligations_open);
        ASSERT_EQ(reg.ack_revoke(*id), R::ok);
        EXPECT_EQ(reg.terminal_join(*id).refusal, J::obligations_open);
        ASSERT_EQ(reg.use_terminal(*id, 500), R::ok);
        EXPECT_EQ(reg.terminal_join(*id).refusal, J::obligations_open);
        ASSERT_EQ(reg.ack_delivery(*id, 600), R::ok);
        EXPECT_EQ(reg.terminal_join(*id).refusal, J::obligations_open);
        EXPECT_TRUE(reg.interest_present(*id).value_or(false));

        ASSERT_EQ(reg.ack_unsubscribe(*id), R::ok);
        EXPECT_EQ(reg.terminal_join(*id).refusal, J::ok);
    });
}

TEST_F(RegistryFixture, DuplicateAndPrematureAcksAreTyped)
{
    run([&] {
        auto id = reg.register_interest(100, 7, 1);

        EXPECT_EQ(reg.ack_revoke(*id), R::not_detach_pending)
            << "a revoke ack cannot precede the detach that initiates it";
        EXPECT_EQ(reg.ack_unsubscribe(*id), R::not_detach_pending);

        ASSERT_EQ(reg.begin_detach(*id), R::ok);
        ASSERT_EQ(reg.ack_revoke(*id), R::ok);
        EXPECT_EQ(reg.ack_revoke(*id), R::already_acked)
            << "duplicate acks are typed no-ops";
        EXPECT_EQ(reg.use_terminal(*id, 999), R::unknown_use);
        EXPECT_EQ(reg.ack_delivery(*id, 999), R::unknown_delivery);
    });
}

TEST_F(RegistryFixture, OffStrandTouchesAreRefusedBeforeAnyRead)
{
    bool checked = false;
    asio::post(sched.io(), [&] {
        EXPECT_FALSE(reg.register_interest(1, 2, 3).has_value());
        EXPECT_EQ(reg.begin_detach(1), R::off_strand);
        EXPECT_EQ(reg.ack_revoke(1), R::off_strand);
        EXPECT_EQ(reg.use_terminal(1, 1), R::off_strand);
        EXPECT_EQ(reg.terminal_join(1).refusal, J::off_strand);
        EXPECT_FALSE(reg.sponsors_of(1).has_value());
        EXPECT_FALSE(reg.draining_of(1).has_value());
        EXPECT_FALSE(reg.interest_present(1).has_value());
        checked = true;
    });
    sched.run_until_idle();
    EXPECT_TRUE(checked);
    run([&] { EXPECT_EQ(reg.sponsors_of(1)->size(), 0u); });
}
