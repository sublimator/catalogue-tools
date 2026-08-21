#include <writ/home.hpp>
#include <writ/retained-signal.hpp>
#include <writ/roster.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace writ {

/// Test-only witness seam for the exact-id law (roster/v1 packet, control
/// 7): with same-label members every PUBLIC observable is
/// permutation-invariant, so only id-level inspection can prove that a
/// retirement consumed exactly its own row. Production code has no access
/// to these ids.
class roster_test_access
{
public:
    template <typename Label>
    static std::uint64_t
    token_id(typename roster<Label>::member const& m)
    {
        return m.id_;
    }

    template <typename Label>
    static std::vector<std::uint64_t>
    live_ids(roster<Label> const& r)
    {
        std::vector<std::uint64_t> ids;
        for (auto const& row : r.state_->rows.live)
            ids.push_back(row.first);
        return ids;
    }
};

}  // namespace writ

namespace asio = boost::asio;
using namespace std::chrono_literals;
using writ::retire_outcome;
using writ::wait_outcome;

namespace {

std::chrono::steady_clock::time_point
in(std::chrono::steady_clock::duration d)
{
    return std::chrono::steady_clock::now() + d;
}

void
drain_io(asio::io_context& io)
{
    while (io.poll() != 0)
        io.restart();
    io.restart();
}

template <class T>
T
await(asio::io_context& io, std::future<T>& result)
{
    while (result.wait_for(1ms) != std::future_status::ready)
    {
        io.poll();
        io.restart();
    }
    io.restart();
    return result.get();
}

using string_roster = writ::roster<std::string>;

bool
fail_fast_armed()
{
    return std::getenv("WRIT_HOME_FAIL_FAST") != nullptr;
}

}  // namespace

TEST(WritRoster, AdmitSealRetireDrainsAndCompletesTheWake)
{
    asio::io_context io;
    writ::home h(io);
    string_roster r(h);

    auto woken = asio::co_spawn(
        io, r.drained_signal()->co_wait_until(in(5s)), asio::use_future);

    auto first_retire = retire_outcome::refused;
    auto second_retire = retire_outcome::refused;
    asio::post(h.executor(), [&]() {
        auto a = r.admit("timer");
        auto b = r.admit("fetch");
        ASSERT_TRUE(a && *a);
        ASSERT_TRUE(b && *b);
        r.seal();
        EXPECT_FALSE(r.drained()) << "sealed but two members live";
        first_retire = r.retire(std::move(*a));
        second_retire = r.retire(std::move(*b));
    });

    EXPECT_EQ(await(io, woken), wait_outcome::signaled)
        << "the drain is received from its owner through the retained wake";
    EXPECT_EQ(first_retire, retire_outcome::retired);
    EXPECT_EQ(second_retire, retire_outcome::drained)
        << "exactly the drain-completing retirement reports it";
}

// THE DIFFERENTIATOR (packet law 6): a label already live admits again.
// This mirrors the VlCache 1/2/3 witness shape — three simultaneous
// obligations of one kind — which census/v1's duplicate-live-key law
// refuses by construction. Drained only after seal + the last retirement.
TEST(WritRoster, DuplicateLiveLabelAdmissionSucceeds)
{
    asio::io_context io;
    writ::home h(io);
    string_roster r(h);

    asio::post(h.executor(), [&]() {
        auto first = r.admit("refresh_timer");
        auto second = r.admit("refresh_timer");
        auto third = r.admit("refresh_timer");
        ASSERT_TRUE(first && *first);
        ASSERT_TRUE(second && *second)
            << "a live label must admit again: the population is a multiset";
        ASSERT_TRUE(third && *third);
        EXPECT_EQ(r.live(), 3u);
        EXPECT_EQ(r.live_with("refresh_timer"), 3u);

        r.seal();
        EXPECT_EQ(r.retire(std::move(*first)), retire_outcome::retired);
        EXPECT_EQ(r.retire(std::move(*second)), retire_outcome::retired);
        EXPECT_FALSE(r.drained());
        EXPECT_EQ(r.retire(std::move(*third)), retire_outcome::drained)
            << "drained only after seal AND the last of the three";
    });
    drain_io(io);
}

// The exact-id law's witness (packet law 7, control 7). Public outcomes
// and counts are permutation-invariant over same-label rows, so this
// control inspects ids through the test seam: retiring the MIDDLE token
// must erase exactly the middle id while both sibling ids stay live. A
// label-first retirement mutant retires the wrong row and dies here.
TEST(WritRoster, SameLabelMembersRetireByTokenNotByLabel)
{
    asio::io_context io;
    writ::home h(io);
    string_roster r(h);

    asio::post(h.executor(), [&]() {
        auto first = r.admit("refresh_timer");
        auto second = r.admit("refresh_timer");
        auto third = r.admit("refresh_timer");
        ASSERT_TRUE(first && second && third);

        auto const first_id =
            writ::roster_test_access::token_id<std::string>(*first);
        auto const second_id =
            writ::roster_test_access::token_id<std::string>(*second);
        auto const third_id =
            writ::roster_test_access::token_id<std::string>(*third);

        // Out of insertion order: the middle token first.
        EXPECT_EQ(r.retire(std::move(*second)), retire_outcome::retired);
        auto ids = writ::roster_test_access::live_ids<std::string>(r);
        std::sort(ids.begin(), ids.end());
        ASSERT_EQ(ids.size(), 2u);
        EXPECT_TRUE(std::ranges::find(ids, second_id) == ids.end())
            << "exactly the middle id must be gone";
        EXPECT_TRUE(std::ranges::find(ids, first_id) != ids.end())
            << "the first sibling must remain live";
        EXPECT_TRUE(std::ranges::find(ids, third_id) != ids.end())
            << "the third sibling must remain live";

        // Then the third, then the first — every order balances.
        EXPECT_EQ(r.retire(std::move(*third)), retire_outcome::retired);
        r.seal();
        EXPECT_EQ(r.retire(std::move(*first)), retire_outcome::drained);
    });
    drain_io(io);
}

TEST(WritRoster, ZeroBeforeSealIsNotDrained)
{
    asio::io_context io;
    writ::home h(io);
    string_roster r(h);

    auto woken = asio::co_spawn(
        io, r.drained_signal()->co_wait_until(in(5s)), asio::use_future);

    asio::post(h.executor(), [&]() {
        auto a = r.admit("a");
        ASSERT_TRUE(a && *a);
        (void)r.retire(std::move(*a));
        EXPECT_EQ(r.live(), 0u);
        EXPECT_FALSE(r.drained()) << "zero live is not drained before seal";
    });
    drain_io(io);
    EXPECT_EQ(woken.wait_for(0ms), std::future_status::timeout);

    asio::post(h.executor(), [&]() { r.seal(); });
    EXPECT_EQ(await(io, woken), wait_outcome::signaled)
        << "sealing an already-empty roster drains it immediately";
}

TEST(WritRoster, SealFencesLateAdmission)
{
    asio::io_context io;
    writ::home h(io);
    string_roster r(h);

    asio::post(h.executor(), [&]() {
        auto a = r.admit("a");
        ASSERT_TRUE(a && *a);
        (void)r.retire(std::move(*a));
        r.seal();
        EXPECT_FALSE(r.admit("late").has_value())
            << "seal fences every later admission";
        EXPECT_TRUE(r.drained());
    });
    drain_io(io);
}

// As census: registration happened BEFORE the fallible launch, so no
// failure shape can skip the roster; a synchronously refused launch still
// holds its member and retires it.
TEST(WritRoster, NeverStartedStillRetiresThroughTheRoster)
{
    asio::io_context io;
    writ::home h(io);
    string_roster r(h);

    asio::post(h.executor(), [&]() {
        auto m = r.admit("servant");
        ASSERT_TRUE(m && *m);
        bool const launch_refused = true;  // simulated synchronous refusal
        if (launch_refused)
            (void)r.retire(std::move(*m));  // never-started disposition
        r.seal();
        EXPECT_TRUE(r.drained());
    });
    drain_io(io);
}

TEST(WritRoster, LeakedMemberWedgesTheDrainVisibly)
{
    if (fail_fast_armed())
        GTEST_SKIP() << "negative control: the deliberate leak aborts under "
                        "WRIT_HOME_FAIL_FAST; that abort is certified by "
                        "WritRosterDeathTest.FailFastAbortsOnALeakedMember";

    asio::io_context io;
    writ::home h(io);
    string_roster r(h);

    auto woken = asio::co_spawn(
        io, r.drained_signal()->co_wait_until(in(50ms)), asio::use_future);

    asio::post(h.executor(), [&]() {
        auto kept = r.admit("kept");
        auto dropped = r.admit("dropped");
        ASSERT_TRUE(kept && *kept);
        ASSERT_TRUE(dropped && *dropped);
        r.seal();
        (void)r.retire(std::move(*kept));
        dropped.reset();  // dropped without retirement: the leak
    });
    drain_io(io);

    asio::post(h.executor(), [&]() {
        EXPECT_EQ(r.leaked(), 1u);
        EXPECT_EQ(r.live(), 0u);
        EXPECT_FALSE(r.drained()) << "a leak wedges the drain forever, visibly";
    });

    EXPECT_EQ(await(io, woken), wait_outcome::deadline_elapsed)
        << "the drain wake must not fire past a recorded leak";
}

TEST(WritRoster, LeakCommitsEvenWhenTheExecutorNeverRunsAgain)
{
    if (fail_fast_armed())
        GTEST_SKIP() << "negative control: the deliberate leak aborts under "
                        "WRIT_HOME_FAIL_FAST; that abort is certified by "
                        "WritRosterDeathTest.FailFastAbortsOnALeakedMember";

    asio::io_context io;
    writ::home h(io);
    string_roster r(h);

    std::optional<string_roster::member> dropped;
    asio::post(h.executor(), [&]() {
        auto admitted = r.admit("dropped");
        ASSERT_TRUE(admitted && *admitted);
        dropped.emplace(std::move(*admitted));
    });
    drain_io(io);
    ASSERT_TRUE(dropped && *dropped);

    io.stop();        // the home's executor will never run another handler
    dropped.reset();  // off-home drop: prints, commits, posts into the void

    EXPECT_EQ(r.leaked(), 1u)
        << "the leak fact must not depend on the executor still running";
}

TEST(WritRoster, ForeignRetireRefusesWithoutConsumingTheObligation)
{
    asio::io_context io;
    writ::home h(io);
    string_roster first(h);
    string_roster second(h);

    asio::post(h.executor(), [&]() {
        auto m = first.admit("a");
        ASSERT_TRUE(m && *m);
        EXPECT_EQ(second.retire(std::move(*m)), retire_outcome::refused)
            << "a foreign member must refuse";
        EXPECT_TRUE(*m) << "the refusal must leave the obligation intact";

        string_roster::member empty;
        EXPECT_EQ(first.retire(std::move(empty)), retire_outcome::refused)
            << "an empty token must refuse";

        first.seal();
        EXPECT_EQ(first.retire(std::move(*m)), retire_outcome::drained)
            << "the surviving token still balances its rightful roster";
        EXPECT_TRUE(first.drained());
    });
    drain_io(io);
}

// The retained drain fact outlives its publication: a subscriber arriving
// after the drain observes the same completed wake.
TEST(WritRoster, LateJoinerObservesRetainedDrainFact)
{
    asio::io_context io;
    writ::home h(io);
    string_roster r(h);

    asio::post(h.executor(), [&]() {
        auto m = r.admit("only");
        ASSERT_TRUE(m && *m);
        r.seal();
        EXPECT_EQ(r.retire(std::move(*m)), retire_outcome::drained);
    });
    drain_io(io);

    auto late = asio::co_spawn(
        io, r.drained_signal()->co_wait_until(in(5s)), asio::use_future);
    EXPECT_EQ(await(io, late), wait_outcome::signaled)
        << "quiescence is retained state, not an edge a joiner can miss";
}

TEST(WritRoster, LiveWithCountsTrackAdmissionsMinusRetirements)
{
    asio::io_context io;
    writ::home h(io);
    string_roster r(h);

    asio::post(h.executor(), [&]() {
        auto t1 = r.admit("timer");
        auto t2 = r.admit("timer");
        auto f1 = r.admit("fetch");
        ASSERT_TRUE(t1 && t2 && f1);
        EXPECT_EQ(r.live(), 3u);
        EXPECT_EQ(r.live_with("timer"), 2u);
        EXPECT_EQ(r.live_with("fetch"), 1u);
        EXPECT_EQ(r.live_with("absent"), 0u);

        (void)r.retire(std::move(*t1));
        EXPECT_EQ(r.live_with("timer"), 1u);
        EXPECT_EQ(r.live_with("fetch"), 1u);

        (void)r.retire(std::move(*f1));
        EXPECT_EQ(r.live_with("fetch"), 0u);
        (void)r.retire(std::move(*t2));
        r.seal();
        EXPECT_TRUE(r.drained());
    });
    drain_io(io);
}

// The subprocess half of the split gate, as census: the fail-fast abort on
// a leak is certified here so the in-process negative controls may skip
// under WRIT_HOME_FAIL_FAST without leaving the behavior unproven.
TEST(WritRosterDeathTest, FailFastAbortsOnALeakedMember)
{
    asio::io_context io;
    writ::home h(io);
    string_roster r(h);

    std::optional<string_roster::member> dropped;
    asio::post(h.executor(), [&]() {
        auto admitted = r.admit("dropped");
        if (admitted && *admitted)
            dropped.emplace(std::move(*admitted));
    });
    drain_io(io);
    ASSERT_TRUE(dropped && *dropped);

    EXPECT_DEATH(
        {
            ::setenv("WRIT_HOME_FAIL_FAST", "1", 1);
            dropped.reset();
        },
        "WRIT-ROSTER-LEAK");

    // Only the CHILD dropped its copy; the parent still holds a live
    // member and must balance it honestly.
    asio::post(h.executor(), [&]() {
        EXPECT_EQ(r.retire(std::move(*dropped)), retire_outcome::retired)
            << "consumed, roster unsealed so not drained";
        dropped.reset();
    });
    drain_io(io);
}
