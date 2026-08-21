// Witness suite for issue 0074: the anchor-bundle wait conflates
// timeout and completion.
//
// ProofEngine::get_anchor_bundle's waiter arm awaits the shared
// steady_timer, IGNORES the wake's error code (proof-engine.cpp:1231-1233
// discards `ec`), and classifies the outcome solely from `entry.present`
// (:1237), throwing one generic "anchor bundle fetch failed" (:1245) for
// every not-present wake. These witnesses prove the misclassification:
// three DISTINCT truthful terminals — waiter deadline expiry, cache
// invalidation, and builder failure — produce byte-identical generic
// failures, and the deadline case is a FALSE failure: the in-flight build
// is still legitimately live and can succeed moments later.
//
// TESTS-ONLY. The waiter under test is the PRODUCTION coroutine (the
// Action::wait branch entered through the real get_anchor_bundle).
// Builder-side cache transitions are reproduced through the existing
// ProofEngineTestAccess friend seam exactly as the production fetch arm
// performs them (creation :1204-1206, success :1283-1307, failure
// :1322-1334) — the same idiom NodeCacheTestAccess::create_in_flight
// established. Recorded delta from production: the in-flight deadline is
// test-scaled (50 ms) in place of the hard-coded 30 s at :1204-1205; the
// timer object, the wait, the wake and the classification site are
// otherwise untouched.
//
// Typed-outcome map (goal requirement):
//   completion  -> CompletionWakeReturnsBundle (correctly classified;
//                  this is the probe-validity control)
//   deadline    -> DeadlineExpiryReportsGenericFetchFailure (misclassified)
//   cancellation/invalidation -> InvalidationWakeIndistinguishable...
//                  (misclassified, identical to deadline)
//   builder failure -> BuilderFailureIndistinguishable... (identical again)
//   shutdown    -> not witnessed here; stop-time interaction with the
//                  anchor cache is a separate lane (non-claim).

#include <xprv/network-config.h>
#include <xprv/proof-engine.h>

#include "proof-engine-test-access.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>

namespace asio = boost::asio;
using xprv::ProofEngineTestAccess;
using namespace std::chrono_literals;

namespace {

constexpr char const* kGenericFailure = "anchor bundle fetch failed";
constexpr uint32_t kSeq = 5000;
constexpr uint32_t kVlSeq = 7;

Hash256
test_anchor_hash()
{
    std::array<uint8_t, 32> bytes{};
    bytes[0] = 0x74;
    bytes[31] = 0x74;
    return Hash256(bytes);
}

struct AnchorWaitHarness
{
    asio::io_context io;
    std::shared_ptr<xprv::ProofEngine> engine =
        xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
    Hash256 hash = test_anchor_hash();

    void
    create_in_flight(std::chrono::milliseconds deadline)
    {
        ProofEngineTestAccess::create_in_flight_anchor(
            *engine, kSeq, hash, kVlSeq, deadline);
    }

    bool
    in_flight()
    {
        return ProofEngineTestAccess::anchor_entry_in_flight(
            *engine, kSeq, hash, kVlSeq);
    }

    std::future<ProofEngineTestAccess::AnchorBundle>
    spawn_waiter()
    {
        return asio::co_spawn(
            io,
            ProofEngineTestAccess::join_anchor_wait(
                engine, kSeq, hash, kVlSeq),
            asio::use_future);
    }

    // Schedule a builder-side transition ON the io_context (production
    // serializes cache transitions and timer cancels the same way: the
    // builder coroutine runs on io_).
    template <typename Fn>
    void
    schedule(std::chrono::milliseconds after, Fn fn)
    {
        asio::co_spawn(
            io,
            [this, after, fn = std::move(fn)]() -> asio::awaitable<void> {
                asio::steady_timer t(io, after);
                co_await t.async_wait(asio::use_awaitable);
                fn();
            },
            asio::detached);
    }

    // Run until all scheduled work completes, with a wall-clock guard.
    void
    run()
    {
        io.restart();
        io.run();
    }
};

std::string
failure_message(std::future<ProofEngineTestAccess::AnchorBundle>& waiter)
{
    try
    {
        (void)waiter.get();
    }
    catch (std::runtime_error const& error)
    {
        return error.what();
    }
    catch (...)
    {
        return "(non-runtime_error exception)";
    }
    return "(no exception)";
}

}  // namespace

// ── Probe-validity control: a completion wake is classified correctly ──
// The waiter distinguishes exactly ONE outcome (present=true). This
// control proves the harness's production waiter returns the bundle on a
// genuine completion wake — so the identical generic failures in the
// witnesses below are the code's classification, not harness blindness.
TEST(ProofEngineAnchorWait0074, CompletionWakeReturnsBundle)
{
    AnchorWaitHarness harness;
    harness.create_in_flight(10s);  // deadline never reached
    auto waiter = harness.spawn_waiter();
    harness.schedule(10ms, [&harness] {
        ProofEngineTestAccess::complete_anchor_build(
            *harness.engine, kSeq, harness.hash, kVlSeq);
    });
    harness.run();

    auto bundle = waiter.get();
    EXPECT_EQ(bundle.seq, kSeq);
    EXPECT_EQ(bundle.vl_sequence, kVlSeq);
}

// ── Witness 1: deadline expiry becomes a generic FETCH FAILURE ─────────
// The truthful outcome is "this waiter's deadline elapsed while the build
// is still legitimately in flight". The reported outcome is the same
// generic error used for a failed build — and the entry is STILL
// in-flight after the throw, proving no builder terminal was received.
TEST(ProofEngineAnchorWait0074, DeadlineExpiryReportsGenericFetchFailure)
{
    AnchorWaitHarness harness;
    harness.create_in_flight(50ms);
    auto waiter = harness.spawn_waiter();
    harness.run();  // no builder transition: the 50ms deadline wins

    EXPECT_EQ(failure_message(waiter), kGenericFailure)
        << "deadline expiry must currently surface as the generic fetch "
           "failure (the conflation under witness)";
    EXPECT_TRUE(harness.in_flight())
        << "the build must still be in flight after the waiter's false "
           "failure — no builder terminal existed";
}

// ── Witness 2: invalidation wake is indistinguishable from deadline ────
// invalidate_anchor_cache() (the real production function,
// proof-engine.cpp:1144-1161) cancels the signal and erases the entry —
// a cancellation-class terminal. The waiter reports the SAME generic
// failure byte-for-byte as deadline expiry.
TEST(ProofEngineAnchorWait0074, InvalidationWakeIndistinguishableFromDeadline)
{
    AnchorWaitHarness harness;
    harness.create_in_flight(10s);
    auto waiter = harness.spawn_waiter();
    harness.schedule(10ms, [&harness] {
        ProofEngineTestAccess::invalidate_anchor_cache(*harness.engine);
    });
    harness.run();

    EXPECT_EQ(failure_message(waiter), kGenericFailure)
        << "an invalidation cancel must currently surface as the same "
           "generic failure — indistinguishable from a deadline";
}

// ── Witness 3: builder failure is indistinguishable from both ──────────
// The fetch arm's failure cleanup (erase + cancel, :1322-1334) produces
// the identical generic message: the original builder error is never
// carried to joiners.
TEST(ProofEngineAnchorWait0074, BuilderFailureIndistinguishableFromDeadline)
{
    AnchorWaitHarness harness;
    harness.create_in_flight(10s);
    auto waiter = harness.spawn_waiter();
    harness.schedule(10ms, [&harness] {
        ProofEngineTestAccess::fail_anchor_build(
            *harness.engine, kSeq, harness.hash, kVlSeq);
    });
    harness.run();

    EXPECT_EQ(failure_message(waiter), kGenericFailure)
        << "builder failure must currently surface as the same generic "
           "failure — the builder's own error is erased";
}

// ── Witness 4: the composed false-failure counterexample ───────────────
// The issue's headline trace, end to end: (a) a waiter deadlines and
// false-fails while the build is live; (b) every LATE joiner then fails
// INSTANTLY — the shared timer is already expired, so its wait completes
// immediately with present=false (the cache amplifies the false failure);
// (c) the build then succeeds and publishes; (d) the next caller receives
// a cache HIT on the very bundle whose earlier absence was reported as a
// fetch failure. The failures in (a) and (b) were therefore FALSE.
TEST(ProofEngineAnchorWait0074, LateBuilderSuccessProvesFailuresWereFalse)
{
    AnchorWaitHarness harness;
    harness.create_in_flight(50ms);

    // (a) first waiter deadlines
    auto first = harness.spawn_waiter();
    harness.run();
    EXPECT_EQ(failure_message(first), kGenericFailure);
    ASSERT_TRUE(harness.in_flight()) << "build still legitimately live";

    // (b) a late joiner false-fails instantly on the already-expired timer
    auto t0 = std::chrono::steady_clock::now();
    auto second = harness.spawn_waiter();
    harness.run();
    auto second_wait = std::chrono::steady_clock::now() - t0;
    EXPECT_EQ(failure_message(second), kGenericFailure);
    EXPECT_LT(second_wait, 5s)
        << "the joiner failed without waiting its own 30s deadline — the "
           "expired shared timer short-circuits every later waiter";
    EXPECT_TRUE(harness.in_flight());

    // (c) the builder succeeds moments later (production success shape)
    ProofEngineTestAccess::complete_anchor_build(
        *harness.engine, kSeq, harness.hash, kVlSeq);

    // (d) the next caller gets a HIT on the bundle both waiters were told
    // had failed
    auto third = harness.spawn_waiter();
    harness.run();
    auto bundle = third.get();
    EXPECT_EQ(bundle.seq, kSeq);
}
