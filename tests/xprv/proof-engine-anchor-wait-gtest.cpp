// Permanent inversion controls for issue 0074.
//
// One anchor key owns one shared operation object. The operation publishes a
// typed terminal under ProofEngine::cache_mutex_ before waking subscribers
// through writ::retained_signal. A subscriber deadline/cancellation retires
// only that subscriber; invalidation and builder failure remain shared
// terminals, and a stale builder cannot publish into a successor row.

#include <xprv/network-config.h>
#include <xprv/proof-engine.h>
#include <xprv/validation-quorum-tracker.h>
#include <xprv/vl-cache.h>

#include "proof-engine-test-access.h"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

namespace asio = boost::asio;
using catl::peer_client::Error;
using catl::peer_client::PeerClientException;
using xprv::ProofEngineTestAccess;
using namespace std::chrono_literals;

namespace {

constexpr char const* kBuilderFailure = "0074 exact builder failure";
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

struct Failure
{
    std::optional<Error> error;
    std::string detail;
    std::string message;
};

Failure
failure_of(std::future<ProofEngineTestAccess::AnchorBundle>& waiter)
{
    try
    {
        (void)waiter.get();
    }
    catch (PeerClientException const& failure)
    {
        return Failure{failure.error, failure.detail, failure.what()};
    }
    catch (std::runtime_error const& failure)
    {
        return Failure{std::nullopt, {}, failure.what()};
    }
    catch (...)
    {
        return Failure{std::nullopt, {}, "(non-runtime_error exception)"};
    }
    return Failure{std::nullopt, {}, "(no exception)"};
}

struct AnchorWaitHarness
{
    asio::io_context io;
    std::shared_ptr<xprv::ProofEngine> engine =
        xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
    Hash256 hash = test_anchor_hash();
    ProofEngineTestAccess::AnchorOperationPtr operation;

    ProofEngineTestAccess::AnchorOperationPtr
    create_in_flight()
    {
        operation = ProofEngineTestAccess::create_in_flight_anchor(
            *engine, kSeq, hash, kVlSeq);
        return operation;
    }

    bool
    in_flight()
    {
        return ProofEngineTestAccess::anchor_entry_in_flight(
            *engine, kSeq, hash, kVlSeq);
    }

    std::future<ProofEngineTestAccess::AnchorBundle>
    spawn_waiter(std::chrono::milliseconds timeout)
    {
        return asio::co_spawn(
            io,
            ProofEngineTestAccess::join_anchor_wait(
                engine, kSeq, hash, kVlSeq, timeout),
            asio::use_future);
    }

    std::future<ProofEngineTestAccess::AnchorBundle>
    spawn_cancellable_waiter(
        std::chrono::milliseconds timeout,
        asio::cancellation_signal& cancellation)
    {
        return asio::co_spawn(
            io,
            ProofEngineTestAccess::join_anchor_wait(
                engine, kSeq, hash, kVlSeq, timeout),
            asio::bind_cancellation_slot(
                cancellation.slot(), asio::use_future));
    }

    template <typename Fn>
    void
    schedule(std::chrono::milliseconds after, Fn fn)
    {
        asio::co_spawn(
            io,
            [this, after, fn = std::move(fn)]() -> asio::awaitable<void> {
                asio::steady_timer timer(io, after);
                co_await timer.async_wait(asio::use_awaitable);
                fn();
            },
            asio::detached);
    }

    // A real builder servant whose frame remains live across a subscriber's
    // deadline, then publishes through the exact operation object it retained
    // at admission.
    std::future<void>
    spawn_builder_success(
        std::shared_ptr<asio::steady_timer> release,
        std::atomic_bool& running)
    {
        auto admitted = operation;
        return asio::co_spawn(
            io,
            [this,
             admitted = std::move(admitted),
             release = std::move(release),
             &running]() -> asio::awaitable<void> {
                running.store(true, std::memory_order_release);
                boost::system::error_code error;
                co_await release->async_wait(
                    asio::redirect_error(asio::use_awaitable, error));
                if (error != asio::error::operation_aborted)
                    throw std::runtime_error(
                        "builder release gate completed unexpectedly");
                if (!ProofEngineTestAccess::complete_anchor_build(
                        *engine, kSeq, hash, kVlSeq, admitted))
                {
                    throw std::runtime_error(
                        "exact builder publication was refused");
                }
                running.store(false, std::memory_order_release);
            },
            asio::use_future);
    }

    template <typename T>
    void
    run_until(std::future<T>& target)
    {
        if (io.stopped())
            io.restart();
        while (target.wait_for(0ms) != std::future_status::ready)
        {
            if (io.run_one() == 0)
                throw std::runtime_error(
                    "io_context exhausted before target future completed");
        }
    }

    std::size_t
    poll()
    {
        if (io.stopped())
            io.restart();
        return io.poll();
    }

    void
    run()
    {
        if (io.stopped())
            io.restart();
        io.run();
    }
};

}  // namespace

TEST(ProofEngineAnchorWait0074, CompletionWakeReturnsBundle)
{
    AnchorWaitHarness harness;
    auto operation = harness.create_in_flight();
    auto waiter = harness.spawn_waiter(10s);
    harness.schedule(10ms, [&harness, operation] {
        EXPECT_TRUE(ProofEngineTestAccess::complete_anchor_build(
            *harness.engine, kSeq, harness.hash, kVlSeq, operation));
    });
    harness.run();

    auto bundle = waiter.get();
    EXPECT_EQ(bundle.seq, kSeq);
    EXPECT_EQ(bundle.vl_sequence, kVlSeq);
}

TEST(ProofEngineAnchorWait0074, WaiterDeadlineIsLocalWhileBuilderRemainsLive)
{
    AnchorWaitHarness harness;
    harness.create_in_flight();
    auto waiter = harness.spawn_waiter(50ms);
    harness.run_until(waiter);

    auto const failure = failure_of(waiter);
    ASSERT_EQ(failure.error, Error::Timeout);
    EXPECT_EQ(failure.detail, "anchor bundle waiter deadline elapsed");
    EXPECT_TRUE(harness.in_flight())
        << "a subscriber deadline must not terminalize the builder";
}

TEST(ProofEngineAnchorWait0074, InvalidationIsTypedCancellation)
{
    AnchorWaitHarness harness;
    harness.create_in_flight();
    auto waiter = harness.spawn_waiter(10s);
    harness.schedule(10ms, [&harness] {
        ProofEngineTestAccess::invalidate_anchor_cache(*harness.engine);
    });
    harness.run();

    auto const failure = failure_of(waiter);
    ASSERT_EQ(failure.error, Error::Cancelled);
    EXPECT_EQ(failure.detail, "anchor bundle operation was invalidated");
}

TEST(ProofEngineAnchorWait0074, BuilderFailureIsRetainedExactly)
{
    AnchorWaitHarness harness;
    auto operation = harness.create_in_flight();
    auto waiter = harness.spawn_waiter(10s);
    harness.schedule(10ms, [&harness, operation] {
        EXPECT_TRUE(ProofEngineTestAccess::fail_anchor_build(
            *harness.engine,
            kSeq,
            harness.hash,
            kVlSeq,
            operation,
            kBuilderFailure));
    });
    harness.run();

    auto const failure = failure_of(waiter);
    EXPECT_FALSE(failure.error);
    EXPECT_EQ(failure.message, kBuilderFailure);
}

TEST(ProofEngineAnchorWait0074, SiblingDeadlineDoesNotPoisonLiveWaiter)
{
    AnchorWaitHarness harness;
    auto operation = harness.create_in_flight();
    auto short_waiter = harness.spawn_waiter(50ms);
    auto long_waiter = harness.spawn_waiter(10s);
    harness.run_until(short_waiter);

    auto const failure = failure_of(short_waiter);
    ASSERT_EQ(failure.error, Error::Timeout);
    EXPECT_TRUE(harness.in_flight());
    EXPECT_NE(long_waiter.wait_for(0ms), std::future_status::ready)
        << "one subscriber deadline must not ready a live sibling";

    EXPECT_TRUE(ProofEngineTestAccess::complete_anchor_build(
        *harness.engine, kSeq, harness.hash, kVlSeq, operation));
    harness.run();
    EXPECT_EQ(long_waiter.get().seq, kSeq);
}

TEST(ProofEngineAnchorWait0074, LocalTimeoutDoesNotAmplifyToLateJoiners)
{
    AnchorWaitHarness harness;
    harness.create_in_flight();
    std::atomic_bool builder_running{false};
    auto builder_release = std::make_shared<asio::steady_timer>(
        harness.io, asio::steady_timer::time_point::max());
    auto builder =
        harness.spawn_builder_success(builder_release, builder_running);

    auto first = harness.spawn_waiter(50ms);
    harness.run_until(first);
    auto const first_failure = failure_of(first);
    ASSERT_EQ(first_failure.error, Error::Timeout);
    ASSERT_TRUE(builder_running.load(std::memory_order_acquire));
    ASSERT_TRUE(harness.in_flight());

    // A later subscriber owns a fresh deadline. Nonblocking poll may start its
    // retained wait, but cannot make it ready while the shared builder lives.
    auto second = harness.spawn_waiter(10s);
    EXPECT_GT(harness.poll(), 0u);
    EXPECT_NE(second.wait_for(0ms), std::future_status::ready);
    EXPECT_TRUE(harness.in_flight());
    EXPECT_TRUE(builder_running.load(std::memory_order_acquire));

    builder_release->cancel();
    harness.run();
    builder.get();
    EXPECT_FALSE(builder_running.load(std::memory_order_acquire));

    auto second_bundle = second.get();
    EXPECT_EQ(second_bundle.seq, kSeq);

    auto third = harness.spawn_waiter(10s);
    harness.run();
    EXPECT_EQ(third.get().seq, kSeq) << "success remains a cache hit";
}

TEST(ProofEngineAnchorWait0074, CompletionBeforeRegistrationIsRetained)
{
    AnchorWaitHarness harness;
    auto operation = harness.create_in_flight();
    bool published = false;
    ProofEngineTestAccess::set_before_anchor_wait_subscribe(
        *harness.engine, [&harness, operation, &published] {
            published = ProofEngineTestAccess::complete_anchor_build(
                *harness.engine, kSeq, harness.hash, kVlSeq, operation);
        });

    auto waiter = harness.spawn_waiter(10s);
    harness.run();

    EXPECT_TRUE(published);
    EXPECT_EQ(waiter.get().seq, kSeq);
}

TEST(ProofEngineAnchorWait0074, StaleBuilderCannotPublishAfterInvalidation)
{
    AnchorWaitHarness harness;
    auto stale = harness.create_in_flight();
    ProofEngineTestAccess::AnchorOperationPtr successor;
    ProofEngineTestAccess::set_before_anchor_wait_subscribe(
        *harness.engine, [&harness, &successor] {
            ProofEngineTestAccess::invalidate_anchor_cache(*harness.engine);
            successor = harness.create_in_flight();
        });

    auto invalidated = harness.spawn_waiter(10s);
    harness.run_until(invalidated);
    auto const failure = failure_of(invalidated);
    ProofEngineTestAccess::set_before_anchor_wait_subscribe(
        *harness.engine, {});
    ASSERT_EQ(failure.error, Error::Cancelled);
    ASSERT_TRUE(successor);
    ASSERT_TRUE(harness.in_flight());

    EXPECT_FALSE(ProofEngineTestAccess::complete_anchor_build(
        *harness.engine, kSeq, harness.hash, kVlSeq, stale));
    EXPECT_FALSE(ProofEngineTestAccess::fail_anchor_build(
        *harness.engine, kSeq, harness.hash, kVlSeq, stale, kBuilderFailure));
    EXPECT_TRUE(harness.in_flight())
        << "stale publication must not overwrite the successor";

    EXPECT_TRUE(ProofEngineTestAccess::complete_anchor_build(
        *harness.engine, kSeq, harness.hash, kVlSeq, successor));
    auto current = harness.spawn_waiter(10s);
    harness.run();
    EXPECT_EQ(current.get().seq, kSeq);
}

TEST(ProofEngineAnchorWait0074, CompletionWithoutRequestScopeReturnsBundle)
{
    AnchorWaitHarness harness;
    auto operation = harness.create_in_flight();
    auto waiter = asio::co_spawn(
        harness.io,
        ProofEngineTestAccess::join_anchor_wait_without_request_scope(
            harness.engine, kSeq, harness.hash, kVlSeq),
        asio::use_future);
    harness.schedule(10ms, [&harness, operation] {
        EXPECT_TRUE(ProofEngineTestAccess::complete_anchor_build(
            *harness.engine, kSeq, harness.hash, kVlSeq, operation));
    });
    harness.run();

    auto bundle = waiter.get();
    EXPECT_EQ(bundle.seq, kSeq);
    EXPECT_EQ(bundle.vl_sequence, kVlSeq);
}

TEST(ProofEngineAnchorWait0074, ParentCancellationIsLocal)
{
    AnchorWaitHarness harness;
    harness.create_in_flight();
    asio::cancellation_signal cancellation;
    auto waiter = harness.spawn_cancellable_waiter(10s, cancellation);
    harness.schedule(10ms, [&cancellation] {
        // HTTP session stop emits `all` (http-server.cpp). That mask includes
        // `terminal`, which is what asio::awaitable's default filter
        // propagates. `total` alone does not.
        cancellation.emit(asio::cancellation_type::all);
    });
    harness.run_until(waiter);

    auto const failure = failure_of(waiter);
    ASSERT_EQ(failure.error, Error::Cancelled);
    EXPECT_EQ(failure.detail, "anchor bundle waiter was cancelled");
    EXPECT_TRUE(harness.in_flight())
        << "one canceled subscriber must not poison the shared operation";
}

// Same laws for the publisher (Nicholas): once the publisher key is revoked the
// engine serves no proof under it, not even one already cached, and the
// tracker holds the revocation for /verify.
TEST(ProofEnginePublisherRevocation, ProveRefusesCachedAndNewProofs)
{
    asio::io_context io;
    auto engine =
        xprv::ProofEngine::create(io, xprv::NetworkConfig::for_network(0));
    auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto tracker = xprv::ValidationQuorumTracker::create(io, protocol, 0);
    ProofEngineTestAccess::set_validation_quorum_tracker(*engine, tracker);
    std::string const tx(64, 'A');
    ProofEngineTestAccess::seed_proof_cache(
        *engine, tx, xprv::ProofEngine::ProveResult{{}, 1000, "ED"});

    catl::vl::Manifest revocation;
    revocation.master_public_key = std::vector<uint8_t>(33, 0xED);
    revocation.sequence = std::numeric_limits<uint32_t>::max();
    revocation.raw = std::vector<uint8_t>(40, 0xEE);
    ProofEngineTestAccess::publisher_revoked(*engine, revocation);

    auto proved = asio::co_spawn(io, engine->prove(tx), asio::use_future);
    io.run();
    io.restart();
    try
    {
        proved.get();
        ADD_FAILURE() << "prove served a proof under a revoked publisher";
    }
    catch (std::runtime_error const& e)
    {
        EXPECT_STREQ(e.what(), xprv::VlCache::kPublisherRevoked);
    }
    auto const held = tracker->held_manifests();
    EXPECT_NE(
        std::find(held->begin(), held->end(), revocation.raw), held->end());

    auto stopped = asio::co_spawn(io, tracker->co_stop(), asio::use_future);
    io.run();
    stopped.get();
}
