#include "node-batch-test-session.h"
#include "peer-set-test-access.h"

#include <catl/peer-client/peer-use.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <thread>

using namespace catl::peer_client;
using namespace std::chrono_literals;

namespace {

template <class T>
T
run(boost::asio::io_context& io, boost::asio::awaitable<T> operation)
{
    auto result = boost::asio::co_spawn(
        io, std::move(operation), boost::asio::use_future);
    while (result.wait_for(1ms) != std::future_status::ready)
    {
        io.poll();
        io.restart();
    }
    io.restart();
    return result.get();
}

struct Fixture
{
    Fixture()
        : peers(PeerSet::create(io))
        , session(std::make_shared<NodeBatchTestSession>("127.0.0.1:51235"))
    {
        session->attach_strand(io);
        generation = PeerSetTestAccess::insert_ready_peer_with_generation(
            io, peers, session);
        ref = PeerSetTestAccess::generation_ref(io, peers, session);
    }

    PeerUse
    acquire(std::chrono::steady_clock::time_point deadline)
    {
        return run(
            io,
            peers->co_acquire_peer_use(
                ref,
                {.kind = PeerUseKind::ledger_header,
                 .ledger_seq = 50,
                 .absolute_deadline = deadline}));
    }

    void
    poll_until(std::function<bool()> predicate)
    {
        auto const limit = std::chrono::steady_clock::now() + 1s;
        while (!predicate() && std::chrono::steady_clock::now() < limit)
        {
            io.poll();
            io.restart();
            std::this_thread::sleep_for(1ms);
        }
        ASSERT_TRUE(predicate());
    }

    boost::asio::io_context io;
    std::shared_ptr<PeerSet> peers;
    std::shared_ptr<NodeBatchTestSession> session;
    std::uint64_t generation = 0;
    PeerGenerationRef ref;
};

TEST(PeerUse, InstallPrecedesCallerAndFinishReleasesExactGeneration)
{
    Fixture f;
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    EXPECT_EQ(use.generation().generation(), f.generation);
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 1u);
    EXPECT_TRUE(
        PeerSetTestAccess::generation_has_use(f.io, f.peers, f.generation));

    use.finish(PeerUseOutcome::success);
    f.poll_until([&] {
        return PeerSetTestAccess::active_peer_use_count(f.io, f.peers) == 0;
    });
    EXPECT_FALSE(
        PeerSetTestAccess::generation_has_use(f.io, f.peers, f.generation));
    EXPECT_EQ(
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::success),
        1u);
}

TEST(PeerUse, DuplicateCompletionHasOneOwnerSideEffect)
{
    Fixture f;
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto duplicate = completion;

    completion.finish(PeerUseOutcome::success);
    duplicate.finish(PeerUseOutcome::transport_failure);
    f.poll_until([&] {
        return PeerSetTestAccess::active_peer_use_count(f.io, f.peers) == 0;
    });
    EXPECT_EQ(
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::success),
        1u);
    EXPECT_EQ(
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::transport_failure),
        0u);
}

TEST(PeerUse, DroppedHandlePostsAbandonedFallback)
{
    Fixture f;
    {
        auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
        ASSERT_TRUE(use);
    }
    f.poll_until([&] {
        return PeerSetTestAccess::active_peer_use_count(f.io, f.peers) == 0;
    });
}

TEST(PeerUse, DeadlineMakesLateCompletionInert)
{
    Fixture f;
    auto use = f.acquire(std::chrono::steady_clock::now() + 20ms);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();

    f.poll_until([&] {
        return PeerSetTestAccess::active_peer_use_count(f.io, f.peers) == 0;
    });
    EXPECT_EQ(
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::timeout),
        1u);
    completion.finish(PeerUseOutcome::success);
    f.io.poll();
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 0u);
}

TEST(PeerUse, StaleGenerationReferenceCannotInstallUse)
{
    Fixture f;
    auto replacement =
        std::make_shared<NodeBatchTestSession>("127.0.0.1:51235");
    replacement->attach_strand(f.io);
    PeerSetTestAccess::insert_ready_peer_with_generation(
        f.io, f.peers, replacement);

    auto stale = f.acquire(std::chrono::steady_clock::now() + 5s);
    EXPECT_FALSE(stale);
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 0u);
}

TEST(PeerUse, ShutdownFinishesOutstandingUseBeforeSessionClose)
{
    Fixture f;
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();

    run(f.io, f.peers->co_stop());
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 0u);
    completion.finish(PeerUseOutcome::success);
    f.io.poll();
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 0u);
}

}  // namespace
