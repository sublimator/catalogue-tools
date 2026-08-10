#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-set.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <future>
#include <optional>
#include <vector>

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
        : peers(PeerSet::create(
              io,
              PeerSetOptions{
                  .max_targeted_connect_attempts = 3,
                  .max_active_ledger_demands = 32,
                  .max_acquisitions_per_demand = 8,
                  .max_history_promotions_per_demand = 1,
                  .max_history_blind_uses_per_demand = 1}))
        , now(catl::core::RequestScope::Clock::now())
        , scope(catl::core::RequestScope::create(now + 5s))
    {
    }

    LedgerDemandOptions
    options(std::uint32_t ledger = 50) const
    {
        return LedgerDemandOptions{
            .ledger_seq = ledger,
            .fallback_at = now + 1s,
            .remaining_new_dials = 99,
            .remaining_promotions = 99,
            .remaining_blind_uses = 99};
    }

    HistoryAcquisitionOptions
    acquisition(std::uint64_t fetch = 1, std::uint64_t epoch = 1) const
    {
        return HistoryAcquisitionOptions{
            .fetch = HistoryFetchId{fetch},
            .fetch_epoch = epoch,
            .request_kind = HistoryRequestKind::ledger_header};
    }

    boost::asio::io_context io;
    std::shared_ptr<PeerSet> peers;
    catl::core::RequestScope::Clock::time_point now;
    std::shared_ptr<catl::core::RequestScope> scope;
};

TEST(HistoryDemand, DuplicateGetReturnsOneParentAndOneClampedBudget)
{
    Fixture f;
    auto first = run(
        f.io, f.peers->co_get_or_create_ledger_demand(f.scope, f.options()));
    auto second = run(
        f.io, f.peers->co_get_or_create_ledger_demand(f.scope, f.options()));

    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_EQ(first.id(), second.id());
    auto snapshot = run(f.io, f.peers->co_ledger_demand_snapshot(first));
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->remaining_new_dials, 3u);
    EXPECT_EQ(snapshot->remaining_promotions, 1u);
    EXPECT_EQ(snapshot->remaining_blind_uses, 1u);
}

TEST(HistoryDemand, IncompatibleDuplicateCannotMintAnotherBudget)
{
    Fixture f;
    auto parent = run(
        f.io, f.peers->co_get_or_create_ledger_demand(f.scope, f.options()));
    auto incompatible = f.options();
    incompatible.fallback_at += 1ms;

    EXPECT_THROW(
        run(f.io,
            f.peers->co_get_or_create_ledger_demand(f.scope, incompatible)),
        PeerClientException);
    auto snapshot = run(f.io, f.peers->co_ledger_demand_snapshot(parent));
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->id, parent.id());
}

TEST(HistoryDemand, ConcurrentGetOrCreateSerializesToOneParent)
{
    Fixture f;
    std::vector<std::future<LedgerDemandRef>> futures;
    for (std::size_t i = 0; i < 16; ++i)
    {
        futures.push_back(boost::asio::co_spawn(
            f.io,
            f.peers->co_get_or_create_ledger_demand(f.scope, f.options()),
            boost::asio::use_future));
    }
    for (;;)
    {
        auto const ready =
            std::all_of(futures.begin(), futures.end(), [](auto& future) {
                return future.wait_for(0ms) == std::future_status::ready;
            });
        if (ready)
            break;
        f.io.poll();
        f.io.restart();
    }
    f.io.restart();

    auto const first = futures.front().get().id();
    ASSERT_TRUE(first);
    for (std::size_t i = 1; i < futures.size(); ++i)
        EXPECT_EQ(futures[i].get().id(), first);
}

TEST(HistoryDemand, ChildrenOwnContextButDoNotDuplicateParentCounters)
{
    Fixture f;
    auto parent = run(
        f.io, f.peers->co_get_or_create_ledger_demand(f.scope, f.options()));
    auto first =
        run(f.io,
            f.peers->co_open_history_acquisition(parent, f.acquisition(7, 2)));
    auto second =
        run(f.io,
            f.peers->co_open_history_acquisition(parent, f.acquisition(7, 2)));

    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_NE(first.context().acquisition, second.context().acquisition);
    EXPECT_EQ(first.context().fetch, HistoryFetchId{7});
    EXPECT_EQ(first.context().fetch_epoch, 2u);
    auto snapshot = run(f.io, f.peers->co_ledger_demand_snapshot(parent));
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->acquisitions, 2u);
    EXPECT_EQ(snapshot->active_acquisitions, 2u);
    EXPECT_EQ(snapshot->remaining_new_dials, 3u);
    EXPECT_EQ(snapshot->remaining_promotions, 1u);
    EXPECT_EQ(snapshot->remaining_blind_uses, 1u);
}

TEST(HistoryDemand, ChildFinishIsStickyAndAcknowledged)
{
    Fixture f;
    auto parent = run(
        f.io, f.peers->co_get_or_create_ledger_demand(f.scope, f.options()));
    auto child = run(
        f.io, f.peers->co_open_history_acquisition(parent, f.acquisition()));

    run(f.io,
        f.peers->co_finish_history_acquisition(
            child, HistoryAcquisitionTerminal::completed));
    run(f.io,
        f.peers->co_finish_history_acquisition(
            child, HistoryAcquisitionTerminal::cancelled));
    auto snapshot = run(f.io, f.peers->co_ledger_demand_snapshot(parent));
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->acquisitions, 1u);
    EXPECT_EQ(snapshot->active_acquisitions, 0u);
}

TEST(HistoryDemand, RequestCancellationClosesParentAndEveryChild)
{
    Fixture f;
    auto parent = run(
        f.io, f.peers->co_get_or_create_ledger_demand(f.scope, f.options()));
    (void)run(
        f.io,
        f.peers->co_open_history_acquisition(parent, f.acquisition(1, 1)));
    (void)run(
        f.io,
        f.peers->co_open_history_acquisition(parent, f.acquisition(2, 1)));

    ASSERT_TRUE(f.scope->cancel(f.now + 100ms));
    auto snapshot = run(f.io, f.peers->co_ledger_demand_snapshot(parent));
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->terminal, LedgerDemandTerminal::cancelled);
    EXPECT_EQ(snapshot->active_acquisitions, 0u);
    EXPECT_THROW(
        run(f.io,
            f.peers->co_open_history_acquisition(parent, f.acquisition(3, 1))),
        PeerClientException);
}

TEST(HistoryDemand, SameRequestGetsDistinctParentsForDistinctLedgers)
{
    Fixture f;
    auto first = run(
        f.io, f.peers->co_get_or_create_ledger_demand(f.scope, f.options(50)));
    auto second = run(
        f.io, f.peers->co_get_or_create_ledger_demand(f.scope, f.options(51)));
    EXPECT_NE(first.id(), second.id());
    EXPECT_EQ(first.request_scope_id(), second.request_scope_id());
}

TEST(HistoryDemand, ForeignParentRefusesBeforeCollidingLocalIdCanMutate)
{
    boost::asio::io_context io;
    auto first = PeerSet::create(io);
    auto second = PeerSet::create(io);
    auto const now = catl::core::RequestScope::Clock::now();
    auto first_scope = catl::core::RequestScope::create(now + 5s);
    auto second_scope = catl::core::RequestScope::create(now + 5s);
    LedgerDemandOptions const options{
        .ledger_seq = 55, .fallback_at = now + 1s, .remaining_new_dials = 1};

    auto foreign =
        run(io, first->co_get_or_create_ledger_demand(first_scope, options));
    EXPECT_FALSE(run(io, second->co_ledger_demand_snapshot(foreign)));
    EXPECT_THROW(
        run(io,
            second->co_open_history_acquisition(
                foreign,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{1}, .fetch_epoch = 1})),
        PeerClientException);

    auto local =
        run(io, second->co_get_or_create_ledger_demand(second_scope, options));
    ASSERT_EQ(local.id(), foreign.id())
        << "the control requires colliding instance-local numeric ids";
    auto before = run(io, second->co_ledger_demand_snapshot(local));
    ASSERT_TRUE(before);

    EXPECT_FALSE(run(io, second->co_ledger_demand_snapshot(foreign)));
    EXPECT_THROW(
        run(io,
            second->co_open_history_acquisition(
                foreign,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{2}, .fetch_epoch = 1})),
        PeerClientException);
    auto after = run(io, second->co_ledger_demand_snapshot(local));
    ASSERT_TRUE(after);
    EXPECT_EQ(after->acquisitions, before->acquisitions);
    EXPECT_EQ(after->active_acquisitions, before->active_acquisitions);
}

TEST(HistoryDemand, PrunedTerminalParentIdentityIsNeverReused)
{
    Fixture f;
    auto first = run(
        f.io, f.peers->co_get_or_create_ledger_demand(f.scope, f.options()));
    ASSERT_TRUE(f.scope->cancel(f.now + 100ms));
    auto terminal = run(f.io, f.peers->co_ledger_demand_snapshot(first));
    ASSERT_TRUE(terminal);
    ASSERT_EQ(terminal->terminal, LedgerDemandTerminal::cancelled);

    f.scope.reset();
    auto successor_scope =
        catl::core::RequestScope::create(f.now + std::chrono::seconds(5));
    auto successor = run(
        f.io,
        f.peers->co_get_or_create_ledger_demand(successor_scope, f.options()));

    EXPECT_GT(successor.id().value, first.id().value);
    EXPECT_FALSE(run(f.io, f.peers->co_ledger_demand_snapshot(first)));
}

TEST(HistoryDemand, LastRequestOwnerClosesParentAndChildrenAsCompleted)
{
    Fixture f;
    auto parent = run(
        f.io, f.peers->co_get_or_create_ledger_demand(f.scope, f.options()));
    (void)run(
        f.io,
        f.peers->co_open_history_acquisition(parent, f.acquisition(1, 1)));

    f.scope.reset();
    auto terminal = run(f.io, f.peers->co_ledger_demand_snapshot(parent));
    ASSERT_TRUE(terminal);
    EXPECT_EQ(terminal->terminal, LedgerDemandTerminal::completed);
    EXPECT_EQ(terminal->active_acquisitions, 0u);

    auto successor_scope =
        catl::core::RequestScope::create(f.now + std::chrono::seconds(5));
    auto successor = run(
        f.io,
        f.peers->co_get_or_create_ledger_demand(successor_scope, f.options()));
    EXPECT_GT(successor.id().value, parent.id().value);
    EXPECT_FALSE(run(f.io, f.peers->co_ledger_demand_snapshot(parent)));
}

TEST(HistoryDemand, AbsoluteDeadlineClosesParentWithoutAnotherCaller)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(io);
    auto const now = catl::core::RequestScope::Clock::now();
    auto scope = catl::core::RequestScope::create(now + 20ms);
    auto parent =
        run(io,
            peers->co_get_or_create_ledger_demand(
                scope,
                LedgerDemandOptions{
                    .ledger_seq = 60,
                    .fallback_at = now + 5ms,
                    .remaining_new_dials = 1}));

    auto snapshot =
        run(io,
            [peers, parent]()
                -> boost::asio::awaitable<std::optional<LedgerDemandSnapshot>> {
                auto ex = co_await boost::asio::this_coro::executor;
                boost::asio::steady_timer timer(ex, 30ms);
                co_await timer.async_wait(boost::asio::use_awaitable);
                co_return co_await peers->co_ledger_demand_snapshot(parent);
            }());
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->terminal, LedgerDemandTerminal::deadline_elapsed);
}

TEST(HistoryDemand, ParentAndChildRegistryLimitsFailClosed)
{
    boost::asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .max_active_ledger_demands = 1, .max_acquisitions_per_demand = 1});
    auto const now = catl::core::RequestScope::Clock::now();
    auto first_scope = catl::core::RequestScope::create(now + 5s);
    auto second_scope = catl::core::RequestScope::create(now + 5s);
    LedgerDemandOptions const options{
        .ledger_seq = 70, .fallback_at = now + 1s, .remaining_new_dials = 1};
    auto parent =
        run(io, peers->co_get_or_create_ledger_demand(first_scope, options));
    (void)run(
        io,
        peers->co_open_history_acquisition(
            parent,
            HistoryAcquisitionOptions{
                .fetch = HistoryFetchId{1}, .fetch_epoch = 1}));

    EXPECT_THROW(
        run(io,
            peers->co_open_history_acquisition(
                parent,
                HistoryAcquisitionOptions{
                    .fetch = HistoryFetchId{1}, .fetch_epoch = 1})),
        PeerClientException);
    EXPECT_THROW(
        run(io, peers->co_get_or_create_ledger_demand(second_scope, options)),
        PeerClientException);
}

}  // namespace
