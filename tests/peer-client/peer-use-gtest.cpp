#include "node-batch-test-session.h"
#include "peer-set-test-access.h"

#include <catl/peer-client/peer-use-operations.h>
#include <catl/peer-client/peer-use.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <numeric>
#include <thread>
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

TEST(PeerUse, ReceiptWaitsForOwnerCommit)
{
    Fixture f;
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto receipt = use.receipt();

    use.finish(PeerUseOutcome::cancelled);
    EXPECT_FALSE(receipt.outcome());
    EXPECT_EQ(run(f.io, receipt.wait()), PeerUseOutcome::cancelled);
    EXPECT_EQ(
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::cancelled),
        1u);
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

TEST(PeerUse, OneTokenSubmitsAtMostOnePhysicalOperation)
{
    Fixture f;
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    bool callback_seen = false;

    ASSERT_TRUE(PeerUseOperations::get_ledger_header(
        completion,
        50,
        [&callback_seen](
            PeerUseCompletion done, Error error, LedgerHeaderResult) {
            callback_seen = true;
            done.finish(classify_peer_use_error(error));
        }));
    EXPECT_FALSE(PeerUseOperations::get_ledger_header(
        completion, 50, [](PeerUseCompletion, Error, LedgerHeaderResult) {}));
    EXPECT_EQ(f.session->header_calls, 1u);
    ASSERT_TRUE(f.session->header_callback);

    auto callback = std::move(f.session->header_callback);
    callback(Error::Success, {});
    EXPECT_TRUE(callback_seen);
    f.poll_until([&] {
        return PeerSetTestAccess::active_peer_use_count(f.io, f.peers) == 0;
    });
    EXPECT_EQ(
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::success),
        1u);
}

TEST(PeerUse, InlineTransportCallbackCanFinishExactlyOnce)
{
    Fixture f;
    f.session->header_callback_inline = true;
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();

    ASSERT_TRUE(PeerUseOperations::get_ledger_header(
        completion,
        50,
        [](PeerUseCompletion done, Error error, LedgerHeaderResult) {
            done.finish(classify_peer_use_error(error));
        }));
    EXPECT_EQ(run(f.io, receipt.wait()), PeerUseOutcome::success);
    EXPECT_EQ(f.session->header_calls, 1u);
}

TEST(PeerUse, InlineConsumerCannotHoldRevocationGate)
{
    Fixture f;
    f.session->header_callback_inline = true;
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();

    std::promise<void> callback_entered;
    auto entered = callback_entered.get_future();
    std::promise<void> release_callback;
    auto release = release_callback.get_future().share();
    std::atomic<bool> installed{false};
    std::thread submitter([&] {
        installed.store(
            PeerUseOperations::get_ledger_header(
                completion,
                50,
                [&callback_entered, release](
                    PeerUseCompletion done, Error, LedgerHeaderResult) mutable {
                    callback_entered.set_value();
                    release.wait();
                    done.finish(PeerUseOutcome::success);
                }),
            std::memory_order_release);
    });

    if (entered.wait_for(1s) != std::future_status::ready)
    {
        release_callback.set_value();
        submitter.join();
        FAIL() << "inline callback did not reach the held consumer boundary";
        return;
    }
    PeerSetTestAccess::expire_peer_use(f.io, f.peers, completion.ticket());
    EXPECT_EQ(receipt.outcome(), PeerUseOutcome::timeout);
    EXPECT_EQ(f.session->co_disconnect_calls.load(), 1u);

    release_callback.set_value();
    submitter.join();
    EXPECT_TRUE(installed.load(std::memory_order_acquire));
    f.io.poll();
    EXPECT_EQ(
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::success),
        0u);
}

TEST(PeerUse, DuplicateTransportCallbackHasOneOwnerSideEffect)
{
    Fixture f;
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();
    ASSERT_TRUE(PeerUseOperations::get_ledger_header(
        completion,
        50,
        [](PeerUseCompletion done, Error error, LedgerHeaderResult) {
            done.finish(classify_peer_use_error(error));
        }));
    ASSERT_TRUE(f.session->header_callback);

    auto callback = f.session->header_callback;
    callback(Error::Success, {});
    callback(Error::Disconnected, {});

    EXPECT_EQ(run(f.io, receipt.wait()), PeerUseOutcome::success);
    EXPECT_EQ(
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::transport_failure),
        0u);
}

TEST(PeerUse, SubmissionFailureIsClassifiedAndReleased)
{
    Fixture f;
    f.session->header_submit_throws = true;
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();

    EXPECT_THROW(
        (void)PeerUseOperations::get_ledger_header(
            completion,
            50,
            [](PeerUseCompletion, Error, LedgerHeaderResult) {}),
        std::runtime_error);
    EXPECT_EQ(run(f.io, receipt.wait()), PeerUseOutcome::write_failure);
}

TEST(PeerUse, ThrowingConsumerCallbackIsContainedAndReleased)
{
    Fixture f;
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();
    ASSERT_TRUE(PeerUseOperations::get_ledger_header(
        completion, 50, [](PeerUseCompletion, Error, LedgerHeaderResult) {
            throw std::runtime_error("injected consumer failure");
        }));
    ASSERT_TRUE(f.session->header_callback);

    auto callback = std::move(f.session->header_callback);
    callback(Error::Success, {});

    EXPECT_EQ(run(f.io, receipt.wait()), PeerUseOutcome::cancelled);
}

TEST(PeerUse, DeadlineWinningBeforeSubmissionMakesTokenInert)
{
    Fixture f;
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();

    PeerSetTestAccess::expire_peer_use(f.io, f.peers, completion.ticket());
    EXPECT_FALSE(PeerUseOperations::get_ledger_header(
        completion, 50, [](PeerUseCompletion, Error, LedgerHeaderResult) {}));
    EXPECT_EQ(f.session->header_calls, 0u);
    EXPECT_EQ(
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::timeout),
        1u);
}

TEST(PeerUse, SameGenerationStatusChangeInvalidatesObservedReference)
{
    Fixture f;
    PeerSetTestAccess::consume_lifecycle_event(
        f.io,
        f.peers,
        {.endpoint = f.session->endpoint(),
         .generation = f.generation,
         .event_sequence = f.ref.event_sequence() + 1,
         .lifecycle = PeerLifecycle::ready,
         .status = {
             .current_seq = 300,
             .advertised_range = {.first_seq = 200, .last_seq = 300}}});

    EXPECT_FALSE(f.acquire(std::chrono::steady_clock::now() + 5s));

    auto fresh = PeerSetTestAccess::generation_ref(f.io, f.peers, f.session);
    ASSERT_TRUE(fresh);
    EXPECT_NE(fresh.event_sequence(), f.ref.event_sequence());
    auto use =
        run(f.io,
            f.peers->co_acquire_peer_use(
                fresh,
                {.kind = PeerUseKind::ledger_header,
                 .ledger_seq = 250,
                 .absolute_deadline = std::chrono::steady_clock::now() + 5s}));
    ASSERT_TRUE(use);
    use.finish(PeerUseOutcome::success);
}

TEST(PeerUse, RequestLedgerMustMatchCurrentObservedRange)
{
    Fixture f;
    auto use =
        run(f.io,
            f.peers->co_acquire_peer_use(
                f.ref,
                {.kind = PeerUseKind::ledger_header,
                 .ledger_seq = 101,
                 .absolute_deadline = std::chrono::steady_clock::now() + 5s}));

    EXPECT_FALSE(use);
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 0u);
}

TEST(PeerUse, ConcurrentDuplicateCompletionsCommitOneOutcome)
{
    Fixture f;
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();

    std::atomic<bool> start{false};
    std::vector<std::thread> contenders;
    for (std::size_t i = 0; i < 16; ++i)
    {
        contenders.emplace_back([completion, &start, i] {
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();
            completion.finish(
                i % 2 == 0 ? PeerUseOutcome::success
                           : PeerUseOutcome::transport_failure);
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& contender : contenders)
        contender.join();

    auto const outcome = run(f.io, receipt.wait());
    EXPECT_TRUE(
        outcome == PeerUseOutcome::success ||
        outcome == PeerUseOutcome::transport_failure);
    auto const committed =
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::success) +
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::transport_failure);
    EXPECT_EQ(committed, 1u);
}

TEST(PeerUse, LostProposalPostDoesNotDisableDeadlineOwner)
{
    Fixture f;
    auto use = f.acquire(std::chrono::steady_clock::now() + 20ms);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();

    PeerSetTestAccess::drop_next_peer_use_finish_posts(f.peers);
    completion.finish(PeerUseOutcome::success);

    EXPECT_EQ(run(f.io, receipt.wait()), PeerUseOutcome::timeout);
    EXPECT_EQ(
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::success),
        0u);
}

TEST(PeerUse, DelayedOwnerCommitCannotCrossImmutableDeadline)
{
    Fixture f;
    auto use = f.acquire(std::chrono::steady_clock::now() + 20ms);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();

    completion.finish(PeerUseOutcome::success);
    std::this_thread::sleep_for(30ms);

    EXPECT_EQ(run(f.io, receipt.wait()), PeerUseOutcome::timeout);
    EXPECT_EQ(
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::success),
        0u);
}

TEST(PeerUse, SubmittedDeadlineReceiptWaitsForPhysicalDisconnectReceipt)
{
    Fixture f;
    f.session->hold_disconnect_receipt();
    auto use = f.acquire(std::chrono::steady_clock::now() + 20ms);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();

    ASSERT_TRUE(PeerUseOperations::get_ledger_header(
        completion,
        50,
        [](PeerUseCompletion done, Error error, LedgerHeaderResult) {
            done.finish(classify_peer_use_error(error));
        }));
    f.poll_until([&] {
        return f.session->co_disconnect_waiting.load(std::memory_order_acquire);
    });

    EXPECT_FALSE(receipt.outcome());
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 1u);

    f.session->release_disconnect_receipt();
    EXPECT_EQ(run(f.io, receipt.wait()), PeerUseOutcome::timeout);
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 0u);

    ASSERT_TRUE(f.session->header_callback);
    auto callback = std::move(f.session->header_callback);
    callback(Error::Success, {});
    f.io.poll();
    EXPECT_EQ(
        PeerSetTestAccess::peer_use_outcome_count(
            f.io, f.peers, PeerUseOutcome::success),
        0u);
}

TEST(PeerUse, DeadlineCannotBypassNaturalTerminalDrainReceipt)
{
    Fixture f;
    f.session->hold_disconnect_receipt();
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();
    ASSERT_TRUE(PeerUseOperations::get_ledger_header(
        completion, 50, [](PeerUseCompletion, Error, LedgerHeaderResult) {}));

    PeerSetTestAccess::consume_lifecycle_event(
        f.io,
        f.peers,
        {.endpoint = f.session->endpoint(),
         .generation = f.generation,
         .event_sequence = f.ref.event_sequence() + 1,
         .lifecycle = PeerLifecycle::failed,
         .reason = {boost::asio::error::connection_reset, "peer failure"}});
    f.poll_until([&] {
        return f.session->co_disconnect_waiting.load(std::memory_order_acquire);
    });

    PeerSetTestAccess::expire_peer_use(f.io, f.peers, completion.ticket());
    EXPECT_FALSE(receipt.outcome());
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 1u);

    f.session->release_disconnect_receipt();
    EXPECT_EQ(run(f.io, receipt.wait()), PeerUseOutcome::transport_failure);
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 0u);
}

TEST(PeerUse, RevocationWaitsForSubmissionInstallBoundary)
{
    Fixture f;
    f.session->hold_header_submission();
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();

    std::atomic<bool> submitted{false};
    std::thread submitter([&] {
        submitted.store(
            PeerUseOperations::get_ledger_header(
                completion,
                50,
                [](PeerUseCompletion, Error, LedgerHeaderResult) {}),
            std::memory_order_release);
    });
    f.poll_until([&] {
        return f.session->header_submission_waiting.load(
            std::memory_order_acquire);
    });

    auto expiry = std::async(std::launch::async, [&] {
        PeerSetTestAccess::expire_peer_use(f.io, f.peers, completion.ticket());
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_NE(expiry.wait_for(0ms), std::future_status::ready);
    EXPECT_FALSE(receipt.outcome());
    EXPECT_EQ(f.session->co_disconnect_calls.load(), 0u);

    f.session->release_header_submission();
    submitter.join();
    ASSERT_TRUE(submitted.load(std::memory_order_acquire));
    ASSERT_EQ(expiry.wait_for(1s), std::future_status::ready);
    expiry.get();

    EXPECT_EQ(run(f.io, receipt.wait()), PeerUseOutcome::timeout);
    EXPECT_EQ(f.session->header_calls.load(), 1u);
    EXPECT_EQ(f.session->co_disconnect_calls.load(), 1u);
}

TEST(PeerUse, SubmittedShutdownWaitsForPhysicalDisconnectReceipt)
{
    Fixture f;
    f.session->hold_disconnect_receipt();
    auto use = f.acquire(std::chrono::steady_clock::now() + 5s);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();
    ASSERT_TRUE(PeerUseOperations::get_ledger_header(
        completion, 50, [](PeerUseCompletion, Error, LedgerHeaderResult) {}));

    auto stopped = boost::asio::co_spawn(
        f.io, f.peers->co_stop(), boost::asio::use_future);
    f.poll_until([&] {
        return f.session->co_disconnect_waiting.load(std::memory_order_acquire);
    });
    EXPECT_FALSE(receipt.outcome());
    EXPECT_NE(stopped.wait_for(0ms), std::future_status::ready);

    f.session->release_disconnect_receipt();
    while (stopped.wait_for(1ms) != std::future_status::ready)
    {
        f.io.poll();
        f.io.restart();
    }
    stopped.get();
    EXPECT_EQ(receipt.outcome(), PeerUseOutcome::shutdown);
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 0u);
}

TEST(PeerUse, FailedBackgroundDrainCannotPublishReleaseReceipt)
{
    Fixture f;
    f.session->co_disconnect_failures_remaining.store(1);
    auto use = f.acquire(std::chrono::steady_clock::now() + 20ms);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();
    ASSERT_TRUE(PeerUseOperations::get_ledger_header(
        completion, 50, [](PeerUseCompletion, Error, LedgerHeaderResult) {}));

    f.poll_until([&] { return f.session->co_disconnect_calls.load() == 1; });
    EXPECT_FALSE(receipt.outcome());
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 1u);

    run(f.io, f.peers->co_stop());
    EXPECT_EQ(f.session->co_disconnect_calls.load(), 2u);
    EXPECT_EQ(receipt.outcome(), PeerUseOutcome::shutdown);
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 0u);
}

TEST(PeerUse, RepeatedDrainFailurePropagatesWithoutFalseReceipt)
{
    Fixture f;
    f.session->co_disconnect_failures_remaining.store(2);
    auto use = f.acquire(std::chrono::steady_clock::now() + 20ms);
    ASSERT_TRUE(use);
    auto completion = std::move(use).into_completion();
    auto receipt = completion.receipt();
    ASSERT_TRUE(PeerUseOperations::get_ledger_header(
        completion, 50, [](PeerUseCompletion, Error, LedgerHeaderResult) {}));

    f.poll_until([&] { return f.session->co_disconnect_calls.load() == 1; });
    EXPECT_THROW(run(f.io, f.peers->co_stop()), std::runtime_error);
    EXPECT_EQ(f.session->co_disconnect_calls.load(), 2u);
    EXPECT_FALSE(receipt.outcome());
    EXPECT_EQ(PeerSetTestAccess::active_peer_use_count(f.io, f.peers), 1u);
}

}  // namespace
