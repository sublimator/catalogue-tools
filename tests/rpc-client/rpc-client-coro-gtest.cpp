// Witness suite for issue 0077: the RPC callback bridge has no serialized
// terminal funnel.
//
// TESTS-ONLY. These controls exercise the production inline adapter and the
// real detached RpcClient request frame without changing either one. The
// suite distinguishes four facts:
//
//   completion     -> the adapter returns the published RpcResult;
//   cancellation   -> currently returns a default RpcResult (FIRED);
//   cancel+complete-> forced across a strand/raw-executor boundary for TSan;
//   callback throw -> swallowed by asio::detached (FIRED).
//
// There is no existing seam between RpcResult assignment and timer cancel,
// so an injected publication fault cannot be witnessed without production
// mutation. That acceptance cell remains BLOCKED-ON-SEAM in issue 0077.

#include <catl/rpc-client/rpc-client-coro.h>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>

namespace asio = boost::asio;
using catl::rpc::RpcCallback;
using catl::rpc::RpcClient;
using catl::rpc::RpcResult;
using namespace std::chrono_literals;

namespace {

RpcResult
marked_result(std::uint64_t marker)
{
    RpcResult result;
    result.result = marker;
    result.success = true;
    return result;
}

template <typename T>
bool
is_ready(std::future<T>& future)
{
    return future.wait_for(5s) == std::future_status::ready;
}

}  // namespace

// Probe-validity control: an ordinary asynchronous callback publication wakes
// the exact waiter and preserves the result.
TEST(RpcCallbackBridge0077, CompletionReturnsPublishedResult)
{
    asio::io_context io;
    std::optional<RpcCallback> callback;
    auto waiter = asio::co_spawn(
        io,
        catl::rpc::detail::callback_to_awaitable(
            [&callback](RpcCallback cb) { callback.emplace(std::move(cb)); }),
        asio::use_future);

    ASSERT_GT(io.poll(), 0u);
    ASSERT_TRUE(callback);
    (*callback)(marked_result(77));
    io.restart();
    io.run();

    ASSERT_TRUE(is_ready(waiter));
    auto result = waiter.get();
    ASSERT_TRUE(result.success);
    ASSERT_TRUE(result.result.is_uint64());
    EXPECT_EQ(result.result.as_uint64(), 77u);
}

// FIRED: caller cancellation is not a typed terminal. The timer reports
// operation_aborted, the adapter ignores that cause, and a value-identical
// default RpcResult is returned as though a callback had published it.
TEST(RpcCallbackBridge0077, CallerCancellationReturnsEmptyRpcResult)
{
    asio::io_context io;
    std::optional<RpcCallback> callback;
    asio::cancellation_signal cancellation;
    auto waiter = asio::co_spawn(
        io,
        catl::rpc::detail::callback_to_awaitable(
            [&callback](RpcCallback cb) { callback.emplace(std::move(cb)); }),
        asio::bind_cancellation_slot(cancellation.slot(), asio::use_future));

    ASSERT_GT(io.poll(), 0u);
    ASSERT_TRUE(callback);
    cancellation.emit(asio::cancellation_type::terminal);
    io.restart();
    io.run();

    ASSERT_TRUE(is_ready(waiter));
    auto result = waiter.get();
    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.error.empty());
    EXPECT_TRUE(result.result.is_null())
        << "cancellation currently counterfeits an empty callback result";

    // The callback retains the shared state after the waiter has consumed the
    // counterfeit result. Invoking it remains safe enough to retire this test
    // fixture, but no consumer exists to observe the late publication.
    (*callback)(marked_result(78));
}

// TSan cell: the waiter is born on a strand, then caller cancellation and
// callback completion are released simultaneously from two foreign threads.
// The current adapter directly mutates the shared RpcResult and timer from the
// callback thread. The test accepts either currently observable value result;
// its oracle is ThreadSanitizer, not scheduler luck.
TEST(RpcCallbackBridge0077, ForeignCompletionRacesCallerCancellation)
{
    constexpr std::size_t rounds = 32;

    for (std::size_t round = 0; round < rounds; ++round)
    {
        asio::io_context io;
        auto owner = asio::make_strand(io);
        auto work = asio::make_work_guard(io);
        asio::cancellation_signal cancellation;
        std::promise<RpcCallback> callback_ready;
        auto callback_future = callback_ready.get_future();
        std::promise<void> owner_fence;
        auto owner_fence_future = owner_fence.get_future();

        auto waiter = asio::co_spawn(
            owner,
            catl::rpc::detail::callback_to_awaitable(
                [&callback_ready](RpcCallback cb) {
                    callback_ready.set_value(std::move(cb));
                }),
            asio::bind_cancellation_slot(
                cancellation.slot(), asio::use_future));
        asio::post(owner, [&owner_fence] { owner_fence.set_value(); });

        std::thread runner_a([&io] { io.run(); });
        std::thread runner_b([&io] { io.run(); });
        auto const callback_became_ready = is_ready(callback_future);
        auto const owner_fence_became_ready = is_ready(owner_fence_future);
        if (!callback_became_ready || !owner_fence_became_ready)
        {
            work.reset();
            io.stop();
            runner_a.join();
            runner_b.join();
            FAIL() << "callback bridge did not arm on its owner strand";
        }
        auto callback = callback_future.get();

        std::barrier release(3);
        std::thread canceler([&] {
            release.arrive_and_wait();
            cancellation.emit(asio::cancellation_type::terminal);
        });
        std::thread completer([&, callback = std::move(callback)]() mutable {
            release.arrive_and_wait();
            callback(marked_result(round + 1));
        });
        release.arrive_and_wait();
        canceler.join();
        completer.join();

        auto const waiter_became_ready = is_ready(waiter);
        std::optional<RpcResult> result;
        if (waiter_became_ready)
            result.emplace(waiter.get());

        work.reset();
        io.stop();
        runner_a.join();
        runner_b.join();

        ASSERT_TRUE(waiter_became_ready);
        EXPECT_TRUE(result->result.is_null() || result->result.is_uint64());
    }
}

// Domination control for the TSan cell above (added in claude's
// adversarial review): a plain unsynchronized read/write pair released
// from two raw threads through the same std::barrier topology the race
// cell uses. A -fsanitize=thread build MUST report it; that proves the
// toolchain catches this defect class in this exact release shape, so a
// quiet TSan run of ForeignCompletionRacesCallerCancellation would be
// an admissible DID-NOT-FIRE rather than a null probe. (In non-TSan
// builds this is benign test-local state.)
TEST(RpcCallbackBridge0077, TsanDominationControlSeededPair)
{
    static std::uint64_t seeded_cell = 0;
    std::uint64_t sink = 0;

    for (int round = 0; round < 32; ++round)
    {
        std::barrier release(3);
        std::thread writer([&] {
            release.arrive_and_wait();
            for (int i = 0; i < 1000; ++i)
                seeded_cell = seeded_cell + 1;
        });
        std::thread reader([&] {
            release.arrive_and_wait();
            for (int i = 0; i < 1000; ++i)
                sink += seeded_cell;
        });
        release.arrive_and_wait();
        writer.join();
        reader.join();
    }
    RecordProperty("seeded_sink", static_cast<int>(sink % 1024));
    SUCCEED();
}

// FIRED: RpcClient invokes the user callback after its catch block, while the
// request coroutine is owned by asio::detached. A thrown callback therefore
// reaches the detached completion handler and disappears: io_context::run()
// returns normally and no caller receives the exception.
TEST(RpcCallbackBridge0077, DetachedRequestDiscardsCallbackException)
{
    using tcp = asio::ip::tcp;

    asio::io_context io;
    tcp::acceptor reservation(
        io, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    auto const unused_port = reservation.local_endpoint().port();
    boost::system::error_code ignored;
    reservation.close(ignored);

    RpcClient client(io, "127.0.0.1", unused_port, 1);
    std::atomic_bool callback_entered = false;
    client.call("server_info", {}, [&callback_entered](RpcResult) {
        callback_entered.store(true, std::memory_order_release);
        throw std::runtime_error("0077 callback publication failure");
    });

    EXPECT_NO_THROW(io.run())
        << "asio::detached currently absorbs the callback exception";
    EXPECT_TRUE(callback_entered.load(std::memory_order_acquire));
}
