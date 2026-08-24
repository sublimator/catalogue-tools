// Permanent inversion controls for issue 0077: the RPC callback bridge now
// commits every terminal disposition through one serialized operation home.
//
// The suite distinguishes five facts:
//
//   completion     -> the adapter returns the published RpcResult;
//   cancellation   -> raises operation_aborted, never an empty RpcResult;
//   cancel+complete-> yields exactly one typed terminal under TSan;
//   publish fault  -> reaches the exact waiter as its original exception;
//   callback throw -> reaches the io_context runner instead of disappearing.

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
using catl::rpc::RpcConnectionError;
using catl::rpc::RpcException;
using catl::rpc::RpcResult;
using namespace std::chrono_literals;

namespace {

#if defined(__has_feature)
constexpr bool kThreadSanitizerEnabled = __has_feature(thread_sanitizer);
#elif defined(__SANITIZE_THREAD__)
constexpr bool kThreadSanitizerEnabled = true;
#else
constexpr bool kThreadSanitizerEnabled = false;
#endif

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

std::uint16_t
unused_local_port(asio::io_context& io)
{
    using tcp = asio::ip::tcp;
    tcp::acceptor reservation(
        io, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    auto const port = reservation.local_endpoint().port();
    boost::system::error_code ignored;
    reservation.close(ignored);
    return port;
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

// Inversion control: caller cancellation is its own terminal disposition. It
// must never be projected as a value-identical default callback result.
TEST(RpcCallbackBridge0077, CallerCancellationRaisesOperationAborted)
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
    try
    {
        (void)waiter.get();
        FAIL() << "cancellation returned a counterfeit RpcResult";
    }
    catch (boost::system::system_error const& error)
    {
        EXPECT_EQ(error.code(), asio::error::operation_aborted);
    }

    // The callback may still own the operation state after its subscriber has
    // canceled. A late terminal publication must be harmless.
    (*callback)(marked_result(78));
    io.restart();
    EXPECT_NO_THROW(io.run());
}

// TSan inversion cell: the waiter is born on a strand, then caller
// cancellation and callback completion are released simultaneously from two
// foreign threads. Both candidates enter the operation strand before touching
// retained state. Either exact terminal may win; an empty result may not.
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
        bool canceled = false;
        if (waiter_became_ready)
        {
            try
            {
                result.emplace(waiter.get());
            }
            catch (boost::system::system_error const& error)
            {
                EXPECT_EQ(error.code(), asio::error::operation_aborted);
                canceled = true;
            }
        }

        work.reset();
        io.stop();
        runner_a.join();
        runner_b.join();

        ASSERT_TRUE(waiter_became_ready);
        ASSERT_TRUE(canceled || result.has_value());
        if (result)
        {
            ASSERT_TRUE(result->success);
            ASSERT_TRUE(result->result.is_uint64());
            EXPECT_EQ(result->result.as_uint64(), round + 1);
        }
    }
}

// Inversion control for the formerly blocked publication-fault cell. The hook
// fires after the RpcResult is staged but before its terminal is committed.
// The exact exception must replace the staged value and wake the waiter.
TEST(RpcCallbackBridge0077, PublicationFaultReachesExactWaiter)
{
    asio::io_context io;
    std::optional<RpcCallback> callback;
    auto waiter = asio::co_spawn(
        io,
        catl::rpc::detail::callback_to_awaitable(
            [&callback](RpcCallback cb) { callback.emplace(std::move(cb)); },
            [] { throw std::runtime_error("0077 staged publication fault"); }),
        asio::use_future);

    ASSERT_GT(io.poll(), 0u);
    ASSERT_TRUE(callback);
    (*callback)(marked_result(79));
    io.restart();
    io.run();

    ASSERT_TRUE(is_ready(waiter));
    try
    {
        (void)waiter.get();
        FAIL() << "publication fault returned the staged RpcResult";
    }
    catch (std::runtime_error const& error)
    {
        EXPECT_STREQ(error.what(), "0077 staged publication fault");
    }
}

// Domination control for the TSan cell above (added in claude's
// adversarial review): a plain unsynchronized read/write pair released
// from two raw threads through the same std::barrier topology the race
// cell uses. A -fsanitize=thread build MUST report it; that proves the
// toolchain catches this defect class in this exact release shape, so a
// quiet TSan run of ForeignCompletionRacesCallerCancellation would be
// an admissible DID-NOT-FIRE rather than a null probe. (In non-TSan
// builds the control is skipped: an intentional data race is still undefined
// behavior even when it is test-local.)
TEST(RpcCallbackBridge0077, TsanDominationControlSeededPair)
{
    if (!kThreadSanitizerEnabled)
        GTEST_SKIP() << "domination control requires a TSan build";

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

// Inversion control: the request frame has an observable completion handler.
// A raw callback exception must reach the io_context runner instead of being
// discarded by asio::detached.
TEST(RpcCallbackBridge0077, RequestSurfacesCallbackException)
{
    asio::io_context io;
    auto const unused_port = unused_local_port(io);

    RpcClient client(io, "127.0.0.1", unused_port, 1);
    std::atomic_bool callback_entered = false;
    client.call("server_info", {}, [&callback_entered](RpcResult) {
        callback_entered.store(true, std::memory_order_release);
        throw std::runtime_error("0077 callback publication failure");
    });

    EXPECT_THROW(io.run(), std::runtime_error);
    EXPECT_TRUE(callback_entered.load(std::memory_order_acquire));
}

// Public-surface control: all three wrappers execute through the repaired
// adapter while preserving their existing result/error mapping. The string
// arguments are deliberately temporary: coroutine parameters must own values
// that survive until their frames first resume.
TEST(RpcCallbackBridge0077, PublicCoroutineWrappersUseTerminalFunnel)
{
    {
        asio::io_context io;
        RpcClient client(io, "127.0.0.1", unused_local_port(io), 1);
        auto result = asio::co_spawn(
            io,
            catl::rpc::co_call(client, std::string("server_info")),
            asio::use_future);
        io.run();
        ASSERT_TRUE(is_ready(result));
        auto value = result.get();
        EXPECT_FALSE(value.success);
        EXPECT_FALSE(value.error.empty());
    }

    {
        asio::io_context io;
        RpcClient client(io, "127.0.0.1", unused_local_port(io), 1);
        auto result = asio::co_spawn(
            io, catl::rpc::co_server_definitions(client), asio::use_future);
        io.run();
        ASSERT_TRUE(is_ready(result));
        EXPECT_THROW((void)result.get(), RpcException);
    }

    {
        asio::io_context io;
        RpcClient client(io, "127.0.0.1", unused_local_port(io), 1);
        auto result = asio::co_spawn(
            io,
            catl::rpc::co_tx(client, std::string(64, 'A')),
            asio::use_future);
        io.run();
        ASSERT_TRUE(is_ready(result));
        EXPECT_THROW((void)result.get(), RpcConnectionError);
    }
}
