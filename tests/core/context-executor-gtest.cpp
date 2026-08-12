#include <catl/core/context-executor.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/execution/executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

namespace {

using ContextExecutor =
    catl::core::ContextExecutor<boost::asio::io_context::executor_type>;

static_assert(boost::asio::execution::is_executor<ContextExecutor>::value);
static_assert(!boost::asio::is_executor<ContextExecutor>::value);

TEST(ContextExecutor, IdentityIncludesRequestCapability)
{
    boost::asio::io_context io;
    auto first_context = std::make_shared<catl::core::RequestContext const>(
        "first", "test", std::chrono::steady_clock::now());
    auto second_context = std::make_shared<catl::core::RequestContext const>(
        "second", "test", std::chrono::steady_clock::now());

    ContextExecutor first(io.get_executor(), first_context);
    ContextExecutor same(io.get_executor(), first_context);
    ContextExecutor second(io.get_executor(), second_context);

    EXPECT_EQ(first, same);
    EXPECT_NE(first, second);
    EXPECT_EQ(
        boost::asio::any_io_executor(first),
        boost::asio::any_io_executor(same));
    EXPECT_NE(
        boost::asio::any_io_executor(first),
        boost::asio::any_io_executor(second));
}

TEST(ContextExecutor, ModernWorkTrackingAndTlsRemainAvailable)
{
    boost::asio::io_context io;
    auto request = std::make_shared<catl::core::RequestContext const>(
        "work", "test", std::chrono::steady_clock::now());
    ContextExecutor executor(io.get_executor(), request);
    auto work = boost::asio::make_work_guard(executor);

    auto observed = boost::asio::co_spawn(
        executor,
        []() -> boost::asio::awaitable<catl::core::RequestContext const*> {
            co_return catl::core::current_request();
        },
        boost::asio::use_future);

    work.reset();
    io.run();
    EXPECT_EQ(observed.get(), request.get());
}

}  // namespace
