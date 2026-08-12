#include <writ/retained-signal.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <utility>

namespace asio = boost::asio;

namespace writ {

retained_signal::retained_signal(home h) : home_(std::move(h))
{
}

void
retained_signal::complete() noexcept
{
    WRIT_ASSERT_HOME(home_);
    if (completed_)
        return;
    completed_ = true;
    for (auto const& retained : waiters_)
    {
        if (auto waiter = retained.lock())
        {
            try
            {
                waiter->cancel();
            }
            catch (...)
            {
            }
        }
    }
    waiters_.clear();
}

void
retained_signal::post_complete() noexcept
{
    try
    {
        asio::post(home_.executor(), [self = shared_from_this()]() noexcept {
            self->complete();
        });
    }
    catch (...)
    {
    }
}

bool
retained_signal::completed() const noexcept
{
    WRIT_ASSERT_HOME(home_);
    return completed_;
}

std::shared_ptr<asio::steady_timer>
retained_signal::register_wait(time_point deadline)
{
    WRIT_ASSERT_HOME(home_);
    if (completed_)
        return {};
    auto waiter =
        std::make_shared<asio::steady_timer>(home_.executor(), deadline);
    std::erase_if(
        waiters_, [](auto const& retained) { return retained.expired(); });
    waiters_.push_back(waiter);
    return waiter;
}

wait_outcome
retained_signal::consume_wait(
    std::shared_ptr<asio::steady_timer> const& waiter,
    boost::system::error_code const& error,
    asio::cancellation_type_t cancellation) noexcept
{
    WRIT_ASSERT_HOME(home_);
    std::erase_if(waiters_, [&waiter](auto const& retained) {
        auto current = retained.lock();
        return !current || current == waiter;
    });
    if (cancellation != asio::cancellation_type::none)
        return wait_outcome::canceled;
    if (completed_)
        return wait_outcome::signaled;
    if (!error)
        return wait_outcome::deadline_elapsed;
    return wait_outcome::canceled;
}

asio::awaitable<wait_outcome, retained_signal::executor_type>
retained_signal::wait_until(time_point deadline)
{
    auto const preexisting = co_await asio::this_coro::cancellation_state;
    if (preexisting.cancelled() != asio::cancellation_type::none)
        co_return wait_outcome::canceled;

    auto self = shared_from_this();
    auto waiter = self->register_wait(deadline);
    if (!waiter)
        co_return wait_outcome::signaled;
    boost::system::error_code error;
    co_await waiter->async_wait(
        asio::redirect_error(asio::use_awaitable_t<executor_type>{}, error));
    auto const resumed = co_await asio::this_coro::cancellation_state;
    co_return self->consume_wait(waiter, error, resumed.cancelled());
}

asio::awaitable<wait_outcome>
retained_signal::co_wait_until(time_point deadline)
{
    auto const caller_cancellation =
        co_await asio::this_coro::cancellation_state;
    if (caller_cancellation.cancelled() != asio::cancellation_type::none)
        co_return wait_outcome::canceled;

    auto self = shared_from_this();
    try
    {
        co_return co_await asio::co_spawn(
            home_.executor(),
            [self, deadline]() -> asio::awaitable<wait_outcome, executor_type> {
                co_return co_await self->wait_until(deadline);
            },
            asio::use_awaitable);
    }
    catch (boost::system::system_error const& error)
    {
        if (error.code() == asio::error::operation_aborted)
            co_return wait_outcome::canceled;
        throw;
    }
}

}  // namespace writ
