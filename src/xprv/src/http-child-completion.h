#pragma once

#include <boost/asio/error.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>

#include <exception>
#include <functional>
#include <memory>
#include <utility>

namespace xprv::detail {

using HttpChildCompletionChannel =
    boost::asio::experimental::concurrent_channel<void(
        boost::system::error_code)>;

/// Consume one exact child receipt without inheriting cancellation from the
/// session which is trying to join it. Child outcome is deliberately separate;
/// receiver cancellation or channel closure cannot manufacture a receipt.
//@@authority transition=prv.T0128 status=provisional scope=function bind=xprv::detail::join_http_child@a660f43fc367f977
// owner=unresolved owner_hypothesis=http_session commit=receipt_receive
// touches=prv.S0044 mutation_class=terminal_receipt
// label=consume_exact_http_child_release_receipt
inline boost::asio::awaitable<void>
join_http_child(std::shared_ptr<HttpChildCompletionChannel> const& channel)
{
    co_await boost::asio::this_coro::reset_cancellation_state(
        boost::asio::disable_cancellation());
    boost::system::error_code receipt_error;
    co_await channel->async_receive(
        boost::asio::redirect_error(boost::asio::use_awaitable, receipt_error));
    if (receipt_error)
        throw boost::system::system_error(receipt_error);

    // Successful arrival is the release receipt. The child outcome is
    // deliberately not the join result; completion diagnostics own that
    // separate concern.
    co_return;
}

/// Completion-token funnel for one HTTP child coroutine.
///
/// The body never owns its join receipt. co_spawn invokes this token for both
/// normal return and exception, so a body-tail throw cannot strand its parent.
class HttpChildCompletion
{
public:
    using Diagnostic = std::function<void(std::exception_ptr const& failure)>;

    explicit HttpChildCompletion(
        std::shared_ptr<HttpChildCompletionChannel> channel,
        Diagnostic diagnostic = {})
        : channel_(std::move(channel)), diagnostic_(std::move(diagnostic))
    {
    }

    //@@authority transition=prv.T0127 status=provisional scope=function bind='xprv::detail::HttpChildCompletion::operator()@ed5bf3879cc9dd76'
    // owner=unresolved owner_hypothesis=http_child commit=completion_token
    // touches=prv.S0044 mutation_class=terminal_receipt
    // label=publish_exact_http_child_release_receipt_before_diagnostic
    void
    operator()(std::exception_ptr failure) const noexcept
    {
        try
        {
            if (!channel_->try_send(boost::system::error_code{}))
                std::terminate();
        }
        catch (...)
        {
            // Losing the exact receipt would let a parent counterfeit drain.
            // There is no legal off-executor repair at this point.
            std::terminate();
        }

        // Diagnostics happen strictly after the exact receipt and can neither
        // retract it nor escape this completion boundary.
        if (failure && diagnostic_)
        {
            try
            {
                diagnostic_(failure);
            }
            catch (...)
            {
            }
        }
    }

private:
    //@@authority state=prv.S0044 status=provisional scope=field bind=xprv::detail::HttpChildCompletion::channel_@6f3607db24145e36
    // owner=unresolved owner_hypothesis=http_session
    // observed_home=http_session label=exact_http_child_release_receipt
    std::shared_ptr<HttpChildCompletionChannel> channel_;
    Diagnostic diagnostic_;
};

}  // namespace xprv::detail
