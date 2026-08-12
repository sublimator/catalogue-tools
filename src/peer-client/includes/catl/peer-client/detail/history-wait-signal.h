#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/system/error_code.hpp>
#include <chrono>
#include <memory>
#include <vector>

namespace catl::peer_client {

class PeerSet;

namespace detail {

// Internal retained one-shot wake. All timer and completion state is consumed
// on strand_. Each waiter owns its deadline timer, so one waiter's timeout
// cannot consume the retained wake or disturb a sibling.
enum class HistoryWaitResult {
    signaled,
    deadline_elapsed,
    canceled,
};

class HistoryWaitSignal : public std::enable_shared_from_this<HistoryWaitSignal>
{
public:
    explicit HistoryWaitSignal(
        boost::asio::strand<boost::asio::io_context::executor_type> strand);

    boost::asio::awaitable<boost::system::error_code>
    wait();

    boost::asio::awaitable<HistoryWaitResult>
    wait_until(std::chrono::steady_clock::time_point deadline);

private:
    friend class catl::peer_client::PeerSet;

    void
    complete() noexcept;

    boost::asio::awaitable<HistoryWaitResult>
    wait_until_on_strand(std::chrono::steady_clock::time_point deadline);

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    //@@authority state=prv.S0137 status=provisional scope=field
    //bind=catl::peer_client::detail::HistoryWaitSignal::waiters_@aa15df6dcebd54a6
    // owner=unresolved owner_hypothesis=history_wait_signal
    // observed_home=history_wait_signal_strand
    // label=registered_per_subscriber_history_deadline_waiters
    std::vector<std::weak_ptr<boost::asio::steady_timer>> waiters_;
    //@@authority state=prv.S0138 status=provisional scope=field
    //bind=catl::peer_client::detail::HistoryWaitSignal::completed_@b08a713a9cb6b9af
    // owner=unresolved owner_hypothesis=history_wait_signal
    // observed_home=history_wait_signal_strand
    // label=retained_history_wait_signal_completion
    bool completed_ = false;
};

}  // namespace detail
}  // namespace catl::peer_client
