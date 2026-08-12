#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/system/error_code.hpp>

namespace catl::peer_client {

class PeerSet;

namespace detail {

// Internal retained one-shot wake. All timer and completion state is consumed
// on strand_; wait() dispatches there when a test or adapter calls it
// elsewhere.
class HistoryWaitSignal
{
public:
    explicit HistoryWaitSignal(
        boost::asio::strand<boost::asio::io_context::executor_type> strand);

    boost::asio::awaitable<boost::system::error_code>
    wait();

private:
    friend class catl::peer_client::PeerSet;

    void
    complete() noexcept;

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    boost::asio::steady_timer timer_;
    bool completed_ = false;
};

}  // namespace detail
}  // namespace catl::peer_client
