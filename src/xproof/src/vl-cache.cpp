#include "xproof/vl-cache.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace xproof {

namespace asio = boost::asio;

LogPartition VlCache::log_("vl-cache", LogLevel::INHERIT);

VlCache::VlCache(asio::io_context& io, std::string vl_host, uint16_t vl_port)
    : io_(io)
    , strand_(asio::make_strand(io))
    , vl_host_(std::move(vl_host))
    , vl_port_(vl_port)
    , signal_(strand_, asio::steady_timer::time_point::max())
    , refresh_(strand_)
{
}

void
VlCache::start()
{
    do_fetch();
}

void
VlCache::do_fetch()
{
    if (fetch_in_progress_)
        return;
    fetch_in_progress_ = true;

    auto self = shared_from_this();
    catl::vl::VlClient client(io_, vl_host_, vl_port_);

    PLOGI(log_, "Fetching VL from ", vl_host_, ":", vl_port_, "...");

    client.fetch([self](catl::vl::VlResult result) {
        // VlClient callback — may fire on any thread.
        // Repost onto our strand to update state.
        asio::post(self->strand_, [self, result = std::move(result)]() mutable {
            self->fetch_in_progress_ = false;

            if (result.success)
            {
                bool first_load = !self->vl_.has_value();
                self->vl_ = std::move(result.vl);
                PLOGI(
                    VlCache::log_,
                    "VL loaded: ",
                    self->vl_->validators.size(),
                    " validators from ",
                    self->vl_host_,
                    first_load ? " (initial)" : " (refresh)");

                if (first_load)
                {
                    // Wake anyone waiting in co_get()
                    self->signal_.cancel();
                }

                // Notify subscriber (e.g. ValidationBuffer) on every load
                if (self->on_refresh_)
                {
                    self->on_refresh_(*self->vl_);
                }
            }
            else
            {
                PLOGE(VlCache::log_, "VL fetch failed: ", result.error);
            }

            self->schedule_refresh();
        });
    });
}

void
VlCache::stop()
{
    refresh_.cancel();
    signal_.cancel();
}

void
VlCache::schedule_refresh()
{
    auto self = shared_from_this();
    refresh_.expires_after(refresh_interval_);
    refresh_.async_wait([self](boost::system::error_code ec) {
        if (!ec)
        {
            self->do_fetch();
        }
    });
}

asio::awaitable<catl::vl::ValidatorList>
VlCache::co_get()
{
    // Hop to our strand to read vl_
    auto self = shared_from_this();
    co_await asio::post(strand_, asio::use_awaitable);

    if (vl_)
    {
        co_return *vl_;
    }

    // Wait for first fetch
    boost::system::error_code ec;
    co_await signal_.async_wait(asio::redirect_error(asio::use_awaitable, ec));

    if (!vl_)
    {
        throw std::runtime_error("VL fetch failed — no data after signal");
    }

    co_return *vl_;
}

asio::awaitable<bool>
VlCache::co_is_loaded()
{
    co_await asio::post(strand_, asio::use_awaitable);
    co_return vl_.has_value();
}

}  // namespace xproof
