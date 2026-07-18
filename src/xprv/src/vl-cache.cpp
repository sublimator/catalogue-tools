#include "xprv/vl-cache.h"
#include "xprv/manifest-utils.h"

#include <catl/common/utils.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>

namespace xprv {

namespace asio = boost::asio;

namespace {

bool
is_active(catl::vl::ValidatorList const& vl)
{
    std::string error;
    return assess_vl_freshness(
               vl, nullptr, catl::common::current_ripple_time(), error) !=
        VlFreshnessDisposition::reject;
}

}  // namespace

LogPartition VlCache::log_("vl-cache", LogLevel::INHERIT);

VlCache::VlCache(asio::io_context& io, std::string vl_host, uint16_t vl_port)
    : io_(io)
    , strand_(asio::make_strand(io))
    , vl_host_(std::move(vl_host))
    , vl_port_(vl_port)
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
            self->fetch_generation_++;

            // Verify fetched content against the pinned publisher key before
            // storing it. Treat rejection like a failed fetch so the previous
            // list remains available.
            if (result.success && self->verify_)
            {
                std::string verr;
                try
                {
                    if (!self->verify_(result.vl, verr))
                    {
                        result.success = false;
                        result.error = "VL verification failed: " + verr;
                    }
                }
                catch (std::exception const& e)
                {
                    result.success = false;
                    result.error =
                        std::string("VL verification failed: ") + e.what();
                }
            }

            auto freshness = VlFreshnessDisposition::accept;
            if (result.success)
            {
                std::string freshness_error;
                freshness = assess_vl_freshness(
                    result.vl,
                    self->vl_ ? &*self->vl_ : nullptr,
                    catl::common::current_ripple_time(),
                    freshness_error);
                if (freshness == VlFreshnessDisposition::reject)
                {
                    result.success = false;
                    result.error =
                        "VL freshness check failed: " + freshness_error;
                }
            }

            if (result.success &&
                freshness == VlFreshnessDisposition::unchanged)
            {
                self->last_fetch_ok_ = true;
                self->last_fetch_error_.clear();
                self->initial_retry_ = std::chrono::seconds(2);
                self->wake_waiters();
                self->schedule_refresh();
                return;
            }

            if (result.success)
            {
                bool first_load = !self->vl_.has_value();
                self->vl_ = std::move(result.vl);
                self->last_fetch_ok_ = true;
                self->last_fetch_error_.clear();
                self->initial_retry_ =
                    std::chrono::seconds(2);  // reset for next cold start
                PLOGI(
                    VlCache::log_,
                    "VL loaded: ",
                    self->vl_->validators.size(),
                    " validators from ",
                    self->vl_host_,
                    first_load ? " (initial)" : " (refresh)");

                // Notify subscriber (e.g. ValidationBuffer) on every load
                if (self->on_refresh_)
                {
                    self->on_refresh_(*self->vl_);
                }

                self->wake_waiters();
                self->schedule_refresh();
            }
            else
            {
                self->last_fetch_ok_ = false;
                self->last_fetch_error_ = result.error;
                PLOGE(VlCache::log_, "VL fetch failed: ", result.error);
                self->wake_waiters();

                if (!self->vl_.has_value() || !is_active(*self->vl_))
                {
                    // Never loaded — retry aggressively
                    auto delay = self->initial_retry_;
                    PLOGW(
                        VlCache::log_,
                        "VL not yet loaded, retrying in ",
                        delay.count(),
                        "s");
                    self->schedule_retry(delay);
                    // Exponential backoff: 2s, 4s, 8s, 16s, 32s, 60s cap
                    self->initial_retry_ = std::min(
                        self->initial_retry_ * 2, self->initial_retry_cap_);
                }
                else
                {
                    // An active cached list remains usable; retry on the normal
                    // refresh cadence.
                    self->schedule_refresh();
                }
            }
        });
    });
}

void
VlCache::stop()
{
    refresh_.cancel();
    wake_waiters();
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

void
VlCache::schedule_retry(std::chrono::seconds delay)
{
    auto self = shared_from_this();
    refresh_.expires_after(delay);
    refresh_.async_wait([self](boost::system::error_code ec) {
        if (!ec)
        {
            self->do_fetch();
        }
    });
}

void
VlCache::wake_waiters()
{
    auto waiters = std::move(waiters_);
    waiters_.clear();
    for (auto& timer : waiters)
        timer->cancel();
}

asio::awaitable<catl::vl::ValidatorList>
VlCache::co_get()
{
    // Hop to our strand to read vl_
    auto self = shared_from_this();
    co_await asio::post(strand_, asio::use_awaitable);

    if (vl_ && is_active(*vl_))
    {
        co_return *vl_;
    }

    if (!fetch_in_progress_)
        do_fetch();

    auto const generation = fetch_generation_;
    auto signal = std::make_shared<asio::steady_timer>(
        strand_, asio::steady_timer::time_point::max());
    waiters_.push_back(signal);

    struct WaiterCleanup
    {
        VlCache* owner = nullptr;
        asio::strand<asio::io_context::executor_type> strand;
        std::weak_ptr<VlCache> self;
        std::shared_ptr<asio::steady_timer> signal;

        void
        clean_now()
        {
            if (!signal)
                return;
            std::erase(owner->waiters_, signal);
            signal.reset();
        }

        ~WaiterCleanup()
        {
            if (!signal)
                return;
            try
            {
                asio::post(
                    strand,
                    [self = std::move(self),
                     signal = std::move(signal)]() mutable {
                        if (auto locked = self.lock())
                            std::erase(locked->waiters_, signal);
                    });
            }
            catch (...)
            {
            }
        }
    } cleanup{this, strand_, self, signal};

    while (true)
    {
        signal->expires_at(asio::steady_timer::time_point::max());
        boost::system::error_code ec;
        co_await signal->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));

        auto const cancel_state = co_await asio::this_coro::cancellation_state;
        if (cancel_state.cancelled() != asio::cancellation_type::none)
            throw boost::system::system_error(asio::error::operation_aborted);

        co_await asio::post(strand_, asio::use_awaitable);

        if (fetch_generation_ == generation)
            continue;

        cleanup.clean_now();

        if (!vl_ || !is_active(*vl_))
        {
            throw std::runtime_error(
                "VL fetch failed: " +
                (last_fetch_error_.empty() ? std::string("unknown error")
                                           : last_fetch_error_));
        }

        co_return *vl_;
    }
}

asio::awaitable<catl::vl::ValidatorList>
VlCache::co_refresh()
{
    auto self = shared_from_this();
    co_await asio::post(strand_, asio::use_awaitable);

    auto const generation = fetch_generation_;
    refresh_.cancel();
    if (!fetch_in_progress_)
        do_fetch();

    auto signal = std::make_shared<asio::steady_timer>(
        strand_, asio::steady_timer::time_point::max());
    waiters_.push_back(signal);

    struct WaiterCleanup
    {
        VlCache* owner = nullptr;
        asio::strand<asio::io_context::executor_type> strand;
        std::weak_ptr<VlCache> self;
        std::shared_ptr<asio::steady_timer> signal;

        void
        clean_now()
        {
            if (!signal)
                return;
            std::erase(owner->waiters_, signal);
            signal.reset();
        }

        ~WaiterCleanup()
        {
            if (!signal)
                return;
            try
            {
                asio::post(
                    strand,
                    [self = std::move(self),
                     signal = std::move(signal)]() mutable {
                        if (auto locked = self.lock())
                            std::erase(locked->waiters_, signal);
                    });
            }
            catch (...)
            {
            }
        }
    } cleanup{this, strand_, self, signal};

    while (true)
    {
        signal->expires_at(asio::steady_timer::time_point::max());
        boost::system::error_code ec;
        co_await signal->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));

        auto const cancel_state = co_await asio::this_coro::cancellation_state;
        if (cancel_state.cancelled() != asio::cancellation_type::none)
            throw boost::system::system_error(asio::error::operation_aborted);

        co_await asio::post(strand_, asio::use_awaitable);

        if (fetch_generation_ == generation)
            continue;

        cleanup.clean_now();

        if (!last_fetch_ok_ || !vl_ || !is_active(*vl_))
        {
            throw std::runtime_error(
                "VL refresh failed: " +
                (last_fetch_error_.empty() ? std::string("unknown error")
                                           : last_fetch_error_));
        }

        co_return *vl_;
    }
}

asio::awaitable<bool>
VlCache::co_is_loaded()
{
    co_await asio::post(strand_, asio::use_awaitable);
    co_return vl_.has_value() && is_active(*vl_);
}

}  // namespace xprv
