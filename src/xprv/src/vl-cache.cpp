#include "xprv/vl-cache.h"
#include "xprv/manifest-utils.h"

#include <catl/common/utils.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
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
    , stop_progress_(strand_, asio::steady_timer::time_point::max())
{
}

//@@authority transition=prv.T0068 status=provisional scope=function
// bind=xprv::start@ff4c4534e3a56929
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0023,prv.S0025 label=start_validator_list_fetch_lifecycle
void
VlCache::start()
{
    auto self = shared_from_this();
    asio::dispatch(strand_, [self]() {
        if (self->phase_ == Phase::running)
            self->do_fetch();
    });
}

//@@authority transition=prv.T0069 status=provisional scope=function
// bind=xprv::do_fetch@08fae73c5cc51d45
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0023,prv.S0025
// label=admit_validator_list_fetch_and_install_terminal_callback
void
VlCache::do_fetch()
{
    if (phase_ != Phase::running || fetch_in_progress_)
        return;
    fetch_in_progress_ = true;

    auto self = shared_from_this();

    PLOGI(log_, "Fetching VL from ", vl_host_, ":", vl_port_, "...");

    auto on_fetch = [self](catl::vl::VlResult result) {
        // VlClient callback — may fire on any thread.
        // Repost onto our strand to update state.
        asio::post(self->strand_, [self, result = std::move(result)]() mutable {
            //@@authority transition=prv.T0075 status=provisional
            // scope=statement bind=expression_statement@9927d359e38a4c49
            // owner=unresolved owner_hypothesis=vl_cache
            // commit=vl_cache_strand touches=prv.S0025
            // label=consume_validator_list_fetch_terminal_callback
            self->fetch_in_progress_ =
                false;  //@@authority-target ids=prv.T0075
            self->fetch_generation_++;
            struct FinishStop
            {
                VlCache& owner;
                ~FinishStop()
                {
                    owner.try_finish_stop_on_strand();
                }
            } finish_stop{*self};

            try
            {
                if (self->phase_ != Phase::running)
                {
                    self->wake_waiters();
                    return;
                }

                // Verify fetched content against the pinned publisher key
                // before storing it. Treat rejection like a failed fetch so
                // the previous list remains available.
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
                        freshness_error,
                        &self->validator_manifest_history_);
                    if (freshness == VlFreshnessDisposition::reject)
                    {
                        result.success = false;
                        result.error =
                            "VL freshness check failed: " + freshness_error;
                    }
                }

                if (result.success)
                {
                    remember_validator_manifests(
                        result.vl, self->validator_manifest_history_);
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
                        // An active cached list remains usable; retry on the
                        // normal refresh cadence.
                        self->schedule_refresh();
                    }
                }
            }
            catch (std::exception const& e)
            {
                self->last_fetch_ok_ = false;
                self->last_fetch_error_ =
                    std::string("VL result observer failed: ") + e.what();
                PLOGE(VlCache::log_, self->last_fetch_error_);
                self->wake_waiters();
            }
            catch (...)
            {
                self->last_fetch_ok_ = false;
                self->last_fetch_error_ = "VL result observer failed";
                PLOGE(VlCache::log_, self->last_fetch_error_);
                self->wake_waiters();
            }
        });
    };

    try
    {
        if (fetch_for_test_)
        {
            fetch_for_test_(on_fetch);
        }
        else
        {
            catl::vl::VlClient client(io_, vl_host_, vl_port_);
            client.fetch(on_fetch);
        }
    }
    catch (std::exception const& e)
    {
        catl::vl::VlResult failed;
        failed.error = std::string("VL fetch initiation failed: ") + e.what();
        on_fetch(std::move(failed));
    }
    catch (...)
    {
        catl::vl::VlResult failed;
        failed.error = "VL fetch initiation failed";
        on_fetch(std::move(failed));
    }
}

void
VlCache::stop()
{
    auto self = shared_from_this();
    asio::dispatch(strand_, [self]() { self->begin_stop_on_strand(); });
}

//@@authority transition=prv.T0070 status=provisional scope=function
// bind=xprv::co_stop@fb06248bb1a1b4d5
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0023,prv.S0024,prv.S0025,prv.S0026
// label=join_validator_list_timer_and_fetch_obligations
asio::awaitable<void>
VlCache::co_stop()
{
    auto self = shared_from_this();
    co_await asio::post(strand_, asio::use_awaitable);
    begin_stop_on_strand();
    if (phase_ == Phase::stopped)
        co_return;

    boost::system::error_code ec;
    co_await stop_progress_.async_wait(
        asio::redirect_error(asio::use_awaitable, ec));

    auto const cancel_state = co_await asio::this_coro::cancellation_state;
    if (cancel_state.cancelled() != asio::cancellation_type::none)
        throw boost::system::system_error(asio::error::operation_aborted);
}

//@@authority transition=prv.T0071 status=provisional scope=function
// bind=xprv::begin_stop_on_strand@47e9653c14d73494
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0023,prv.S0024 label=seal_validator_list_refresh_admission
void
VlCache::begin_stop_on_strand()
{
    if (phase_ == Phase::stopped)
        return;
    phase_ = Phase::stopping;
    try
    {
        refresh_.cancel();
    }
    catch (...)
    {
    }
    wake_waiters();
    try_finish_stop_on_strand();
}

//@@authority transition=prv.T0072 status=provisional scope=function
// bind=xprv::try_finish_stop_on_strand@de9c8c9d34878d72
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0023,prv.S0024,prv.S0025,prv.S0026
// label=commit_validator_list_cache_stopped_when_children_empty
void
VlCache::try_finish_stop_on_strand()
{
    if (phase_ != Phase::stopping || refresh_wait_outstanding_ != 0 ||
        fetch_in_progress_)
        return;
    phase_ = Phase::stopped;
    try
    {
        stop_progress_.cancel();
    }
    catch (...)
    {
    }
}

//@@authority transition=prv.T0073 status=provisional scope=function
// bind=xprv::schedule_refresh@4728af8d30731abe
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0024 label=arm_refresh_timer_obligation
void
VlCache::schedule_refresh()
{
    if (phase_ != Phase::running)
        return;
    auto self = shared_from_this();
    refresh_.expires_after(refresh_interval_);
    ++refresh_wait_outstanding_;
    try
    {
        refresh_.async_wait([self](boost::system::error_code ec) {
            if (self->refresh_wait_outstanding_ != 0)
                --self->refresh_wait_outstanding_;
            if (!ec && self->phase_ == Phase::running)
                self->do_fetch();
            self->try_finish_stop_on_strand();
        });
    }
    catch (...)
    {
        --refresh_wait_outstanding_;
        throw;
    }
}

//@@authority transition=prv.T0074 status=provisional scope=function
// bind=xprv::schedule_retry@d94ed85885bf7cbd
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0024 label=arm_retry_timer_obligation
void
VlCache::schedule_retry(std::chrono::seconds delay)
{
    if (phase_ != Phase::running)
        return;
    auto self = shared_from_this();
    refresh_.expires_after(delay);
    ++refresh_wait_outstanding_;
    try
    {
        refresh_.async_wait([self](boost::system::error_code ec) {
            if (self->refresh_wait_outstanding_ != 0)
                --self->refresh_wait_outstanding_;
            if (!ec && self->phase_ == Phase::running)
                self->do_fetch();
            self->try_finish_stop_on_strand();
        });
    }
    catch (...)
    {
        --refresh_wait_outstanding_;
        throw;
    }
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

    if (phase_ != Phase::running)
        throw boost::system::system_error(asio::error::operation_aborted);

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

        if (phase_ != Phase::running)
        {
            cleanup.clean_now();
            throw boost::system::system_error(asio::error::operation_aborted);
        }

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

    if (phase_ != Phase::running)
        throw boost::system::system_error(asio::error::operation_aborted);

    auto const generation = fetch_generation_;
    try
    {
        refresh_.cancel();
    }
    catch (...)
    {
    }
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

        if (phase_ != Phase::running)
        {
            cleanup.clean_now();
            throw boost::system::system_error(asio::error::operation_aborted);
        }

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
