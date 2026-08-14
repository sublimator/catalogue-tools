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
#include <cassert>
#include <stdexcept>

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
// touches=prv.S0023,prv.S0025,prv.S0067,prv.S0068
// label=admit_validator_list_fetch_and_install_terminal_callback
void
VlCache::do_fetch()
{
    if (phase_ != Phase::running || fetch_in_progress_)
        return;

    auto self = shared_from_this();

    // Diagnostics are observers, never fetch-admission authority. Contain
    // every failure before the operation census becomes visible.
    try
    {
        PLOGI(log_, "Fetching VL from ", vl_host_, ":", vl_port_, "...");
    }
    catch (...)
    {
    }
    fetch_in_progress_ = true;

    struct FetchAdmissionRollback
    {
        VlCache& owner;
        bool terminal_producer_committed = false;

        ~FetchAdmissionRollback() noexcept
        {
            if (terminal_producer_committed || !owner.fetch_in_progress_)
                return;
            auto const operation = owner.active_fetch_;
            owner.retire_fetch_on_strand(operation);
            owner.try_finish_stop_on_strand();
        }
    } admission_rollback{*this};

    auto on_fetch = [self](
                        catl::vl::VlFetchOperationPtr operation,
                        catl::vl::VlResult result) {
        auto consume = [self,
                        operation = std::move(operation),
                        result = std::move(result)]() mutable {
            try
            {
                if (self->fetch_terminal_observer_for_test_)
                    self->fetch_terminal_observer_for_test_(operation);
            }
            catch (...)
            {
            }

            if (!self->retire_fetch_on_strand(operation))
            {
                try
                {
                    PLOGW(
                        VlCache::log_,
                        "Ignoring stale VL fetch terminal from a replaced "
                        "operation");
                }
                catch (...)
                {
                }
                return;
            }

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

                    // Notify subscriber (e.g. ValidationQuorumTracker) on every
                    // load
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
        };

        // Production operations complete from co_spawn's physical terminal
        // boundary on this exact owner strand. The post is retained only for
        // the deterministic test seam, whose callback may be injected from
        // outside the executor.
        if (self->strand_.running_in_this_thread())
            consume();
        else
            asio::post(self->strand_, std::move(consume));
    };

    auto publish_start_refusal = [&](char const* detail) {
        catl::vl::VlResult failed;
        try
        {
            if (initiation_failure_diagnostic_for_test_)
                initiation_failure_diagnostic_for_test_();
            failed.error = "VL fetch initiation failed";
            if (detail && *detail)
            {
                failed.error += ": ";
                failed.error += detail;
            }
        }
        catch (...)
        {
        }
        //@@authority transition=prv.T0216 status=provisional scope=statement
        // bind=expression_statement@04751e2fe4506cda
        // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
        // boundary_ref=vl_fetch.operation_terminal
        // receipt=vl_fetch.operation_terminal receipt_role=publish
        // label=publish_synchronous_fetch_start_refusal_terminal
        on_fetch(
            active_fetch_,
            std::move(failed));  //@@authority-target ids=prv.T0216
    };

    try
    {
        if (fetch_for_test_)
        {
            fetch_for_test_([on_fetch](catl::vl::VlResult result) mutable {
                on_fetch({}, std::move(result));
            });
            admission_rollback.terminal_producer_committed = true;
        }
        else
        {
            catl::vl::VlClient client(io_, vl_host_, vl_port_);
            auto operation = client.prepare_fetch(strand_);
            //@@authority transition=prv.T0208 status=provisional
            // bind=expression_statement@bf6644464277f70a
            // scope=statement owner=unresolved owner_hypothesis=vl_cache
            // commit=vl_cache_strand touches=prv.S0067
            // label=install_exact_validator_list_fetch_cancellation_capability
            active_fetch_ = operation;  //@@authority-target ids=prv.T0208
            if (fail_fetch_operation_starts_for_test_ != 0)
            {
                --fail_fetch_operation_starts_for_test_;
                throw std::runtime_error("VL fetch operation start failpoint");
            }
            operation->start(on_fetch);
            admission_rollback.terminal_producer_committed = true;
        }
    }
    catch (std::exception const& e)
    {
        publish_start_refusal(e.what());
    }
    catch (...)
    {
        publish_start_refusal(nullptr);
    }
}

//@@authority transition=prv.T0075 status=provisional scope=function
// bind=xprv::retire_fetch_on_strand@58f2ee55d206538a
// owner=unresolved owner_hypothesis=vl_cache
// commit=vl_cache_strand touches=prv.S0025,prv.S0067,prv.S0068
// boundary_ref=vl_fetch.operation_terminal
// receipt=vl_fetch.operation_terminal receipt_role=consume
// writ_roles=exact.validate_or_consume
// label=consume_exact_validator_list_fetch_terminal_and_retire_owner_homes
bool
VlCache::retire_fetch_on_strand(
    catl::vl::VlFetchOperationPtr const& operation) noexcept
{
    assert(strand_.running_in_this_thread());
    if (operation ? active_fetch_ != operation
                  : static_cast<bool>(active_fetch_))
    {
        return false;
    }
    active_fetch_.reset();
    active_fetch_cancel_requested_ = false;
    fetch_in_progress_ = false;
    //@@authority transition=prv.T0332 status=provisional scope=statement
    // bind=expression_statement@c0a327372583afbf
    // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
    // touches=prv.S0141,prv.S0067
    // writ_roles=exact.mint_generation
    // label=mint_next_validator_list_fetch_generation
    ++fetch_generation_;  //@@authority-target ids=prv.T0332
    return true;
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
    co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<void> {
            if (self->stop_entry_observer_for_test_)
                self->stop_entry_observer_for_test_();

            self->begin_stop_on_strand();
            if (self->phase_ == Phase::stopped)
                co_return;

            // Keep the state check and waiter installation on the owner
            // strand. A post(strand_, use_awaitable) only fences that
            // executor; it resumes the caller on the caller's executor. With
            // multiple runners the refresh terminal could therefore publish
            // stopped and cancel stop_progress_ between this check and the
            // async_wait, losing the one-shot wakeup forever.
            boost::system::error_code ec;
            co_await self->stop_progress_.async_wait(
                asio::redirect_error(asio::use_awaitable, ec));

            auto const cancel_state =
                co_await asio::this_coro::cancellation_state;
            if (cancel_state.cancelled() != asio::cancellation_type::none)
            {
                throw boost::system::system_error(
                    asio::error::operation_aborted);
            }
        },
        asio::use_awaitable);
}

//@@authority transition=prv.T0071 status=provisional scope=function
// bind=xprv::begin_stop_on_strand@47e9653c14d73494
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0023,prv.S0024,prv.S0025,prv.S0067,prv.S0068
// label=seal_validator_list_refresh_admission_and_request_child_cancel
void
VlCache::begin_stop_on_strand()
{
    assert(strand_.running_in_this_thread());
    if (phase_ == Phase::stopped)
        return;
    phase_ = Phase::stopping;
    try
    {
        PLOGI(
            log_,
            "VL cache stop entered: refresh_handlers=",
            refresh_wait_outstanding_,
            " fetch_in_progress=",
            fetch_in_progress_,
            " cancellation_disposition=",
            active_fetch_ ? static_cast<unsigned>(
                                active_fetch_->cancellation_disposition())
                          : 0u);
    }
    catch (...)
    {
    }
    if (fetch_in_progress_ && !active_fetch_cancel_requested_)
    {
        bool requested = false;
        if (active_fetch_)
        {
            //@@authority transition=prv.T0209 status=provisional
            // bind=expression_statement@4d2bd899bf060dde
            // scope=statement owner=unresolved owner_hypothesis=vl_cache
            // commit=vl_cache_strand touches=prv.S0067
            // label=request_active_validator_list_fetch_cancellation
            requested =
                active_fetch_
                    ->request_cancel();  //@@authority-target ids=prv.T0209
        }
        else if (cancel_fetch_for_test_)
        {
            try
            {
                requested = cancel_fetch_for_test_();
            }
            catch (...)
            {
            }
        }
        //@@authority transition=prv.T0214 status=provisional scope=statement
        // bind=expression_statement@36c9154a7737ee7d
        // owner=unresolved owner_hypothesis=vl_cache
        // commit=vl_cache_strand touches=prv.S0068
        // label=retain_owner_accepted_fetch_cancellation_request
        active_fetch_cancel_requested_ =
            requested;  //@@authority-target ids=prv.T0214
    }
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
// bind=xprv::try_finish_stop_on_strand@bf7a4ce5d9a98cd5
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0023,prv.S0024,prv.S0025,prv.S0026
// label=commit_validator_list_cache_stopped_when_children_empty
void
VlCache::try_finish_stop_on_strand() noexcept
{
    if (phase_ != Phase::stopping || refresh_wait_outstanding_ != 0 ||
        fetch_in_progress_)
        return;
    phase_ = Phase::stopped;
    try
    {
        PLOGI(log_, "VL cache drained");
    }
    catch (...)
    {
    }
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

//@@authority transition=prv.T0333 status=provisional scope=function
// bind=xprv::wake_waiters@d8aae799c039b7ae
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0140
// writ_roles=retained_signal.publish
// label=wake_every_registered_fetch_waiter
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
    auto self = shared_from_this();
    // clang-format off
    //@@authority transition=prv.T0324 status=provisional scope=statement bind=co_return_statement@8a7534e7f3dc3b15
    // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
    // execution_home=vl_cache.strand birth=co_spawn executor_expr=strand_
    // drives=co_get_on_strand continuation=birth_executor
    // label=establish_validator_list_get_owner_birth
    co_return co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<catl::vl::ValidatorList> {
            co_return co_await self->co_get_on_strand();
        },
        asio::use_awaitable); //@@authority-target ids=prv.T0324
    // clang-format on
}

asio::awaitable<catl::vl::ValidatorList>
VlCache::co_get_on_strand()
{
    assert(strand_.running_in_this_thread());
    if (public_entry_observer_for_test_)
        public_entry_observer_for_test_();

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
    //@@authority transition=prv.T0334 status=provisional scope=statement
    // bind=expression_statement@690a732601aeff18
    // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
    // touches=prv.S0140,prv.S0141
    // writ_roles=retained_signal.subscribe
    // label=register_fetch_wake_waiter_after_sampling_generation
    waiters_.push_back(signal);  //@@authority-target ids=prv.T0334

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
    } cleanup{this, strand_, weak_from_this(), signal};

    while (true)
    {
        signal->expires_at(asio::steady_timer::time_point::max());
        boost::system::error_code ec;
        co_await signal->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));

        //@@authority transition=prv.T0335 status=provisional scope=statement
        // bind=declaration@ebafec8af9998ea8
        // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
        // touches=prv.S0140
        // writ_roles=retained_signal.deadline_or_cancel
        // label=consume_fetch_wait_cancellation_and_abort
        auto const cancel_state = co_await asio::this_coro::cancellation_state; //@@authority-target ids=prv.T0335
        if (cancel_state.cancelled() != asio::cancellation_type::none)
            throw boost::system::system_error(asio::error::operation_aborted);
        if (public_resume_observer_for_test_)
            public_resume_observer_for_test_();

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
    // clang-format off
    //@@authority transition=prv.T0325 status=provisional scope=statement bind=co_return_statement@f7ea1f5282c1bfc5
    // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
    // execution_home=vl_cache.strand birth=co_spawn executor_expr=strand_
    // drives=co_refresh_on_strand continuation=birth_executor
    // label=establish_validator_list_refresh_owner_birth
    co_return co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<catl::vl::ValidatorList> {
            co_return co_await self->co_refresh_on_strand();
        },
        asio::use_awaitable); //@@authority-target ids=prv.T0325
    // clang-format on
}

asio::awaitable<catl::vl::ValidatorList>
VlCache::co_refresh_on_strand()
{
    assert(strand_.running_in_this_thread());
    if (public_entry_observer_for_test_)
        public_entry_observer_for_test_();

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
    } cleanup{this, strand_, weak_from_this(), signal};

    while (true)
    {
        signal->expires_at(asio::steady_timer::time_point::max());
        boost::system::error_code ec;
        co_await signal->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));

        auto const cancel_state = co_await asio::this_coro::cancellation_state;
        if (cancel_state.cancelled() != asio::cancellation_type::none)
            throw boost::system::system_error(asio::error::operation_aborted);
        if (public_resume_observer_for_test_)
            public_resume_observer_for_test_();

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
    auto self = shared_from_this();
    // clang-format off
    //@@authority transition=prv.T0326 status=provisional scope=statement bind=co_return_statement@77f6cfb99078e27f
    // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
    // execution_home=vl_cache.strand birth=co_spawn executor_expr=strand_
    // drives=co_is_loaded_on_strand continuation=birth_executor
    // label=establish_validator_list_status_owner_birth
    co_return co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<bool> {
            co_return co_await self->co_is_loaded_on_strand();
        },
        asio::use_awaitable); //@@authority-target ids=prv.T0326
    // clang-format on
}

asio::awaitable<bool>
VlCache::co_is_loaded_on_strand()
{
    assert(strand_.running_in_this_thread());
    if (public_entry_observer_for_test_)
        public_entry_observer_for_test_();
    co_return vl_.has_value() && is_active(*vl_);
}

}  // namespace xprv
