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

void
consume_fetch_wait_outcome(writ::wait_outcome const outcome)
{
    //@@writ attachment=catl.ann.A0522
    switch (outcome)
    {
        case writ::wait_outcome::signaled:
            return;
        case writ::wait_outcome::deadline_elapsed:
            throw boost::system::system_error(asio::error::timed_out);
        case writ::wait_outcome::canceled:
            throw boost::system::system_error(asio::error::operation_aborted);
    }
}

}  // namespace

LogPartition VlCache::log_("vl-cache", LogLevel::INHERIT);

VlCache::VlCache(asio::io_context& io, std::string vl_host, uint16_t vl_port)
    : io_(io)
    , strand_(asio::make_strand(io))
    , home_(strand_)
    , vl_host_(std::move(vl_host))
    , vl_port_(vl_port)
    , refresh_(strand_)
    , stop_roster_(home_)
{
}

//@@authority transition=prv.T0068 status=provisional scope=function
// bind=xprv::start@ff4c4534e3a56929
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0023,prv.S0141 label=start_validator_list_fetch_lifecycle
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
// touches=prv.S0023,prv.S0141,prv.S0067,prv.S0068
// writ_roles=roster.admit
// label=admit_validator_list_fetch_and_install_terminal_callback
void
VlCache::do_fetch()
{
    WRIT_ASSERT_HOME(home_);
    if (phase_ != Phase::running || fetch_obligation_)
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

    // A signal generation belongs to one admitted fetch. Mint it before the
    // admission becomes visible so a synchronous terminal is retained even
    // when it precedes subscriber registration. Terminal publication stays
    // allocation-free; the next fetch admission replaces the completed
    // generation.
    //@@writ attachment=catl.ann.A0520
    fetch_wait_signal_ = std::make_shared<writ::retained_signal>(home_);
    fetch_obligation_ = stop_roster_.admit(StopObligation::fetch);
    if (!fetch_obligation_)
    {
        // Refusal is a disposition, not a vanishing: the generation minted
        // above must not strand its waiters, so the refusal publishes a
        // truthful terminal through it before returning.
        last_fetch_ok_ = false;
        last_fetch_error_ = "VL fetch admission refused";
        publish_fetch_wait_change();
        return;
    }

    struct FetchAdmissionRollback
    {
        VlCache& owner;
        bool terminal_producer_committed = false;

        ~FetchAdmissionRollback() noexcept
        {
            if (terminal_producer_committed || !owner.fetch_obligation_)
                return;
            auto const operation = owner.active_fetch_;
            owner.retire_fetch_on_strand(operation);
            owner.try_finish_stop_on_strand();
        }
    } admission_rollback{*this};

    auto on_fetch = [self](
                        std::optional<catl::vl::VlFetchTerminalReceipt> receipt,
                        catl::vl::VlFetchOperationPtr operation,
                        catl::vl::VlResult result) {
        auto consume = [self,
                        receipt = std::move(receipt),
                        operation = std::move(operation),
                        result = std::move(result)]() mutable {
            WRIT_ASSERT_HOME(self->home_);
            try
            {
                if (self->fetch_terminal_observer_for_test_)
                    self->fetch_terminal_observer_for_test_(operation);
            }
            catch (...)
            {
            }

            bool retired = false;
            if (receipt)
            {
                switch (receipt->disposition())
                {
                    case catl::vl::VlFetchTerminalFact::disposition::
                        physical_completion:
                        break;
                }

                // The receipt stays alive through the exact owner-home
                // validation and roster retirement it authorizes.
                // clang-format off
                //@@authority transition=prv.T0075 status=provisional
                // scope=statement
                // bind=expression_statement@c3019699f2deab2c
                // owner=unresolved owner_hypothesis=vl_cache
                // commit=vl_cache_strand touches=prv.S0141,prv.S0067,prv.S0068
                // boundary_ref=vl_fetch.physical_completion
                // receipt=vl_fetch.physical_completion receipt_role=consume
                // writ_roles=receipt.consumer,receipt.exact_acceptance,roster.retire
                // label=consume_exact_physical_fetch_receipt_and_retire_owner_homes
                //@@writ attachment=catl.ann.A0526
                retired = self->retire_fetch_on_strand(
                    receipt->fact().operation()); //@@authority-target ids=prv.T0075
                // clang-format on
            }
            else
            {
                retired = self->retire_fetch_on_strand(operation);
            }

            if (!retired)
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
                    self->publish_fetch_wait_change();
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
                    self->publish_fetch_wait_change();
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

                    self->publish_fetch_wait_change();
                    self->schedule_refresh();
                }
                else
                {
                    self->last_fetch_ok_ = false;
                    self->last_fetch_error_ = result.error;
                    PLOGE(VlCache::log_, "VL fetch failed: ", result.error);
                    self->publish_fetch_wait_change();

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
                self->publish_fetch_wait_change();
            }
            catch (...)
            {
                self->last_fetch_ok_ = false;
                self->last_fetch_error_ = "VL result observer failed";
                PLOGE(VlCache::log_, self->last_fetch_error_);
                self->publish_fetch_wait_change();
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
        // bind=expression_statement@c59dbeee8087e5d3
        // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
        // touches=prv.S0141,prv.S0067,prv.S0068
        // writ_roles=roster.retire
        // label=settle_synchronous_fetch_start_refusal_locally
        //@@mutation-site vl-fetch-start-refusal-remains-local
        on_fetch(
            std::nullopt,
            active_fetch_,
            std::move(failed));  //@@authority-target ids=prv.T0216
        //@@mutation-end vl-fetch-start-refusal-remains-local
    };

    try
    {
        if (fetch_for_test_)
        {
            fetch_for_test_([on_fetch](catl::vl::VlResult result) mutable {
                //@@mutation-site vl-fetch-test-injection-remains-local
                on_fetch(std::nullopt, {}, std::move(result));
                //@@mutation-end vl-fetch-test-injection-remains-local
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
            // writ_roles=exact.install_or_issue
            // label=install_exact_validator_list_fetch_cancellation_capability
            active_fetch_ = operation;  //@@authority-target ids=prv.T0208
            if (fail_fetch_operation_starts_for_test_ != 0)
            {
                --fail_fetch_operation_starts_for_test_;
                throw std::runtime_error("VL fetch operation start failpoint");
            }
            operation->start([on_fetch](
                                 catl::vl::VlFetchTerminalReceipt receipt,
                                 catl::vl::VlResult result) mutable {
                auto operation = receipt.fact().operation();
                on_fetch(
                    std::move(receipt),
                    std::move(operation),
                    std::move(result));
            });
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

bool
VlCache::retire_fetch_on_strand(
    catl::vl::VlFetchOperationPtr const& operation) noexcept
{
    WRIT_ASSERT_HOME(home_);
    //@@mutation-site vl-fetch-retire-exact-operation
    if (operation ? active_fetch_ != operation
                  : static_cast<bool>(active_fetch_))
    {
        return false;
    }
    //@@mutation-end vl-fetch-retire-exact-operation
    active_fetch_.reset();
    active_fetch_cancel_requested_ = false;
    //@@mutation-site vl-fetch-retire-roster-member
    if (fetch_obligation_)
        (void)stop_roster_.retire(std::move(*fetch_obligation_));
    fetch_obligation_.reset();
    //@@mutation-end vl-fetch-retire-roster-member
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
// touches=prv.S0023,prv.S0141
// label=join_validator_list_timer_and_fetch_obligations
asio::awaitable<void>
VlCache::co_stop()
{
    auto self = shared_from_this();
    co_await asio::co_spawn(
        strand_,
        [self]() -> writ::home_awaitable<void> {
            WRIT_ASSERT_HOME(self->home_);
            if (self->stop_entry_observer_for_test_)
                self->stop_entry_observer_for_test_();

            self->begin_stop_on_strand();

            // The drained wake is RETAINED: a joiner arriving after the
            // commit observes the same fact, so there is no already-stopped
            // special case and no lost-wake window between a state check
            // and a waiter installation.
            auto const outcome =
                co_await self->stop_roster_.drained_signal()->wait_until(
                    asio::steady_timer::time_point::max());
            if (outcome == writ::wait_outcome::canceled)
            {
                throw boost::system::system_error(
                    asio::error::operation_aborted);
            }

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
// touches=prv.S0023,prv.S0141,prv.S0067,prv.S0068
// writ_roles=roster.seal
// label=seal_validator_list_refresh_admission_and_request_child_cancel
void
VlCache::begin_stop_on_strand()
{
    WRIT_ASSERT_HOME(home_);
    if (phase_ == Phase::stopped)
        return;
    phase_ = Phase::stopping;
    try
    {
        PLOGI(
            log_,
            "VL cache stop entered: refresh_handlers=",
            stop_roster_.live_with(StopObligation::refresh_timer),
            " fetch_in_progress=",
            fetch_obligation_.has_value(),
            " cancellation_disposition=",
            active_fetch_ ? static_cast<unsigned>(
                                active_fetch_->cancellation_disposition())
                          : 0u);
    }
    catch (...)
    {
    }
    if (fetch_obligation_ && !active_fetch_cancel_requested_)
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
    publish_fetch_wait_change();
    stop_roster_.seal();
    try_finish_stop_on_strand();
}

//@@authority transition=prv.T0072 status=provisional scope=function
// bind=xprv::try_finish_stop_on_strand@bf7a4ce5d9a98cd5
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0023,prv.S0141
// writ_roles=roster.drain_publication
// label=commit_validator_list_cache_stopped_when_children_empty
void
VlCache::try_finish_stop_on_strand() noexcept
{
    // The drain is a conjunction the roster owns: sealed AND zero live AND
    // zero leaked. A dropped obligation wedges drained() false forever —
    // visibly (WRIT-ROSTER-LEAK on stderr, sticky counter) instead of the
    // former silent hang. The stopped commit publishes through the
    // roster's retained drained wake; no cancellation-edge latch remains.
    if (phase_ != Phase::stopping || !stop_roster_.drained())
        return;
    phase_ = Phase::stopped;
    try
    {
        PLOGI(log_, "VL cache drained");
    }
    catch (...)
    {
    }
}

//@@authority transition=prv.T0073 status=provisional scope=function
// bind=xprv::schedule_refresh@4728af8d30731abe
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0141 writ_roles=roster.admit,roster.retire
// label=arm_refresh_timer_obligation
void
VlCache::schedule_refresh()
{
    if (phase_ != Phase::running)
        return;
    // EVERY fallible step is placed relative to admission so that no throw
    // can destroy an armed member (a destroyed member commits the leak
    // wedge, permanently wedging stop): the self pointer and the shared
    // holder ALLOCATE BEFORE admission; the member moves into the holder
    // with a noexcept move; expires_after and async_wait sit inside ONE
    // catch that retires the exact holder. The handler and the catch are
    // mutually exclusive consumers; a defused token retires as a typed
    // refused no-op.
    auto self = shared_from_this();
    auto holder = std::make_shared<std::optional<StopRoster::member>>();
    auto admitted = stop_roster_.admit(StopObligation::refresh_timer);
    if (!admitted)
        return;  // sealed or identity-exhausted: nothing may be armed
    holder->emplace(std::move(*admitted));
    try
    {
        refresh_.expires_after(refresh_interval_);
        if (fail_timer_arms_for_test_ != 0)
        {
            --fail_timer_arms_for_test_;
            throw std::runtime_error("VL timer arm failpoint");
        }
        refresh_.async_wait([self, holder](boost::system::error_code ec) {
            (void)self->stop_roster_.retire(std::move(**holder));
            if (!ec && self->phase_ == Phase::running)
                self->do_fetch();
            self->try_finish_stop_on_strand();
        });
    }
    catch (...)
    {
        (void)stop_roster_.retire(std::move(**holder));
        throw;
    }
}

//@@authority transition=prv.T0074 status=provisional scope=function
// bind=xprv::schedule_retry@d94ed85885bf7cbd
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0141 writ_roles=roster.admit,roster.retire
// label=arm_retry_timer_obligation
void
VlCache::schedule_retry(std::chrono::seconds delay)
{
    if (phase_ != Phase::running)
        return;
    // Same fallible-step placement as schedule_refresh: allocate before
    // admission, one catch over the whole arming window.
    auto self = shared_from_this();
    auto holder = std::make_shared<std::optional<StopRoster::member>>();
    auto admitted = stop_roster_.admit(StopObligation::refresh_timer);
    if (!admitted)
        return;  // sealed or identity-exhausted: nothing may be armed
    holder->emplace(std::move(*admitted));
    try
    {
        refresh_.expires_after(delay);
        if (fail_timer_arms_for_test_ != 0)
        {
            --fail_timer_arms_for_test_;
            throw std::runtime_error("VL timer arm failpoint");
        }
        refresh_.async_wait([self, holder](boost::system::error_code ec) {
            (void)self->stop_roster_.retire(std::move(**holder));
            if (!ec && self->phase_ == Phase::running)
                self->do_fetch();
            self->try_finish_stop_on_strand();
        });
    }
    catch (...)
    {
        (void)stop_roster_.retire(std::move(**holder));
        throw;
    }
}

//@@authority transition=prv.T0333 status=provisional scope=function
// bind=xprv::publish_fetch_wait_change@aa946fda0e29e5c3
// owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
// touches=prv.S0140
// writ_roles=retained_signal.publish
// label=retain_fetch_terminal_or_stop_and_wake_current_generation
void
VlCache::publish_fetch_wait_change() noexcept
{
    WRIT_ASSERT_HOME(home_);
    if (fetch_wait_signal_)
    {
        //@@writ attachment=catl.ann.A0521
        fetch_wait_signal_->complete();
    }
}

asio::awaitable<catl::vl::ValidatorList>
VlCache::co_get()
{
    auto self = shared_from_this();
    // clang-format off
    //@@authority transition=prv.T0324 status=provisional scope=statement bind=co_return_statement@15aacd7542b9b9a0
    // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
    // execution_home=vl_cache.strand birth=co_spawn executor_expr=strand_
    // drives=co_get_on_strand continuation=birth_executor
    // label=establish_validator_list_get_owner_birth
    co_return co_await asio::co_spawn(
        strand_,
        [self]() -> writ::home_awaitable<catl::vl::ValidatorList> {
            co_return co_await self->co_get_on_strand();
        },
        asio::use_awaitable); //@@authority-target ids=prv.T0324
    // clang-format on
}

writ::home_awaitable<catl::vl::ValidatorList>
VlCache::co_get_on_strand()
{
    WRIT_ASSERT_HOME(home_);
    if (public_entry_observer_for_test_)
        public_entry_observer_for_test_();

    if (phase_ != Phase::running)
        throw boost::system::system_error(asio::error::operation_aborted);

    if (vl_ && is_active(*vl_))
    {
        co_return *vl_;
    }

    if (!fetch_obligation_)
        do_fetch();
    auto signal = fetch_wait_signal_;
    if (!signal)
        throw std::logic_error("VL fetch admitted without a wait generation");

    //@@authority transition=prv.T0334 status=provisional scope=statement
    // bind=declaration@0a6686f13eb84bda
    // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
    // touches=prv.S0140
    // writ_roles=retained_signal.subscribe
    // label=wait_for_retained_fetch_terminal_or_stop_generation
    // clang-format off
    //@@writ attachment=catl.ann.A0523
    auto const outcome = co_await signal->wait_until(
        asio::steady_timer::time_point::max()); //@@authority-target ids=prv.T0334
    // clang-format on
    consume_fetch_wait_outcome(outcome);
    if (public_resume_observer_for_test_)
        public_resume_observer_for_test_();

    if (phase_ != Phase::running)
        throw boost::system::system_error(asio::error::operation_aborted);

    if (!vl_ || !is_active(*vl_))
    {
        throw std::runtime_error(
            "VL fetch failed: " +
            (last_fetch_error_.empty() ? std::string("unknown error")
                                       : last_fetch_error_));
    }

    co_return *vl_;
}

asio::awaitable<catl::vl::ValidatorList>
VlCache::co_refresh()
{
    auto self = shared_from_this();
    // clang-format off
    //@@authority transition=prv.T0325 status=provisional scope=statement bind=co_return_statement@ba278a65c206ef16
    // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
    // execution_home=vl_cache.strand birth=co_spawn executor_expr=strand_
    // drives=co_refresh_on_strand continuation=birth_executor
    // label=establish_validator_list_refresh_owner_birth
    co_return co_await asio::co_spawn(
        strand_,
        [self]() -> writ::home_awaitable<catl::vl::ValidatorList> {
            co_return co_await self->co_refresh_on_strand();
        },
        asio::use_awaitable); //@@authority-target ids=prv.T0325
    // clang-format on
}

writ::home_awaitable<catl::vl::ValidatorList>
VlCache::co_refresh_on_strand()
{
    WRIT_ASSERT_HOME(home_);
    if (public_entry_observer_for_test_)
        public_entry_observer_for_test_();

    if (phase_ != Phase::running)
        throw boost::system::system_error(asio::error::operation_aborted);

    try
    {
        refresh_.cancel();
    }
    catch (...)
    {
    }
    if (!fetch_obligation_)
        do_fetch();
    auto signal = fetch_wait_signal_;
    if (!signal)
        throw std::logic_error("VL fetch admitted without a wait generation");

    //@@authority transition=prv.T0336 status=provisional scope=statement
    // bind=declaration@2d2229d14f66b9dc
    // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
    // touches=prv.S0140
    // writ_roles=retained_signal.subscribe
    // label=wait_for_retained_refresh_terminal_or_stop_generation
    // clang-format off
    //@@writ attachment=catl.ann.A0524
    auto const outcome = co_await signal->wait_until(
        asio::steady_timer::time_point::max()); //@@authority-target ids=prv.T0336
    // clang-format on
    consume_fetch_wait_outcome(outcome);
    if (public_resume_observer_for_test_)
        public_resume_observer_for_test_();

    if (phase_ != Phase::running)
        throw boost::system::system_error(asio::error::operation_aborted);

    if (!last_fetch_ok_ || !vl_ || !is_active(*vl_))
    {
        throw std::runtime_error(
            "VL refresh failed: " +
            (last_fetch_error_.empty() ? std::string("unknown error")
                                       : last_fetch_error_));
    }

    co_return *vl_;
}

asio::awaitable<bool>
VlCache::co_is_loaded()
{
    auto self = shared_from_this();
    // clang-format off
    //@@authority transition=prv.T0326 status=provisional scope=statement bind=co_return_statement@ee62d1deac2237a9
    // owner=unresolved owner_hypothesis=vl_cache commit=vl_cache_strand
    // execution_home=vl_cache.strand birth=co_spawn executor_expr=strand_
    // drives=co_is_loaded_on_strand continuation=birth_executor
    // label=establish_validator_list_status_owner_birth
    co_return co_await asio::co_spawn(
        strand_,
        [self]() -> writ::home_awaitable<bool> {
            co_return co_await self->co_is_loaded_on_strand();
        },
        asio::use_awaitable); //@@authority-target ids=prv.T0326
    // clang-format on
}

writ::home_awaitable<bool>
VlCache::co_is_loaded_on_strand()
{
    WRIT_ASSERT_HOME(home_);
    if (public_entry_observer_for_test_)
        public_entry_observer_for_test_();
    co_return vl_.has_value() && is_active(*vl_);
}

}  // namespace xprv
