#include "service-lifecycle.h"

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/post.hpp>

#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <utility>

namespace xprv::detail {

namespace asio = boost::asio;

namespace {

struct HttpStopCompletion
{
    std::optional<RequestDrainReceipt> receipt;
    std::exception_ptr failure;
};

struct EngineStopCompletion
{
    std::optional<ProofEngine::DrainReceipt> receipt;
    std::exception_ptr failure;
};

std::exception_ptr
timer_failure(boost::system::error_code error)
{
    return std::make_exception_ptr(boost::system::system_error(error));
}

}  // namespace

EngineDrainOperation
EngineDrainOperation::for_engine(std::shared_ptr<ProofEngine> engine)
{
    if (!engine)
        throw std::invalid_argument(
            "engine drain operation requires an engine");

    return EngineDrainOperation{
        engine,
        [engine](RequestDrainReceipt const& request_drain) {
            return engine->co_stop(request_drain);
        },
    };
}

ServiceTimerControl
make_service_timer_control(std::shared_ptr<asio::steady_timer> timer)
{
    if (!timer)
        throw std::invalid_argument("service timer cannot be null");

    return {
        .expires_after =
            [timer](auto duration) { timer->expires_after(duration); },
        .async_wait =
            [timer](auto owner, auto handler) {
                timer->async_wait(
                    asio::bind_executor(std::move(owner), std::move(handler)));
            },
        .cancel = [timer]() { (void)timer->cancel(); },
    };
}

std::shared_ptr<ServiceLifecycle>
ServiceLifecycle::create(
    asio::io_context& io,
    ServiceTimerControl status_timer,
    ServiceTimerControl deadline_timer,
    ServiceLifecycleOperations operations,
    std::chrono::steady_clock::duration status_interval,
    std::chrono::steady_clock::duration shutdown_timeout)
{
    if (!status_timer || !deadline_timer || !operations.report_status ||
        !operations.stop_http || !operations.stop_io ||
        std::ranges::any_of(operations.stop_engines, [](auto const& operation) {
            return !operation;
        }))
    {
        throw std::invalid_argument(
            "service lifecycle requires complete timer and operation controls");
    }

    return std::shared_ptr<ServiceLifecycle>(new ServiceLifecycle(
        io,
        std::move(status_timer),
        std::move(deadline_timer),
        std::move(operations),
        status_interval,
        shutdown_timeout));
}

ServiceLifecycle::ServiceLifecycle(
    asio::io_context& io,
    ServiceTimerControl status_timer,
    ServiceTimerControl deadline_timer,
    ServiceLifecycleOperations operations,
    std::chrono::steady_clock::duration status_interval,
    std::chrono::steady_clock::duration shutdown_timeout)
    : control_(asio::make_strand(io))
    , status_timer_(std::move(status_timer))
    , deadline_timer_(std::move(deadline_timer))
    , operations_(std::move(operations))
    , status_interval_(status_interval)
    , shutdown_timeout_(shutdown_timeout)
{
}

void
ServiceLifecycle::start()
{
    auto self = shared_from_this();
    asio::dispatch(control_, [self]() { self->start_on_control(); });
}

//@@authority transition=prv.T0150 status=provisional scope=function
//bind=xprv::detail::start_on_control@81010d2d1ad86567
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0050
// label=start_service_root_status_lifecycle
void
ServiceLifecycle::start_on_control()
{
    assert(control_.running_in_this_thread());
    if (phase_ != Phase::created)
        return;
    phase_ = Phase::running;
    arm_status_timer_on_control();
}

//@@authority transition=prv.T0151 status=provisional scope=function
//bind=xprv::detail::arm_status_timer_on_control@ea92a9a7b3f76da7
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0051
// label=admit_status_timer_handler_obligation
void
ServiceLifecycle::arm_status_timer_on_control()
{
    assert(control_.running_in_this_thread());
    if (phase_ != Phase::running || status_timer_outstanding_ ||
        status_report_outstanding_ || status_reporting_disabled_)
    {
        return;
    }

    status_timer_outstanding_ = true;
    try
    {
        status_timer_.expires_after(status_interval_);
        auto self = shared_from_this();
        status_timer_.async_wait(
            control_, [self](boost::system::error_code error) {
                if (!self->control_.running_in_this_thread())
                    std::terminate();
                // The adapter binds this raw handler to the owner strand.
                // Posting the immutable event is its final act, so no other
                // runner can consume the receipt before this callback returns.
                try
                {
                    asio::post(self->control_, [self, error]() {
                        self->finish_status_timer_on_control(error);
                    });
                }
                catch (...)
                {
                    std::terminate();
                }
            });
    }
    catch (...)
    {
        auto const failure = std::current_exception();
        status_timer_outstanding_ = false;
        status_reporting_disabled_ = true;
        record_failure_on_control(failure);
        if (operations_.report_diagnostic)
        {
            try
            {
                operations_.report_diagnostic(failure);
            }
            catch (...)
            {
            }
        }
    }
}

//@@authority transition=prv.T0152 status=provisional scope=function
//bind=xprv::detail::finish_status_timer_on_control@657407810e2e6476
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0051
// label=consume_status_timer_handler_receipt
void
ServiceLifecycle::finish_status_timer_on_control(
    boost::system::error_code error)
{
    assert(control_.running_in_this_thread());
    if (!status_timer_outstanding_)
        return;
    status_timer_outstanding_ = false;

    if (!error && phase_ == Phase::running)
    {
        launch_status_report_on_control();
        return;
    }

    auto const expected_shutdown_cancel =
        error == asio::error::operation_aborted && phase_ != Phase::running;
    if (error && !expected_shutdown_cancel)
    {
        auto const failure = timer_failure(error);
        record_failure_on_control(failure);
        if (phase_ == Phase::running)
            status_reporting_disabled_ = true;
        if (operations_.report_diagnostic)
        {
            try
            {
                operations_.report_diagnostic(failure);
            }
            catch (...)
            {
            }
        }
    }

    if (phase_ == Phase::running && !status_reporting_disabled_)
        arm_status_timer_on_control();
    else
        try_finish_shutdown_on_control();
}

//@@authority transition=prv.T0153 status=provisional scope=function
//bind=xprv::detail::launch_status_report_on_control@f8af8562c0a20e96
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0051
// label=admit_active_status_report_obligation
void
ServiceLifecycle::launch_status_report_on_control()
{
    assert(control_.running_in_this_thread());
    if (phase_ != Phase::running || status_report_outstanding_)
        return;

    status_report_outstanding_ = true;
    auto self = shared_from_this();
    try
    {
        asio::co_spawn(
            control_,
            [self]() -> asio::awaitable<std::exception_ptr> {
                try
                {
                    co_await self->operations_.report_status();
                    co_return std::exception_ptr{};
                }
                catch (...)
                {
                    co_return std::current_exception();
                }
            },
            asio::bind_executor(
                control_,
                [self](
                    std::exception_ptr outer_failure,
                    std::exception_ptr report_failure) {
                    self->finish_status_report_on_control(
                        outer_failure ? std::move(outer_failure)
                                      : std::move(report_failure));
                }));
    }
    catch (...)
    {
        finish_status_report_on_control(std::current_exception());
    }
}

//@@authority transition=prv.T0154 status=provisional scope=function
//bind=xprv::detail::finish_status_report_on_control@3fe91f68fc197eee
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0051
// label=consume_active_status_report_receipt
void
ServiceLifecycle::finish_status_report_on_control(std::exception_ptr failure)
{
    assert(control_.running_in_this_thread());
    status_report_outstanding_ = false;

    if (failure)
    {
        record_failure_on_control(failure);
        if (operations_.report_diagnostic)
        {
            try
            {
                operations_.report_diagnostic(failure);
            }
            catch (...)
            {
            }
        }
    }

    if (phase_ == Phase::running)
        arm_status_timer_on_control();
    else
        try_finish_shutdown_on_control();
}

void
ServiceLifecycle::begin_shutdown() noexcept
{
    try
    {
        auto self = shared_from_this();
        asio::dispatch(
            control_, [self]() { self->begin_shutdown_on_control(); });
    }
    catch (...)
    {
        // Once the external shutdown request has been accepted there is no
        // second authority which may safely perform this owner transition.
        // Refuse an indeterminate half-shutdown rather than unwinding out of
        // the one-shot signal handler.
        std::terminate();
    }
}

//@@authority transition=prv.T0155 status=provisional scope=function
//bind=xprv::detail::begin_shutdown_on_control@1ea7166a198135ea
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0050,prv.S0051
// label=seal_service_root_admission_and_status_rearm
void
ServiceLifecycle::begin_shutdown_on_control()
{
    assert(control_.running_in_this_thread());
    if (phase_ == Phase::draining || terminal_)
        return;
    if (phase_ != Phase::created && phase_ != Phase::running)
        return;

    phase_ = Phase::draining;
    if (status_timer_outstanding_)
    {
        try
        {
            status_timer_.cancel();
        }
        catch (...)
        {
            auto const failure = std::current_exception();
            record_failure_on_control(failure);
            if (operations_.report_diagnostic)
            {
                try
                {
                    operations_.report_diagnostic(failure);
                }
                catch (...)
                {
                }
            }
        }
    }

    arm_deadline_on_control();
    if (phase_ != Phase::draining)
        return;
    launch_http_stop_on_control();
    try_finish_shutdown_on_control();
}

//@@authority transition=prv.T0156 status=provisional scope=function
//bind=xprv::detail::arm_deadline_on_control@c1747038a774bb01
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0054
// label=admit_shutdown_deadline_handler_obligation
void
ServiceLifecycle::arm_deadline_on_control()
{
    assert(control_.running_in_this_thread());
    if (phase_ != Phase::draining || deadline_outstanding_)
        return;

    deadline_outstanding_ = true;
    try
    {
        deadline_timer_.expires_after(shutdown_timeout_);
        auto self = shared_from_this();
        deadline_timer_.async_wait(
            control_, [self](boost::system::error_code error) {
                if (!self->control_.running_in_this_thread())
                    std::terminate();
                // As with the status timer, handler return precedes receipt
                // consumption. cancel() and callback entry are not quiescence.
                try
                {
                    asio::post(self->control_, [self, error]() {
                        self->finish_deadline_on_control(error);
                    });
                }
                catch (...)
                {
                    std::terminate();
                }
            });
    }
    catch (...)
    {
        deadline_outstanding_ = false;
        deadline_fired_ = true;
        record_failure_on_control(std::current_exception());
        try_finish_shutdown_on_control();
    }
}

//@@authority transition=prv.T0157 status=provisional scope=function
//bind=xprv::detail::finish_deadline_on_control@a25e322fe30f19d2
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0054
// label=consume_shutdown_deadline_handler_receipt
void
ServiceLifecycle::finish_deadline_on_control(boost::system::error_code error)
{
    assert(control_.running_in_this_thread());
    if (!deadline_outstanding_)
        return;
    deadline_outstanding_ = false;

    if (!error)
    {
        deadline_fired_ = true;
        record_failure_on_control(std::make_exception_ptr(
            std::runtime_error("service shutdown deadline expired")));
    }
    else if (
        error != asio::error::operation_aborted || !deadline_cancel_requested_)
    {
        deadline_fired_ = true;
        record_failure_on_control(timer_failure(error));
    }
    try_finish_shutdown_on_control();
}

//@@authority transition=prv.T0158 status=provisional scope=function
//bind=xprv::detail::launch_http_stop_on_control@f241826ec4352327
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0052
// label=admit_http_request_drain_obligation
void
ServiceLifecycle::launch_http_stop_on_control()
{
    assert(control_.running_in_this_thread());
    if (phase_ != Phase::draining || http_stop_admitted_)
        return;

    http_stop_admitted_ = true;
    auto self = shared_from_this();
    try
    {
        asio::co_spawn(
            control_,
            [self]() -> asio::awaitable<HttpStopCompletion> {
                try
                {
                    auto receipt = co_await self->operations_.stop_http();
                    co_return HttpStopCompletion{
                        std::optional<RequestDrainReceipt>{std::move(receipt)},
                        {}};
                }
                catch (...)
                {
                    co_return HttpStopCompletion{
                        std::nullopt, std::current_exception()};
                }
            },
            asio::bind_executor(
                control_,
                [self](
                    std::exception_ptr outer_failure,
                    HttpStopCompletion completion) mutable {
                    self->finish_http_stop_on_control(
                        std::move(completion.receipt),
                        outer_failure ? std::move(outer_failure)
                                      : std::move(completion.failure));
                }));
    }
    catch (...)
    {
        finish_http_stop_on_control(std::nullopt, std::current_exception());
    }
}

//@@authority transition=prv.T0159 status=provisional scope=function
//bind=xprv::detail::finish_http_stop_on_control@949cac9f0b9d0663
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0052
// label=consume_http_request_drain_receipt
void
ServiceLifecycle::finish_http_stop_on_control(
    std::optional<RequestDrainReceipt> receipt,
    std::exception_ptr failure)
{
    assert(control_.running_in_this_thread());
    if (!http_stop_admitted_ || http_stop_completed_)
        return;

    http_stop_completed_ = true;
    if (failure || !receipt)
    {
        engine_admission_ = EngineAdmission::refused;
        record_failure_on_control(
            failure ? std::move(failure)
                    : std::make_exception_ptr(std::logic_error(
                          "HTTP drain completed without its exact receipt")));
    }
    else
    {
        try
        {
            auto receipt_home =
                std::make_shared<RequestDrainReceipt>(std::move(*receipt));
            std::vector<EngineDrainRow> prepared(
                operations_.stop_engines.size());
            if (operations_.before_engine_admission_seal_for_test)
                operations_.before_engine_admission_seal_for_test();

            // One no-throw owner-strand commit: the exact receipt home, every
            // reserved engine row, and the census seal become visible
            // together. sealed({}) is distinct from unopened.
            request_drain_ = std::move(receipt_home);
            engine_drains_.swap(prepared);
            engine_admission_ = EngineAdmission::sealed;
            launch_engine_stops_on_control();
        }
        catch (...)
        {
            engine_admission_ = EngineAdmission::refused;
            record_failure_on_control(std::current_exception());
        }
    }
    try_finish_shutdown_on_control();
}

//@@authority transition=prv.T0160 status=provisional scope=function
//bind=xprv::detail::launch_engine_stops_on_control@45a1bfeedceb2892
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0053
// label=reserve_all_engine_drain_rows_before_launch
void
ServiceLifecycle::launch_engine_stops_on_control()
{
    assert(control_.running_in_this_thread());
    if (phase_ != Phase::draining || !request_drain_ ||
        engine_admission_ != EngineAdmission::sealed)
    {
        return;
    }

    auto self = shared_from_this();
    auto receipt = request_drain_;
    for (std::size_t index = 0; index < operations_.stop_engines.size();
         ++index)
    {
        auto& row = engine_drains_[index];
        if (row.phase != EngineDrainRow::Phase::reserved)
            continue;
        row.phase = EngineDrainRow::Phase::running;
        try
        {
            if (operations_.before_engine_launch_for_test)
                operations_.before_engine_launch_for_test(index);
            asio::co_spawn(
                control_,
                [self,
                 receipt,
                 index]() -> asio::awaitable<EngineStopCompletion> {
                    try
                    {
                        auto engine_receipt =
                            co_await self->operations_.stop_engines[index]
                                .co_stop(*receipt);
                        co_return EngineStopCompletion{
                            .receipt = std::move(engine_receipt),
                            .failure = {},
                        };
                    }
                    catch (...)
                    {
                        co_return EngineStopCompletion{
                            .receipt = std::nullopt,
                            .failure = std::current_exception(),
                        };
                    }
                },
                asio::bind_executor(
                    control_,
                    [self, index](
                        std::exception_ptr outer_failure,
                        EngineStopCompletion completion) {
                        self->finish_engine_stop_on_control(
                            index,
                            std::move(completion.receipt),
                            outer_failure ? std::move(outer_failure)
                                          : std::move(completion.failure));
                    }));
        }
        catch (...)
        {
            row.phase = EngineDrainRow::Phase::launch_failed;
            row.failure = std::current_exception();
            record_failure_on_control(row.failure);
        }
    }
    try_finish_shutdown_on_control();
}

//@@authority transition=prv.T0161 status=provisional scope=function
// bind=xprv::detail::finish_engine_stop_on_control@861ed9ddc88b68d0
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0053
// label=consume_one_proof_engine_drain_outcome
void
ServiceLifecycle::finish_engine_stop_on_control(
    std::size_t index,
    std::optional<ProofEngine::DrainReceipt> receipt,
    std::exception_ptr failure)
{
    assert(control_.running_in_this_thread());
    if (index >= engine_drains_.size())
    {
        record_failure_on_control(std::make_exception_ptr(
            std::logic_error("engine drain receipt has no admitted row")));
        return;
    }

    auto& row = engine_drains_[index];
    if (row.phase != EngineDrainRow::Phase::running)
        return;
    if (!failure &&
        (!receipt || !operations_.stop_engines[index].accepts(*receipt)))
    {
        failure = std::make_exception_ptr(std::logic_error(
            "engine drain completed without its exact receipt"));
    }
    if (failure)
    {
        //@@authority transition=prv.T0180 status=provisional scope=statement
        //bind=expression_statement@271015ba0d3ef897
        // owner=unresolved owner_hypothesis=service_lifecycle
        // commit=service_root_strand touches=prv.S0053
        // label=record_one_proof_engine_drain_failure_outcome
        // clang-format off
        row.phase = EngineDrainRow::Phase::failed; //@@authority-target ids=prv.T0180
        // clang-format on
        row.failure = failure;
        record_failure_on_control(std::move(failure));
    }
    else
    {
        // Only this success arm consumes the receipt published by T0176.
        //@@authority transition=prv.T0179 status=provisional scope=statement
        //bind=expression_statement@a3eaf432a225c2c3
        // owner=unresolved owner_hypothesis=service_lifecycle
        // commit=service_root_strand touches=prv.S0053
        // boundary_ref=proof_engine.drain_receipt
        // receipt=proof_engine.drain_receipt
        // label=consume_one_exact_proof_engine_drain_receipt
        // clang-format off
        row.phase = EngineDrainRow::Phase::succeeded; //@@authority-target ids=prv.T0179
        // clang-format on
        row.failure = {};
    }
    try_finish_shutdown_on_control();
}

void
ServiceLifecycle::record_failure_on_control(std::exception_ptr failure)
{
    assert(control_.running_in_this_thread());
    if (failure && !first_failure_)
        first_failure_ = std::move(failure);
}

bool
ServiceLifecycle::non_deadline_children_complete_on_control() const
{
    assert(control_.running_in_this_thread());
    if (status_timer_outstanding_ || status_report_outstanding_ ||
        !http_stop_admitted_ || !http_stop_completed_)
    {
        return false;
    }

    if (engine_admission_ == EngineAdmission::unopened)
        return false;
    if (engine_admission_ == EngineAdmission::refused)
        return !request_drain_;
    if (!request_drain_ ||
        engine_drains_.size() != operations_.stop_engines.size())
    {
        return false;
    }
    return std::ranges::all_of(engine_drains_, [](EngineDrainRow const& row) {
        return row.phase == EngineDrainRow::Phase::succeeded ||
            row.phase == EngineDrainRow::Phase::failed ||
            row.phase == EngineDrainRow::Phase::launch_failed;
    });
}

void
ServiceLifecycle::try_finish_shutdown_on_control()
{
    assert(control_.running_in_this_thread());
    if (phase_ != Phase::draining)
        return;

    if (deadline_fired_)
    {
        commit_terminal_on_control(ServiceTerminalOutcome::forced);
        return;
    }
    if (!non_deadline_children_complete_on_control())
        return;

    if (deadline_outstanding_)
    {
        if (!deadline_cancel_requested_)
        {
            deadline_cancel_requested_ = true;
            try
            {
                deadline_timer_.cancel();
            }
            catch (...)
            {
                record_failure_on_control(std::current_exception());
                deadline_fired_ = true;
                commit_terminal_on_control(ServiceTerminalOutcome::forced);
            }
        }
        return;
    }

    commit_terminal_on_control(
        first_failure_ ? ServiceTerminalOutcome::failed
                       : ServiceTerminalOutcome::graceful);
}

//@@authority transition=prv.T0162 status=provisional scope=function
//bind=xprv::detail::commit_terminal_on_control@03ee3b82474034fd
// owner=unresolved owner_hypothesis=service_lifecycle
// commit=service_root_strand touches=prv.S0050,prv.S0055
// label=commit_one_retained_service_terminal_outcome
void
ServiceLifecycle::commit_terminal_on_control(ServiceTerminalOutcome outcome)
{
    assert(control_.running_in_this_thread());
    if (terminal_)
        return;

    switch (outcome)
    {
        case ServiceTerminalOutcome::graceful:
            phase_ = Phase::graceful;
            break;
        case ServiceTerminalOutcome::failed:
            phase_ = Phase::failed;
            break;
        case ServiceTerminalOutcome::forced:
            phase_ = Phase::forced;
            break;
    }
    terminal_ = ServiceTerminalResult{outcome, first_failure_};
    {
        std::lock_guard lock(terminal_mutex_);
        terminal_snapshot_ = terminal_;
    }

    if (operations_.terminal_diagnostic)
    {
        try
        {
            operations_.terminal_diagnostic(*terminal_);
        }
        catch (...)
        {
        }
    }

    if (stop_io_called_)
        return;
    stop_io_called_ = true;
    try
    {
        //@@authority transition=prv.T0163 status=provisional scope=statement
        //bind=expression_statement@31b8a1743dbe93d7
        // owner=unresolved owner_hypothesis=service_lifecycle
        // commit=service_root_strand touches=prv.S0055
        // label=invoke_io_stop_once_after_terminal_commit
        operations_.stop_io(); //@@authority-target ids=prv.T0163
    }
    catch (...)
    {
        std::terminate();
    }
}

std::optional<ServiceTerminalResult>
ServiceLifecycle::terminal_result() const
{
    std::lock_guard lock(terminal_mutex_);
    return terminal_snapshot_;
}

}  // namespace xprv::detail
