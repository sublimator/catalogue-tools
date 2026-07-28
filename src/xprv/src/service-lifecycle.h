#pragma once

#include "xprv/proof-engine.h"

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/system/error_code.hpp>

#include <chrono>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace xprv::detail {

struct ServiceTimerControl
{
    using Handler = std::function<void(boost::system::error_code)>;

    std::function<void(std::chrono::steady_clock::duration)> expires_after;
    std::function<void(boost::asio::any_io_executor, Handler)> async_wait;
    std::function<void()> cancel;

    explicit
    operator bool() const noexcept
    {
        return expires_after && async_wait && cancel;
    }
};

/// Bind a production steady_timer's full completion handler to the supplied
/// lifecycle owner executor. This is the sole production adapter for the
/// timer-receipt boundary.
ServiceTimerControl
make_service_timer_control(std::shared_ptr<boost::asio::steady_timer> timer);

enum class ServiceTerminalOutcome {
    graceful,
    failed,
    forced,
};

struct ServiceTerminalResult
{
    ServiceTerminalOutcome outcome = ServiceTerminalOutcome::forced;
    std::exception_ptr failure;
};

class EngineDrainOperation
{
public:
    static EngineDrainOperation
    for_engine(std::shared_ptr<ProofEngine> engine);

    boost::asio::awaitable<ProofEngine::DrainReceipt>
    co_stop(RequestDrainReceipt const& request_drain) const
    {
        return stop_(request_drain);
    }

    bool
    accepts(ProofEngine::DrainReceipt const& receipt) const noexcept
    {
        return engine_ && engine_->accepts_drain_receipt(receipt);
    }

    explicit
    operator bool() const noexcept
    {
        return engine_ && stop_;
    }

private:
#ifdef XPRV_TESTING
    friend class ::xprv::ProofEngineTestAccess;
#endif

    EngineDrainOperation(
        std::shared_ptr<ProofEngine const> engine,
        std::function<boost::asio::awaitable<ProofEngine::DrainReceipt>(
            RequestDrainReceipt const&)> stop)
        : engine_(std::move(engine)), stop_(std::move(stop))
    {
    }

    std::shared_ptr<ProofEngine const> engine_;
    std::function<boost::asio::awaitable<ProofEngine::DrainReceipt>(
        RequestDrainReceipt const&)>
        stop_;
};

struct ServiceLifecycleOperations
{
    std::function<boost::asio::awaitable<void>()> report_status;
    std::function<boost::asio::awaitable<RequestDrainReceipt>()> stop_http;
    std::vector<EngineDrainOperation> stop_engines;
    std::function<void(std::exception_ptr const&)> report_diagnostic;
    std::function<void(ServiceTerminalResult const&)> terminal_diagnostic;
    std::function<void()> stop_io;

    // Narrow failpoints for deterministic conservation controls. Production
    // leaves these empty.
    std::function<void()> before_engine_admission_seal_for_test;
    std::function<void(std::size_t)> before_engine_launch_for_test;
};

/// One owner-strand aggregate for periodic status work and service shutdown.
///
/// Component quiescence is received from HttpServer and each ProofEngine. The
/// aggregate owns only their admission/result rows and the final service
/// outcome; it does not infer child drain from executor idleness.
class ServiceLifecycle final
    : public std::enable_shared_from_this<ServiceLifecycle>
{
public:
    static std::shared_ptr<ServiceLifecycle>
    create(
        boost::asio::io_context& io,
        ServiceTimerControl status_timer,
        ServiceTimerControl deadline_timer,
        ServiceLifecycleOperations operations,
        std::chrono::steady_clock::duration status_interval,
        std::chrono::steady_clock::duration shutdown_timeout);

    void
    start();

    void
    begin_shutdown() noexcept;

    /// Return the immutable terminal projection without requiring executor
    /// progress. This remains usable after the sole finalizer calls io.stop().
    std::optional<ServiceTerminalResult>
    terminal_result() const;

private:
    enum class Phase {
        created,
        running,
        draining,
        graceful,
        failed,
        forced,
    };

    struct EngineDrainRow
    {
        enum class Phase {
            reserved,
            running,
            succeeded,
            failed,
            launch_failed,
        };

        Phase phase = Phase::reserved;
        std::exception_ptr failure;
    };

    enum class EngineAdmission {
        unopened,
        sealed,
        refused,
    };

    ServiceLifecycle(
        boost::asio::io_context& io,
        ServiceTimerControl status_timer,
        ServiceTimerControl deadline_timer,
        ServiceLifecycleOperations operations,
        std::chrono::steady_clock::duration status_interval,
        std::chrono::steady_clock::duration shutdown_timeout);

    void
    start_on_control();

    void
    arm_status_timer_on_control();

    void
    finish_status_timer_on_control(boost::system::error_code error);

    void
    launch_status_report_on_control();

    void
    finish_status_report_on_control(std::exception_ptr failure);

    void
    begin_shutdown_on_control();

    void
    arm_deadline_on_control();

    void
    finish_deadline_on_control(boost::system::error_code error);

    void
    launch_http_stop_on_control();

    void
    finish_http_stop_on_control(
        std::optional<RequestDrainReceipt> receipt,
        std::exception_ptr failure);

    void
    launch_engine_stops_on_control();

    void
    finish_engine_stop_on_control(
        std::size_t index,
        std::optional<ProofEngine::DrainReceipt> receipt,
        std::exception_ptr failure);

    void
    record_failure_on_control(std::exception_ptr failure);

    void
    try_finish_shutdown_on_control();

    bool
    non_deadline_children_complete_on_control() const;

    void
    commit_terminal_on_control(ServiceTerminalOutcome outcome);

    boost::asio::strand<boost::asio::io_context::executor_type> control_;
    ServiceTimerControl status_timer_;
    ServiceTimerControl deadline_timer_;
    ServiceLifecycleOperations operations_;
    std::chrono::steady_clock::duration status_interval_;
    std::chrono::steady_clock::duration shutdown_timeout_;

    //@@authority state=prv.S0050 status=provisional scope=field
    //bind=xprv::detail::ServiceLifecycle::phase_@e8a3623c7145351a
    // owner=unresolved owner_hypothesis=service_lifecycle
    // observed_home=service_lifecycle label=service_root_lifecycle
    Phase phase_ = Phase::created;

    //@@authority state=prv.S0051 status=provisional scope=field
    //bind=xprv::detail::ServiceLifecycle::status_timer_outstanding_@d9cc1410ced11f02
    // owner=unresolved owner_hypothesis=service_lifecycle
    // observed_home=service_lifecycle
    // label=status_timer_and_report_obligations
    bool status_timer_outstanding_ = false;
    bool status_report_outstanding_ = false;
    bool status_reporting_disabled_ = false;

    //@@authority state=prv.S0052 status=provisional scope=field
    //bind=xprv::detail::ServiceLifecycle::http_stop_admitted_@498addcb8671ce17
    // owner=unresolved owner_hypothesis=service_lifecycle
    // observed_home=service_lifecycle label=http_drain_obligation_and_receipt
    bool http_stop_admitted_ = false;
    bool http_stop_completed_ = false;
    std::shared_ptr<RequestDrainReceipt const> request_drain_;

    //@@authority state=prv.S0053 status=provisional scope=field
    //bind=xprv::detail::ServiceLifecycle::engine_admission_@a24c8b15e19405a9
    // owner=unresolved owner_hypothesis=service_lifecycle
    // observed_home=service_lifecycle
    // label=sealed_exact_engine_drain_census_and_result_rows
    EngineAdmission engine_admission_ = EngineAdmission::unopened;
    std::vector<EngineDrainRow> engine_drains_;

    //@@authority state=prv.S0054 status=provisional scope=field
    //bind=xprv::detail::ServiceLifecycle::deadline_outstanding_@096d7fc191c97324
    // owner=unresolved owner_hypothesis=service_lifecycle
    // observed_home=service_lifecycle label=shutdown_deadline_obligation
    bool deadline_outstanding_ = false;
    bool deadline_cancel_requested_ = false;
    bool deadline_fired_ = false;

    //@@authority state=prv.S0055 status=provisional scope=field
    //bind=xprv::detail::ServiceLifecycle::terminal_@04d18aa9ccef0727
    // owner=unresolved owner_hypothesis=service_lifecycle
    // observed_home=service_lifecycle label=retained_service_terminal_outcome
    std::optional<ServiceTerminalResult> terminal_;
    std::exception_ptr first_failure_;
    bool stop_io_called_ = false;

    // Immutable observation projection of the owner-strand terminal commit.
    // It grants no mutation authority and deliberately remains readable after
    // io.stop() prevents further dispatch.
    mutable std::mutex terminal_mutex_;
    std::optional<ServiceTerminalResult> terminal_snapshot_;
};

}  // namespace xprv::detail
