#pragma once

// VlCache — strand-protected, auto-refreshing Validator List cache.
//
// Wraps VlClient with caching and periodic refresh. All reads go through
// the strand — no sync const accessors.
//
// Usage:
//   auto cache = VlCache::create(io, "vl.ripple.com", 443);
//   cache->start();
//   auto vl = co_await cache->co_get();  // blocks until first fetch

#include "xprv/manifest-utils.h"

#include <catl/core/logger.h>
#include <catl/vl-client/vl-client.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace xprv {

class VlCache : public std::enable_shared_from_this<VlCache>
{
    friend class VlCacheTestAccess;

public:
    static std::shared_ptr<VlCache>
    create(
        boost::asio::io_context& io,
        std::string vl_host,
        uint16_t vl_port = 443)
    {
        return std::shared_ptr<VlCache>(
            new VlCache(io, std::move(vl_host), vl_port));
    }

    /// Begin background fetch + periodic refresh.
    /// Must be called after create() — uses shared_from_this().
    void
    start();

    /// Request stop without waiting for an already-running fetch. Process-exit
    /// compatibility only; service shutdown must use co_stop().
    void
    stop();

    /// Seal refresh/retry admission and wait until an already-running fetch
    /// has delivered its terminal callback on this cache's strand.
    boost::asio::awaitable<void>
    co_stop();

    /// Awaitable: returns a copy of the cached VL.
    /// Blocks until the first fetch completes. The complete owner coroutine is
    /// born on this cache's strand.
    boost::asio::awaitable<catl::vl::ValidatorList>
    co_get();

    /// Awaitable: force a refresh now and wait for the next fetch completion.
    /// Returns the updated VL or throws if the fetch failed.
    boost::asio::awaitable<catl::vl::ValidatorList>
    co_refresh();

    /// Awaitable: true if VL has been fetched at least once.
    boost::asio::awaitable<bool>
    co_is_loaded();

    /// Refresh interval (default 15 minutes).
    void
    set_refresh_interval(std::chrono::seconds interval)
    {
        refresh_interval_ = interval;
    }

    /// Called on every successful VL fetch (initial + refresh).
    using RefreshCallback = std::function<void(catl::vl::ValidatorList const&)>;
    void
    set_on_refresh(RefreshCallback cb)
    {
        on_refresh_ = std::move(cb);
    }

    /// Optional verifier run on every fetched VL BEFORE it is trusted/stored.
    /// Returning false makes the fetch behave exactly like a failed fetch (the
    /// last-known-good VL is kept). This lets the caller enforce the pinned
    /// publisher key + signatures without VlCache depending on crypto/config.
    using VerifyCallback =
        std::function<bool(catl::vl::ValidatorList const&, std::string&)>;
    void
    set_verifier(VerifyCallback cb)
    {
        verify_ = std::move(cb);
    }

private:
    VlCache(boost::asio::io_context& io, std::string vl_host, uint16_t vl_port);

    void
    do_fetch();

    void
    schedule_refresh();

    void
    schedule_retry(std::chrono::seconds delay);

    void
    wake_waiters();

    boost::asio::awaitable<catl::vl::ValidatorList>
    co_get_on_strand();

    boost::asio::awaitable<catl::vl::ValidatorList>
    co_refresh_on_strand();

    boost::asio::awaitable<bool>
    co_is_loaded_on_strand();

    void
    begin_stop_on_strand();

    void
    try_finish_stop_on_strand() noexcept;

    bool
    retire_fetch_on_strand(
        catl::vl::VlFetchOperationPtr const& operation) noexcept;

    boost::asio::io_context& io_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::string vl_host_;
    uint16_t vl_port_;

    // Strand-owned state:
    std::optional<catl::vl::ValidatorList> vl_;
    boost::asio::steady_timer refresh_;  // periodic refresh
    //@@authority state=prv.S0026 status=provisional scope=field
    // bind=xprv::VlCache::stop_progress_@b7a23fd626ede29a
    // owner=unresolved owner_hypothesis=vl_cache observed_home=vl_cache
    // label=shared_validator_list_stopped_receipt_latch
    boost::asio::steady_timer stop_progress_;
    //@@authority state=prv.S0140 status=provisional scope=field
    // bind=xprv::VlCache::waiters_@ac8170ff62e4cdaa
    // owner=unresolved owner_hypothesis=vl_cache observed_home=vl_cache
    // label=registered_per_waiter_validator_list_fetch_wake_timers
    std::vector<std::shared_ptr<boost::asio::steady_timer>> waiters_;
    std::chrono::seconds refresh_interval_{900};  // 15 minutes
    std::chrono::seconds initial_retry_{2};       // first retry delay
    std::chrono::seconds initial_retry_cap_{
        60};  // max retry delay before first success
    enum class Phase : std::uint8_t { running, stopping, stopped };
    //@@authority state=prv.S0023 status=provisional scope=field
    // bind=xprv::VlCache::phase_@bf94d68e5d8364ff
    // owner=unresolved owner_hypothesis=vl_cache observed_home=vl_cache
    // label=validator_list_cache_lifecycle
    Phase phase_ = Phase::running;
    //@@authority state=prv.S0024 status=provisional scope=field
    // bind=xprv::VlCache::refresh_wait_outstanding_@95b5b1bc66e67146
    // owner=unresolved owner_hypothesis=vl_cache observed_home=vl_cache
    // label=armed_refresh_or_retry_handler_obligation
    std::size_t refresh_wait_outstanding_ = 0;
    //@@authority state=prv.S0025 status=provisional scope=field
    // bind=xprv::VlCache::fetch_in_progress_@2e2b24112b573817
    // owner=unresolved owner_hypothesis=vl_cache observed_home=vl_cache
    // label=in_flight_validator_list_fetch_obligation
    bool fetch_in_progress_ = false;
    //@@authority state=prv.S0067 status=provisional scope=field
    // bind=xprv::VlCache::active_fetch_@4f2904e878920aea
    // owner=unresolved owner_hypothesis=vl_cache observed_home=vl_cache
    // label=exact_active_validator_list_fetch_operation_capability
    catl::vl::VlFetchOperationPtr active_fetch_;
    //@@authority state=prv.S0068 status=provisional scope=field
    // bind=xprv::VlCache::active_fetch_cancel_requested_@30b746823bdea8a5
    // owner=unresolved owner_hypothesis=vl_cache observed_home=vl_cache
    // label=accepted_active_fetch_cancellation_request_latch
    bool active_fetch_cancel_requested_ = false;
    //@@authority state=prv.S0141 status=provisional scope=field
    // bind=xprv::VlCache::fetch_generation_@d9e48c98dcaf4526
    // owner=unresolved owner_hypothesis=vl_cache observed_home=vl_cache
    // label=monotonic_validator_list_fetch_generation
    uint64_t fetch_generation_ = 0;
    bool last_fetch_ok_ = false;
    std::string last_fetch_error_;
    ValidatorManifestHistory validator_manifest_history_;
    RefreshCallback on_refresh_;
    VerifyCallback verify_;

    /// Deterministic test seam. Production constructs VlClient directly.
    std::function<void(catl::vl::VlCallback)> fetch_for_test_;
    std::function<bool()> cancel_fetch_for_test_;
    std::function<void(catl::vl::VlFetchOperationPtr const&)>
        fetch_terminal_observer_for_test_;
    std::function<void()> initiation_failure_diagnostic_for_test_;
    std::function<void()> stop_entry_observer_for_test_;
    std::function<void()> public_entry_observer_for_test_;
    std::function<void()> public_resume_observer_for_test_;
    std::size_t fail_fetch_operation_starts_for_test_ = 0;

    static LogPartition log_;
};

}  // namespace xprv
