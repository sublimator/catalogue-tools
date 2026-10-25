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
#include <writ/home.hpp>
#include <writ/retained-signal.hpp>
#include <writ/roster.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>

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

    /// Optional check, run on a fetched list's publisher manifest when it is a
    /// revocation: true when it is a verified revocation of the pinned
    /// publisher key. Such a revocation is final for this cache (§1.3, D8):
    /// the list is dropped, co_get() and co_refresh() throw, every later fetch
    /// is refused, and the revoked callback runs once. Set before start().
    using RevocationCheck = std::function<bool(catl::vl::Manifest const&)>;
    using RevokedCallback = std::function<void(catl::vl::Manifest const&)>;
    // clang-format off
    //@@audit-ack unannotated_authority_candidate reason="pre-start wiring setter, like set_verifier and set_on_refresh; it installs callbacks before start() and decides nothing itself"
    // clang-format on
    void
    set_publisher_revocation(RevocationCheck check, RevokedCallback on_revoked)
    {
        revocation_check_ = std::move(check);
        on_publisher_revoked_ = std::move(on_revoked);
    }

    static constexpr char const* kPublisherRevoked =
        "validator list publisher key revoked";

private:
    VlCache(boost::asio::io_context& io, std::string vl_host, uint16_t vl_port);

    void
    do_fetch();

    void
    schedule_refresh();

    void
    schedule_retry(std::chrono::seconds delay);

    void
    publish_fetch_wait_change() noexcept;

    writ::home_awaitable<catl::vl::ValidatorList>
    co_get_on_strand();

    writ::home_awaitable<catl::vl::ValidatorList>
    co_refresh_on_strand();

    writ::home_awaitable<bool>
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
    writ::home home_;
    std::string vl_host_;
    uint16_t vl_port_;

    // Strand-owned state:
    std::optional<catl::vl::ValidatorList> vl_;
    boost::asio::steady_timer refresh_;  // periodic refresh

    /// The closed classification of stop obligations. A label, never an
    /// identity: the roster resolves retirement by its minted member id.
    enum class StopObligation : std::uint8_t { fetch, refresh_timer };
    using StopRoster = writ::roster<StopObligation>;

    //@@authority state=prv.S0141 status=provisional scope=field
    // bind=xprv::VlCache::stop_roster_@897bb06fb0f45d0f
    // owner=unresolved owner_hypothesis=vl_cache observed_home=vl_cache
    // writ_roles=roster.instance,roster.population
    // label=admitted_stop_obligation_roster
    StopRoster stop_roster_;
    //@@authority state=prv.S0140 status=provisional scope=field
    // bind=xprv::VlCache::fetch_wait_signal_@7d5b64d47245244c
    // owner=unresolved owner_hypothesis=vl_cache observed_home=vl_cache
    // writ_roles=retained_signal.instance
    // label=retained_validator_list_fetch_terminal_or_stop_generation
    std::shared_ptr<writ::retained_signal> fetch_wait_signal_;
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
    // The in-flight fetch's obligation token: storage only — the roster
    // (prv.S0141) owns the population and the admission/retirement laws.
    // has_value() is the old fetch_in_progress_ fact. Each armed
    // refresh/retry handler owns its own token inside its closure.
    std::optional<StopRoster::member> fetch_obligation_;
    //@@authority state=prv.S0067 status=provisional scope=field
    // bind=xprv::VlCache::active_fetch_@4f2904e878920aea
    // owner=unresolved owner_hypothesis=vl_cache observed_home=vl_cache
    // writ_roles=exact.identity
    // label=exact_active_validator_list_fetch_operation_capability
    catl::vl::VlFetchOperationPtr active_fetch_;
    //@@authority state=prv.S0068 status=provisional scope=field
    // bind=xprv::VlCache::active_fetch_cancel_requested_@30b746823bdea8a5
    // owner=unresolved owner_hypothesis=vl_cache observed_home=vl_cache
    // label=accepted_active_fetch_cancellation_request_latch
    bool active_fetch_cancel_requested_ = false;
    bool last_fetch_ok_ = false;
    std::string last_fetch_error_;
    ValidatorManifestHistory validator_manifest_history_;
    RefreshCallback on_refresh_;
    VerifyCallback verify_;
    RevocationCheck revocation_check_;
    RevokedCallback on_publisher_revoked_;
    // The verified revocation of the pinned publisher key, once seen. Final.
    std::optional<catl::vl::Manifest> publisher_revocation_;

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
    std::size_t fail_timer_arms_for_test_ = 0;

    static LogPartition log_;
};

}  // namespace xprv
