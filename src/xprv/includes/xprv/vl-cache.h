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

namespace xprv {

class VlCache : public std::enable_shared_from_this<VlCache>
{
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

    /// Cancel refresh timer. Existing fetch completes but no new ones start.
    void
    stop();

    /// Awaitable: returns a copy of the cached VL.
    /// Blocks until the first fetch completes. Hops to strand internally.
    boost::asio::awaitable<catl::vl::ValidatorList>
    co_get();

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
    using RefreshCallback =
        std::function<void(catl::vl::ValidatorList const&)>;
    void
    set_on_refresh(RefreshCallback cb)
    {
        on_refresh_ = std::move(cb);
    }

private:
    VlCache(boost::asio::io_context& io, std::string vl_host, uint16_t vl_port);

    void
    do_fetch();

    void
    schedule_refresh();

    void
    schedule_retry(std::chrono::seconds delay);

    boost::asio::io_context& io_;
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::string vl_host_;
    uint16_t vl_port_;

    // Strand-owned state:
    std::optional<catl::vl::ValidatorList> vl_;
    boost::asio::steady_timer signal_;            // wake waiters on first fetch
    boost::asio::steady_timer refresh_;           // periodic refresh
    std::chrono::seconds refresh_interval_{900};  // 15 minutes
    std::chrono::seconds initial_retry_{2};       // first retry delay
    std::chrono::seconds initial_retry_cap_{60};  // max retry delay before first success
    bool fetch_in_progress_ = false;
    RefreshCallback on_refresh_;

    static LogPartition log_;
};

}  // namespace xprv
