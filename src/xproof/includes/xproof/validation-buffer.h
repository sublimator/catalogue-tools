#pragma once

// ValidationBuffer — strand-protected continuous validation collection.
//
// Collects STValidation messages from the peer network, maintains a ring
// buffer of recent quorum entries, and lets proof requests wait for or
// sample the latest quorum.
//
// Key differences from ValidationCollector (which this wraps):
//   - Never goes deaf — keeps collecting after quorum is reached
//   - Ring buffer of recent quorums — concurrent requests can sample
//   - co_wait_quorum() takes no hash — caller doesn't know anchor upfront
//   - All reads go through the strand — no sync const accessors

#include "validation-collector.h"

#include <catl/core/logger.h>
#include <catl/vl-client/vl-client.h>
#include <catl/xdata/protocol.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <chrono>
#include <deque>
#include <memory>
#include <optional>

namespace xproof {

struct QuorumEntry
{
    Hash256 ledger_hash;
    uint32_t ledger_seq = 0;
    std::vector<ValidationCollector::Entry> validations;
    std::chrono::steady_clock::time_point when;
};

class ValidationBuffer : public std::enable_shared_from_this<ValidationBuffer>
{
public:
    static std::shared_ptr<ValidationBuffer>
    create(boost::asio::io_context& io, catl::xdata::Protocol const& protocol)
    {
        return std::shared_ptr<ValidationBuffer>(
            new ValidationBuffer(io, protocol));
    }

    /// Feed from unsolicited handler. Safe to call from any thread/strand —
    /// dispatches onto our strand internally.
    void
    on_packet(uint16_t type, std::vector<uint8_t> const& data);

    /// Update UNL keys (called when VlCache refreshes).
    /// Dispatches onto strand.
    void
    set_unl(std::vector<catl::vl::Manifest> const& validators);

    /// Awaitable: wait until we have a quorum, return the latest.
    /// Does NOT take a hash — the caller doesn't know the anchor upfront.
    /// Uses 90% quorum threshold (matching XRPL consensus requirements).
    boost::asio::awaitable<QuorumEntry>
    co_wait_quorum(std::chrono::seconds timeout = std::chrono::seconds(30));

    /// Awaitable: snapshot of latest quorum for health checks.
    boost::asio::awaitable<std::optional<QuorumEntry>>
    co_latest_quorum();

private:
    ValidationBuffer(
        boost::asio::io_context& io,
        catl::xdata::Protocol const& protocol);

    static constexpr int kQuorumPercent = 90;

    void
    check_for_new_quorum();

    void
    prune_old_entries();

    void
    wake_waiters();

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    // Strand-owned state:
    ValidationCollector collector_;
    std::deque<QuorumEntry> recent_quorums_;  // newest at back
    Hash256 last_quorum_hash_;                // dedup: don't re-add same quorum

    // Per-waiter signals — each co_wait_quorum() call gets its own timer.
    // wake_waiters() cancels all of them when a new quorum arrives.
    std::vector<std::shared_ptr<boost::asio::steady_timer>> waiters_;

    static constexpr size_t kMaxQuorumEntries = 30;
    static constexpr size_t kMaxCollectorLedgers = 64;
    static constexpr auto kMaxQuorumAge = std::chrono::seconds(120);

    static LogPartition log_;
};

}  // namespace xproof
