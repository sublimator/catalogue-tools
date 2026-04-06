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

namespace xprv {

struct QuorumEntry
{
    Hash256 ledger_hash;
    uint32_t ledger_seq = 0;
    std::vector<ValidationCollector::Entry> validations;
    std::chrono::steady_clock::time_point when;
};

enum class QuorumCollectStopReason { full, next_ledger, timeout };

struct QuorumCollectResult
{
    QuorumEntry quorum;
    QuorumCollectStopReason stop_reason = QuorumCollectStopReason::timeout;
};

class ValidationBuffer : public std::enable_shared_from_this<ValidationBuffer>
{
public:
    static std::shared_ptr<ValidationBuffer>
    create(
        boost::asio::io_context& io,
        catl::xdata::Protocol const& protocol,
        uint32_t network_id = 0)
    {
        return std::shared_ptr<ValidationBuffer>(
            new ValidationBuffer(io, protocol, network_id));
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
    co_wait_quorum(
        std::chrono::seconds timeout = std::chrono::seconds(30),
        ValidationCollector::QuorumMode mode =
            ValidationCollector::QuorumMode::live);

    /// Awaitable: snapshot of latest quorum for health checks.
    boost::asio::awaitable<std::optional<QuorumEntry>>
    co_latest_quorum();

    /// After quorum has been reached for a ledger, keep collecting additional
    /// validations for that same ledger until either a newer ledger appears or
    /// the linger timeout expires.
    boost::asio::awaitable<QuorumCollectResult>
    co_collect_quorum_validations(
        Hash256 const& ledger_hash,
        uint32_t ledger_seq,
        std::chrono::milliseconds linger_timeout,
        ValidationCollector::QuorumMode mode =
            ValidationCollector::QuorumMode::proof);

    /// Awaitable: true when live peer manifests can explain a quorum but the
    /// current VL cannot. This is the signal to force a VL refresh rather than
    /// broadening the proof format.
    boost::asio::awaitable<bool>
    co_has_live_only_quorum();

    struct Stats
    {
        size_t recent_quorums = 0;
        size_t collector_ledgers = 0;
        size_t collector_validations = 0;
        size_t waiters = 0;
    };

    boost::asio::awaitable<Stats>
    co_stats();

private:
    ValidationBuffer(
        boost::asio::io_context& io,
        catl::xdata::Protocol const& protocol,
        uint32_t network_id);

    static constexpr int kQuorumPercent = 90;

    void
    check_for_new_quorum();

    void
    prune_old_entries();

    void
    wake_waiters();

    std::optional<QuorumEntry>
    latest_quorum_locked(ValidationCollector::QuorumMode mode) const;

    bool
    has_newer_ledger_locked(uint32_t ledger_seq) const;

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    // Strand-owned state:
    std::string net_label_;
    ValidationCollector collector_;
    std::deque<QuorumEntry> recent_quorums_;  // newest at back
    Hash256 last_quorum_hash_;                // dedup: don't re-add same quorum
    Hash256 last_proof_quorum_hash_;          // wake proof waiters on VL refresh
    std::optional<QuorumEntry> active_live_quorum_log_;
    // Per-waiter signals — each co_wait_quorum() call gets its own timer.
    // wake_waiters() cancels all of them when a new quorum arrives.
    std::vector<std::shared_ptr<boost::asio::steady_timer>> waiters_;

    void
    start_heartbeat();

    boost::asio::steady_timer heartbeat_timer_;
    std::chrono::steady_clock::time_point last_quorum_at_;
    bool heartbeat_started_ = false;

    static constexpr size_t kMaxQuorumEntries = 30;
    static constexpr size_t kMaxCollectorLedgers = 64;
    static constexpr auto kMaxQuorumAge = std::chrono::seconds(120);
    static constexpr auto kHeartbeatInterval = std::chrono::seconds(10);

    static LogPartition log_;
};

}  // namespace xprv
