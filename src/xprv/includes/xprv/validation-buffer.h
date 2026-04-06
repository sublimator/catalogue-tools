#pragma once

// ValidationBuffer — strand-protected continuous validation collection.
//
// Collects STValidation messages from the peer network, maintains a ring
// buffer of recent quorum entries, and services proof requests.
//
// All mutation happens on the strand. Coroutine callers suspend via a
// completion callback invoked directly from check_for_new_quorum() —
// no timer signals, no async races.

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
#include <functional>
#include <memory>
#include <optional>
#include <vector>

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

    /// Feed from unsolicited handler. Safe to call from any thread/strand.
    void
    on_packet(uint16_t type, std::vector<uint8_t> const& data);

    /// Update UNL keys (called when VlCache refreshes).
    void
    set_unl(std::vector<catl::vl::Manifest> const& validators);

    /// Awaitable: snapshot of latest quorum for health checks.
    boost::asio::awaitable<std::optional<QuorumEntry>>
    co_latest_quorum();

    /// Wait for best quorum for a proof anchor. Returns when:
    ///   - full UNL coverage achieved, OR
    ///   - next ledger's first validation seen (our ledger is finalized), OR
    ///   - timeout
    /// Pure callback — no timer signals. check_for_new_quorum() invokes
    /// pending callbacks directly on the strand on every validation.
    boost::asio::awaitable<QuorumCollectResult>
    co_wait_best_quorum(
        ValidationCollector::QuorumMode mode =
            ValidationCollector::QuorumMode::proof,
        std::chrono::seconds timeout = std::chrono::seconds(30));

    /// True when live peer manifests can explain a quorum but VL cannot.
    boost::asio::awaitable<bool>
    co_has_live_only_quorum();

    struct Stats
    {
        size_t recent_quorums = 0;
        size_t collector_ledgers = 0;
        size_t collector_validations = 0;
        size_t pending_callbacks = 0;
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

    /// Try to resolve pending proof callbacks. Called from check_for_new_quorum.
    void
    resolve_pending();

    std::optional<QuorumEntry>
    latest_quorum_locked(ValidationCollector::QuorumMode mode) const;

    bool
    has_newer_ledger_locked(uint32_t ledger_seq) const;

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    // Strand-owned state:
    std::string net_label_;
    ValidationCollector collector_;
    std::deque<QuorumEntry> recent_quorums_;
    Hash256 last_quorum_hash_;
    Hash256 last_proof_quorum_hash_;
    std::optional<QuorumEntry> active_live_quorum_log_;

    // Pending proof callbacks — resolved by check_for_new_quorum on strand.
    struct PendingQuorum
    {
        ValidationCollector::QuorumMode mode;
        uint32_t ledger_seq = 0;       // 0 = waiting for ANY quorum
        Hash256 ledger_hash;            // set after initial quorum
        std::chrono::steady_clock::time_point deadline;
        std::function<void(QuorumCollectResult)> callback;
    };
    std::vector<PendingQuorum> pending_;

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
