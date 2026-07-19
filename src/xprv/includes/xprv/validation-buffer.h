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
#include <mutex>
#include <optional>
#include <vector>

namespace xprv {

class ManifestWorker;

struct QuorumEntry
{
    Hash256 ledger_hash;
    uint32_t ledger_seq = 0;
    std::vector<ValidationCollector::Entry> validations;
    std::chrono::steady_clock::time_point when;
    uint32_t vl_sequence = 0;
};

enum class QuorumCollectStopReason {
    full,
    next_ledger,
    timeout,
    validator_list_changed
};

struct QuorumCollectResult
{
    QuorumEntry quorum;
    QuorumCollectStopReason stop_reason = QuorumCollectStopReason::timeout;
};

class ValidationBuffer : public std::enable_shared_from_this<ValidationBuffer>
{
public:
    ~ValidationBuffer();

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

    /// Stop and join background manifest verification.
    void
    stop();

    /// Update UNL keys (called when VlCache refreshes). Older list sequences
    /// are ignored.
    void
    set_unl(
        std::vector<catl::vl::Manifest> const& validators,
        uint32_t vl_sequence);

    /// Apply the list on the validation strand, or report that a newer list is
    /// already active. Returning true guarantees subsequent quorum reads use
    /// this list sequence.
    boost::asio::awaitable<bool>
    co_sync_unl(
        std::vector<catl::vl::Manifest> const& validators,
        uint32_t vl_sequence);

    /// Awaitable: snapshot of latest quorum for health checks.
    boost::asio::awaitable<std::optional<QuorumEntry>>
    co_latest_quorum();

    /// Last quorum that reached full UNL coverage. Updated event-driven
    /// by check_for_new_quorum when "full" is detected. Returns nullopt
    /// if no full quorum exists or it's older than max_age.
    boost::asio::awaitable<std::optional<QuorumEntry>>
    co_last_full_quorum(
        std::chrono::seconds max_age,
        std::optional<uint32_t> expected_vl_sequence = std::nullopt);

    /// Wait for best quorum for a proof anchor. Returns when:
    ///   - full UNL coverage achieved, OR
    ///   - next ledger's first validation seen (our ledger is finalized), OR
    ///   - timeout, OR
    ///   - the expected validator-list sequence changes
    /// Pure callback — no timer signals. check_for_new_quorum() invokes
    /// pending callbacks directly on the strand on every validation.
    boost::asio::awaitable<QuorumCollectResult>
    co_wait_best_quorum(
        std::chrono::seconds timeout = std::chrono::seconds(30),
        std::optional<uint32_t> expected_vl_sequence = std::nullopt);

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
    apply_unl_locked(
        std::vector<catl::vl::Manifest> const& validators,
        uint32_t vl_sequence);

    void
    start_manifest_packet(std::shared_ptr<std::vector<uint8_t>> data);

    void
    finish_manifest_packet();

    void
    check_for_new_quorum();

    void
    prune_old_entries();

    /// Try to resolve pending proof callbacks. Called from
    /// check_for_new_quorum.
    void
    resolve_pending();

    std::optional<QuorumEntry>
    latest_quorum_locked(ValidationCollector::QuorumMode mode) const;

    bool
    has_newer_ledger_locked(uint32_t ledger_seq) const;

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    // Strand-owned state:
    std::string net_label_;
    std::shared_ptr<catl::xdata::Protocol const> protocol_;
    ValidationCollector collector_;
    std::unique_ptr<ManifestWorker> manifest_worker_;

    // Manifest admission is callable from any peer thread. Keep at most one
    // packet running and one waiting so work and retained frame memory remain
    // bounded without discarding every concurrent update.
    std::mutex manifest_mutex_;
    bool manifest_in_flight_ = false;
    bool manifest_pending_admitted_ = false;
    bool stopped_ = false;
    std::shared_ptr<std::vector<uint8_t>> pending_manifest_;
    std::deque<std::shared_ptr<std::vector<uint8_t>>> deferred_validations_;
    std::size_t deferred_validation_bytes_ = 0;

    std::deque<QuorumEntry> recent_quorums_;
    Hash256 last_quorum_hash_;
    std::optional<QuorumEntry> active_live_quorum_log_;
    std::optional<QuorumEntry> last_full_quorum_;
    std::optional<uint32_t> vl_sequence_;

    // Pending proof callbacks — resolved by check_for_new_quorum on strand.
    struct PendingQuorum
    {
        uint64_t id = 0;          // unique ID for cleanup on cancel
        uint32_t ledger_seq = 0;  // 0 = waiting for ANY quorum
        Hash256 ledger_hash;      // set after initial quorum
        std::optional<uint32_t> vl_sequence;
        std::chrono::steady_clock::time_point deadline;
        std::function<void(QuorumCollectResult)> callback;
    };
    std::vector<PendingQuorum> pending_;
    uint64_t next_pending_id_ = 0;

    void
    start_heartbeat();

    boost::asio::steady_timer heartbeat_timer_;
    std::chrono::steady_clock::time_point last_quorum_at_;
    bool heartbeat_started_ = false;

    static constexpr size_t kMaxQuorumEntries = 30;
    static constexpr size_t kMaxCollectorLedgers = 64;
    static constexpr size_t kMaxDeferredValidationBytes = 1024 * 1024;
    static constexpr size_t kMaxDeferredValidations = 1024;
    static constexpr auto kMaxQuorumAge = std::chrono::seconds(120);
    static constexpr auto kHeartbeatInterval = std::chrono::seconds(10);

    static LogPartition log_;
};

}  // namespace xprv
