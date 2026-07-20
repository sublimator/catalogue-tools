#pragma once

// ValidationBuffer — strand-protected continuous validation collection.
//
// Collects STValidation messages from the peer network, maintains a ring
// buffer of recent quorum entries, and services proof requests.
//
// Collector and quorum mutation happens on the strand. Packet admission is
// bounded under a separate mutex before work reaches that strand. Coroutine
// callers suspend via a completion callback invoked directly from
// check_for_new_quorum() — no timer signals, no async races.

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
#include <map>
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

    /// Stop admission and join background manifest verification. One collector
    /// operation already running on the validation strand may finish.
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
        size_t queued_validations = 0;
        size_t queued_validation_bytes = 0;
        size_t validation_packets_received = 0;
        size_t validation_packets_processed = 0;
        size_t validation_packets_deduplicated = 0;
        size_t validation_packets_dropped = 0;
        size_t manifest_packets_received = 0;
        size_t manifest_packets_admitted = 0;
        size_t manifest_packets_processed = 0;
        size_t manifest_packets_dropped = 0;
        size_t manifest_packets_abandoned = 0;
        bool manifest_in_flight = false;
        bool manifest_pending = false;
    };

    boost::asio::awaitable<Stats>
    co_stats();

#ifdef XPRV_TESTING
    void
    set_validation_commit_hook_for_test(std::function<void()> hook)
    {
        validation_commit_hook_for_test_ = std::move(hook);
    }

    void
    set_manifest_apply_hook_for_test(std::function<void()> hook)
    {
        manifest_apply_hook_for_test_ = std::move(hook);
    }
#endif

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
    abandon_manifest_packets_locked();

    void
    schedule_validation_drain();

    void
    drain_validation_queue();

    void
    clear_validation_queue_locked(bool count_as_dropped);

    void
    erase_validation_index_locked(
        Hash256 const& digest,
        uint64_t manifest_generation,
        std::shared_ptr<std::vector<uint8_t>> const& packet);

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

    // Packet admission is callable from any peer thread. Manifest work and
    // validation ingress are both bounded before they reach the strand. A
    // manifest burst admits one running packet and one successor.
    std::mutex admission_mutex_;
    bool manifest_in_flight_ = false;
    bool manifest_pending_admitted_ = false;
    bool stopped_ = false;
    std::shared_ptr<std::vector<uint8_t>> pending_manifest_;

    struct QueuedValidation
    {
        std::shared_ptr<std::vector<uint8_t>> packet;
        struct Key
        {
            Hash256 digest;
            uint64_t manifest_generation = 0;

            bool
            operator<(Key const& other) const
            {
                if (manifest_generation != other.manifest_generation)
                    return manifest_generation < other.manifest_generation;
                return digest < other.digest;
            }
        } key;
    };
    std::deque<QueuedValidation> pending_validations_;
    std::multimap<QueuedValidation::Key, std::shared_ptr<std::vector<uint8_t>>>
        pending_validation_index_;
    std::size_t pending_validation_bytes_ = 0;
    std::size_t active_validation_bytes_ = 0;
    std::size_t active_validations_ = 0;
    bool validation_drain_scheduled_ = false;
    // Gives one validation batch priority after a manifest burst.
    bool validation_drain_priority_ = false;
    uint64_t manifest_admission_generation_ = 0;
    std::size_t validation_packets_received_ = 0;
    std::size_t validation_packets_processed_ = 0;
    std::size_t validation_packets_deduplicated_ = 0;
    std::size_t validation_packets_dropped_ = 0;
    std::size_t manifest_packets_received_ = 0;
    std::size_t manifest_packets_admitted_ = 0;
    std::size_t manifest_packets_processed_ = 0;
    std::size_t manifest_packets_dropped_ = 0;
    std::size_t manifest_packets_abandoned_ = 0;

#ifdef XPRV_TESTING
    std::function<void()> validation_commit_hook_for_test_;
    std::function<void()> manifest_apply_hook_for_test_;
#endif

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
    static constexpr size_t kMaxPendingValidationBytes = 1024 * 1024;
    static constexpr size_t kMaxPendingValidations = 1024;
    static constexpr size_t kValidationDrainBatch = 16;
    static constexpr auto kValidationDrainSlice = std::chrono::milliseconds(2);
    static constexpr auto kMaxQuorumAge = std::chrono::seconds(120);
    static constexpr auto kHeartbeatInterval = std::chrono::seconds(10);

    static LogPartition log_;
};

}  // namespace xprv
