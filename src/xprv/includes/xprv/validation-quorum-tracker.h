#pragma once

// ValidationQuorumTracker — strand-protected continuous validation collection.
//
// Collects STValidation messages from the peer network, maintains a ring
// buffer of recent quorum entries, and services proof requests.
// TODO: Evaluate extracting only the generic ingress/worker/timer/barrier
// mechanics; see extract-common-ingress-mechanics-from-validation-quorum-tracker.md.
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
#include <stdexcept>
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

class ValidationQuorumTracker
    : public std::enable_shared_from_this<ValidationQuorumTracker>
{
public:
    static constexpr int kQuorumPercent = 90;
    // Proof anchors need exactly what the reference verifiers accept,
    // ceil(80%) of the embedded list (spec §1.3). Live tracking keeps the
    // stricter kQuorumPercent.
    static constexpr int kProofQuorumPercent = 80;

    static constexpr std::size_t
    proof_quorum_threshold(std::size_t validator_count) noexcept
    {
        return (validator_count * kProofQuorumPercent + 99) / 100;
    }

    ~ValidationQuorumTracker();

    static std::shared_ptr<ValidationQuorumTracker>
    create(
        boost::asio::io_context& io,
        catl::xdata::Protocol const& protocol,
        uint32_t network_id = 0)
    {
        return std::shared_ptr<ValidationQuorumTracker>(
            new ValidationQuorumTracker(io, protocol, network_id));
    }

    /// Feed from unsolicited handler. Safe to call from any thread/strand.
    void
    on_packet(uint16_t type, std::vector<uint8_t> const& data);

    /// Compatibility/destructor seal. Stops admission and joins background
    /// manifest verification, but does not attest that queued strand work has
    /// retired. Graceful service shutdown must use co_stop().
    void
    stop();

    /// Synchronously seal packet/trust admission, cancel the heartbeat, and
    /// join the manifest worker. This is an idempotent admission fence, not a
    /// drain receipt: queued owner-strand work is retired only by co_stop().
    void
    seal_admission_and_join_worker();

    /// Stop admission, join background manifest verification, and cross the
    /// validation owner strand after every previously queued owner operation.
    /// Completion is the exact ValidationQuorumTracker drain receipt.
    boost::asio::awaitable<void>
    co_stop();

    /// Update UNL keys (called when VlCache refreshes). Older list sequences
    /// are ignored. The list's publisher manifest joins held_manifests().
    void
    set_unl(
        std::vector<catl::vl::Manifest> const& validators,
        uint32_t vl_sequence,
        catl::vl::Manifest const& publisher_manifest = catl::vl::Manifest());

    /// Hold a publisher manifest that arrived without a list to apply, such as
    /// a verified publisher revocation, so held_manifests() carries it.
    void
    note_publisher_manifest(catl::vl::Manifest manifest);

    /// Apply the list on the validation strand, or report that a newer list is
    /// already active. Returning true guarantees subsequent quorum reads use
    /// this list sequence. The arguments are taken by value: the coroutine
    /// starts lazily, so a reference could outlive a temporary argument when
    /// the awaitable is handed to co_spawn.
    boost::asio::awaitable<bool>
    co_sync_unl(
        std::vector<catl::vl::Manifest> validators,
        uint32_t vl_sequence,
        catl::vl::Manifest publisher_manifest = catl::vl::Manifest());

    /// Awaitable: snapshot of latest quorum for health checks.
    boost::asio::awaitable<std::optional<QuorumEntry>>
    co_latest_quorum();

    /// Newest proof-mode quorum at or below a transport-supplied ledger
    /// boundary. Returns nullopt across validator-list epoch changes.
    boost::asio::awaitable<std::optional<QuorumEntry>>
    co_latest_quorum_at_or_before(
        uint32_t max_ledger_sequence,
        std::optional<uint32_t> expected_vl_sequence = std::nullopt);

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

    /// Newer verified manifests for listed masters (§1.3), as raw bytes.
    /// Thread-safe snapshot; for verifying proofs on this server.
    std::shared_ptr<std::vector<std::vector<uint8_t>> const>
    held_manifests() const
    {
        std::lock_guard lock(held_manifests_mutex_);
        return held_manifests_;
    }

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
    boost::asio::awaitable<bool>
    co_insert_validation_for_test(ValidationCollector::Entry entry);

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

    void
    set_manifest_parse_hook_for_test(std::function<void()> hook)
    {
        manifest_parse_hook_for_test_ = std::move(hook);
    }

    void
    set_validation_publish_hook_for_test(std::function<void()> hook)
    {
        validation_publish_hook_for_test_ = std::move(hook);
    }

    void
    set_unl_admission_hook_for_test(std::function<void()> hook)
    {
        unl_admission_hook_for_test_ = std::move(hook);
    }

    void
    set_deadline_publish_hook_for_test(std::function<void()> hook)
    {
        deadline_publish_hook_for_test_ = std::move(hook);
    }

    void
    set_quorum_wait_resume_hook_for_test(std::function<void(bool)> hook)
    {
        quorum_wait_resume_hook_for_test_ = std::move(hook);
    }

    void
    set_quorum_deadline_arm_hook_for_test(std::function<void()> hook)
    {
        quorum_deadline_arm_hook_for_test_ = std::move(hook);
    }

    void
    set_before_heartbeat_publication_hook_for_test(std::function<void()> hook)
    {
        before_heartbeat_publication_hook_for_test_ = std::move(hook);
    }

    void
    set_inside_heartbeat_publication_hook_for_test(std::function<void()> hook)
    {
        inside_heartbeat_publication_hook_for_test_ = std::move(hook);
    }

    void
    set_heartbeat_effect_hook_for_test(std::function<void()> hook)
    {
        heartbeat_effect_hook_for_test_ = std::move(hook);
    }

    void
    post_heartbeat_tick_for_test();

    boost::asio::strand<boost::asio::io_context::executor_type>&
    owner_strand_for_test() noexcept
    {
        return strand_;
    }

    void
    set_next_pending_id_for_test(std::uint64_t value)
    {
        if (!strand_.running_in_this_thread())
            throw std::logic_error(
                "pending identity test seed off owner strand");
        next_pending_id_ = value;
    }

    std::uint64_t
    next_pending_id_for_test() const
    {
        if (!strand_.running_in_this_thread())
            throw std::logic_error(
                "pending identity test read off owner strand");
        return next_pending_id_;
    }

    bool
    publication_gate_available_for_test()
    {
        if (!publication_mutex_.try_lock())
            return false;
        publication_mutex_.unlock();
        return true;
    }
#endif

private:
    ValidationQuorumTracker(
        boost::asio::io_context& io,
        catl::xdata::Protocol const& protocol,
        uint32_t network_id);

    // A list's validators and its publisher manifest, carried to the strand
    // together.
    struct UnlUpdate
    {
        std::vector<catl::vl::Manifest> validators;
        catl::vl::Manifest publisher_manifest;
    };

    bool
    apply_unl_locked(
        UnlUpdate const& update,
        uint32_t vl_sequence,
        bool publish);

    void
    publish_trust_state_locked();

    // Hold a newer publisher manifest and republish the snapshot (owner
    // strand, publication_mutex_ held). Independent of the list sequence: a
    // publisher can re-issue its manifest for an unchanged list.
    // clang-format off
    //@@audit-ack unannotated_authority_candidate reason="records a verified publisher manifest in the collector's held set and republishes the read-only snapshot, as refresh_held_manifests_locked does; it grants and revokes nothing itself, and its callers are the list hand-offs and note_publisher_manifest on the owner strand"
    // clang-format on
    void
    note_publisher_manifest_locked(catl::vl::Manifest const& manifest)
    {
        if (collector_.note_publisher_manifest(manifest))
            refresh_held_manifests_locked();
    }

    void
    start_manifest_packet(std::shared_ptr<std::vector<uint8_t>> data);

    void
    finish_manifest_packet();

    std::shared_ptr<std::vector<uint8_t>>
    finish_manifest_packet_locked(bool& schedule_validation_drain);

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
        uint64_t trust_generation,
        std::shared_ptr<std::vector<uint8_t>> const& packet);

    void
    check_for_new_quorum();

    void
    prune_old_entries();

    /// Try to resolve pending proof callbacks. Called from
    /// check_for_new_quorum.
    void
    resolve_pending();

    void
    cancel_pending_locked(QuorumCollectStopReason reason);

    std::optional<QuorumEntry>
    latest_quorum_locked(ValidationCollector::QuorumMode mode) const;

    boost::asio::awaitable<bool>
    co_sync_unl_on_strand(
        std::shared_ptr<UnlUpdate> update,
        uint32_t vl_sequence);

    boost::asio::awaitable<std::optional<QuorumEntry>>
    co_latest_quorum_on_strand();

    boost::asio::awaitable<std::optional<QuorumEntry>>
    co_latest_quorum_at_or_before_on_strand(
        uint32_t max_ledger_sequence,
        std::optional<uint32_t> expected_vl_sequence);

    boost::asio::awaitable<std::optional<QuorumEntry>>
    co_last_full_quorum_on_strand(
        std::chrono::seconds max_age,
        std::optional<uint32_t> expected_vl_sequence);

    boost::asio::awaitable<QuorumCollectResult>
    co_wait_best_quorum_on_strand(
        std::chrono::seconds timeout,
        std::optional<uint32_t> expected_vl_sequence);

    boost::asio::awaitable<bool>
    co_has_live_only_quorum_on_strand();

    boost::asio::awaitable<Stats>
    co_stats_on_strand();

#ifdef XPRV_TESTING
    boost::asio::awaitable<bool>
    co_insert_validation_for_test_on_strand(ValidationCollector::Entry entry);
#endif

    bool
    has_newer_ledger_locked(
        Hash256 const& ledger_hash,
        uint32_t ledger_seq,
        ValidationCollector::QuorumMode mode) const;

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    // Strand-owned state:
    std::string net_label_;
    std::shared_ptr<catl::xdata::Protocol const> protocol_;
    ValidationCollector collector_;
    std::unique_ptr<ManifestWorker> manifest_worker_;

    // Trust admission and quorum publication share an ordering gate. When both
    // locks are needed, publication_mutex_ must be acquired first. Validation
    // enqueue only needs admission_mutex_, so signature/quorum work never
    // blocks ordinary peer ingress.
    std::mutex publication_mutex_;
    std::mutex admission_mutex_;

    // Immutable snapshot of collector_.held_manifest_bytes(), republished on
    // the strand after each list update and manifest packet; read from any
    // thread by held_manifests(). Leaf lock: never held while taking another.
    mutable std::mutex held_manifests_mutex_;

    // Republish held_manifests_ from the collector (owner strand only).
    // The ack must stay ONE line, so clang-format is off around it.
    // clang-format off
    //@@audit-ack unannotated_authority_candidate reason="republishes an immutable copy of collector state for read-only verifiers behind a leaf mutex and grants or revokes nothing itself; its only callers, apply_unl_locked and the manifest-apply completion on the owner strand, are where trust state changes, and they are unannotated debt in the xprv-only scope"
    // clang-format on
    void
    refresh_held_manifests_locked()
    {
        auto held = std::make_shared<std::vector<std::vector<uint8_t>> const>(
            collector_.held_manifest_bytes());
        std::lock_guard lock(held_manifests_mutex_);
        held_manifests_ = std::move(held);
    }
    std::shared_ptr<std::vector<std::vector<uint8_t>> const> held_manifests_ =
        std::make_shared<std::vector<std::vector<uint8_t>> const>();

    // Packet admission is callable from any peer thread. Manifest work and
    // validation ingress are both bounded before they reach the strand. A
    // manifest burst admits one running packet and one successor.
    bool manifest_in_flight_ = false;
    bool manifest_pending_admitted_ = false;
    //@@authority state=prv.S0080 status=provisional scope=field
    // bind=xprv::ValidationQuorumTracker::stopped_@3114749f1ea2a030
    // owner=unresolved owner_hypothesis=validation_quorum_tracker
    // observed_home=ValidationQuorumTracker::stopped_
    // label=sticky_validation_admission_seal
    bool stopped_ = false;
    std::shared_ptr<std::vector<uint8_t>> pending_manifest_;

    struct QueuedValidation
    {
        std::shared_ptr<std::vector<uint8_t>> packet;
        struct Key
        {
            Hash256 digest;
            uint64_t trust_generation = 0;

            bool
            operator<(Key const& other) const
            {
                if (trust_generation != other.trust_generation)
                    return trust_generation < other.trust_generation;
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
    std::size_t pending_unl_updates_ = 0;
    uint64_t trust_admission_generation_ = 0;
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
    std::function<void()> manifest_parse_hook_for_test_;
    std::function<void()> validation_publish_hook_for_test_;
    std::function<void()> unl_admission_hook_for_test_;
    std::function<void()> deadline_publish_hook_for_test_;
    std::function<void(bool)> quorum_wait_resume_hook_for_test_;
    std::function<void()> quorum_deadline_arm_hook_for_test_;
#endif

    std::deque<QuorumEntry> recent_quorums_;
    Hash256 last_quorum_hash_;
    std::optional<QuorumEntry> active_live_quorum_log_;
    std::optional<QuorumEntry> last_full_quorum_;
    std::optional<uint32_t> vl_sequence_;
    bool unpublished_vl_change_ = false;

    // Pending proof callbacks — resolved by check_for_new_quorum on strand.
    struct PendingQuorum
    {
        uint64_t id = 0;          // unique ID for cleanup on cancel
        uint32_t ledger_seq = 0;  // 0 = waiting for ANY quorum
        Hash256 ledger_hash;      // set after initial quorum
        // Preserve the threshold-backed snapshot selected for this waiter.
        // Collector retention may discard old votes while the waiter remains
        // open, but must not weaken a quorum that was already established.
        std::vector<ValidationCollector::Entry> validations;
        std::optional<uint32_t> vl_sequence;
        std::optional<uint32_t> observed_vl_sequence;
        std::chrono::steady_clock::time_point deadline;
        std::function<void(QuorumCollectResult)> callback;
    };
    std::vector<PendingQuorum> pending_;
    uint64_t next_pending_id_ = 0;

    void
    start_heartbeat();

    void
    emit_heartbeat_on_strand();

    boost::asio::steady_timer heartbeat_timer_;
    std::chrono::steady_clock::time_point last_quorum_at_;
    bool heartbeat_started_ = false;

#ifdef XPRV_TESTING
    std::function<void()> before_heartbeat_publication_hook_for_test_;
    std::function<void()> inside_heartbeat_publication_hook_for_test_;
    std::function<void()> heartbeat_effect_hook_for_test_;
#endif

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
