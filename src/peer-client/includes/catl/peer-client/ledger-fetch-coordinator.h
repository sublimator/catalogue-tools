#pragma once

#include "node-transport.h"
#include "peer-generation-pin.h"

#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace catl::peer_client {

class PeerSet;
class LedgerFetchCoordinatorTestAccess;

/// Externally supplied coordinator owner key. The coordinator does NOT mint
/// this: provider composition owns it, and live routing stays blocked until that
/// composition supplies a real one. Candidate tests inject fixed keys. It exists
/// now so the target `(ProviderId, PartitionId)` boundary is not erased merely
/// because its production source is a later cut.
struct CoordinatorOwnerKey
{
    std::uint64_t provider_id = 0;
    std::uint64_t partition_id = 0;

    auto
    operator<=>(CoordinatorOwnerKey const&) const = default;
};

/**
 * Coordinator-owned sender half of the NodeBatch packet-write protocol.
 *
 * Exists because production had no executor able to own a replayable sender
 * row: `NodeCache` serialises with a mutex plus a header-state strand, and a
 * mutex cannot own an asynchronous row lifecycle. `PeerClient` owns the
 * RECEIVER side, so sender authority must not be minted there.
 *
 * Scope is deliberately smaller than C09: the only fact established is that one
 * framed node batch left one exact admitted peer generation's write queue. Node
 * ARRIVAL is not in this lifecycle, because XRPL node responses carry no local
 * attempt identity. Nothing here claims retry selection, proof membership, C09
 * membership, or generation closure.
 *
 * A terminal receipt is NOT replayable into success. `write_failed`,
 * `disconnected`, `cancelled` and `generation_closed` are retained terminal
 * facts: re-publishing the same command can only replay that same terminal
 * receipt. So a retry after a non-`written` terminal mints a NEW
 * `TransportAttemptId` and binds again — it is never replay of the old command.
 */
class LedgerFetchCoordinator
    : public std::enable_shared_from_this<LedgerFetchCoordinator>
{
public:
    /// Hard cap on live sender rows. The outbox reserves this once, so the
    /// commit section performs no allocation.
    static constexpr std::size_t kMaxOutboxRows = 64;

    /// Serialized-byte ceiling for one batch, enforced before any row exists.
    static constexpr std::size_t kMaxNodeBatchPayloadBytes = 8192;

    /// Why the coordinator did or did not accept responsibility for a
    /// submission. This is ADMISSION only. It never reports transport
    /// terminality — that is the write receipt's job, and the two must not
    /// compete for the same meaning.
    enum class Admission : std::uint8_t {
        accepted,
        stopping,
        node_count_exceeded,
        payload_bytes_exceeded,
        outbox_full,
        attempt_ids_exhausted,
        sender_generations_exhausted,
        unpinned_peer,
    };

    /// Where one transport attempt has got to. Explicit, because publication
    /// replay, terminal-receipt replay, and retry are three different things
    /// and a boolean set conflated them.
    enum class Phase : std::uint8_t {
        awaiting_bind,           // committed; no sealed command yet
        publishable,             // sealed command held; may be (re)published
        publication_in_flight,   // handed to the receiver; awaiting a receipt
        bind_refused,            // stale generation; a retry needs a new attempt
        terminal_receipt,        // retained terminal fact; not republishable
        ack_ingress_pending,     // ack refused; settlement retained for retry
    };

    enum class Lifecycle : std::uint8_t { running, stopping, stopped };

    static std::shared_ptr<LedgerFetchCoordinator>
    create(
        boost::asio::io_context& io,
        CoordinatorOwnerKey owner,
        std::shared_ptr<PeerSet> peers);

    LedgerFetchCoordinator(LedgerFetchCoordinator const&) = delete;
    LedgerFetchCoordinator&
    operator=(LedgerFetchCoordinator const&) = delete;

    /// Compatibility ingress: submit against an ALREADY selected physical pin.
    ///
    /// The name is deliberate. This is the legacy seam still preventing live
    /// cutover, and it does not claim the caller owns retry or transport
    /// authority. When the target coordinator can ask `PeerSet` to select by
    /// ledger availability, this entry point is what goes away.
    ///
    /// Admission is decided and returned SYNCHRONOUSLY, so a stopping or
    /// capacity-closed coordinator cannot silently discard a request. A granted
    /// admission has already reserved its row slot, so the on-strand commit
    /// cannot fail for capacity.
    [[nodiscard]] Admission
    submit_node_batch_on_pin(
        Hash256 ledger_hash,
        NodeTree tree,
        std::vector<SHAMapNodeID> node_ids,
        PeerGenerationPin pin);

    /// Seal admission and drain. `on_stopped` fires only once every ingress
    /// permit, in-flight bind, retained row and pin share is accounted for.
    ///
    /// Composition precondition: this coordinator's executor, `PeerSet`'s
    /// strand, and the relevant `PeerClient` receiver strand must keep making
    /// progress until completion fires. An executor stopped early is not a
    /// successful drain.
    void
    stop(std::function<void()> on_stopped = {});

    Lifecycle
    lifecycle() const noexcept
    {
        return lifecycle_.load(std::memory_order_acquire);
    }

    /// Immutable projections; observability never reads a container across the
    /// owner-strand boundary.
    std::size_t
    outbox_size() const noexcept
    {
        return outbox_projection_.load(std::memory_order_relaxed);
    }

    std::size_t
    ingress_permits() const noexcept
    {
        return ingress_permits_.load(std::memory_order_relaxed);
    }

    SenderGenerationId
    sender_generation() const noexcept
    {
        return sender_generation_;
    }

    CoordinatorOwnerKey const&
    owner() const noexcept
    {
        return owner_;
    }

    /// Retry settlements whose acknowledgement did not queue. Publication stays
    /// stopped; this never republishes a command.
    void
    retry_pending_acks();

    /// Canonical semantic payload size used by the sender budget. Named
    /// "semantic" because it is not the exact protobuf encoding: `PeerClient`
    /// builds the wire message with query type and depth separately.
    static std::size_t
    semantic_payload_bytes(std::size_t node_count) noexcept;

private:
    friend class LedgerFetchCoordinatorTestAccess;

    LedgerFetchCoordinator(
        boost::asio::io_context& io,
        CoordinatorOwnerKey owner,
        std::shared_ptr<PeerSet> peers);

    /// Process-common-fate, never-reused sender generation. A restarted
    /// coordinator therefore cannot alias a receiver row retained from an
    /// earlier incarnation. Returns nullopt at the ceiling rather than wrapping.
    static std::optional<SenderGenerationId>
    allocate_sender_generation() noexcept;

    struct OutboxRow
    {
        OutboxRow(NodeBatchIntent intent_, PeerGenerationPin pin_) noexcept
            : intent(std::move(intent_)), pin(std::move(pin_))
        {
        }

        OutboxRow(OutboxRow const&) = delete;
        OutboxRow&
        operator=(OutboxRow const&) = delete;
        OutboxRow(OutboxRow&&) noexcept = default;
        OutboxRow&
        operator=(OutboxRow&&) noexcept = default;

        NodeBatchIntent intent;
        /// The ORIGINAL pin. The bind consumes a share(); this retains the
        /// eviction veto through ack ingress.
        PeerGenerationPin pin;
        /// Sealed after bind and never rebuilt during ordinary replay.
        std::optional<NodeBatchCommand> sealed;
        std::optional<NodeBatchWriteReceipt> receipt;
        Phase phase = Phase::awaiting_bind;
        std::uint32_t publish_attempts = 0;
        std::uint32_t ack_attempts = 0;
    };

    // No asynchronous handler captures an OutboxRow&; every continuation
    // re-finds its row by attempt id.
    OutboxRow*
    find_row(TransportAttemptId attempt) noexcept;

    void
    commit_row_on_strand(NodeBatchIntent intent, PeerGenerationPin pin);

    void
    bind_on_strand(TransportAttemptId attempt);

    void
    publish_on_strand(TransportAttemptId attempt);

    void
    on_receipt(NodeBatchWriteReceipt receipt);

    void
    consume_receipt_on_strand(NodeBatchWriteReceipt receipt);

    void
    post_ack_on_strand(TransportAttemptId attempt);

    void
    retire_row_on_strand(TransportAttemptId attempt);

    static bool
    receipt_matches(OutboxRow const& row, NodeBatchWriteReceipt const& receipt);

    void
    refresh_projection() noexcept;

    /// Fires on_stopped_ when every permit, bind, row and pin is accounted for.
    void
    check_drained_on_strand();

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    CoordinatorOwnerKey owner_;
    std::shared_ptr<PeerSet> peers_;
    SenderGenerationId sender_generation_;
    std::uint64_t next_attempt_ = 0;
    std::vector<OutboxRow> outbox_;
    std::size_t binds_in_flight_ = 0;
    std::function<void()> on_stopped_;

    /// Granted-but-not-yet-committed submissions. Capped, so ingress cannot
    /// post unbounded work before the strand runs.
    std::atomic<std::size_t> ingress_permits_{0};
    std::atomic<std::size_t> outbox_projection_{0};
    std::atomic<Lifecycle> lifecycle_{Lifecycle::running};
};

}  // namespace catl::peer_client
