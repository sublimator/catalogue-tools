#pragma once

#include "types.h"

#include <boost/system/error_code.hpp>

#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

namespace catl::peer_client {

class PeerClient;
class PeerClientTestAccess;
class PeerSet;
class LedgerFetchCoordinator;
class NodeBatchReceiptAck;
class NodeBatchIntent;

inline constexpr std::size_t kMaxNodeBatchReceiptRows = 256;
inline constexpr std::size_t kMaxNodeBatchReceiptWaiters = 8;
inline constexpr std::size_t kMaxNodeBatchIngressProjections = 256;
inline constexpr std::size_t kMaxNodeBatchRequestNodes = 64;

enum class NodeTree : std::uint8_t {
    transaction = 1,
    account_state = 2,
};

class TransportAttemptId
{
public:
    TransportAttemptId() = default;

    std::uint64_t
    value() const noexcept
    {
        return value_;
    }

    auto
    operator<=>(TransportAttemptId const&) const = default;

private:
    friend class LedgerFetchCoordinator;
    friend class NodeBatchIntent;
    friend class PeerClientTestAccess;

    explicit TransportAttemptId(std::uint64_t value) : value_(value)
    {
    }

    std::uint64_t value_ = 0;
};

class SenderGenerationId
{
public:
    SenderGenerationId() = default;

    std::uint64_t
    value() const noexcept
    {
        return value_;
    }

    auto
    operator<=>(SenderGenerationId const&) const = default;

private:
    friend class LedgerFetchCoordinator;
    friend class NodeBatchIntent;
    friend class PeerClientTestAccess;

    explicit SenderGenerationId(std::uint64_t value) : value_(value)
    {
    }

    std::uint64_t value_ = 0;
};

class PeerConnectionGenerationId
{
public:
    PeerConnectionGenerationId() = default;

    std::uint64_t
    value() const noexcept
    {
        return value_;
    }

    auto
    operator<=>(PeerConnectionGenerationId const&) const = default;

private:
    friend class PeerClientTestAccess;
    friend class PeerSet;

    explicit PeerConnectionGenerationId(std::uint64_t value) : value_(value)
    {
    }

    std::uint64_t value_ = 0;
};

/// Pure digest of the wire-relevant node request payload. It deliberately
/// excludes local attempt identity.
Hash256
node_batch_payload_digest(
    Hash256 const& ledger_hash,
    NodeTree tree,
    std::vector<SHAMapNodeID> const& node_ids);

/// Coordinator-minted transport intent. It has sender identity and payload,
/// but no authority to choose or use a peer generation.
class NodeBatchIntent
{
public:
    TransportAttemptId
    attempt_id() const noexcept
    {
        return attempt_id_;
    }

    SenderGenerationId
    sender_generation() const noexcept
    {
        return sender_generation_;
    }

    Hash256 const&
    ledger_hash() const noexcept
    {
        return ledger_hash_;
    }

    NodeTree
    tree() const noexcept
    {
        return tree_;
    }

    std::vector<SHAMapNodeID> const&
    node_ids() const noexcept
    {
        return node_ids_;
    }

    Hash256 const&
    payload_digest() const noexcept
    {
        return payload_digest_;
    }

private:
    friend class LedgerFetchCoordinator;
    friend class PeerClientTestAccess;

    NodeBatchIntent(
        TransportAttemptId attempt_id,
        SenderGenerationId sender_generation,
        Hash256 ledger_hash,
        NodeTree tree,
        std::vector<SHAMapNodeID> node_ids);

    TransportAttemptId attempt_id_;
    SenderGenerationId sender_generation_;
    Hash256 ledger_hash_;
    NodeTree tree_ = NodeTree::transaction;
    std::vector<SHAMapNodeID> node_ids_;
    Hash256 payload_digest_;
};

/// A sealed request to publish one node batch on one exact admitted peer
/// generation. Node arrival is intentionally not part of this command's
/// lifecycle: XRPL node responses carry no local attempt identity.
class NodeBatchCommand
{
public:
    TransportAttemptId
    attempt_id() const noexcept
    {
        return intent_.attempt_id();
    }

    SenderGenerationId
    sender_generation() const noexcept
    {
        return intent_.sender_generation();
    }

    PeerConnectionGenerationId
    peer_generation() const noexcept
    {
        return peer_generation_;
    }

    Hash256 const&
    ledger_hash() const noexcept
    {
        return intent_.ledger_hash();
    }

    NodeTree
    tree() const noexcept
    {
        return intent_.tree();
    }

    std::vector<SHAMapNodeID> const&
    node_ids() const noexcept
    {
        return intent_.node_ids();
    }

    Hash256 const&
    payload_digest() const noexcept
    {
        return intent_.payload_digest();
    }

private:
    friend class PeerClientTestAccess;
    friend class PeerSet;

    NodeBatchCommand(
        NodeBatchIntent intent,
        PeerConnectionGenerationId peer_generation);

    NodeBatchIntent intent_;
    PeerConnectionGenerationId peer_generation_;
};

enum class NodeBatchWriteOutcome : std::uint8_t {
    written,
    not_ready,
    generation_mismatch,
    identity_conflict,
    receipt_capacity_exhausted,
    receipt_sequence_exhausted,
    write_failed,
    disconnected,
    cancelled,
    generation_closed,
};

/// Exact owner-local terminal fact for the packet-write lifecycle only.
/// `written` means that the framed packet left the connection write queue. It
/// does not claim that a peer returned any requested node. A non-zero receiver
/// commit sequence identifies a retained terminal home. Admission refusals
/// have sequence zero and cannot mint a production acknowledgement.
class NodeBatchWriteReceipt
{
public:
    TransportAttemptId
    attempt_id() const noexcept
    {
        return attempt_id_;
    }

    SenderGenerationId
    sender_generation() const noexcept
    {
        return sender_generation_;
    }

    PeerConnectionGenerationId
    peer_generation() const noexcept
    {
        return peer_generation_;
    }

    Hash256 const&
    payload_digest() const noexcept
    {
        return payload_digest_;
    }

    NodeBatchWriteOutcome
    outcome() const noexcept
    {
        return outcome_;
    }

    boost::system::error_code
    error() const noexcept
    {
        return error_;
    }

    std::uint64_t
    receiver_instance_id() const noexcept
    {
        return receiver_instance_id_;
    }

    std::uint64_t
    receiver_commit_sequence() const noexcept
    {
        return receiver_commit_sequence_;
    }

private:
    friend class PeerClient;

    NodeBatchWriteReceipt(
        NodeBatchCommand const& command,
        NodeBatchWriteOutcome outcome,
        std::uint64_t receiver_instance_id,
        boost::system::error_code error = {}) noexcept
        : attempt_id_(command.attempt_id())
        , sender_generation_(command.sender_generation())
        , peer_generation_(command.peer_generation())
        , payload_digest_(command.payload_digest())
        , outcome_(outcome)
        , error_(error)
        , receiver_instance_id_(receiver_instance_id)
    {
    }

    NodeBatchWriteReceipt(
        NodeBatchCommand const& command,
        NodeBatchWriteOutcome outcome,
        std::uint64_t receiver_instance_id,
        std::uint64_t receiver_commit_sequence,
        boost::system::error_code error) noexcept
        : attempt_id_(command.attempt_id())
        , sender_generation_(command.sender_generation())
        , peer_generation_(command.peer_generation())
        , payload_digest_(command.payload_digest())
        , outcome_(outcome)
        , error_(error)
        , receiver_instance_id_(receiver_instance_id)
        , receiver_commit_sequence_(receiver_commit_sequence)
    {
    }

    TransportAttemptId attempt_id_;
    SenderGenerationId sender_generation_;
    PeerConnectionGenerationId peer_generation_;
    Hash256 payload_digest_;
    NodeBatchWriteOutcome outcome_ = NodeBatchWriteOutcome::cancelled;
    boost::system::error_code error_;
    std::uint64_t receiver_instance_id_ = 0;
    std::uint64_t receiver_commit_sequence_ = 0;
};

using NodeBatchWriteHandler = std::function<void(NodeBatchWriteReceipt)>;

/// Capability derived from a receipt. Consuming it retires only the exact
/// receiver-side terminal receipt home from which it was derived. Only the
/// coordinator transition that consumes the matching sender outbox row may
/// mint this capability.
class NodeBatchReceiptAck
{
public:
    TransportAttemptId
    attempt_id() const noexcept
    {
        return attempt_id_;
    }

    SenderGenerationId
    sender_generation() const noexcept
    {
        return sender_generation_;
    }

    PeerConnectionGenerationId
    peer_generation() const noexcept
    {
        return peer_generation_;
    }

    Hash256 const&
    payload_digest() const noexcept
    {
        return payload_digest_;
    }

    std::uint64_t
    receiver_instance_id() const noexcept
    {
        return receiver_instance_id_;
    }

    std::uint64_t
    receiver_commit_sequence() const noexcept
    {
        return receiver_commit_sequence_;
    }

private:
    friend class LedgerFetchCoordinator;
    friend class PeerClientTestAccess;

    explicit NodeBatchReceiptAck(NodeBatchWriteReceipt const& receipt) noexcept
        : attempt_id_(receipt.attempt_id())
        , sender_generation_(receipt.sender_generation())
        , peer_generation_(receipt.peer_generation())
        , payload_digest_(receipt.payload_digest())
        , receiver_instance_id_(receipt.receiver_instance_id())
        , receiver_commit_sequence_(receipt.receiver_commit_sequence())
    {
    }

    TransportAttemptId attempt_id_;
    SenderGenerationId sender_generation_;
    PeerConnectionGenerationId peer_generation_;
    Hash256 payload_digest_;
    std::uint64_t receiver_instance_id_ = 0;
    std::uint64_t receiver_commit_sequence_ = 0;
};

}  // namespace catl::peer_client
