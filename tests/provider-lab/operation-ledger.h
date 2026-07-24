#pragma once

// Exact root/child operation leases — target-stack package 3, increment 2
// (ledger-data-provider decomposition v4, "Root and child operation
// leases"). TEST-ONLY: production construction must never reference this.
//
// The contract, from the design body:
//  - one move-only root lease per top-level operation; children are
//    move-only leases with non-reused ChildIds, registered BEFORE any
//    async work is published and refused once the root is closing;
//  - each child owns a sealed level-triggered publication cell:
//    registered_not_published -> published(DestinationId, WorkId)
//                              -> never_published (one sealed ack);
//  - after the published flip, ONLY that destination's exact terminal
//    acknowledgement wins the exactly-once gate that removes the child.
//    Completion, cancellation, abandonment, deadline, and shutdown may
//    COMPETE to request terminality, but none of them can fabricate it:
//    duplicate, late, wrong-destination, and wrong-work acks are no-ops;
//  - a cancellation request makes the child's destination capability
//    inert but never decrements the child;
//  - the root row is removed only when its child map is empty; a dropped
//    lease leaves a visible row — false-empty is never the policy;
//  - destructors are passive. All state is owner-strand confined and
//    off-strand mutation is refused, not raced.

#include "coordinator-artifacts.h"
#include "deterministic-scheduler.h"

#include <cstdint>
#include <map>
#include <optional>
#include <utility>

namespace catl::provider_lab {

using OperationId = std::uint64_t;
using ChildId = std::uint64_t;
using DestinationId = std::uint64_t;
using WorkId = std::uint64_t;

/// Move-only handle to a registered row. Dropping it does NOT release
/// the row (destructors are passive); only the ledger's exactly-once
/// gates remove rows. A moved-from lease is empty.
template <typename Tag>
class Lease
{
public:
    Lease() = default;

    Lease(Lease&& other) noexcept
        : id_(other.id_), valid_(std::exchange(other.valid_, false))
    {
    }
    Lease&
    operator=(Lease&& other) noexcept
    {
        id_ = other.id_;
        valid_ = std::exchange(other.valid_, false);
        return *this;
    }
    Lease(Lease const&) = delete;
    Lease&
    operator=(Lease const&) = delete;

    bool
    valid() const
    {
        return valid_;
    }
    std::uint64_t
    id() const
    {
        return id_;
    }

private:
    friend class OperationLedger;  // sole minter: handles are non-forgeable
    explicit Lease(std::uint64_t id) : id_(id), valid_(true) {}

    std::uint64_t id_ = 0;
    bool valid_ = false;
};

class OperationLedger;

struct RootTag
{
};
struct ChildTag
{
};
using RootLease = Lease<RootTag>;
using ChildLease = Lease<ChildTag>;

class OperationLedger
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    enum class PublicationState : std::uint8_t {
        registered_not_published,
        published,
        never_published,
    };

    enum class AckOutcome : std::uint8_t {
        removed,            // exactly-once win
        duplicate_or_late,  // child already gone — no-op
        wrong_destination,  // published to someone else — no-op
        wrong_work,         // destination matches, work does not — no-op
        unpublished,        // cell never flipped to published — no-op
        off_strand,         // refused: caller is not the owner
        wrong_scope,    // artifact from another partition instance
        wrong_binding,  // ack's operation does not own its child
    };

    enum class Refusal : std::uint8_t {
        ok,
        root_closing,
        unknown_root,
        off_strand,
        cell_not_registered,
        already_flipped,
        cancelled_inert,  // pre-publication cancel: seal never_published
    };

    explicit OperationLedger(strand_type strand, PartitionScope scope)
        : strand_(std::move(strand)), scope_(scope)
    {
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    /// T50-analog: mint one root operation row.
    //@@authority transition=provider_lab.open_root owner=provider_lab commit=strand
    std::optional<RootLease>
    open_root()
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto const id = ++next_operation_;
        operations_.emplace(id, Operation{});
        return RootLease(id);
    }

    /// Register one child under a live, non-closing root. The returned
    /// lease's ChildId is never reused, and registration linearizes
    /// before any publication of work.
    //@@authority transition=provider_lab.register_child owner=provider_lab commit=strand
    std::pair<Refusal, ChildLease>
    register_child(RootLease const& root)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, ChildLease{}};
        auto it = root.valid() ? operations_.find(root.id())
                               : operations_.end();
        if (it == operations_.end())
            return {Refusal::unknown_root, ChildLease{}};
        if (it->second.closing)
            return {Refusal::root_closing, ChildLease{}};
        auto const child_id = ++next_child_;
        it->second.children.emplace(child_id, Child{});
        child_index_.emplace(child_id, root.id());
        return {Refusal::ok, ChildLease(child_id)};
    }

    /// Level-triggered flip: registered_not_published -> published.
    //@@authority transition=provider_lab.publish_child owner=provider_lab commit=strand
    Refusal
    publish(ChildLease const& child, DestinationId dest, WorkId work)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto* c = find_child(child);
        if (!c)
            return Refusal::cell_not_registered;
        if (c->state != PublicationState::registered_not_published)
            return Refusal::already_flipped;
        if (c->inert)
            return Refusal::cancelled_inert;  // work must never publish;
                                              // the never_published path
                                              // (mark + sealed ack) remains
        c->state = PublicationState::published;
        c->destination = dest;
        c->work = work;
        return Refusal::ok;
    }

    /// Enqueue failure: registered_not_published -> never_published,
    /// permitting exactly one sealed never-published acknowledgement.
    //@@authority transition=provider_lab.mark_never_published owner=provider_lab commit=strand
    Refusal
    mark_never_published(ChildLease const& child)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto* c = find_child(child);
        if (!c)
            return Refusal::cell_not_registered;
        if (c->state != PublicationState::registered_not_published)
            return Refusal::already_flipped;
        c->state = PublicationState::never_published;
        return Refusal::ok;
    }

    /// Consume the sealed never-published ack: removes the child.
    /// Exactly-once; a second call is duplicate_or_late.
    //@@authority transition=provider_lab.ack_never_published owner=provider_lab commit=strand
    AckOutcome
    ack_never_published(ChildId child_id)
    {
        if (!on_owner_strand())
            return AckOutcome::off_strand;
        auto located = locate(child_id);
        if (!located)
            return AckOutcome::duplicate_or_late;
        auto [op_it, child_it] = *located;
        if (child_it->second.state != PublicationState::never_published)
            return AckOutcome::unpublished;
        remove_child(op_it, child_it);
        return AckOutcome::removed;
    }

    /// The exactly-once terminality gate. Only the published
    /// destination's exact (DestinationId, WorkId) acknowledgement
    /// removes the child; everything else is a typed no-op.
    //@@authority transition=provider_lab.ack_terminal owner=provider_lab commit=strand
    AckOutcome
    ack_terminal(ChildId child_id, DestinationId dest, WorkId work)
    {
        if (!on_owner_strand())
            return AckOutcome::off_strand;
        auto located = locate(child_id);
        if (!located)
            return AckOutcome::duplicate_or_late;
        auto [op_it, child_it] = *located;
        auto& c = child_it->second;
        if (c.state != PublicationState::published)
            return AckOutcome::unpublished;
        if (c.destination != dest)
            return AckOutcome::wrong_destination;
        if (c.work != work)
            return AckOutcome::wrong_work;
        remove_child(op_it, child_it);
        return AckOutcome::removed;
    }

    struct TerminalConsumeResult
    {
        AckOutcome outcome{};
        // Fresh on the exactly-once win; the IDENTICAL retained
        // witness when the same ack is re-presented after the win.
        std::optional<InterestTerminalConsumed> witness;
    };

    /// Consume an interest's terminal acknowledgement: phase three of
    /// the terminal handoff. Scope is authenticated FIRST (an ack
    /// from another partition instance refuses before any read); the
    /// retained-witness dedup domain is consulted BEFORE the removal
    /// gate (an exact replay re-derives the identical witness instead
    /// of falling into duplicate_or_late); then the existing
    /// exactly-once gate runs, keyed by the ack's original child and
    /// its published (destination, work). The win removes the child
    /// and mints + RETAINS the InterestTerminalConsumed witness —
    /// the only artifact that can retire the terminal-ready interest.
    /// Witness rows are retained here (bounded by consumed interests);
    /// their reclamation composes with root-close reclamation later
    /// and is stated as an honest gap until then.
    //@@authority transition=provider_lab.ledger_consume_terminal_ack owner=provider_lab commit=strand
    TerminalConsumeResult
    consume_terminal_ack(
        InterestTerminalAck const& ack,
        DestinationId dest,
        WorkId work)
    {
        if (!on_owner_strand())
            return {AckOutcome::off_strand, std::nullopt};
        if (ack.scope() != scope_)
            return {AckOutcome::wrong_scope, std::nullopt};
        if (auto w = witnesses_.find(ack.interest());
            w != witnesses_.end())
        {
            if (w->second.operation() == ack.operation() &&
                w->second.child() == ack.child())
                return {AckOutcome::removed, w->second};  // replay
            return {AckOutcome::wrong_binding, std::nullopt};
        }
        auto located = locate(ack.child());
        if (!located)
            return {AckOutcome::duplicate_or_late, std::nullopt};
        auto [op_it, child_it] = *located;
        if (op_it->first != ack.operation())
            return {AckOutcome::wrong_binding, std::nullopt};
        auto& c = child_it->second;
        if (c.state != PublicationState::published)
            return {AckOutcome::unpublished, std::nullopt};
        if (c.destination != dest)
            return {AckOutcome::wrong_destination, std::nullopt};
        if (c.work != work)
            return {AckOutcome::wrong_work, std::nullopt};
        remove_child(op_it, child_it);
        auto const [w_it, inserted] = witnesses_.emplace(
            ack.interest(),
            InterestTerminalConsumed{
                ack.scope(), ack.interest(), ack.operation(),
                ack.child()});
        return {AckOutcome::removed, w_it->second};
    }

    /// Replay home for the consumption witness: survives loss of the
    /// returned copy and of the ack itself.
    std::optional<InterestTerminalConsumed>
    replay_terminal_consumed(InterestId interest) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto it = witnesses_.find(interest);
        if (it == witnesses_.end())
            return std::nullopt;
        return it->second;
    }

    /// Cancellation/deadline/shutdown may request terminality: the
    /// child's destination capability becomes inert, but the child is
    /// NOT removed — only the destination's matching ack removes it.
    //@@authority transition=provider_lab.request_cancel owner=provider_lab commit=strand
    Refusal
    request_cancel(ChildId child_id)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto located = locate(child_id);
        if (!located)
            return Refusal::cell_not_registered;
        located->second->second.inert = true;
        return Refusal::ok;
    }

    /// Root finish: mark closing (refusing new children). The root row
    /// itself is removed only when the child map is empty.
    //@@authority transition=provider_lab.begin_close owner=provider_lab commit=strand
    Refusal
    begin_close(RootLease const& root)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = root.valid() ? operations_.find(root.id())
                               : operations_.end();
        if (it == operations_.end())
            return Refusal::unknown_root;
        it->second.closing = true;
        maybe_remove_root(it);
        return Refusal::ok;
    }

    // Owner-strand introspection (tests and waiters).
    bool
    root_active(OperationId id) const
    {
        if (!on_owner_strand())
            return false;
        return operations_.count(id) > 0;
    }
    std::size_t
    child_count(OperationId id) const
    {
        if (!on_owner_strand())
            return 0;
        auto it = operations_.find(id);
        return it != operations_.end() ? it->second.children.size() : 0;
    }
    std::optional<PublicationState>
    publication_state(ChildId child_id) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto located = const_cast<OperationLedger*>(this)->locate(child_id);
        if (!located)
            return std::nullopt;
        return located->second->second.state;
    }
    bool
    is_inert(ChildId child_id) const
    {
        if (!on_owner_strand())
            return false;
        auto located = const_cast<OperationLedger*>(this)->locate(child_id);
        return located && located->second->second.inert;
    }

private:
    struct Child
    {
        PublicationState state = PublicationState::registered_not_published;
        DestinationId destination = 0;
        WorkId work = 0;
        bool inert = false;
    };
    struct Operation
    {
        bool closing = false;
        std::map<ChildId, Child> children;
    };

    using OpIter = std::map<OperationId, Operation>::iterator;
    using ChildIter = std::map<ChildId, Child>::iterator;

    Child*
    find_child(ChildLease const& lease)
    {
        if (!lease.valid())
            return nullptr;
        auto located = locate(lease.id());
        return located ? &located->second->second : nullptr;
    }

    std::optional<std::pair<OpIter, ChildIter>>
    locate(ChildId child_id)
    {
        auto idx = child_index_.find(child_id);
        if (idx == child_index_.end())
            return std::nullopt;
        auto op_it = operations_.find(idx->second);
        if (op_it == operations_.end())
            return std::nullopt;
        auto child_it = op_it->second.children.find(child_id);
        if (child_it == op_it->second.children.end())
            return std::nullopt;
        return std::make_pair(op_it, child_it);
    }

    void
    remove_child(OpIter op_it, ChildIter child_it)
    {
        child_index_.erase(child_it->first);
        op_it->second.children.erase(child_it);
        maybe_remove_root(op_it);
    }

    void
    maybe_remove_root(OpIter op_it)
    {
        if (op_it->second.closing && op_it->second.children.empty())
            operations_.erase(op_it);
    }

    //@@authority state=provider_lab.operation_children owner=provider_lab
    strand_type strand_;
    PartitionScope scope_;
    OperationId next_operation_ = 0;
    ChildId next_child_ = 0;
    std::map<OperationId, Operation> operations_;
    std::map<ChildId, OperationId> child_index_;
    std::map<InterestId, InterestTerminalConsumed> witnesses_;
};

}  // namespace catl::provider_lab
