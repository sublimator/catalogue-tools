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
//
// Round-4 composed rulings folded in:
//  - operation/child minters FAIL CLOSED at their ceilings with typed
//    exhaustion refusals (the wrap class, closed here too); zero stays
//    reserved; test-only first-id ctor seams;
//  - publication records a CLOSED terminal-consumer variant: a row is
//    either store-acknowledged (raw ack_terminal) or interest-bound
//    three-phase (publish_for_interest records the exact InterestId).
//    Raw ack_terminal REFUSES interest-bound rows; consume_terminal_ack
//    REFUSES non-interest rows and requires the exact retained
//    InterestId. The old gate cannot accept the new variant, so the
//    protocol is structural, not conventional;
//  - consume_terminal_ack is exception-atomic: the witness node is
//    PREPARED (all allocation) before the non-throwing commit section
//    (node insert + child removal), with a test failpoint between.
//    Scope is authenticated first; the witness dedup domain is
//    consulted before the removal gate;
//  - the witness home has a NAMED release: the aggregate façade's
//    atomic retire_interest_terminal erases the interest row and this
//    ledger's witness row in one commit (release_child_terminal_witness,
//    façade-only). Bound: witness rows <= interests whose child is
//    terminal but which await aggregate retirement — a hard bound
//    over live protocol state, not process lifetime.

#include "coordinator-artifacts.h"
#include "deterministic-scheduler.h"

#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
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
///
/// A lease is bound to the exact LEDGER INCARNATION that minted it:
/// it carries an opaque owner token (a shared control block whose
/// address cannot alias any other live or reachable incarnation,
/// because the lease itself keeps the block alive). Numeric ids are
/// instance-local and DO collide across ledgers; the token, not the
/// id and not the PartitionScope, is the authority identity. Every
/// lease-accepting verb validates the token before any id lookup.
template <typename Tag>
class Lease
{
public:
    Lease() = default;

    Lease(Lease&& other) noexcept
        : id_(other.id_)
        , owner_(std::move(other.owner_))
        , valid_(std::exchange(other.valid_, false))
    {
    }
    Lease&
    operator=(Lease&& other) noexcept
    {
        id_ = other.id_;
        owner_ = std::move(other.owner_);
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
    Lease(std::uint64_t id, std::shared_ptr<void const> owner)
        : id_(id), owner_(std::move(owner)), valid_(true)
    {
    }

    std::uint64_t id_ = 0;
    std::shared_ptr<void const> owner_;
    bool valid_ = false;
};

class OperationLedger;
class CoordinatorInterestSeams;

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
        wrong_scope,     // artifact from another partition instance
        wrong_binding,   // ack's operation/interest does not own the row
        interest_bound,  // raw gate refused: row needs the 3-phase path
        not_interest_bound,  // consume refused: row is store-terminal
    };

    enum class Refusal : std::uint8_t {
        ok,
        root_closing,
        unknown_root,
        off_strand,
        cell_not_registered,
        already_flipped,
        already_never_published,
        cancelled_inert,  // pre-publication cancel: seal never_published
        operation_id_exhausted,
        child_id_exhausted,
        interest_reserved,  // store verbs refuse an interest-bound child
        not_reserved,       // interest publication needs a reservation
        wrong_ledger,  // lease minted by another ledger incarnation
    };

    struct RootResult
    {
        Refusal refusal{};
        RootLease lease;
    };

    explicit OperationLedger(
        strand_type strand,
        PartitionScope scope,
        OperationId first_minted_operation = 1,
        ChildId first_minted_child = 1)
        : strand_(std::move(strand))
        , scope_(scope)
        , next_operation_(
              first_minted_operation == 0 ? 1 : first_minted_operation)
        , next_child_(first_minted_child == 0 ? 1 : first_minted_child)
    {
        // Zero reserved for both mints; first-id parameters are
        // test-only seams so ceiling controls avoid 2^64 warm-ups.
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    PartitionScope
    scope() const
    {
        return scope_;
    }

    /// Incarnation authentication: a VALID lease minted by any other
    /// ledger incarnation is foreign, whatever its numeric id.
    /// Checked before any id lookup in every lease-accepting verb.
    template <typename Tag>
    bool
    foreign(Lease<Tag> const& lease) const
    {
        return lease.valid() && lease.owner_.get() != incarnation_.get();
    }

    /// T50-analog: mint one root operation row. Fail-closed at the id
    /// ceiling; prepare-then-commit (the node is built before any
    /// state or counter mutation, and the commit is allocation-free).
    //@@authority transition=provider_lab.open_root owner=provider_lab commit=strand
    RootResult
    open_root()
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, RootLease{}};
        if (next_operation_ == std::numeric_limits<OperationId>::max())
            return {Refusal::operation_id_exhausted, RootLease{}};
        auto const id = next_operation_;
        // PREPARE (throwing): the operation node.
        std::map<OperationId, Operation> tmp;
        tmp.emplace(id, Operation{});
        auto node = tmp.extract(id);
        if (test_hooks.open_root_pre_commit)
            test_hooks.open_root_pre_commit();
        // COMMIT (non-throwing): insert, then advance the counter.
        operations_.insert(std::move(node));
        ++next_operation_;
        return {Refusal::ok, RootLease(id, incarnation_)};
    }

    /// Register one child under a live, non-closing root. The returned
    /// lease's ChildId is never reused (fail-closed at the ceiling).
    /// The child row and its index entry are TWO homes: both nodes are
    /// prepared before either commits, the commit is allocation-free,
    /// and the counter advances only after both inserts — a throw at
    /// any preparation point leaves row, index, and counter mutually
    /// unchanged.
    //@@authority transition=provider_lab.register_child owner=provider_lab commit=strand
    std::pair<Refusal, ChildLease>
    register_child(RootLease const& root)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, ChildLease{}};
        if (foreign(root))
            return {Refusal::wrong_ledger, ChildLease{}};
        auto it = root.valid() ? operations_.find(root.id())
                               : operations_.end();
        if (it == operations_.end())
            return {Refusal::unknown_root, ChildLease{}};
        if (it->second.closing)
            return {Refusal::root_closing, ChildLease{}};
        if (next_child_ == std::numeric_limits<ChildId>::max())
            return {Refusal::child_id_exhausted, ChildLease{}};
        auto const child_id = next_child_;
        // PREPARE (throwing): both nodes, no state changed.
        std::map<ChildId, Child> tmp_child;
        tmp_child.emplace(child_id, Child{});
        auto child_node = tmp_child.extract(child_id);
        std::map<ChildId, OperationId> tmp_index;
        tmp_index.emplace(child_id, root.id());
        auto index_node = tmp_index.extract(child_id);
        if (test_hooks.register_child_pre_commit)
            test_hooks.register_child_pre_commit();
        // COMMIT (non-throwing): both inserts, then the counter.
        it->second.children.insert(std::move(child_node));
        child_index_.insert(std::move(index_node));
        ++next_child_;
        return {Refusal::ok, ChildLease(child_id, incarnation_)};
    }

    /// Level-triggered flip to the STORE-terminal variant: raw
    /// ack_terminal is this row's remover; consume_terminal_ack will
    /// refuse it. A child RESERVED for an interest refuses the store
    /// variant outright — the closed-variant choice is made at
    /// reservation, never by publication-time caller convention.
    //@@authority transition=provider_lab.publish_child owner=provider_lab commit=strand
    Refusal
    publish(ChildLease const& child, DestinationId dest, WorkId work)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (foreign(child))
            return Refusal::wrong_ledger;
        auto* c = find_child(child);
        if (!c)
            return Refusal::cell_not_registered;
        if (c->interest_binding.has_value())
            return Refusal::interest_reserved;
        return publish_impl(*c, dest, work);
    }

    /// Enqueue failure: registered_not_published -> never_published,
    /// permitting exactly one sealed never-published acknowledgement.
    //@@authority transition=provider_lab.mark_never_published owner=provider_lab commit=strand lifecycle=coordinator_3c_3d
    Refusal
    mark_never_published(ChildLease const& child)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (foreign(child))
            return Refusal::wrong_ledger;
        auto* c = find_child(child);
        if (!c)
            return Refusal::cell_not_registered;
        if (c->state == PublicationState::never_published)
            return Refusal::already_never_published;
        if (c->state != PublicationState::registered_not_published)
            return Refusal::already_flipped;
        c->state = PublicationState::never_published;
        return Refusal::ok;
    }

    /// Consume the sealed never-published ack: removes the child.
    /// Exactly-once; a second call is duplicate_or_late. An
    /// INTEREST-RESERVED child that fails enqueue is still that
    /// interest's original child, so this path mints and retains the
    /// same InterestChildTerminalWitness witness the published path would
    /// — otherwise the never-published gate would be a second bypass
    /// that destroys the terminal obligation with no witness, and the
    /// interest could never retire. Witness node prepared before the
    /// non-throwing removal.
    //@@authority transition=provider_lab.ack_never_published owner=provider_lab commit=strand lifecycle=coordinator_3c_3d
    AckOutcome
    ack_never_published(ChildId child_id)
    {
        if (!on_owner_strand())
            return AckOutcome::off_strand;
        auto located = locate(child_id);
        if (!located)
            return AckOutcome::duplicate_or_late;
        auto [op_it, child_it] = *located;
        auto const& c = child_it->second;
        if (c.state != PublicationState::never_published)
            return AckOutcome::unpublished;
        if (c.interest_binding.has_value())
        {
            // PREPARE (throwing): the witness node — with the HONEST
            // disposition: terminality is established here WITHOUT
            // consuming an interest terminal acknowledgement. (One
            // may legitimately exist later — the interest's
            // terminal_join mints into its retained outbox and
            // aggregate retirement reconciles it there; this witness
            // simply never claims to have consumed it.)
            WitnessMap tmp;
            tmp.emplace(
                *c.interest_binding,
                InterestChildTerminalWitness{
                    scope_, *c.interest_binding, op_it->first, child_id,
                    TerminalDisposition::never_published});
            auto node = tmp.extract(*c.interest_binding);
            // COMMIT (non-throwing).
            witnesses_.insert(std::move(node));
        }
        remove_child(op_it, child_it);
        return AckOutcome::removed;
    }

    /// The exactly-once terminality gate for STORE-terminal rows.
    /// Only the published destination's exact (DestinationId, WorkId)
    /// acknowledgement removes the child; an interest-bound row is
    /// REFUSED here — its remover is the three-phase consume below,
    /// and this refusal is what makes the protocol structural.
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
        if (c.interest_binding.has_value())
            return AckOutcome::interest_bound;
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
        std::optional<InterestChildTerminalWitness> witness;
    };

    /// Consume an interest's terminal acknowledgement: phase three of
    /// the terminal handoff, for INTEREST-BOUND rows only. Order:
    /// strand guard; SCOPE authenticated before any lookup; the
    /// retained-witness dedup domain before the removal gate; then
    /// the exactly-once gate, which additionally requires the row's
    /// retained InterestId to equal the ack's — an ack from a
    /// different interest in the same scope cannot steal the child.
    /// The win is exception-atomic: the witness node is PREPARED
    /// (allocation) before the non-throwing commit (node insert +
    /// child removal); `test_pre_commit_hook` is a test-only
    /// failpoint proving pre-state survives a throw at the boundary.
    //@@authority transition=provider_lab.ledger_consume_terminal_ack owner=provider_lab commit=strand lifecycle=coordinator_3c_3d
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
            if (w->second.operation() != ack.operation() ||
                w->second.child() != ack.child())
                return {AckOutcome::wrong_binding, std::nullopt};
            // never_published means terminality was established
            // WITHOUT consuming an acknowledgement: this verb must
            // not report one as consumed, whatever exists elsewhere.
            if (w->second.disposition() ==
                TerminalDisposition::never_published)
                return {AckOutcome::unpublished, std::nullopt};
            return {AckOutcome::removed, w->second};  // ack replay
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
        if (!c.interest_binding.has_value())
            return {AckOutcome::not_interest_bound, std::nullopt};
        if (*c.interest_binding != ack.interest())
            return {AckOutcome::wrong_binding, std::nullopt};
        if (c.destination != dest)
            return {AckOutcome::wrong_destination, std::nullopt};
        if (c.work != work)
            return {AckOutcome::wrong_work, std::nullopt};

        // PREPARE (throwing): build the witness node off to the side.
        WitnessMap tmp;
        tmp.emplace(
            ack.interest(),
            InterestChildTerminalWitness{
                ack.scope(), ack.interest(), ack.operation(),
                ack.child(),
                TerminalDisposition::terminal_ack_consumed});
        auto node = tmp.extract(ack.interest());
        if (test_hooks.consume_pre_commit)
            test_hooks.consume_pre_commit();
        // COMMIT (non-throwing): node insert + erasures.
        auto const w_it = witnesses_.insert(std::move(node)).position;
        remove_child(op_it, child_it);
        return {AckOutcome::removed, w_it->second};
    }

    /// Replay home for the child-terminal witness: survives loss of the
    /// returned copy and of the ack itself. Released only by the
    /// façade's atomic retire_interest_terminal.
    std::optional<InterestChildTerminalWitness>
    replay_child_terminal_witness(InterestId interest) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto it = witnesses_.find(interest);
        if (it == witnesses_.end())
            return std::nullopt;
        return it->second;
    }

    /// Cancellation/deadline/shutdown atomically select the terminal
    /// path. Before publication, cancellation seals never_published so
    /// no later caller must remember a second flip. After publication,
    /// the destination capability becomes inert, but the child is NOT
    /// removed — only the matching destination terminal path removes it.
    //@@authority transition=provider_lab.request_cancel owner=provider_lab commit=strand
    Refusal
    request_cancel(ChildId child_id)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto located = locate(child_id);
        if (!located)
            return Refusal::cell_not_registered;
        auto& child = located->second->second;
        child.inert = true;
        if (child.state == PublicationState::registered_not_published)
            child.state = PublicationState::never_published;
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
        if (foreign(root))
            return Refusal::wrong_ledger;
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

    /// TEST-ONLY failpoints, called between prepare and commit in the
    /// allocating transitions. Empty in production shapes.
    struct TestHooks
    {
        std::function<void()> open_root_pre_commit;
        std::function<void()> register_child_pre_commit;
        std::function<void()> consume_pre_commit;
    };
    TestHooks test_hooks;

private:
    struct Child
    {
        PublicationState state = PublicationState::registered_not_published;
        DestinationId destination = 0;
        WorkId work = 0;
        std::optional<InterestId> interest_binding;  // closed variant
        bool inert = false;
    };
    struct Operation
    {
        bool closing = false;
        std::map<ChildId, Child> children;
    };

    using OpIter = std::map<OperationId, Operation>::iterator;
    using ChildIter = std::map<ChildId, Child>::iterator;
    using WitnessMap = std::map<InterestId, InterestChildTerminalWitness>;

    // Witness release, child reservation, and interest publication are
    // owned by the aggregate façade — never public verbs. Component
    // tests reach reservation/publication through the explicit
    // test-only seam.
    friend class CoordinatorInterestSeams;
    friend class OperationLedgerTestAccess;

    void
    release_child_terminal_witness(InterestId interest) noexcept
    {
        witnesses_.erase(interest);
    }

    /// Reservation validation: the lease must be OURS, and the child
    /// must exist, be unpublished, and be unclaimed. Read-only.
    Refusal
    can_reserve(ChildLease const& child) const
    {
        if (foreign(child))
            return Refusal::wrong_ledger;
        auto* c = const_cast<OperationLedger*>(this)->find_child(child);
        if (!c)
            return Refusal::cell_not_registered;
        if (c->state != PublicationState::registered_not_published)
            return Refusal::already_flipped;
        if (c->interest_binding.has_value())
            return Refusal::interest_reserved;
        return Refusal::ok;
    }

    /// Reservation commit: a nothrow flip on the existing row. Part of
    /// the façade's atomic pair-mint commit section.
    void
    commit_reserve(ChildLease const& child, InterestId interest) noexcept
    {
        find_child(child)->interest_binding = interest;
    }

    /// Interest publication: the InterestId is DERIVED from the
    /// retained reservation — no caller can supply one. Refuses an
    /// unreserved child; the store verb refuses a reserved one, so the
    /// closed variant is chosen exactly once, at reservation.
    Refusal
    publish_reserved(
        ChildLease const& child, DestinationId dest, WorkId work)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (foreign(child))
            return Refusal::wrong_ledger;
        auto* c = find_child(child);
        if (!c)
            return Refusal::cell_not_registered;
        if (!c->interest_binding.has_value())
            return Refusal::not_reserved;
        return publish_impl(*c, dest, work);
    }

    /// Locate the owning operation of a lease's child (read-only).
    /// Foreign leases resolve to nothing: the id is never consulted.
    std::optional<OperationId>
    operation_of(ChildLease const& child) const
    {
        if (!child.valid() || foreign(child))
            return std::nullopt;
        auto it = child_index_.find(child.id());
        if (it == child_index_.end())
            return std::nullopt;
        return it->second;
    }

    Refusal
    publish_impl(Child& c, DestinationId dest, WorkId work)
    {
        if (c.inert && c.state == PublicationState::never_published)
            return Refusal::cancelled_inert;
        if (c.state != PublicationState::registered_not_published)
            return Refusal::already_flipped;
        c.state = PublicationState::published;
        c.destination = dest;
        c.work = work;
        return Refusal::ok;
    }

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

    //@@authority state=provider_lab.operation_children owner=provider_lab lifecycle=coordinator_3c_3d
    strand_type strand_;
    PartitionScope scope_;
    // The incarnation token: an opaque control block whose address is
    // unique among all incarnations any live lease can name (leases
    // pin their minter's block, so it can never be freed and reused
    // while a lease survives). PartitionScope stays diagnostic only.
    std::shared_ptr<void const> incarnation_ = std::make_shared<int>(0);
    OperationId next_operation_;
    ChildId next_child_;
    std::map<OperationId, Operation> operations_;
    std::map<ChildId, OperationId> child_index_;
    WitnessMap witnesses_;
};

/// Test-only access to the façade-owned reservation/publication seam,
/// so the ledger's COMPONENT suite can exercise its variant gates
/// without composing the full aggregate. Explicitly not production
/// authority.
class OperationLedgerTestAccess
{
public:
    static OperationLedger::Refusal
    reserve(OperationLedger& l, ChildLease const& child, InterestId id)
    {
        if (!l.on_owner_strand())
            return OperationLedger::Refusal::off_strand;
        auto r = l.can_reserve(child);
        if (r == OperationLedger::Refusal::ok)
            l.commit_reserve(child, id);
        return r;
    }

    static OperationLedger::Refusal
    publish_reserved(
        OperationLedger& l,
        ChildLease const& child,
        DestinationId dest,
        WorkId work)
    {
        return l.publish_reserved(child, dest, work);
    }
};

}  // namespace catl::provider_lab
