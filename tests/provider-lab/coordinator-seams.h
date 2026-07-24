#pragma once

// Coordinator aggregate: the single transition owner for every state
// change that spans the interest registry, the participant cell
// registry, and the operation ledger's interest-terminal seam.
// TEST-ONLY: production construction must never reference this.
//
// Round-4 rulings folded in (on top of round 3's facade):
//  - the façade OWNS its components. It is constructed from one
//    (strand, scope) pair and constructs all three registries itself,
//    so mismatched scopes or strands are unrepresentable — there is
//    no constructor accepting arbitrary registry references to join;
//  - interest/cell creation is ONE atomic transition
//    (open_interest_with_cell): both ceilings checked, both nodes
//    PREPARED (all allocation), then both committed with
//    allocation-free node inserts that cannot throw. Failure at any
//    step leaves neither an orphan cell nor an orphan interest, and
//    no guessed id ever bridges the two homes;
//  - sponsor_use / finish_use revalidate the SHARED binding — the
//    interest's retained ParticipantDestination against the cell's
//    actual epoch/revision and the pair's fetch/operation — before
//    validating each home, so an interest expecting revision R1 can
//    never spend against a cell at R2;
//  - sponsor_use is exception-atomic: the debt node is prepared
//    before the non-throwing commit (node insert + grant flip).
//    finish_use's commit section is erase+flip, structurally
//    non-throwing;
//  - retire_interest_terminal is the ONLY terminal retirement: it
//    consumes the ledger-minted witness, validates it against BOTH
//    retained homes, and in one non-throwing commit erases the
//    interest row AND releases the ledger's witness replay home.
//    That is the named release protocol for the witness map: rows
//    are bounded by terminal-consumed-but-not-yet-retired interests;
//  - test failpoints (test_hooks) sit between prepare and commit in
//    the two allocating transitions, proving pre-state survival.

#include "coordinator-artifacts.h"
#include "interest-registry.h"
#include "operation-ledger.h"
#include "participant-cell.h"

#include <functional>
#include <limits>
#include <optional>
#include <type_traits>
#include <utility>

namespace catl::provider_lab {

static_assert(
    std::is_same_v<InterestUseId, GrantId>,
    "the seam registers interest debt under the grant identity");

/// One strand, one scope, one owner: first-id parameters are
/// test-only ceiling seams forwarded to the façade's members.
/// (Namespace-scope so it can carry default member initializers into
/// a defaulted constructor argument.)
struct CoordinatorSeamSeeds
{
    InterestId first_interest = 1;
    RevokeCommandId first_command = 1;
    CellRevision first_revision = 1;
    GrantId first_grant = 1;
    OperationId first_operation = 1;
    ChildId first_child = 1;
};

class CoordinatorInterestSeams
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    enum class Refusal : std::uint8_t {
        ok,
        off_strand,
        wrong_scope,       // artifact scope-first refusal
        cell_refuses,      // grant/phase invalid on the cell
        interest_refuses,  // interest missing/sealed or debt state wrong
        ledger_refuses,    // child absent/foreign/already published
        child_already_claimed,  // another interest reserved this child
        binding_mismatch,  // retained expectation != cell's actuality
        unknown_interest,
        not_terminal_ready,
        wrong_witness,  // witness disagrees with a retained home
        interest_id_exhausted,
        revision_exhausted,
    };

    struct OpenResult
    {
        Refusal refusal{};
        InterestId interest = 0;
        CellRevision revision = 0;
    };

    /// Test-only failpoints, called between PREPARE and COMMIT.
    struct TestHooks
    {
        std::function<void()> open_pre_commit;
        std::function<void()> sponsor_pre_commit;
        std::function<void()> retire_pre_commit;
    };

    using Seeds = CoordinatorSeamSeeds;

    CoordinatorInterestSeams(
        strand_type strand,
        PartitionScope scope,
        Seeds seeds = {})
        : scope_(scope)
        , interests_(
              strand, scope, seeds.first_interest, seeds.first_command)
        , cells_(strand, scope, seeds.first_revision, seeds.first_grant)
        , ledger_(
              strand, scope, seeds.first_operation, seeds.first_child)
    {
    }

    bool
    on_owner_strand() const
    {
        return interests_.on_owner_strand();
    }

    PartitionScope
    scope() const
    {
        return scope_;
    }

    // Component access: component-level verbs remain their own
    // authorities; only the pair/seam transitions live here.
    InterestRegistry&
    interests()
    {
        return interests_;
    }
    ParticipantCellRegistry&
    cells()
    {
        return cells_;
    }
    OperationLedger&
    ledger()
    {
        return ledger_;
    }

    /// THE pair mint — now a TRIPLE bind: one interest, its
    /// participant cell, and the exact ledger child it sponsors,
    /// committed all-or-nothing. The child is presented as a genuine
    /// ChildLease; its owning operation is DERIVED from the ledger
    /// (never caller-supplied), it must exist, be unpublished, and be
    /// unclaimed, and the commit reserves it for the newly minted
    /// InterestId. The aggregate thereby owns one exact relation
    /// InterestId <-> cell <-> (OperationId, ChildId), not two pairs
    /// joined by caller convention. Ceilings and ledger refusals are
    /// typed BEFORE any preparation; preparation does all allocation;
    /// the commit is node inserts plus a nothrow reservation flip. A
    /// failpoint throw leaves every home and counter untouched.
    //@@authority transition=provider_lab.seam_open_interest_with_cell owner=provider_lab commit=strand
    OpenResult
    open_interest_with_cell(
        ChildLease const& child,
        InterestFetchId fetch,
        SubscriptionId subscription,
        ProviderEpochId epoch)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, 0, 0};
        auto const operation = ledger_.operation_of(child);
        if (!operation.has_value())
            return {Refusal::ledger_refuses, 0, 0};
        switch (ledger_.can_reserve(child))
        {
            case OperationLedger::Refusal::ok:
                break;
            case OperationLedger::Refusal::interest_reserved:
                return {Refusal::child_already_claimed, 0, 0};
            default:
                return {Refusal::ledger_refuses, 0, 0};
        }
        if (!interests_.interest_mint_available())
            return {Refusal::interest_id_exhausted, 0, 0};
        if (!cells_.revision_mint_available())
            return {Refusal::revision_exhausted, 0, 0};
        auto const id = interests_.peek_interest_id();
        auto const revision = cells_.peek_revision();
        // PREPARE (throwing allowed; no state changed).
        auto interest_node = interests_.prepare_interest(
            id, *operation, child.id(), fetch, subscription,
            ParticipantDestination{scope_, epoch, revision});
        auto cell_node = cells_.prepare_cell(
            id, fetch, *operation, epoch, revision);
        if (test_hooks.open_pre_commit)
            test_hooks.open_pre_commit();
        // COMMIT (non-throwing): two inserts + the reservation flip.
        interests_.commit_interest(std::move(interest_node));
        cells_.commit_cell(std::move(cell_node));
        ledger_.commit_reserve(child, id);
        return {Refusal::ok, id, revision};
    }

    /// Interest publication: flips the reserved child to published.
    /// The InterestId is derived from the retained reservation — no
    /// caller-supplied identity exists on this path, and the store
    /// verb refuses reserved children, so neither variant can be
    /// selected by caller convention.
    //@@authority transition=provider_lab.seam_publish_for_interest owner=provider_lab commit=strand
    OperationLedger::Refusal
    publish_for_interest(
        ChildLease const& child, DestinationId dest, WorkId work)
    {
        return ledger_.publish_reserved(child, dest, work);
    }

    /// Atomic sponsor: grant unspent -> use_open AND the interest debt
    /// registered under the same GrantId. The SHARED binding is
    /// revalidated first — retained expectation vs the cell's actual
    /// epoch/revision and the pair's fetch/operation — then each home,
    /// then prepare-debt / commit-both. A refusal or failpoint throw
    /// leaves the grant unspent and the interest debt-free.
    //@@authority transition=provider_lab.seam_sponsor_use owner=provider_lab commit=strand
    Refusal
    sponsor_use(InterestId interest, GrantId grant)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (auto r = validate_pair_binding(interest); r != Refusal::ok)
            return r;
        if (!cells_.can_spend(interest, grant))
            return Refusal::cell_refuses;
        if (!interests_.can_add_use(interest, grant))
            return Refusal::interest_refuses;
        // PREPARE (throwing): the debt node.
        auto node = interests_.prepare_use(grant);
        if (test_hooks.sponsor_pre_commit)
            test_hooks.sponsor_pre_commit();
        // COMMIT (non-throwing).
        interests_.commit_add_use(interest, std::move(node));
        cells_.commit_spend(interest, grant);
        return Refusal::ok;
    }

    /// Atomic finish: grant use_open -> finished AND the exact
    /// interest debt closed. The commit section is erase + enum flip:
    /// structurally non-throwing, no failpoint needed.
    //@@authority transition=provider_lab.seam_finish_use owner=provider_lab commit=strand
    Refusal
    finish_use(InterestId interest, GrantId grant)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (auto r = validate_pair_binding(interest); r != Refusal::ok)
            return r;
        if (!cells_.can_finish(interest, grant))
            return Refusal::cell_refuses;
        if (!interests_.can_close_use(interest, grant))
            return Refusal::interest_refuses;
        interests_.commit_close_use(interest, grant);
        cells_.commit_finish(interest, grant);
        return Refusal::ok;
    }

    /// THE terminal retirement: the aggregate reconciles EVERY
    /// remaining home before releasing ANY of them. Scope-first, then:
    /// the terminal witness must match the interest's retained ack
    /// outbox AND the ledger's retained witness row; the interest's
    /// RETAINED ParticipantReceiptConsumed (never a caller copy) must
    /// match the participant cell's retained receipt home; the cell
    /// must be drained. Then one non-throwing commit erases the
    /// participant cell, the interest row, and the ledger witness —
    /// all three or, at the failpoint, none. No home is ever left
    /// behind without a replay path, and no caller-held copy is ever
    /// load-bearing. Exactly-once: the second attempt finds no
    /// interest row.
    //@@authority transition=provider_lab.seam_retire_interest_terminal owner=provider_lab commit=strand
    Refusal
    retire_interest_terminal(InterestTerminalConsumed const& witness)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (witness.scope() != scope_)
            return Refusal::wrong_scope;
        auto const* row = interests_.find_row(witness.interest());
        if (!row)
            return Refusal::unknown_interest;
        if (!interests_.row_terminal_ready(*row))
            return Refusal::not_terminal_ready;
        if (!row->terminal_outbox.has_value() ||
            witness.operation() != row->terminal_outbox->operation() ||
            witness.child() != row->terminal_outbox->child())
            return Refusal::wrong_witness;
        auto ledger_home =
            ledger_.replay_terminal_consumed(witness.interest());
        if (!ledger_home.has_value() || !(*ledger_home == witness))
            return Refusal::wrong_witness;
        // The participant homes, derived from RETAINED state only.
        auto const* cell = cells_.find_cell(witness.interest());
        if (!cell || !cells_.cell_drained(witness.interest()))
            return Refusal::cell_refuses;
        if (!row->revoke_consumed.has_value() ||
            !cell->receipt_outbox.has_value() ||
            row->revoke_consumed->command() !=
                cell->receipt_outbox->command() ||
            row->revoke_consumed->epoch() !=
                cell->receipt_outbox->epoch() ||
            row->revoke_consumed->revision() !=
                cell->receipt_outbox->revision())
            return Refusal::wrong_witness;
        if (test_hooks.retire_pre_commit)
            test_hooks.retire_pre_commit();
        // COMMIT (non-throwing): all three erasures, one owner.
        cells_.erase_cell(witness.interest());
        interests_.erase_interest(witness.interest());
        ledger_.release_terminal_witness(witness.interest());
        return Refusal::ok;
    }

    TestHooks test_hooks;

private:
    /// The shared-binding revalidation: the interest's retained
    /// expectation must equal the cell's ACTUAL identity right now —
    /// epoch, revision, fetch, and operation. Scope equality is
    /// structural (one façade, one scope, both members constructed
    /// from it).
    Refusal
    validate_pair_binding(InterestId interest) const
    {
        auto const* row = interests_.find_row(interest);
        if (!row)
            return Refusal::interest_refuses;
        auto const* cell = cells_.find_cell(interest);
        if (!cell)
            return Refusal::cell_refuses;
        if (row->expected.epoch != cell->epoch ||
            row->expected.revision != cell->revision ||
            row->fetch != cell->fetch ||
            row->operation != cell->operation)
            return Refusal::binding_mismatch;
        return Refusal::ok;
    }

    PartitionScope scope_;
    InterestRegistry interests_;
    ParticipantCellRegistry cells_;
    OperationLedger ledger_;
};

}  // namespace catl::provider_lab
