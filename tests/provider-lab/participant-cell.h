#pragma once

// Coordinator increment 3d, round 3: participant cells —
// coordinator-owned one-shot authority. TEST-ONLY: production
// construction must never reference this.
//
// Round-4 composed rulings folded in (on top of earlier rounds):
//  - SCOPE FIRST, actually: revoke_cell and retire_cell authenticate
//    the artifact's PartitionScope against this registry's immutable
//    scope immediately after the strand guard and BEFORE the interest
//    id touches any lookup. Foreign-scope artifacts get the same
//    wrong_scope whether the local id exists or not;
//  - cell creation is no longer a public verb: a cell exists only as
//    half of an interest/cell pair minted by the aggregate façade's
//    atomic open_interest_with_cell (prepare/commit primitives below;
//    commit is allocation-free and cannot throw). Component tests use
//    the explicit test seam. No public path can create half a pair;
//  - spend/finish remain façade-only (round 3): the façade prepares
//    the interest debt node BEFORE flipping the grant, so the seam
//    commit is exception-atomic;
//  - retirement consumes the INTEREST-minted third-phase witness,
//    never the cell's own outgoing receipt (round 3).

#include "coordinator-artifacts.h"
#include "deterministic-scheduler.h"

#include <cstdint>
#include <limits>
#include <map>
#include <optional>

namespace catl::provider_lab {

using CellRevision = ParticipantRevision;
using GrantId = std::uint64_t;

class ParticipantCellRegistry
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    enum class Refusal : std::uint8_t {
        ok,
        off_strand,
        wrong_scope,  // scope-first: refused before any lookup
        unknown_cell,
        revision_mismatch,
        cell_revoked,
        not_revoked,
        uses_open,
        wrong_binding,
        grant_id_exhausted,
    };

    enum class GrantState : std::uint8_t {
        unspent,
        use_open,
        finished,
        revoked_unspent,
    };

    struct GrantResult
    {
        Refusal refusal{};
        GrantId grant = 0;
    };

    struct RevokeResult
    {
        Refusal refusal{};
        std::optional<ParticipantRevokedReceipt> receipt;  // replayable
    };

    explicit ParticipantCellRegistry(
        strand_type strand,
        PartitionScope scope,
        CellRevision first_minted_revision = 1,
        GrantId first_minted_grant = 1)
        : strand_(std::move(strand))
        , scope_(scope)
        , next_revision_(
              first_minted_revision == 0 ? 1 : first_minted_revision)
        , next_grant_(first_minted_grant == 0 ? 1 : first_minted_grant)
    {
        // Zero reserved for both mints; the first-id parameters are
        // test-only seams so ceiling controls avoid 2^64 warm-ups.
        // Revision/grant uniqueness is scoped to THIS registry
        // instance — cross-instance discrimination comes from the
        // PartitionScope bound into every artifact.
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

    /// Mint a one-shot grant under an ACTIVE cell. The caller proves
    /// it holds the current cell instance by presenting its revision;
    /// the revision gate dominates every other read. Consistent minter
    /// rule: the node is prepared before the allocation-free commit,
    /// and the counter advances only after the insert.
    //@@authority transition=provider_lab.participant_issue_grant owner=provider_lab commit=strand
    GrantResult
    issue_grant(InterestId interest, CellRevision revision)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, 0};
        auto it = cells_.find(interest);
        if (it == cells_.end())
            return {Refusal::unknown_cell, 0};
        auto& c = it->second;
        if (revision != c.revision)
            return {Refusal::revision_mismatch, 0};
        if (c.revoked)
            return {Refusal::cell_revoked, 0};
        if (next_grant_ == std::numeric_limits<GrantId>::max())
            return {Refusal::grant_id_exhausted, 0};
        auto const grant = next_grant_;
        // PREPARE (throwing), then allocation-free COMMIT + counter.
        std::map<GrantId, GrantState> tmp;
        tmp.emplace(grant, GrantState::unspent);
        auto node = tmp.extract(grant);
        c.grants.insert(std::move(node));
        ++next_grant_;
        return {Refusal::ok, grant};
    }

    /// Consume the sender-owned revoke command. SCOPE FIRST: the
    /// command's destination scope is authenticated against this
    /// registry before its interest id touches any lookup. Then the
    /// remaining destination — provider epoch, TARGET REVISION, fetch,
    /// operation — must all match the cell, so a stale-instance (ABA)
    /// command refuses before any state changes. Then, in one
    /// owner-strand commit: mark revoked, kill every UNSPENT grant,
    /// spare open uses, and mint + RETAIN the bound receipt.
    /// Identical replay on the identical command; a different command
    /// against the revoked cell is wrong_binding, never a second mint.
    //@@authority transition=provider_lab.participant_revoke_cell owner=provider_lab commit=strand
    RevokeResult
    revoke_cell(RevokeCommand const& command)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto const dest = command.destination();
        if (dest.scope != scope_)
            return {Refusal::wrong_scope, std::nullopt};
        auto it = cells_.find(command.interest());
        if (it == cells_.end())
            return {Refusal::unknown_cell, std::nullopt};
        auto& c = it->second;
        if (dest.epoch != c.epoch || dest.revision != c.revision ||
            command.fetch() != c.fetch ||
            command.operation() != c.operation)
            return {Refusal::wrong_binding, std::nullopt};
        if (c.receipt_outbox.has_value())
        {
            if (c.receipt_outbox->command() == command.command())
                return {Refusal::ok, c.receipt_outbox};  // replay
            return {Refusal::wrong_binding, std::nullopt};
        }
        c.revoked = true;
        for (auto& [id, state] : c.grants)
        {
            if (state == GrantState::unspent)
                state = GrantState::revoked_unspent;
        }
        c.receipt_outbox = ParticipantRevokedReceipt{
            scope_, c.epoch, command.interest(), command.command(),
            c.revision};
        return {Refusal::ok, c.receipt_outbox};
    }

    // Owner-strand introspection; refusal dominates every read.
    std::optional<CellRevision>
    cell_revision(InterestId interest) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto it = cells_.find(interest);
        if (it == cells_.end())
            return std::nullopt;
        return it->second.revision;
    }

    std::optional<GrantState>
    grant_state(InterestId interest, GrantId grant) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        auto it = cells_.find(interest);
        if (it == cells_.end())
            return std::nullopt;
        auto g = it->second.grants.find(grant);
        if (g == it->second.grants.end())
            return std::nullopt;
        return g->second;
    }

    std::optional<bool>
    cell_present(InterestId interest) const
    {
        if (!on_owner_strand())
            return std::nullopt;
        return cells_.count(interest) > 0;
    }

private:
    struct Cell
    {
        InterestFetchId fetch;
        InterestOperationId operation;
        ProviderEpochId epoch;
        CellRevision revision;
        bool revoked = false;
        std::optional<ParticipantRevokedReceipt> receipt_outbox;
        std::map<GrantId, GrantState> grants;
    };

    using CellMap = std::map<InterestId, Cell>;
    using CellNode = CellMap::node_type;

    // Cell creation, spend/finish, and retirement are committed ONLY
    // by the aggregate seam façade (which owns this registry);
    // component tests reach them through the explicit test seam. NOT
    // public authority: the façade's transitions carry the
    // annotations.
    friend class CoordinatorInterestSeams;
    friend class ParticipantCellTestAccess;

    /// The exactly-once remover: consumes the INTEREST-minted
    /// third-phase witness whose binding matches the retained receipt
    /// outbox, and requires the cell drained (revoked, no open uses).
    /// SCOPE FIRST, before the witness's interest id is used to look
    /// anything up. The cell's own outgoing receipt is not evidence —
    /// it proves production, not consumption — and does not compile
    /// here. Sole production caller: the aggregate's terminal
    /// retirement, which derives the witness from the interest's
    /// RETAINED home and releases every remaining home in one commit.
    Refusal
    retire_cell(ParticipantReceiptConsumed const& witness)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (witness.scope() != scope_)
            return Refusal::wrong_scope;
        auto it = cells_.find(witness.interest());
        if (it == cells_.end())
            return Refusal::unknown_cell;
        auto& c = it->second;
        if (!c.revoked)
            return Refusal::not_revoked;
        if (!c.receipt_outbox.has_value() ||
            witness.epoch() != c.receipt_outbox->epoch() ||
            witness.command() != c.receipt_outbox->command() ||
            witness.revision() != c.receipt_outbox->revision())
            return Refusal::wrong_binding;
        for (auto const& [id, state] : c.grants)
        {
            if (state == GrantState::use_open)
                return Refusal::uses_open;
        }
        cells_.erase(it);
        return Refusal::ok;
    }

    /// Erasure primitive for the aggregate's non-throwing commit
    /// section; every validation happens at the seam first.
    void
    erase_cell(InterestId interest) noexcept
    {
        cells_.erase(interest);
    }

    /// Drain check for the aggregate's final retirement (read-only).
    bool
    cell_drained(InterestId interest) const
    {
        auto it = cells_.find(interest);
        if (it == cells_.end() || !it->second.revoked)
            return false;
        for (auto const& [id, state] : it->second.grants)
        {
            if (state == GrantState::use_open)
                return false;
        }
        return true;
    }

    bool
    revision_mint_available() const
    {
        return next_revision_ !=
            std::numeric_limits<CellRevision>::max();
    }

    CellRevision
    peek_revision() const
    {
        return next_revision_;
    }

    /// PREPARE: all allocation happens here, no state changes.
    CellNode
    prepare_cell(
        InterestId interest,
        InterestFetchId fetch,
        InterestOperationId operation,
        ProviderEpochId epoch,
        CellRevision revision)
    {
        CellMap tmp;
        tmp.emplace(
            interest,
            Cell{
                .fetch = fetch,
                .operation = operation,
                .epoch = epoch,
                .revision = revision});
        return tmp.extract(interest);
    }

    /// COMMIT: allocation-free node insertion; cannot throw. Counter
    /// advances only here.
    void
    commit_cell(CellNode&& node) noexcept
    {
        cells_.insert(std::move(node));
        ++next_revision_;
    }

    Cell const*
    find_cell(InterestId interest) const
    {
        auto it = cells_.find(interest);
        return it == cells_.end() ? nullptr : &it->second;
    }

    bool
    can_spend(InterestId interest, GrantId grant) const
    {
        auto it = cells_.find(interest);
        if (it == cells_.end())
            return false;
        auto g = it->second.grants.find(grant);
        return g != it->second.grants.end() &&
            g->second == GrantState::unspent;
    }

    void
    commit_spend(InterestId interest, GrantId grant) noexcept
    {
        cells_.find(interest)->second.grants.find(grant)->second =
            GrantState::use_open;
    }

    bool
    can_finish(InterestId interest, GrantId grant) const
    {
        auto it = cells_.find(interest);
        if (it == cells_.end())
            return false;
        auto g = it->second.grants.find(grant);
        return g != it->second.grants.end() &&
            g->second == GrantState::use_open;
    }

    void
    commit_finish(InterestId interest, GrantId grant) noexcept
    {
        cells_.find(interest)->second.grants.find(grant)->second =
            GrantState::finished;
    }

    strand_type strand_;
    PartitionScope scope_;
    CellRevision next_revision_;
    GrantId next_grant_;
    //@@authority state=provider_lab.participant_cells owner=provider_lab
    CellMap cells_;
};

/// Test-only cell creation for COMPONENT tests of this registry
/// alone. Production cells exist only as half of the pair minted by
/// the façade's atomic open_interest_with_cell. Explicitly not
/// production authority.
class ParticipantCellTestAccess
{
public:
    struct Result
    {
        bool ok = false;
        CellRevision revision = 0;
    };

    static Result
    open(
        ParticipantCellRegistry& reg,
        InterestId interest,
        InterestFetchId fetch,
        InterestOperationId operation,
        ProviderEpochId epoch)
    {
        if (!reg.on_owner_strand() || !reg.revision_mint_available() ||
            reg.cells_.count(interest))
            return {};
        auto const revision = reg.peek_revision();
        reg.commit_cell(reg.prepare_cell(
            interest, fetch, operation, epoch, revision));
        return {true, revision};
    }

    static ParticipantCellRegistry::Refusal
    retire(
        ParticipantCellRegistry& reg,
        ParticipantReceiptConsumed const& witness)
    {
        return reg.retire_cell(witness);
    }
};

}  // namespace catl::provider_lab
