#pragma once

// Coordinator increment 3d, round 2: participant cells —
// coordinator-owned one-shot authority. TEST-ONLY: production
// construction must never reference this.
//
// Composed-review rulings folded in:
//  - the registry carries its PartitionScope and every cell binds
//    (scope, epoch, interest, fetch, operation, revision). revoke_cell
//    compares the COMPLETE destination — including the target
//    revision — before any other read, so a delayed command minted
//    for a retired instance (ABA) and any artifact from a different
//    registry instance with colliding local counters refuse without
//    effect. Counter uniqueness is scoped to THIS registry instance
//    and stated as such;
//  - retirement consumes the INTEREST-minted third-phase witness
//    (ParticipantReceiptConsumed), never the cell's own outgoing
//    receipt: possession of your own outbox message proves
//    production, not downstream consumption. The receipt replay home
//    therefore survives until the interest provably consumed it;
//  - grant spend and use finish are NOT public verbs: they are
//    committed solely by the coordinator seam façade that owns both
//    state homes on one strand (split-atomic spend/register is
//    inexpressible). issue_grant remains cell-local authority.

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
        unknown_cell,
        already_open,
        revision_mismatch,
        cell_revoked,
        not_revoked,
        uses_open,
        wrong_binding,
        revision_exhausted,
        grant_id_exhausted,
    };

    enum class GrantState : std::uint8_t {
        unspent,
        use_open,
        finished,
        revoked_unspent,
    };

    struct OpenResult
    {
        Refusal refusal{};
        CellRevision revision = 0;
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

    /// Open the cell for one interest's participation in one fetch,
    /// under one provider epoch, minting a fresh revision (unique
    /// within this registry). Fail-closed at the ceiling.
    //@@authority transition=provider_lab.participant_open_cell owner=provider_lab commit=strand
    OpenResult
    open_cell(
        InterestId interest,
        InterestFetchId fetch,
        InterestOperationId operation,
        ProviderEpochId epoch)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, 0};
        if (cells_.count(interest))
            return {Refusal::already_open, 0};
        if (next_revision_ == std::numeric_limits<CellRevision>::max())
            return {Refusal::revision_exhausted, 0};
        auto const revision = next_revision_++;
        cells_.emplace(
            interest,
            Cell{
                .fetch = fetch,
                .operation = operation,
                .epoch = epoch,
                .revision = revision});
        return {Refusal::ok, revision};
    }

    /// Mint a one-shot grant under an ACTIVE cell. The caller proves
    /// it holds the current cell instance by presenting its revision;
    /// the revision gate dominates every other read.
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
        auto const grant = next_grant_++;
        c.grants.emplace(grant, GrantState::unspent);
        return {Refusal::ok, grant};
    }

    /// Consume the sender-owned revoke command. Destination
    /// authentication FIRST and COMPLETE: partition scope, provider
    /// epoch, fetch, operation, and target revision must all match
    /// this cell — a stale-instance (ABA) or cross-registry command
    /// refuses before any state changes. Then, in one owner-strand
    /// commit: mark revoked, kill every UNSPENT grant, spare open
    /// uses, and mint + RETAIN the bound receipt. Identical replay on
    /// the identical command; a different command against the revoked
    /// cell is wrong_binding, never a second mint.
    //@@authority transition=provider_lab.participant_revoke_cell owner=provider_lab commit=strand
    RevokeResult
    revoke_cell(RevokeCommand const& command)
    {
        if (!on_owner_strand())
            return {Refusal::off_strand, std::nullopt};
        auto it = cells_.find(command.interest());
        if (it == cells_.end())
            return {Refusal::unknown_cell, std::nullopt};
        auto& c = it->second;
        auto const dest = command.destination();
        if (dest.scope != scope_ || dest.epoch != c.epoch ||
            dest.revision != c.revision ||
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

    /// The exactly-once remover: consumes the INTEREST-minted
    /// third-phase witness whose binding matches the retained receipt
    /// outbox, and requires the cell drained (revoked, no open uses).
    /// The cell's own outgoing receipt is not evidence — it proves
    /// production, not consumption — and it does not compile here.
    //@@authority transition=provider_lab.participant_retire_cell owner=provider_lab commit=strand
    Refusal
    retire_cell(ParticipantReceiptConsumed const& witness)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = cells_.find(witness.interest());
        if (it == cells_.end())
            return Refusal::unknown_cell;
        auto& c = it->second;
        if (!c.revoked)
            return Refusal::not_revoked;
        if (!c.receipt_outbox.has_value() ||
            witness.scope() != c.receipt_outbox->scope() ||
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

    // Spend/finish are committed ONLY by the seam façade, which owns
    // both state homes on this strand. NOT public authority: the
    // façade's transitions carry the annotations.
    friend class CoordinatorInterestSeams;

    bool
    can_spend(InterestId interest, CellRevision revision, GrantId grant)
        const
    {
        auto it = cells_.find(interest);
        if (it == cells_.end() || revision != it->second.revision)
            return false;
        auto g = it->second.grants.find(grant);
        return g != it->second.grants.end() &&
            g->second == GrantState::unspent;
    }

    void
    commit_spend(InterestId interest, GrantId grant)
    {
        cells_.at(interest).grants.at(grant) = GrantState::use_open;
    }

    bool
    can_finish(InterestId interest, CellRevision revision, GrantId grant)
        const
    {
        auto it = cells_.find(interest);
        if (it == cells_.end() || revision != it->second.revision)
            return false;
        auto g = it->second.grants.find(grant);
        return g != it->second.grants.end() &&
            g->second == GrantState::use_open;
    }

    void
    commit_finish(InterestId interest, GrantId grant)
    {
        cells_.at(interest).grants.at(grant) = GrantState::finished;
    }

    strand_type strand_;
    PartitionScope scope_;
    CellRevision next_revision_;
    GrantId next_grant_;
    //@@authority state=provider_lab.participant_cells owner=provider_lab
    std::map<InterestId, Cell> cells_;
};

}  // namespace catl::provider_lab
