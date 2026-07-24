#pragma once

// Coordinator increment 3d: participant cells — coordinator-owned
// one-shot authority. TEST-ONLY: production construction must never
// reference this.
//
// Shape (accepted coordinator design, increment 3d):
//  - a cell is revision + active/revoked + one-shot grants;
//  - spend is exactly-once: unspent -> use_open -> finished, and a
//    spent (submitted) use retains the ORIGINAL cell revision until
//    finish — revocation never invalidates in-flight work;
//  - revoke atomically marks the cell revoked and every UNSPENT grant
//    revoked, in one owner-strand commit;
//  - revision mismatch refuses BEFORE any grant-state read: identity
//    checks dominate reads of reclaimable state.
//
// 3c seam obligations carried here:
//  - this registry is the SOLE PRODUCTION MINTER of
//    ParticipantRevokedReceipt: revoke_cell consumes the sender-owned
//    RevokeCommand (full destination authentication: interest, fetch,
//    AND operation must match the cell — a receipt must authenticate
//    where the command was addressed, not merely where a receiver
//    says it committed), mints the bound receipt ONCE, and RETAINS it
//    for identical replay — the witness survives the fact it attests;
//  - retirement mirrors 3c's retire_terminal: only possession of the
//    genuinely minted receipt (unforgeable type, matching binding)
//    plus a fully drained cell removes the row, exactly once. The
//    replay home cannot be destroyed by a caller that never held the
//    witness;
//  - revision and grant ids fail closed at their ceilings (the
//    wrap/false-agreement class, refused at its fifth and sixth
//    habitats) with zero reserved and test-only first-id seams.

#include "deterministic-scheduler.h"
#include "interest-registry.h"

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
        unknown_grant,
        not_spent,
        already_spent,
        already_finished,
        grant_revoked,
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
        CellRevision first_minted_revision = 1,
        GrantId first_minted_grant = 1)
        : strand_(std::move(strand))
        , next_revision_(
              first_minted_revision == 0 ? 1 : first_minted_revision)
        , next_grant_(first_minted_grant == 0 ? 1 : first_minted_grant)
    {
        // Zero reserved for both mints; the first-id parameters are
        // test-only seams so ceiling controls avoid 2^64 warm-ups.
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    /// Open the cell for one interest's participation in one fetch,
    /// minting a fresh revision. Revisions are minted from ONE global
    /// monotonic counter, so no two cell instances — across interests
    /// or across a retire/reopen — ever share a revision. Fail-closed
    /// at the ceiling: nothing minted, nothing mutated.
    //@@authority transition=provider_lab.participant_open_cell owner=provider_lab commit=strand
    OpenResult
    open_cell(
        InterestId interest,
        InterestFetchId fetch,
        InterestOperationId operation)
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
            Cell{.fetch = fetch,
                 .operation = operation,
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

    /// Spend exactly once: unspent -> use_open. The submitted use
    /// retains the original cell revision until finish; a revoked
    /// UNSPENT grant refuses — revocation killed it atomically.
    //@@authority transition=provider_lab.participant_spend_grant owner=provider_lab commit=strand
    Refusal
    spend_grant(InterestId interest, CellRevision revision, GrantId grant)
    {
        return mutate_grant(
            interest, revision, grant, [](GrantState& s) {
                switch (s)
                {
                    case GrantState::unspent:
                        s = GrantState::use_open;
                        return Refusal::ok;
                    case GrantState::use_open:
                        return Refusal::already_spent;
                    case GrantState::finished:
                        return Refusal::already_finished;
                    case GrantState::revoked_unspent:
                        return Refusal::grant_revoked;
                }
                return Refusal::unknown_grant;
            });
    }

    /// Finish a submitted use: use_open -> finished. LEGAL while the
    /// cell is revoked — detach-then-finish and finish-then-detach
    /// must both converge (in-flight work drains under retained
    /// authority, never ownerless).
    //@@authority transition=provider_lab.participant_finish_use owner=provider_lab commit=strand
    Refusal
    finish_use(InterestId interest, CellRevision revision, GrantId grant)
    {
        return mutate_grant(
            interest, revision, grant, [](GrantState& s) {
                switch (s)
                {
                    case GrantState::use_open:
                        s = GrantState::finished;
                        return Refusal::ok;
                    case GrantState::unspent:
                        return Refusal::not_spent;
                    case GrantState::finished:
                        return Refusal::already_finished;
                    case GrantState::revoked_unspent:
                        return Refusal::grant_revoked;
                }
                return Refusal::unknown_grant;
            });
    }

    /// THE 3c seam: consume the sender-owned revoke command and, in
    /// one owner-strand commit, mark the cell revoked, kill every
    /// unspent grant, and mint the bound ParticipantRevokedReceipt —
    /// retained for identical replay. Destination authentication
    /// first: the command's interest, fetch, AND operation must all
    /// match this cell; a command addressed elsewhere refuses before
    /// any state changes. An identical replay returns the IDENTICAL
    /// retained receipt; a different command against a revoked cell is
    /// wrong_binding, never a second mint.
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
        if (command.fetch() != c.fetch ||
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
            command.interest(), command.command(), c.revision};
        return {Refusal::ok, c.receipt_outbox};
    }

    /// The exactly-once remover, mirroring 3c's retire_terminal:
    /// consumes a genuinely minted receipt (possession is evidence —
    /// the type is compile-closed against forgery) whose binding
    /// matches the retained outbox, and requires the cell drained
    /// (revoked, no open uses). The replay home cannot be destroyed
    /// by a caller that never held the witness.
    //@@authority transition=provider_lab.participant_retire_cell owner=provider_lab commit=strand
    Refusal
    retire_cell(ParticipantRevokedReceipt const& receipt)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = cells_.find(receipt.interest());
        if (it == cells_.end())
            return Refusal::unknown_cell;
        auto& c = it->second;
        if (!c.revoked)
            return Refusal::not_revoked;
        if (!c.receipt_outbox.has_value() ||
            !(*c.receipt_outbox == receipt))
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
        CellRevision revision;
        bool revoked = false;
        std::optional<ParticipantRevokedReceipt> receipt_outbox;
        std::map<GrantId, GrantState> grants;
    };

    template <typename Fn>
    Refusal
    mutate_grant(
        InterestId interest,
        CellRevision revision,
        GrantId grant,
        Fn fn)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        auto it = cells_.find(interest);
        if (it == cells_.end())
            return Refusal::unknown_cell;
        auto& c = it->second;
        if (revision != c.revision)
            return Refusal::revision_mismatch;  // before any grant read
        auto g = c.grants.find(grant);
        if (g == c.grants.end())
            return Refusal::unknown_grant;
        return fn(g->second);
    }

    strand_type strand_;
    CellRevision next_revision_;
    GrantId next_grant_;
    //@@authority state=provider_lab.participant_cells owner=provider_lab
    std::map<InterestId, Cell> cells_;
};

}  // namespace catl::provider_lab
