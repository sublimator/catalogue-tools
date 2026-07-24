#pragma once

// Coordinator seam façade: the single transition owner for state
// changes that span the interest registry and the participant cell
// registry. TEST-ONLY: production construction must never reference
// this.
//
// Composed-review ruling folded in: grant spend and interest debt
// registration were two independent public commits, so an
// interruption between them could terminal-join an interest while the
// participant still owned an open use (or strand an undischargeable
// debt), and raw-id use_terminal let any caller clear a debt the
// participant never finished. Both state homes live on ONE owner
// strand with ONE transition owner — this façade. Each seam verb
// validates BOTH sides first and then commits BOTH in the same
// owner-strand call: there is no observable intermediate state, and
// the underlying single-home mutators are private to the registries
// (friend-gated), so the split orders are inexpressible, not merely
// discouraged.
//
// The grant/use identity is ONE typed identity here: the seam
// registers the interest debt under the GrantId itself
// (InterestUseId == GrantId by this façade's construction, asserted
// below). No caller supplies a second identifier that could alias or
// diverge.

#include "coordinator-artifacts.h"
#include "interest-registry.h"
#include "participant-cell.h"

#include <type_traits>

namespace catl::provider_lab {

static_assert(
    std::is_same_v<InterestUseId, GrantId>,
    "the seam registers interest debt under the grant identity");

class CoordinatorInterestSeams
{
public:
    enum class Refusal : std::uint8_t {
        ok,
        off_strand,
        cell_refuses,      // grant/revision/phase invalid on the cell
        interest_refuses,  // interest missing/sealed or debt state wrong
    };

    /// Both registries must be owned by the SAME strand: one strand,
    /// one transition owner, genuinely atomic seam commits.
    CoordinatorInterestSeams(
        InterestRegistry& interests,
        ParticipantCellRegistry& cells)
        : interests_(interests), cells_(cells)
    {
    }

    bool
    on_owner_strand() const
    {
        return interests_.on_owner_strand() && cells_.on_owner_strand();
    }

    /// Atomic sponsor: grant unspent -> use_open AND the interest
    /// debt registered under the same GrantId, in one owner-strand
    /// commit. Validates both sides BEFORE mutating either — a
    /// refusal leaves the grant unspent and the interest debt-free.
    //@@authority transition=provider_lab.seam_sponsor_use owner=provider_lab commit=strand
    Refusal
    sponsor_use(InterestId interest, CellRevision revision, GrantId grant)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (!cells_.can_spend(interest, revision, grant))
            return Refusal::cell_refuses;
        if (!interests_.can_add_use(interest, grant))
            return Refusal::interest_refuses;
        cells_.commit_spend(interest, grant);
        interests_.commit_add_use(interest, grant);
        return Refusal::ok;
    }

    /// Atomic finish: grant use_open -> finished AND the exact
    /// interest debt closed, in one owner-strand commit. The interest
    /// debt can be closed by NO other path (raw-id use_terminal does
    /// not exist), so a debt can never be cleared before the
    /// participant finished, and a finish can never leave the debt
    /// behind.
    //@@authority transition=provider_lab.seam_finish_use owner=provider_lab commit=strand
    Refusal
    finish_use(InterestId interest, CellRevision revision, GrantId grant)
    {
        if (!on_owner_strand())
            return Refusal::off_strand;
        if (!cells_.can_finish(interest, revision, grant))
            return Refusal::cell_refuses;
        if (!interests_.can_close_use(interest, grant))
            return Refusal::interest_refuses;
        cells_.commit_finish(interest, grant);
        interests_.commit_close_use(interest, grant);
        return Refusal::ok;
    }

private:
    InterestRegistry& interests_;
    ParticipantCellRegistry& cells_;
};

}  // namespace catl::provider_lab
