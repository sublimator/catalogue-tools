#pragma once

// TARGET-ONLY. Nothing in production construction may reference this.
//
// The first target-stack increment after the C11 preflight refused C11: an
// EMPTY-OPERATION LIFECYCLE. Every home it mints is consumed exactly, it admits no
// uses, it exposes no PeerUse, and it touches neither NodeCache nor the live proof
// route. It exists to make an operation open and close honestly before anything is
// allowed to happen inside one.
//
// PACKAGING, because the first attempt got this wrong
// ---------------------------------------------------
// This lives in the peer-client module and the `catl::peer_client` namespace even
// though its canonical owner is `ledger_data_control`. The first cut gave it a new
// top-level CMake library and public namespace, and that conflated three separate
// boundaries: the authority/model owner, the internal C++ component, and the
// repository package. The model requires the first and says nothing about the third.
//
//   **Authority decomposition does not determine package decomposition.**
//
// A package boundary needs an independent build, API, reuse or deployment reason.
// There is none here, and a new module would have put target-only code in the
// production tree as a component that must never be constructed. The owner and
// strand boundary below is real and preserved; only its packaging changed.
//
// Canonical records this implements (authgraph.py show):
//
//   T50  ledger_data_control  S01.running + S04.consumed_same_epoch + S10
//                               -> produces S30                       [pre_fence]
//   T53  ledger_data_control  S30.open -> produces S31, S35           [pre_fence]
//   T54  operation_admission_cell
//                             S31 + S35 + S21.zero + S22.zero
//                               -> consumes S31, S35; retains S30     [terminal]
//   T52  ledger_data_control  S30 + S31.zero + S35.zero
//                               -> consumes S30
//                               -> produces Q01.operations, ...       [terminal]
//
// WHAT THIS SLICE DELIBERATELY DOES NOT CLAIM, and why
// ----------------------------------------------------
// There are TWO runtime owners in the future protocol, not three:
//
//   ledger_data_control  owns S30 and S31
//   peer_set             owns S35, S21 and S22
//
// An earlier version of this comment named `operation_admission_cell` as a third
// owner because canonical lists it as T54's owner. Independent audit established it
// is a model WART, not an executor: canonical's registry declares the Control and
// PeerSet executors and not that cell, later reconciliation says to remove it because
// it names a SEAM rather than an executor, and it is already an `unregistered_owner`
// baseline blocker. It is not memorialized here as an actor.
//
// The raw T51/T53/T54/T52 family is therefore mixed-owner model debt and needs a
// staged split before any of it can be implemented as written.
//
// S35 is not an open choice: it must wait for the PeerSet-owned lease protocol. A
// slice with no PeerSet participant cannot mint peer_set's homes without fabricating
// the authority this cut exists to refuse.
//
// This primitive is therefore explicitly UNBOUND: it carries no `v4=` reference and
// is NOT an implementation of T50/T52. It represents none of S01/S04/S10, publishes
// neither Q01 witness, and collapses S31 (ledger_data_control) and S35 (peer_set) —
// two separately owned facts — into one local counter. The first version claimed
// T50/T52 and the conformance gate stayed green, because that gate verifies declared
// LABELS, not semantic coverage. It cannot become T50/T52 by later relabelling; it
// becomes them only when the required homes, witnesses and cross-owner boundary
// exist.
//
// T53 and T54 are absent, not stubbed, because a stub that returns success is a
// fabricated fact and a stub that returns failure is a control that can never go
// green.
//
// `close_control_root()` requires the operation count to be zero — true by
// construction here, and exactly the guard that must keep working once operations can
// be minted, which is why it is strand-serialized rather than merely flagged.
//
// CLAIM AND PROJECTION ARE SEPARATE STATE
// ---------------------------------------
// Two earlier versions failed here in the same way. The first loaded, minted, then
// stored. The second claimed with a compare-exchange first — which merely MOVED the
// split commit, because the claimed bit was also the externally readable projection,
// so an off-strand reader could observe an open root with no identity behind it.
//
// One bit cannot be both the owner's internal claim on a transition and the committed
// fact the world reads. They are now different state:
//
//   phase_            owner-local, strand-confined: closed -> opening -> open
//   open_projection_  externally readable, published ONLY at the commit
//
//   closed -> opening          claim, not published
//   mint identity              may still refuse; rolls the phase back
//   commit: identity, then publish the projection      (no-throw)
//   opening -> open
//
// A re-entrant open while `opening` is refused as `open_in_progress`, distinct from
// `already_open` because the root does not exist yet. A re-entrant close while
// `opening` is refused for the same reason: it cannot consume a root that has not
// committed. Close retracts the projection BEFORE clearing the identity, or the
// symmetric window opens on the way out.

#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>

/// Declared at GLOBAL scope so the friendships below name THIS class. An
/// UNQUALIFIED friend declaration inside the namespace names
/// `catl::peer_client::OperationLifecycleTestAccess` instead — a different,
/// never-defined class — and the real seam stays locked out. Hence the leading `::`.
class OperationLifecycleTestAccess;

namespace catl::peer_client {

namespace asio = boost::asio;

/// Non-reused control-root identity. Zero is reserved as "no root", so a default
/// value can never be mistaken for a minted one.
class ControlRootId
{
public:
    constexpr ControlRootId() noexcept = default;

    constexpr std::uint64_t
    value() const noexcept
    {
        return value_;
    }

    constexpr explicit operator bool() const noexcept
    {
        return value_ != 0;
    }

    friend constexpr bool
    operator==(ControlRootId lhs, ControlRootId rhs) noexcept
    {
        return lhs.value_ == rhs.value_;
    }

private:
    // Only the lifecycle mints one. A test cannot forge an identity and then
    // present it as evidence the production minter produced it.
    friend class OperationLifecycle;
    friend class ::OperationLifecycleTestAccess;

    constexpr explicit ControlRootId(std::uint64_t value) noexcept : value_(value)
    {
    }

    std::uint64_t value_ = 0;
};

/// The control root's lifecycle, S30, and nothing else yet.
///
/// OWNER: `ledger_data_control` on its control strand. That is a real boundary —
/// one strand owns every mutation, there is no second writer, and the public
/// surface returns projections rather than references so a caller cannot reach past
/// it. It is simply not a package boundary.
class OperationLifecycle : public std::enable_shared_from_this<OperationLifecycle>
{
public:
    /// T50 disposition. Every refusal is named; there is no bare bool, because
    /// "false" merges "already open" with "the minter is exhausted" and those need
    /// different responses from a caller.
    enum class Open
    {
        opened,
        already_open,
        /// A transition is in flight on this strand: claimed, not yet committed.
        /// Distinct from `already_open` because there is no root to be already open
        /// — reporting that would name a fact that does not exist yet.
        open_in_progress,
        /// The root minter reached its ceiling. Absorbing: once exhausted it stays
        /// exhausted, and it never wraps back through the reserved zero.
        root_exhausted,
    };

    /// T52 disposition.
    enum class Close
    {
        closed,
        /// No root to close. Distinct from a second success on purpose: closing
        /// something that never opened is a caller error, closing twice is a race.
        not_open,
        /// An open is claimed but not committed. A close cannot consume a root that
        /// does not exist yet, and saying `not_open` would be a different claim.
        open_in_progress,
        /// S31.zero / S35.zero is violated. The whole point of the empty lifecycle
        /// is that this guard is what makes closing legitimate.
        operations_outstanding,
    };

    static std::shared_ptr<OperationLifecycle> create(asio::io_context& io);

    /// Mint the control root. **MUST run on `strand()`**; it asserts, and aborts
    /// under XPRV_STRAND_FAIL_FAST.
    ///
    /// The first version called itself "a pure function of one conserved atomic
    /// word". It was not: load, mint, store are three operations, so two callers
    /// could both pass the check, both mint, and race the non-atomic identity write,
    /// each returning `opened`. That prose was copied from the coordinator's ingress,
    /// which genuinely IS one word carrying count and seal bit — the mechanism was
    /// not copied with it. A single compare-exchange now CLAIMS the transition first.
    Open
    open_control_root();

    /// Consume the control root. **MUST run on `strand()`.**
    ///
    /// Strand confinement, not the flag, is what keeps this correct once T53 can mint
    /// an operation: the zero-check and the close must be serialized against that
    /// mint, or the race simply moves one phase later.
    Close
    close_control_root();

    /// The owner executor. Callers enter it; this class never posts on their behalf,
    /// so there is exactly one place ownership can be violated and it asserts there.
    asio::strand<asio::io_context::executor_type> const&
    strand() const noexcept
    {
        return strand_;
    }

    bool
    control_root_open() const noexcept;

    std::uint64_t
    operations_outstanding() const noexcept;

    /// True when an invariant this class is supposed to maintain has been observed
    /// broken. A test that ends with this set has not passed, whatever else it
    /// asserted.
    bool
    accounting_broken() const noexcept;

private:
    friend class ::OperationLifecycleTestAccess;

    explicit OperationLifecycle(asio::io_context& io);

    /// Process-wide, never reused, absorbing at the ceiling — the same rule as the
    /// coordinator's sender generation, and for the same reason: an identity whose
    /// only job is to be distinct across incarnations must never re-issue a value.
    static std::atomic<std::uint64_t> next_root_id_;

    asio::strand<asio::io_context::executor_type> strand_;

    /// Owner-local transition phase. NOT externally readable: this is the claim,
    /// and publishing a claim is what the last two versions did wrong.
    enum class Phase : std::uint8_t
    {
        closed,
        opening,
        open,
    };
    Phase phase_ = Phase::closed;

    ControlRootId root_;

    /// One conserved word: the count of live operation homes. Increment 1 never
    /// raises it, and the guard that reads it is the thing this slice is proving.
    std::atomic<std::uint64_t> operations_{0};
    /// The COMMITTED projection, and the only thing `control_root_open()` reads.
    /// Released at the commit, after the identity exists; acquired by readers, so a
    /// reader that sees true also sees the identity behind it.
    std::atomic<bool> open_projection_{false};
    std::atomic<bool> accounting_broken_{false};

    /// Default-inert failpoint fired inside `open_control_root()` AFTER the claim and
    /// before it returns. A control re-enters the lifecycle from here, which is the
    /// deterministic form of "a second caller arrives mid-transition" — no threads,
    /// no timing, no hoping the scheduler cooperates.
    std::function<void()> before_commit_for_test_;
};

}  // namespace catl::peer_client
