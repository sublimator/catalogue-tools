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
// S35, S21 and S22 are all owned by `peer_set` (authgraph.py show S35/S21/S22).
// T54 consumes S35 and requires S21.zero and S22.zero. So the full four-transition
// lifecycle spans THREE owners — ledger_data_control, operation_admission_cell, and
// peer_set — and a slice with no PeerSet participant cannot mint peer_set's homes
// without fabricating the authority this cut exists to refuse.
//
// Therefore this increment implements the two transitions that are unambiguously
// ledger_data_control's, T50 and T52, with T52's zero-guard modelled as a real
// refusal rather than an assumption. T53 and T54 are shaped for but not implemented:
// they are absent, not stubbed, because a stub that returns success is a fabricated
// fact and a stub that returns failure is a control that can never go green.
//
// The open question, put to review with this pin: whether the admission home S35 may
// be represented at all before a PeerSet participant exists, or whether T53/T54 must
// wait for one. Until that is answered, `close_control_root()` requires the operation
// count to be zero — true by construction here, and exactly the guard that must keep
// working once operations can be minted.

#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

#include <atomic>
#include <cstdint>
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
        /// S31.zero / S35.zero is violated. The whole point of the empty lifecycle
        /// is that this guard is what makes closing legitimate.
        operations_outstanding,
    };

    static std::shared_ptr<OperationLifecycle> create(asio::io_context& io);

    /// T50. Mint the control root. Synchronous disposition on the caller's thread:
    /// the decision is a pure function of one conserved atomic word, so a caller
    /// learns the outcome without a hop, and no identity is published by this call.
    Open
    open_control_root();

    /// T52. Consume the control root. Refuses while any operation home is
    /// outstanding, which is the S31.zero / S35.zero requirement.
    Close
    close_control_root();

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

    ControlRootId root_;

    /// One conserved word: the count of live operation homes. Increment 1 never
    /// raises it, and the guard that reads it is the thing this slice is proving.
    std::atomic<std::uint64_t> operations_{0};
    std::atomic<bool> root_open_{false};
    std::atomic<bool> accounting_broken_{false};
};

}  // namespace catl::peer_client
