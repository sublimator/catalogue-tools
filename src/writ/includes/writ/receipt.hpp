#pragma once

// writ::receipt — an unforgeable, move-only terminal fact with a closed
// disposition domain.
// schema: writ.core/receipt/v1
//
// Extracted from the peer-client's drain receipts and PeerUseReceiptState.
// Kept deliberately generic per the round-2 constraint: owner, generation,
// and operation identity are typed PAYLOAD (the Tag), never baked-in fields.
//
// The capability model is the Tag itself, and it has THREE load-bearing
// legs:
//
//   construction    a receipt<Tag> can be minted exactly by whoever can
//   is the mint     construct Tag. An owner makes Tag's constructor private
//                   and befriends only its minting site; first-mint forgery
//                   then fails to compile.
//
//   the Tag is      fact() hands out Tag const&. If Tag were copyable, any
//   move-only       receipt holder could copy the Tag back out and re-mint
//                   it with a different disposition — round 4's compiled
//                   counterexample. The static_asserts below reject copyable
//                   Tags outright: possession of a receipt must never grant
//                   the mint capability.
//
//   the population  an enum TYPE is nominal, not a closed enumerator
//   is validated    population: C++ admits static_cast'd values no
//                   enumerator names (round 5's blocker). The Tag therefore
//                   declares its population (is_disposition), the raw
//                   constructor is private, and the only public mint is a
//                   validated factory whose refusal is the uniform
//                   std::nullopt. "Invalid construction is impossible" is a
//                   true sentence, not schema prose — "a type name is a
//                   proof claim, and one legal variant contradicting the
//                   name is a modeling defect."
//
// Honest claim boundary, stated where it can't be missed: this type proves
// UNFORGEABILITY and single-consumption only. Producer totality — that the
// legitimate minter cannot wedge before minting — is census's job; a
// receipt does not imply it ("an unforgeable receipt whose producer can
// wedge is a safe type wrapped around a liveness hole").
//
// receipt_slot is the one-shot consumption home. retain() refuses an
// occupied slot WITHOUT consuming the caller's receipt (a refusal must not
// counterfeit consumption — the same rule as census::retire), and consume()
// exchanges-with-empty BEFORE handing the receipt out, so re-entry observes
// absence regardless of what the consumer does — one-shot observers are
// authority-bearing state, and moving a callable/fact into a local is not a
// consumption transition unless the slot empties first.

#include <concepts>
#include <optional>
#include <type_traits>
#include <utility>

namespace writ {

template <typename Tag>
class receipt
{
public:
    using disposition_type = typename Tag::disposition;

    static_assert(
        std::is_enum_v<disposition_type>,
        "a receipt's disposition domain must be a closed enum declared by "
        "its Tag");
    static_assert(
        requires(disposition_type d) {
            { Tag::is_disposition(d) } noexcept -> std::same_as<bool>;
        },
        "a Tag must declare its closed population — static constexpr bool "
        "is_disposition(disposition) noexcept — because an enum TYPE is "
        "nominal, not a closed enumerator population (static_cast can "
        "conjure values no enumerator names)");
    static_assert(
        std::is_move_constructible_v<Tag>,
        "a receipt transports its Tag by move");
    static_assert(
        !std::is_copy_constructible_v<Tag>,
        "a receipt's Tag must be move-only: a copyable Tag can be copied out "
        "of fact() by a non-friend and re-minted through the public receipt "
        "constructor, forging provenance (round 4's compiled counterexample)");
    static_assert(
        std::is_nothrow_move_constructible_v<Tag>,
        "receipt moves are declared noexcept; a Tag whose move can throw "
        "would make that declaration a lie");

    /// The ONE way to obtain a receipt: possession of a Tag plus a
    /// disposition the Tag's own closed population admits. An out-of-range
    /// cast receives the uniform std::nullopt refusal instead of becoming
    /// an unclassifiable terminal fact.
    [[nodiscard]] static std::optional<receipt>
    mint(Tag fact, disposition_type disposition) noexcept
    {
        if (!Tag::is_disposition(disposition))
            return std::nullopt;
        return receipt(std::move(fact), disposition);
    }

    receipt(receipt&&) noexcept = default;
    receipt&
    operator=(receipt&&) noexcept = default;
    receipt(receipt const&) = delete;
    receipt&
    operator=(receipt const&) = delete;

    Tag const&
    fact() const noexcept
    {
        return fact_;
    }

    disposition_type
    disposition() const noexcept
    {
        return disposition_;
    }

private:
    receipt(Tag fact, disposition_type disposition)
        : fact_(std::move(fact)), disposition_(disposition)
    {
    }

    Tag fact_;
    disposition_type disposition_;
};

/// One-shot consumption slot. Not thread-safe by design: like every writ
/// authority home, confinement is the owner's serialization.
template <typename Tag>
class receipt_slot
{
public:
    /// Retain exactly one receipt; refuses when occupied (the first fact is
    /// latched — a later publication cannot overwrite history's winner). A
    /// refusal moves NOTHING: the caller's receipt is untouched and still
    /// carries its obligation, exactly as census::retire leaves a refused
    /// member intact. Only admission consumes.
    [[nodiscard]] bool
    retain(receipt<Tag>&& r) noexcept
    {
        if (retained_.has_value())
            return false;
        retained_.emplace(std::move(r));
        return true;
    }

    /// Exchange-with-empty, then return: re-entry during consumption sees
    /// absence.
    [[nodiscard]] std::optional<receipt<Tag>>
    consume() noexcept
    {
        return std::exchange(retained_, std::nullopt);
    }

    bool
    occupied() const noexcept
    {
        return retained_.has_value();
    }

private:
    std::optional<receipt<Tag>> retained_;
};

}  // namespace writ
