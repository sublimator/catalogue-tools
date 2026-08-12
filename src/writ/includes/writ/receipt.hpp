#pragma once

// writ::receipt — an unforgeable, move-only terminal fact with a closed
// disposition domain.
//
// Extracted from the peer-client's drain receipts and PeerUseReceiptState.
// Kept deliberately generic per the round-2 constraint: owner, generation,
// and operation identity are typed PAYLOAD (the Tag), never baked-in fields.
//
// The capability model is the Tag itself: a receipt<Tag> can be constructed
// exactly by whoever can construct Tag. An owner makes Tag's constructor
// private and befriends only its minting site; forgery then fails to
// compile. The disposition domain is Tag-declared and closed (an enum whose
// invalid construction is impossible) — "a type name is a proof claim, and
// one legal variant contradicting the name is a modeling defect."
//
// Honest claim boundary, stated where it can't be missed: this type proves
// UNFORGEABILITY and single-consumption only. Producer totality — that the
// legitimate minter cannot wedge before minting — is census's job; a
// receipt does not imply it ("an unforgeable receipt whose producer can
// wedge is a safe type wrapped around a liveness hole").
//
// receipt_slot is the one-shot consumption home. It exchanges-with-empty
// BEFORE handing the receipt out, so re-entry observes absence regardless
// of what the consumer does — one-shot observers are authority-bearing
// state, and moving a callable/fact into a local is not a consumption
// transition unless the slot empties first.

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

    receipt(Tag fact, disposition_type disposition)
        : fact_(std::move(fact)), disposition_(disposition)
    {
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
    /// latched — a later publication cannot overwrite history's winner).
    [[nodiscard]] bool
    retain(receipt<Tag> r) noexcept
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
