#pragma once

// writ::exact — non-reused incarnation identity.
// schema: writ.core/exact/v1
//
// Extracted from the peer-client's connection-generation discipline: endpoint
// strings (or any logical key) cannot distinguish an old incarnation's
// delayed death from the incarnation currently admitted, so instance identity
// is minted, never inferred. Two live registries mint colliding ids and a
// recreated owner restarts its counters, so a handle presented across an
// incarnation boundary must refuse rather than alias.
//
// Four rules from the modeling lessons are load-bearing here:
//
//   scope-first     validation happens against the owner's CURRENT
//                   incarnation, presented by the owner. A stale handle
//                   receives the identical refusal whether the object is
//                   dead, replaced, or foreign — presence is never leaked.
//
//   incarnation is  the identity is the exact (object, generation) pair.
//   not a key       The same object re-admitted under a new generation is a
//                   different incarnation; the old handle refuses.
//
//   counter honesty generations are never reused; exhaustion is a sticky
//                   refusal, never a wrap. (The last representable value is
//                   minted once; the mint then refuses forever.)
//
//   owner identity  a generation names its MINT as well as its sequence: it
//   is unforgeable  carries the mint's process-unique epoch, and only a mint
//                   can construct a nonzero generation. Two owners holding
//                   the same object at the same local sequence therefore
//                   refuse one another's refs, and a restored owner (fresh
//                   epoch, resumed sequence) refuses every predecessor ref.
//                   The fossil is PeerSet's peer_set_instance_id_, checked
//                   before any generation comparison. The epoch's identity
//                   domain is one process; refs are not serializable, so a
//                   handle can never cross a process boundary to collide.
//
// This is identity mechanics only. The owner's population map, admission,
// and drain live in later nouns (census, scope); nothing here keeps an
// object alive or claims to.

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

namespace writ {

class mint;

namespace detail {

/// Process-unique mint epochs (the fossil: mint_peer_set_instance_id).
/// Exhaustion returns zero — a refusal the mint keeps for its lifetime, so a
/// mint born after 2^64-1 predecessors refuses every next() instead of
/// aliasing another owner's epoch space.
inline std::uint64_t
mint_epoch() noexcept
{
    static std::atomic<std::uint64_t> next{0};
    auto current = next.load(std::memory_order_relaxed);
    while (current != std::numeric_limits<std::uint64_t>::max())
    {
        if (next.compare_exchange_weak(
                current,
                current + 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed))
        {
            return current + 1;
        }
    }
    return 0;
}

}  // namespace detail

/// A minted, non-reused incarnation number: the owning mint's process-unique
/// epoch plus that mint's local sequence. Zero in either half is "none" — the
/// refusal value — and is never minted. Only a mint can construct a nonzero
/// generation, so identity cannot be invented, only received from an owner.
class generation
{
public:
    generation() = default;

    std::uint64_t
    epoch() const noexcept
    {
        return epoch_;
    }

    std::uint64_t
    value() const noexcept
    {
        return value_;
    }

    explicit
    operator bool() const noexcept
    {
        return epoch_ != 0 && value_ != 0;
    }

    friend auto
    operator<=>(generation, generation) = default;

private:
    friend class mint;

    generation(std::uint64_t epoch, std::uint64_t value) noexcept
        : epoch_(epoch), value_(value)
    {
    }

    std::uint64_t epoch_ = 0;
    std::uint64_t value_ = 0;
};

/// A monotonic, non-reused generation mint. Confinement is the owner's
/// serialization (a home); the mint itself is deliberately not thread-safe
/// (its epoch allocation is the one cross-thread step, and it happens once,
/// at construction).
///
/// A mint is an owner capability and is therefore neither copyable NOR
/// movable: a copy would carry the same epoch and sequence and mint
/// identical generations — duplicated identity authority, the exact thing
/// the epoch exists to prevent — and a move would need a destructive
/// moved-from state to avoid the same aliasing. The only owner-recreation
/// path is the restore seam, which is a NEW mint (fresh epoch) on purpose.
class mint
{
public:
    /// A fresh owner: fresh process-unique epoch, sequence from zero.
    mint() = default;

    /// Restore seam: resume the SEQUENCE after `last` (persisted owners,
    /// tests). The epoch is deliberately fresh — a recreated owner is a new
    /// incarnation boundary, and every predecessor ref must refuse against
    /// it even where the restored sequence would collide.
    explicit mint(std::uint64_t last) noexcept : last_(last)
    {
    }

    mint(mint const&) = delete;
    mint&
    operator=(mint const&) = delete;
    mint(mint&&) = delete;
    mint&
    operator=(mint&&) = delete;

    /// The next generation, or the zero refusal on exhaustion. Refusal is
    /// sticky: once the last representable sequence has been minted (or the
    /// process's epoch space was already exhausted at construction), every
    /// later call refuses.
    [[nodiscard]] generation
    next() noexcept
    {
        if (epoch_ == 0 || last_ == std::numeric_limits<std::uint64_t>::max())
            return {};
        return generation{epoch_, ++last_};
    }

    std::uint64_t
    epoch() const noexcept
    {
        return epoch_;
    }

    std::uint64_t
    last() const noexcept
    {
        return last_;
    }

private:
    std::uint64_t epoch_ = detail::mint_epoch();
    std::uint64_t last_ = 0;
};

template <typename T>
class exact_ref;

/// Owner-held: the one current incarnation of T under this owner. Replacing
/// the incarnation (a new object, or the same object under a fresh
/// generation) invalidates every previously issued ref. The generation an
/// incarnation carries can only have come from a mint; reusing one minted
/// generation for two different objects is owner misuse, but even then the
/// exact (object, generation) pair is what refs compare, so no ref can be
/// retargeted across objects by it.
template <typename T>
class incarnation
{
public:
    incarnation() = default;

    incarnation(std::shared_ptr<T> object, generation g)
        : object_(std::move(object)), generation_(g)
    {
    }

    std::shared_ptr<T> const&
    object() const noexcept
    {
        return object_;
    }

    generation
    gen() const noexcept
    {
        return generation_;
    }

    explicit
    operator bool() const noexcept
    {
        return object_ != nullptr && static_cast<bool>(generation_);
    }

    /// An inert handle naming exactly this incarnation.
    [[nodiscard]] exact_ref<T>
    ref() const noexcept;

private:
    std::shared_ptr<T> object_;
    generation generation_{};
};

/// Inert handle: names one exact incarnation, keeps nothing alive, carries
/// no authority. The only question it answers is asked scope-first, against
/// the owner's current incarnation.
template <typename T>
class exact_ref
{
public:
    exact_ref() = default;

    generation
    gen() const noexcept
    {
        return generation_;
    }

    /// The object, only when this ref names the CURRENT incarnation exactly:
    /// same object identity and same generation — where generation now means
    /// epoch AND sequence, so a foreign owner's colliding sequence refuses.
    /// Dead, replaced, foreign, and never-issued all receive the identical
    /// nullptr.
    [[nodiscard]] std::shared_ptr<T>
    lock_exact(incarnation<T> const& current) const noexcept
    {
        auto object = identity_.lock();
        if (!object || object != current.object() || !generation_ ||
            generation_ != current.gen())
        {
            return nullptr;
        }
        return object;
    }

private:
    friend class incarnation<T>;

    exact_ref(std::weak_ptr<T> identity, generation g)
        : identity_(std::move(identity)), generation_(g)
    {
    }

    std::weak_ptr<T> identity_;
    generation generation_{};
};

template <typename T>
exact_ref<T>
incarnation<T>::ref() const noexcept
{
    return exact_ref<T>(object_, generation_);
}

}  // namespace writ
