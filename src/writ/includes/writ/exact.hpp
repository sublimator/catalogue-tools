#pragma once

// writ::exact — non-reused incarnation identity.
//
// Extracted from the peer-client's connection-generation discipline: endpoint
// strings (or any logical key) cannot distinguish an old incarnation's
// delayed death from the incarnation currently admitted, so instance identity
// is minted, never inferred. Two live registries mint colliding ids and a
// recreated owner restarts its counters, so a handle presented across an
// incarnation boundary must refuse rather than alias.
//
// Three rules from the modeling lessons are load-bearing here:
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
// This is identity mechanics only. The owner's population map, admission,
// and drain live in later nouns (census, scope); nothing here keeps an
// object alive or claims to.

#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

namespace writ {

/// A minted, non-reused incarnation number. Zero is "none" — the refusal
/// value — and is never minted.
struct generation
{
    std::uint64_t value = 0;

    explicit
    operator bool() const noexcept
    {
        return value != 0;
    }

    friend auto
    operator<=>(generation, generation) = default;
};

/// A monotonic, non-reused generation mint. Confinement is the owner's
/// serialization (a home); the mint itself is deliberately not thread-safe.
class mint
{
public:
    mint() = default;

    /// Restore seam: resume minting after `last` (persisted owners, tests).
    explicit mint(std::uint64_t last) noexcept : last_(last)
    {
    }

    /// The next generation, or the zero refusal on exhaustion. Refusal is
    /// sticky: once the last representable value has been minted, every
    /// later call refuses.
    [[nodiscard]] generation
    next() noexcept
    {
        if (last_ == std::numeric_limits<std::uint64_t>::max())
            return {};
        return generation{++last_};
    }

    std::uint64_t
    last() const noexcept
    {
        return last_;
    }

private:
    std::uint64_t last_ = 0;
};

template <typename T>
class exact_ref;

/// Owner-held: the one current incarnation of T under this owner. Replacing
/// the incarnation (a new object, or the same object under a fresh
/// generation) invalidates every previously issued ref.
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
    /// same object identity and same generation. Dead, replaced, foreign,
    /// and never-issued all receive the identical nullptr.
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
