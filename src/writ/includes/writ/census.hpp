#pragma once

// writ::census — an admitted population with sealed admission and a
// received (never inferred) drain.
// schema: writ.core/census/v1
//
// Extracted from the peer-client's servant censuses (`in_flight_`,
// `crawl_in_flight_`, `draining_generations_`) and co_stop's drain loops.
// The rules, each with a named counterexample in the corpus:
//
//   admit before      registration is the ONLY way to enter the population.
//   launch            A launch that fails synchronously, or a body that
//                     inherited cancellation and never started, still holds
//                     a member and still retires it — "never started" is a
//                     caller-side disposition, not an excuse to vanish.
//
//   sealed admission  seal() fences every later admit. A census can only be
//                     judged against a closed population.
//
//   drain is a        drained() is sealed ∧ zero live ∧ zero leaked. An
//   conjunction       empty map before seal is a photograph, not quiescence
//                     ("zero is a conjunction over every authority-bearing
//                     home, not the size of the largest visible container").
//
//   a leak wedges     a member dropped without retirement is recorded and
//   VISIBLY, even     drained() stays false forever. That wedge is the
//   through teardown  feature: the fossil shutdown waits on unaccounted
//                     obligations silently; a census makes the wedge
//                     attributable. The leak FACT (stderr line, fail-fast
//                     abort, sticky counter) commits on the DROPPING thread,
//                     before any executor is involved — a post to a stopped
//                     or destroyed executor can destroy its handler without
//                     running it, so only live-row bookkeeping rides the
//                     post and the wedge itself cannot be lost to teardown
//                     (round 4: the previous shape posted the whole commit
//                     and lost the leak exactly on the shutdown edge it
//                     existed for).
//
// Member identities come from a writ::mint, so identity exhaustion is a
// sticky admission refusal, never a wrap through zero.
//
// The drained wake is a writ::retained_signal — completed exactly when the
// census drains, retained for late joiners (quiescence is received from its
// owner). Drained is absorbing, so one signal generation suffices.
//
// All operations are home-confined except two, stated where they happen:
// the member destructor's leak commit (any thread), and leaked(), which
// reads the cross-thread counter and stays legal even after the home's
// executor is gone.
//
// Implementation note (roster/v1 cut): the seal/leak/drain/wake machinery
// lives in the shared detail::obligation_state engine; the row store below
// — one map<Key, id>, the duplicate-live-key law, key-first retirement
// with the exact (key, id) match — is census's own and unchanged. The
// complete pre-refit conformance suite is the stationarity gate.

#include <writ/detail/obligation-lifecycle.hpp>
#include <writ/exact.hpp>
#include <writ/home.hpp>
#include <writ/retained-signal.hpp>

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace writ {

namespace detail {

/// Census's fixed private row store: the current map<Key, id>, verbatim.
/// The live row itself is the dedup state.
template <typename Key>
struct census_store
{
    std::map<Key, std::uint64_t> live;

    bool
    empty() const noexcept
    {
        return live.empty();
    }

    std::size_t
    size() const noexcept
    {
        return live.size();
    }

    void
    erase_leaked(Key const& key, std::uint64_t const id) noexcept
    {
        auto const it = live.find(key);
        if (it != live.end() && it->second == id)
            live.erase(it);
    }
};

}  // namespace detail

template <typename Key>
class census
{
    static_assert(
        std::is_nothrow_move_constructible_v<Key> &&
            std::is_nothrow_move_assignable_v<Key>,
        "census member tokens move their Key inside noexcept move operations "
        "and the noexcept leak path; a throwing-move Key would make those "
        "declarations lies");

    using state = detail::obligation_state<detail::census_store<Key>>;

public:
    /// A move-only obligation token. Retire it exactly once through its
    /// census; dropping it is a recorded leak.
    class member
    {
    public:
        member() = default;

        member(member&& other) noexcept
            : census_(std::move(other.census_))
            , key_(std::move(other.key_))
            , id_(std::exchange(other.id_, 0))
        {
            other.census_.reset();
        }

        member&
        operator=(member&& other) noexcept
        {
            if (this != &other)
            {
                abandon();
                census_ = std::move(other.census_);
                key_ = std::move(other.key_);
                id_ = std::exchange(other.id_, 0);
                other.census_.reset();
            }
            return *this;
        }

        member(member const&) = delete;
        member&
        operator=(member const&) = delete;

        ~member()
        {
            abandon();
        }

        explicit
        operator bool() const noexcept
        {
            return id_ != 0;
        }

        Key const&
        key() const noexcept
        {
            return key_;
        }

    private:
        friend class census;

        member(std::weak_ptr<state> census, Key key, std::uint64_t id)
            : census_(std::move(census)), key_(std::move(key)), id_(id)
        {
        }

        void
        defuse() noexcept
        {
            id_ = 0;
            census_.reset();
        }

        void
        abandon() noexcept
        {
            if (id_ == 0)
                return;
            auto const id = std::exchange(id_, 0);
            detail::abandon_obligation(
                census_,
                id,
                "WRIT-CENSUS-LEAK",
                " outlived census",
                [key = std::move(key_), id](state& s) noexcept {
                    s.rows.erase_leaked(key, id);
                });
            census_.reset();
        }

        std::weak_ptr<state> census_;
        Key key_;
        std::uint64_t id_ = 0;
    };

    explicit census(home h) : state_(std::make_shared<state>(std::move(h)))
    {
    }

    census(census const&) = delete;
    census&
    operator=(census const&) = delete;
    census(census&&) noexcept = default;
    census&
    operator=(census&&) noexcept = default;

    /// On the home. Refuses after seal, refuses a duplicate LIVE key (the
    /// row itself is the dedup state), and refuses on member-identity
    /// exhaustion (the mint's sticky refusal). A retired key may be
    /// re-admitted; chained stages that must avoid a transient zero use
    /// distinct member identities.
    [[nodiscard]] std::optional<member>
    admit(Key key)
    {
        auto& s = *state_;
        WRIT_ASSERT_HOME(s.owner);
        if (s.sealed || s.rows.live.contains(key))
            return std::nullopt;
        auto const id = s.member_mint.next();
        if (!id)
            return std::nullopt;
        s.rows.live.emplace(key, id.value());
        return member(state_, std::move(key), id.value());
    }

    /// On the home. Fences every later admit; an already-empty census
    /// drains (and completes its wake) immediately.
    void
    seal() noexcept
    {
        auto& s = *state_;
        WRIT_ASSERT_HOME(s.owner);
        s.sealed = true;
        s.complete_if_drained();
    }

    /// On the home. The outcome is the closed retire_outcome population:
    /// `refused` (foreign or empty member) does NOT consume — the token
    /// stays live in the caller's hands, because a refusal must never
    /// counterfeit consumption; `retired` consumed the obligation with the
    /// census still owing others; `drained` consumed it AND completed the
    /// census.
    retire_outcome
    retire(member&& m) noexcept
    {
        auto& s = *state_;
        WRIT_ASSERT_HOME(s.owner);
        if (m.id_ == 0 || m.census_.lock() != state_)
            return retire_outcome::refused;
        auto const it = s.rows.live.find(m.key_);
        if (it == s.rows.live.end() || it->second != m.id_)
            return retire_outcome::refused;
        s.rows.live.erase(it);
        m.defuse();
        s.complete_if_drained();
        return s.drained() ? retire_outcome::drained : retire_outcome::retired;
    }

    // ── observations ─────────────────────────────────────────────────

    std::size_t
    live() const noexcept
    {
        WRIT_ASSERT_HOME(state_->owner);
        return state_->rows.size();
    }

    /// The sticky leak count. Unlike every other observation this one is
    /// legal from ANY thread: leaks commit on the dropping thread, and the
    /// count must remain observable even after the home's executor is gone
    /// (the teardown control reads it with the io_context stopped).
    std::size_t
    leaked() const noexcept
    {
        return state_->leaked.load(std::memory_order_acquire);
    }

    bool
    sealed() const noexcept
    {
        WRIT_ASSERT_HOME(state_->owner);
        return state_->sealed;
    }

    bool
    drained() const noexcept
    {
        WRIT_ASSERT_HOME(state_->owner);
        return state_->drained();
    }

    /// The retained drain wake: completed exactly when the census drains;
    /// late joiners observe the same fact. Wait from anywhere via
    /// co_wait_until.
    std::shared_ptr<retained_signal> const&
    drained_signal() const noexcept
    {
        return state_->drained_wake;
    }

private:
    std::shared_ptr<state> state_;
};

}  // namespace writ
