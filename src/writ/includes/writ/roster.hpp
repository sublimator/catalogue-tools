#pragma once

// writ::roster — an admitted MULTISET of obligations with sealed admission
// and a received (never inferred) drain.
// schema: writ.core/roster/v1
//
// census minus the duplicate-live-key law. Extracted from the VlCache V2
// evidence phase: under asio's cancellation-is-a-request, any
// rearm-before-receipt pattern holds multiple simultaneous obligations of
// one kind with no honest consumer-side dedup identity (the witnessed
// 1/2/3 stacking of refresh-timer obligations). A set-shaped census cannot
// admit what that source does; a keyless multiset is the honest model.
// census/v1 keeps its law for consumers whose keys are real.
//
// The laws, shared with census where they coincide (the seal/leak/drain/
// wake machinery is the ONE shared detail::obligation_state engine — no
// copied wedge):
//
//   admit before launch    as census.
//   sealed admission       as census.
//   drain is a conjunction as census: sealed ∧ zero live ∧ zero leaked.
//   a leak wedges visibly  as census, committed on the dropping thread.
//   refusal preserves      as census: a refused retirement consumes
//   the obligation         nothing.
//
//   multiset admission     THE DIFFERENTIATOR: admitting a label already
//                          live succeeds. Admission refusal comes only
//                          from seal or identity exhaustion, never from a
//                          duplicate label.
//
//   label is not identity  the label participates in NO admission refusal
//                          and NO retirement lookup; retirement resolves
//                          by the internally minted member id alone, which
//                          the move-only token carries privately. The
//                          label is diagnostic attribution (live_with, the
//                          wedge line's population), nothing more.
//
// The member token exposes no label() and no id: the consumer names the
// label at the site it admitted, and no consumer closure has demanded id
// observation. Requirements on Label: an ordinary movable value stored in
// the row — the census-style nothrow-move constraint is deliberately NOT
// inherited (the token never carries the Label and the leak path never
// touches it); admit and live_with may throw normally. Equality is
// required only by live_with. Ordering is not required.

#include <writ/detail/obligation-lifecycle.hpp>
#include <writ/exact.hpp>
#include <writ/home.hpp>
#include <writ/retained-signal.hpp>

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <utility>

namespace writ {

class roster_test_access;

namespace detail {

/// Roster's fixed private row store: id-keyed, no dedup question to ask.
template <typename Label>
struct roster_store
{
    std::map<std::uint64_t, Label> live;

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
    erase_leaked(std::uint64_t const id) noexcept
    {
        live.erase(id);
    }
};

}  // namespace detail

template <typename Label>
class roster
{
    using state = detail::obligation_state<detail::roster_store<Label>>;

public:
    /// A move-only obligation token. Retire it exactly once through its
    /// roster; dropping it is a recorded leak. Carries its minted id
    /// privately and nothing else.
    class member
    {
    public:
        member() = default;

        member(member&& other) noexcept
            : roster_(std::move(other.roster_))
            , id_(std::exchange(other.id_, 0))
        {
            other.roster_.reset();
        }

        member&
        operator=(member&& other) noexcept
        {
            if (this != &other)
            {
                abandon();
                roster_ = std::move(other.roster_);
                id_ = std::exchange(other.id_, 0);
                other.roster_.reset();
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

    private:
        friend class roster;
        friend class ::writ::roster_test_access;

        member(std::weak_ptr<state> roster, std::uint64_t id)
            : roster_(std::move(roster)), id_(id)
        {
        }

        void
        defuse() noexcept
        {
            id_ = 0;
            roster_.reset();
        }

        void
        abandon() noexcept
        {
            if (id_ == 0)
                return;
            auto const id = std::exchange(id_, 0);
            detail::abandon_obligation(
                roster_,
                id,
                "WRIT-ROSTER-LEAK",
                " outlived roster",
                [id](state& s) noexcept { s.rows.erase_leaked(id); });
            roster_.reset();
        }

        std::weak_ptr<state> roster_;
        std::uint64_t id_ = 0;
    };

    explicit roster(home h) : state_(std::make_shared<state>(std::move(h)))
    {
    }

    roster(roster const&) = delete;
    roster&
    operator=(roster const&) = delete;
    roster(roster&&) noexcept = default;
    roster&
    operator=(roster&&) noexcept = default;

    /// On the home. Refuses ONLY after seal or on member-identity
    /// exhaustion (the mint's sticky refusal). A label already live admits
    /// again — the population is a multiset and the label is never
    /// consulted.
    [[nodiscard]] std::optional<member>
    admit(Label label)
    {
        auto& s = *state_;
        WRIT_ASSERT_HOME(s.owner);
        if (s.sealed)
            return std::nullopt;
        auto const id = s.member_mint.next();
        if (!id)
            return std::nullopt;
        s.rows.live.emplace(id.value(), std::move(label));
        return member(state_, id.value());
    }

    /// On the home. Fences every later admit; an already-empty roster
    /// drains (and completes its wake) immediately.
    void
    seal() noexcept
    {
        auto& s = *state_;
        WRIT_ASSERT_HOME(s.owner);
        s.sealed = true;
        s.complete_if_drained();
    }

    /// On the home. The closed retire_outcome population, exactly as
    /// census: `refused` (foreign or empty member) consumes nothing;
    /// `retired` consumed the obligation with the roster still owing
    /// others; `drained` consumed it AND completed the roster. Resolution
    /// is by the token's minted id alone — never the label.
    retire_outcome
    retire(member&& m) noexcept
    {
        auto& s = *state_;
        WRIT_ASSERT_HOME(s.owner);
        if (m.id_ == 0 || m.roster_.lock() != state_)
            return retire_outcome::refused;
        auto const it = s.rows.live.find(m.id_);
        if (it == s.rows.live.end())
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

    /// Diagnostic attribution count for one label — the label consulted
    /// for OBSERVATION only. Deliberately not noexcept: it evaluates
    /// Label equality, which v1 does not require to be nonthrowing.
    std::size_t
    live_with(Label const& label) const
    {
        WRIT_ASSERT_HOME(state_->owner);
        auto const& rows = state_->rows.live;
        return static_cast<std::size_t>(std::count_if(
            rows.begin(), rows.end(), [&](auto const& row) {
                return row.second == label;
            }));
    }

    /// The sticky leak count; legal from ANY thread, as census.
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

    /// The retained drain wake: completed exactly when the roster drains;
    /// late joiners observe the same fact.
    std::shared_ptr<retained_signal> const&
    drained_signal() const noexcept
    {
        return state_->drained_wake;
    }

private:
    friend class ::writ::roster_test_access;

    std::shared_ptr<state> state_;
};

}  // namespace writ
