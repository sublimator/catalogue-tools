#pragma once

// writ::census — an admitted population with sealed admission and a
// received (never inferred) drain.
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
//   VISIBLY           drained() stays false forever. That wedge is the
//                     feature: the fossil shutdown waits on unaccounted
//                     obligations silently; a census makes the wedge
//                     attributable. The leak is reported on stderr and
//                     aborts under WRIT_HOME_FAIL_FAST.
//
// The drained wake is a writ::retained_signal — completed exactly when the
// census drains, retained for late joiners (quiescence is received from its
// owner). Drained is absorbing, so one signal generation suffices.
//
// All operations are home-confined except the member destructor's leak
// notice, which may run anywhere and posts home-ward.

#include <writ/home.hpp>
#include <writ/retained-signal.hpp>

#include <boost/asio/post.hpp>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <optional>
#include <utility>

namespace writ {

template <typename Key>
class census
{
    struct state
    {
        explicit state(home h)
            : owner(std::move(h))
            , drained_wake(std::make_shared<retained_signal>(owner))
        {
        }

        home owner;
        std::map<Key, std::uint64_t> live;
        std::uint64_t next_member = 0;
        bool sealed = false;
        std::size_t leaked = 0;
        std::shared_ptr<retained_signal> drained_wake;

        bool
        drained() const noexcept
        {
            return sealed && live.empty() && leaked == 0;
        }

        void
        complete_if_drained() noexcept
        {
            if (drained())
                drained_wake->complete();
        }

        // Cross-thread entry: a member was dropped without retirement.
        void
        leak(Key key, std::uint64_t id) noexcept
        {
            std::fprintf(
                stderr,
                "WRIT-CENSUS-LEAK: member %llu\n",
                static_cast<unsigned long long>(id));
            std::fflush(stderr);
            if (std::getenv("WRIT_HOME_FAIL_FAST") != nullptr)
                std::abort();
            auto const it = live.find(key);
            if (it != live.end() && it->second == id)
                live.erase(it);
            ++leaked;  // sticky: drained() can never become true
        }
    };

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

        // Dropped live: record the leak on the home. May run on any thread;
        // if the census itself is gone, the leak is print-only.
        void
        abandon() noexcept
        {
            if (id_ == 0)
                return;
            auto owner = census_.lock();
            auto const id = std::exchange(id_, 0);
            if (!owner)
            {
                std::fprintf(
                    stderr,
                    "WRIT-CENSUS-LEAK: member %llu outlived census\n",
                    static_cast<unsigned long long>(id));
                std::fflush(stderr);
                return;
            }
            try
            {
                boost::asio::post(
                    owner->owner.executor(),
                    [owner, key = std::move(key_), id]() mutable noexcept {
                        owner->leak(std::move(key), id);
                    });
            }
            catch (...)
            {
                std::fprintf(
                    stderr,
                    "WRIT-CENSUS-LEAK: member %llu (post failed)\n",
                    static_cast<unsigned long long>(id));
                std::fflush(stderr);
            }
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

    /// On the home. Refuses after seal and refuses a duplicate LIVE key
    /// (the row itself is the dedup state). A retired key may be
    /// re-admitted; chained stages that must avoid a transient zero use
    /// distinct member identities.
    [[nodiscard]] std::optional<member>
    admit(Key key)
    {
        auto& s = *state_;
        WRIT_ASSERT_HOME(s.owner);
        if (s.sealed || s.live.contains(key))
            return std::nullopt;
        auto const id = ++s.next_member;
        s.live.emplace(key, id);
        return member(state_, std::move(key), id);
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

    /// On the home. Consumes the obligation exactly once on success and
    /// returns true when this retirement drained the census. A refusal
    /// (foreign or empty member) does NOT consume: the token stays live in
    /// the caller's hands, because a refusal must never counterfeit
    /// consumption — the obligation it carries still has to balance
    /// somewhere.
    bool
    retire(member&& m) noexcept
    {
        auto& s = *state_;
        WRIT_ASSERT_HOME(s.owner);
        if (m.id_ == 0 || m.census_.lock() != state_)
            return false;
        auto const it = s.live.find(m.key_);
        if (it == s.live.end() || it->second != m.id_)
            return false;
        s.live.erase(it);
        m.defuse();
        s.complete_if_drained();
        return s.drained();
    }

    // ── observations, on the home ────────────────────────────────────

    std::size_t
    live() const noexcept
    {
        WRIT_ASSERT_HOME(state_->owner);
        return state_->live.size();
    }

    std::size_t
    leaked() const noexcept
    {
        WRIT_ASSERT_HOME(state_->owner);
        return state_->leaked;
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
