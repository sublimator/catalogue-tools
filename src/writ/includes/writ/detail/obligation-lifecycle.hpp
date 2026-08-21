#pragma once

// writ::detail — the shared obligation lifecycle engine.
//
// The single implementation owner of the machinery census and roster have
// in common: the sealed flag, the sticky cross-thread leak counter, the
// retained drained wake, the drain conjunction, member-identity minting,
// and the leak wedge's dropping-thread commit. Extracted from census/v1
// under the roster/v1 design packet's constraint: ONE owner for the hard
// machinery, no copied wedge.
//
// What is deliberately NOT here: rows. Each face owns a fixed private row
// store inside the same shared state object (census: map<Key,id> with the
// duplicate-live-key law; roster: map<id,Label> with none). The engine
// asks the store only rows.empty() for the drain conjunction; admission
// and retirement lookups stay in the face, so census keeps its key-first
// (key, id) behavior verbatim and roster resolves by id alone.
//
// Lifetime shape (pinned in the packet, rev 3): each face holds ONE shared
// state object containing lifecycle AND store together. Members hold a
// weak reference plus their payload. abandon_obligation commits the leak
// FACT on the dropping thread — stderr line, fail-fast abort, sticky
// counter — before any executor is involved; only the row bookkeeping
// rides the post, which captures the state STRONGLY so face teardown can
// lose tidiness, never the wedge, and the store can neither outlive nor
// predecease the lifecycle it answers to. An expired weak reports the
// member as having outlived its face.

#include <writ/exact.hpp>
#include <writ/home.hpp>
#include <writ/retained-signal.hpp>

#include <boost/asio/post.hpp>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <utility>

namespace writ {

/// The closed disposition of one retirement attempt, shared by census and
/// roster. `refused` preserves the caller's member token (a refusal must
/// never counterfeit consumption); `retired` and `drained` both consume
/// it, and `drained` additionally reports that THIS retirement completed
/// the population. Round 5: the previous bool collapsed refused with
/// retired and forced callers to recover consumption from the token — an
/// implicit side channel the census/v1 schema never declared.
enum class [[nodiscard]] retire_outcome : std::uint8_t {
    refused,
    retired,
    drained,
};

namespace detail {

/// Lifecycle + the face's fixed row store, in one shared allocation.
template <typename Store>
struct obligation_state
{
    explicit obligation_state(home h)
        : owner(std::move(h))
        , drained_wake(std::make_shared<retained_signal>(owner))
    {
    }

    home owner;
    Store rows;
    mint member_mint;
    bool sealed = false;
    std::atomic<std::size_t> leaked{0};
    std::shared_ptr<retained_signal> drained_wake;

    bool
    drained() const noexcept
    {
        return sealed && rows.empty() &&
            leaked.load(std::memory_order_acquire) == 0;
    }

    void
    complete_if_drained() noexcept
    {
        if (drained())
            drained_wake->complete();
    }
};

/// The leak wedge, written once. Dropped live: the leak FACT commits here,
/// on the dropping thread — stderr line, fail-fast abort, sticky
/// cross-thread counter — BEFORE any executor is involved. Only the
/// row bookkeeping (`erase`, a face-supplied noexcept callable over the
/// state) rides the home-ward post, capturing the state strongly, so
/// executor teardown can lose tidiness, never the wedge.
template <typename State, typename EraseFn>
void
abandon_obligation(
    std::weak_ptr<State> const& state,
    std::uint64_t const id,
    char const* const leak_prefix,
    char const* const outlived_suffix,
    EraseFn erase) noexcept
{
    auto owner = state.lock();
    std::fprintf(
        stderr,
        "%s: member %llu%s\n",
        leak_prefix,
        static_cast<unsigned long long>(id),
        owner ? "" : outlived_suffix);
    std::fflush(stderr);
    if (std::getenv("WRIT_HOME_FAIL_FAST") != nullptr)
        std::abort();
    if (!owner)
        return;
    owner->leaked.fetch_add(1, std::memory_order_acq_rel);
    try
    {
        boost::asio::post(
            owner->owner.executor(),
            [owner, erase = std::move(erase)]() noexcept { erase(*owner); });
    }
    catch (...)
    {
        // The row stays live and the counter is already committed: both
        // halves of the wedge hold, and the report above has already been
        // made.
    }
}

}  // namespace detail

}  // namespace writ
