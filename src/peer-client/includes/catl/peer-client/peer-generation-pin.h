#pragma once

#include "peer-session.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace catl::peer_client {

class PeerSet;

/**
 * A physical pin on one admitted PeerSet connection generation.
 *
 * This is deliberately not the authority model's semantic PeerUse. It carries
 * no request/outcome identity and grants no C09/S21 operation authority. It is
 * itself a narrow legacy eviction/liveness capability: the exact admitted
 * connection generation cannot be selected as an eviction victim while a
 * shared transport operation still depends on it.
 *
 * The handle is move-only so an ordinary parameter pass cannot silently extend
 * that veto. A coalesced entry or round must call share() explicitly to retain
 * the same PeerSet pin record. The record is released when the final explicit
 * share dies.
 *
 * The session is exposed only as a non-owning borrow. Code cannot copy the
 * underlying shared_ptr, release the pin, and continue using an unpinned
 * admitted session.
 */
class PeerGenerationPin
{
public:
    PeerGenerationPin() = default;
    PeerGenerationPin(PeerGenerationPin const&) = delete;
    PeerGenerationPin&
    operator=(PeerGenerationPin const&) = delete;
    PeerGenerationPin(PeerGenerationPin&&) noexcept = default;
    PeerGenerationPin&
    operator=(PeerGenerationPin&&) noexcept = default;

    ~PeerGenerationPin() = default;

    [[nodiscard]] PeerGenerationPin
    share() const noexcept
    {
        PeerGenerationPin shared;
        shared.state_ = state_;
        return shared;
    }

    explicit
    operator bool() const noexcept
    {
        return get() != nullptr;
    }

    PeerSession*
    get() const noexcept
    {
        return state_ ? state_->session.get() : nullptr;
    }

    PeerSession*
    operator->() const noexcept
    {
        return get();
    }

    PeerSession&
    operator*() const noexcept
    {
        return *get();
    }

    std::string const&
    endpoint() const noexcept
    {
        static std::string const empty;
        return state_ ? state_->endpoint : empty;
    }

    std::uint64_t
    connection_generation() const noexcept
    {
        return state_ ? state_->connection_generation : 0;
    }

    std::uint64_t
    pin_id() const noexcept
    {
        return state_ ? state_->pin_id : 0;
    }

    std::shared_ptr<PeerSet>
    release_owner() const noexcept
    {
        return state_ ? state_->release_owner.lock() : nullptr;
    }

    void
    reset() noexcept
    {
        state_.reset();
    }

private:
    friend class PeerSet;

    using Release = std::function<void()>;

    struct State
    {
        PeerSessionPtr session;
        std::string endpoint;
        std::uint64_t connection_generation = 0;
        std::uint64_t pin_id = 0;
        std::weak_ptr<PeerSet> release_owner;
        Release release;

        State(
            PeerSessionPtr session_,
            std::string endpoint_,
            std::uint64_t connection_generation_,
            std::uint64_t pin_id_,
            std::weak_ptr<PeerSet> release_owner_,
            Release release_)
            : session(std::move(session_))
            , endpoint(std::move(endpoint_))
            , connection_generation(connection_generation_)
            , pin_id(pin_id_)
            , release_owner(std::move(release_owner_))
            , release(std::move(release_))
        {
        }

        ~State()
        {
            if (!release)
                return;
            try
            {
                release();
            }
            catch (...)
            {
                // PeerSet's weak liveness witness keeps this fail-closed only
                // until its next owner-strand prune/release barrier.
            }
        }
    };

    PeerGenerationPin(
        PeerSessionPtr session,
        std::string endpoint,
        std::uint64_t connection_generation,
        std::uint64_t pin_id,
        std::weak_ptr<PeerSet> release_owner,
        Release release)
        : state_(std::make_shared<State>(
              std::move(session),
              std::move(endpoint),
              connection_generation,
              pin_id,
              std::move(release_owner),
              std::move(release)))
    {
    }

    std::shared_ptr<State> state_;
};

}  // namespace catl::peer_client
