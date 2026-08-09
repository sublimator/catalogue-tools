#pragma once

#include "peer-lifecycle.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace catl::peer_client {

class PeerSet;
class PeerUseCompletion;

enum class PeerUseKind : std::uint8_t {
    ledger_header,
    ledger_nodes,
    proof_path,
    ping,
    other,
};

enum class PeerUseOutcome : std::uint8_t {
    success,
    no_ledger,
    no_node,
    invalid_data,
    write_failure,
    transport_failure,
    timeout,
    cancelled,
    shutdown,
    abandoned,
    count,
};

struct PeerUseRequest
{
    PeerUseKind kind = PeerUseKind::other;
    std::uint32_t ledger_seq = 0;
    std::chrono::steady_clock::time_point absolute_deadline{};
};

struct PeerUseTicket
{
    std::uint64_t peer_set_instance = 0;
    std::uint64_t use_id = 0;
    std::uint64_t generation = 0;

    explicit constexpr
    operator bool() const noexcept
    {
        return peer_set_instance != 0 && use_id != 0 && generation != 0;
    }

    friend constexpr bool
    operator==(PeerUseTicket const&, PeerUseTicket const&) = default;
};

/**
 * Inert value naming one observed ready generation.
 *
 * It carries no session and cannot submit transport work. PeerSet must
 * revalidate it immediately before installing a PeerUse.
 */
class PeerGenerationRef
{
public:
    PeerGenerationRef() = default;

    explicit
    operator bool() const noexcept
    {
        return peer_set_instance_ != 0 && generation_ != 0 &&
            !endpoint_.empty();
    }

    std::string const&
    endpoint() const noexcept
    {
        return endpoint_;
    }

    std::uint64_t
    generation() const noexcept
    {
        return generation_;
    }

    PeerStatusSnapshot const&
    status() const noexcept
    {
        return status_;
    }

private:
    friend class PeerSet;

    PeerGenerationRef(
        std::uint64_t peer_set_instance,
        std::string endpoint,
        std::uint64_t generation,
        PeerStatusSnapshot status)
        : peer_set_instance_(peer_set_instance)
        , endpoint_(std::move(endpoint))
        , generation_(generation)
        , status_(std::move(status))
    {
    }

    std::uint64_t peer_set_instance_ = 0;
    std::string endpoint_;
    std::uint64_t generation_ = 0;
    PeerStatusSnapshot status_;
};

namespace detail {

struct PeerUseState
{
    using Finish = std::function<void(PeerUseOutcome)>;

    PeerSessionPtr session;
    PeerGenerationRef generation;
    PeerUseTicket ticket;
    std::weak_ptr<PeerSet> owner;
    Finish finish;
    //@@authority state=prv.S0094 status=provisional scope=field
    //bind=catl::peer_client::detail::PeerUseState::completion_claimed@aab0a73a48077661
    // owner=unresolved owner_hypothesis=peer_use_adapter
    // observed_home=peer_use_state
    // label=exactly_once_peer_use_completion_claim
    std::atomic<bool> completion_claimed{false};

    //@@authority transition=prv.T0245 status=provisional scope=function
    //bind=catl::peer_client::detail::PeerUseState::try_claim@1473a090b1157fd2
    // owner=unresolved owner_hypothesis=peer_use_adapter
    // commit=atomic_compare_exchange touches=prv.S0094
    // label=claim_one_peer_use_terminal_outcome
    bool
    try_claim() noexcept
    {
        bool expected = false;
        return completion_claimed.compare_exchange_strong(
            expected,
            true,
            std::memory_order_acq_rel,
            std::memory_order_acquire);
    }

    void
    notify(PeerUseOutcome outcome) noexcept
    {
        if (finish)
        {
            try
            {
                finish(outcome);
            }
            catch (...)
            {
                // The owner also retains a weak liveness witness. Its bounded
                // expiry/shutdown sweep removes a row if posting is refused.
            }
        }
    }

    bool
    claim(PeerUseOutcome outcome) noexcept
    {
        if (!try_claim())
            return false;
        notify(outcome);
        return true;
    }

    ~PeerUseState()
    {
        (void)claim(PeerUseOutcome::abandoned);
    }
};

}  // namespace detail

/**
 * Move-only ownership of one exact pooled operation on one peer generation.
 *
 * A caller either finishes it explicitly or transfers it to a completion
 * adapter retained by the transport callback. Dropping the final owner posts
 * an abandoned outcome; no destructor mutates PeerSet directly.
 */
class PeerUse
{
public:
    PeerUse() = default;
    PeerUse(PeerUse const&) = delete;
    PeerUse&
    operator=(PeerUse const&) = delete;
    PeerUse(PeerUse&&) noexcept = default;
    PeerUse&
    operator=(PeerUse&&) noexcept = default;
    ~PeerUse() = default;

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

    PeerUseTicket
    ticket() const noexcept
    {
        return state_ ? state_->ticket : PeerUseTicket{};
    }

    PeerGenerationRef const&
    generation() const noexcept
    {
        static PeerGenerationRef const empty;
        return state_ ? state_->generation : empty;
    }

    std::shared_ptr<PeerSet>
    owner() const noexcept
    {
        return state_ ? state_->owner.lock() : nullptr;
    }

    void
    finish(PeerUseOutcome outcome) noexcept
    {
        auto state = std::move(state_);
        if (state)
            (void)state->claim(outcome);
    }

    PeerUseCompletion
    into_completion() && noexcept;

private:
    friend class PeerSet;
    friend class PeerUseCompletion;

    explicit PeerUse(std::shared_ptr<detail::PeerUseState> state)
        : state_(std::move(state))
    {
    }

    std::shared_ptr<detail::PeerUseState> state_;
};

/** Copyable exactly-once callback adapter produced by consuming PeerUse. */
class PeerUseCompletion
{
public:
    PeerUseCompletion() = default;

    explicit
    operator bool() const noexcept
    {
        return static_cast<bool>(state_);
    }

    void
    finish(PeerUseOutcome outcome) const noexcept
    {
        if (state_)
            (void)state_->claim(outcome);
    }

    PeerUseTicket
    ticket() const noexcept
    {
        return state_ ? state_->ticket : PeerUseTicket{};
    }

private:
    friend class PeerUse;

    explicit PeerUseCompletion(std::shared_ptr<detail::PeerUseState> state)
        : state_(std::move(state))
    {
    }

    std::shared_ptr<detail::PeerUseState> state_;
};

inline PeerUseCompletion
PeerUse::into_completion() && noexcept
{
    return PeerUseCompletion{std::move(state_)};
}

}  // namespace catl::peer_client
