#pragma once

#include "peer-lifecycle.h"

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

namespace catl::peer_client {

class PeerSet;
class PeerUseCompletion;
class PeerUseOperations;

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

enum class PeerUseReceiptFault : std::uint8_t {
    physical_drain_failed,
};

class PeerUseReceiptError final : public std::runtime_error
{
public:
    explicit PeerUseReceiptError(PeerUseReceiptFault fault)
        : std::runtime_error("peer use physical drain failed"), fault_(fault)
    {
    }

    PeerUseReceiptFault
    fault() const noexcept
    {
        return fault_;
    }

private:
    PeerUseReceiptFault fault_;
};

inline constexpr PeerUseOutcome
classify_peer_use_error(Error error) noexcept
{
    switch (error)
    {
        case Error::Success:
            return PeerUseOutcome::success;
        case Error::PeerNoLedger:
        case Error::PeerCoverageChanged:
            return PeerUseOutcome::no_ledger;
        case Error::NoNode:
            return PeerUseOutcome::no_node;
        case Error::ParseError:
            return PeerUseOutcome::invalid_data;
        case Error::Timeout:
            return PeerUseOutcome::timeout;
        case Error::Cancelled:
            return PeerUseOutcome::cancelled;
        case Error::SendNotInitiated:
            return PeerUseOutcome::write_failure;
        case Error::Disconnected:
            return PeerUseOutcome::transport_failure;
        case Error::BadRequest:
        case Error::FeatureDisabled:
        case Error::DuplicateRequest:
        case Error::ResourceExhausted:
        case Error::NoSuitablePeer:
            return PeerUseOutcome::cancelled;
    }
    return PeerUseOutcome::transport_failure;
}

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
            event_sequence_ != 0 && !endpoint_.empty();
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

    std::uint64_t
    event_sequence() const noexcept
    {
        return event_sequence_;
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
        std::uint64_t event_sequence,
        PeerStatusSnapshot status)
        : peer_set_instance_(peer_set_instance)
        , endpoint_(std::move(endpoint))
        , generation_(generation)
        , event_sequence_(event_sequence)
        , status_(std::move(status))
    {
    }

    std::uint64_t peer_set_instance_ = 0;
    std::string endpoint_;
    std::uint64_t generation_ = 0;
    std::uint64_t event_sequence_ = 0;
    PeerStatusSnapshot status_;
};

namespace detail {

enum class PeerUsePhase : std::uint8_t {
    reserved,
    submitted,
    revoked,
};

enum class PeerUseProposalSource : std::uint8_t {
    local,
    physical_terminal,
};

class PeerUseReceiptState
    : public std::enable_shared_from_this<PeerUseReceiptState>
{
public:
    explicit PeerUseReceiptState(boost::asio::any_io_executor executor)
        : executor_(std::move(executor))
        , signal_(executor_, boost::asio::steady_timer::time_point::max())
    {
    }

    boost::asio::awaitable<PeerUseOutcome>
    wait()
    {
        auto self = shared_from_this();
        co_return co_await boost::asio::co_spawn(
            executor_,
            [self]() -> boost::asio::awaitable<PeerUseOutcome> {
                if (self->outcome_)
                    co_return *self->outcome_;
                if (self->fault_)
                    throw PeerUseReceiptError(*self->fault_);
                boost::system::error_code ec;
                co_await self->signal_.async_wait(boost::asio::redirect_error(
                    boost::asio::use_awaitable, ec));
                (void)ec;
                if (self->outcome_)
                    co_return *self->outcome_;
                if (self->fault_)
                    throw PeerUseReceiptError(*self->fault_);
                co_return PeerUseOutcome::abandoned;
            },
            boost::asio::use_awaitable);
    }

    //@@authority transition=prv.T0251 status=provisional scope=function
    // bind=catl::peer_client::detail::PeerUseReceiptState::complete_on_owner@c062fecec17450b4
    // owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
    // touches=prv.S0096
    // label=publish_owner_committed_peer_use_release_receipt
    void
    complete_on_owner(PeerUseOutcome outcome) noexcept
    {
        if (outcome_ || fault_)
            return;
        outcome_ = outcome;
        {
            std::lock_guard lock(projection_mutex_);
            projected_outcome_ = outcome;
        }
        try
        {
            signal_.cancel();
        }
        catch (...)
        {
        }
    }

    //@@authority transition=prv.T0253 status=provisional scope=function bind=catl::peer_client::detail::PeerUseReceiptState::fail_on_owner@3b0790e89fbda6e4
    // owner=unresolved owner_hypothesis=peer_set commit=peer_set_strand
    // touches=prv.S0099
    // label=publish_retained_peer_use_physical_drain_fault
    void
    fail_on_owner(PeerUseReceiptFault fault) noexcept
    {
        if (outcome_ || fault_)
            return;
        fault_ = fault;
        {
            std::lock_guard lock(projection_mutex_);
            projected_fault_ = fault;
        }
        try
        {
            signal_.cancel();
        }
        catch (...)
        {
        }
    }

    std::optional<PeerUseOutcome>
    projected_outcome() const noexcept
    {
        std::lock_guard lock(projection_mutex_);
        return projected_outcome_;
    }

    std::optional<PeerUseReceiptFault>
    projected_fault() const noexcept
    {
        std::lock_guard lock(projection_mutex_);
        return projected_fault_;
    }

private:
    boost::asio::any_io_executor executor_;
    boost::asio::steady_timer signal_;
    //@@authority state=prv.S0096 status=provisional scope=field
    // bind=catl::peer_client::detail::PeerUseReceiptState::outcome_@bf13f7cacf56b27b
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_use_receipt
    // label=owner_committed_peer_use_release_receipt_outcome
    // NOTE: outcome_ is strand-confined; projected_outcome_ is its
    // mutex-protected foreign-thread observation.
    std::optional<PeerUseOutcome> outcome_;
    //@@authority state=prv.S0099 status=provisional scope=field bind=catl::peer_client::detail::PeerUseReceiptState::fault_@1dd63f24ccedcc91
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_use_receipt
    // label=retained_peer_use_physical_drain_fault
    std::optional<PeerUseReceiptFault> fault_;
    mutable std::mutex projection_mutex_;
    std::optional<PeerUseOutcome> projected_outcome_;
    std::optional<PeerUseReceiptFault> projected_fault_;
};

struct PeerUseControl
{
    using Finish =
        std::function<void(PeerUseOutcome, PeerUseProposalSource)>;

    // Access only while submission_gate is held. The pointer is installed
    // before publication and cleared by revocation under that same gate.
    PeerSessionPtr session;
    PeerGenerationRef generation;
    //@@authority state=prv.S0100 status=provisional scope=field bind=catl::peer_client::detail::PeerUseControl::request@a2e6cc89992721c8
    // owner=unresolved owner_hypothesis=peer_set observed_home=peer_use_control
    // label=immutable_peer_use_request_operation_binding
    PeerUseRequest request;
    PeerUseTicket ticket;
    std::weak_ptr<PeerSet> owner;
    Finish finish;
    std::shared_ptr<PeerUseReceiptState> receipt;
    //@@authority state=prv.S0094 status=provisional scope=field
    // bind=catl::peer_client::detail::PeerUseControl::phase@45e57d6589dbf3cb
    // owner=unresolved owner_hypothesis=peer_use_adapter
    // observed_home=peer_use_control
    // label=peer_use_reserved_submitted_revoked_phase
    std::atomic<PeerUsePhase> phase{PeerUsePhase::reserved};
    /**
     * Short gate shared by operation submission and revoke.
     *
     * The session API only schedules work; it must not wait for transport.
     * Holding this guard across that call closes the otherwise possible gap
     * where an owner could drain and publish the release receipt after the
     * phase commit but before the transport call was actually installed.
     */
    std::mutex submission_gate;
    //@@authority state=prv.S0095 status=provisional scope=field
    // bind=catl::peer_client::detail::PeerUseControl::proposal_claimed@2ad1c7b271ef8d5a
    // owner=unresolved owner_hypothesis=peer_use_adapter
    // observed_home=peer_use_control
    // label=deduplicated_peer_use_terminal_proposal
    std::atomic<bool> proposal_claimed{false};

    template <class Submit>
    bool
    try_submit(Submit&& submit)
    {
        if (phase.load(std::memory_order_acquire) != PeerUsePhase::reserved)
            return false;

        std::lock_guard guard(submission_gate);
        if (phase.load(std::memory_order_relaxed) != PeerUsePhase::reserved)
            return false;
        auto current = session;
        if (!current)
            return false;

        // Publish submitted while revoke is excluded, then install the
        // physical operation before releasing the same gate.
        //@@authority transition=prv.T0246 status=provisional scope=statement
        // bind=expression_statement@5fd92af54d63965b
        // owner=unresolved owner_hypothesis=peer_use_adapter
        // commit=submission_gate touches=prv.S0094
        // label=commit_and_install_one_physical_submission_before_revocation
        //@@mutation-site peer-use-submission-precedes-physical-install
        phase.store(
            PeerUsePhase::submitted,
            std::memory_order_release); //@@authority-target ids=prv.T0246
        std::invoke(std::forward<Submit>(submit), std::move(current));
        //@@mutation-end peer-use-submission-precedes-physical-install
        return true;
    }

    //@@authority transition=prv.T0252 status=provisional scope=function
    // bind=catl::peer_client::detail::PeerUseControl::revoke@307aa6c39d134aa3
    // owner=unresolved owner_hypothesis=peer_use_adapter
    // commit=submission_gate touches=prv.S0094
    // label=revoke_peer_use_only_after_physical_submission_install_returns
    PeerUsePhase
    revoke() noexcept
    {
        std::lock_guard guard(submission_gate);
        auto const prior =
            phase.exchange(PeerUsePhase::revoked, std::memory_order_acq_rel);
        session.reset();
        return prior;
    }

    /**
     * Revoke only when no physical submission has committed.
     *
     * A submitted operation is left intact so its owner can first install a
     * drain servant, then revoke it with every sibling under one generation
     * transition.
     */
    PeerUsePhase
    revoke_if_reserved() noexcept
    {
        std::lock_guard guard(submission_gate);
        auto const prior = phase.load(std::memory_order_relaxed);
        if (prior == PeerUsePhase::reserved)
        {
            phase.store(PeerUsePhase::revoked, std::memory_order_release);
            session.reset();
        }
        return prior;
    }

    //@@authority transition=prv.T0245 status=provisional scope=function
    // bind=catl::peer_client::detail::PeerUseControl::try_propose@8c2b1e519e4c5b21
    // owner=unresolved owner_hypothesis=peer_use_adapter
    // commit=atomic_compare_exchange touches=prv.S0095
    // label=deduplicate_one_peer_use_terminal_proposal
    bool
    try_propose() noexcept
    {
        bool expected = false;
        return proposal_claimed.compare_exchange_strong(
            expected,
            true,
            std::memory_order_acq_rel,
            std::memory_order_acquire);
    }

    void
    notify(PeerUseOutcome outcome, PeerUseProposalSource source) noexcept
    {
        if (finish)
        {
            try
            {
                finish(outcome, source);
            }
            catch (...)
            {
                // The owner also retains a weak liveness witness. Its bounded
                // expiry/shutdown sweep removes a row if posting is refused.
            }
        }
    }

    bool
    propose(
        PeerUseOutcome outcome,
        PeerUseProposalSource source = PeerUseProposalSource::local) noexcept
    {
        if (!try_propose())
            return false;
        notify(outcome, source);
        return true;
    }
};

struct PeerUseState
{
    std::shared_ptr<PeerUseControl> control;

    ~PeerUseState()
    {
        if (control)
            (void)control->propose(PeerUseOutcome::abandoned);
    }
};

}  // namespace detail

/** Owner-side acknowledgement of exact use classification and release. */
class PeerUseReceipt
{
public:
    PeerUseReceipt() = default;

    explicit
    operator bool() const noexcept
    {
        return static_cast<bool>(state_);
    }

    boost::asio::awaitable<PeerUseOutcome>
    wait() const
    {
        if (!state_)
            co_return PeerUseOutcome::abandoned;
        co_return co_await state_->wait();
    }

    std::optional<PeerUseOutcome>
    outcome() const noexcept
    {
        return state_ ? state_->projected_outcome()
                      : std::optional<PeerUseOutcome>{};
    }

    std::optional<PeerUseReceiptFault>
    fault() const noexcept
    {
        return state_ ? state_->projected_fault()
                      : std::optional<PeerUseReceiptFault>{};
    }

private:
    friend class PeerSet;
    friend class PeerUse;
    friend class PeerUseCompletion;

    explicit PeerUseReceipt(std::shared_ptr<detail::PeerUseReceiptState> state)
        : state_(std::move(state))
    {
    }

    std::shared_ptr<detail::PeerUseReceiptState> state_;
};

/**
 * Move-only ownership of one exact pooled operation on one peer generation.
 *
 * A caller either finishes it explicitly or transfers it to a completion
 * adapter retained by the transport callback. Dropping the final owner posts
 * an abandoned outcome; no destructor mutates PeerSet directly. Like other
 * move-only handles, one PeerUse object has one calling owner. The copyable
 * PeerUseCompletion and PeerUseReceipt projections are safe across threads.
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
        return static_cast<bool>(state_);
    }

    PeerUseTicket
    ticket() const noexcept
    {
        return state_ && state_->control ? state_->control->ticket
                                         : PeerUseTicket{};
    }

    PeerGenerationRef const&
    generation() const noexcept
    {
        static PeerGenerationRef const empty;
        return state_ && state_->control ? state_->control->generation : empty;
    }

    std::shared_ptr<PeerSet>
    owner() const noexcept
    {
        return state_ && state_->control ? state_->control->owner.lock()
                                         : nullptr;
    }

    PeerUseReceipt
    receipt() const noexcept
    {
        return state_ && state_->control
            ? PeerUseReceipt{state_->control->receipt}
            : PeerUseReceipt{};
    }

    void
    finish(PeerUseOutcome outcome) noexcept
    {
        auto state = std::move(state_);
        if (state)
        {
            if (state->control)
                (void)state->control->propose(outcome);
        }
    }

    boost::asio::awaitable<PeerUseOutcome>
    co_finish(PeerUseOutcome outcome)
    {
        auto done = receipt();
        finish(outcome);
        co_return co_await done.wait();
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
        {
            if (state_->control)
                (void)state_->control->propose(
                    outcome,
                    physical_terminal_ ?
                        detail::PeerUseProposalSource::physical_terminal :
                        detail::PeerUseProposalSource::local);
        }
    }

    PeerUseReceipt
    receipt() const noexcept
    {
        return state_ && state_->control
            ? PeerUseReceipt{state_->control->receipt}
            : PeerUseReceipt{};
    }

    boost::asio::awaitable<PeerUseOutcome>
    co_finish(PeerUseOutcome outcome) const
    {
        auto done = receipt();
        finish(outcome);
        co_return co_await done.wait();
    }

    PeerUseTicket
    ticket() const noexcept
    {
        return state_ && state_->control ? state_->control->ticket
                                         : PeerUseTicket{};
    }

    std::string const&
    endpoint() const noexcept
    {
        static std::string const empty;
        return state_ && state_->control
            ? state_->control->generation.endpoint()
            : empty;
    }

private:
    friend class PeerUse;
    friend class PeerUseOperations;

    explicit PeerUseCompletion(std::shared_ptr<detail::PeerUseState> state)
        : state_(std::move(state))
    {
    }

    /** Install one internal transport operation under the revoke gate. */
    template <class Submit>
    //@@authority transition=prv.T0255 status=provisional scope=function bind=catl::peer_client::PeerUseCompletion::install_operation@3fd49684978c8f91
    // owner=unresolved owner_hypothesis=peer_use_adapter commit=submission_gate
    // touches=prv.S0094,prv.S0100
    // label=validate_acquired_request_before_consuming_operation_authority
    bool
    install_operation(
        PeerUseKind kind,
        std::uint32_t ledger_seq,
        Submit&& submit) const
    {
        auto state = state_;
        auto control = state ? state->control : nullptr;
        //@@mutation-site peer-use-operation-matches-acquired-request
        if (!control || control->request.kind != kind ||
            control->request.ledger_seq != ledger_seq)
            return false;
        //@@mutation-end peer-use-operation-matches-acquired-request
        try
        {
            return control->try_submit(std::forward<Submit>(submit));
        }
        catch (...)
        {
            finish(PeerUseOutcome::write_failure);
            throw;
        }
    }

    PeerUseCompletion
    with_physical_terminal_capability() const noexcept
    {
        auto result = *this;
        result.physical_terminal_ = true;
        return result;
    }

    bool
    physical_delivery_allowed() const noexcept
    {
        auto state = state_;
        auto control = state ? state->control : nullptr;
        return control &&
            control->phase.load(std::memory_order_acquire) ==
            detail::PeerUsePhase::submitted;
    }

    std::shared_ptr<detail::PeerUseState> state_;
    bool physical_terminal_ = false;
};

inline PeerUseCompletion
PeerUse::into_completion() && noexcept
{
    return PeerUseCompletion{std::move(state_)};
}

}  // namespace catl::peer_client
