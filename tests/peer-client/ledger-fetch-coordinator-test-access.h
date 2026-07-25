#pragma once

#include <catl/peer-client/ledger-fetch-coordinator.h>

#include <boost/asio/io_context.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

/// Direct, non-hopping access to coordinator state.
///
/// Unlike PeerSetTestAccess, nothing here posts onto the owner strand. That is
/// deliberate: run_on_strand busy-waits while POLLING the io_context, so using it
/// would advance the very handler queue these controls exist to step through one
/// handler at a time. Every control in the suite drives a single-threaded
/// io_context from the test thread, so the strand only ever runs on that thread
/// and a direct read is not a race — it is the same thread, between handlers.
///
/// That property is a precondition, not a hope: a control that started a second
/// thread would invalidate every accessor below.
class LedgerFetchCoordinatorTestAccess
{
public:
    using Coordinator = catl::peer_client::LedgerFetchCoordinator;
    using Phase = Coordinator::Phase;
    using Row = Coordinator::OutboxRow;

    // --- failpoints, all default-inert ------------------------------------

    static void
    set_ingress_projection_failure(
        std::shared_ptr<Coordinator> const& c,
        bool fail)
    {
        c->fail_ingress_projection_for_test_ = fail;
    }

    /// Fail the fallible preparation inside the row commit, after the ingress
    /// permit has already been granted.
    static void
    set_row_commit_failure(std::shared_ptr<Coordinator> const& c, bool fail)
    {
        c->fail_row_commit_for_test_ = fail;
    }

    /// Fail bind initiation after the revision has been opened — the window where
    /// a row could be left in awaiting_bind with nothing in flight.
    static void
    set_bind_initiation_failure(std::shared_ptr<Coordinator> const& c, bool fail)
    {
        c->fail_bind_initiation_for_test_ = fail;
    }

    /// Fail the fence projection, leaving ingress sealed with no owner-strand
    /// drain running. Recoverable only because fence() reports it.
    static void
    set_fence_projection_failure(
        std::shared_ptr<Coordinator> const& c,
        bool fail)
    {
        c->fail_fence_projection_for_test_ = fail;
    }

    /// Drop the completion for exactly this bind revision, reproducing a lost
    /// completion post. Revisions start at 1, so 0 disarms.
    static void
    set_drop_bind_completion_revision(
        std::shared_ptr<Coordinator> const& c,
        std::uint32_t revision)
    {
        c->drop_bind_completion_revision_ = revision;
    }

    // --- bounds, so each can be forced independently ----------------------

    static void
    set_max_bind_attempts(
        std::shared_ptr<Coordinator> const& c,
        std::uint32_t attempts)
    {
        c->max_bind_attempts_ = attempts;
    }

    static void
    set_bind_deadline(
        std::shared_ptr<Coordinator> const& c,
        std::chrono::milliseconds deadline)
    {
        c->bind_deadline_ = deadline;
    }

    static void
    set_absolute_bind_deadline(
        std::shared_ptr<Coordinator> const& c,
        std::chrono::milliseconds deadline)
    {
        c->absolute_bind_deadline_ = deadline;
    }

    /// Retire the absolute bound for one row without touching its attempt
    /// budget, so a control can prove which bound terminalized it.
    static bool
    expire_absolute_deadline(
        std::shared_ptr<Coordinator> const& c,
        catl::peer_client::TransportAttemptId attempt)
    {
        auto* row = find_any(c, attempt);
        if (row == nullptr)
        {
            return false;
        }
        row->absolute_deadline = std::chrono::steady_clock::now();
        return true;
    }

    /// Drive the PROCESS-WIDE sender generation counter. Static, so a control
    /// must restore it or it would poison every later coordinator in the run.
    static void
    set_next_sender_generation(std::uint64_t value)
    {
        Coordinator::next_sender_generation_.store(
            value, std::memory_order_release);
    }

    static std::uint64_t
    next_sender_generation()
    {
        return Coordinator::next_sender_generation_.load(
            std::memory_order_acquire);
    }

    static std::optional<catl::peer_client::SenderGenerationId>
    allocate_sender_generation()
    {
        return Coordinator::allocate_sender_generation();
    }

    /// Drive the attempt-ID space to its ceiling.
    static void
    set_next_attempt(std::shared_ptr<Coordinator> const& c, std::uint64_t value)
    {
        c->next_attempt_ = value;
    }

    static std::uint64_t
    next_attempt(std::shared_ptr<Coordinator> const& c)
    {
        return c->next_attempt_;
    }

    // --- observation ------------------------------------------------------

    /// Rows INCLUDING retired ones. Terminal rows are retired and invisible to
    /// production lookups, so a control that could not see them could not tell a
    /// terminal row from a row that was never committed.
    static Row*
    find_any(
        std::shared_ptr<Coordinator> const& c,
        catl::peer_client::TransportAttemptId attempt)
    {
        for (auto& row : c->rows_)
        {
            if (row.intent.attempt_id() == attempt)
            {
                return &row;
            }
        }
        return nullptr;
    }

    static std::size_t
    row_count(std::shared_ptr<Coordinator> const& c)
    {
        return c->rows_.size();
    }

    static std::size_t
    live_row_count(std::shared_ptr<Coordinator> const& c)
    {
        std::size_t live = 0;
        for (auto const& row : c->rows_)
        {
            if (!row.retired)
            {
                ++live;
            }
        }
        return live;
    }

    /// Attempt ID of the nth committed row. Tests cannot mint one: the
    /// coordinator is the only minter, so they must learn it from the row.
    static std::optional<catl::peer_client::TransportAttemptId>
    attempt_at(std::shared_ptr<Coordinator> const& c, std::size_t index)
    {
        if (index >= c->rows_.size())
        {
            return std::nullopt;
        }
        return c->rows_[index].intent.attempt_id();
    }

    static std::uint64_t
    completions_seen(std::shared_ptr<Coordinator> const& c)
    {
        return c->completions_seen_;
    }

    static std::uint64_t
    completions_sealed(std::shared_ptr<Coordinator> const& c)
    {
        return c->completions_sealed_;
    }

    /// Both fence buffers' capacities. Stability across fence cycles is what
    /// proves delivery never allocates after the terminal commit.
    static std::pair<std::size_t, std::size_t>
    fence_buffer_capacities(std::shared_ptr<Coordinator> const& c)
    {
        return {c->fence_waiters_.capacity(), c->fence_delivery_.capacity()};
    }

    /// Run the fence body directly on the caller's thread.
    ///
    /// This forces a state a single-threaded control cannot schedule: several
    /// fence bodies running while a granted ingress permit is still uncommitted.
    /// In production that arises from a submit on another thread racing the fence
    /// — the permit is CAS'd before the seal lands, and its commit post arrives
    /// after the fence bodies. Without this, the waiter-capacity bound would be
    /// unreachable from a test and its refusal would be decoration.
    static void
    begin_fence(
        std::shared_ptr<Coordinator> const& c,
        Coordinator::FenceHandler handler)
    {
        c->begin_fence_on_strand(std::move(handler));
    }

    /// Drive one bind outcome directly, exactly as a late callback would. Used to
    /// force stale/future revision tokens against an open bind.
    static bool
    settle_bind(
        std::shared_ptr<Coordinator> const& c,
        catl::peer_client::TransportAttemptId attempt,
        std::uint32_t revision,
        bool timed_out)
    {
        return c->settle_bind_on_strand(
            attempt, revision, std::nullopt, timed_out);
    }
};
