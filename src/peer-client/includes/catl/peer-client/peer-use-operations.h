#pragma once

#include "node-transport.h"
#include "peer-use.h"

#include <functional>
#include <mutex>
#include <optional>
#include <utility>

namespace catl::peer_client {

/**
 * Protocol-specific operations installed through an exact PeerUse.
 *
 * PeerUse itself owns only lifecycle, deadline, and release authority. This
 * adapter is the trusted boundary allowed to see the session long enough to
 * install one operation; the session never escapes to its caller.
 */
class PeerUseOperations final
{
public:
    using LedgerHeaderCallback =
        std::function<void(PeerUseCompletion, Error, LedgerHeaderResult)>;

    /** Replay/ack capability restricted to one exact ledger-node PeerUse. */
    class NodeBatchChannel
    {
    public:
        NodeBatchChannel() = default;

        explicit
        operator bool() const noexcept
        {
            return static_cast<bool>(completion_);
        }

        bool
        submit(
            NodeBatchCommand const& command,
            NodeBatchWriteHandler handler) const
        {
            if (!completion_ || !handler ||
                command.ledger_seq() != ledger_seq_ ||
                command.peer_generation().value() !=
                    completion_.ticket().generation)
            {
                return false;
            }
            auto accepted = false;
            auto const invoked = completion_.install_or_replay_operation(
                PeerUseKind::ledger_nodes,
                ledger_seq_,
                [this, &command, &handler, &accepted](PeerSessionPtr session) {
                    session->set_node_response_observer(observer_);
                    accepted = session->submit_node_batch(
                        command, std::move(handler));
                });
            return invoked && accepted;
        }

        bool
        acknowledge(NodeBatchReceiptAck ack) const
        {
            if (!completion_ || ack.peer_generation().value() !=
                    completion_.ticket().generation)
            {
                return false;
            }
            auto accepted = false;
            auto const invoked = completion_.invoke_submitted_operation(
                PeerUseKind::ledger_nodes,
                ledger_seq_,
                [&ack, &accepted](PeerSessionPtr session) mutable {
                    accepted = session->acknowledge_node_batch_receipt(
                        std::move(ack));
                });
            return invoked && accepted;
        }

        PeerUseCompletion const&
        completion() const noexcept
        {
            return completion_;
        }

        /** Classify the use once this channel has no transport work left. */
        void
        finish_operation(PeerUseOutcome outcome) const noexcept
        {
            if (!completion_)
                return;
            if (completion_.physical_delivery_allowed())
            {
                completion_.with_physical_terminal_capability().finish(outcome);
            }
            else
            {
                completion_.finish(outcome);
            }
        }

    private:
        friend class PeerUseOperations;

        NodeBatchChannel(
            PeerUseCompletion completion,
            std::uint32_t ledger_seq,
            PeerSession::NodeResponseObserver observer) noexcept
            : completion_(std::move(completion))
            , ledger_seq_(ledger_seq)
            , observer_(std::move(observer))
        {
        }

        PeerUseCompletion completion_;
        std::uint32_t ledger_seq_ = 0;
        PeerSession::NodeResponseObserver observer_;
    };

    static NodeBatchChannel
    node_batch_channel(
        PeerUseCompletion completion,
        std::uint32_t ledger_seq,
        PeerSession::NodeResponseObserver observer) noexcept
    {
        if (!completion || completion.ticket().generation == 0 || !observer)
            return {};
        return NodeBatchChannel{
            std::move(completion), ledger_seq, std::move(observer)};
    }

    /**
     * Consume the use's sole operation-install authority for a header read.
     *
     * A false return means terminality already won or the token was already
     * consumed. Throwing synchronous initiation is classified as a write
     * failure by the substrate and then rethrown to the caller.
     */
    [[nodiscard]] static bool
    get_ledger_header(
        PeerUseCompletion const& completion,
        std::uint32_t ledger_seq,
        LedgerHeaderCallback callback,
        RequestOptions opts = {})
    {
        if (!callback)
            return false;
        auto delivery = std::make_shared<LedgerHeaderDelivery>(
            completion, std::move(callback));
        auto const installed = completion.install_operation(
            PeerUseKind::ledger_header,
            ledger_seq,
            [ledger_seq, delivery, opts](PeerSessionPtr session) mutable {
                session->get_ledger_header(
                    ledger_seq,
                    [delivery](
                        Error error, LedgerHeaderResult result) noexcept {
                        delivery->observe(error, std::move(result));
                    },
                    opts);
            });
        if (installed)
            delivery->installation_returned();
        return installed;
    }

private:
    struct PendingLedgerHeader
    {
        Error error = Error::Cancelled;
        LedgerHeaderResult result;
    };

    class LedgerHeaderDelivery
    {
    public:
        LedgerHeaderDelivery(
            PeerUseCompletion completion,
            LedgerHeaderCallback callback)
            : completion_(std::move(completion)), callback_(std::move(callback))
        {
        }

        void
        observe(Error error, LedgerHeaderResult result) noexcept
        {
            try
            {
                std::lock_guard lock(mutex_);
                //@@mutation-site peer-use-physical-callback-delivered-once
                if (delivered_)
                    return;
                //@@mutation-end peer-use-physical-callback-delivered-once
                if (!installation_returned_)
                {
                    // A well-formed session invokes at most once. Retaining
                    // only the first pre-return event also bounds a broken
                    // duplicate source while install is still active.
                    if (!pending_)
                    {
                        pending_.emplace(
                            PendingLedgerHeader{error, std::move(result)});
                    }
                    return;
                }
                deliver_locked(error, std::move(result));
            }
            catch (...)
            {
                completion_.with_physical_terminal_capability().finish(
                    PeerUseOutcome::cancelled);
            }
        }

        void
        installation_returned() noexcept
        {
            try
            {
                std::lock_guard lock(mutex_);
                installation_returned_ = true;
                if (!pending_)
                    return;
                auto pending = std::move(*pending_);
                pending_.reset();
                deliver_locked(pending.error, std::move(pending.result));
            }
            catch (...)
            {
                completion_.finish(PeerUseOutcome::cancelled);
            }
        }

    private:
        //@@authority transition=prv.T0256 status=provisional scope=function bind=catl::peer_client::PeerUseOperations::LedgerHeaderDelivery::deliver_locked@91bffaf26fb6f6e9
        // owner=unresolved owner_hypothesis=peer_use_adapter commit=delivery_mutex
        // touches=prv.S0094,prv.S0101
        // label=deliver_one_physical_callback_with_terminal_capability
        void
        deliver_locked(Error error, LedgerHeaderResult result) noexcept
        {
            if (delivered_)
                return;
            delivered_ = true;
            //@@mutation-site peer-use-revocation-makes-late-callback-inert
            if (!completion_.physical_delivery_allowed())
                return;
            //@@mutation-end peer-use-revocation-makes-late-callback-inert
            try
            {
                callback_(
                    completion_.with_physical_terminal_capability(),
                    error,
                    std::move(result));
            }
            catch (...)
            {
                completion_.with_physical_terminal_capability().finish(
                    PeerUseOutcome::cancelled);
            }
        }

        std::mutex mutex_;
        bool installation_returned_ = false;
        //@@authority state=prv.S0101 status=provisional scope=field bind=catl::peer_client::PeerUseOperations::LedgerHeaderDelivery::delivered_@107115f93eb5a3c7
        // owner=unresolved owner_hypothesis=peer_use_adapter
        // observed_home=peer_use_operation_delivery
        // label=one_shot_physical_callback_delivery
        bool delivered_ = false;
        std::optional<PendingLedgerHeader> pending_;
        PeerUseCompletion completion_;
        LedgerHeaderCallback callback_;
    };
};

}  // namespace catl::peer_client
