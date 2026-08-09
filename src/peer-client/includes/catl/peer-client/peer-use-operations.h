#pragma once

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
                completion_.finish(PeerUseOutcome::cancelled);
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
        void
        deliver_locked(Error error, LedgerHeaderResult result) noexcept
        {
            try
            {
                callback_(completion_, error, std::move(result));
            }
            catch (...)
            {
                completion_.finish(PeerUseOutcome::cancelled);
            }
        }

        std::mutex mutex_;
        bool installation_returned_ = false;
        std::optional<PendingLedgerHeader> pending_;
        PeerUseCompletion completion_;
        LedgerHeaderCallback callback_;
    };
};

}  // namespace catl::peer_client
