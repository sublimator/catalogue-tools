#pragma once

#include <catl/peer-client/peer-set.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

/// A ready-by-default admitted session for NodeBatch bind controls.
///
/// Shared by the PeerSet bind suite and the coordinator suite. Extracted rather
/// than copied: two divergent fakes of the same interface is how a control ends
/// up proving something about its fixture instead of about the code.
///
/// Everything the bind path does not call is deliberately inert. A fake that
/// quietly did work here would make refusals ambiguous.
class NodeBatchTestSession
    : public catl::peer_client::PeerSession,
      public std::enable_shared_from_this<NodeBatchTestSession>
{
public:
    explicit NodeBatchTestSession(std::string endpoint)
        : endpoint_(std::move(endpoint))
    {
    }

    std::string const&
    endpoint() const override
    {
        return endpoint_;
    }
    bool
    is_ready() const override
    {
        return ready;
    }
    std::uint32_t
    peer_ledger_seq() const override
    {
        return 100;
    }
    catl::peer_client::AdvertisedLedgerRange
    advertised_ledger_range() const override
    {
        return {.first_seq = 1, .last_seq = 100};
    }

    /// Accepts publication and NEVER invokes the handler.
    ///
    /// That is not a convenience — it is PeerClient's duplicate-waiter-capacity
    /// behaviour exactly: admission succeeds, no receipt is ever delivered, and
    /// the sender is expected to replay once the original terminal receipt
    /// exists. A fake could not deliver a receipt anyway, since
    /// NodeBatchWriteReceipt's only friend is PeerClient. Controls that need a
    /// REAL receipt publish to a real PeerClient instead.
    bool
    submit_node_batch(
        catl::peer_client::NodeBatchCommand,
        catl::peer_client::NodeBatchWriteHandler) override
    {
        ++publish_calls;
        return publish_accepts;
    }
    bool
    acknowledge_node_batch_receipt(
        catl::peer_client::NodeBatchReceiptAck) override
    {
        return false;
    }

    std::size_t
    pending_count() const override
    {
        return 0;
    }
    void
    disconnect() override
    {
    }
    boost::asio::awaitable<void>
    co_disconnect() override
    {
        co_disconnect_calls.fetch_add(1, std::memory_order_relaxed);
        auto failures =
            co_disconnect_failures_remaining.load(std::memory_order_acquire);
        while (failures != 0 &&
               !co_disconnect_failures_remaining.compare_exchange_weak(
                   failures,
                   failures - 1,
                   std::memory_order_acq_rel,
                   std::memory_order_acquire))
        {
        }
        if (failures != 0)
            throw std::runtime_error("injected close receipt failure");
        auto gate = disconnect_receipt_gate_;
        if (gate)
        {
            co_disconnect_waiting.store(true, std::memory_order_release);
            boost::system::error_code ec;
            co_await gate->async_wait(
                boost::asio::redirect_error(boost::asio::use_awaitable, ec));
            co_disconnect_waiting.store(false, std::memory_order_release);
        }
        co_return;
    }
    boost::asio::strand<boost::asio::io_context::executor_type>&
    strand() override
    {
        return *strand_;
    }
    void
    set_unsolicited_handler(catl::peer_client::UnsolicitedHandler) override
    {
    }
    void
    set_node_response_handler(
        catl::peer_client::PeerSession::NodeResponseHandler) override
    {
    }
    void
    get_ledger_header(
        std::uint32_t,
        catl::peer_client::Callback<catl::peer_client::LedgerHeaderResult>
            callback,
        catl::peer_client::RequestOptions) override
    {
        ++header_calls;
        {
            std::unique_lock lock(header_submission_mutex_);
            if (hold_header_submission_)
            {
                header_submission_waiting.store(
                    true, std::memory_order_release);
                header_submission_cv_.notify_all();
                header_submission_cv_.wait(
                    lock, [this] { return release_header_submission_; });
                header_submission_waiting.store(
                    false, std::memory_order_release);
            }
        }
        if (header_submit_throws)
            throw std::runtime_error("injected header submission failure");
        if (header_callback_inline)
        {
            callback(header_inline_error, {});
            return;
        }
        header_callback = std::move(callback);
    }
    void
    get_ledger_header(
        Hash256 const&,
        catl::peer_client::Callback<catl::peer_client::LedgerHeaderResult>,
        catl::peer_client::RequestOptions) override
    {
    }
    void
    get_state_nodes(
        Hash256 const&,
        std::vector<catl::peer_client::SHAMapNodeID> const&,
        catl::peer_client::Callback<catl::peer_client::LedgerNodesResult>,
        catl::peer_client::RequestOptions) override
    {
    }
    void
    get_tx_nodes(
        Hash256 const&,
        std::vector<catl::peer_client::SHAMapNodeID> const&,
        catl::peer_client::Callback<catl::peer_client::LedgerNodesResult>,
        catl::peer_client::RequestOptions) override
    {
    }
    void
    get_tx_proof_path(
        Hash256 const&,
        Hash256 const&,
        catl::peer_client::Callback<catl::peer_client::ProofPathResult>,
        catl::peer_client::RequestOptions) override
    {
    }
    void
    get_state_proof_path(
        Hash256 const&,
        Hash256 const&,
        catl::peer_client::Callback<catl::peer_client::ProofPathResult>,
        catl::peer_client::RequestOptions) override
    {
    }
    void
    ping(
        catl::peer_client::Callback<catl::peer_client::PingResult>,
        catl::peer_client::RequestOptions) override
    {
    }
    void
    send_get_nodes(
        Hash256 const&,
        std::uint32_t,
        int,
        std::vector<catl::peer_client::SHAMapNodeID> const&,
        std::function<void(boost::system::error_code)> on_write) override
    {
        if (on_write)
            on_write({});
    }

    void
    attach_strand(boost::asio::io_context& io)
    {
        strand_.emplace(boost::asio::make_strand(io));
    }

    void
    hold_disconnect_receipt()
    {
        disconnect_receipt_gate_ = std::make_shared<boost::asio::steady_timer>(
            *strand_, boost::asio::steady_timer::time_point::max());
    }

    void
    hold_header_submission()
    {
        std::lock_guard lock(header_submission_mutex_);
        hold_header_submission_ = true;
        release_header_submission_ = false;
    }

    void
    release_header_submission()
    {
        {
            std::lock_guard lock(header_submission_mutex_);
            release_header_submission_ = true;
        }
        header_submission_cv_.notify_all();
    }

    void
    release_disconnect_receipt()
    {
        auto gate = disconnect_receipt_gate_;
        boost::asio::post(*strand_, [gate] {
            if (gate)
                gate->cancel();
        });
    }

    bool ready = true;
    bool publish_accepts = true;
    std::size_t publish_calls = 0;
    std::atomic<std::size_t> header_calls{0};
    std::atomic<bool> header_submission_waiting{false};
    bool header_submit_throws = false;
    bool header_callback_inline = false;
    catl::peer_client::Error header_inline_error =
        catl::peer_client::Error::Success;
    catl::peer_client::Callback<catl::peer_client::LedgerHeaderResult>
        header_callback;
    std::atomic<std::size_t> co_disconnect_calls{0};
    std::atomic<std::size_t> co_disconnect_failures_remaining{0};
    std::atomic<bool> co_disconnect_waiting{false};

private:
    std::string endpoint_;
    std::optional<boost::asio::strand<boost::asio::io_context::executor_type>>
        strand_;
    std::shared_ptr<boost::asio::steady_timer> disconnect_receipt_gate_;
    std::mutex header_submission_mutex_;
    std::condition_variable header_submission_cv_;
    bool hold_header_submission_ = false;
    bool release_header_submission_ = false;
};
