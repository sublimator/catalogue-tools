#pragma once

#include <catl/peer-client/peer-set.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
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
class NodeBatchTestSession : public catl::peer_client::PeerSession,
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
    std::uint32_t
    peer_first_seq() const override
    {
        return 1;
    }
    std::uint32_t
    peer_last_seq() const override
    {
        return 100;
    }

    /// Accepts publication and NEVER invokes the handler.
    ///
    /// That is not a convenience — it is PeerClient's duplicate-waiter-capacity
    /// behaviour exactly: admission succeeds, no receipt is ever delivered, and the
    /// sender is expected to replay once the original terminal receipt exists. A
    /// fake could not deliver a receipt anyway, since NodeBatchWriteReceipt's only
    /// friend is PeerClient. Controls that need a REAL receipt publish to a real
    /// PeerClient instead.
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
    set_node_response_handler(catl::peer_client::PeerSession::NodeResponseHandler) override
    {
    }
    void
    get_ledger_header(
        std::uint32_t,
        catl::peer_client::Callback<catl::peer_client::LedgerHeaderResult>,
        catl::peer_client::RequestOptions) override
    {
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
        int,
        std::vector<catl::peer_client::SHAMapNodeID> const&,
        std::function<void(boost::system::error_code)>) override
    {
    }

    void
    attach_strand(boost::asio::io_context& io)
    {
        strand_.emplace(boost::asio::make_strand(io));
    }

    bool ready = true;
    bool publish_accepts = true;
    std::size_t publish_calls = 0;

private:
    std::string endpoint_;
    std::optional<boost::asio::strand<boost::asio::io_context::executor_type>>
        strand_;
};
