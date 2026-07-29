#pragma once

#include "node-transport.h"
#include "types.h"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/system/error_code.hpp>

#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace catl::peer_client {

template <typename T>
using Callback = std::function<void(Error, T)>;

using UnsolicitedHandler =
    std::function<void(uint16_t type, std::vector<uint8_t> const& data)>;

/// Immutable terminal fact for one connection instance, formed exactly once
/// on the owning client strand when that instance dies — possibly after
/// transport came up but before PeerSet admission/Ready. The
/// receiving owner treats it as evidence, never as authority: delivery is an
/// in-process post whose loss is common-fate with the io_context.
struct PeerTerminalReason
{
    boost::system::error_code ec{};
    std::string detail;
};

/// One coherent TMStatusChange ledger-range publication.
///
/// The endpoints are packed into one atomic word by real PeerClient. Readers
/// must consume this value as a unit: separately published `first` and `last`
/// values can synthesize a range that the peer never advertised.
struct AdvertisedLedgerRange
{
    std::uint32_t first_seq = 0;
    std::uint32_t last_seq = 0;

    [[nodiscard]] constexpr bool
    valid() const noexcept
    {
        return first_seq != 0 && last_seq != 0 && first_seq <= last_seq;
    }

    [[nodiscard]] constexpr bool
    contains(std::uint32_t ledger_seq) const noexcept
    {
        return valid() && first_seq <= ledger_seq && ledger_seq <= last_seq;
    }

    [[nodiscard]] constexpr std::uint64_t
    packed() const noexcept
    {
        return (static_cast<std::uint64_t>(first_seq) << 32) | last_seq;
    }

    [[nodiscard]] static constexpr AdvertisedLedgerRange
    from_packed(std::uint64_t value) noexcept
    {
        return {
            .first_seq = static_cast<std::uint32_t>(value >> 32),
            .last_seq = static_cast<std::uint32_t>(value),
        };
    }

    friend constexpr bool
    operator==(AdvertisedLedgerRange const&, AdvertisedLedgerRange const&) =
        default;
};

struct NodeResponseObservation
{
    bool accepted = false;
    std::exception_ptr failure;
};

class PeerSession
{
public:
    using NodeResponseHandler =
        std::function<void(std::shared_ptr<protocol::TMLedgerData> const&)>;
    using NodeResponseObserver = std::function<NodeResponseObservation(
        std::shared_ptr<protocol::TMLedgerData> const&)>;

    virtual ~PeerSession() = default;

    virtual std::string const&
    endpoint() const = 0;

    virtual bool
    is_ready() const = 0;

    virtual uint32_t
    peer_ledger_seq() const = 0;

    virtual AdvertisedLedgerRange
    advertised_ledger_range() const = 0;

    virtual std::map<std::string, std::string>
    peer_headers() const
    {
        return {};
    }

    virtual size_t
    pending_count() const = 0;

    virtual void
    disconnect() = 0;

    /// Consume the physical session lifecycle on its owning executor.
    ///
    /// Completion is a receipt that every implementation-owned physical
    /// transport operation admitted before close has returned. It is stronger
    /// than disconnect(), whose thread-safe close projection may still be
    /// queued when that call returns.
    virtual boost::asio::awaitable<void>
    co_disconnect() = 0;

    virtual boost::asio::strand<boost::asio::io_context::executor_type>&
    strand() = 0;

    virtual void
    set_unsolicited_handler(UnsolicitedHandler handler) = 0;

    virtual void
    set_node_response_handler(NodeResponseHandler handler) = 0;

    /// Install an observing response sink. Existing PeerSession
    /// implementations remain source-compatible: their legacy void sink is
    /// used when they do not provide observation semantics.
    virtual void
    set_node_response_observer(NodeResponseObserver observer)
    {
        if (!observer)
        {
            set_node_response_handler({});
            return;
        }
        set_node_response_handler(
            [observer = std::move(observer)](
                std::shared_ptr<protocol::TMLedgerData> const&
                    message) mutable { (void)observer(message); });
    }

    virtual void
    get_ledger_header(
        uint32_t ledger_seq,
        Callback<LedgerHeaderResult> callback,
        RequestOptions opts = {}) = 0;

    virtual void
    get_ledger_header(
        Hash256 const& ledger_hash,
        Callback<LedgerHeaderResult> callback,
        RequestOptions opts = {}) = 0;

    virtual void
    get_state_nodes(
        Hash256 const& ledger_hash,
        std::vector<SHAMapNodeID> const& node_ids,
        Callback<LedgerNodesResult> callback,
        RequestOptions opts = {}) = 0;

    virtual void
    get_tx_nodes(
        Hash256 const& ledger_hash,
        std::vector<SHAMapNodeID> const& node_ids,
        Callback<LedgerNodesResult> callback,
        RequestOptions opts = {}) = 0;

    virtual void
    get_tx_proof_path(
        Hash256 const& ledger_hash,
        Hash256 const& key,
        Callback<ProofPathResult> callback,
        RequestOptions opts = {}) = 0;

    virtual void
    get_state_proof_path(
        Hash256 const& ledger_hash,
        Hash256 const& key,
        Callback<ProofPathResult> callback,
        RequestOptions opts = {}) = 0;

    virtual void
    ping(Callback<PingResult> callback, RequestOptions opts = {}) = 0;

    virtual void
    send_get_nodes(
        Hash256 const& ledger_hash,
        uint32_t ledger_seq,
        int type,
        std::vector<SHAMapNodeID> const& node_ids,
        std::function<void(boost::system::error_code)> on_write = nullptr) = 0;

    /// Publish one sealed node-batch projection. A true return means the
    /// projection was queued for owner-strand admission; the sender must retain
    /// its outbox until it consumes a receipt. Executor progress and exhaustive
    /// quiescence are lifecycle preconditions, not consequences of this call.
    /// A false return means no projection was queued.
    virtual bool
    submit_node_batch(
        NodeBatchCommand command,
        NodeBatchWriteHandler on_receipt) = 0;

    /// Queue retirement of one exact retained terminal receipt. Stale,
    /// duplicate, conflicting, and wrong-generation acknowledgements are
    /// fail-closed and do not retire another row. The sole sender executor must
    /// stop projecting the matching command before it derives this capability;
    /// command and acknowledgement submissions to this session are then
    /// ordered by the receiver strand. This causal rule is enforced by the
    /// coordinator outbox transition, not reconstructed by PeerSession.
    virtual bool
    acknowledge_node_batch_receipt(NodeBatchReceiptAck ack) = 0;
};

using PeerSessionPtr = std::shared_ptr<PeerSession>;

}  // namespace catl::peer_client
