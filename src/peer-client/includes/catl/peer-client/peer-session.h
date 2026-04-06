#pragma once

#include "types.h"

#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

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

class PeerSession
{
public:
    using NodeResponseHandler =
        std::function<void(std::shared_ptr<protocol::TMLedgerData> const&)>;

    virtual ~PeerSession() = default;

    virtual std::string const&
    endpoint() const = 0;

    virtual bool
    is_ready() const = 0;

    virtual uint32_t
    peer_ledger_seq() const = 0;

    virtual uint32_t
    peer_first_seq() const = 0;

    virtual uint32_t
    peer_last_seq() const = 0;

    virtual std::map<std::string, std::string>
    peer_headers() const
    {
        return {};
    }

    virtual size_t
    pending_count() const = 0;

    virtual void
    disconnect() = 0;

    virtual boost::asio::strand<boost::asio::io_context::executor_type>&
    strand() = 0;

    virtual void
    set_unsolicited_handler(UnsolicitedHandler handler) = 0;

    virtual void
    set_node_response_handler(NodeResponseHandler handler) = 0;

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
        int type,
        std::vector<SHAMapNodeID> const& node_ids,
        std::function<void(boost::system::error_code)> on_error = nullptr) = 0;
};

using PeerSessionPtr = std::shared_ptr<PeerSession>;

}  // namespace catl::peer_client
