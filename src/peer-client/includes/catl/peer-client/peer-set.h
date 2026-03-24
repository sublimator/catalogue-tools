#pragma once

// PeerSet — manages multiple peer connections with ledger range awareness.
//
// Wraps EndpointTracker (knows ranges) with live PeerClient connections.
// peer_for(ledger_seq) returns a connected peer that has the requested
// ledger, connecting on demand if needed.
//
// Usage:
//   PeerSet peers(io);
//   co_await peers.add("s1.ripple.com", 51235);  // connect + learn range
//   auto client = co_await peers.peer_for(103053705);
//   auto hdr = co_await co_get_ledger_header(client, 103053705);

#include "endpoint-tracker.h"
#include "peer-client.h"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <map>
#include <memory>
#include <string>

namespace catl::peer_client {

/// Well-known bootstrap peers by network ID.
struct BootstrapPeer
{
    std::string host;
    uint16_t port;
};

/// Get bootstrap peers for a network.
std::vector<BootstrapPeer> const&
get_bootstrap_peers(uint32_t network_id);

class PeerSet
{
public:
    explicit PeerSet(boost::asio::io_context& io, uint32_t network_id = 0);

    /// The shared tracker — peers feed it via TMStatusChange.
    std::shared_ptr<EndpointTracker>
    tracker()
    {
        return tracker_;
    }

    /// Connect to a peer and add it to the set.
    /// Returns the PeerClient once ready.
    boost::asio::awaitable<std::shared_ptr<PeerClient>>
    add(std::string const& host, uint16_t port);

    /// Connect to all bootstrap peers for the configured network.
    /// Connects in parallel, waits for all to complete (or fail).
    /// Returns number of successful connections.
    boost::asio::awaitable<int>
    bootstrap();

    /// Get a connected peer that has the given ledger sequence.
    /// Checks existing connections only. Returns nullopt if none found.
    std::optional<std::shared_ptr<PeerClient>>
    peer_for(uint32_t ledger_seq) const;

    /// Wait until a peer with the given ledger sequence is available.
    /// Polls existing connections every second. New connections may
    /// appear from bootstrap() or TMEndpoints gossip in the background.
    /// Returns nullopt on timeout.
    boost::asio::awaitable<std::optional<std::shared_ptr<PeerClient>>>
    wait_for_peer(uint32_t ledger_seq, int timeout_secs = 60);

    /// Get any ready peer (e.g. for current-ledger operations).
    std::shared_ptr<PeerClient>
    any_peer() const;

    /// Number of active connections.
    size_t
    size() const
    {
        return connections_.size();
    }

    /// Set an unsolicited handler on ALL current and future connections.
    void
    set_unsolicited_handler(UnsolicitedHandler handler)
    {
        unsolicited_handler_ = std::move(handler);
        for (auto& [_, client] : connections_)
        {
            client->set_unsolicited_handler(unsolicited_handler_);
        }
    }

private:
    boost::asio::io_context& io_;
    uint32_t network_id_;
    std::shared_ptr<EndpointTracker> tracker_;
    std::map<std::string, std::shared_ptr<PeerClient>> connections_;
    UnsolicitedHandler unsolicited_handler_;

    static std::string
    make_key(std::string const& host, uint16_t port)
    {
        return host + ":" + std::to_string(port);
    }

    /// Try to connect to an endpoint. Returns nullptr on failure.
    boost::asio::awaitable<std::shared_ptr<PeerClient>>
    try_connect(std::string const& host, uint16_t port);
};

}  // namespace catl::peer_client
