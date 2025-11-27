#pragma once

#include "catl/peer/peer-connection.h"
#include "catl/peer/peer-events.h"

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace catl::peer {

class PeerEventBus
{
public:
    using SubscriberId = std::uint64_t;
    using Callback = std::function<void(PeerEvent const&)>;

    SubscriberId
    subscribe(Callback cb);

    void
    unsubscribe(SubscriberId id);

    void
    publish(PeerEvent const& event) const;

private:
    mutable std::mutex mutex_;
    std::unordered_map<SubscriberId, Callback> subscribers_;
    std::atomic<SubscriberId> next_id_{1};
};

class PeerSession : public std::enable_shared_from_this<PeerSession>
{
public:
    PeerSession(
        std::string id,
        asio::io_context& io_context,
        asio::ssl::context& ssl_context,
        peer_config config,
        std::shared_ptr<PeerEventBus> bus);

    void
    start();

    void
    stop();

    std::string const&
    id() const
    {
        return id_;
    }

    std::string
    remote_endpoint() const;

    bool
    is_connected() const
    {
        return connected_;
    }

    std::shared_ptr<peer_connection>
    connection()
    {
        return connection_;
    }

    // Lifecycle events for manager to surface add/remove
    void
    publish_lifecycle(PeerLifecycleEvent::Action action);

private:
    void
    handle_connect_result(boost::system::error_code const& ec);

    void
    publish_state(PeerStateEvent::State state, std::string const& message = {});

    void
    publish_state_error(boost::system::error_code const& ec);

    void
    publish_packet(
        packet_header const& header,
        std::vector<std::uint8_t> payload);

    void
    publish_stats();

private:
    std::string id_;
    peer_config config_;
    asio::io_context& io_context_;
    asio::ssl::context& ssl_context_;
    std::shared_ptr<peer_connection> connection_;
    std::shared_ptr<PeerEventBus> bus_;
    bool started_{false};
    bool connected_{false};
    packet_counters counters_;
};

class PeerManager
{
public:
    PeerManager(
        asio::io_context& io_context,
        asio::ssl::context& ssl_context,
        std::shared_ptr<PeerEventBus> bus);

    std::string
    add_peer(peer_config config);

    void
    remove_peer(std::string const& peer_id);

    void
    stop_all();

    std::vector<std::string>
    peer_ids() const;

    std::shared_ptr<PeerEventBus>
    bus() const
    {
        return bus_;
    }

private:
    asio::io_context& io_context_;
    asio::ssl::context& ssl_context_;
    std::shared_ptr<PeerEventBus> bus_;
    std::atomic<std::uint64_t> next_peer_id_{1};
    std::map<std::string, std::shared_ptr<PeerSession>> sessions_;
    mutable std::mutex mutex_;
};

}  // namespace catl::peer
