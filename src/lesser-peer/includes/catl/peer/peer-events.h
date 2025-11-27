#pragma once

#include "types.h"

#include <boost/system/error_code.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace catl::peer {

class peer_connection;

enum class PeerEventType { State, Packet, Stats, Lifecycle };

struct PeerStateEvent
{
    enum class State { Connecting, Connected, Disconnected, Error };

    State state;
    std::string message;
    boost::system::error_code error;
    std::shared_ptr<peer_connection> connection;
};

struct PeerPacketEvent
{
    std::shared_ptr<peer_connection> connection;
    packet_header header;
    std::vector<std::uint8_t> payload;
};

struct PeerStatsEvent
{
    packet_counters counters;
};

struct PeerLifecycleEvent
{
    enum class Action { Added, Removed };
    Action action;
};

using PeerEventData = std::variant<
    PeerStateEvent,
    PeerPacketEvent,
    PeerStatsEvent,
    PeerLifecycleEvent>;

struct PeerEvent
{
    std::string peer_id;
    PeerEventType type;
    std::chrono::steady_clock::time_point timestamp;
    PeerEventData data;
};

}  // namespace catl::peer
