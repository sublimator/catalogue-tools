#pragma once

#include <catl/peer/types.h>
#include <set>
#include <string>
#include <vector>

namespace catl::peer::monitor {

enum class MonitorMode {
    Monitor,  // Passive listening (Default)
    Query,    // Active query for specific objects
    Harvest   // Specialized data harvesting (e.g. manifests)
};

enum class ViewMode {
    Stream,    // Scrolling log output (stdout)
    Dashboard  // TUI Dashboard (FTXUI)
};

struct packet_filter
{
    std::set<int> show;
    std::set<int> hide;
};

struct monitor_config
{
    peer::peer_config peer;  // Primary peer
    std::vector<std::pair<std::string, std::uint16_t>> additional_peers;

    MonitorMode mode = MonitorMode::Monitor;
    ViewMode view = ViewMode::Stream;

    packet_filter filter;

    // Configuration
    bool enable_txset_acquire = false;

    // Query Mode params
    std::vector<std::string> query_tx_hashes;
};

}  // namespace catl::peer::monitor