#pragma once

#include <catl/peer/types.h>
#include <set>

namespace catl::peer::monitor {

struct display_config
{
    bool use_cls = true;
    bool no_dump = false;
    bool slow = false;
    bool raw_hex = false;
    bool no_stats = false;
    bool no_hex = false;
    bool no_json = false;
    bool manifests_only = false;
    bool no_http = false;
    bool use_dashboard = false;  // Enable FTXUI dashboard with log redirection
    bool query_mode =
        false;  // Special mode for transaction queries - only show results
};

struct packet_filter
{
    std::set<int> show;
    std::set<int> hide;
};

struct monitor_config
{
    peer::peer_config peer;
    display_config display;
    packet_filter filter;

    // Transaction queries
    std::vector<std::string>
        query_tx_hashes;  // List of transaction hashes to query
};

}  // namespace catl::peer::monitor