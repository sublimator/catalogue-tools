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
};

}  // namespace catl::peer::monitor