#pragma once

#include <catl/peer-client/peer-set.h>

#include <boost/json.hpp>

#include <map>
#include <optional>
#include <string>

namespace xprv {

struct PeerSnapshotFilters
{
    std::optional<bool> connected;

    [[nodiscard]] bool
    empty() const
    {
        return !connected.has_value();
    }
};

PeerSnapshotFilters
parse_peer_snapshot_filters(std::map<std::string, std::string> const& params);

bool
matches_peer_snapshot_filters(
    catl::peer_client::PeerSet::SnapshotEntry const& peer,
    PeerSnapshotFilters const& filters);

boost::json::object
peer_snapshot_to_json(
    catl::peer_client::PeerSet::Snapshot const& snapshot,
    std::optional<uint32_t> network_id,
    PeerSnapshotFilters const& filters,
    bool include_filters);

}  // namespace xprv
