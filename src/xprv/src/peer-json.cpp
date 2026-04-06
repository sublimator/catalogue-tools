#include "xprv/peer-json.h"

#include <cctype>
#include <string_view>

namespace xprv {

namespace {

std::string
to_lower_copy(std::string_view s)
{
    std::string out(s);
    for (auto& c : out)
    {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return out;
}

std::optional<bool>
parse_bool_value(std::string_view value)
{
    auto const normalized = to_lower_copy(value);
    if (normalized == "1" || normalized == "true" || normalized == "yes" ||
        normalized == "on")
    {
        return true;
    }
    if (normalized == "0" || normalized == "false" || normalized == "no" ||
        normalized == "off")
    {
        return false;
    }
    return std::nullopt;
}

std::string
network_slug_for_id(uint32_t network_id)
{
    switch (network_id)
    {
        case 0:
            return "xrpl-mainnet";
        case 21337:
            return "xahau-mainnet";
        default:
            return std::to_string(network_id);
    }
}

boost::json::object
filters_to_json(PeerSnapshotFilters const& filters)
{
    boost::json::object out;
    if (filters.connected.has_value())
        out["connected"] = *filters.connected;
    return out;
}

bool
is_public_peer_header(std::string const& name)
{
    // `/peers` is intended to be safe for public exposure. Keep this to
    // operationally useful handshake metadata and do not echo per-connection
    // identifiers or addressing details here. If we ever need a fuller dump,
    // add a separate localhost/debug-only path instead of widening this list.
    return name == "Upgrade" || name == "Server" || name == "Network-ID" ||
        name == "X-Protocol-Ctl" || name == "Public-Key" ||
        name == "Closed-Ledger" || name == "Previous-Ledger" ||
        name == "Crawl";
}

}  // namespace

PeerSnapshotFilters
parse_peer_snapshot_filters(std::map<std::string, std::string> const& params)
{
    PeerSnapshotFilters filters;

    if (auto it = params.find("connected"); it != params.end())
        filters.connected = parse_bool_value(it->second);

    return filters;
}

bool
matches_peer_snapshot_filters(
    catl::peer_client::PeerSet::SnapshotEntry const& peer,
    PeerSnapshotFilters const& filters)
{
    if (filters.connected.has_value() && peer.connected != *filters.connected)
        return false;

    return true;
}

boost::json::object
peer_snapshot_to_json(
    catl::peer_client::PeerSet::Snapshot const& snapshot,
    std::optional<uint32_t> network_id,
    PeerSnapshotFilters const& filters,
    bool include_filters)
{
    boost::json::object body;
    if (network_id.has_value())
    {
        body["network_id"] = *network_id;
        body["network"] = network_slug_for_id(*network_id);
    }
    if (include_filters && !filters.empty())
        body["filters"] = filters_to_json(filters);

    body["known_endpoints"] = snapshot.known_endpoints;
    body["tracked_endpoints"] = snapshot.tracked_endpoints;
    body["connected_peers"] = snapshot.connected_peers;
    body["ready_peers"] = snapshot.ready_peers;
    body["in_flight_connects"] = snapshot.in_flight_connects;
    body["queued_connects"] = snapshot.queued_connects;
    body["crawl_in_flight"] = snapshot.crawl_in_flight;
    body["queued_crawls"] = snapshot.queued_crawls;

    boost::json::array wanted_ledgers;
    for (auto ledger_seq : snapshot.wanted_ledgers)
        wanted_ledgers.push_back(ledger_seq);
    body["wanted_ledgers"] = std::move(wanted_ledgers);

    boost::json::array peers;
    for (auto const& peer : snapshot.peers)
    {
        if (!matches_peer_snapshot_filters(peer, filters))
            continue;

        boost::json::object item;
        item["endpoint"] = peer.endpoint;
        item["connected"] = peer.connected;
        item["ready"] = peer.ready;
        item["in_flight"] = peer.in_flight;
        item["queued_connect"] = peer.queued_connect;
        item["crawl_in_flight"] = peer.crawl_in_flight;
        item["queued_crawl"] = peer.queued_crawl;
        item["crawled"] = peer.crawled;
        item["first_seq"] = peer.first_seq;
        item["last_seq"] = peer.last_seq;
        item["current_seq"] = peer.current_seq;
        item["last_seen_at"] = peer.last_seen_at;
        item["last_success_at"] = peer.last_success_at;
        item["last_failure_at"] = peer.last_failure_at;
        item["success_count"] = peer.success_count;
        item["failure_count"] = peer.failure_count;
        item["selection_count"] = peer.selection_count;
        item["last_selected_ticket"] = peer.last_selected_ticket;
        if (!peer.peer_headers.empty())
        {
            boost::json::object headers;
            for (auto const& [name, value] : peer.peer_headers)
            {
                if (is_public_peer_header(name))
                    headers[name] = value;
            }
            if (!headers.empty())
                item["headers"] = std::move(headers);
        }
        peers.push_back(std::move(item));
    }

    body["returned_peers"] = peers.size();
    body["peers"] = std::move(peers);
    return body;
}

}  // namespace xprv
