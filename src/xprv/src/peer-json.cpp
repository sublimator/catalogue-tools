#include "xprv/peer-json.h"

#include <cctype>
#include <charconv>
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

std::optional<uint32_t>
parse_uint32_value(std::string_view value)
{
    uint32_t parsed = 0;
    auto const [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error != std::errc{} || end != value.data() + value.size())
        return std::nullopt;
    return parsed;
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
    if (filters.ledger.has_value())
        out["ledger"] = *filters.ledger;
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
        name == "Closed-Ledger" || name == "Previous-Ledger" || name == "Crawl";
}

}  // namespace

PeerSnapshotFilters
parse_peer_snapshot_filters(std::map<std::string, std::string> const& params)
{
    PeerSnapshotFilters filters;

    if (auto it = params.find("connected"); it != params.end())
        filters.connected = parse_bool_value(it->second);
    if (auto it = params.find("ledger"); it != params.end())
        filters.ledger = parse_uint32_value(it->second);

    return filters;
}

bool
matches_peer_snapshot_filters(
    catl::peer_client::PeerSet::SnapshotEntry const& peer,
    PeerSnapshotFilters const& filters)
{
    if (filters.connected.has_value() && peer.connected != *filters.connected)
        return false;
    if (filters.ledger.has_value() &&
        (peer.first_seq == 0 || peer.last_seq == 0 ||
         *filters.ledger < peer.first_seq || *filters.ledger > peer.last_seq))
    {
        return false;
    }

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
    body["hub_peers"] = snapshot.hub_peers;
    body["archival_peers"] = snapshot.archival_peers;
    body["in_flight_connects"] = snapshot.in_flight_connects;
    body["queued_connects"] = snapshot.queued_connects;
    body["crawl_in_flight"] = snapshot.crawl_in_flight;
    body["queued_crawls"] = snapshot.queued_crawls;
    body["targeted_connects"] = snapshot.targeted_connects;
    body["cached_endpoint_limit"] = snapshot.cached_endpoint_limit;
    body["max_targeted_connect_attempts"] =
        snapshot.max_targeted_connect_attempts;
    body["endpoint_cache_state"] = snapshot.endpoint_cache_state;
    if (!snapshot.endpoint_cache_error.empty())
        body["endpoint_cache_error"] = snapshot.endpoint_cache_error;

    body["reported_crawl_observation_batches"] =
        snapshot.crawl_observation_batches;
    body["reported_crawl_observation_count"] = snapshot.crawl_observation_count;
    boost::json::object crawl_truncation;
    crawl_truncation["peer_rows"] = snapshot.crawl_truncated_peer_rows;
    crawl_truncation["interval_rows"] = snapshot.crawl_truncated_interval_rows;
    crawl_truncation["public_keys"] = snapshot.crawl_truncated_public_keys;
    crawl_truncation["global_observations"] =
        snapshot.crawl_truncated_observations;
    crawl_truncation["invalid_public_keys"] =
        snapshot.crawl_invalid_public_keys;
    crawl_truncation["malformed_interval_tokens"] =
        snapshot.crawl_malformed_interval_tokens;
    crawl_truncation["capture_failures"] =
        snapshot.crawl_observation_capture_failures;
    body["reported_crawl_truncation"] = std::move(crawl_truncation);

    boost::json::array crawl_observations;
    for (auto const& observation : snapshot.reported_crawl_observations)
    {
        boost::json::object item;
        item["evidence"] = "reported_unverified";
        item["reporter_endpoint"] = observation.reporter_endpoint;
        item["subject_endpoint"] = observation.subject_endpoint;
        if (observation.reported_subject_key)
            item["reported_subject_key"] = *observation.reported_subject_key;
        item["observed_at"] = observation.observed_at;
        item["crawl_epoch"] = observation.crawl_epoch;
        item["truncated"] = observation.truncated;
        item["reported_key_conflict"] = observation.reported_key_conflict;
        boost::json::array intervals;
        for (auto const& interval : observation.intervals)
        {
            boost::json::object range;
            range["first_seq"] = interval.first_seq;
            range["last_seq"] = interval.last_seq;
            intervals.push_back(std::move(range));
        }
        item["reported_intervals"] = std::move(intervals);
        crawl_observations.push_back(std::move(item));
    }
    body["reported_crawl_observations"] = std::move(crawl_observations);

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
        item["targeted_connect"] = peer.targeted_connect;
        item["first_seq"] = peer.first_seq;
        item["last_seq"] = peer.last_seq;
        item["current_seq"] = peer.current_seq;
        if (peer.ready)
            item["range_source"] = "live_handshake";
        else if (peer.range_is_persistent_hint)
            item["range_source"] = "persistent_hint";
        else if (peer.first_seq != 0 && peer.last_seq != 0)
            item["range_source"] = "observed";
        else
            item["range_source"] = "unknown";
        item["last_seen_at"] = peer.last_seen_at;
        item["last_success_at"] = peer.last_success_at;
        item["last_failure_at"] = peer.last_failure_at;
        item["success_count"] = peer.success_count;
        item["failure_count"] = peer.failure_count;
        item["ledger_failure_count"] = peer.ledger_failure_count;
        item["recent_failed_ledgers"] = peer.recent_failed_ledgers;
        item["selection_count"] = peer.selection_count;
        item["last_selected_ticket"] = peer.last_selected_ticket;
        boost::json::array targeted_ledgers;
        for (auto ledger_seq : peer.targeted_ledgers)
            targeted_ledgers.push_back(ledger_seq);
        item["targeted_ledgers"] = std::move(targeted_ledgers);
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
