#pragma once

// A survey of one network's peers, measured over the peer protocol.
//
// Every endpoint gets the full peer handshake. A peer that completes it is
// timed with TMPing round trips and held open briefly so its TMEndpoints gossip
// arrives. A peer at capacity answers 503 with ten other peers. Every peer
// whose port answered is also asked for /crawl. Endpoints learned from gossip,
// redirects and crawl listings join the queue until nothing new turns up.

#include <boost/json.hpp>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace peer_seed {

struct SurveyOptions
{
    uint32_t network_id = 0;
    int concurrency = 32;
    int pings = 5;
    int timeout_secs = 5;
    int gossip_secs = 0;  // hold ready peers this long for TMEndpoints
    std::size_t max_peers = 20000;
    bool crawl = true;
    bool follow = true;  // queue what peers tell us about
    bool ipv6 = false;
};

struct PeerRecord
{
    std::string address;  // ip:port, or [ip]:port, as the bootcache spells it
    std::string source;   // hub, bootcache, crawl, gossip, redirect, argument
    std::string outcome;  // ready, full, wrong-network, unreachable, failed
    std::string detail;
    std::optional<double> handshake_ms;
    std::vector<double> ping_ms;
    std::string server;
    std::string public_key;
    std::optional<std::string> network_id;
    std::optional<uint32_t> first_ledger;
    std::optional<uint32_t> last_ledger;
    std::string crawl;  // ok, failed, skipped
    std::size_t crawl_listed = 0;
    std::size_t redirects = 0;
    std::size_t gossip = 0;
    boost::json::object geo;  // ip-api fields, with --geo

    std::optional<double>
    ping_median() const;

    /// The fastest round trip: queueing only ever adds delay, so on a busy
    /// link this is the best estimate of how near the peer is.
    std::optional<double>
    ping_min() const;
};

boost::json::object
to_json(PeerRecord const& record);

PeerRecord
record_from_json(boost::json::object const& object);

/// Normalize a discovered endpoint to the bootcache spelling, dropping
/// non-public addresses (and IPv6 ones unless allowed).
std::optional<std::string>
normalize_endpoint(std::string const& endpoint, bool allow_ipv6);

using RecordObserver = std::function<void(PeerRecord const&)>;

/// Survey from these start points. Each finished peer is passed to observer
/// as it completes; progress goes to stderr.
std::vector<PeerRecord>
run_survey(
    std::vector<std::pair<std::string, std::string>> const& seeds,
    SurveyOptions const& options,
    RecordObserver const& observer);

}  // namespace peer_seed
