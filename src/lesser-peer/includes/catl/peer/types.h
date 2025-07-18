#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

namespace catl::peer {

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using ssl_socket = asio::ssl::stream<tcp::socket>;

struct packet_stats
{
    std::uint64_t packet_count = 0;
    std::uint64_t total_bytes = 0;
};

using packet_counters = std::map<int, packet_stats>;

// Core peer connection configuration
struct peer_config
{
    std::string host;
    std::uint16_t port;
    bool listen_mode = false;

    // SSL/TLS config
    std::string cert_path = "listen.cert";
    std::string key_path = "listen.key";

    // Async config
    std::size_t io_threads = 1;
    std::chrono::seconds connection_timeout{30};

    // Protocol definitions
    std::string protocol_definitions_path;

    // Node identity (optional base58-encoded private key)
    std::optional<std::string> node_private_key;
};

enum class packet_type : std::uint16_t {
    manifests = 2,
    ping = 3,
    cluster = 5,
    endpoints = 15,
    transaction = 30,
    get_ledger = 31,
    ledger_data = 32,
    propose_ledger = 33,
    status_change = 34,
    have_set = 35,
    validation = 41,
    get_objects = 42,
    get_shard_info = 50,
    shard_info = 51,
    get_peer_shard_info = 52,
    peer_shard_info = 53,
    validator_list = 54,
    squelch = 55,
    validator_list_collection = 56,
    proof_path_req = 57,
    proof_path_response = 58,
    replay_delta_req = 59,
    replay_delta_response = 60,
    get_peer_shard_info_v2 = 61,
    peer_shard_info_v2 = 62,
    have_transactions = 63,
    transactions = 64,
    resource_report = 65
};

struct packet_header
{
    std::uint32_t payload_size;
    std::uint16_t type;
    bool compressed;
    std::uint32_t uncompressed_size;
};

}  // namespace catl::peer