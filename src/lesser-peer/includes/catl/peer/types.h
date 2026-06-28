#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
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

// Maximum accepted wire-frame payload size. The on-wire length field is
// 28 bits (max ~256 MiB); an untrusted peer could otherwise force a
// quarter-gigabyte allocation per frame. 16 MiB is ~2x the largest message
// realistically received and keeps aggregate memory survivable across many
// peers. Mirrors peer-client's kMaxFramePayloadSize (see its rationale).
inline constexpr std::uint32_t kMaxFramePayloadSize = 16u * 1024 * 1024;

// Process-wide ceiling on the SUM of in-flight inbound frame buffers across
// all peer connections (security #0055). The per-frame cap above bounds a
// single frame; this bounds a coordinated fill across many peers. Mirrors
// peer-client's InboundBudget (see its rationale).
inline constexpr std::size_t kAggregateInboundBudget = 256u * 1024 * 1024;

// Lock-free tracker of total in-flight inbound-buffer bytes against a ceiling;
// one process-wide instance via global_inbound_budget(). See peer-client.
class InboundBudget
{
public:
    explicit InboundBudget(std::size_t ceiling) : ceiling_(ceiling)
    {
    }

    // Reserve n bytes iff that keeps the running total within the ceiling
    // (overflow-safe). Reserves nothing on failure.
    bool
    try_acquire(std::size_t n)
    {
        std::size_t cur = in_flight_.load(std::memory_order_relaxed);
        do
        {
            if (n > ceiling_ - cur)
                return false;
        } while (!in_flight_.compare_exchange_weak(
            cur, cur + n, std::memory_order_relaxed));
        return true;
    }

    void
    release(std::size_t n)
    {
        in_flight_.fetch_sub(n, std::memory_order_relaxed);
    }

    std::size_t
    in_flight() const
    {
        return in_flight_.load(std::memory_order_relaxed);
    }

    std::size_t
    ceiling() const
    {
        return ceiling_;
    }

private:
    std::size_t const ceiling_;
    std::atomic<std::size_t> in_flight_{0};
};

inline InboundBudget&
global_inbound_budget()
{
    static InboundBudget budget(kAggregateInboundBudget);
    return budget;
}

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

    // Network identifier (Xahau Testnet=21338, Mainnet=21337)
    std::uint32_t network_id = 21338;
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
