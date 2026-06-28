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

namespace catl::peer_client {

// Maximum accepted wire-frame payload size. The on-wire length field is
// 28 bits (max ~256 MiB); an untrusted peer could set it arbitrarily and
// force a quarter-gigabyte allocation per frame.
//
// 16 MiB is ~2x headroom over the largest message this read-only client
// realistically receives (a TMLedgerData node batch ≈ rippled's
// maxReplyNodes × ~932 B ≲ 8 MiB), so it never rejects legitimate traffic.
// It also keeps AGGREGATE memory survivable: payload_buffer_ is one
// in-flight buffer per connection, so a coordinated fill across ~20 peers
// is 20 × 16 = 320 MiB, under a 512 MiB instance — whereas 64 MiB would be
// 1.28 GiB. (A hard aggregate inbound budget is the complete fix for the
// multi-peer vector; tracked as a follow-up in security issue #0055.)
inline constexpr std::uint32_t kMaxFramePayloadSize = 16u * 1024 * 1024;

// Maximum node entries accepted in a single TMLedgerData response. The frame
// cap bounds the message to 16 MiB, but a densely-packed message could still
// encode ~millions of tiny repeated node entries; iterating them builds an
// O(N) nodeid vector and runs an O(N × pending × requested) match
// (peer-client.cpp find_node_key) — CPU + transient-memory amplification.
// Our tree-walk only ever requests small node batches, so a few thousand is
// far above any legitimate reply (generous vs rippled's reply-node limit).
inline constexpr int kMaxLedgerReplyNodes = 8192;

// Process-wide ceiling on the SUM of in-flight inbound frame buffers across
// ALL peer connections (security issue #0055). The per-connection 16 MiB
// kMaxFramePayloadSize bounds a single frame, but a coordinated fill across
// many peers (each holding a max-size frame mid-read) could still pressure
// memory — 20 peers × 16 MiB = 320 MiB. This caps the aggregate so the total
// stays well under a 512 MiB instance regardless of peer count. 256 MiB = 16
// concurrent max-size frames, far above legitimate traffic (the largest reply,
// a TMLedgerData node batch, is ≲ 8 MiB and rarely concurrent).
inline constexpr std::size_t kAggregateInboundBudget = 256u * 1024 * 1024;

// Tracks total in-flight inbound-buffer bytes against a ceiling. A connection
// reserves a frame's payload_size before allocating its buffer and releases it
// once the frame is dispatched (or the connection tears down); when the
// ceiling would be exceeded the allocation is refused (the connection is
// dropped) instead of growing memory without bound. Lock-free / thread-safe;
// one process-wide instance via global_inbound_budget().
class InboundBudget
{
public:
    explicit InboundBudget(std::size_t ceiling) : ceiling_(ceiling)
    {
    }

    // Reserve n bytes iff that keeps the running total within the ceiling.
    // Returns true and reserves on success; returns false and reserves
    // nothing on failure. Overflow-safe (n is checked against headroom).
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
        // Clamp so a stray double-release can't underflow in_flight_ and wrap
        // to ~SIZE_MAX — which would silently DISABLE the budget, since
        // try_acquire's `n > ceiling_ - cur` headroom check would then always
        // pass. Release is exactly-once today (#0055); this is defense-in-depth
        // (sec #0054).
        std::size_t cur = in_flight_.load(std::memory_order_relaxed);
        std::size_t dec;
        do
        {
            dec = n < cur ? n : cur;
        } while (!in_flight_.compare_exchange_weak(
            cur, cur - dec, std::memory_order_relaxed));
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

// The process-wide inbound budget shared by every peer connection.
inline InboundBudget&
global_inbound_budget()
{
    static InboundBudget budget(kAggregateInboundBudget);
    return budget;
}

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using strand_type = asio::strand<asio::io_context::executor_type>;
using strand_tcp_socket = asio::basic_stream_socket<tcp, strand_type>;
using ssl_socket = asio::ssl::stream<strand_tcp_socket>;

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

}  // namespace catl::peer_client
