#pragma once

// Configuration loading with layering:
//   defaults → ~/.config/xprv/config.toml → XPRV_* env vars → CLI flags
//
// Usage:
//   auto config = xprv::load_config();       // loads file + env
//   // then apply CLI flags on top in main.cpp

#include <xprv/network-config.h>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

namespace xprv {

/// Resolved configuration after layering.
/// CLI flags are applied by main.cpp after loading.
struct Config
{
    /// Network ID: 0 = XRPL mainnet, 21337 = Xahau mainnet.
    /// Determines which [network.<id>] section to load from config.toml.
    uint32_t network_id = 0;

    /// HTTP server bind address for `xprv serve`.
    std::string bind_address = "127.0.0.1";

    /// HTTP server port for `xprv serve`.
    uint16_t port = 8080;

    /// Number of I/O threads. 1 = single-threaded (simplest).
    /// Higher values use per-object strands for thread safety.
    unsigned int threads = 1;

    /// Disable the proof-level LRU cache. When true, every /prove
    /// request rebuilds from scratch (node cache still active).
    bool no_cache = false;

    /// Max entries in the content-addressed SHAMap node cache.
    /// Each entry is ~500 bytes (inner) or ~8KB (leaf). Default 64K
    /// entries ≈ 32-64MB depending on tree shape.
    size_t node_cache_size = 65536;

    /// Per-caller timeout for each node fetch. Each waiter has their own
    /// deadline — timing out returns nullptr but does NOT erase the cache
    /// entry, so late-arriving responses still get cached. The walk_to
    /// caller retries with a different peer on timeout.
    /// Lower = faster failover, higher = more tolerant of slow peers.
    std::chrono::milliseconds fetch_timeout{1500};

    /// Max concurrent RPC connections. Limits fd usage and avoids
    /// overwhelming the RPC endpoint under high load.
    int rpc_max_concurrent = 8;

    /// Max peer retries per walk_to call. When a node fetch times out,
    /// the walk retries with a different peer up to this many times.
    int max_walk_peer_retries = 3;

    /// Stale in-flight multiplier. An in-flight cache entry is
    /// considered stale when now - last_fetch_at > fetch_timeout *
    /// this multiplier. A new caller will re-send the request.
    int fetch_stale_multiplier = 2;

    /// Path to the peer endpoint SQLite cache. Empty = platform default
    /// (~/.config/xprv/peer-endpoints.sqlite3).
    std::string peer_cache_path;

    // ── Peer management ─────────────────────────────────────────────

    /// Peer pool settings — separate presets for server vs CLI mode.
    struct PeerPool
    {
        size_t max_hub_peers = 20;
        size_t max_archival_peers = 5;
        size_t max_in_flight_connects = 8;
        size_t max_in_flight_crawls = 4;
    };

    PeerPool peers_server;  // [peers.server] — defaults for serve mode
    PeerPool peers_cli{5, 2, 4, 2};  // [peers.cli] — lighter defaults

    /// Ledger range span threshold for "archival" classification.
    /// Shared between server and CLI modes.
    uint32_t archival_range_threshold = 1'000'000;

    /// After this duration without finding a range-matching peer,
    /// fall back to any ready peer. 0ms = disable. Default 1000ms.
    std::chrono::milliseconds peer_fallback{1000};

    /// File descriptor soft limit for serve mode. 0 = don't touch.
    unsigned int fd_limit = 8192;

    /// RPC host for transaction lookups (e.g., "s1.ripple.com").
    std::string rpc_host;

    /// RPC port (typically 443 for HTTPS).
    uint16_t rpc_port = 0;

    /// Initial peer host for XRPL peer protocol connections.
    std::string peer_host;

    /// Initial peer port (typically 51235 for XRPL, 21337 for Xahau).
    uint16_t peer_port = 0;

    /// Validator list host (e.g., "vl.ripple.com").
    std::string vl_host;

    /// Validator list port (typically 443).
    uint16_t vl_port = 0;

    /// Trusted VL publisher master key (hex). Used to verify the UNL
    /// blob signature. Network-specific default from NetworkConfig.
    std::string publisher_key;
};

/// Find config file: $XDG_CONFIG_HOME/xprv/config.toml or
/// ~/.config/xprv/config.toml. Returns empty if not found.
std::string
config_file_path();

/// Load and resolve config:
///   1. Hard-coded defaults
///   2. config.toml (if present)
///   3. XPRV_* env vars
///   4. NetworkConfig::for_network defaults for empty fields
/// CLI flags are NOT applied here.
Config
load_config(uint32_t network_id_hint = 0);

/// Populate NetworkConfig from resolved Config.
NetworkConfig
to_network_config(Config const& config);

/// Dump resolved config as TOML to a stream (cout, log, file, etc).
void
dump_config(Config const& config, std::ostream& os);

}  // namespace xprv
