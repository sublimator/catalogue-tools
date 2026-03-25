#pragma once

// Configuration loading with layering:
//   defaults → ~/.config/xproof/config.toml → XPROOF_* env vars → CLI flags
//
// Usage:
//   auto config = xproof::load_config();       // loads file + env
//   // then apply CLI flags on top in main.cpp

#include "commands.h"
#include <xproof/network-config.h>

#include <cstdint>
#include <optional>
#include <string>

namespace xproof {

/// Resolved configuration after layering.
/// CLI flags are applied by main.cpp after loading.
struct Config
{
    /// Network ID: 0 = XRPL mainnet, 21337 = Xahau mainnet.
    /// Determines which [network.<id>] section to load from config.toml.
    uint32_t network_id = 0;

    /// HTTP server bind address for `xproof serve`.
    std::string bind_address = "127.0.0.1";

    /// HTTP server port for `xproof serve`.
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

    /// Per-node fetch timeout in seconds. When a peer doesn't respond
    /// within this window, the request is retried on a different peer.
    /// Lower = faster failover, higher = more tolerant of slow peers.
    int fetch_timeout_secs = 5;

    /// Max concurrent RPC connections. Limits fd usage and avoids
    /// overwhelming the RPC endpoint under high load.
    int rpc_max_concurrent = 8;

    /// Path to the peer endpoint SQLite cache. Empty = platform default
    /// (~/.config/xproof/peer-endpoints.sqlite3).
    std::string peer_cache_path;

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

/// Find config file: $XDG_CONFIG_HOME/xproof/config.toml or
/// ~/.config/xproof/config.toml. Returns empty if not found.
std::string
config_file_path();

/// Load and resolve config:
///   1. Hard-coded defaults
///   2. config.toml (if present)
///   3. XPROOF_* env vars
///   4. NetworkConfig::for_network defaults for empty fields
/// CLI flags are NOT applied here.
Config
load_config(uint32_t network_id_hint = 0);

/// Populate ServeOptions from resolved Config.
ServeOptions
to_serve_options(Config const& config);

/// Populate NetworkConfig from resolved Config.
NetworkConfig
to_network_config(Config const& config);

/// Dump resolved config as TOML to a stream (cout, log, file, etc).
void
dump_config(Config const& config, std::ostream& os);

}  // namespace xproof
