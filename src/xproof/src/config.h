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
    uint32_t network_id = 0;

    // Server
    std::string bind_address = "127.0.0.1";
    uint16_t port = 8080;
    unsigned int threads = 1;

    // Cache
    bool no_cache = false;
    size_t node_cache_size = 65536;
    std::string peer_cache_path;

    // Network endpoints (resolved for active network)
    std::string rpc_host;
    uint16_t rpc_port = 0;
    std::string peer_host;
    uint16_t peer_port = 0;
    std::string vl_host;
    uint16_t vl_port = 0;
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

}  // namespace xproof
