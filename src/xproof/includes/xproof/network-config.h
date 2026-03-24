#pragma once

// Network configuration for xproof.
//
// Resolves per-network defaults (XRPL mainnet, Xahau mainnet) so
// callers don't need to know VL hosts, bootstrap peers, or RPC
// endpoints for each chain.

#include <catl/peer-client/peer-set.h>
#include <catl/xdata/protocol.h>

#include <cstdint>
#include <string>
#include <vector>

namespace xproof {

struct NetworkConfig
{
    uint32_t network_id = 0;  // 0=XRPL mainnet, 21337=Xahau mainnet

    std::string vl_host;
    uint16_t vl_port = 443;
    std::string publisher_key;  // hex, trusted VL publisher master key

    std::string rpc_host;
    uint16_t rpc_port = 443;

    std::string peer_host;
    uint16_t peer_port = 51235;

    std::string peer_cache_path;  // empty = platform default
    std::vector<catl::peer_client::BootstrapPeer> extra_bootstrap;

    /// Fill any empty fields with defaults for this network_id.
    void
    apply_defaults();

    /// Load the correct protocol definitions for this network.
    catl::xdata::Protocol
    load_protocol() const;

    /// Create a config with all defaults for a network.
    static NetworkConfig
    for_network(uint32_t network_id);
};

}  // namespace xproof
