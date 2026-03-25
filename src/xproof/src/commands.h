#pragma once

#include <catl/xdata/protocol.h>

#include <cstdint>
#include <string>

// Shared helpers and command declarations for xproof CLI.

// Well-known defaults (kept for backward compat in ping/header/dev commands)
inline constexpr auto DEFAULT_RPC = "s1.ripple.com:443";
inline constexpr auto DEFAULT_PEER = "s1.ripple.com:51235";
inline constexpr auto DEFAULT_PUBLISHER_KEY =
    "ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734";

bool
parse_endpoint(std::string const& endpoint, std::string& host, uint16_t& port);

int
cmd_ping(std::string const& endpoint);

int
cmd_header(std::string const& endpoint, uint32_t ledger_seq);

struct ProveOptions
{
    uint32_t network_id = 0;
    std::string rpc_endpoint;   // empty = use NetworkConfig default
    std::string peer_endpoint;  // empty = use NetworkConfig default
    std::string peer_cache_path;
    std::string tx_hash;
    bool binary = false;
    bool compress = false;
    std::string output;
};

int
cmd_prove(ProveOptions const& opts, catl::xdata::Protocol const& protocol);

int
cmd_verify(
    std::string const& proof_path,
    std::string const& trusted_key,
    catl::xdata::Protocol const& protocol);

// cmd_serve takes Config directly — no intermediate ServeOptions.
// Config has everything: network, peers, cache, server, fd_limit.

int
cmd_serve();

// Dev commands (unlisted)
int
cmd_dev_check_ledger(
    std::string const& endpoint,
    catl::xdata::Protocol const& protocol);

int
cmd_dev_tx(
    std::string const& endpoint,
    std::string const& tx_hash,
    catl::xdata::Protocol const& protocol);
