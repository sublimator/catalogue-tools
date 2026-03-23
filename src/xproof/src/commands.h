#pragma once

#include <catl/xdata/protocol.h>

#include <cstdint>
#include <string>

// Shared helpers and command declarations for xproof CLI.

bool
parse_endpoint(std::string const& endpoint, std::string& host, uint16_t& port);

int
cmd_ping(std::string const& endpoint);

int
cmd_header(std::string const& endpoint, uint32_t ledger_seq);

struct ProveOptions
{
    std::string rpc_endpoint;
    std::string peer_endpoint;
    std::string tx_hash;
    bool binary = false;
    bool compress = false;
    std::string output = "proof";  // base name, extension added by format
};

int
cmd_prove(ProveOptions const& opts, catl::xdata::Protocol const& protocol);

int
cmd_verify(
    std::string const& proof_path,
    std::string const& trusted_key,
    catl::xdata::Protocol const& protocol);

int
cmd_dev_check_ledger(
    std::string const& endpoint,
    catl::xdata::Protocol const& protocol);

int
cmd_dev_tx(
    std::string const& endpoint,
    std::string const& tx_hash,
    catl::xdata::Protocol const& protocol);
