#pragma once

// Builds a ProofChain from peer/RPC data.
//
// Connects to an XRPL peer, fetches the VL, collects validations,
// walks state/TX trees, and assembles a complete proof chain.

#include "proof-chain.h"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <catl/peer-client/peer-client.h>
#include <catl/xdata/protocol.h>
#include <cstdint>
#include <string>

namespace xproof {

/// Build a complete proof chain for a transaction.
/// Connects to peer, fetches VL, collects validations, walks trees.
/// Returns the proof chain + the publisher key used (for verification).
struct BuildResult
{
    ProofChain chain;
    std::string publisher_key_hex;  // from VL, for resolver
};

boost::asio::awaitable<BuildResult>
build_proof(
    boost::asio::io_context& io,
    std::string const& rpc_host,
    uint16_t rpc_port,
    std::string const& peer_host,
    uint16_t peer_port,
    std::string const& peer_cache_path,
    std::string const& tx_hash_str);

}  // namespace xproof
