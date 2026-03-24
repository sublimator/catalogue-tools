#pragma once

// Builds a ProofChain from peer/RPC data.
//
// Two modes:
//   1. build_proof(io, host, port, ...) — self-contained, creates its own
//      PeerSet/VL/collector. Used by dev commands and legacy path.
//   2. build_proof(services, ...) — borrows shared services from ProofEngine.
//      Used by the engine for persistent peer pool + cached VL.

#include "proof-chain.h"
#include "validation-buffer.h"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <catl/peer-client/peer-set.h>
#include <catl/rpc-client/rpc-client-coro.h>
#include <catl/vl-client/vl-client.h>
#include <catl/xdata/protocol.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace xproof {

struct BuildResult
{
    ProofChain chain;
    std::string publisher_key_hex;  // from VL, for resolver
    uint32_t tx_ledger_seq = 0;
};

/// Shared services for build_proof — borrowed from ProofEngine.
struct BuildServices
{
    boost::asio::io_context& io;
    std::shared_ptr<catl::peer_client::PeerSet> peers;
    catl::vl::ValidatorList const& vl;
    std::vector<ValidationCollector::Entry> const& anchor_validations;
    catl::xdata::Protocol const& protocol;
    std::string rpc_host;
    uint16_t rpc_port = 443;
};

/// Build using shared services (ProofEngine path).
boost::asio::awaitable<BuildResult>
build_proof(BuildServices const& svc, std::string const& tx_hash_str);

/// Self-contained build (legacy/dev path). Creates its own PeerSet, VL,
/// validation collector.
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
