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
#include <catl/peer-client/node-cache.h>
#include <catl/peer-client/peer-set.h>
#include <catl/rpc-client/rpc-client-coro.h>
#include <catl/vl-client/vl-client.h>
#include <catl/xdata/protocol.h>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace xprv {

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
    std::shared_ptr<catl::peer_client::NodeCache> node_cache;
    std::shared_ptr<catl::rpc::RpcClient> rpc;

    // Cached tx → ledger_seq (0 = not cached, must do RPC lookup)
    uint32_t tx_ledger_seq_hint = 0;

    // Pre-built anchor bundle — shared across concurrent proves
    catl::peer_client::LedgerHeaderResult anchor_hdr;
    Hash256 anchor_hash;
    Hash256 anchor_account_hash;  // for state walks

    // Cancel token — set by HTTP session on client disconnect.
    // Checked at cancellation boundaries (walk_to, with_peer_failover)
    // to stop work without disrupting shared NodeCache state.
    std::shared_ptr<std::atomic<bool>> cancel_token;

    /// Optional SSE step callback — co_awaited with each step as it
    /// becomes ready during the build. If null, steps are only returned
    /// in the final BuildResult (batch mode). The callback receives the
    /// step variant; caller is responsible for serialization and writing.
    using StepCallback =
        std::function<boost::asio::awaitable<void>(ChainStep const&)>;
    StepCallback on_step;
};

/// Build using shared services (ProofEngine path).
boost::asio::awaitable<BuildResult>
build_proof(BuildServices svc, std::string const& tx_hash_str);

// Legacy self-contained build_proof removed — all callers use
// build_proof(BuildServices) via ProofEngine.

}  // namespace xprv
