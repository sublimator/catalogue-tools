#pragma once

// ProofEngine — reusable core that owns persistent shared state.
//
// Both the CLI (xproof prove) and the HTTP server (xproof serve)
// are thin wrappers around this. create() + start() pattern:
// observer registration happens in start(), not the constructor,
// because shared_from_this() is unsafe in constructors.
//
// Usage:
//   auto engine = ProofEngine::create(io, NetworkConfig::for_network(0));
//   engine->start();
//   auto chain = co_await engine->prove(tx_hash);

#include "network-config.h"
#include "proof-chain.h"
#include "validation-buffer.h"
#include "vl-cache.h"

#include <catl/peer-client/peer-set.h>
#include <catl/xdata/protocol.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <memory>
#include <span>
#include <string>

namespace xproof {

struct VerifyResult
{
    bool ok = false;
    std::string error;
};

class ProofEngine : public std::enable_shared_from_this<ProofEngine>
{
public:
    static std::shared_ptr<ProofEngine>
    create(boost::asio::io_context& io, NetworkConfig config);

    /// Start all background services (peer bootstrap, VL fetch, validation
    /// collection). Must be called after create().
    void
    start();

    /// Result from prove() — chain + metadata the caller needs.
    struct ProveResult
    {
        ProofChain chain;
        uint32_t tx_ledger_seq = 0;
        std::string publisher_key_hex;  // actual key from VL fetch
    };

    /// Build a proof chain for a transaction. Safe to call concurrently
    /// (each call runs its own coroutine, borrows shared services).
    boost::asio::awaitable<ProveResult>
    prove(std::string const& tx_hash);

    /// Verify a proof chain. Sync pure function — no shared state.
    VerifyResult
    verify(std::span<const uint8_t> data, std::string const& trusted_key = "");

    /// Engine status for health checks. Awaitable — hops to each
    /// service strand to collect a snapshot.
    struct Status
    {
        size_t peer_count = 0;
        bool vl_loaded = false;
        std::optional<uint32_t> latest_quorum_seq;
    };
    boost::asio::awaitable<Status>
    co_health();

    /// Access shared services (for advanced usage / testing).
    std::shared_ptr<catl::peer_client::PeerSet>
    peers()
    {
        return peers_;
    }
    std::shared_ptr<VlCache>
    vl_cache()
    {
        return vl_cache_;
    }
    std::shared_ptr<ValidationBuffer>
    val_buffer()
    {
        return val_buffer_;
    }

    NetworkConfig const&
    config() const
    {
        return config_;
    }

    catl::xdata::Protocol const&
    protocol() const
    {
        return protocol_;
    }

private:
    ProofEngine(boost::asio::io_context& io, NetworkConfig config);

    boost::asio::io_context& io_;
    NetworkConfig config_;
    catl::xdata::Protocol protocol_;

    std::shared_ptr<catl::peer_client::PeerSet> peers_;
    std::shared_ptr<VlCache> vl_cache_;
    std::shared_ptr<ValidationBuffer> val_buffer_;
};

}  // namespace xproof
