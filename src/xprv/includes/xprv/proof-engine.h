#pragma once

// ProofEngine — reusable core that owns persistent shared state.
//
// Both the CLI (xprv prove) and the HTTP server (xprv serve)
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

#include <catl/peer-client/node-cache.h>
#include <catl/peer-client/peer-set.h>
#include <catl/rpc-client/rpc-client.h>
#include <catl/xdata/protocol.h>

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>

namespace xprv {

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

    /// Stop background services (cancel VL refresh, etc).
    /// Does not drain in-flight prove() calls — those complete or timeout.
    void
    stop();

    /// Result from prove() — chain + metadata the caller needs.
    struct ProveResult
    {
        ProofChain chain;
        uint32_t tx_ledger_seq = 0;
        std::string publisher_key_hex;  // actual key from VL fetch
    };

    /// Build a proof chain for a transaction. Safe to call concurrently
    /// (each call runs its own coroutine, borrows shared services).
    /// @param cancel_token  Optional cancel flag — set by HTTP session on
    ///                      client disconnect. Checked at cancellation
    ///                      boundaries to stop work without disrupting
    ///                      shared NodeCache state.
    /// @param on_step       Optional async callback co_awaited with each step
    ///                      as it becomes ready during the build (for SSE).
    using StepCallback =
        std::function<boost::asio::awaitable<void>(ChainStep const&)>;

    boost::asio::awaitable<ProveResult>
    prove(
        std::string const& tx_hash,
        std::shared_ptr<std::atomic<bool>> cancel_token = nullptr,
        uint32_t ledger_seq_hint = 0,
        StepCallback on_step = nullptr);

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

    /// Cache stats for /health.
    struct CacheStats
    {
        size_t entries = 0;
        size_t max_entries = 0;
        size_t hits = 0;
        size_t misses = 0;
    };
    CacheStats
    cache_stats() const;

    catl::peer_client::NodeCache::Stats
    node_cache_stats() const
    {
        return node_cache_ ? node_cache_->stats()
                           : catl::peer_client::NodeCache::Stats{};
    }

    void
    set_cache_enabled(bool enabled)
    {
        cache_enabled_ = enabled;
    }

    void
    set_node_cache_size(size_t max_entries)
    {
        node_cache_size_ = max_entries;
    }

    void
    set_fetch_timeout(int secs)
    {
        fetch_timeout_secs_ = secs;
    }

    void
    set_max_walk_peer_retries(int n)
    {
        max_walk_peer_retries_ = n;
    }

    void
    set_fetch_stale_multiplier(int n)
    {
        fetch_stale_multiplier_ = n;
    }

    void
    set_rpc_max_concurrent(int n)
    {
        rpc_max_concurrent_ = n;
    }

    /// Set single-shot mode (fewer peers, less crawling).
    /// Must be called before start().
    void
    set_single_shot(bool enabled)
    {
        single_shot_ = enabled;
    }

private:
    ProofEngine(boost::asio::io_context& io, NetworkConfig config);

    boost::asio::io_context& io_;
    NetworkConfig config_;
    catl::xdata::Protocol protocol_;

    std::shared_ptr<catl::peer_client::PeerSet> peers_;
    std::shared_ptr<catl::peer_client::NodeCache> node_cache_;
    std::shared_ptr<catl::rpc::RpcClient> rpc_;
    std::shared_ptr<VlCache> vl_cache_;
    std::shared_ptr<ValidationBuffer> val_buffer_;

    // LRU proof cache: tx_hash → ProveResult.
    // Proofs are immutable — a tx is in exactly one ledger forever.
    // Mutex-protected — prove() runs on arbitrary threads.
    static constexpr size_t kMaxCacheEntries = 256;
    using CacheList = std::list<std::pair<std::string, ProveResult>>;
    mutable std::mutex cache_mutex_;
    CacheList cache_lru_;  // front = most recently used
    std::unordered_map<std::string, CacheList::iterator> cache_map_;
    std::atomic<bool> cache_enabled_{true};
    size_t node_cache_size_ = 65536;
    int fetch_timeout_secs_ = 5;
    int max_walk_peer_retries_ = 3;
    int fetch_stale_multiplier_ = 2;
    int rpc_max_concurrent_ = 8;
    bool single_shot_ = false;
    std::atomic<size_t> cache_hits_{0};
    std::atomic<size_t> cache_misses_{0};

    void
    cache_put(std::string const& tx_hash, ProveResult const& result);

    std::optional<ProveResult>
    cache_get(std::string const& tx_hash);

    // ── TX → ledger_seq LRU cache ─────────────────────────────────
    // A tx is in exactly one ledger forever. Cache the RPC lookup.
    static constexpr size_t kMaxTxLedgerCache = 4096;
    using TxLedgerList = std::list<std::pair<std::string, uint32_t>>;
    TxLedgerList tx_ledger_lru_;
    std::unordered_map<std::string, TxLedgerList::iterator> tx_ledger_map_;
    // Uses cache_mutex_

    void
    tx_ledger_put(std::string const& tx_hash, uint32_t seq);

    std::optional<uint32_t>
    tx_ledger_get(std::string const& tx_hash);

    // ── Anchor bundle cache (future-based) ──────────────────────────
    // Built once per quorum window, shared across all concurrent proves.
    // Contains everything build_proof needs from the anchor step:
    // the header result, anchor hash, and account_hash for state walks.
    struct AnchorBundle
    {
        catl::peer_client::LedgerHeaderResult header_result;
        Hash256 anchor_hash;
        Hash256 account_hash;  // header.account_hash() for state walks
        uint32_t seq = 0;
        std::vector<ValidationCollector::Entry> validations;
        std::chrono::steady_clock::time_point built_at;
    };

    struct AnchorEntry
    {
        AnchorBundle bundle;
        std::shared_ptr<boost::asio::steady_timer> signal;
        bool present = false;
    };

    std::map<uint32_t, AnchorEntry> anchor_cache_;
    // Uses cache_mutex_ (same lock as proof cache)

    // The most recently built anchor — reused when fresh enough.
    std::optional<AnchorBundle> current_anchor_;

    /// Get or build anchor bundle for a given seq. First caller builds,
    /// others co_await the signal.
    boost::asio::awaitable<AnchorBundle>
    get_anchor_bundle(uint32_t anchor_seq, Hash256 anchor_hash,
                      std::vector<ValidationCollector::Entry> validations);

    /// Reuse the current anchor if recent enough, otherwise wait for
    /// a new quorum and build a fresh one.
    boost::asio::awaitable<AnchorBundle>
    get_or_reuse_anchor(catl::vl::ValidatorList const& vl);
};

}  // namespace xprv
