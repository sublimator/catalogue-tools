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

#include <catl/peer-client/ledger-fetch-coordinator.h>
#include <catl/peer-client/node-cache.h>
#include <catl/peer-client/peer-set.h>
#include <catl/rpc-client/rpc-client.h>
#include <catl/xdata/protocol.h>

#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>

namespace xprv {

class HttpServer;
class ProofEngine;
class RequestDrainBinding;
class RequestDrainTestAccess;
class ProofEngineTestAccess;

/// Identity shared by one HTTP admission owner and the exact engines whose
/// request frames it owns. The epoch advances whenever that owner starts a new
/// admission lifecycle, invalidating receipts from every prior lifecycle.
class RequestDrainAuthority final
{
private:
    friend class HttpServer;
    friend class ProofEngine;
    friend class RequestDrainReceipt;
    friend class RequestDrainTestAccess;

    std::atomic<std::uint64_t> current_epoch_{0};
};

/// Unforgeable evidence that an HttpServer has sealed request admission and
/// joined every admitted session frame for one exact admission authority and
/// lifecycle epoch. One receipt may authorize draining each ProofEngine bound
/// to that same service.
class RequestDrainReceipt
{
public:
    RequestDrainReceipt(RequestDrainReceipt const&) = delete;
    RequestDrainReceipt&
    operator=(RequestDrainReceipt const&) = delete;
    RequestDrainReceipt(RequestDrainReceipt&&) noexcept = default;
    RequestDrainReceipt&
    operator=(RequestDrainReceipt&&) noexcept = default;

private:
    friend class HttpServer;
    friend class ProofEngine;
    friend class RequestDrainBinding;
    friend class RequestDrainTestAccess;

    RequestDrainReceipt(
        std::shared_ptr<RequestDrainAuthority const> authority,
        std::uint64_t epoch) noexcept
        : authority_(std::move(authority)), epoch_(epoch)
    {
    }

    bool
    matches(std::shared_ptr<RequestDrainAuthority const> const& authority)
        const noexcept
    {
        return authority_ && authority && authority_.get() == authority.get() &&
            epoch_ != 0 &&
            authority_->current_epoch_.load(std::memory_order_acquire) ==
            epoch_;
    }

    std::shared_ptr<RequestDrainAuthority const> authority_;
    std::uint64_t epoch_ = 0;
};

/// Engine-owned binding to its one live HTTP admission authority.
///
/// A second server cannot silently share an engine while a first server or
/// one of its receipts still retains the original authority.
class RequestDrainBinding final
{
private:
    friend class HttpServer;
    friend class ProofEngine;
    friend class RequestDrainTestAccess;

    void
    bind(std::shared_ptr<RequestDrainAuthority const> authority)
    {
        if (!authority)
            throw std::invalid_argument("request-drain authority is required");

        std::lock_guard lock(mutex_);
        if (auto const current = authority_.lock();
            current && current.get() != authority.get())
        {
            throw std::logic_error(
                "ProofEngine already has a different HTTP admission owner");
        }
        authority_ = std::move(authority);
    }

    bool
    accepts(RequestDrainReceipt const& receipt) const noexcept
    {
        std::lock_guard lock(mutex_);
        return receipt.matches(authority_.lock());
    }

    mutable std::mutex mutex_;
    std::weak_ptr<RequestDrainAuthority const> authority_;
};

struct VerifyResult
{
    bool ok = false;
    std::string error;
};

class ProofEngine : public std::enable_shared_from_this<ProofEngine>
{
#ifdef XPRV_TESTING
    friend class ProofEngineTestAccess;
#endif

public:
    /// Unforgeable evidence that this exact engine instance consumed its
    /// admitted request-drain authority and completed its installed child
    /// population. Failure produces no receipt.
    class [[nodiscard]] DrainReceipt
    {
    public:
        DrainReceipt(DrainReceipt const&) = delete;
        DrainReceipt&
        operator=(DrainReceipt const&) = delete;
        DrainReceipt(DrainReceipt&&) noexcept = default;
        DrainReceipt&
        operator=(DrainReceipt&&) noexcept = default;

    private:
        friend class ProofEngine;
#ifdef XPRV_TESTING
        friend class ProofEngineTestAccess;
#endif

        explicit DrainReceipt(
            std::shared_ptr<ProofEngine const> engine) noexcept
            : engine_(std::move(engine))
        {
        }

        std::shared_ptr<ProofEngine const> engine_;
    };

    /// Verify a drain receipt against this exact engine without granting its
    /// consumer authority to construct one.
    bool
    accepts_drain_receipt(DrainReceipt const& receipt) const noexcept
    {
        return receipt.engine_ && receipt.engine_.get() == this;
    }

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

    /// Consume the engine-owned service lifecycle after its request-admission
    /// owner has stopped and all prove() frames have completed.
    ///
    /// Drains NodeCache node-operation and header servants, fences and
    /// reconciles managed transport, then awaits PeerSet's physical
    /// session/servant receipt.
    [[nodiscard]] boost::asio::awaitable<DrainReceipt>
    co_stop(RequestDrainReceipt const& request_drain);

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

    /// @param max_anchor_age_secs  Max age of cached anchor to reuse (0 =
    /// always
    ///                             wait for latest quorum, the default).
    boost::asio::awaitable<ProveResult>
    prove(
        std::string const& tx_hash,
        std::shared_ptr<std::atomic<bool>> cancel_token = nullptr,
        uint32_t ledger_seq_hint = 0,
        uint32_t max_anchor_age_secs = 0,
        StepCallback on_step = nullptr);

    /// Verify a proof chain. Sync pure function — no shared state.
    VerifyResult
    verify(std::span<const uint8_t> data, std::string const& trusted_key = "");

    /// Look up which ledger a transaction is in (RPC only, no proof
    /// building). Returns the ledger sequence, or 0 if not found on
    /// this network.
    boost::asio::awaitable<uint32_t>
    lookup_tx(std::string const& tx_hash);

    /// Engine status for health checks. Awaitable — hops to each
    /// service strand to collect a snapshot.
    struct Status
    {
        struct Coordinator
        {
            bool enabled = false;
            catl::peer_client::LedgerFetchCoordinator::Lifecycle lifecycle =
                catl::peer_client::LedgerFetchCoordinator::Lifecycle::running;
            bool ingress_sealed = false;
            size_t work_outstanding = 0;
            size_t outstanding_bind_projections = 0;
            size_t receipt_obligations = 0;
            bool accounting_broken = false;
        };

        size_t peer_count = 0;
        uint64_t total_connects = 0;
        uint64_t total_disconnects = 0;
        bool vl_loaded = false;
        std::optional<uint32_t> latest_quorum_seq;
        ValidationBuffer::Stats validation_buffer;
        Coordinator ledger_fetch_coordinator;
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
    set_fetch_timeout(std::chrono::milliseconds timeout)
    {
        fetch_timeout_ = timeout;
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

    /// Route managed node-fetch writes through LedgerFetchCoordinator.
    /// Default-off until deterministic and live proof/SSE gates pass.
    /// Must be selected before start(). Once selected, a missing generation pin
    /// or expired coordinator refuses rather than silently using raw transport.
    void
    set_ledger_fetch_coordinator_enabled(bool enabled)
    {
        if (start_called_)
            throw std::logic_error(
                "ledger fetch transport must be selected before start");
        ledger_fetch_coordinator_enabled_ = enabled;
    }

    /// Set single-shot mode (fewer peers, less crawling).
    /// Must be called before start().
    void
    set_single_shot(bool enabled)
    {
        single_shot_ = enabled;
    }

private:
    friend class HttpServer;

    ProofEngine(boost::asio::io_context& io, NetworkConfig config);

    void
    bind_request_drain_authority(
        std::shared_ptr<RequestDrainAuthority const> authority);

    boost::asio::io_context& io_;
    NetworkConfig config_;
    std::string net_label_;
    catl::xdata::Protocol protocol_;

    std::shared_ptr<catl::peer_client::PeerSet> peers_;
    //@@authority state=prv.S0015 status=provisional scope=field
    // bind=xprv::ProofEngine::ledger_fetch_coordinator_@6cee545daf224610
    // owner=unresolved owner_hypothesis=proof_engine observed_home=proof_engine
    // label=proof_engine_owned_ledger_fetch_coordinator_lifecycle
    std::shared_ptr<catl::peer_client::LedgerFetchCoordinator>
        ledger_fetch_coordinator_;
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
    std::chrono::milliseconds fetch_timeout_{1500};
    int max_walk_peer_retries_ = 3;
    int fetch_stale_multiplier_ = 2;
    int rpc_max_concurrent_ = 8;
    bool single_shot_ = false;
    //@@authority state=prv.S0016 status=provisional scope=field
    // bind=xprv::ProofEngine::ledger_fetch_coordinator_enabled_@568396b37d0ef4a3
    // owner=unresolved owner_hypothesis=proof_engine observed_home=proof_engine
    // label=pre_start_managed_node_transport_selection
    bool ledger_fetch_coordinator_enabled_ = false;
    //@@authority state=prv.S0017 status=provisional scope=field
    // bind=xprv::ProofEngine::start_called_@75aec8130a675b5f
    // owner=unresolved owner_hypothesis=proof_engine observed_home=proof_engine
    // label=proof_engine_start_once_lifecycle_boundary
    bool start_called_ = false;
    RequestDrainBinding request_drain_binding_;
    // Test-only observation point after every admitted child receipt has
    // returned but before co_stop() publishes the engine drain receipt.
    // Production leaves this empty.
    std::function<void()> before_drain_receipt_publish_for_test_;
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
        uint32_t vl_sequence = 0;
        std::vector<ValidationCollector::Entry> validations;
        std::chrono::steady_clock::time_point built_at;
    };

    struct AnchorEntry
    {
        AnchorBundle bundle;
        std::shared_ptr<boost::asio::steady_timer> signal;
        bool present = false;
    };

    using AnchorCacheKey = std::tuple<uint32_t, uint32_t, Hash256>;
    std::map<AnchorCacheKey, AnchorEntry> anchor_cache_;
    // Uses cache_mutex_ (same lock as proof cache)

    // The most recently built anchor — reused when fresh enough.
    std::optional<AnchorBundle> current_anchor_;

    /// Get or build anchor bundle for a given seq. First caller builds,
    /// others co_await the signal.
    boost::asio::awaitable<AnchorBundle>
    get_anchor_bundle(
        uint32_t anchor_seq,
        Hash256 anchor_hash,
        uint32_t vl_sequence,
        uint32_t vl_effective,
        uint32_t vl_expiration,
        std::vector<ValidationCollector::Entry> validations);

    /// Reuse the current anchor if recent enough, otherwise wait for
    /// a new quorum and build a fresh one.
    /// @param max_age_secs  0 = always fresh (wait for latest quorum)
    boost::asio::awaitable<AnchorBundle>
    get_or_reuse_anchor(
        std::shared_ptr<catl::vl::ValidatorList> const& vl,
        uint32_t max_age_secs);

    void
    invalidate_anchor_cache();

    void
    invalidate_anchor_cache_locked();
};

}  // namespace xprv
