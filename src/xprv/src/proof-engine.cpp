#include "xprv/proof-engine.h"
#include "xprv/hex-utils.h"
#include "xprv/proof-builder.h"
#include "xprv/proof-chain-binary.h"
#include "xprv/proof-chain-json.h"
#include "xprv/proof-resolver.h"

#include <catl/core/logger.h>
#include <catl/rpc-client/rpc-client-coro.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace xprv {

namespace asio = boost::asio;
using namespace catl::peer_client;

static LogPartition log_("engine", LogLevel::INFO);

// ═══════════════════════════════════════════════════════════════════════
// Lifecycle
// ═══════════════════════════════════════════════════════════════════════

ProofEngine::ProofEngine(asio::io_context& io, NetworkConfig config)
    : io_(io), config_(std::move(config)), protocol_(config_.load_protocol())
{
    config_.apply_defaults();
}

std::shared_ptr<ProofEngine>
ProofEngine::create(asio::io_context& io, NetworkConfig config)
{
    return std::shared_ptr<ProofEngine>(new ProofEngine(io, std::move(config)));
}

void
ProofEngine::start()
{
    // VL cache
    vl_cache_ = VlCache::create(io_, config_.vl_host, config_.vl_port);
    vl_cache_->start();

    // Validation buffer
    val_buffer_ = ValidationBuffer::create(io_, protocol_);

    // Wire VL → validation buffer: when VL loads, push UNL keys
    // We do this via a one-shot fetch, then the refresh will re-push.
    // For now, the VL co_get in prove() handles the initial UNL push.

    // Node cache (content-addressed wire node store)
    node_cache_ = NodeCache::create(
        io_,
        {.max_entries = node_cache_size_,
         .fetch_timeout_secs = fetch_timeout_secs_,
         .max_walk_peer_retries = max_walk_peer_retries_,
         .fetch_stale_multiplier = fetch_stale_multiplier_});

    // Shared RPC client with concurrency limiter
    rpc_ = std::make_shared<catl::rpc::RpcClient>(
        io_, config_.rpc_host, config_.rpc_port, rpc_max_concurrent_);

    // Peer set — pick pool based on mode (server vs single-shot CLI)
    auto const& pool = single_shot_ ? config_.peers_cli : config_.peers_server;
    PeerSetOptions peer_opts;
    peer_opts.network_id = config_.network_id;
    peer_opts.endpoint_cache_path = config_.peer_cache_path;
    peer_opts.max_connected_peers = pool.max_hub_peers;
    peer_opts.max_archival_peers = pool.max_archival_peers;
    peer_opts.max_in_flight_connects = pool.max_in_flight_connects;
    peer_opts.max_in_flight_crawls = pool.max_in_flight_crawls;
    peer_opts.archival_range_threshold = config_.archival_range_threshold;
    PLOGI(
        log_,
        "Peer pool (",
        single_shot_ ? "cli" : "server",
        "): hubs=",
        pool.max_hub_peers,
        " archival=",
        pool.max_archival_peers,
        " connects=",
        pool.max_in_flight_connects,
        " crawls=",
        pool.max_in_flight_crawls);
    peers_ = PeerSet::create(io_, peer_opts);

    // Wire peers → validation buffer: unsolicited validations
    auto vbuf = val_buffer_;
    peers_->set_unsolicited_handler(
        [vbuf](uint16_t type, std::vector<uint8_t> const& data) {
            vbuf->on_packet(type, data);
        });

    // Wire observer repost (must be after create, uses shared_from_this)
    peers_->start();

    // Bootstrap peer discovery
    peers_->bootstrap();

    // Push UNL to validation buffer on every VL load (initial + refresh).
    // set_unl() clears the quorum cache, so this must only fire on actual
    // VL changes — not per-request.
    vl_cache_->set_on_refresh([vbuf](catl::vl::ValidatorList const& vl) {
        vbuf->set_unl(vl.validators);
    });

    // Try the configured initial peer
    auto self = shared_from_this();
    asio::co_spawn(
        io_,
        [self]() -> asio::awaitable<void> {
            auto client = co_await self->peers_->try_connect(
                self->config_.peer_host, self->config_.peer_port);
            if (client)
            {
                PLOGI(
                    log_,
                    "Initial peer connected: ",
                    client->endpoint(),
                    " (ledger ",
                    client->peer_ledger_seq(),
                    ")");
            }
            else
            {
                PLOGW(
                    log_,
                    "Initial peer ",
                    self->config_.peer_host,
                    ":",
                    self->config_.peer_port,
                    " failed — using bootstrap discovery");
            }
        },
        asio::detached);

    // Pre-resolve RPC hostname so concurrent proves don't hammer DNS.
    // RPC host doesn't change at runtime — resolve once, use IP forever.
    try
    {
        boost::asio::ip::tcp::resolver resolver(io_);
        auto results = resolver.resolve(
            config_.rpc_host, std::to_string(config_.rpc_port));
        if (!results.empty())
        {
            auto resolved_ip =
                results.begin()->endpoint().address().to_string();
            PLOGI(
                log_,
                "Resolved RPC host ",
                config_.rpc_host,
                " → ",
                resolved_ip);
            config_.rpc_host = resolved_ip;
        }
    }
    catch (std::exception const& e)
    {
        PLOGW(log_, "Failed to pre-resolve RPC host: ", e.what());
        // Continue with hostname — individual requests will resolve
    }

    PLOGI(
        log_,
        "Engine started (network=",
        config_.network_id,
        ", vl=",
        config_.vl_host,
        ", rpc=",
        config_.rpc_host,
        ":",
        config_.rpc_port,
        ", peer=",
        config_.peer_host,
        ":",
        config_.peer_port,
        ")");
}

void
ProofEngine::stop()
{
    PLOGI(log_, "Engine stopping...");
    if (vl_cache_)
    {
        vl_cache_->stop();
    }
    // PeerSet and ValidationBuffer stop naturally when io_context stops —
    // they hold no timers that keep the io_context alive on their own.
    // The VL refresh timer is the main thing that prevents exit.
}

// ═══════════════════════════════════════════════════════════════════════
// prove() — build a proof chain using shared services
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<ProofEngine::ProveResult>
ProofEngine::prove(
    std::string const& tx_hash,
    std::shared_ptr<std::atomic<bool>> cancel_token)
{
    // Enable cancellation so the || operator in the HTTP session can
    // abort this coroutine when the client disconnects.
    co_await asio::this_coro::reset_cancellation_state(
        asio::enable_total_cancellation());

    // Check cache first — returns by value, safe across co_await
    if (cache_enabled_)
    {
        if (auto cached = cache_get(tx_hash))
        {
            PLOGI(log_, "Cache hit for ", tx_hash.substr(0, 16), "...");
            co_return *cached;
        }
    }

    // Step 1: Ensure VL is loaded (UNL push happens once in start(),
    // not per-request — set_unl clears the quorum cache)
    auto vl = co_await vl_cache_->co_get();

    // Step 2: Wait for a quorum (may already have one)
    auto quorum =
        co_await val_buffer_->co_wait_quorum(std::chrono::seconds(30));

    PLOGI(
        log_,
        "Anchor: seq=",
        quorum.ledger_seq,
        " hash=",
        quorum.ledger_hash.hex().substr(0, 16),
        "... (",
        quorum.validations.size(),
        "/",
        vl.validators.size(),
        " validators)");

    // Step 3: Get anchor bundle (future-based — built once, shared)
    auto anchor =
        co_await get_anchor_bundle(quorum.ledger_seq, quorum.ledger_hash);

    // Step 4: Build proof using shared services
    // Check tx→ledger_seq cache (avoids RPC lookup on repeated requests)
    auto cached_seq = tx_ledger_get(tx_hash);

    BuildServices svc{
        .io = io_,
        .peers = peers_,
        .vl = vl,
        .anchor_validations = quorum.validations,
        .protocol = protocol_,
        .node_cache = node_cache_,
        .rpc = rpc_,
        .tx_ledger_seq_hint = cached_seq.value_or(0),
        .anchor_hdr = anchor.header_result,
        .anchor_hash = anchor.anchor_hash,
        .anchor_account_hash = anchor.account_hash,
        .cancel_token = cancel_token,
    };

    auto result = co_await build_proof(svc, tx_hash);
    result.chain.network_id = config_.network_id;

    // Cache the tx→ledger mapping for future requests
    tx_ledger_put(tx_hash, result.tx_ledger_seq);

    ProveResult prove_result{
        std::move(result.chain),
        result.tx_ledger_seq,
        result.publisher_key_hex};

    if (cache_enabled_)
    {
        cache_put(tx_hash, prove_result);
    }
    co_return prove_result;
}

// ═══════════════════════════════════════════════════════════════════════
// Proof cache
// ═══════════════════════════════════════════════════════════════════════

void
ProofEngine::cache_put(std::string const& tx_hash, ProveResult const& result)
{
    std::lock_guard lock(cache_mutex_);

    auto it = cache_map_.find(tx_hash);
    if (it != cache_map_.end())
    {
        cache_lru_.splice(cache_lru_.begin(), cache_lru_, it->second);
        return;
    }

    while (cache_lru_.size() >= kMaxCacheEntries)
    {
        auto& back = cache_lru_.back();
        cache_map_.erase(back.first);
        cache_lru_.pop_back();
    }

    cache_lru_.emplace_front(tx_hash, result);
    cache_map_[tx_hash] = cache_lru_.begin();
    PLOGD(
        log_,
        "Cached proof for ",
        tx_hash.substr(0, 16),
        "... (",
        cache_lru_.size(),
        " entries)");
}

std::optional<ProofEngine::ProveResult>
ProofEngine::cache_get(std::string const& tx_hash)
{
    std::lock_guard lock(cache_mutex_);

    auto it = cache_map_.find(tx_hash);
    if (it == cache_map_.end())
    {
        cache_misses_++;
        return std::nullopt;
    }

    cache_hits_++;
    cache_lru_.splice(cache_lru_.begin(), cache_lru_, it->second);
    return it->second->second;
}

ProofEngine::CacheStats
ProofEngine::cache_stats() const
{
    std::lock_guard lock(cache_mutex_);
    return {
        .entries = cache_lru_.size(),
        .max_entries = kMaxCacheEntries,
        .hits = cache_hits_,
        .misses = cache_misses_,
    };
}

// ═══════════════════════════════════════════════════════════════════════
// verify() — sync pure function
// ═══════════════════════════════════════════════════════════════════════

VerifyResult
ProofEngine::verify(
    std::span<const uint8_t> data,
    std::string const& trusted_key)
{
    VerifyResult result;
    try
    {
        // Auto-detect format
        ProofChain chain;
        if (data.size() >= 4 && data[0] == 'X' && data[1] == 'P' &&
            data[2] == 'R' && data[3] == 'F')
        {
            chain = from_binary(data);
        }
        else
        {
            auto json = boost::json::parse(std::string_view(
                reinterpret_cast<const char*>(data.data()), data.size()));
            chain = from_json(json);
        }

        // Use the proof's embedded network_id to select protocol and
        // publisher key, unless the caller explicitly overrides the key.
        auto proof_config = NetworkConfig::for_network(chain.network_id);
        auto protocol = proof_config.load_protocol();
        auto key =
            trusted_key.empty() ? proof_config.publisher_key : trusted_key;

        result.ok = resolve_proof_chain(chain, protocol, key);
        if (!result.ok)
            result.error = "verification failed";
    }
    catch (std::exception const& e)
    {
        result.ok = false;
        result.error = e.what();
    }
    return result;
}

// ═══════════════════════════════════════════════════════════════════════
// TX → ledger_seq LRU cache
// ═══════════════════════════════════════════════════════════════════════

void
ProofEngine::tx_ledger_put(std::string const& tx_hash, uint32_t seq)
{
    std::lock_guard lock(cache_mutex_);
    auto it = tx_ledger_map_.find(tx_hash);
    if (it != tx_ledger_map_.end())
    {
        tx_ledger_lru_.splice(
            tx_ledger_lru_.begin(), tx_ledger_lru_, it->second);
        return;
    }
    while (tx_ledger_lru_.size() >= kMaxTxLedgerCache)
    {
        auto& back = tx_ledger_lru_.back();
        tx_ledger_map_.erase(back.first);
        tx_ledger_lru_.pop_back();
    }
    tx_ledger_lru_.emplace_front(tx_hash, seq);
    tx_ledger_map_[tx_hash] = tx_ledger_lru_.begin();
}

std::optional<uint32_t>
ProofEngine::tx_ledger_get(std::string const& tx_hash)
{
    std::lock_guard lock(cache_mutex_);
    auto it = tx_ledger_map_.find(tx_hash);
    if (it == tx_ledger_map_.end())
        return std::nullopt;
    tx_ledger_lru_.splice(tx_ledger_lru_.begin(), tx_ledger_lru_, it->second);
    return it->second->second;
}

// ═══════════════════════════════════════════════════════════════════════
// co_health() — collect status from each service
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<ProofEngine::Status>
ProofEngine::co_health()
{
    Status status;
    status.peer_count = co_await peers_->co_size();

    status.vl_loaded = co_await vl_cache_->co_is_loaded();

    auto latest = co_await val_buffer_->co_latest_quorum();
    if (latest)
        status.latest_quorum_seq = latest->ledger_seq;

    co_return status;
}

// ═══════════════════════════════════════════════════════════════════════
// Anchor bundle cache — future-based, built once per quorum
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<ProofEngine::AnchorBundle>
ProofEngine::get_anchor_bundle(uint32_t anchor_seq, Hash256 anchor_hash)
{
    enum class Action { hit, wait, fetch };
    Action action;
    std::shared_ptr<asio::steady_timer> signal;
    AnchorBundle hit_bundle;

    {
        std::lock_guard lock(cache_mutex_);
        auto [it, inserted] = anchor_cache_.try_emplace(anchor_seq);

        if (!inserted && it->second.present)
        {
            action = Action::hit;
            hit_bundle = it->second.bundle;
            PLOGD(log_, "anchor_bundle: HIT seq=", anchor_seq);
        }
        else if (!inserted && it->second.signal)
        {
            action = Action::wait;
            signal = it->second.signal;
            PLOGD(log_, "anchor_bundle: IN-FLIGHT seq=", anchor_seq);
        }
        else
        {
            it->second.signal = std::make_shared<asio::steady_timer>(
                io_, std::chrono::seconds(30));
            it->second.present = false;
            action = Action::fetch;
            PLOGD(log_, "anchor_bundle: MISS seq=", anchor_seq, " — building");
        }
    }

    if (action == Action::hit)
        co_return hit_bundle;

    if (action == Action::wait)
    {
        boost::system::error_code ec;
        co_await signal->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));

        std::lock_guard lock(cache_mutex_);
        auto it = anchor_cache_.find(anchor_seq);
        if (it != anchor_cache_.end() && it->second.present)
            co_return it->second.bundle;

        throw std::runtime_error("anchor bundle fetch failed");
    }

    // Build: fetch the anchor header
    try
    {
        auto hdr = co_await node_cache_->get_header(anchor_seq, peers_);

        AnchorBundle bundle;
        bundle.header_result = hdr;
        bundle.anchor_hash = anchor_hash;
        bundle.account_hash = hdr.header().account_hash();
        bundle.seq = anchor_seq;

        // Populate cache and wake waiters
        std::shared_ptr<asio::steady_timer> sig;
        {
            std::lock_guard lock(cache_mutex_);
            auto& entry = anchor_cache_[anchor_seq];
            sig = entry.signal;
            entry.bundle = bundle;
            entry.present = true;
            entry.signal = nullptr;
        }
        if (sig)
            sig->cancel();

        PLOGI(
            log_,
            "anchor_bundle: built seq=",
            anchor_seq,
            " hash=",
            anchor_hash.hex().substr(0, 16));
        co_return bundle;
    }
    catch (...)
    {
        // Clean up and wake waiters
        std::shared_ptr<asio::steady_timer> sig;
        {
            std::lock_guard lock(cache_mutex_);
            auto it = anchor_cache_.find(anchor_seq);
            if (it != anchor_cache_.end())
            {
                sig = it->second.signal;
                anchor_cache_.erase(it);
            }
        }
        if (sig)
            sig->cancel();
        throw;
    }
}

}  // namespace xprv
