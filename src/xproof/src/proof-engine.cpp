#include "xproof/proof-engine.h"
#include "xproof/hex-utils.h"
#include "xproof/proof-builder.h"
#include "xproof/proof-chain-binary.h"
#include "xproof/proof-chain-json.h"
#include "xproof/proof-resolver.h"

#include <catl/core/logger.h>
#include <catl/rpc-client/rpc-client-coro.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace xproof {

namespace asio = boost::asio;
using namespace catl::peer_client;

static LogPartition log_("engine", LogLevel::INFO);

// ═══════════════════════════════════════════════════════════════════════
// Lifecycle
// ═══════════════════════════════════════════════════════════════════════

ProofEngine::ProofEngine(asio::io_context& io, NetworkConfig config)
    : io_(io)
    , config_(std::move(config))
    , protocol_(config_.load_protocol())
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

    // Peer set
    PeerSetOptions peer_opts;
    peer_opts.network_id = config_.network_id;
    peer_opts.endpoint_cache_path = config_.peer_cache_path;
    peers_ = PeerSet::create(io_, peer_opts);

    // Wire peers → validation buffer: unsolicited validations
    auto vbuf = val_buffer_;
    peers_->set_unsolicited_handler(
        [vbuf](uint16_t type, std::vector<uint8_t> const& data) {
            vbuf->on_packet(type, data);
        });

    // Bootstrap peer discovery
    peers_->bootstrap();

    // Push UNL to validation buffer on every VL load (initial + refresh).
    // set_unl() clears the quorum cache, so this must only fire on actual
    // VL changes — not per-request.
    vl_cache_->set_on_refresh(
        [vbuf](catl::vl::ValidatorList const& vl) {
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

    PLOGI(
        log_,
        "Engine started (network=",
        config_.network_id,
        ", vl=",
        config_.vl_host,
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
ProofEngine::prove(std::string const& tx_hash)
{
    // Check cache first — returns by value, safe across co_await
    if (auto cached = cache_get(tx_hash))
    {
        PLOGI(log_, "Cache hit for ", tx_hash.substr(0, 16), "...");
        co_return *cached;
    }

    // Step 1: Ensure VL is loaded (UNL push happens once in start(),
    // not per-request — set_unl clears the quorum cache)
    auto vl = co_await vl_cache_->co_get();

    // Step 2: Wait for a quorum (may already have one)
    auto quorum = co_await val_buffer_->co_wait_quorum(30);

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

    // Step 3: Build proof using shared services
    BuildServices svc{
        .io = io_,
        .peers = peers_,
        .vl = vl,
        .anchor_validations = quorum.validations,
        .protocol = protocol_,
        .rpc_host = config_.rpc_host,
        .rpc_port = config_.rpc_port,
    };

    auto result = co_await build_proof(svc, tx_hash);
    result.chain.network_id = config_.network_id;

    ProveResult prove_result{
        std::move(result.chain),
        result.tx_ledger_seq,
        result.publisher_key_hex};

    cache_put(tx_hash, prove_result);
    co_return prove_result;
}

// ═══════════════════════════════════════════════════════════════════════
// Proof cache
// ═══════════════════════════════════════════════════════════════════════

void
ProofEngine::cache_put(
    std::string const& tx_hash,
    ProveResult const& result)
{
    auto it = cache_map_.find(tx_hash);
    if (it != cache_map_.end())
    {
        // Already cached — move to front
        cache_lru_.splice(cache_lru_.begin(), cache_lru_, it->second);
        return;
    }

    // Evict LRU if at capacity
    while (cache_lru_.size() >= kMaxCacheEntries)
    {
        auto& back = cache_lru_.back();
        cache_map_.erase(back.first);
        cache_lru_.pop_back();
    }

    cache_lru_.emplace_front(tx_hash, result);
    cache_map_[tx_hash] = cache_lru_.begin();
    PLOGD(log_, "Cached proof for ", tx_hash.substr(0, 16), "... (", cache_lru_.size(), " entries)");
}

std::optional<ProofEngine::ProveResult>
ProofEngine::cache_get(std::string const& tx_hash)
{
    auto it = cache_map_.find(tx_hash);
    if (it == cache_map_.end())
    {
        cache_misses_++;
        return std::nullopt;
    }

    cache_hits_++;
    cache_lru_.splice(cache_lru_.begin(), cache_lru_, it->second);
    return it->second->second;  // return by value
}

ProofEngine::CacheStats
ProofEngine::cache_stats() const
{
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
        auto key = trusted_key.empty() ? proof_config.publisher_key : trusted_key;

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

}  // namespace xproof
