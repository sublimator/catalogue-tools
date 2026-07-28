#include "xprv/proof-engine.h"
#include "xprv/anchor-verifier.h"
#include "xprv/hex-utils.h"
#include "xprv/manifest-utils.h"
#include "xprv/proof-builder.h"
#include "xprv/proof-chain-binary.h"
#include "xprv/proof-chain-json.h"
#include "xprv/proof-resolver.h"

#include <catl/core/logger.h>
#include <catl/core/request-context.h>
#include <catl/rpc-client/rpc-client-coro.h>
#include <cstring>

#include <algorithm>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/tcp.hpp>
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
    : io_(io)
    , config_(std::move(config))
    , net_label_(network_label(config_.network_id))
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
ProofEngine::bind_request_drain_authority(
    std::shared_ptr<RequestDrainAuthority const> authority)
{
    request_drain_binding_.bind(std::move(authority));
}

void
ProofEngine::start()
{
    if (start_called_)
        throw std::logic_error("ProofEngine::start may be called only once");
    start_called_ = true;

    // VL cache
    vl_cache_ = VlCache::create(io_, config_.vl_host, config_.vl_port);
    // Authenticate list contents against the pinned publisher key independently
    // of transport authentication.
    // weak_ptr capture: no dangling if the engine outlives an in-flight fetch,
    // and no engine<->cache reference cycle.
    vl_cache_->set_verifier(
        [weak = std::weak_ptr<ProofEngine>(shared_from_this())](
            catl::vl::ValidatorList const& vl, std::string& err) -> bool {
            auto self = weak.lock();
            if (!self)
            {
                err = "engine gone";
                return false;
            }
            return verify_fetched_vl(
                vl, self->config_.publisher_key, self->protocol_, err);
        });
    vl_cache_->start();

    // Validation buffer
    val_buffer_ = ValidationBuffer::create(io_, protocol_, config_.network_id);

    // Wire VL → validation buffer: when VL loads, push UNL keys
    // We do this via a one-shot fetch, then the refresh will re-push.
    // For now, the VL co_get in prove() handles the initial UNL push.

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
        "[",
        net_label_,
        "] Peer pool (",
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

    if (ledger_fetch_coordinator_enabled_)
    {
        //@@authority transition=prv.T0044 status=provisional scope=statement
        // bind=create@c921008e8a6ff63e
        // owner=unresolved owner_hypothesis=proof_engine
        // commit=startup_coroutine calls=create
        // label=construct_one_ledger_fetch_coordinator_per_peer_set
        ledger_fetch_coordinator_ = LedgerFetchCoordinator::create(
            io_, peers_);  //@@authority-target ids=prv.T0044
        if (!ledger_fetch_coordinator_)
            throw std::runtime_error(
                "failed to create ledger fetch coordinator generation");
    }

    // Node cache (content-addressed wire node store). The optional coordinator
    // is selected once here; request callsites do not branch on server mode.
    //@@authority transition=prv.T0004 status=provisional scope=statement
    // bind=create@c881629500fe6506
    // owner=unresolved owner_hypothesis=ledger_data_provider
    // commit=startup_coroutine calls=create
    // label=construct_live_node_cache_with_selected_transport_capability
    node_cache_ = NodeCache::create(
        io_,
        {.max_entries = node_cache_size_,
         .fetch_timeout = fetch_timeout_,
         .max_walk_peer_retries = max_walk_peer_retries_,
         .fetch_stale_multiplier = fetch_stale_multiplier_},
        ledger_fetch_coordinator_);  //@@authority-target ids=prv.T0004

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
        vbuf->set_unl(vl.validators, vl.sequence);
    });

    // The configured endpoint uses PeerSet's owned background census. A
    // detached try_connect caller would be invisible to PeerSet::co_stop().
    peers_->start_connect(config_.peer_host, config_.peer_port);

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
                "[",
                net_label_,
                "] Resolved RPC host ",
                config_.rpc_host,
                " → ",
                resolved_ip);
            config_.rpc_host = resolved_ip;
        }
    }
    catch (std::exception const& e)
    {
        PLOGW(
            log_,
            "[",
            net_label_,
            "] Failed to pre-resolve RPC host: ",
            e.what());
        // Continue with hostname — individual requests will resolve
    }

    PLOGI(
        log_,
        "[",
        net_label_,
        "] Engine started (network=",
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
    PLOGI(log_, "[", net_label_, "] Engine stopping...");
    if (vl_cache_)
    {
        vl_cache_->stop();
    }
    if (val_buffer_)
    {
        val_buffer_->stop();
    }
    if (ledger_fetch_coordinator_)
    {
        // Logical seal only. The owning io_context must keep running to execute
        // the coordinator's drain; stop() does not pretend this is quiescence.
        //@@authority transition=prv.T0045 status=provisional scope=statement
        // bind=fence@f07d60bfb8be76ad
        // owner=unresolved owner_hypothesis=proof_engine commit=atomic
        // calls=fence label=seal_managed_node_ingress_before_io_context_stop
        auto const disposition =
            ledger_fetch_coordinator_
                ->fence();  //@@authority-target ids=prv.T0045
        if (disposition != LedgerFetchCoordinator::Fence::drain_queued)
        {
            PLOGW(
                log_,
                "[",
                net_label_,
                "] ledger fetch coordinator fence notification refused");
        }
    }
    // The validator worker is joined above. The owning command halts peer and
    // heartbeat execution by stopping the io_context; that process-terminal
    // boundary is not a graceful drain or a reusable-engine quiescence claim.
}

// Machine-readable target anchors in this block must remain on their
// statements.
// clang-format off
//@@authority state=prv.S0056 status=provisional scope=function
//bind=xprv::co_stop@eb43bbcc90625807
// owner=unresolved owner_hypothesis=proof_engine_drain_frame
// observed_home=ProofEngine::co_stop_coroutine_frame
// label=exact_proof_engine_child_drain_join_frame
//@@authority transition=prv.T0165 status=provisional scope=function
//bind=xprv::co_stop@eb43bbcc90625807
// owner=unresolved owner_hypothesis=proof_engine_drain_frame
// commit=proof_engine_drain_coroutine touches=prv.S0056
// label=join_exact_proof_engine_child_drain_sequence
//@@authority channel=prv.C0004 status=provisional scope=function
//bind=xprv::co_stop@eb43bbcc90625807
// endpoints=service_lifecycle,proof_engine
// channel_semantics=process_local_join
// label=await_exact_proof_engine_drain_receipt
asio::awaitable<void>
ProofEngine::co_stop(RequestDrainReceipt const& request_drain)
{
    //@@authority transition=prv.T0166 status=provisional scope=statement
    //bind=if_statement@5f390c1b907fdbdf
    // owner=unresolved owner_hypothesis=proof_engine
    // commit=request_drain_binding_mutex touches=prv.S0056
    // label=accept_exact_http_request_drain_authority
    if (!request_drain_binding_.accepts(request_drain))
    {
        throw std::invalid_argument(
            "request-drain receipt does not authorize this engine");
    } //@@authority-target ids=prv.T0166

    // This is shared service lifecycle work. A canceled signal waiter must not
    // abandon the drain between components and leave later joiners observing a
    // half-sealed engine.
    co_await asio::this_coro::reset_cancellation_state(
        asio::disable_cancellation());

    PLOGI(log_, "[", net_label_, "] Engine draining...");

    // The HTTP admission owner has already drained every request frame before
    // entering here, so no new cache operation can be minted by this service.
    if (vl_cache_)
    {
        //@@authority transition=prv.T0167 status=provisional scope=statement
        //bind=co_stop@e29d5977c3f4b067
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine calls=co_stop touches=prv.S0056
        // label=consume_vl_cache_stop_receipt
        co_await vl_cache_->co_stop(); //@@authority-target ids=prv.T0167
    }
    if (val_buffer_)
    {
        //@@authority transition=prv.T0168 status=provisional scope=statement
        //bind=stop@73b285cdf9ba60f9
        // owner=unresolved owner_hypothesis=validation_buffer
        // commit=validation_buffer_publication_and_admission_mutex calls=stop
        // touches=prv.S0056 label=request_and_join_manifest_worker_stop
        val_buffer_->stop(); //@@authority-target ids=prv.T0168
    }

    if (node_cache_)
    {
        //@@authority transition=prv.T0169 status=provisional scope=statement
        //bind=co_stop_nodes@d56e96e1aa7057c4
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine calls=co_stop_nodes
        // touches=prv.S0056 label=consume_node_cache_node_stop_receipt
        co_await node_cache_
            ->co_stop_nodes(); //@@authority-target ids=prv.T0169
        //@@authority transition=prv.T0170 status=provisional scope=statement
        //bind=co_clear_headers@5b732a4bad82ab4e
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine calls=co_clear_headers
        // touches=prv.S0056 label=consume_node_cache_header_clear_receipt
        co_await node_cache_
            ->co_clear_headers(); //@@authority-target ids=prv.T0170
    }

    if (ledger_fetch_coordinator_)
    {
        //@@authority transition=prv.T0171 status=provisional scope=statement
        //bind=co_fence@71ef652c577a3710
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine calls=co_fence touches=prv.S0056
        // label=consume_coordinator_fence_receipt
        co_await ledger_fetch_coordinator_
            ->co_fence(); //@@authority-target ids=prv.T0171

        // A PeerSet seal handler can enqueue its pin release behind the first
        // barrier. The second pass consumes those causally generated releases.
        //@@authority transition=prv.T0172 status=provisional scope=statement
        //bind=co_generation_pin_release_barrier@4008f5b2ea323561
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine touches=prv.S0056
        // calls=co_generation_pin_release_barrier
        // label=consume_first_peer_set_pin_release_barrier
        co_await peers_->co_generation_pin_release_barrier(); //@@authority-target ids=prv.T0172
        //@@authority transition=prv.T0173 status=provisional scope=statement
        //bind=co_generation_pin_release_barrier@f6a9e514365b5562
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine touches=prv.S0056
        // calls=co_generation_pin_release_barrier
        // label=consume_causally_generated_peer_set_pin_release_barrier
        co_await peers_->co_generation_pin_release_barrier(); //@@authority-target ids=prv.T0173

        //@@authority transition=prv.T0174 status=provisional scope=statement
        //bind=co_reconcile_shutdown_projection_residue@0c39121bc8a9f209
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine touches=prv.S0056
        // calls=co_reconcile_shutdown_projection_residue
        // label=consume_coordinator_projection_reconciliation_receipt
        auto const lost = co_await ledger_fetch_coordinator_
                              ->co_reconcile_shutdown_projection_residue(); //@@authority-target ids=prv.T0174
        if (lost != 0)
        {
            PLOGW(
                log_,
                "[",
                net_label_,
                "] reconciled ",
                lost,
                " lost coordinator bind completion(s) at shutdown");
        }
    }

    if (peers_)
    {
        //@@authority transition=prv.T0175 status=provisional scope=statement
        //bind=co_stop@ec1addb3f351b7a5
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine calls=co_stop touches=prv.S0056
        // label=consume_peer_set_aggregate_stop_receipt
        co_await peers_->co_stop(); //@@authority-target ids=prv.T0175
    }

    PLOGI(log_, "[", net_label_, "] Engine drained");
    //@@authority transition=prv.T0176 status=provisional scope=statement
    //bind=co_return_statement@a7e5f24d79554a8d
    // owner=unresolved owner_hypothesis=proof_engine_drain_frame
    // commit=proof_engine_drain_coroutine touches=prv.S0056
    // label=publish_exact_proof_engine_drain_receipt
    co_return; //@@authority-target ids=prv.T0176
}
// clang-format on

// ═══════════════════════════════════════════════════════════════════════
// prove() — build a proof chain using shared services
// ═══════════════════════════════════════════════════════════════════════

//@@authority transition=prv.T0005 status=provisional scope=function
// bind=xprv::prove@3023246064adff8e
// owner=unresolved owner_hypothesis=ledger_data_control commit=unresolved
// label=begin_real_proof_operation_without_authority_identity
asio::awaitable<ProofEngine::ProveResult>
ProofEngine::prove(
    std::string const& tx_hash,
    std::shared_ptr<std::atomic<bool>> cancel_token,
    uint32_t ledger_seq_hint,
    uint32_t max_anchor_age_secs,
    StepCallback on_step)
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
            PLOGI(
                log_,
                "[",
                net_label_,
                "] Cache hit for ",
                tx_hash.substr(0, 16),
                "...");
            // Replay cached steps through the callback for SSE
            if (on_step)
            {
                for (auto const& step : cached->chain.steps)
                    co_await on_step(step);
            }
            co_return *cached;
        }
    }

    // Step 1: Take an authenticated list snapshot and order its UNL update
    // before any proof-quorum read. A concurrent refresh may make the first
    // snapshot obsolete, in which case retry with the cache's current list.
    std::shared_ptr<catl::vl::ValidatorList> vl;
    for (int attempt = 0; attempt < 3; ++attempt)
    {
        vl = std::make_shared<catl::vl::ValidatorList>(
            co_await vl_cache_->co_get());
        if (co_await val_buffer_->co_sync_unl(vl->validators, vl->sequence))
            break;
        vl.reset();
    }
    if (!vl)
        throw std::runtime_error("validator list changed while starting proof");

    // Check tx→ledger_seq cache (avoids RPC lookup on repeated requests)
    auto cached_seq = tx_ledger_get(tx_hash);

    // Capture self + vl + age for the lazy anchor provider
    auto self = shared_from_this();
    auto anchor_age = max_anchor_age_secs;

    BuildServices svc{
        .io = io_,
        .peers = peers_,
        .vl = vl,
        .protocol = protocol_,
        .node_cache = node_cache_,
        .rpc = rpc_,
        .network_id = config_.network_id,
        .tx_ledger_seq_hint =
            ledger_seq_hint ? ledger_seq_hint : cached_seq.value_or(0),
        .get_anchor = [self, vl, anchor_age](uint32_t min_seq)
            -> boost::asio::awaitable<BuildServices::AnchorData> {
            auto bundle = co_await self->get_or_reuse_anchor(vl, anchor_age);
            // If anchor is behind min_seq, force a fresh one once.
            if (min_seq > 0 && bundle.seq < min_seq)
            {
                PLOGW(
                    log_,
                    "[",
                    self->net_label_,
                    "] Anchor seq=",
                    bundle.seq,
                    " behind min_seq=",
                    min_seq,
                    " — fetching fresh");
                bundle = co_await self->get_or_reuse_anchor(vl, 0);
            }
            co_return BuildServices::AnchorData{
                .hdr = bundle.header_result,
                .hash = bundle.anchor_hash,
                .account_hash = bundle.account_hash,
                .validations = bundle.validations,
            };
        },
        .cancel_token = cancel_token,
        .on_step = std::move(on_step),
    };

    //@@authority transition=prv.T0006 status=provisional scope=statement
    // bind=build_proof@628b5988ba4dbec4
    // owner=unresolved owner_hypothesis=ledger_data_control commit=unresolved
    // calls=build_proof
    // label=enter_shared_proof_build_without_operation_identity
    auto result =
        co_await build_proof(svc, tx_hash);  //@@authority-target ids=prv.T0006
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
// lookup_tx() — check tx_ledger cache then RPC, return ledger_seq or 0
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<uint32_t>
ProofEngine::lookup_tx(std::string const& tx_hash)
{
    // Check tx→ledger cache first
    if (auto cached = tx_ledger_get(tx_hash))
        co_return *cached;

    try
    {
        auto result = co_await catl::rpc::co_tx(*rpc_, tx_hash);
        auto const& obj = result.as_object();
        if (auto it = obj.find("ledger_index"); it != obj.end())
        {
            auto seq = static_cast<uint32_t>(it->value().as_int64());
            PLOGI(log_, "[", net_label_, "] lookup_tx found seq=", seq);
            tx_ledger_put(tx_hash, seq);
            co_return seq;
        }
        PLOGW(
            log_, "[", net_label_, "] lookup_tx: no ledger_index in response");
        co_return 0u;
    }
    catch (catl::rpc::RpcTxNotFound const&)
    {
        co_return 0u;
    }
    catch (std::exception const& e)
    {
        PLOGW(
            log_,
            "[",
            net_label_,
            "] lookup_tx(",
            tx_hash.substr(0, 16),
            "...) rpc=",
            config_.rpc_host,
            ":",
            config_.rpc_port,
            " error: ",
            e.what());
        co_return 0u;
    }
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
        "[",
        net_label_,
        "] Cached proof for ",
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
        if (data.size() >= 4 && std::memcmp(data.data(), XPRV_MAGIC, 4) == 0)
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
    auto conn_stats = co_await peers_->co_connection_stats();
    status.peer_count = conn_stats.connected;
    status.total_connects = conn_stats.total_connects;
    status.total_disconnects = conn_stats.total_disconnects;

    status.vl_loaded = co_await vl_cache_->co_is_loaded();

    auto latest = co_await val_buffer_->co_latest_quorum();
    if (latest)
        status.latest_quorum_seq = latest->ledger_seq;
    status.validation_buffer = co_await val_buffer_->co_stats();

    if (ledger_fetch_coordinator_)
    {
        status.ledger_fetch_coordinator.enabled = true;
        status.ledger_fetch_coordinator.lifecycle =
            ledger_fetch_coordinator_->lifecycle();
        status.ledger_fetch_coordinator.ingress_sealed =
            ledger_fetch_coordinator_->ingress_sealed();
        status.ledger_fetch_coordinator.work_outstanding =
            ledger_fetch_coordinator_->work_outstanding();
        status.ledger_fetch_coordinator.outstanding_bind_projections =
            ledger_fetch_coordinator_->outstanding_bind_projections();
        status.ledger_fetch_coordinator.receipt_obligations =
            ledger_fetch_coordinator_->receipt_obligations();
        status.ledger_fetch_coordinator.accounting_broken =
            ledger_fetch_coordinator_->accounting_broken();
    }

    co_return status;
}

// ═══════════════════════════════════════════════════════════════════════
// Anchor bundle cache — future-based, built once per quorum
// ═══════════════════════════════════════════════════════════════════════

asio::awaitable<ProofEngine::AnchorBundle>
ProofEngine::get_or_reuse_anchor(
    std::shared_ptr<catl::vl::ValidatorList> const& vl,
    uint32_t max_age_secs)
{
    if (!vl)
        throw std::runtime_error("validator list snapshot is missing");
    auto const quorum_threshold =
        ValidationBuffer::proof_quorum_threshold(vl->validators.size());
    if (quorum_threshold == 0)
        throw std::runtime_error("validator list contains no validators");

    PLOGI(
        log_,
        "[",
        net_label_,
        "] get_or_reuse_anchor: ENTER max_age=",
        max_age_secs);
    // TODO: why wait for a NEW quorum at all? recent_quorums_ already has
    // the last 30 finalized quorums. The one at .back() is at most ~4s old
    // and already has 34-35/35 validations. Just use it:
    //   auto latest = co_await val_buffer_->co_latest_quorum();
    //   if (latest) → use it, skip co_wait_best_quorum entirely
    //   else → cold start, wait for first quorum
    // Pick the most recent entry with the highest validation count.
    // Don't wait for a ledger that's still "open" — use the prior one
    // that's already finalized with full or near-full validations.
    // Also: max_anchor_age defaults to 0 (always fresh) but the web UI
    // never passes it. Should default to 30s so repeated requests reuse.
    //
    // When max_age_secs > 0, reuse the current anchor if fresh enough.
    // When 0 (default), always wait for the latest quorum.
    std::optional<AnchorBundle> reusable;
    if (max_age_secs > 0)
    {
        std::lock_guard lock(cache_mutex_);
        if (current_anchor_ && current_anchor_->vl_sequence == vl->sequence &&
            current_anchor_->validations.size() >= quorum_threshold &&
            (std::chrono::steady_clock::now() - current_anchor_->built_at) <
                std::chrono::seconds(max_age_secs))
        {
            auto age_s = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::steady_clock::now() -
                             current_anchor_->built_at)
                             .count();
            PLOGI(
                log_,
                "[",
                net_label_,
                "] Anchor: reusing seq=",
                current_anchor_->seq,
                " hash=",
                current_anchor_->anchor_hash.hex().substr(0, 16),
                "... (age=",
                age_s,
                "s)");
            catl::core::emit_status(
                "anchor: reusing seq=" + std::to_string(current_anchor_->seq) +
                " (age=" + std::to_string(age_s) + "s)");
            reusable = *current_anchor_;
        }
    }

    if (reusable)
    {
        if (!validator_list_active_at(
                vl->effective,
                vl->expiration,
                reusable->header_result.header().close_time()))
        {
            std::lock_guard lock(cache_mutex_);
            if (current_anchor_ &&
                current_anchor_->anchor_hash == reusable->anchor_hash &&
                current_anchor_->vl_sequence == reusable->vl_sequence)
            {
                current_anchor_.reset();
            }
            reusable.reset();
        }
    }
    if (reusable)
    {
        if (!co_await val_buffer_->co_sync_unl(vl->validators, vl->sequence))
        {
            throw std::runtime_error(
                "validator list changed while reusing proof anchor");
        }
        co_return *reusable;
    }

    // Use the last full quorum if available — no waiting needed.
    // Fall back to co_wait_best_quorum only on cold start.
    auto full = co_await val_buffer_->co_last_full_quorum(
        std::chrono::seconds(max_age_secs > 0 ? max_age_secs : 10),
        vl->sequence);
    if (full)
    {
        PLOGI(
            log_,
            "[",
            net_label_,
            "] get_or_reuse_anchor: using last full quorum seq=",
            full->ledger_seq,
            " (",
            full->validations.size(),
            "/",
            (vl ? vl->validators.size() : 0),
            ")");
        catl::core::emit_status(
            "using recent full quorum seq=" + std::to_string(full->ledger_seq));
    }
    else
    {
        catl::core::emit_status("waiting for fresh quorum...");
        PLOGI(
            log_,
            "[",
            net_label_,
            "] get_or_reuse_anchor: no full quorum yet, waiting...");
        auto collected = co_await val_buffer_->co_wait_best_quorum(
            std::chrono::seconds(30), vl->sequence);
        if (collected.stop_reason ==
            QuorumCollectStopReason::validator_list_changed)
        {
            throw std::runtime_error(
                "validator list changed while collecting proof quorum");
        }
        full = std::move(collected.quorum);
    }
    auto quorum = std::move(*full);

    if (quorum.ledger_seq == 0 || quorum.ledger_hash == Hash256() ||
        quorum.validations.size() < quorum_threshold)
    {
        throw std::runtime_error("timed out before collecting a proof quorum");
    }
    if (quorum.vl_sequence != vl->sequence)
    {
        throw std::runtime_error(
            "proof quorum does not match validator list snapshot");
    }

    PLOGI(
        log_,
        "[",
        net_label_,
        "] Anchor: seq=",
        quorum.ledger_seq,
        " hash=",
        quorum.ledger_hash.hex().substr(0, 16),
        "... (",
        quorum.validations.size(),
        "/",
        (vl ? vl->validators.size() : 0),
        " validators)");
    catl::core::emit_status(
        "quorum: seq=" + std::to_string(quorum.ledger_seq) + " (" +
        std::to_string(quorum.validations.size()) + "/" +
        std::to_string(vl ? vl->validators.size() : 0) + " validators)");

    auto anchor = co_await get_anchor_bundle(
        quorum.ledger_seq,
        quorum.ledger_hash,
        quorum.vl_sequence,
        vl->effective,
        vl->expiration,
        quorum.validations);

    if (!validator_list_active_at(
            vl->effective,
            vl->expiration,
            anchor.header_result.header().close_time()))
    {
        throw std::runtime_error(
            "anchor is outside validator list validity interval");
    }
    if (!co_await val_buffer_->co_sync_unl(vl->validators, vl->sequence))
    {
        throw std::runtime_error(
            "validator list changed while building proof anchor");
    }

    co_return anchor;
}

void
ProofEngine::invalidate_anchor_cache_locked()
{
    current_anchor_.reset();
    for (auto& [_, entry] : anchor_cache_)
    {
        if (entry.signal)
            entry.signal->cancel();
    }
    anchor_cache_.clear();
}

void
ProofEngine::invalidate_anchor_cache()
{
    std::lock_guard lock(cache_mutex_);
    invalidate_anchor_cache_locked();
}

asio::awaitable<ProofEngine::AnchorBundle>
ProofEngine::get_anchor_bundle(
    uint32_t anchor_seq,
    Hash256 anchor_hash,
    uint32_t vl_sequence,
    uint32_t vl_effective,
    uint32_t vl_expiration,
    std::vector<ValidationCollector::Entry> validations)
{
    enum class Action { hit, wait, fetch };
    Action action;
    std::shared_ptr<asio::steady_timer> signal;
    AnchorBundle hit_bundle;
    AnchorCacheKey const cache_key{vl_sequence, anchor_seq, anchor_hash};

    {
        std::lock_guard lock(cache_mutex_);
        auto [it, inserted] = anchor_cache_.try_emplace(cache_key);

        if (!inserted && it->second.present)
        {
            action = Action::hit;
            hit_bundle = it->second.bundle;
            PLOGI(
                log_, "[", net_label_, "] anchor_bundle: HIT seq=", anchor_seq);
        }
        else if (!inserted && it->second.signal)
        {
            action = Action::wait;
            signal = it->second.signal;
            PLOGI(
                log_,
                "[",
                net_label_,
                "] anchor_bundle: WAIT seq=",
                anchor_seq,
                " — another request is building it");
        }
        else
        {
            it->second.signal = std::make_shared<asio::steady_timer>(
                io_, std::chrono::seconds(30));
            it->second.present = false;
            action = Action::fetch;
            PLOGI(
                log_,
                "[",
                net_label_,
                "] anchor_bundle: FETCH seq=",
                anchor_seq,
                " — building");
        }
    }

    if (action == Action::hit)
    {
        catl::core::emit_status(
            "anchor header: cached seq=" + std::to_string(anchor_seq));
        co_return hit_bundle;
    }

    if (action == Action::wait)
    {
        catl::core::emit_status(
            "anchor header: waiting for in-flight build seq=" +
            std::to_string(anchor_seq));

        boost::system::error_code ec;
        co_await signal->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));

        std::lock_guard lock(cache_mutex_);
        auto it = anchor_cache_.find(cache_key);
        if (it != anchor_cache_.end() && it->second.present)
        {
            catl::core::emit_status(
                "anchor header: in-flight build completed seq=" +
                std::to_string(anchor_seq));
            co_return it->second.bundle;
        }

        throw std::runtime_error("anchor bundle fetch failed");
    }

    // Build: fetch the anchor header
    //
    // get_header owns header fan-out, retry rounds, and per-ledger peer
    // exclusions. Callers propagate its final result instead of layering a
    // second retry loop around the shared in-flight fetch.
    try
    {
        catl::core::emit_status(
            "anchor header: fetching from peer seq=" +
            std::to_string(anchor_seq));
        auto hdr = co_await node_cache_->get_header(anchor_seq, peers_);
        if (hdr.seq() != anchor_seq || hdr.ledger_hash256() != anchor_hash)
        {
            throw std::runtime_error(
                "fetched anchor header does not match validation quorum");
        }
        if (!validator_list_active_at(
                vl_effective, vl_expiration, hdr.header().close_time()))
        {
            throw std::runtime_error(
                "anchor is outside validator list validity interval");
        }

        AnchorBundle bundle;
        bundle.header_result = hdr;
        bundle.anchor_hash = anchor_hash;
        bundle.account_hash = hdr.header().account_hash();
        bundle.seq = anchor_seq;
        bundle.vl_sequence = vl_sequence;
        bundle.validations = std::move(validations);
        bundle.built_at = std::chrono::steady_clock::now();

        // Populate cache and wake waiters
        std::shared_ptr<asio::steady_timer> sig;
        {
            std::lock_guard lock(cache_mutex_);
            auto& entry = anchor_cache_[cache_key];
            sig = entry.signal;
            entry.bundle = bundle;
            entry.present = true;
            entry.signal = nullptr;

            // Update current anchor for reuse
            current_anchor_ = bundle;

            // Prune old anchors — keep at most 10
            static constexpr size_t kMaxAnchorCache = 10;
            while (anchor_cache_.size() > kMaxAnchorCache)
            {
                // The map is ordered by list and ledger sequence.
                auto oldest = anchor_cache_.begin();
                // Don't evict entries with in-flight waiters
                if (oldest->second.signal)
                    break;
                anchor_cache_.erase(oldest);
            }
        }
        if (sig)
            sig->cancel();

        PLOGI(
            log_,
            "[",
            net_label_,
            "] anchor_bundle: built seq=",
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
            auto it = anchor_cache_.find(cache_key);
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
