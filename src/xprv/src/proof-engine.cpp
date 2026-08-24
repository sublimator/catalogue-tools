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
#include <utility>
#include <vector>

namespace xprv {

namespace asio = boost::asio;
using namespace catl::peer_client;

static LogPartition log_("engine", LogLevel::INFO);

template <class... Args>
void
engine_trace_noexcept(Args&&... args) noexcept
{
    try
    {
        PLOGD(log_, std::forward<Args>(args)...);
    }
    catch (...)
    {
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Lifecycle
// ═══════════════════════════════════════════════════════════════════════

ProofEngine::ProofEngine(asio::io_context& io, NetworkConfig config)
    : io_(io)
    , anchor_wait_home_(io)
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

    // Validation quorum tracker
    quorum_tracker_ =
        ValidationQuorumTracker::create(io_, protocol_, config_.network_id);

    // Wire VL → quorum tracker: when VL loads, push UNL keys
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
            io_, peers_); //@@authority-target ids=prv.T0044
        if (!ledger_fetch_coordinator_)
            throw std::runtime_error(
                "failed to create ledger fetch coordinator generation");
    }

    // Node cache (content-addressed wire node store). The optional coordinator
    // is selected once here; request callsites do not branch on server mode.
    //@@authority transition=prv.T0004 status=provisional scope=statement
    // bind=create@0bf3d062cfeb1a36
    // owner=unresolved owner_hypothesis=ledger_data_provider
    // commit=startup_coroutine calls=create
    // label=construct_live_node_cache_with_selected_transport_capability
    node_cache_ = NodeCache::create(
        io_,
        {.max_entries = node_cache_size_,
         .fetch_timeout = fetch_timeout_,
         .max_walk_peer_retries = max_walk_peer_retries_,
         .fetch_stale_multiplier = fetch_stale_multiplier_},
        ledger_fetch_coordinator_); //@@authority-target ids=prv.T0004

    // Wire peers → quorum tracker: unsolicited validations
    auto vbuf = quorum_tracker_;
    peers_->set_unsolicited_handler(
        [vbuf](uint16_t type, std::vector<uint8_t> const& data) {
            vbuf->on_packet(type, data);
        });

    // Wire observer repost (must be after create, uses shared_from_this)
    peers_->start();

    // Bootstrap peer discovery
    peers_->bootstrap();

    // Push UNL to quorum tracker on every VL load (initial + refresh).
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
    if (quorum_tracker_)
    {
        quorum_tracker_->stop();
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
                ->fence(); //@@authority-target ids=prv.T0045
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
//bind=xprv::co_stop@3da48b9d47023132
// owner=unresolved owner_hypothesis=proof_engine_drain_frame
// observed_home=ProofEngine::co_stop_coroutine_frame
// label=owned_installed_proof_engine_child_population_and_receipt_census
//@@authority channel=prv.C0004 status=provisional scope=function
//bind=xprv::co_stop@3da48b9d47023132
// endpoints=service_lifecycle,proof_engine
// channel_semantics=process_local_join
// label=await_exact_proof_engine_drain_receipt
asio::awaitable<ProofEngine::DrainReceipt>
ProofEngine::co_stop(RequestDrainReceipt const& request_drain)
{
    //@@authority transition=prv.T0166 status=provisional scope=statement
    //bind=if_statement@5f390c1b907fdbdf
    // owner=unresolved owner_hypothesis=proof_engine
    // commit=request_drain_binding_mutex touches=prv.S0056
    // boundary_ref=http.request_drain_receipt
    // receipt=http.request_drain_receipt receipt_role=consume
    // label=accept_exact_http_request_drain_receipt
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

    struct InstalledDrainPopulation
    {
        std::shared_ptr<VlCache> vl_cache;
        std::shared_ptr<ValidationQuorumTracker> validation_quorum_tracker;
        std::shared_ptr<NodeCache> node_cache;
        std::shared_ptr<LedgerFetchCoordinator> ledger_fetch_coordinator;
        std::shared_ptr<PeerSet> peer_set;
    };

    // Snapshot the complete installed child population once. The coroutine
    // frame owns these exact capabilities until every admitted receipt has
    // returned; later member reads cannot silently change the census.
    //@@authority transition=prv.T0165 status=provisional scope=statement bind=declaration@cc899b39a5e62e1a
    // owner=unresolved owner_hypothesis=proof_engine_drain_frame
    // commit=proof_engine_drain_coroutine touches=prv.S0056
    // population_ref=proof_engine.installed_drain_population
    // population_members=proof_engine.vl_cache,proof_engine.validation_quorum_tracker,proof_engine.node_cache,proof_engine.ledger_fetch_coordinator,proof_engine.peer_set
    // label=snapshot_exact_proof_engine_installed_drain_population
    auto const population = InstalledDrainPopulation{
        .vl_cache = vl_cache_,
        .validation_quorum_tracker = quorum_tracker_,
        .node_cache = node_cache_,
        .ledger_fetch_coordinator = ledger_fetch_coordinator_,
        .peer_set = peers_,
    }; //@@authority-target ids=prv.T0165

    // Seal externally admitted ValidationQuorumTracker work before publishing the
    // engine drain fence. This is intentionally not its drain receipt: T0168
    // still crosses the owner strand after the producer (VlCache) has stopped.
    if (population.validation_quorum_tracker)
    {
        //@@authority transition=prv.T0233 status=provisional scope=statement
        // bind=seal_admission_and_join_worker@e4a4fb28b1ae6561
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine calls=seal_admission_and_join_worker
        // touches=prv.S0056
        // population_ref=proof_engine.installed_drain_population
        // member_guard=proof_engine.validation_quorum_tracker
        // label=consume_validation_admission_seal_before_engine_drain_fence
        population.validation_quorum_tracker
            ->seal_admission_and_join_worker(); //@@authority-target ids=prv.T0233
    }

    if (before_engine_draining_log_for_test_)
        before_engine_draining_log_for_test_();
    PLOGI(log_, "[", net_label_, "] Engine draining...");

    // The HTTP admission owner has already drained every request frame before
    // entering here, so no new cache operation can be minted by this service.
    if (population.vl_cache)
    {
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=vl-cache before");
        //@@authority transition=prv.T0167 status=provisional scope=statement
        //bind=co_stop@b589a374022f320d
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine calls=co_stop touches=prv.S0056
        // boundary_ref=vl_cache.co_stop receipt=vl_cache.stop_receipt
        // receipt_role=consume
        // population_ref=proof_engine.installed_drain_population
        // member_guard=proof_engine.vl_cache
        // label=consume_vl_cache_stop_receipt
        co_await population.vl_cache
            ->co_stop(); //@@authority-target ids=prv.T0167
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=vl-cache after");
    }
    if (population.validation_quorum_tracker)
    {
        engine_trace_noexcept(
            "[",
            net_label_,
            "] ProofEngine shutdown stage=quorum-tracker before");
        //@@authority transition=prv.T0168 status=provisional scope=statement
        //bind=co_stop@55fb7a55ae5f23f1
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine calls=co_stop
        // touches=prv.S0056 boundary_ref=validation_quorum_tracker.co_stop
        // receipt=validation_quorum_tracker.stop_receipt receipt_role=consume
        // population_ref=proof_engine.installed_drain_population
        // member_guard=proof_engine.validation_quorum_tracker
        // label=consume_validation_quorum_tracker_stop_receipt
        co_await population.validation_quorum_tracker
            ->co_stop(); //@@authority-target ids=prv.T0168
        engine_trace_noexcept(
            "[",
            net_label_,
            "] ProofEngine shutdown stage=quorum-tracker after");
    }

    if (population.node_cache)
    {
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=node-cache-nodes before");
        //@@authority transition=prv.T0169 status=provisional scope=statement
        //bind=co_stop_nodes@f12dec27e55dcf66
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine calls=co_stop_nodes
        // touches=prv.S0056 boundary_ref=node_cache.co_stop_nodes
        // receipt=node_cache.node_stop_receipt receipt_role=consume
        // population_ref=proof_engine.installed_drain_population
        // member_guard=proof_engine.node_cache
        // label=consume_node_cache_node_stop_receipt
        co_await population.node_cache
            ->co_stop_nodes(); //@@authority-target ids=prv.T0169
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=node-cache-nodes after");
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=node-cache-headers before");
        //@@authority transition=prv.T0170 status=provisional scope=statement
        //bind=co_clear_headers@cc80174195a4f8dc
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine calls=co_clear_headers
        // touches=prv.S0056 boundary_ref=node_cache.co_clear_headers
        // receipt=node_cache.header_clear_receipt receipt_role=consume
        // population_ref=proof_engine.installed_drain_population
        // member_guard=proof_engine.node_cache
        // label=consume_node_cache_header_clear_receipt
        co_await population.node_cache
            ->co_clear_headers(); //@@authority-target ids=prv.T0170
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=node-cache-headers after");
    }

    if (population.ledger_fetch_coordinator)
    {
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=coordinator-fence before");
        //@@authority transition=prv.T0171 status=provisional scope=statement
        //bind=co_fence@ea5809880139a5b6
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine calls=co_fence touches=prv.S0056
        // boundary_ref=ledger_fetch_coordinator.co_fence
        // receipt=ledger_fetch_coordinator.fence_receipt receipt_role=consume
        // population_ref=proof_engine.installed_drain_population
        // member_guard=proof_engine.ledger_fetch_coordinator
        // label=consume_ledger_fetch_coordinator_fence_receipt
        co_await population.ledger_fetch_coordinator
            ->co_fence(); //@@authority-target ids=prv.T0171
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=coordinator-fence after");

        // A PeerSet seal handler can enqueue its pin release behind the first
        // barrier. The second pass consumes those causally generated releases.
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=coordinator-barrier-1 before");
        //@@authority transition=prv.T0172 status=provisional scope=statement
        //bind=co_generation_pin_release_barrier@76093bfed0567b52
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine touches=prv.S0056
        // calls=co_generation_pin_release_barrier
        // boundary_ref=peer_set.first_generation_pin_release_barrier
        // receipt=peer_set.first_generation_pin_barrier_receipt
        // receipt_role=consume
        // population_ref=proof_engine.installed_drain_population
        // member_guard=proof_engine.ledger_fetch_coordinator
        // label=consume_peer_set_first_generation_pin_barrier_receipt
        co_await population.peer_set
            ->co_generation_pin_release_barrier(); //@@authority-target ids=prv.T0172
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=coordinator-barrier-1 after");
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=coordinator-barrier-2 before");
        //@@authority transition=prv.T0173 status=provisional scope=statement
        //bind=co_generation_pin_release_barrier@f48cdc88ed467869
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine touches=prv.S0056
        // calls=co_generation_pin_release_barrier
        // boundary_ref=peer_set.causal_generation_pin_release_barrier
        // receipt=peer_set.causal_generation_pin_barrier_receipt
        // receipt_role=consume
        // population_ref=proof_engine.installed_drain_population
        // member_guard=proof_engine.ledger_fetch_coordinator
        // label=consume_peer_set_causal_generation_pin_barrier_receipt
        co_await population.peer_set
            ->co_generation_pin_release_barrier(); //@@authority-target ids=prv.T0173
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=coordinator-barrier-2 after");

        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=coordinator-reconciliation before");
        //@@authority transition=prv.T0174 status=provisional scope=statement
        //bind=co_reconcile_shutdown_projection_residue@0530249f995cad5d
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine touches=prv.S0056
        // calls=co_reconcile_shutdown_projection_residue
        // boundary_ref=ledger_fetch_coordinator.co_reconcile_shutdown_projection_residue
        // receipt=ledger_fetch_coordinator.projection_reconciliation_receipt
        // receipt_role=consume
        // population_ref=proof_engine.installed_drain_population
        // member_guard=proof_engine.ledger_fetch_coordinator
        // label=consume_ledger_fetch_coordinator_projection_reconciliation_receipt
        auto const lost = co_await population.ledger_fetch_coordinator
                              ->co_reconcile_shutdown_projection_residue(); //@@authority-target ids=prv.T0174
        engine_trace_noexcept(
            "[",
            net_label_,
            "] ProofEngine shutdown stage=coordinator-reconciliation after lost=",
            lost);
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

    if (population.peer_set)
    {
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=peer-set before");
        //@@authority transition=prv.T0175 status=provisional scope=statement
        //bind=co_stop@92d5d679f4dc349e
        // owner=unresolved owner_hypothesis=proof_engine_drain_frame
        // commit=proof_engine_drain_coroutine calls=co_stop touches=prv.S0056
        // boundary_ref=peer_set.co_stop
        // receipt=peer_set.aggregate_stop_receipt receipt_role=consume
        // population_ref=proof_engine.installed_drain_population
        // member_guard=proof_engine.peer_set
        // label=consume_peer_set_aggregate_stop_receipt
        co_await population.peer_set
            ->co_stop(); //@@authority-target ids=prv.T0175
        engine_trace_noexcept(
            "[", net_label_, "] ProofEngine shutdown stage=peer-set after");
    }

    PLOGI(log_, "[", net_label_, "] Engine drained");
    if (before_drain_receipt_publish_for_test_)
        before_drain_receipt_publish_for_test_();
    //@@authority transition=prv.T0176 status=provisional scope=statement
    //bind=co_return_statement@14dc731903ca3625
    // owner=unresolved owner_hypothesis=proof_engine_drain_frame
    // commit=proof_engine_drain_coroutine touches=prv.S0056
    // boundary_ref=proof_engine.drain_receipt
    // receipt=proof_engine.drain_receipt receipt_role=publish
    // population_ref=proof_engine.installed_drain_population
    // label=publish_exact_proof_engine_drain_receipt
    co_return DrainReceipt{shared_from_this()}; //@@authority-target ids=prv.T0176
}
// clang-format on

// ═══════════════════════════════════════════════════════════════════════
// prove() — build a proof chain using shared services
// ═══════════════════════════════════════════════════════════════════════

//@@authority transition=prv.T0005 status=provisional scope=function
// bind=xprv::prove@0687ab7802c2dbbe
// owner=unresolved owner_hypothesis=ledger_data_control commit=unresolved
// label=begin_real_proof_operation_without_authority_identity
asio::awaitable<ProofEngine::ProveResult>
ProofEngine::prove(
    std::string const& tx_hash,
    std::shared_ptr<std::atomic<bool>> cancel_token,
    uint32_t ledger_seq_hint,
    uint32_t max_anchor_age_secs,
    StepCallback on_step,
    std::shared_ptr<catl::core::RequestScope> request_scope)
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
        if (co_await quorum_tracker_->co_sync_unl(vl->validators, vl->sequence))
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
        .request_scope = request_scope,
        .tx_ledger_seq_hint =
            ledger_seq_hint ? ledger_seq_hint : cached_seq.value_or(0),
        .get_anchor = [self, vl, anchor_age, request_scope](uint32_t min_seq)
            -> boost::asio::awaitable<BuildServices::AnchorData> {
            auto bundle = co_await self->get_or_reuse_anchor(
                vl, anchor_age, request_scope);
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
                bundle =
                    co_await self->get_or_reuse_anchor(vl, 0, request_scope);
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
    // bind=build_proof@0846ac119509f6e8
    // owner=unresolved owner_hypothesis=ledger_data_control commit=unresolved
    // calls=build_proof
    // label=enter_shared_proof_build_without_operation_identity
    auto result =
        co_await build_proof(svc, tx_hash); //@@authority-target ids=prv.T0006
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

    auto latest = co_await quorum_tracker_->co_latest_quorum();
    if (latest)
        status.latest_quorum_seq = latest->ledger_seq;
    status.validation_quorum_tracker = co_await quorum_tracker_->co_stats();

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
    uint32_t max_age_secs,
    std::shared_ptr<catl::core::RequestScope> request_scope)
{
    if (!vl)
        throw std::runtime_error("validator list snapshot is missing");
    auto const quorum_threshold =
        ValidationQuorumTracker::proof_quorum_threshold(vl->validators.size());
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
    //   auto latest = co_await quorum_tracker_->co_latest_quorum();
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
        if (!co_await quorum_tracker_->co_sync_unl(
                vl->validators, vl->sequence))
        {
            throw std::runtime_error(
                "validator list changed while reusing proof anchor");
        }
        co_return *reusable;
    }

    // Prefer a proof quorum that a ready peer explicitly advertises. Peer
    // status commonly trails validations by several ledgers; selecting the
    // newest validation quorum unconditionally turns normal status lag into a
    // blind header fetch. Walk the merged ranges newest-first and let the
    // validation owner choose the newest proof quorum below each boundary.
    std::optional<QuorumEntry> full;
    auto const coverage = co_await peers_->co_history_coverage();
    for (auto range = coverage.advertised_ranges.rbegin();
         range != coverage.advertised_ranges.rend();
         ++range)
    {
        auto covered = co_await quorum_tracker_->co_latest_quorum_at_or_before(
            range->last_seq, vl->sequence);
        if (covered && range->contains(covered->ledger_seq))
        {
            PLOGI(
                log_,
                "[",
                net_label_,
                "] get_or_reuse_anchor: using transport-covered quorum seq=",
                covered->ledger_seq,
                " in [",
                range->first_seq,
                ",",
                range->last_seq,
                "]");
            full = std::move(covered);
            break;
        }
    }

    // If no retained proof quorum intersects current peer coverage, preserve
    // the bounded blind fallback for cold start and genuinely stale status.
    if (!full)
    {
        full = co_await quorum_tracker_->co_last_full_quorum(
            std::chrono::seconds(max_age_secs > 0 ? max_age_secs : 10),
            vl->sequence);
    }
    if (full)
    {
        PLOGI(
            log_,
            "[",
            net_label_,
            "] get_or_reuse_anchor: using selected quorum seq=",
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
        auto collected = co_await quorum_tracker_->co_wait_best_quorum(
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
        quorum.validations,
        std::move(request_scope));

    if (!validator_list_active_at(
            vl->effective,
            vl->expiration,
            anchor.header_result.header().close_time()))
    {
        throw std::runtime_error(
            "anchor is outside validator list validity interval");
    }
    if (!co_await quorum_tracker_->co_sync_unl(vl->validators, vl->sequence))
    {
        throw std::runtime_error(
            "validator list changed while building proof anchor");
    }

    co_return anchor;
}

//@@authority transition=prv.T0339 status=provisional scope=function bind=xprv::invalidate_anchor_cache_locked@0fafaa9e26da1d91
// owner=unresolved owner_hypothesis=proof_engine commit=cache_mutex
// touches=prv.S0142
// label=retain_anchor_invalidation_terminal_before_wake
void
ProofEngine::invalidate_anchor_cache_locked()
{
    // Mutex-held terminal commit only. Publication is the unlocked wrapper's
    // job: post_complete is an advisory wake and must not run as a substitute
    // serialization step while cache_mutex_ is held.
    current_anchor_.reset();
    for (auto& [_, operation] : anchor_cache_)
    {
        if (!operation || operation->terminal != AnchorTerminal::in_flight)
            continue;
        operation->terminal = AnchorTerminal::invalidated;
        operation->failure = nullptr;
        operation->bundle.reset();
    }
    anchor_cache_.clear();
}

//@@authority transition=prv.T0340 status=provisional scope=function bind=xprv::invalidate_anchor_cache@50050b274f60b885
// owner=unresolved owner_hypothesis=proof_engine
// commit=cache_mutex_then_signal_home touches=prv.S0142
// writ_roles=retained_signal.publish
// label=publish_retained_anchor_invalidation_after_terminal_commit
void
ProofEngine::invalidate_anchor_cache()
{
    std::vector<AnchorOperationPtr> to_wake;
    {
        std::lock_guard lock(cache_mutex_);
        for (auto const& [_, operation] : anchor_cache_)
        {
            if (operation && operation->terminal == AnchorTerminal::in_flight)
                to_wake.push_back(operation);
        }
        invalidate_anchor_cache_locked();
    }
    for (auto const& operation : to_wake)
    {
        if (operation && operation->signal)
            operation->signal->post_complete();
    }
}

asio::awaitable<ProofEngine::AnchorBundle>
ProofEngine::get_anchor_bundle(
    uint32_t anchor_seq,
    Hash256 anchor_hash,
    uint32_t vl_sequence,
    uint32_t vl_effective,
    uint32_t vl_expiration,
    std::vector<ValidationCollector::Entry> validations,
    std::shared_ptr<catl::core::RequestScope> request_scope)
{
    enum class Action { hit, wait, fetch };
    Action action;
    AnchorOperationPtr operation;
    AnchorBundle hit_bundle;
    AnchorCacheKey const cache_key{vl_sequence, anchor_seq, anchor_hash};

    {
        std::lock_guard lock(cache_mutex_);
        auto it = anchor_cache_.find(cache_key);
        //@@authority transition=prv.T0341 status=provisional scope=statement bind=if_statement@51558c5c99082fbf
        // owner=unresolved owner_hypothesis=proof_engine commit=cache_mutex
        // touches=prv.S0142
        // label=select_hit_join_or_install_exact_anchor_shared_operation
        if (it != anchor_cache_.end() && it->second &&
            it->second->terminal == AnchorTerminal::completed)
        {
            if (!it->second->bundle)
                throw std::logic_error(
                    "completed anchor operation has no retained bundle");
            action = Action::hit;
            hit_bundle = *it->second->bundle;
            PLOGI(
                log_, "[", net_label_, "] anchor_bundle: HIT seq=", anchor_seq);
        }
        else if (
            it != anchor_cache_.end() && it->second &&
            it->second->terminal == AnchorTerminal::in_flight)
        {
            action = Action::wait;
            operation = it->second;
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
            operation = std::make_shared<AnchorOperation>();
            operation->signal =
                std::make_shared<writ::retained_signal>(anchor_wait_home_);
            anchor_cache_.insert_or_assign(cache_key, operation);
            action = Action::fetch;
            PLOGI(
                log_,
                "[",
                net_label_,
                "] anchor_bundle: FETCH seq=",
                anchor_seq,
                " — building");
        } //@@authority-target ids=prv.T0341
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

        auto throw_request_terminal =
            [](catl::core::RequestScopeTerminal terminal) {
                if (terminal ==
                    catl::core::RequestScopeTerminal::deadline_elapsed)
                {
                    throw PeerClientException(
                        Error::Timeout,
                        "anchor bundle waiter deadline elapsed");
                }
                throw PeerClientException(
                    Error::Cancelled, "anchor bundle waiter was cancelled");
            };

        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);
        if (request_scope)
        {
            auto const request = request_scope->snapshot();
            if (request.terminal)
                throw_request_terminal(*request.terminal);
            deadline = request.absolute_deadline;
        }

        if (auto hook = before_anchor_wait_subscribe_for_test_)
            hook();

        //@@authority transition=prv.T0342 status=provisional scope=statement bind=declaration@cde7028a5cfff710
        // owner=unresolved owner_hypothesis=proof_engine
        // commit=anchor_wait_home touches=prv.S0142
        // writ_roles=retained_signal.subscribe
        // label=subscribe_to_exact_anchor_shared_operation_terminal
        auto const outcome = co_await operation->signal->co_wait_until(
            deadline); //@@authority-target ids=prv.T0342

        AnchorTerminal terminal;
        std::optional<AnchorBundle> completed;
        std::exception_ptr failure;
        {
            std::lock_guard lock(cache_mutex_);
            //@@authority transition=prv.T0343 status=provisional bind=expression_statement@06158e4bec728d5c
            //scope=statement
            // owner=unresolved owner_hypothesis=proof_engine
            // commit=cache_mutex touches=prv.S0142
            // writ_roles=retained_signal.revalidate
            // label=revalidate_exact_anchor_terminal_after_wait
            terminal = operation->terminal; //@@authority-target ids=prv.T0343
            completed = operation->bundle;
            failure = operation->failure;
        }

        if (terminal == AnchorTerminal::completed)
        {
            if (!completed)
                throw std::logic_error(
                    "completed anchor operation lost its retained bundle");
            catl::core::emit_status(
                "anchor header: in-flight build completed seq=" +
                std::to_string(anchor_seq));
            co_return *completed;
        }
        if (terminal == AnchorTerminal::failed)
        {
            if (!failure)
                throw std::logic_error(
                    "failed anchor operation lost its retained exception");
            std::rethrow_exception(failure);
        }
        if (terminal == AnchorTerminal::invalidated)
        {
            throw PeerClientException(
                Error::Cancelled, "anchor bundle operation was invalidated");
        }

        if (request_scope)
        {
            auto const request = request_scope->snapshot();
            if (request.terminal)
                throw_request_terminal(*request.terminal);
        }

        //@@authority transition=prv.T0344 status=provisional scope=statement bind=switch_statement@f567139c00543cc7
        // owner=unresolved owner_hypothesis=proof_engine commit=waiter_frame
        // touches=prv.S0142 writ_roles=retained_signal.deadline_or_cancel
        // label=map_waiter_local_deadline_or_cancellation_without_shared_terminal
        switch (outcome)
        {
            case writ::wait_outcome::deadline_elapsed:
                throw PeerClientException(
                    Error::Timeout, "anchor bundle waiter deadline elapsed");
            case writ::wait_outcome::canceled:
                throw PeerClientException(
                    Error::Cancelled, "anchor bundle waiter was cancelled");
            case writ::wait_outcome::signaled:
                throw std::logic_error(
                    "anchor bundle wake had no retained terminal");
        } //@@authority-target ids=prv.T0344
        throw std::logic_error("unknown anchor bundle wait outcome");
    }

    // Build: fetch the anchor header
    //
    // get_header owns header fan-out, retry rounds, and per-ledger peer
    // exclusions. Callers propagate its final result instead of layering a
    // second retry loop around the shared in-flight fetch.
    AnchorBundle bundle;
    try
    {
        catl::core::emit_status(
            "anchor header: fetching from peer seq=" +
            std::to_string(anchor_seq));
        auto hdr = co_await node_cache_->get_header(
            anchor_seq, peers_, nullptr, std::move(request_scope));
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

        bundle.header_result = hdr;
        bundle.anchor_hash = anchor_hash;
        bundle.account_hash = hdr.header().account_hash();
        bundle.seq = anchor_seq;
        bundle.vl_sequence = vl_sequence;
        bundle.validations = std::move(validations);
        bundle.built_at = std::chrono::steady_clock::now();
    }
    catch (...)
    {
        auto const failure = std::current_exception();
        bool publish = false;
        {
            std::lock_guard lock(cache_mutex_);
            auto it = anchor_cache_.find(cache_key);
            //@@authority transition=prv.T0345 status=provisional bind=if_statement@329a89cb890f819b
            //scope=statement
            // owner=unresolved owner_hypothesis=proof_engine
            // commit=cache_mutex touches=prv.S0142
            // label=retain_exact_anchor_builder_failure_or_stale_invalidation
            if (operation->terminal == AnchorTerminal::in_flight)
            {
                if (it != anchor_cache_.end() && it->second == operation)
                {
                    operation->failure = failure;
                    operation->terminal = AnchorTerminal::failed;
                    anchor_cache_.erase(it);
                }
                else
                {
                    operation->terminal = AnchorTerminal::invalidated;
                }
                publish = true;
            } //@@authority-target ids=prv.T0345
        }
        //@@authority transition=prv.T0346 status=provisional scope=statement bind=if_statement@98850c5477c7c2a3
        // owner=unresolved owner_hypothesis=proof_engine
        // commit=anchor_wait_home touches=prv.S0142
        // writ_roles=retained_signal.publish
        // label=publish_exact_anchor_builder_failure_after_terminal_commit
        if (publish)
            operation->signal
                ->post_complete(); //@@authority-target ids=prv.T0346
        std::rethrow_exception(failure);
    }

    bool publish_success = false;
    bool publish_invalidation = false;
    {
        std::lock_guard lock(cache_mutex_);
        auto it = anchor_cache_.find(cache_key);
        //@@authority transition=prv.T0347 status=provisional scope=statement bind=if_statement@39f5735b57f032e1
        // owner=unresolved owner_hypothesis=proof_engine commit=cache_mutex
        // touches=prv.S0142
        // label=retain_exact_anchor_success_or_refuse_stale_publication
        if (it != anchor_cache_.end() && it->second == operation &&
            operation->terminal == AnchorTerminal::in_flight)
        {
            operation->bundle = bundle;
            operation->failure = nullptr;
            operation->terminal = AnchorTerminal::completed;
            current_anchor_ = bundle;
            publish_success = true;

            // Prune old anchors — keep at most 10. A live operation remains a
            // hard predecessor; completed operation objects may be erased
            // because existing waiters retain their exact shared object.
            static constexpr size_t kMaxAnchorCache = 10;
            while (anchor_cache_.size() > kMaxAnchorCache)
            {
                auto oldest = anchor_cache_.begin();
                if (oldest->second &&
                    oldest->second->terminal == AnchorTerminal::in_flight)
                {
                    break;
                }
                anchor_cache_.erase(oldest);
            }
        }
        else if (operation->terminal == AnchorTerminal::in_flight)
        {
            operation->terminal = AnchorTerminal::invalidated;
            publish_invalidation = true;
        } //@@authority-target ids=prv.T0347
    }

    //@@authority transition=prv.T0348 status=provisional scope=statement bind=if_statement@307320d59ddefb36
    // owner=unresolved owner_hypothesis=proof_engine
    // commit=anchor_wait_home touches=prv.S0142
    // writ_roles=retained_signal.publish
    // label=publish_exact_anchor_success_or_invalidation_after_terminal_commit
    if (publish_success || publish_invalidation)
        operation->signal->post_complete(); //@@authority-target ids=prv.T0348
    if (!publish_success)
    {
        throw PeerClientException(
            Error::Cancelled,
            "anchor bundle operation was invalidated before publication");
    }

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

}  // namespace xprv
