#pragma once

// Test-side friend surface for ProofEngine (XPRV_TESTING builds only —
// the friend declaration lives in xprv/proof-engine.h). Hoisted from
// service-lifecycle-gtest.cpp so multiple test TUs in the single
// xprv-gtest binary can share one definition without ODR violations.

#include <xprv/proof-engine.h>

#include "service-lifecycle.h"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>

#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <utility>

namespace xprv {

class ProofEngineTestAccess
{
public:
    static ProofEngine::DrainReceipt
    mint_drain_receipt(std::shared_ptr<ProofEngine> const& engine)
    {
        return ProofEngine::DrainReceipt{engine};
    }

    static detail::EngineDrainOperation
    make_engine_drain_operation(
        std::shared_ptr<ProofEngine> const& engine,
        std::function<boost::asio::awaitable<ProofEngine::DrainReceipt>(
            RequestDrainReceipt const&)> stop)
    {
        return detail::EngineDrainOperation{engine, std::move(stop)};
    }

    static void
    set_validation_quorum_tracker(
        ProofEngine& engine,
        std::shared_ptr<ValidationQuorumTracker> validation_quorum_tracker)
    {
        engine.quorum_tracker_ = std::move(validation_quorum_tracker);
    }

    static void
    publisher_revoked(ProofEngine& engine, catl::vl::Manifest const& revocation)
    {
        engine.on_publisher_revoked(revocation);
    }

    static void
    seed_proof_cache(
        ProofEngine& engine,
        std::string const& tx_hash,
        ProofEngine::ProveResult const& result)
    {
        engine.cache_put(tx_hash, result);
    }

    static void
    set_node_cache(
        ProofEngine& engine,
        std::shared_ptr<catl::peer_client::NodeCache> node_cache)
    {
        engine.node_cache_ = std::move(node_cache);
    }

    static void
    set_ledger_fetch_coordinator(
        ProofEngine& engine,
        std::shared_ptr<catl::peer_client::LedgerFetchCoordinator> coordinator)
    {
        engine.ledger_fetch_coordinator_ = std::move(coordinator);
    }

    static void
    seed_tx_ledger(
        ProofEngine& engine,
        std::string const& tx_hash,
        uint32_t seq)
    {
        engine.tx_ledger_put(tx_hash, seq);
    }

    static void
    set_peer_set(
        ProofEngine& engine,
        std::shared_ptr<catl::peer_client::PeerSet> peers)
    {
        engine.peers_ = std::move(peers);
    }

    static void
    set_before_drain_receipt_publish(
        ProofEngine& engine,
        std::function<void()> hook)
    {
        engine.before_drain_receipt_publish_for_test_ = std::move(hook);
    }

    static void
    set_before_engine_draining_log(
        ProofEngine& engine,
        std::function<void()> hook)
    {
        engine.before_engine_draining_log_for_test_ = std::move(hook);
    }

    // ── Issue 0074: anchor-bundle wait witnesses (tests-only) ────────
    //
    // The waiter path under test is the PRODUCTION get_anchor_bundle
    // Action::wait branch, entered through join_anchor_wait below. The
    // builder-side helpers reproduce the exact cache transitions the
    // production fetch arm performs — exact operation installation, retained
    // terminal publication, and exact-row failure cleanup — with a
    // test-owned RequestScope deadline. This mirrors the established
    // NodeCacheTestAccess::create_in_flight idiom.

    using AnchorBundle = ProofEngine::AnchorBundle;
    using AnchorOperationPtr = ProofEngine::AnchorOperationPtr;

    static AnchorOperationPtr
    create_in_flight_anchor(
        ProofEngine& engine,
        uint32_t anchor_seq,
        Hash256 const& anchor_hash,
        uint32_t vl_sequence)
    {
        auto operation = std::make_shared<ProofEngine::AnchorOperation>();
        operation->signal =
            std::make_shared<writ::retained_signal>(engine.anchor_wait_home_);
        std::lock_guard lock(engine.cache_mutex_);
        engine.anchor_cache_.insert_or_assign(
            ProofEngine::AnchorCacheKey{vl_sequence, anchor_seq, anchor_hash},
            operation);
        return operation;
    }

    static bool
    anchor_entry_in_flight(
        ProofEngine& engine,
        uint32_t anchor_seq,
        Hash256 const& anchor_hash,
        uint32_t vl_sequence)
    {
        std::lock_guard lock(engine.cache_mutex_);
        auto it = engine.anchor_cache_.find(
            ProofEngine::AnchorCacheKey{vl_sequence, anchor_seq, anchor_hash});
        return it != engine.anchor_cache_.end() && it->second &&
            it->second->terminal == ProofEngine::AnchorTerminal::in_flight;
    }

    /// The production waiter/hit coroutine. vl_effective/vl_expiration
    /// are consumed only by the build arm, which these witnesses never
    /// enter (an in-flight or present entry always exists first).
    static boost::asio::awaitable<AnchorBundle>
    join_anchor_wait(
        std::shared_ptr<ProofEngine> engine,
        uint32_t anchor_seq,
        Hash256 anchor_hash,
        uint32_t vl_sequence,
        std::chrono::milliseconds timeout)
    {
        auto request_scope = catl::core::RequestScope::create(
            catl::core::RequestScope::Clock::now() + timeout);
        if (!request_scope)
            throw std::runtime_error("test request scope unavailable");
        co_return co_await engine->get_anchor_bundle(
            anchor_seq,
            anchor_hash,
            vl_sequence,
            0,
            std::numeric_limits<uint32_t>::max(),
            {},
            std::move(request_scope));
    }

    /// Production waiter path with no RequestScope: the 30-second join
    /// policy in get_anchor_bundle, not a test-scaled deadline.
    static boost::asio::awaitable<AnchorBundle>
    join_anchor_wait_without_request_scope(
        std::shared_ptr<ProofEngine> engine,
        uint32_t anchor_seq,
        Hash256 anchor_hash,
        uint32_t vl_sequence)
    {
        co_return co_await engine->get_anchor_bundle(
            anchor_seq,
            anchor_hash,
            vl_sequence,
            0,
            std::numeric_limits<uint32_t>::max(),
            {},
            nullptr);
    }

    static void
    set_before_anchor_wait_subscribe(
        ProofEngine& engine,
        std::function<void()> hook)
    {
        engine.before_anchor_wait_subscribe_for_test_ = std::move(hook);
    }

    /// Mirrors the fetch arm's SUCCESS transition: accept only the exact
    /// installed operation, retain its bundle terminal, then publish its wake.
    static bool
    complete_anchor_build(
        ProofEngine& engine,
        uint32_t anchor_seq,
        Hash256 const& anchor_hash,
        uint32_t vl_sequence,
        AnchorOperationPtr const& operation)
    {
        AnchorBundle bundle;
        bundle.anchor_hash = anchor_hash;
        bundle.seq = anchor_seq;
        bundle.vl_sequence = vl_sequence;
        bundle.built_at = std::chrono::steady_clock::now();

        bool publish = false;
        {
            std::lock_guard lock(engine.cache_mutex_);
            auto const it =
                engine.anchor_cache_.find(ProofEngine::AnchorCacheKey{
                    vl_sequence, anchor_seq, anchor_hash});
            if (it != engine.anchor_cache_.end() && it->second == operation &&
                operation->terminal == ProofEngine::AnchorTerminal::in_flight)
            {
                operation->bundle = bundle;
                operation->failure = nullptr;
                operation->terminal = ProofEngine::AnchorTerminal::completed;
                engine.current_anchor_ = bundle;
                publish = true;
            }
        }
        if (publish)
            operation->signal->post_complete();
        return publish;
    }

    /// Mirrors the fetch arm's FAILURE transition: retain the original
    /// exception in the exact operation, remove only its row, then wake.
    static bool
    fail_anchor_build(
        ProofEngine& engine,
        uint32_t anchor_seq,
        Hash256 const& anchor_hash,
        uint32_t vl_sequence,
        AnchorOperationPtr const& operation,
        std::string message)
    {
        bool publish = false;
        {
            std::lock_guard lock(engine.cache_mutex_);
            auto it = engine.anchor_cache_.find(ProofEngine::AnchorCacheKey{
                vl_sequence, anchor_seq, anchor_hash});
            if (it != engine.anchor_cache_.end() && it->second == operation &&
                operation->terminal == ProofEngine::AnchorTerminal::in_flight)
            {
                operation->failure = std::make_exception_ptr(
                    std::runtime_error(std::move(message)));
                operation->terminal = ProofEngine::AnchorTerminal::failed;
                engine.anchor_cache_.erase(it);
                publish = true;
            }
        }
        if (publish)
            operation->signal->post_complete();
        return publish;
    }

    /// The real production invalidation.
    static void
    invalidate_anchor_cache(ProofEngine& engine)
    {
        engine.invalidate_anchor_cache();
    }
};

}  // namespace xprv
