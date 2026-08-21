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
    // production fetch arm performs — entry creation
    // (proof-engine.cpp:1204-1206), success publication (:1283-1307),
    // failure cleanup (:1322-1334) — with a test-scaled deadline in
    // place of the hard-coded 30 s. This mirrors the established
    // NodeCacheTestAccess::create_in_flight idiom.

    using AnchorBundle = ProofEngine::AnchorBundle;

    static void
    create_in_flight_anchor(
        ProofEngine& engine,
        uint32_t anchor_seq,
        Hash256 const& anchor_hash,
        uint32_t vl_sequence,
        std::chrono::milliseconds deadline)
    {
        std::lock_guard lock(engine.cache_mutex_);
        auto& entry = engine.anchor_cache_[ProofEngine::AnchorCacheKey{
            vl_sequence, anchor_seq, anchor_hash}];
        entry.signal =
            std::make_shared<boost::asio::steady_timer>(engine.io_, deadline);
        entry.present = false;
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
        return it != engine.anchor_cache_.end() && it->second.signal &&
            !it->second.present;
    }

    /// The production waiter/hit coroutine. vl_effective/vl_expiration
    /// are consumed only by the build arm, which these witnesses never
    /// enter (an in-flight or present entry always exists first).
    static boost::asio::awaitable<AnchorBundle>
    join_anchor_wait(
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

    /// Mirrors the fetch arm's SUCCESS transition verbatim
    /// (proof-engine.cpp:1283-1307): publish bundle, drop the signal
    /// from the entry, cancel it outside the lock.
    static void
    complete_anchor_build(
        ProofEngine& engine,
        uint32_t anchor_seq,
        Hash256 const& anchor_hash,
        uint32_t vl_sequence)
    {
        AnchorBundle bundle;
        bundle.anchor_hash = anchor_hash;
        bundle.seq = anchor_seq;
        bundle.vl_sequence = vl_sequence;
        bundle.built_at = std::chrono::steady_clock::now();

        std::shared_ptr<boost::asio::steady_timer> sig;
        {
            std::lock_guard lock(engine.cache_mutex_);
            auto& entry = engine.anchor_cache_[ProofEngine::AnchorCacheKey{
                vl_sequence, anchor_seq, anchor_hash}];
            sig = entry.signal;
            entry.bundle = bundle;
            entry.present = true;
            entry.signal = nullptr;
            engine.current_anchor_ = bundle;
        }
        if (sig)
            sig->cancel();
    }

    /// Mirrors the fetch arm's FAILURE transition verbatim
    /// (proof-engine.cpp:1322-1334): erase the entry, cancel outside
    /// the lock.
    static void
    fail_anchor_build(
        ProofEngine& engine,
        uint32_t anchor_seq,
        Hash256 const& anchor_hash,
        uint32_t vl_sequence)
    {
        std::shared_ptr<boost::asio::steady_timer> sig;
        {
            std::lock_guard lock(engine.cache_mutex_);
            auto it = engine.anchor_cache_.find(ProofEngine::AnchorCacheKey{
                vl_sequence, anchor_seq, anchor_hash});
            if (it != engine.anchor_cache_.end())
            {
                sig = it->second.signal;
                engine.anchor_cache_.erase(it);
            }
        }
        if (sig)
            sig->cancel();
    }

    /// The real production invalidation (proof-engine.cpp:1156-1161).
    static void
    invalidate_anchor_cache(ProofEngine& engine)
    {
        engine.invalidate_anchor_cache();
    }
};

}  // namespace xprv
