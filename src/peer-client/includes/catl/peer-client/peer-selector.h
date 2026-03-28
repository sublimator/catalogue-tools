#pragma once

// PeerSelector — pure, testable peer selection logic.
//
// Extracts the "which peer should I use for this ledger?" decision
// from the async PeerSet machinery. No asio, no strand, no network.
// Just data in, decision out.
//
// The selection algorithm:
//
//   1. RANGE MATCH (always, any elapsed time):
//      Find a ready, non-excluded peer whose advertised range
//      [first_seq, last_seq] includes the target ledger.
//      Among matches, prefer the one with the lowest selection_count
//      (round-robin load balancing).
//
//   2. FALLBACK (after fallback_ms without a range match):
//      Try any ready, non-excluded peer, preferring wider ranges
//      (last_seq - first_seq descending). Full-history nodes often
//      report narrow ranges but can still serve old ledger data.
//      The caller retries with a different peer if it can't serve.
//
//   3. WAIT (before fallback_ms, or no peers at all):
//      No peer selected yet. Caller should sleep and retry.
//      Discovery (crawl + connect) runs in the background.
//
//   4. GIVE UP (elapsed >= timeout_ms):
//      No peer found within the timeout. Caller should bail.

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

namespace catl::peer_client {

struct PeerCandidate
{
    std::string endpoint;
    uint32_t first_seq = 0;
    uint32_t last_seq = 0;
    uint64_t selection_count = 0;
    bool is_ready = false;
};

enum class PeerDecision
{
    use_range_match,  // peer covers target — best case
    use_fallback,     // no range match, trying widest peer
    wait,             // too early for fallback, keep polling
    give_up           // exhausted timeout
};

struct PeerSelection
{
    PeerDecision decision = PeerDecision::give_up;
    std::string endpoint;  // which peer (empty if wait/give_up)
};

/// Pure peer selection — no side effects, fully unit-testable.
///
/// @param target_seq    Ledger sequence we need a peer for
/// @param peers         Snapshot of currently connected peers
/// @param excluded      Peers that already failed for this request
/// @param elapsed_ms    How long we've been searching (0 on first call)
/// @param fallback_ms   When to start trying non-range-matched peers
///                      (0 = never fallback, positive = delay in ms)
/// @param timeout_ms    Total search budget before giving up
PeerSelection
select_peer(
    uint32_t target_seq,
    std::vector<PeerCandidate> const& peers,
    std::unordered_set<std::string> const& excluded,
    int elapsed_ms,
    int fallback_ms,
    int timeout_ms);

}  // namespace catl::peer_client
