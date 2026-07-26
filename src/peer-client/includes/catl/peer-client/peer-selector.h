#pragma once

// PeerSelector — pure, testable peer selection logic.
//
// Extracts the "which peer should I use for this ledger?" decision
// from the async PeerSet machinery. No asio, no strand, no network.
// Just data in, decision out.
//
// The selection algorithm:
//
//   1. RANGE MATCH:
//      Find a ready, non-excluded peer whose advertised range
//      [first_seq, last_seq] includes the target ledger.
//      Among matches, prefer the one with the lowest selection_count
//      (round-robin load balancing).
//
//   2. WAIT:
//      No covering peer is currently available. Caller should sleep and retry.
//      Discovery (crawl + connect) runs in the background.
//
//   3. GIVE UP (elapsed >= timeout_ms):
//      No covering peer was found within the timeout. Caller should bail.

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
    bool failed_for_target =
        false;  // true if this peer failed for the target ledger
};

enum class PeerDecision {
    use_range_match,  // peer advertises the target ledger
    wait,             // no covering peer yet, keep polling
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
/// @param timeout_ms    Total search budget before giving up
PeerSelection
select_peer(
    uint32_t target_seq,
    std::vector<PeerCandidate> const& peers,
    std::unordered_set<std::string> const& excluded,
    int elapsed_ms,
    int timeout_ms);

}  // namespace catl::peer_client
