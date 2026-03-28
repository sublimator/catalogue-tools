#include "catl/peer-client/peer-selector.h"

#include <algorithm>
#include <limits>

namespace catl::peer_client {

PeerSelection
select_peer(
    uint32_t target_seq,
    std::vector<PeerCandidate> const& peers,
    std::unordered_set<std::string> const& excluded,
    int elapsed_ms,
    int fallback_ms,
    int timeout_ms)
{
    // ── Phase 1: Always try range match first ────────────────────
    //
    // A range-matched peer is one whose advertised [first_seq, last_seq]
    // includes our target. Among matches, pick the least-selected
    // (round-robin load balancing).
    {
        PeerCandidate const* best = nullptr;

        for (auto const& p : peers)
        {
            if (!p.is_ready)
                continue;
            if (excluded.count(p.endpoint))
                continue;
            if (p.first_seq == 0 || p.last_seq == 0)
                continue;
            if (target_seq < p.first_seq || target_seq > p.last_seq)
                continue;

            if (!best || p.selection_count < best->selection_count)
                best = &p;
        }

        if (best)
            return {PeerDecision::use_range_match, best->endpoint};
    }

    // ── Phase 2: Fallback to widest-range peer ───────────────────
    //
    // After fallback_ms without a range match, try any ready peer.
    // Prefer peers with the widest advertised range — they're most
    // likely to be full-history nodes that can serve old data even
    // if their status message doesn't cover the target.
    //
    // fallback_ms <= 0 means never fallback (old behavior).
    if (fallback_ms > 0 && elapsed_ms >= fallback_ms)
    {
        PeerCandidate const* best = nullptr;
        uint64_t best_span = 0;

        for (auto const& p : peers)
        {
            if (!p.is_ready)
                continue;
            if (excluded.count(p.endpoint))
                continue;

            uint64_t span = (p.last_seq > p.first_seq)
                ? static_cast<uint64_t>(p.last_seq - p.first_seq)
                : 0;

            if (!best || span > best_span ||
                (span == best_span &&
                 p.selection_count < best->selection_count))
            {
                best = &p;
                best_span = span;
            }
        }

        if (best)
            return {PeerDecision::use_fallback, best->endpoint};
    }

    // ── Phase 3: Wait or give up ─────────────────────────────────

    if (elapsed_ms >= timeout_ms)
        return {PeerDecision::give_up, {}};

    return {PeerDecision::wait, {}};
}

}  // namespace catl::peer_client
