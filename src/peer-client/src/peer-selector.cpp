#include "catl/peer-client/peer-selector.h"

namespace catl::peer_client {

PeerSelection
select_peer(
    uint32_t target_seq,
    std::vector<PeerCandidate> const& peers,
    std::unordered_set<std::string> const& excluded,
    int elapsed_ms,
    int timeout_ms)
{
    // A covering peer is one whose advertised [first_seq, last_seq]
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
            if (p.failed_for_target)
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

    if (elapsed_ms >= timeout_ms)
        return {PeerDecision::give_up, {}};

    return {PeerDecision::wait, {}};
}

}  // namespace catl::peer_client
