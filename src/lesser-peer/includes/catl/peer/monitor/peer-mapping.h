#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace catl::peer::monitor {

// Maps validator master keys to peer connection indices (V0 = peer-0's
// validator, etc.) using first-delivery voting.
//
// Why voting? In XRPL/Xahau, node identity keys (from the peer handshake
// Public-Key header) and validator keys are completely separate key pairs
// loaded from different config sections ([node_seed] vs [validator_token]).
// There is no protocol message that bridges them. Even xahaud's own
// reduce_relay::Slots uses a similar observation-based approach internally.
//
// How it works: peermon connects directly to each node. When a validation
// arrives, the delivering peer is recorded. Since the direct path (1 hop)
// always beats relayed paths (2+ hops through the hub), the peer that most
// often delivers a validator's validation first is that validator's direct
// connection. In practice this resolves correctly on the very first round.
class PeerMapping
{
public:
    struct PeerInfo
    {
        int index;               // 0-based, extracted from peer-N
        std::string peer_id;     // "peer-0", "peer-1", ...
        std::string master_key;  // validator master key (base58)
    };

    // Called when a validator's validation is first seen for a ledger round.
    // The delivering peer is likely the direct connection to that validator.
    // Accumulates votes; the peer that most often delivers first wins.
    void
    on_first_validation(
        std::string const& peer_id,
        std::string const& validator_key);

    // Lookup: master key → peer index (for V{0-4} display)
    std::optional<int>
    get_peer_index(std::string const& master_key) const;

    // All resolved mappings (sorted by index)
    std::vector<PeerInfo>
    get_all() const;

    // Count of resolved validators
    size_t
    resolved_count() const;

    // Transfer votes from old_key to new_key (ephemeral → master resolution)
    void
    reconcile_key(
        std::string const& old_key,
        std::string const& new_key);

    // Reset all votes and mappings (e.g. on network restart)
    void
    clear();

private:
    static int
    parse_peer_index(std::string const& peer_id);

    void
    rebuild_mapping();

    mutable std::mutex mutex_;

    // Vote counts: validator_key → (peer_id → first-delivery count)
    std::map<std::string, std::map<std::string, int>> first_delivery_votes_;

    // Resolved mapping: master_key → peer index
    std::map<std::string, int> master_to_index_;
};

}  // namespace catl::peer::monitor
