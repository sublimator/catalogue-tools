#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace catl::peer::monitor {

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
