#include <catl/core/logger.h>
#include <catl/peer/monitor/peer-mapping.h>

#include <algorithm>
#include <set>

namespace catl::peer::monitor {

static LogPartition&
peer_mapping_log()
{
    static LogPartition partition("PEERMAP", LogLevel::INFO);
    return partition;
}

int
PeerMapping::parse_peer_index(std::string const& peer_id)
{
    auto pos = peer_id.find('-');
    if (pos != std::string::npos && pos + 1 < peer_id.size())
        return std::stoi(peer_id.substr(pos + 1));
    return -1;
}

void
PeerMapping::on_first_validation(
    std::string const& peer_id,
    std::string const& validator_key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    first_delivery_votes_[validator_key][peer_id]++;
    rebuild_mapping();
}

std::optional<int>
PeerMapping::get_peer_index(std::string const& master_key) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = master_to_index_.find(master_key);
    if (it != master_to_index_.end())
        return it->second;
    return std::nullopt;
}

std::vector<PeerMapping::PeerInfo>
PeerMapping::get_all() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PeerInfo> result;
    result.reserve(master_to_index_.size());
    for (auto const& [master_key, index] : master_to_index_)
    {
        PeerInfo info;
        info.index = index;
        info.master_key = master_key;
        // Find the winning peer_id for this validator
        auto vit = first_delivery_votes_.find(master_key);
        if (vit != first_delivery_votes_.end())
        {
            int best_count = 0;
            for (auto const& [pid, count] : vit->second)
            {
                if (count > best_count)
                {
                    best_count = count;
                    info.peer_id = pid;
                }
            }
        }
        result.push_back(std::move(info));
    }
    std::sort(
        result.begin(), result.end(), [](PeerInfo const& a, PeerInfo const& b) {
            return a.index < b.index;
        });
    return result;
}

size_t
PeerMapping::resolved_count() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return master_to_index_.size();
}

void
PeerMapping::rebuild_mapping()
{
    // For each validator, find the peer that most frequently delivers first
    // Then assign that peer's index to the validator.
    // Resolve greedily: highest vote count first, no index conflicts.

    struct Candidate
    {
        std::string validator_key;
        std::string peer_id;
        int peer_index;
        int votes;
    };

    std::vector<Candidate> candidates;
    for (auto const& [vk, peer_counts] : first_delivery_votes_)
    {
        for (auto const& [pid, count] : peer_counts)
        {
            int idx = parse_peer_index(pid);
            if (idx >= 0)
                candidates.push_back({vk, pid, idx, count});
        }
    }

    // Sort by votes descending (greedy: strongest associations first)
    std::sort(
        candidates.begin(),
        candidates.end(),
        [](Candidate const& a, Candidate const& b) {
            return a.votes > b.votes;
        });

    std::map<std::string, int> new_mapping;
    std::set<int> used_indices;
    std::set<std::string> mapped_validators;

    for (auto const& c : candidates)
    {
        if (mapped_validators.count(c.validator_key))
            continue;
        if (used_indices.count(c.peer_index))
            continue;

        new_mapping[c.validator_key] = c.peer_index;
        used_indices.insert(c.peer_index);
        mapped_validators.insert(c.validator_key);
    }

    // Log if mapping changed
    if (new_mapping != master_to_index_)
    {
        PLOGI(peer_mapping_log(), "mapping updated:");
        for (auto const& [vk, idx] : new_mapping)
        {
            PLOGI(
                peer_mapping_log(),
                "  V",
                idx,
                " -> ",
                vk.substr(0, 12),
                "...");
        }
    }

    master_to_index_ = std::move(new_mapping);
}

}  // namespace catl::peer::monitor
