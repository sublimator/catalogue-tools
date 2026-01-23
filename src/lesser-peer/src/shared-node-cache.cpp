#include <catl/peer/shared-node-cache.h>

namespace catl::peer {

SharedNodeCache::GetResult
SharedNodeCache::get_or_claim(Hash256 const& hash, WaiterCallback waiter)
{
    std::lock_guard<std::mutex> lock(mutex_);

    std::string key = hash.hex();
    auto it = entries_.find(key);
    if (it == entries_.end())
    {
        // Not in cache - create pending entry, caller should fetch
        // DON'T add waiter here - caller is the fetcher, not a waiter
        Entry entry;
        entry.state = State::Pending;
        entries_[key] = std::move(entry);
        return {ClaimResult::Claimed, {}};
    }

    auto& entry = it->second;
    switch (entry.state)
    {
        case State::Ready:
            return {ClaimResult::Ready, entry.data};

        case State::Pending:
            // Someone else is fetching - add as waiter
            if (waiter)
                entry.waiters.push_back(std::move(waiter));
            return {ClaimResult::Waiting, {}};

        case State::Failed:
            // Previous attempt failed - let caller retry
            // DON'T add waiter here - caller is the fetcher, not a waiter
            entry.state = State::Pending;
            entry.waiters.clear();
            return {ClaimResult::Claimed, {}};
    }

    return {ClaimResult::Claimed, {}};
}

bool
SharedNodeCache::has(Hash256 const& hash) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::string key = hash.hex();
    auto it = entries_.find(key);
    return it != entries_.end() && it->second.state == State::Ready;
}

void
SharedNodeCache::resolve(Hash256 const& hash, Slice data)
{
    std::string key = hash.hex();
    std::vector<WaiterCallback> waiters_to_notify;
    std::vector<uint8_t> stored_data;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it == entries_.end())
        {
            // Weird - resolving something not in cache
            // Just add it
            Entry entry;
            entry.state = State::Ready;
            entry.data.assign(data.data(), data.data() + data.size());
            entries_[key] = std::move(entry);
            return;
        }

        auto& entry = it->second;
        entry.state = State::Ready;
        entry.data.assign(data.data(), data.data() + data.size());
        waiters_to_notify = std::move(entry.waiters);
        entry.waiters.clear();
        stored_data = entry.data;  // Copy for notifying outside lock
    }

    // Notify waiters outside the lock
    Slice result_data(stored_data.data(), stored_data.size());
    for (auto& cb : waiters_to_notify)
    {
        if (cb)
            cb(true, result_data);
    }
}

void
SharedNodeCache::reject(Hash256 const& hash)
{
    std::string key = hash.hex();
    std::vector<WaiterCallback> waiters_to_notify;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it == entries_.end())
            return;

        waiters_to_notify = std::move(it->second.waiters);
        // Remove entry so it can be retried
        entries_.erase(it);
    }

    // Notify waiters outside the lock
    for (auto& cb : waiters_to_notify)
    {
        if (cb)
            cb(false, Slice{});
    }
}

SharedNodeCache::Stats
SharedNodeCache::get_stats() const
{
    std::lock_guard<std::mutex> lock(mutex_);

    Stats stats{};
    stats.total_entries = entries_.size();

    for (auto const& [key, entry] : entries_)
    {
        if (entry.state == State::Ready)
        {
            stats.ready_entries++;
            stats.total_bytes += entry.data.size();
        }
        else if (entry.state == State::Pending)
        {
            stats.pending_entries++;
        }
    }

    return stats;
}

void
SharedNodeCache::clear()
{
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.clear();
}

}  // namespace catl::peer
