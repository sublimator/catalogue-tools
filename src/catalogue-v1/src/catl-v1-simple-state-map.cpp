#include "catl/v1/catl-v1-simple-state-map.h"

namespace catl::v1 {

bool
SimpleStateMap::set_item(const Hash256& key, const std::vector<uint8_t>& data)
{
    auto [it, inserted] = items_.try_emplace(key, data);
    if (!inserted)
    {
        // Key already exists, update the value
        it->second = data;
    }
    return inserted;
}

bool
SimpleStateMap::remove_item(const Hash256& key)
{
    return items_.erase(key) > 0;
}

const std::vector<uint8_t>&
SimpleStateMap::get_item(const Hash256& key) const
{
    return items_.at(key);
}

bool
SimpleStateMap::contains(const Hash256& key) const
{
    return items_.find(key) != items_.end();
}

size_t
SimpleStateMap::size() const
{
    return items_.size();
}

bool
SimpleStateMap::empty() const
{
    return items_.empty();
}

void
SimpleStateMap::clear()
{
    items_.clear();
}

}  // namespace catl::v1