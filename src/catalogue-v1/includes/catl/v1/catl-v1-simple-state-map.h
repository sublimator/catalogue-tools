#pragma once

#include "catl/core/types.h"
#include "catl/shamap/shamap-nodetype.h"
#include <iostream>
#include <map>
#include <vector>

namespace catl::v1 {

/**
 * A simple state map implementation that wraps std::map for storing
 * key-value pairs using Hash256 as keys. This provides a simpler interface
 * than the full SHAMap implementation when just the key-value storage is
 * needed.
 *
 * Unlike SHAMap, this implementation does not provide advanced features like:
 * - Copy-on-write semantics
 * - Tree-based hierarchical storage
 * - Path compression
 *
 * It's primarily designed for use cases where raw state data needs to be
 * manipulated without these advanced features.
 */
class SimpleStateMap
{
public:
    /**
     * Default constructor creates an empty state map
     */
    SimpleStateMap() = default;

    /**
     * Destructor
     */
    ~SimpleStateMap() = default;

    /**
     * Copy constructor
     */
    SimpleStateMap(const SimpleStateMap& other) = default;

    /**
     * Move constructor
     */
    SimpleStateMap(SimpleStateMap&& other) noexcept = default;

    /**
     * Copy assignment operator
     */
    SimpleStateMap&
    operator=(const SimpleStateMap& other) = default;

    /**
     * Move assignment operator
     */
    SimpleStateMap&
    operator=(SimpleStateMap&& other) noexcept = default;

    /**
     * Add or update an item in the map
     *
     * @param key The Hash256 key
     * @param data Vector containing the data to store
     * @return true if item was added, false if replaced
     */
    bool
    set_item(const Hash256& key, const std::vector<uint8_t>& data);

    /**
     * Remove an item from the map
     *
     * @param key The Hash256 key of the item to remove
     * @return true if the item was removed, false if it didn't exist
     */
    bool
    remove_item(const Hash256& key);

    /**
     * Get an item from the map
     *
     * @param key The Hash256 key of the item to retrieve
     * @return Reference to the data vector, or throws if not found
     * @throws std::out_of_range if the key does not exist
     */
    const std::vector<uint8_t>&
    get_item(const Hash256& key) const;

    /**
     * Check if the map contains an item with the given key
     *
     * @param key The Hash256 key to check
     * @return true if the key exists in the map, false otherwise
     */
    bool
    contains(const Hash256& key) const;

    /**
     * Get the number of items in the map
     *
     * @return The item count
     */
    size_t
    size() const;

    /**
     * Check if the map is empty
     *
     * @return true if the map is empty, false otherwise
     */
    bool
    empty() const;

    /**
     * Clear all items from the map
     */
    void
    clear();

    /**
     * Visit all items in the map
     *
     * @param visitor Function to call for each key-value pair
     */
    template <typename Visitor>
    void
    visit_items(Visitor&& visitor) const
    {
        for (const auto& [key, data] : items_)
        {
            visitor(key, data);
        }
    }

private:
    // Comparison functor for Hash256 keys
    struct HashCompare
    {
        bool
        operator()(const Hash256& lhs, const Hash256& rhs) const
        {
            return std::memcmp(lhs.data(), rhs.data(), Hash256::size()) < 0;
        }
    };

    // Using std::map for ordered iteration
    std::map<Hash256, std::vector<uint8_t>, HashCompare> items_;
};

/**
 * Write a SimpleStateMap to an output stream in snapshot format
 *
 * This function serializes the state map in the format:
 * - tnACCOUNT_STATE (1 byte)
 * - Key (32 bytes)
 * - Value Length (4 bytes)
 * - Value Data (Value Length bytes)
 * - ... repeat for all items ...
 * - tnTERMINAL (1 byte)
 *
 * This format is compatible with the state snapshot format described in the
 * catl1-slice specification, and can be directly used for generating snapshot
 * files.
 *
 * @param map The SimpleStateMap to serialize
 * @param out The output stream to write to
 * @return Number of bytes written
 * @throws std::ios_base::failure if writing to the stream fails
 */
inline size_t
write_map_to_stream(const SimpleStateMap& map, std::ostream& out)
{
    size_t bytes_written = 0;

    // Write each item in the map
    map.visit_items([&](const Hash256& key, const std::vector<uint8_t>& data) {
        // Write node type
        uint8_t node_type =
            static_cast<uint8_t>(shamap::SHAMapNodeType::tnACCOUNT_STATE);
        out.write(reinterpret_cast<const char*>(&node_type), sizeof(node_type));
        bytes_written += sizeof(node_type);

        // Write key (32 bytes)
        out.write(reinterpret_cast<const char*>(key.data()), key.size());
        bytes_written += key.size();

        // Write value length (uint32_t)
        uint32_t value_length = static_cast<uint32_t>(data.size());
        out.write(
            reinterpret_cast<const char*>(&value_length), sizeof(value_length));
        bytes_written += sizeof(value_length);

        // Write value data
        out.write(reinterpret_cast<const char*>(data.data()), data.size());
        bytes_written += data.size();
    });

    // Write terminal marker
    uint8_t terminal = static_cast<uint8_t>(shamap::SHAMapNodeType::tnTERMINAL);
    out.write(reinterpret_cast<const char*>(&terminal), sizeof(terminal));
    bytes_written += sizeof(terminal);

    return bytes_written;
}

}  // namespace catl::v1