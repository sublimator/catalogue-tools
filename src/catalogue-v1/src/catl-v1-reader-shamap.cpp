#include "catl/core/log-macros.h"
#include "catl/core/logger.h"
#include "catl/core/types.h"
#include "catl/v1/catl-v1-errors.h"
#include "catl/v1/catl-v1-reader.h"

namespace catl::v1 {

// LogPartition for detailed tracking of map operations
// Default to NONE (disabled) - enable with map_ops_log.enable(LogLevel::DEBUG)
// if needed To enable from debugger:
// catl::v1::map_ops_log.enable(LogLevel::DEBUG) To enable programmatically: Add
// map_ops_log.enable() at start of function
LogPartition map_ops_log("MAP_OPS", LogLevel::NONE);

class OwnedMmapItem : public MmapItem
{
private:
    // ReSharper disable once CppDFANotInitializedField
    std::unique_ptr<uint8_t[]> owned_memory_;  // One allocation for everything

    OwnedMmapItem(
        const uint8_t* key_ptr,
        const uint8_t* data_ptr,
        std::size_t data_size,
        std::unique_ptr<uint8_t[]> owned_memory)
        : MmapItem(key_ptr, data_ptr, data_size)
        , owned_memory_(std::move(owned_memory))
    {
    }

public:
    static boost::intrusive_ptr<MmapItem>
    create(const Key& key, std::vector<uint8_t>&& data)
    {
        // One allocation: [32-byte key][variable data]
        std::size_t total_size = 32 + data.size();
        auto owned_memory = std::make_unique<uint8_t[]>(total_size);

        // Copy key to start of buffer
        std::memcpy(owned_memory.get(), key.data(), 32);

        // Copy data after key
        std::memcpy(owned_memory.get() + 32, data.data(), data.size());

        // Create slices into the single buffer
        const uint8_t* key_ptr = owned_memory.get();        // First 32 bytes
        const uint8_t* data_ptr = owned_memory.get() + 32;  // Rest of buffer

        boost::intrusive_ptr<MmapItem> owned_item(new OwnedMmapItem(
            key_ptr, data_ptr, data.size(), std::move(owned_memory)));
        // Return as base class pointer
        return owned_item;
    }

    ~OwnedMmapItem() = default;
};

/**
 * Implementation of reading a SHAMap that uses an external storage vector
 * to ensure memory persistence for SHAMap items.
 */
template <typename Traits>
MapOperations
Reader::read_map_to_shamap(
    shamap::SHAMapT<Traits>& map,
    shamap::SHAMapNodeType node_type,
    std::vector<uint8_t>& storage,
    bool allow_delta,
    const std::function<void(size_t current_size, size_t growth)>&
        on_storage_growth)
{
    MapOperations ops;

    // Temporary vectors for reading when not storing directly
    std::vector<uint8_t> temp_key(Key::size());

    // Record starting position in storage
    size_t storage_start_pos = storage.size();

    while (true)
    {
        // Read node type directly
        shamap::SHAMapNodeType current_type;
        try
        {
            current_type = read_node_type();
        }
        catch (const CatlV1Error& e)
        {
            LOGE("Error reading node type: ", e.what());
            break;
        }

        // Terminal marker reached
        if (current_type == shamap::tnTERMINAL)
        {
            break;
        }

        // Process based on type
        if (current_type == node_type)
        {
            // Remember position in storage where this item's key begins
            size_t key_pos = storage.size();

            // Read key directly into storage (without trimming to avoid
            // reallocations)
            read_node_key(storage, false);
            // Remember position in storage where this item's data begins
            size_t data_pos = storage.size();
            // Read data directly into storage (without trimming)
            uint32_t data_size = read_node_data(storage, false);

            // Call storage growth callback if provided
            if (on_storage_growth)
            {
                size_t key_growth = data_pos - key_pos;
                size_t data_growth = storage.size() - data_pos;
                size_t total_growth = key_growth + data_growth;
                on_storage_growth(storage.size(), total_growth);
            }

            // Create MmapItem with pointers to our persistent storage
            // The key is at storage[key_pos] through storage[key_pos +
            // Key::size() - 1] The data is at storage[data_pos] through
            // storage[data_pos + data_size - 1]
            auto item = boost::intrusive_ptr(
                new MmapItem(&storage[key_pos], &storage[data_pos], data_size));

            // Log the operation
            PLOGD(
                map_ops_log,
                "Processing item - key: ",
                item->key().hex().substr(0, 16),
                "..., data_size: ",
                data_size,
                " bytes");

            shamap::SetMode mode = allow_delta ? shamap::SetMode::ADD_OR_UPDATE
                                               : shamap::SetMode::ADD_ONLY;
            shamap::SetResult result = map.set_item(item, mode);

            // If the operation failed, throw an appropriate error
            if (!allow_delta && result == shamap::SetResult::FAILED)
            {
                throw CatlV1Error(
                    "Attempted to update existing with allow_delta=false");
            }

            // Track the appropriate operation type
            if (result == shamap::SetResult::ADD)
            {
                ops.nodes_added++;
                PLOGD(
                    map_ops_log,
                    "  -> ADDED item with key: ",
                    item->key().hex().substr(0, 16),
                    "...");
            }
            else if (result == shamap::SetResult::UPDATE)
            {
                ops.nodes_updated++;
                PLOGD(
                    map_ops_log,
                    "  -> UPDATED item with key: ",
                    item->key().hex().substr(0, 16),
                    "...");
            }
            else
            {
                PLOGD(
                    map_ops_log,
                    "  -> FAILED to add/update item with key: ",
                    item->key().hex().substr(0, 16),
                    "...");
            }
        }
        else if (current_type == shamap::tnREMOVE)
        {
            // Check if we're allowed to perform a delete
            if (!allow_delta)
            {
                throw CatlV1DeltaError(
                    "Deletion operation attempted when allow_delta is "
                    "false");
            }

            // For removal nodes, we don't need to add to storage
            // Just read into our temporary vector
            read_node_key(temp_key);
            // Allow_delta is true, so we can remove the item
            Key key(temp_key.data());

            PLOGD(
                map_ops_log,
                "Removing item - key: ",
                key.hex().substr(0, 16),
                "...");

            bool removed = map.remove_item(key);

            // Track removal
            if (removed)
            {
                ops.nodes_deleted++;
                PLOGD(
                    map_ops_log,
                    "  -> DELETED item with key: ",
                    key.hex().substr(0, 16),
                    "...");
            }
            else
            {
                PLOGD(
                    map_ops_log,
                    "  -> FAILED to delete item with key: ",
                    key.hex().substr(0, 16),
                    "... (not found)");
            }
        }
        else
        {
            throw CatlV1Error("Unexpected node type in map");
        }
    }

    // Calculate total nodes processed
    ops.nodes_processed =
        ops.nodes_added + ops.nodes_updated + ops.nodes_deleted;

    LOGD(
        "Processed ",
        ops.nodes_processed,
        " nodes in SHAMap (",
        ops.nodes_added,
        " added, ",
        ops.nodes_updated,
        " updated, ",
        ops.nodes_deleted,
        " deleted), storage increased by ",
        (storage.size() - storage_start_pos),
        " bytes");

    // Note: In a future v2 format implementation, providing a storage estimate
    // in the file header would allow for better pre-allocation and memory
    // management. For now, the caller should either monitor storage growth via
    // the callback or ensure sufficient pre-allocation to avoid reallocation
    // overhead.

    return ops;
}

/**
 * Implementation of read_and_skip_node
 */
shamap::SHAMapNodeType
Reader::read_and_skip_node()
{
    // Read the node type
    shamap::SHAMapNodeType node_type = read_node_type();

    // All node types except TERMINAL have a key
    if (node_type != shamap::tnTERMINAL)
    {
        // Skip key data
        skip_with_tee(Key::size(), "key");

        // For nodes with data, skip the data too
        if (node_type != shamap::tnREMOVE)
        {
            // Read data length
            uint32_t data_length;
            read_value(data_length, "data length");

            // Skip data
            skip_with_tee(data_length, "data");
        }
    }

    return node_type;
}

/**
 * Efficient implementation of skip_map that minimizes allocations
 */
void
Reader::skip_map(shamap::SHAMapNodeType node_type)
{
    while (true)
    {
        // Read node type and skip the rest of the node
        shamap::SHAMapNodeType current_type = read_and_skip_node();

        // Terminal marker reached
        if (current_type == shamap::tnTERMINAL)
        {
            break;
        }

        // Verify node type matches expected type
        if (current_type != node_type && current_type != shamap::tnREMOVE)
        {
            throw CatlV1Error("Unexpected node type in map");
        }
    }
}

/**
 * Implementation of read_node_type
 */
shamap::SHAMapNodeType
Reader::read_node_type()
{
    uint8_t type_byte;
    read_bytes(&type_byte, 1, "node type");
    return static_cast<shamap::SHAMapNodeType>(type_byte);
}

/**
 * Implementation of read_node_key
 */
void
Reader::read_node_key(std::vector<uint8_t>& key_out, bool resize_to_fit)
{
    if (resize_to_fit)
    {
        // Resize vector to hold exactly the key data
        key_out.resize(Key::size());

        // Read key data
        read_bytes(key_out.data(), Key::size(), "key");
    }
    else
    {
        // Read directly into vector's unused capacity and update size
        read_bytes_into_capacity(key_out, Key::size(), "key");
    }
}

/**
 * Implementation of read_node_data
 */
uint32_t
Reader::read_node_data(std::vector<uint8_t>& data_out, bool resize_to_fit)
{
    // Read data length
    uint32_t data_length;
    read_value(data_length, "data length");

    if (resize_to_fit)
    {
        // Resize vector to hold exactly the data - this ensures the vector size
        // matches the exact data length and allows the caller to rely on this
        // size
        data_out.resize(data_length);

        // Read data
        if (data_length > 0)
        {
            read_bytes(data_out.data(), data_length, "data");
        }
    }
    else
    {
        // Read directly into vector's unused capacity if data exists
        if (data_length > 0)
        {
            read_bytes_into_capacity(data_out, data_length, "data");
        }
    }

    return data_length;
}

/**
 * Implementation of read_map_node
 */
bool
Reader::read_map_node(
    shamap::SHAMapNodeType& type_out,
    std::vector<uint8_t>& key_out,
    std::vector<uint8_t>& data_out)
{
    // Read type byte
    type_out = read_node_type();

    // Check for terminal marker
    if (type_out == shamap::tnTERMINAL)
    {
        return false;
    }

    // Read key (with trim=true by default)
    read_node_key(key_out);

    // For non-remove nodes, read data
    if (type_out != shamap::tnREMOVE)
    {
        read_node_data(data_out);
    }
    else
    {
        // For removal nodes, clear data buffer
        data_out.clear();
    }

    return true;
}

/**
 * Implementation of read_map with separate callbacks for nodes and deletions
 */
MapOperations
Reader::read_map_with_callbacks(
    shamap::SHAMapNodeType type,
    const std::function<
        void(const std::vector<uint8_t>&, const std::vector<uint8_t>&)>&
        on_node,
    const std::function<void(const std::vector<uint8_t>&)>& on_delete)
{
    MapOperations ops;
    std::vector<uint8_t> key_buffer(Key::size());
    std::vector<uint8_t> data_buffer;

    while (true)
    {
        // Read node type
        shamap::SHAMapNodeType current_type = read_node_type();

        // Terminal marker reached
        if (current_type == shamap::tnTERMINAL)
        {
            break;
        }

        // Process based on node type
        if (current_type == type)
        {
            // Read key
            read_node_key(key_buffer);

            // Read data
            read_node_data(data_buffer);

            // Call callback with key and data if provided
            if (on_node)
            {
                on_node(key_buffer, data_buffer);
            }

            // Track as node added - note we don't know if it's an update
            // without additional context, so we count it as an addition
            ops.nodes_added++;
        }
        else if (current_type == shamap::tnREMOVE)
        {
            // Read key
            read_node_key(key_buffer);

            // Call on_delete callback if provided
            if (on_delete)
            {
                on_delete(key_buffer);
            }

            // Track as node deleted
            ops.nodes_deleted++;
        }
        else
        {
            throw CatlV1Error("Unexpected node type in map");
        }
    }

    // Calculate total nodes processed
    ops.nodes_processed =
        ops.nodes_added + ops.nodes_updated + ops.nodes_deleted;

    return ops;
}

/**
 * Implementation of reading a SHAMap using OwnedMmapItem instances
 * that own their memory rather than referencing external storage.
 */
template <typename Traits>
MapOperations
Reader::read_map_with_shamap_owned_items(
    shamap::SHAMapT<Traits>& map,
    shamap::SHAMapNodeType node_type,
    bool allow_delta)
{
    MapOperations ops;

    // Temporary vector for removal keys only
    std::vector<uint8_t> temp_key(Key::size());

    while (true)
    {
        // Read node type directly
        shamap::SHAMapNodeType current_type;
        try
        {
            current_type = read_node_type();
        }
        catch (const CatlV1Error& e)
        {
            LOGE("Error reading node type: ", e.what());
            break;
        }

        // Terminal marker reached
        if (current_type == shamap::tnTERMINAL)
        {
            break;
        }

        // Process based on type
        if (current_type == node_type)
        {
            // Read key into temporary buffer
            std::vector<uint8_t> key_data;
            read_node_key(key_data);

            // Read data into temporary buffer
            std::vector<uint8_t> item_data;
            read_node_data(item_data);

            // Create Key from the read data
            Key key(key_data.data());

            // Log key and data size before creating item
            PLOGD(
                map_ops_log,
                "Processing item - key: ",
                key.hex().substr(0, 16),
                "..., data_size: ",
                item_data.size(),
                " bytes");

            // Create OwnedMmapItem with the data
            auto item = OwnedMmapItem::create(key, std::move(item_data));

            shamap::SetMode mode = allow_delta ? shamap::SetMode::ADD_OR_UPDATE
                                               : shamap::SetMode::ADD_ONLY;
            shamap::SetResult result = map.set_item(item, mode);

            // If the operation failed, throw an appropriate error
            if (!allow_delta && result == shamap::SetResult::FAILED)
            {
                throw CatlV1Error(
                    "Attempted to update existing with allow_delta=false");
            }

            // Track the appropriate operation type
            if (result == shamap::SetResult::ADD)
            {
                ops.nodes_added++;
                PLOGD(
                    map_ops_log,
                    "  -> ADDED item with key: ",
                    key.hex().substr(0, 16),
                    "...");
            }
            else if (result == shamap::SetResult::UPDATE)
            {
                ops.nodes_updated++;
                PLOGD(
                    map_ops_log,
                    "  -> UPDATED item with key: ",
                    key.hex().substr(0, 16),
                    "...");
            }
            else
            {
                PLOGD(
                    map_ops_log,
                    "  -> FAILED to add/update item with key: ",
                    key.hex().substr(0, 16),
                    "...");
            }
        }
        else if (current_type == shamap::tnREMOVE)
        {
            // Check if we're allowed to perform a delete
            if (!allow_delta)
            {
                throw CatlV1DeltaError(
                    "Deletion operation attempted when allow_delta is "
                    "false");
            }

            // For removal nodes, read key into temporary buffer
            read_node_key(temp_key);

            // Allow_delta is true, so we can remove the item
            Key key(temp_key.data());

            PLOGD(
                map_ops_log,
                "Removing item - key: ",
                key.hex().substr(0, 16),
                "...");

            bool removed = map.remove_item(key);

            // Track removal
            if (removed)
            {
                ops.nodes_deleted++;
                PLOGD(
                    map_ops_log,
                    "  -> DELETED item with key: ",
                    key.hex().substr(0, 16),
                    "...");
            }
            else
            {
                PLOGD(
                    map_ops_log,
                    "  -> FAILED to delete item with key: ",
                    key.hex().substr(0, 16),
                    "... (not found)");
            }
        }
        else
        {
            throw CatlV1Error("Unexpected node type in map");
        }
    }

    // Calculate total nodes processed
    ops.nodes_processed =
        ops.nodes_added + ops.nodes_updated + ops.nodes_deleted;

    LOGD(
        "Processed ",
        ops.nodes_processed,
        " nodes in SHAMap with owned items (",
        ops.nodes_added,
        " added, ",
        ops.nodes_updated,
        " updated, ",
        ops.nodes_deleted,
        " deleted)");

    return ops;
}

INSTANTIATE_READER_SHAMAP_NODE_TRAITS(shamap::DefaultNodeTraits)
}  // namespace catl::v1
