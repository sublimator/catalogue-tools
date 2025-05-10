#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/v1/catl-v1-errors.h"
#include "catl/v1/catl-v1-reader.h"

namespace catl::v1 {

/**
 * Implementation of read_shamap that uses an external storage vector
 * to ensure memory persistence for SHAMap items.
 */
uint32_t
Reader::read_shamap(
    SHAMap& map,
    SHAMapNodeType node_type,
    std::vector<uint8_t>& storage,
    bool allow_updates)
{
    uint32_t nodes_processed = 0;

    // Temporary vectors for reading when not storing directly
    std::vector<uint8_t> temp_key(Key::size());

    // Record starting position in storage
    size_t storage_start_pos = storage.size();

    while (true)
    {
        // Read node type directly
        SHAMapNodeType current_type;
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
        if (current_type == tnTERMINAL)
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

            // Create MmapItem with pointers to our persistent storage
            // The key is at storage[key_pos] through storage[key_pos +
            // Key::size() - 1] The data is at storage[data_pos] through
            // storage[data_pos + data_size - 1]
            auto item = boost::intrusive_ptr(
                new MmapItem(&storage[key_pos], &storage[data_pos], data_size));

            // Add to map
            map.set_item(item);
            nodes_processed++;
        }
        else if (current_type == tnREMOVE)
        {
            // For removal nodes, we don't need to add to storage
            // Just read into our temporary vector
            read_node_key(temp_key);

            // Remove from map
            Key key(temp_key.data());
            map.remove_item(key);
            nodes_processed++;
        }
        else
        {
            throw CatlV1Error("Unexpected node type in map");
        }
    }

    LOGI(
        "Added ",
        nodes_processed,
        " nodes to SHAMap, storage increased by ",
        (storage.size() - storage_start_pos),
        " bytes");

    return nodes_processed;
}

/**
 * Implementation of read_and_skip_node
 */
SHAMapNodeType
Reader::read_and_skip_node()
{
    // Read the node type
    SHAMapNodeType node_type = read_node_type();

    // All node types except TERMINAL have a key
    if (node_type != tnTERMINAL)
    {
        // Skip key data
        skip_with_tee(Key::size(), "key");

        // For nodes with data, skip the data too
        if (node_type != tnREMOVE)
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
Reader::skip_map(SHAMapNodeType node_type)
{
    while (true)
    {
        // Read node type and skip the rest of the node
        SHAMapNodeType current_type = read_and_skip_node();

        // Terminal marker reached
        if (current_type == tnTERMINAL)
        {
            break;
        }

        // Verify node type matches expected type
        if (current_type != node_type && current_type != tnREMOVE)
        {
            throw CatlV1Error("Unexpected node type in map");
        }
    }
}

/**
 * Implementation of read_node_type
 */
SHAMapNodeType
Reader::read_node_type()
{
    uint8_t type_byte;
    read_bytes(&type_byte, 1, "node type");
    return static_cast<SHAMapNodeType>(type_byte);
}

/**
 * Implementation of read_node_key
 */
void
Reader::read_node_key(std::vector<uint8_t>& key_out, bool trim)
{
    if (trim)
    {
        // Resize vector to hold exactly the key data
        key_out.resize(Key::size());

        // Read key data
        read_bytes(key_out.data(), Key::size(), "key");
    }
    else
    {
        // Get position to read into
        size_t pos = key_out.size();

        // Expand vector (no reallocation thanks to ensure_capacity)
        key_out.resize(pos + Key::size());

        // Read key data directly into the expanded area
        read_bytes_into_vector(key_out, pos, Key::size(), "key");
    }
}

/**
 * Implementation of read_node_data
 */
uint32_t
Reader::read_node_data(std::vector<uint8_t>& data_out, bool trim)
{
    // Read data length
    uint32_t data_length;
    read_value(data_length, "data length");

    if (trim)
    {
        // Resize vector to hold exactly the data
        data_out.resize(data_length);

        // Read data
        if (data_length > 0)
        {
            read_bytes(data_out.data(), data_length, "data");
        }
    }
    else
    {
        // Get position to read into
        size_t pos = data_out.size();

        // Expand vector (no reallocation thanks to ensure_capacity)
        data_out.resize(pos + data_length);

        // Read data directly into the expanded area
        if (data_length > 0)
        {
            read_bytes_into_vector(data_out, pos, data_length, "data");
        }
    }

    return data_length;
}

/**
 * Implementation of read_map_node
 */
bool
Reader::read_map_node(
    SHAMapNodeType& type_out,
    std::vector<uint8_t>& key_out,
    std::vector<uint8_t>& data_out)
{
    // Read type byte
    type_out = read_node_type();

    // Check for terminal marker
    if (type_out == tnTERMINAL)
    {
        return false;
    }

    // Read key (with trim=true by default)
    read_node_key(key_out);

    // For non-remove nodes, read data
    if (type_out != tnREMOVE)
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
size_t
Reader::read_map(
    SHAMapNodeType type,
    const std::function<
        void(const std::vector<uint8_t>&, const std::vector<uint8_t>&)>&
        on_node,
    const std::function<void(const std::vector<uint8_t>&)>& on_delete)
{
    size_t nodes_processed = 0;
    std::vector<uint8_t> key_buffer(Key::size());
    std::vector<uint8_t> data_buffer;

    while (true)
    {
        // Read node type
        SHAMapNodeType current_type = read_node_type();

        // Terminal marker reached
        if (current_type == tnTERMINAL)
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

            nodes_processed++;
        }
        else if (current_type == tnREMOVE)
        {
            // Read key
            read_node_key(key_buffer);

            // Call on_delete callback if provided
            if (on_delete)
            {
                on_delete(key_buffer);
            }
            // Otherwise fall back to on_node with empty data if provided
            else if (on_node)
            {
                data_buffer.clear();
                on_node(key_buffer, data_buffer);
            }

            nodes_processed++;
        }
        else
        {
            throw CatlV1Error("Unexpected node type in map");
        }
    }

    return nodes_processed;
}

}  // namespace catl::v1
