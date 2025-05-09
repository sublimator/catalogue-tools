#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/v1/catl-v1-errors.h"
#include "catl/v1/catl-v1-reader.h"

namespace {  // Anonymous namespace for utility functions

/**
 * Ensure the vector has enough capacity for additional data without
 * reallocation
 *
 * @param vec Vector to check and potentially resize
 * @param additional_size Number of additional bytes needed
 * @param growth_factor How much extra to allocate beyond what's needed
 * (default: 2x)
 */
void
ensure_capacity(
    std::vector<uint8_t>& vec,
    size_t additional_size,
    float growth_factor = 2.0f)
{
    if (vec.size() + additional_size > vec.capacity())
    {
        // Need to grow the vector - reserve more than we need to reduce future
        // reallocations
        size_t new_capacity = vec.size() + additional_size * growth_factor;
        // If we already have a large vector, be more conservative with growth
        if (vec.capacity() > 1024 * 1024)
        {
            auto comp = static_cast<size_t>(vec.capacity() * growth_factor);
            new_capacity = std::max(new_capacity, comp);
        }
        else
        {
            new_capacity = std::max(new_capacity, vec.capacity() * 2);
        }
        vec.reserve(new_capacity);
    }
}

}  // Anonymous namespace

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

    // Pre-reserve some space to reduce reallocations
    ensure_capacity(storage, 1024 * 1024);  // 1MB initial reservation

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
 * Efficient implementation of skip_map that minimizes allocations
 */
void
Reader::skip_map(SHAMapNodeType node_type)
{
    while (true)
    {
        // Read node type without vector allocations
        SHAMapNodeType current_type = read_node_type();

        // Terminal marker reached
        if (current_type == tnTERMINAL)
        {
            break;
        }

        // Skip the appropriate number of bytes based on node type
        if (current_type == node_type || current_type == tnREMOVE)
        {
            // Skip key (always present for both node types)
            input_stream_->seekg(Key::size(), std::ios::cur);

            // For state/transaction nodes, also skip data length and data
            if (current_type != tnREMOVE)
            {
                uint32_t data_length;
                if (read_raw_data(
                        reinterpret_cast<uint8_t*>(&data_length),
                        sizeof(data_length)) != sizeof(data_length))
                {
                    throw CatlV1Error(
                        "Unexpected EOF while reading data length");
                }

                // Skip data bytes
                input_stream_->seekg(data_length, std::ios::cur);
            }
        }
        else
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
 * Implementation of skip_node
 */
void
Reader::skip_node(SHAMapNodeType expected_type)
{
    // Read and verify type
    SHAMapNodeType actual_type = read_node_type();
    if (actual_type != expected_type)
    {
        throw CatlV1Error("Node type mismatch while skipping");
    }

    // All node types except TERMINAL have a key
    if (actual_type != tnTERMINAL)
    {
        // Skip key data
        input_stream_->seekg(Key::size(), std::ios::cur);

        // For items with data, skip the data too
        if (actual_type != tnREMOVE)
        {
            // Read data length
            uint32_t data_length;
            if (read_raw_data(
                    reinterpret_cast<uint8_t*>(&data_length),
                    sizeof(data_length)) != sizeof(data_length))
            {
                throw CatlV1Error("Unexpected EOF while reading data length");
            }

            // Skip data
            input_stream_->seekg(data_length, std::ios::cur);
        }
    }
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
        // Ensure there's enough space without reallocation
        ensure_capacity(key_out, Key::size());

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
        // Ensure there's enough space without reallocation
        ensure_capacity(data_out, data_length);

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
    uint8_t type_byte;
    if (read_raw_data(&type_byte, 1) != 1)
    {
        throw CatlV1Error("Unexpected EOF while reading node type");
    }

    type_out = static_cast<SHAMapNodeType>(type_byte);

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
 * Implementation of copy_map_to_stream optimized for slicing
 *
 * This implementation uses efficient buffer copy operations rather than
 * allocating vectors for node data when possible.
 */
size_t
Reader::copy_map_to_stream(
    std::ostream& output,
    std::function<void(
        SHAMapNodeType,
        const std::vector<uint8_t>&,
        const std::vector<uint8_t>&)> process_nodes)
{
    size_t bytes_copied = 0;
    std::vector<uint8_t> key_data;
    std::vector<uint8_t> item_data;

    // Buffer for direct copying
    std::vector<uint8_t> buffer(1024);  // Initial size, will grow if needed

    while (true)
    {
        // Mark current position
        std::streampos start_pos = input_stream_->tellg();

        if (start_pos == std::streampos(-1))
        {
            throw CatlV1Error("Cannot determine current stream position");
        }

        // Read node type
        uint8_t type_byte;
        if (read_raw_data(&type_byte, 1) != 1)
        {
            throw CatlV1Error("Unexpected EOF while copying map");
        }

        SHAMapNodeType current_type = static_cast<SHAMapNodeType>(type_byte);

        // Terminal marker handling
        if (current_type == tnTERMINAL)
        {
            // Write terminal marker
            output.write(reinterpret_cast<const char*>(&type_byte), 1);
            bytes_copied += 1;
            break;
        }

        // Determine size of the entire node
        size_t node_size = 1;  // Type byte

        // All non-terminal nodes have a key
        node_size += Key::size();

        // Data part only for regular nodes (not removal)
        uint32_t data_length = 0;
        if (current_type != tnREMOVE)
        {
            // Peek at data length without permanently advancing
            std::streampos before_length = input_stream_->tellg();
            if (read_raw_data(
                    reinterpret_cast<uint8_t*>(&data_length),
                    sizeof(data_length)) != sizeof(data_length))
            {
                throw CatlV1Error("Unexpected EOF while reading data length");
            }
            // Go back to position before data length
            input_stream_->seekg(before_length);

            // Add size of length field + actual data
            node_size += sizeof(data_length) + data_length;
        }

        // If callback is provided, we need to actually parse the node
        if (process_nodes)
        {
            // Read the key
            input_stream_->seekg(
                start_pos + std::streampos(1));  // Skip type byte
            key_data.resize(Key::size());
            if (read_raw_data(key_data.data(), Key::size()) != Key::size())
            {
                throw CatlV1Error("Unexpected EOF while reading key");
            }

            // For non-removal nodes, read the data too
            if (current_type != tnREMOVE)
            {
                // Read length field
                if (read_raw_data(
                        reinterpret_cast<uint8_t*>(&data_length),
                        sizeof(data_length)) != sizeof(data_length))
                {
                    throw CatlV1Error(
                        "Unexpected EOF while reading data length");
                }

                // Read data
                item_data.resize(data_length);
                if (read_raw_data(item_data.data(), data_length) != data_length)
                {
                    throw CatlV1Error("Unexpected EOF while reading data");
                }
            }
            else
            {
                item_data.clear();
            }

            // Call the callback
            process_nodes(current_type, key_data, item_data);
        }

        // Reset position to beginning of node
        input_stream_->seekg(start_pos);

        // Ensure buffer is large enough
        if (buffer.size() < node_size)
        {
            buffer.resize(node_size);
        }

        // Read the entire node into buffer
        if (read_raw_data(buffer.data(), node_size) != node_size)
        {
            throw CatlV1Error("Failed to read node data");
        }

        // Write to output
        output.write(reinterpret_cast<const char*>(buffer.data()), node_size);
        bytes_copied += node_size;
    }

    return bytes_copied;
}

}  // namespace catl::v1
