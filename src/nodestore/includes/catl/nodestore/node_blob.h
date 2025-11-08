#pragma once

#include "catl/nodestore/buffer_factory.h"
#include "catl/nodestore/compression_types.h"
#include "catl/nodestore/inner_node_format.h"
#include "catl/nodestore/inner_node_source.h"
#include "catl/nodestore/lz4_codec.h"
#include "catl/nodestore/node_types.h"
#include "catl/nodestore/varint.h"
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <lz4.h>
#include <span>
#include <stdexcept>
#include <vector>

namespace catl::nodestore {

/**
 * A node blob - universal container for node data.
 *
 * Can hold either:
 * - Compressed data (from compress, for storage)
 * - Decompressed data (from decompress, for use)
 *
 * Both formats include the 9-byte header:
 * [8 unused bytes][1 type byte][payload...]
 *
 * NOTE: Hash is NOT stored here! Hash is the KEY used to fetch/store.
 * Storing it would waste 32 bytes when the caller already knows it.
 *
 * NOTE: Only hot_* types (0-255) are ever serialized. Pinned types are
 * runtime-only and get downgraded to their hot equivalents before storage.
 */
struct node_blob
{
    std::vector<std::uint8_t>
        data;  // OWNS the bytes (compressed OR decompressed)

    /**
     * Extract node_type from byte 8 of the header.
     */
    node_type
    get_type() const
    {
        if (data.size() < 9)
            throw std::runtime_error("node_blob: buffer too small");
        return static_cast<node_type>(data[8]);
    }

    /**
     * Get payload span (after 9-byte header).
     */
    std::span<std::uint8_t const>
    payload() const
    {
        if (data.size() < 9)
            return {};
        return std::span<std::uint8_t const>(data.data() + 9, data.size() - 9);
    }

    /**
     * Get mutable payload span (for in-place modification).
     */
    std::span<std::uint8_t>
    payload_mut()
    {
        if (data.size() < 9)
            return {};
        return std::span<std::uint8_t>(data.data() + 9, data.size() - 9);
    }
};

/**
 * Compress an inner node directly to storage format.
 *
 * Uses the inner_node_source concept to access node data directly,
 * avoiding wasteful serialize→parse→re-encode cycle.
 *
 * @param node Inner node satisfying inner_node_source concept
 * @return node_blob with compressed data, ready for storage
 */
node_blob
nodeobject_compress(inner_node_source auto const& node)
{
    node_type const type = node_type::hot_unknown;  // Inner nodes
    std::uint16_t const mask = node.get_node_source_branch_mask();
    auto const branch_count =
        static_cast<std::size_t>(__builtin_popcount(mask));

    // Choose compression type based on sparsity
    auto const comp_type = (branch_count < format::INNER_NODE_BRANCH_COUNT)
        ? compression_type::inner_node_compressed
        : compression_type::inner_node_full;

    auto const vn = size_varint(static_cast<std::size_t>(comp_type));

    // Calculate FINAL size including 9-byte header
    std::size_t total_size;
    if (comp_type == compression_type::inner_node_compressed)
    {
        total_size = 9 + vn + 2 + (branch_count * format::INNER_NODE_HASH_SIZE);
    }
    else
    {
        total_size = 9 + vn + format::INNER_NODE_HASH_ARRAY_SIZE;
    }

    // ONE allocation for everything
    node_blob blob;
    blob.data.resize(total_size);
    std::uint8_t* out = blob.data.data();

    // Write 9-byte header
    std::memset(out, 0, 8);
    out[8] = static_cast<std::uint8_t>(type);
    out += 9;

    // Write compression type varint
    write_varint(out, static_cast<std::size_t>(comp_type));
    out += vn;

    // Write compressed format
    if (comp_type == compression_type::inner_node_compressed)
    {
        // Type 2: Write bitmask (big-endian) + sparse hashes
        out[0] = static_cast<std::uint8_t>(mask >> 8);
        out[1] = static_cast<std::uint8_t>(mask & 0xFF);
        out += 2;

        // NOTE: mask is already in rippled/xahaud canonical format from
        // get_node_source_branch_mask() Canonical format: branch i = bit (15 -
        // i) Write hashes in branch order (0, 1, 2, ...) but check canonical
        // bit (15-i)
        for (int i = 0; i < 16; ++i)
        {
            if (mask & (1 << (15 - i)))  // Check canonical bit for branch i
            {
                auto const& hash = node.get_node_source_child_hash(i);
                std::memcpy(out, hash.data(), format::INNER_NODE_HASH_SIZE);
                out += format::INNER_NODE_HASH_SIZE;
            }
        }
    }
    else
    {
        // Type 3: Write all 16 hashes
        for (int i = 0; i < 16; ++i)
        {
            auto const& hash = node.get_node_source_child_hash(i);
            std::memcpy(out, hash.data(), format::INNER_NODE_HASH_SIZE);
            out += format::INNER_NODE_HASH_SIZE;
        }
    }

    return blob;
}

/**
 * Compress raw payload data to storage format.
 *
 * Uses LZ4 compression for the payload.
 *
 * @param type The node type
 * @param payload The raw data to compress
 * @return node_blob with compressed data, ready for storage
 */
inline node_blob
nodeobject_compress(node_type type, std::span<std::uint8_t const> payload)
{
    auto const comp_type = compression_type::lz4;
    auto const vn = size_varint(static_cast<std::size_t>(comp_type));
    auto const lz4_vn = size_varint(payload.size());
    auto const lz4_bound = LZ4_compressBound(static_cast<int>(payload.size()));

    // Calculate maximum total size including 9-byte header
    auto const total_max = 9 + vn + lz4_vn + lz4_bound;

    // ONE allocation for everything
    node_blob blob;
    blob.data.resize(total_max);
    std::uint8_t* out = blob.data.data();

    // Write 9-byte header
    std::memset(out, 0, 8);
    out[8] = static_cast<std::uint8_t>(type);
    out += 9;

    // Write compression type varint
    write_varint(out, static_cast<std::size_t>(comp_type));
    out += vn;

    // Write LZ4 original size varint
    write_varint(out, payload.size());
    out += lz4_vn;

    // LZ4 compress DIRECTLY to offset buffer
    auto const compressed_size = LZ4_compress_default(
        reinterpret_cast<char const*>(payload.data()),
        reinterpret_cast<char*>(out),
        static_cast<int>(payload.size()),
        static_cast<int>(lz4_bound));

    if (compressed_size == 0)
        throw std::runtime_error(
            "nodeobject_compress: LZ4_compress_default failed");

    // Resize to actual size (not maximum)
    blob.data.resize(9 + vn + lz4_vn + compressed_size);
    return blob;
}

/**
 * Decompress a compressed node blob.
 *
 * @param compressed_blob The compressed data (with 9-byte header)
 * @return node_blob with decompressed data (still has 9-byte header)
 */
inline node_blob
nodeobject_decompress(node_blob const& compressed_blob)
{
    auto const* p = compressed_blob.data.data();
    auto in_size = compressed_blob.data.size();

    // Check minimum size for header
    if (in_size < 9)
        throw std::runtime_error("nodeobject_decompress: buffer too small");

    // Extract node_type from byte 8
    node_type const type = static_cast<node_type>(p[8]);

    // Skip 9-byte header
    p += 9;
    in_size -= 9;

    // Read compression type varint
    std::size_t comp_type_value;
    auto const vn = read_varint(p, in_size, comp_type_value);
    if (vn == 0)
        throw std::runtime_error("nodeobject_decompress: invalid varint");

    p += vn;
    in_size -= vn;

    auto const comp_type = static_cast<compression_type>(comp_type_value);

    // Decompress payload (will reconstruct with 9-byte header)
    std::vector<std::uint8_t> payload_only;

    switch (comp_type)
    {
        case compression_type::uncompressed:
            payload_only.assign(p, p + in_size);
            break;

        case compression_type::lz4: {
            // Use factory internally (modifies payload_only directly)
            lz4_decompress(p, in_size, make_vector_factory(payload_only));
            break;
        }

        case compression_type::inner_node_compressed: {
            // Decode compressed inner node (bitmask + sparse hashes)
            std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
            if (!inner_node::decode_compressed(p, in_size, branches))
                throw std::runtime_error(
                    "nodeobject_decompress: invalid compressed inner node");

            // Reconstruct just the 16 hashes (512 bytes)
            payload_only.resize(format::INNER_NODE_HASH_ARRAY_SIZE);
            for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
            {
                std::memcpy(
                    payload_only.data() + (i * format::INNER_NODE_HASH_SIZE),
                    branches[i].data(),
                    format::INNER_NODE_HASH_SIZE);
            }
            break;
        }

        case compression_type::inner_node_full: {
            // Decode full inner node (all 16 hashes)
            std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
            if (!inner_node::decode_full(p, in_size, branches))
                throw std::runtime_error(
                    "nodeobject_decompress: invalid full inner node");

            // Reconstruct just the 16 hashes (512 bytes)
            payload_only.resize(format::INNER_NODE_HASH_ARRAY_SIZE);
            for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
            {
                std::memcpy(
                    payload_only.data() + (i * format::INNER_NODE_HASH_SIZE),
                    branches[i].data(),
                    format::INNER_NODE_HASH_SIZE);
            }
            break;
        }

        default:
            throw std::runtime_error(
                "nodeobject_decompress: unknown compression type " +
                std::to_string(comp_type_value));
    }

    // Reconstruct node_blob with 9-byte header + decompressed payload
    node_blob decompressed;
    decompressed.data.resize(9 + payload_only.size());

    // Write 9-byte header
    std::memset(decompressed.data.data(), 0, 8);
    decompressed.data[8] = static_cast<std::uint8_t>(type);

    // Copy payload
    std::memcpy(
        decompressed.data.data() + 9, payload_only.data(), payload_only.size());

    return decompressed;
}

}  // namespace catl::nodestore
