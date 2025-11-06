#pragma once

#include "catl/nodestore/buffer_factory.h"
#include "catl/nodestore/compression_types.h"
#include "catl/nodestore/inner_node_format.h"
#include "catl/nodestore/lz4_codec.h"
#include "catl/nodestore/node_types.h"
#include "catl/nodestore/varint.h"
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <utility>

namespace catl::nodestore {

/**
 * Decompress a nodeobject using the appropriate codec.
 *
 * The format is: [varint: compression_type][type-specific payload]
 *
 * Supported types:
 * - 0: Uncompressed (returns pointer into input buffer)
 * - 1: LZ4 compressed
 * - 2: Compressed v1 inner node (bitmask + sparse hashes)
 * - 3: Full v1 inner node (all 16 hashes)
 *
 * @param in Pointer to compressed nodeobject data
 * @param in_size Size of compressed data
 * @param bf BufferFactory for allocating decompression buffer
 * @return pair of (decompressed_data_ptr, decompressed_size)
 * @throws std::runtime_error on decompression failure or invalid type
 */
template <buffer_factory BufferFactory>
std::pair<void const*, std::size_t>
nodeobject_decompress(void const* in, std::size_t in_size, BufferFactory&& bf)
{
    auto const* p = static_cast<std::uint8_t const*>(in);
    std::size_t type_value;

    // Read compression type varint
    auto const vn = read_varint(p, in_size, type_value);
    if (vn == 0)
        throw std::runtime_error("nodeobject_decompress: invalid varint");

    p += vn;
    in_size -= vn;

    auto const type = static_cast<compression_type>(type_value);
    std::pair<void const*, std::size_t> result;

    switch (type)
    {
        case compression_type::uncompressed: {
            // Return pointer directly into input (no copy)
            result.first = p;
            result.second = in_size;
            break;
        }

        case compression_type::lz4: {
            // LZ4 compressed - delegate to lz4_decompress
            result = lz4_decompress(p, in_size, bf);
            break;
        }

        case compression_type::inner_node_compressed: {
            // Compressed v1 inner node with bitmask
            // Decode to branches array
            std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
            if (!inner_node::decode_compressed(p, in_size, branches))
                throw std::runtime_error(
                    "nodeobject_decompress: invalid compressed inner node");

            // Reconstruct full 525-byte inner node format
            result.second = format::INNER_NODE_V1_SIZE;
            void* const out = bf(result.second);
            result.first = out;

            auto* bytes = static_cast<std::uint8_t*>(out);

            // Write header: 8 unused bytes + 1 type byte
            std::memset(bytes, 0, 8);
            bytes[8] = static_cast<std::uint8_t>(node_type::hot_unknown);

            // Write hash prefix (4 bytes)
            std::uint32_t prefix = inner_node::HASH_PREFIX_INNER_NODE;
            std::memcpy(bytes + 9, &prefix, 4);

            // Write 16 branch hashes (512 bytes)
            for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
            {
                std::memcpy(
                    bytes + 13 + (i * format::INNER_NODE_HASH_SIZE),
                    branches[i].data(),
                    format::INNER_NODE_HASH_SIZE);
            }
            break;
        }

        case compression_type::inner_node_full: {
            // Full v1 inner node (all 16 hashes)
            if (in_size != format::INNER_NODE_HASH_ARRAY_SIZE)
                throw std::runtime_error(
                    "nodeobject_decompress: invalid full inner node size");

            // Decode to branches array
            std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
            if (!inner_node::decode_full(p, in_size, branches))
                throw std::runtime_error(
                    "nodeobject_decompress: invalid full inner node");

            // Reconstruct full 525-byte inner node format
            result.second = format::INNER_NODE_V1_SIZE;
            void* const out = bf(result.second);
            result.first = out;

            auto* bytes = static_cast<std::uint8_t*>(out);

            // Write header: 8 unused bytes + 1 type byte
            std::memset(bytes, 0, 8);
            bytes[8] = static_cast<std::uint8_t>(node_type::hot_unknown);

            // Write hash prefix (4 bytes)
            std::uint32_t prefix = inner_node::HASH_PREFIX_INNER_NODE;
            std::memcpy(bytes + 9, &prefix, 4);

            // Write 16 branch hashes (512 bytes)
            for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
            {
                std::memcpy(
                    bytes + 13 + (i * format::INNER_NODE_HASH_SIZE),
                    branches[i].data(),
                    format::INNER_NODE_HASH_SIZE);
            }
            break;
        }

        default:
            throw std::runtime_error(
                "nodeobject_decompress: unknown compression type " +
                std::to_string(type_value));
    }

    return result;
}

/**
 * Compress a nodeobject using the optimal codec.
 *
 * Automatically detects v1 inner nodes and uses compressed format.
 * All other data is compressed with LZ4.
 *
 * @param in Pointer to nodeobject data
 * @param in_size Size of nodeobject data
 * @param bf BufferFactory for allocating compression buffer
 * @return pair of (compressed_data_ptr, compressed_size)
 * @throws std::runtime_error on compression failure
 */
template <buffer_factory BufferFactory>
std::pair<void const*, std::size_t>
nodeobject_compress(void const* in, std::size_t in_size, BufferFactory&& bf)
{
    // Check for v1 inner node (525 bytes with inner node prefix)
    if (in_size == format::INNER_NODE_V1_SIZE)
    {
        auto const* bytes = static_cast<std::uint8_t const*>(in);
        std::uint32_t prefix;
        std::memcpy(&prefix, bytes + 9, 4);

        if (prefix == inner_node::HASH_PREFIX_INNER_NODE)
        {
            // Extract branch hashes (skip 13-byte header)
            std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
            for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
            {
                branches[i] =
                    Hash256(bytes + 13 + (i * format::INNER_NODE_HASH_SIZE));
            }

            // Count non-zero branches
            auto const branch_count = inner_node::count_branches(branches);

            // Choose format based on sparsity
            std::pair<void const*, std::size_t> result;

            if (branch_count < format::INNER_NODE_BRANCH_COUNT)
            {
                // Type 2: Compressed (bitmask + sparse hashes)
                auto const type = compression_type::inner_node_compressed;
                auto const vn = size_varint(static_cast<std::size_t>(type));
                auto const max_size =
                    vn + 2 + (branch_count * format::INNER_NODE_HASH_SIZE);

                std::uint8_t* out = static_cast<std::uint8_t*>(bf(max_size));
                result.first = out;

                // Write type varint
                write_varint(out, static_cast<std::size_t>(type));
                out += vn;

                // Encode compressed inner node
                auto const encoded_size =
                    inner_node::encode_compressed(branches, out);
                result.second = vn + encoded_size;
                return result;
            }
            else
            {
                // Type 3: Full (all 16 hashes)
                auto const type = compression_type::inner_node_full;
                auto const vn = size_varint(static_cast<std::size_t>(type));
                auto const total_size = vn + format::INNER_NODE_HASH_ARRAY_SIZE;

                std::uint8_t* out = static_cast<std::uint8_t*>(bf(total_size));
                result.first = out;

                // Write type varint
                write_varint(out, static_cast<std::size_t>(type));
                out += vn;

                // Encode full inner node
                auto const encoded_size =
                    inner_node::encode_full(branches, out);
                result.second = vn + encoded_size;
                return result;
            }
        }
    }

    // Default: LZ4 compression
    auto const type = compression_type::lz4;
    auto const vn = size_varint(static_cast<std::size_t>(type));

    std::pair<void const*, std::size_t> result;
    std::uint8_t* p;

    auto const lzr = lz4_compress(in, in_size, [&p, &vn, &bf](std::size_t n) {
        p = static_cast<std::uint8_t*>(bf(vn + n));
        return p + vn;
    });

    // Write type varint at the beginning
    write_varint(p, static_cast<std::size_t>(type));

    result.first = p;
    result.second = vn + lzr.second;
    return result;
}

}  // namespace catl::nodestore
