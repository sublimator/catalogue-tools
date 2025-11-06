#pragma once

#include "catl/nodestore/varint.h"
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <lz4.h>
#include <stdexcept>
#include <utility>

namespace catl::nodestore {

/**
 * Decompress LZ4-compressed data using a buffer factory.
 *
 * Format: [varint: decompressed_size][lz4_compressed_data]
 *
 * @param in Pointer to compressed data
 * @param in_size Size of compressed data
 * @param bf BufferFactory callable: void* bf(size_t needed_size)
 * @return pair of (decompressed_data_ptr, decompressed_size)
 * @throws std::runtime_error on decompression failure
 */
template <class BufferFactory>
std::pair<void const*, std::size_t>
lz4_decompress(void const* in, std::size_t in_size, BufferFactory&& bf)
{
    if (static_cast<int>(in_size) < 0)
        throw std::runtime_error("lz4_decompress: integer overflow (input)");

    std::size_t out_size = 0;

    // Read the decompressed size varint
    auto const n = read_varint(
        reinterpret_cast<std::uint8_t const*>(in), in_size, out_size);

    if (n == 0 || n >= in_size)
        throw std::runtime_error("lz4_decompress: invalid blob");

    if (static_cast<int>(out_size) <= 0)
        throw std::runtime_error("lz4_decompress: integer overflow (output)");

    // Allocate output buffer via factory
    void* const out = bf(out_size);

    // Decompress
    if (LZ4_decompress_safe(
            reinterpret_cast<char const*>(in) + n,
            reinterpret_cast<char*>(out),
            static_cast<int>(in_size - n),
            static_cast<int>(out_size)) != static_cast<int>(out_size))
        throw std::runtime_error("lz4_decompress: LZ4_decompress_safe");

    return {out, out_size};
}

/**
 * Compress data using LZ4 with a buffer factory.
 *
 * Format: [varint: original_size][lz4_compressed_data]
 *
 * @param in Pointer to data to compress
 * @param in_size Size of data to compress
 * @param bf BufferFactory callable: void* bf(size_t needed_size)
 * @return pair of (compressed_data_ptr, compressed_size)
 * @throws std::runtime_error on compression failure
 */
template <class BufferFactory>
std::pair<void const*, std::size_t>
lz4_compress(void const* in, std::size_t in_size, BufferFactory&& bf)
{
    std::pair<void const*, std::size_t> result;

    // Write the original size as varint
    std::array<std::uint8_t, varint_traits<std::size_t>::max> vi;
    auto const n = write_varint(vi.data(), in_size);

    // Calculate max compressed size
    auto const out_max = LZ4_compressBound(in_size);

    // Allocate buffer for varint + compressed data
    std::uint8_t* out = reinterpret_cast<std::uint8_t*>(bf(n + out_max));
    result.first = out;

    // Copy varint to output
    std::memcpy(out, vi.data(), n);

    // Compress
    auto const out_size = LZ4_compress_default(
        reinterpret_cast<char const*>(in),
        reinterpret_cast<char*>(out + n),
        in_size,
        out_max);

    if (out_size == 0)
        throw std::runtime_error("lz4_compress");

    result.second = n + out_size;
    return result;
}

}  // namespace catl::nodestore
