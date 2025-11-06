#pragma once

#include <concepts>
#include <cstddef>
#include <vector>

namespace catl::nodestore {

/**
 * Concept defining the requirements for a BufferFactory.
 *
 * A BufferFactory is a callable that allocates a buffer of requested size
 * and returns a pointer to it. The factory is responsible for managing the
 * buffer's lifetime.
 *
 * Usage pattern:
 *   - Codec calls: void* ptr = factory(needed_size)
 *   - Factory allocates buffer (e.g., vector.resize(), arena allocator, etc.)
 *   - Factory returns pointer to allocated memory
 *   - Codec writes decompressed data to the buffer
 *   - Factory maintains ownership of buffer
 *
 * Example implementations:
 *
 * 1. std::vector-based (most common):
 *    ```cpp
 *    std::vector<uint8_t> buffer;
 *    auto factory = [&buffer](size_t size) {
 *        buffer.resize(size);
 *        return buffer.data();
 *    };
 *    auto [data, size] = lz4_decompress(in, in_size, factory);
 *    // buffer now contains decompressed data
 *    ```
 *
 * 2. Pre-allocated buffer:
 *    ```cpp
 *    std::array<uint8_t, 1024> buffer;
 *    auto factory = [&buffer](size_t size) {
 *        if (size > buffer.size()) throw std::bad_alloc();
 *        return buffer.data();
 *    };
 *    ```
 *
 * 3. Arena allocator:
 *    ```cpp
 *    Arena arena;
 *    auto factory = [&arena](size_t size) {
 *        return arena.allocate(size);
 *    };
 *    ```
 */
template <typename T>
concept buffer_factory = requires(T factory, std::size_t size)
{
    /**
     * The factory must be callable with a size_t parameter
     * and return a void* (or convertible to void*)
     */
    {
        factory(size)
    }
    ->std::convertible_to<void*>;
};

/**
 * Helper: Create a vector-based buffer factory.
 *
 * This is the most common pattern - returns a lambda that resizes
 * a vector and returns its data pointer.
 *
 * Example:
 * ```cpp
 * std::vector<uint8_t> buffer;
 * auto [data, size] = lz4_decompress(in, in_size,
 *     make_vector_factory(buffer));
 * ```
 */
template <typename T>
inline auto
make_vector_factory(std::vector<T>& vec)
{
    return [&vec](std::size_t size) {
        vec.resize(size);
        return vec.data();
    };
}

}  // namespace catl::nodestore
