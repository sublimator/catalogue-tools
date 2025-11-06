#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace catl::nodestore {

// This is a variant of the base128 varint format from
// google protocol buffers:
// https://developers.google.com/protocol-buffers/docs/encoding#varints

// Metafunction to return largest
// possible size of T represented as varint.
// T must be unsigned
template <class T, bool = std::is_unsigned<T>::value>
struct varint_traits;

template <class T>
struct varint_traits<T, true>
{
    explicit varint_traits() = default;

    static std::size_t constexpr max = (8 * sizeof(T) + 6) / 7;
};

// Returns: Number of bytes consumed or 0 on error,
//          if the buffer was too small or t overflowed.
//
template <class = void>
std::size_t
read_varint(void const* buf, std::size_t buflen, std::size_t& t)
{
    if (buflen == 0)
        return 0;
    t = 0;
    std::uint8_t const* p = reinterpret_cast<std::uint8_t const*>(buf);
    std::size_t n = 0;
    while (p[n] & 0x80)
        if (++n >= buflen)
            return 0;
    if (++n > buflen)
        return 0;
    // Special case for 0
    if (n == 1 && *p == 0)
    {
        t = 0;
        return 1;
    }
    auto const used = n;
    while (n--)
    {
        auto const d = p[n];
        auto const t0 = t;
        t *= 127;
        t += d & 0x7f;
        if (t <= t0)
            return 0;  // overflow
    }
    return used;
}

template <class T, std::enable_if_t<std::is_unsigned<T>::value>* = nullptr>
std::size_t
size_varint(T v)
{
    std::size_t n = 0;
    do
    {
        v /= 127;
        ++n;
    } while (v != 0);
    return n;
}

template <class = void>
std::size_t
write_varint(void* p0, std::size_t v)
{
    std::uint8_t* p = reinterpret_cast<std::uint8_t*>(p0);
    do
    {
        std::uint8_t d = v % 127;
        v /= 127;
        if (v != 0)
            d |= 0x80;
        *p++ = d;
    } while (v != 0);
    return p - reinterpret_cast<std::uint8_t*>(p0);
}

}  // namespace catl::nodestore
