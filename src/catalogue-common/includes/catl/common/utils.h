#pragma once

#include <cstdint>
#include <string>

namespace catl::common {

/**
 * Convert a Ripple network time value into a human-readable string
 * @param net_clock_time Time in Ripple epoch (seconds since January 1st, 2000
 * (00:00 UTC))
 * @return Formatted time string in UTC
 */
std::string
format_ripple_time(uint64_t net_clock_time);

/**
 * Convert Ripple network time to Unix timestamp
 * @param netClockTime Time in Ripple epoch
 * @return Unix timestamp
 */
uint32_t
to_unix_time(uint32_t netClockTime);

/**
 * Write uint32_t to buffer in big-endian format (platform-independent)
 * @param buffer Output buffer (must have at least 4 bytes available)
 * @param value Value to write
 */
inline void
put_uint32_be(uint8_t* buffer, uint32_t value)
{
    buffer[0] = static_cast<uint8_t>((value >> 24) & 0xFF);
    buffer[1] = static_cast<uint8_t>((value >> 16) & 0xFF);
    buffer[2] = static_cast<uint8_t>((value >> 8) & 0xFF);
    buffer[3] = static_cast<uint8_t>(value & 0xFF);
}

/**
 * Write uint64_t to buffer in big-endian format (platform-independent)
 * @param buffer Output buffer (must have at least 8 bytes available)
 * @param value Value to write
 */
inline void
put_uint64_be(uint8_t* buffer, uint64_t value)
{
    buffer[0] = static_cast<uint8_t>((value >> 56) & 0xFF);
    buffer[1] = static_cast<uint8_t>((value >> 48) & 0xFF);
    buffer[2] = static_cast<uint8_t>((value >> 40) & 0xFF);
    buffer[3] = static_cast<uint8_t>((value >> 32) & 0xFF);
    buffer[4] = static_cast<uint8_t>((value >> 24) & 0xFF);
    buffer[5] = static_cast<uint8_t>((value >> 16) & 0xFF);
    buffer[6] = static_cast<uint8_t>((value >> 8) & 0xFF);
    buffer[7] = static_cast<uint8_t>(value & 0xFF);
}

/**
 * Read uint32_t from buffer in big-endian format (platform-independent)
 * @param buffer Input buffer (must have at least 4 bytes available)
 * @return The uint32_t value
 */
inline uint32_t
get_uint32_be(const uint8_t* buffer)
{
    return (static_cast<uint32_t>(buffer[0]) << 24) |
        (static_cast<uint32_t>(buffer[1]) << 16) |
        (static_cast<uint32_t>(buffer[2]) << 8) |
        static_cast<uint32_t>(buffer[3]);
}

/**
 * Read uint64_t from buffer in big-endian format (platform-independent)
 * @param buffer Input buffer (must have at least 8 bytes available)
 * @return The uint64_t value
 */
inline uint64_t
get_uint64_be(const uint8_t* buffer)
{
    return (static_cast<uint64_t>(buffer[0]) << 56) |
        (static_cast<uint64_t>(buffer[1]) << 48) |
        (static_cast<uint64_t>(buffer[2]) << 40) |
        (static_cast<uint64_t>(buffer[3]) << 32) |
        (static_cast<uint64_t>(buffer[4]) << 24) |
        (static_cast<uint64_t>(buffer[5]) << 16) |
        (static_cast<uint64_t>(buffer[6]) << 8) |
        static_cast<uint64_t>(buffer[7]);
}

}  // namespace catl::common