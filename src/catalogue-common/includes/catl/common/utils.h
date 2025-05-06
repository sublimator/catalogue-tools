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

}  // namespace catl::common