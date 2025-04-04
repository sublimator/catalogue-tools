#pragma once

#include <cstdint>
#include <string>

/**
 * General utility functions for CATL processing
 */
namespace utils {

/**
 * Convert a Ripple network time value into a human-readable string
 * @param netClockTime Time in Ripple epoch (seconds since January 1st, 2000
 * (00:00 UTC))
 * @return Formatted time string in UTC
 */
std::string
format_ripple_time(uint64_t netClockTime);

// Add more utility functions here as needed

}  // namespace utils