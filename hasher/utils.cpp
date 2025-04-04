#include "hasher/utils.h"
#include <ctime>

namespace utils {
std::string
format_ripple_time(uint64_t netClockTime)
{
    static constexpr time_t rippleEpochOffset = 946684800;
    time_t unixTime = netClockTime + rippleEpochOffset;
    const std::tm* tm = std::gmtime(&unixTime);
    if (!tm)
        return "Invalid time";
    char timeStr[30];
    std::strftime(timeStr, sizeof(timeStr), "%Y-%m-%d %H:%M:%S UTC", tm);
    return timeStr;
}

/**
 * Convert a Ripple timestamp to Unix timestamp
 * Ripple time starts at January 1st, 2000 (946684800)
 *
 * @param ripple_time Ripple timestamp
 * @return Unix timestamp (seconds since January 1st, 1970)
 */
uint32_t
to_unix_time(const uint32_t ripple_time)
{
    // The Ripple Epoch is January 1st, 2000 (946684800 Unix time)
    static constexpr uint32_t RIPPLE_EPOCH = 946684800;
    return ripple_time + RIPPLE_EPOCH;
}
}  // namespace utils
