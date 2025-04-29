#include "catl/common/utils.h"
#include <cstdint>
#include <ctime>
#include <string>

namespace catl::common {

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

uint32_t
to_unix_time(const uint32_t ripple_time)
{
    // The Ripple Epoch is January 1st, 2000 (946684800 Unix time)
    static constexpr uint32_t RIPPLE_EPOCH = 946684800;
    return ripple_time + RIPPLE_EPOCH;
}

}  // namespace catl::common