#include "catl/common/utils.h"
#include <cstdint>
#include <ctime>
#include <string>

namespace catl::common {

std::string
format_ripple_time(uint64_t net_clock_time)
{
    static constexpr time_t ripple_epoch_offset = 946684800;
    time_t unix_time = net_clock_time + ripple_epoch_offset;
    const std::tm* tm = std::gmtime(&unix_time);
    if (!tm)
        return "Invalid time";
    char time_str[30];
    std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S UTC", tm);
    return time_str;
}

uint32_t
to_unix_time(const uint32_t ripple_time)
{
    // The Ripple Epoch is January 1st, 2000 (946684800 Unix time)
    static constexpr uint32_t RIPPLE_EPOCH = 946684800;
    return ripple_time + RIPPLE_EPOCH;
}

}  // namespace catl::common