#include "catl/hasher-v1/utils.h"
#include "catl/common/utils.h"
#include <cstdint>
#include <ctime>
#include <string>

namespace utils {
std::string
format_ripple_time(uint64_t netClockTime)
{
    return catl::common::format_ripple_time(netClockTime);
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
    return catl::common::to_unix_time(ripple_time);
}
}  // namespace utils
