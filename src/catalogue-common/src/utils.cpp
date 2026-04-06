#include "catl/common/utils.h"

#include <chrono>
#include <cstdint>
#include <ctime>
#include <string>

namespace catl::common {

namespace {

static constexpr uint32_t RIPPLE_EPOCH = 946684800;

}  // namespace

std::string
format_ripple_time(uint64_t net_clock_time)
{
    time_t unix_time = net_clock_time + RIPPLE_EPOCH;
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
    return ripple_time + RIPPLE_EPOCH;
}

uint32_t
to_ripple_time(const uint32_t unix_time)
{
    if (unix_time <= RIPPLE_EPOCH)
        return 0;
    return unix_time - RIPPLE_EPOCH;
}

uint32_t
current_ripple_time()
{
    auto const unix_now = std::chrono::duration_cast<std::chrono::seconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
    return to_ripple_time(static_cast<uint32_t>(unix_now));
}

}  // namespace catl::common
