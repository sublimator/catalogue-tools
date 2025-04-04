#include "utils.h"
#include <ctime>

namespace utils {
    std::string format_ripple_time(uint64_t netClockTime) {
        static constexpr time_t rippleEpochOffset = 946684800;
        time_t unixTime = netClockTime + rippleEpochOffset;
        std::tm *tm = std::gmtime(&unixTime);
        if (!tm) return "Invalid time";
        char timeStr[30];
        std::strftime(timeStr, sizeof(timeStr), "%Y-%m-%d %H:%M:%S UTC", tm);
        return timeStr;
    }

    // Add more utility function implementations here
} // namespace utils
