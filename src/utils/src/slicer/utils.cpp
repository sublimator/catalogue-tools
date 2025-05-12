#include "catl/utils/slicer/utils.h"
#include <iomanip>
#include <sstream>

namespace catl::utils::slicer {

std::string
format_file_size(uint64_t bytes)
{
    static const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB"};
    int unit_index = 0;
    auto size = static_cast<double>(bytes);

    while (size >= 1024.0 && unit_index < 5)
    {
        size /= 1024.0;
        unit_index++;
    }

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << size << " "
        << units[unit_index];
    return oss.str();
}

}  // namespace catl::utils::slicer