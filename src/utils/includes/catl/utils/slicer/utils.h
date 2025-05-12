#pragma once

#include <cstdint>
#include <string>

namespace catl::utils::slicer {

/**
 * Format a file size in human-readable format (B, KB, MB, GB, TB, PB)
 *
 * @param bytes The file size in bytes
 * @return Formatted string with appropriate units
 */
std::string
format_file_size(uint64_t bytes);

}  // namespace catl::utils::slicer