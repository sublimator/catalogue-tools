#pragma once

#include "catl/v1/catl-v1-simple-state-map.h"
#include <boost/filesystem.hpp>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace catl::utils::slicer {

namespace fs = boost::filesystem;

/**
 * Exception class for snapshot operations
 */
class SnapshotError : public std::runtime_error
{
public:
    explicit SnapshotError(const std::string& message)
        : std::runtime_error(message)
    {
    }

    // Constructor that wraps another exception
    SnapshotError(const std::string& message, const std::exception& cause)
        : std::runtime_error(message + ": " + cause.what())
    {
    }
};

/**
 * Format a file size in human-readable format (B, KB, MB, GB, TB, PB)
 *
 * @param bytes The file size in bytes
 * @return Formatted string with appropriate units
 */
std::string
format_file_size(uint64_t bytes);

/**
 * Create a state snapshot file from a SimpleStateMap
 *
 * This function creates a state snapshot file in the format described in the
 * catl-slice specification. The file contains a zlib-compressed stream of
 * serialized state map entries.
 *
 * @param state_map The SimpleStateMap to serialize
 * @param snapshot_path Path where the snapshot file will be created
 * @param compression_level Zlib compression level (0-9)
 * @param force_overwrite Whether to overwrite existing file without prompting
 * @throws SnapshotError for any errors during snapshot creation
 */
void
create_state_snapshot(
    const catl::v1::SimpleStateMap& state_map,
    const fs::path& snapshot_path,
    uint8_t compression_level = 9,
    bool force_overwrite = false);

/**
 * Copy a decompressed snapshot directly to an output stream
 *
 * This function is optimized for the slice operation - it decompresses
 * a snapshot file and writes it directly to the provided output stream
 * without building an in-memory state map.
 *
 * @param snapshot_path Path to the snapshot file
 * @param output_stream The output stream to write the decompressed data to
 * @throws SnapshotError for any errors during snapshot reading or copying
 * @return The number of bytes copied
 */
size_t
copy_snapshot_to_stream(
    const fs::path& snapshot_path,
    std::ostream& output_stream);

}  // namespace catl::utils::slicer