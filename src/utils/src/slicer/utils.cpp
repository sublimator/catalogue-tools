#include "catl/utils/slicer/utils.h"
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <fstream>
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

void
create_state_snapshot(
    const catl::v1::SimpleStateMap& state_map,
    const fs::path& snapshot_path,
    uint8_t compression_level,
    bool force_overwrite)
{
    try
    {
        // Check if file exists and handle overwrite policy
        if (fs::exists(snapshot_path) && !force_overwrite)
        {
            throw SnapshotError(
                "Snapshot file already exists: " + snapshot_path.string() +
                ". Use force_overwrite to replace it.");
        }

        // Create parent directory if it doesn't exist
        if (!fs::exists(snapshot_path.parent_path()))
        {
            fs::create_directories(snapshot_path.parent_path());
        }

        // Open output file
        std::ofstream file(
            snapshot_path.string(), std::ios::binary | std::ios::trunc);
        if (!file)
        {
            throw SnapshotError(
                "Failed to open snapshot file for writing: " +
                snapshot_path.string());
        }

        // Setup zlib compression
        boost::iostreams::filtering_ostream compressed_out;
        boost::iostreams::zlib_params zlib_params;
        zlib_params.level = compression_level;
        zlib_params.noheader = false;
        zlib_params.window_bits = 15;

        try
        {
            // Add zlib compression filter
            compressed_out.push(boost::iostreams::zlib_compressor(zlib_params));
            compressed_out.push(file);
        }
        catch (const std::exception& e)
        {
            throw SnapshotError("Failed to initialize compression", e);
        }

        // Serialize the state map to the compressed stream
        try
        {
            size_t bytes_written =
                catl::v1::write_map_to_stream(state_map, compressed_out);

            // Make sure to flush the stream
            compressed_out.flush();

            // Clean up and close streams
            compressed_out.reset();
            file.close();
        }
        catch (const std::exception& e)
        {
            throw SnapshotError("Failed to write state map to snapshot", e);
        }
    }
    catch (const SnapshotError&)
    {
        // Re-throw SnapshotErrors as-is
        throw;
    }
    catch (const std::exception& e)
    {
        // Wrap other exceptions
        throw SnapshotError("Unexpected error creating snapshot", e);
    }
}

size_t
copy_snapshot_to_stream(
    const fs::path& snapshot_path,
    std::ostream& output_stream)
{
    try
    {
        // Check if snapshot file exists
        if (!fs::exists(snapshot_path))
        {
            throw SnapshotError(
                "Snapshot file does not exist: " + snapshot_path.string());
        }

        // Open the snapshot file
        std::ifstream file(snapshot_path.string(), std::ios::binary);
        if (!file)
        {
            throw SnapshotError(
                "Failed to open snapshot file for reading: " +
                snapshot_path.string());
        }

        // Setup zlib decompression
        boost::iostreams::filtering_istream decompressed_in;
        boost::iostreams::zlib_params zlib_params;
        zlib_params.noheader = false;
        zlib_params.window_bits = 15;

        try
        {
            // Add zlib decompression filter
            decompressed_in.push(
                boost::iostreams::zlib_decompressor(zlib_params));
            decompressed_in.push(file);
        }
        catch (const std::exception& e)
        {
            throw SnapshotError("Failed to initialize decompression", e);
        }

        // Copy from decompressed stream to output stream
        size_t bytes_copied = 0;
        try
        {
            bytes_copied =
                boost::iostreams::copy(decompressed_in, output_stream);

            // Clean up and close streams
            decompressed_in.reset();
            file.close();
        }
        catch (const std::exception& e)
        {
            throw SnapshotError(
                "Failed to copy snapshot data to output stream", e);
        }

        return bytes_copied;
    }
    catch (const SnapshotError&)
    {
        // Re-throw SnapshotErrors as-is
        throw;
    }
    catch (const std::exception& e)
    {
        // Wrap other exceptions
        throw SnapshotError("Unexpected error copying snapshot", e);
    }
}

}  // namespace catl::utils::slicer