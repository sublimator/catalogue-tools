//
// Index Builder - Helper for creating and extending .index files
//

#ifndef NUDB_UTIL_INDEX_BUILDER_HPP
#define NUDB_UTIL_INDEX_BUILDER_HPP

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <cstdint>
#include <functional>
#include <nudbview/detail/format.hpp>
#include <nudbview/native_file.hpp>
#include <nudbview/view/index_format.hpp>
#include <vector>

namespace nudbutil {

/**
 * Options for building an index
 */
struct IndexBuildOptions
{
    std::uint64_t index_interval = 10000;
    bool show_progress = false;
    std::function<void(std::uint64_t, std::uint64_t)> progress_callback;
};

/**
 * Result of building an index
 */
struct IndexBuildResult
{
    std::uint64_t total_records = 0;
    std::uint64_t entry_count = 0;
    std::uint64_t scan_time_ms = 0;
    std::uint64_t write_time_ms = 0;
    bool success = false;
    std::string error_message;
};

/**
 * Helper class for building .index files
 *
 * Encapsulates the logic for creating and extending index files.
 * CLI tools should use this instead of implementing logic directly.
 *
 * IMPORTANT: Record numbers are based on physical file order
 * ----------------------------------------------------------
 * The index maps "record N" to byte offsets where N is the Nth DATA RECORD
 * in physical file scan order, NOT insertion order. NuDB writes records in
 * sorted key order (lexicographic memcmp), so the Nth record in the file
 * is not necessarily the Nth record that was inserted.
 *
 * IMPORTANT: Handling live .dat files (concurrent writes)
 * --------------------------------------------------------
 * IndexBuilder can be used on LIVE .dat files that are being actively written
 * by another process. This is critical for creating slices of running
 * databases.
 *
 * The challenge: NuDB record writes have intermediate states:
 *   1. Write size header (6 bytes)
 *   2. Write key (key_size bytes)
 *   3. Write value (value_size bytes)
 *
 * If we scan during steps 1-2, we see a partial record. The dat_scanner will
 * detect this as a "corrupt tail" and stop scanning. This is correct behavior!
 *
 * The IndexBuilder gracefully handles this by:
 * - Scanning as far as it can (stops at first corrupt/partial record)
 * - Recording the total_records it successfully indexed
 * - Later operations (like slice creation) use this total_records count
 *
 * When using extend() mode on a live file, it resumes from the last known good
 * offset and indexes any new complete records that have been written since.
 */
class IndexBuilder
{
public:
    /**
     * Build a new index file for a .dat file
     *
     * @param dat_path Path to nudb.dat file
     * @param index_path Output path for .index file
     * @param options Build options (interval, progress, etc)
     * @return Build result with statistics
     */
    static IndexBuildResult
    build(
        std::string const& dat_path,
        std::string const& index_path,
        IndexBuildOptions const& options = IndexBuildOptions{});

    /**
     * Extend an existing index file
     *
     * Reads the existing index, resumes from last offset, appends new entries.
     *
     * @param dat_path Path to nudb.dat file
     * @param index_path Existing .index file to extend
     * @param options Build options (must match existing interval!)
     * @return Build result with statistics (only NEW records counted)
     */
    static IndexBuildResult
    extend(
        std::string const& dat_path,
        std::string const& index_path,
        IndexBuildOptions const& options = IndexBuildOptions{});

    /**
     * Verify an index file matches a .dat file
     *
     * Checks:
     * - Header magic and version
     * - uid/appnum/key_size match dat file
     * - All offsets point to valid record boundaries
     * - Record count matches scan
     *
     * @param dat_path Path to nudb.dat file
     * @param index_path Path to .index file
     * @param ec Error code output
     * @return true if valid
     */
    static bool
    verify(
        std::string const& dat_path,
        std::string const& index_path,
        nudbview::error_code& ec);

    /**
     * Create a slice from an indexed database (HIGH-LEVEL API)
     *
     * This is the CORRECT way to create slices. It validates that slice
     * boundaries are at interval boundaries and returns an error if not.
     *
     * CRITICAL REQUIREMENT: start_record and end_record MUST be multiples
     * of the index's interval! You CANNOT slice to arbitrary record numbers
     * because that would require scanning forward on potentially live files,
     * which risks hitting partial records.
     *
     * With interval boundaries, we get EXACT byte offsets from the index
     * without any scanning. This is safe even on live databases.
     *
     * @param dat_path Path to the .dat file (read-only)
     * @param index_path Path to the .index file (must exist)
     * @param start_record_incl First record in slice (must be multiple of
     * interval)
     * @param end_record_excl One past last record (must be multiple of
     * interval, <= last indexed record + interval)
     * @param slice_key_path Output path for slice .key file
     * @param slice_meta_path Output path for slice .meta file
     * @param ec Error code output (returns error::invalid_slice_boundary for
     * invalid boundaries)
     * @return true if successful, false on error
     *
     * Example: With 142 records, interval=10:
     *   - Index has 14 entries (0-13) at records [0, 10, 20, ..., 130]
     * (total_records rounded to 140)
     *   - Last indexed record = (14-1) * 10 = 130
     *   - Max end_record = 130 + 10 = 140 (one past last indexed record)
     *   - Valid slices: [0, 10), [0, 130), [0, 140), [10, 130), etc.
     *   - INVALID: [0, 150) - exceeds max_end_record
     */
    static bool
    create_slice_from_index(
        std::string const& dat_path,
        std::string const& index_path,
        std::uint64_t start_record_incl,
        std::uint64_t end_record_excl,
        std::string const& slice_key_path,
        std::string const& slice_meta_path,
        nudbview::error_code& ec);

private:
    // Internal implementation shared by build() and extend()
    static IndexBuildResult
    build_internal(
        std::string const& dat_path,
        std::string const& index_path,
        IndexBuildOptions const& options,
        bool extend_mode);
};

}  // namespace nudbutil

#include <nudbview/impl/view/index_builder.ipp>

#endif
