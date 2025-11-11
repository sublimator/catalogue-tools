//
// Slice Rekey - Create optimized key and meta files for a slice of a .dat file
// Part of nudbview/view - Read-only slice database implementation
//

#ifndef NUDBVIEW_VIEW_SLICE_REKEY_HPP
#define NUDBVIEW_VIEW_SLICE_REKEY_HPP

#include <nudbview/error.hpp>
#include <nudbview/file.hpp>
#include <nudbview/type_traits.hpp>
#include <cstddef>
#include <cstdint>

namespace nudbview {
namespace view {

/** Create optimized key and meta files for a slice of a data file.

    This algorithm builds a key file and meta file for a contiguous
    range of records in a .dat file. The key file provides fast hash
    lookup, while the meta file contains:

    1. Index - Maps record numbers to .dat offsets (every Nth record)
    2. Spills - Bucket overflow records (can't append to read-only .dat)
    3. Statistics - Key count, slice boundaries, etc.

    The algorithm works by:
    - Scanning the [start_offset, end_offset] range once to count keys
    - Building an optimized hash table sized for this key count
    - Creating an index for fast seeking (every index_interval records)
    - Writing spills to the meta file instead of the .dat file

    IMPORTANT: Handling live .dat files (concurrent writes)
    --------------------------------------------------------
    This function can slice LIVE .dat files being written by another process.
    This is critical for creating slices of running Ripple/Xahau nodes.

    The bulk_reader will throw error::short_read if it tries to read a partial
    record at the tail. This is EXPECTED and should be handled gracefully:
    - The caller (stress tests, CLI tools) should catch short_read errors
    - The slice should be created up to the last COMPLETE record
    - Use IndexBuilder to determine safe bounds before calling rekey_slice

    Never call rekey_slice with end_offset beyond the last complete record!

    @par Template Parameters

    @tparam Hasher The hash function to use. This type must
    meet the requirements of @b Hasher. The hash function
    must be the same as that used with the database.

    @tparam File The type of file to use. This type must meet
    the requirements of @b File.

    @param dat_path The path to the data file (read-only).

    @param start_offset Byte offset of first record in slice
    (typically after dat_file_header for first slice).

    @param end_offset Byte offset of last byte in slice (inclusive).
    Must be at a record boundary.

    @param slice_key_path The path to the slice key file to create.

    @param slice_meta_path The path to the slice meta file to create.

    @param blockSize The size of a key file block. Larger
    blocks hold more keys but require more I/O cycles per
    operation. Should match original database block size.

    @param loadFactor A number between zero and one
    representing the average bucket occupancy (number of
    items). A value of 0.5 is perfect. Lower numbers
    waste space, and higher numbers produce negligible
    savings at the cost of increased I/O cycles.

    @param index_interval Create an index entry every N records.
    Larger values save space but make seeking slower.
    Typical values: 1000-10000.

    @param bufferSize The number of bytes to allocate for the buffer.

    @param ec Set to the error if any occurred.

    @param progress A function which will be called periodically
    as the algorithm proceeds. The equivalent signature of the
    progress function must be:
    @code
    void progress(
        std::uint64_t amount,   // Amount of work done so far
        std::uint64_t total     // Total amount of work to do
    );
    @endcode

    @param expected_record_count Optional expected number of records in the slice.
    If provided (non-zero), skips Pass 1 (counting scan) and uses this count directly.
    During Pass 2, validates that actual record count matches. Use when you already
    have an index and know the exact record count. Errors if mismatch detected.

    @param args Optional arguments passed to @b File constructors.
*/
template<
    class Hasher,
    class File,
    class Progress,
    class... Args
>
void
rekey_slice(
    path_type const& dat_path,
    noff_t start_offset,
    noff_t end_offset,
    path_type const& slice_key_path,
    path_type const& slice_meta_path,
    std::size_t blockSize,
    float loadFactor,
    std::uint64_t index_interval,
    std::size_t bufferSize,
    error_code& ec,
    Progress&& progress,
    std::uint64_t expected_record_count = 0,
    Args&&... args);

} // view
} // nudbview

#include <nudbview/impl/view/slice_rekey.ipp>

#endif
