//
// Index File Format - Global index for .dat file data records
// Part of nudbview/view - Read-only slice database implementation
//
// IMPORTANT: Record numbering reflects physical file order, not insertion order
// ----------------------------------------------------------------------------
// Record numbers in the index are based on sequential scan order through the .dat file.
// "Record N" = the Nth DATA RECORD encountered when scanning the file.
//
// NuDB writes records in SORTED KEY ORDER (lexicographic memcmp), not insertion order,
// because it commits from a std::map sorted by key bytes. This means:
// - Record 0 = first data record in the file (smallest key lexicographically)
// - Record N ≠ the Nth inserted record
// - The index maps these physical record numbers to byte offsets
//

#ifndef NUDBVIEW_VIEW_INDEX_FORMAT_HPP
#define NUDBVIEW_VIEW_INDEX_FORMAT_HPP

#include <nudbview/error.hpp>
#include <nudbview/type_traits.hpp>
#include <nudbview/detail/field.hpp>
#include <nudbview/detail/stream.hpp>
#include <nudbview/detail/format.hpp>
#include <array>
#include <cstdint>
#include <cstring>
#include <string>

namespace nudbview {
namespace view {

// Current version for index files
static std::size_t constexpr index_file_version = 1;

/**
 * Index File Header
 *
 * The index file provides fast O(log n) lookup of data records by number.
 * It's a simple array of byte offsets - array index is implicit record number.
 *
 * File structure:
 *  [Header: 68 bytes]
 *  [Offset Array: 8 bytes × entry_count]
 *
 * Example: For index_interval = 10,000
 *   offset[0] = byte offset of record 0
 *   offset[1] = byte offset of record 10,000
 *   offset[2] = byte offset of record 20,000
 *   ...
 *
 * To find record N:
 *   array_index = N / index_interval
 *   byte_offset = offset_array[array_index]
 *   (then scan forward N % index_interval records)
 */
struct index_file_header
{
    static std::size_t constexpr size = 68;  // 8+2+8+8+2+8+8+8+16

    // File identification
    char magic[8];                      // "nudb.idx"
    std::size_t version;                // Version = 1

    // Must match source .dat file
    std::uint64_t uid;                  // Database UID
    std::uint64_t appnum;               // Application number
    nsize_t key_size;                   // Key size in bytes

    // Index configuration
    std::uint64_t total_records;        // Total data records in .dat file
    std::uint64_t index_interval;       // Index every N records
    std::uint64_t entry_count;          // Number of offset entries in array

    // Reserved for future expansion
    char reserved[16];
};

//------------------------------------------------------------------------------
// Header I/O Functions
//------------------------------------------------------------------------------

/**
 * Read index file header from stream
 */
template<class = void>
void
read(detail::istream& is, index_file_header& ifh)
{
    detail::read(is, ifh.magic, sizeof(ifh.magic));
    detail::read<std::uint16_t>(is, ifh.version);
    detail::read<std::uint64_t>(is, ifh.uid);
    detail::read<std::uint64_t>(is, ifh.appnum);
    detail::read<std::uint16_t>(is, ifh.key_size);
    detail::read<std::uint64_t>(is, ifh.total_records);
    detail::read<std::uint64_t>(is, ifh.index_interval);
    detail::read<std::uint64_t>(is, ifh.entry_count);
    std::array<std::uint8_t, 16> reserved;
    detail::read(is, reserved.data(), reserved.size());
}

/**
 * Read index file header from file
 */
template<class File>
void
read(File& f, index_file_header& ifh, error_code& ec)
{
    std::array<std::uint8_t, index_file_header::size> buf;
    f.read(0, buf.data(), buf.size(), ec);
    if(ec)
        return;
    detail::istream is{buf};
    read(is, ifh);
}

/**
 * Write index file header to stream
 */
template<class = void>
void
write(detail::ostream& os, index_file_header const& ifh)
{
    detail::write(os, "nudb.idx", 8);
    detail::write<std::uint16_t>(os, ifh.version);
    detail::write<std::uint64_t>(os, ifh.uid);
    detail::write<std::uint64_t>(os, ifh.appnum);
    detail::write<std::uint16_t>(os, ifh.key_size);
    detail::write<std::uint64_t>(os, ifh.total_records);
    detail::write<std::uint64_t>(os, ifh.index_interval);
    detail::write<std::uint64_t>(os, ifh.entry_count);
    std::array<std::uint8_t, 16> reserved;
    reserved.fill(0);
    detail::write(os, reserved.data(), reserved.size());
}

/**
 * Write index file header to file
 */
template<class File>
void
write(File& f, index_file_header const& ifh, error_code& ec)
{
    std::array<std::uint8_t, index_file_header::size> buf;
    detail::ostream os{buf};
    write(os, ifh);
    f.write(0, buf.data(), buf.size(), ec);
}

//------------------------------------------------------------------------------
// Index Array I/O Functions
//------------------------------------------------------------------------------

/**
 * Write offset entry to stream (8 bytes, big-endian)
 */
template<class = void>
void
write_offset(detail::ostream& os, noff_t offset)
{
    detail::write<std::uint64_t>(os, offset);
}

/**
 * Read offset entry from stream (8 bytes, big-endian)
 */
template<class = void>
void
read_offset(detail::istream& is, noff_t& offset)
{
    detail::read<std::uint64_t>(is, offset);
}

//------------------------------------------------------------------------------
// Verification Functions
//------------------------------------------------------------------------------

/**
 * Verify index file header contents
 */
template<class = void>
void
verify(index_file_header const& ifh, error_code& ec)
{
    std::string const magic{ifh.magic, 8};
    if(magic != "nudb.idx")
    {
        ec = error::not_data_file;  // VFALCO: Add index-specific error codes?
        return;
    }
    if(ifh.version != index_file_version)
    {
        ec = error::different_version;
        return;
    }
    if(ifh.key_size < 1)
    {
        ec = error::invalid_key_size;
        return;
    }
    if(ifh.total_records < 1)
    {
        ec = error::invalid_key_size;  // VFALCO: Better error code
        return;
    }
    if(ifh.index_interval < 1)
    {
        ec = error::invalid_key_size;  // VFALCO: Better error code
        return;
    }
    if(ifh.entry_count < 1)
    {
        ec = error::invalid_key_size;  // VFALCO: Better error code
        return;
    }
}

/**
 * Verify index file header matches dat file header
 */
template<class = void>
void
verify(detail::dat_file_header const& dh,
    index_file_header const& ifh, error_code& ec)
{
    verify(ifh, ec);
    if(ec)
        return;
    if(ifh.uid != dh.uid)
    {
        ec = error::uid_mismatch;
        return;
    }
    if(ifh.appnum != dh.appnum)
    {
        ec = error::appnum_mismatch;
        return;
    }
    if(ifh.key_size != dh.key_size)
    {
        ec = error::key_size_mismatch;
        return;
    }
}

/**
 * Lookup byte offset for a given data record number
 *
 * Uses binary-searchable index to find the closest indexed record,
 * then caller must scan forward the remaining records.
 *
 * @param index_array Pointer to memory-mapped or loaded index array
 * @param entry_count Number of entries in index array
 * @param index_interval Interval between indexed records
 * @param record_number The data record number to look up
 * @param[out] closest_offset Byte offset of closest indexed record
 * @param[out] records_to_skip Number of records to scan forward from closest_offset
 */
template<class = void>
void
lookup_record_offset(
    noff_t const* index_array,
    std::uint64_t entry_count,
    std::uint64_t index_interval,
    std::uint64_t record_number,
    noff_t& closest_offset,
    std::uint64_t& records_to_skip)
{
    // Calculate which index entry to use
    std::uint64_t array_index = record_number / index_interval;

    // Clamp to array bounds
    if(array_index >= entry_count)
        array_index = entry_count - 1;

    // Get offset from array
    closest_offset = index_array[array_index];

    // Calculate how many records to skip
    std::uint64_t indexed_record = array_index * index_interval;
    records_to_skip = record_number - indexed_record;
}

} // view
} // nudbview

#endif
