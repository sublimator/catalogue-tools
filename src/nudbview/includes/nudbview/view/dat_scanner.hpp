//
// Reusable .dat file scanning utilities
//
// IMPORTANT: Record ordering in NuDB .dat files
// --------------------------------------------
// The .dat file contains data records in SORTED KEY ORDER, not insertion order.
// NuDB buffers inserts in a std::map sorted by lexicographic key comparison.
// On commit, it writes records to .dat in the map's iteration order (sorted by
// key bytes).
//
// When scanning:
// - record_num is the Nth DATA RECORD in physical scan order (0-based)
// - Spill records (size==0) are skipped and don't count toward record_num
// - Data records appear in sorted key order, not the order they were inserted
// - This is why indexes map "record N" to byte offsets in this physical order
//

#ifndef NUDB_UTIL_DAT_SCANNER_HPP
#define NUDB_UTIL_DAT_SCANNER_HPP

#include <boost/iostreams/device/mapped_file.hpp>
#include <cstdint>
#include <functional>
#include <nudbview/detail/field.hpp>
#include <nudbview/detail/format.hpp>

namespace nudbutil {

// Size field is 48 bits (6 bytes) in NuDB .dat files
constexpr std::size_t SIZE_FIELD_BYTES = 6;

// Bucket size field is 16 bits (2 bytes) in spill records
constexpr std::size_t BUCKET_SIZE_FIELD_BYTES = 2;

/**
 * Read 48-bit big-endian value (6 bytes)
 * Used for reading record size fields in NuDB .dat files
 */
inline std::uint64_t
read_size48_be(const std::uint8_t* data)
{
    return (static_cast<std::uint64_t>(data[0]) << 40) |
        (static_cast<std::uint64_t>(data[1]) << 32) |
        (static_cast<std::uint64_t>(data[2]) << 24) |
        (static_cast<std::uint64_t>(data[3]) << 16) |
        (static_cast<std::uint64_t>(data[4]) << 8) |
        static_cast<std::uint64_t>(data[5]);
}

/**
 * Read 16-bit big-endian value (2 bytes)
 * Used for reading bucket size in spill records
 */
inline std::uint16_t
read_uint16_be(const std::uint8_t* data)
{
    return (static_cast<std::uint16_t>(data[0]) << 8) |
        static_cast<std::uint16_t>(data[1]);
}

/**
 * Scan .dat file records using mmap
 *
 * TODO: Consider refactoring to use raw pointers instead of mmap for
 * flexibility
 *
 * Efficiently scans through a .dat file, calling a callback for each data
 * record. Automatically handles spill records (skips them).
 *
 * IMPORTANT: Handling LIVE .dat files (concurrent writes)
 * --------------------------------------------------------
 * This scanner is designed to work on .dat files being actively written by
 * another process (e.g., running Ripple/Xahau node). This is CRITICAL for
 * the "history problem" - we must slice hot databases!
 *
 * NuDB record writes are NOT atomic. A write has stages:
 *   1. Write size header (6 bytes)
 *   2. Write key (key_size bytes)
 *   3. Write value (value_size bytes)
 *
 * If we scan mid-write (between steps), we see a PARTIAL RECORD:
 * - Size header may be present but key/value incomplete
 * - This is detected by bounds check: offset + key_size + size > file_size
 * - We stop scanning immediately and return records found so far
 * - This is CORRECT behavior! The partial record is not valid yet.
 *
 * Used by IndexBuilder to safely index live databases.
 * See tests/nudbview/slice-stress-gtest.cpp for stress testing.
 *
 * @param mmap Memory-mapped .dat file
 * @param key_size Key size from dat file header
 * @param callback Called for each data record: callback(record_num, offset,
 * size)
 * @param start_offset_incl Byte offset to start scanning from (default: after
 * header)
 * @param start_record_num Starting record number (default: 0)
 * @return Total number of data records found (stops at first incomplete record)
 */
inline std::uint64_t
scan_dat_records(
    boost::iostreams::mapped_file_source const& mmap,
    std::uint16_t key_size,
    std::function<void(
        std::uint64_t record_num,
        std::uint64_t offset,
        std::uint64_t size)> callback,
    std::uint64_t start_offset_incl = nudbview::detail::dat_file_header::size,
    std::uint64_t start_record_num = 0)
{
    using namespace nudbview::detail;

    auto const* data = reinterpret_cast<std::uint8_t const*>(mmap.data());
    std::uint64_t file_size = mmap.size();
    std::uint64_t offset = start_offset_incl;
    std::uint64_t record_num = start_record_num;

    while (offset + SIZE_FIELD_BYTES <= file_size)
    {
        auto const record_offset = offset;

        // Read 6-byte size field
        std::uint64_t size = read_size48_be(data + offset);

        offset += SIZE_FIELD_BYTES;

        if (size > 0)
        {
            // Data Record

            // CRITICAL BOUNDS CHECK: Detects partial records in live files
            // If another process is mid-write, we may have:
            // - Size header written (6 bytes we just read)
            // - But key+value NOT fully written yet
            // This check detects that case and stops scanning gracefully.
            if (offset + key_size + size > file_size)
                break;  // Corrupt/partial tail - stop here!

            // Call callback
            callback(record_num, record_offset, size);

            // Skip key + data
            offset += key_size;
            offset += size;

            ++record_num;
        }
        else
        {
            // Spill Record (size == 0)
            if (offset + BUCKET_SIZE_FIELD_BYTES > file_size)
                break;

            // Read 2-byte bucket_size
            std::uint16_t bucket_size = read_uint16_be(data + offset);

            offset += BUCKET_SIZE_FIELD_BYTES;

            // Skip bucket data
            if (offset + bucket_size > file_size)
                break;

            offset += bucket_size;

            // Don't count spill records
        }
    }

    return record_num;
}

/**
 * Calculate the END offset of a data record (last byte, inclusive)
 *
 * Given the start offset of a data record, reads its size field and
 * calculates where the record ends (inclusive).
 *
 * @param mmap Memory-mapped .dat file
 * @param key_size Key size from dat file header
 * @param record_start_offset Offset where the record starts (points to size
 * field)
 * @param end_offset_incl [OUT] Last byte offset of the record (inclusive),
 * unchanged on error
 * @return true if successful, false if record is incomplete/invalid
 */
inline bool
get_record_end_offset_incl(
    boost::iostreams::mapped_file_source const& mmap,
    std::uint16_t key_size,
    std::uint64_t record_start_offset,
    std::uint64_t& end_offset_incl)
{
    auto const* data = reinterpret_cast<std::uint8_t const*>(mmap.data());
    std::uint64_t file_size = mmap.size();

    // Check we can read the size field
    if (record_start_offset + SIZE_FIELD_BYTES > file_size)
        return false;

    // Read size
    std::uint64_t value_size = read_size48_be(data + record_start_offset);

    // Spill record, not a data record
    if (value_size == 0)
        return false;

    // Check record is complete: size field + key + value
    std::uint64_t record_total_size = SIZE_FIELD_BYTES + key_size + value_size;
    if (record_start_offset + record_total_size > file_size)
        return false;  // Incomplete record

    // Calculate end (last byte, inclusive)
    end_offset_incl = record_start_offset + record_total_size - 1;
    return true;
}

/**
 * Visit all spill records in a .dat file buffer
 *
 * Spill records have size==0 and contain overflow bucket data.
 * They are created when a hash bucket in the .key file overflows.
 *
 * @param data Raw pointer to .dat file data
 * @param file_size Size of the data buffer
 * @param key_size Key size (needed to skip data records)
 * @param callback Called for each spill record: callback(offset, bucket_size)
 * @param start_offset_incl Byte offset to start scanning from (default: after
 * header)
 * @return Total number of spill records found
 */
inline std::uint64_t
visit_spill_records(
    const std::uint8_t* data,
    std::uint64_t file_size,
    std::uint16_t key_size,
    std::function<void(std::uint64_t offset, std::uint16_t bucket_size)>
        callback,
    std::uint64_t start_offset_incl = nudbview::detail::dat_file_header::size)
{
    using namespace nudbview::detail;

    std::uint64_t offset = start_offset_incl;
    std::uint64_t spill_count = 0;

    while (offset + SIZE_FIELD_BYTES <= file_size)
    {
        auto const record_offset = offset;

        // Read size field
        std::uint64_t size = read_size48_be(data + offset);
        offset += SIZE_FIELD_BYTES;

        if (size == 0)
        {
            // Spill Record!
            if (offset + BUCKET_SIZE_FIELD_BYTES > file_size)
                break;

            // Read bucket_size
            std::uint16_t bucket_size = read_uint16_be(data + offset);
            offset += BUCKET_SIZE_FIELD_BYTES;

            // Call callback
            callback(record_offset, bucket_size);
            spill_count++;

            // Skip bucket data
            if (offset + bucket_size > file_size)
                break;

            offset += bucket_size;
        }
        else
        {
            // Data Record
            // Bounds check
            if (offset + key_size + size > file_size)
                break;  // Corrupt/partial tail

            // Skip key + data
            offset += key_size + size;
        }
    }

    return spill_count;
}

/**
 * Count spill records in a .dat file buffer
 *
 * @param data Raw pointer to .dat file data
 * @param file_size Size of the data buffer
 * @param key_size Key size from dat file header
 * @param start_offset_incl Byte offset to start scanning from (default: after
 * header)
 * @return Total number of spill records found
 */
inline std::uint64_t
count_spill_records(
    const std::uint8_t* data,
    std::uint64_t file_size,
    std::uint16_t key_size,
    std::uint64_t start_offset_incl = nudbview::detail::dat_file_header::size)
{
    using namespace nudbview::detail;
    std::uint64_t offset = start_offset_incl;
    std::uint64_t spill_count = 0;

    while (offset + SIZE_FIELD_BYTES <= file_size)
    {
        // Read 6-byte size field
        std::uint64_t size = read_size48_be(data + offset);

        offset += SIZE_FIELD_BYTES;

        if (size == 0)
        {
            // Spill Record!
            if (offset + BUCKET_SIZE_FIELD_BYTES > file_size)
                break;

            // Read 2-byte bucket_size
            std::uint16_t bucket_size = read_uint16_be(data + offset);

            offset += BUCKET_SIZE_FIELD_BYTES;

            // Skip bucket data
            if (offset + bucket_size > file_size)
                break;

            offset += bucket_size;
            spill_count++;
        }
        else
        {
            // Data Record
            // Bounds check
            if (offset + key_size + size > file_size)
                break;  // Corrupt/partial tail

            // Skip key + data
            offset += key_size;
            offset += size;
        }
    }

    return spill_count;
}

}  // namespace nudbutil

#endif
