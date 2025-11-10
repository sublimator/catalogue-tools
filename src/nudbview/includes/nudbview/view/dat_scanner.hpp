//
// Reusable .dat file scanning utilities
//
// IMPORTANT: Record ordering in NuDB .dat files
// --------------------------------------------
// The .dat file contains data records in SORTED KEY ORDER, not insertion order.
// NuDB buffers inserts in a std::map sorted by lexicographic key comparison.
// On commit, it writes records to .dat in the map's iteration order (sorted by key bytes).
//
// When scanning:
// - record_num is the Nth DATA RECORD in physical scan order (0-based)
// - Spill records (size==0) are skipped and don't count toward record_num
// - Data records appear in sorted key order, not the order they were inserted
// - This is why indexes map "record N" to byte offsets in this physical order
//

#ifndef NUDB_UTIL_DAT_SCANNER_HPP
#define NUDB_UTIL_DAT_SCANNER_HPP

#include <nudbview/detail/format.hpp>
#include <nudbview/detail/field.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <cstdint>
#include <functional>

namespace nudbutil {

/**
 * Scan .dat file records using mmap
 *
 * Efficiently scans through a .dat file, calling a callback for each data record.
 * Automatically handles spill records (skips them).
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
 * @param callback Called for each data record: callback(record_num, offset, size)
 * @param start_offset Byte offset to start scanning from (default: after header)
 * @param start_record_num Starting record number (default: 0)
 * @return Total number of data records found (stops at first incomplete record)
 */
inline std::uint64_t
scan_dat_records(
    boost::iostreams::mapped_file_source const& mmap,
    std::uint16_t key_size,
    std::function<void(std::uint64_t record_num, std::uint64_t offset, std::uint64_t size)> callback,
    std::uint64_t start_offset = nudbview::detail::dat_file_header::size,
    std::uint64_t start_record_num = 0)
{
    using namespace nudbview::detail;

    auto const* data = reinterpret_cast<std::uint8_t const*>(mmap.data());
    std::uint64_t file_size = mmap.size();
    std::uint64_t offset = start_offset;
    std::uint64_t record_num = start_record_num;

    while (offset + 6 <= file_size)
    {
        auto const record_offset = offset;

        // Read 6-byte size field (big-endian!)
        std::uint64_t size =
            (static_cast<std::uint64_t>(data[offset + 0]) << 40) |
            (static_cast<std::uint64_t>(data[offset + 1]) << 32) |
            (static_cast<std::uint64_t>(data[offset + 2]) << 24) |
            (static_cast<std::uint64_t>(data[offset + 3]) << 16) |
            (static_cast<std::uint64_t>(data[offset + 4]) << 8) |
            static_cast<std::uint64_t>(data[offset + 5]);

        offset += 6;

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
            if (offset + 2 > file_size)
                break;

            // Read 2-byte bucket_size (big-endian)
            std::uint16_t bucket_size =
                (static_cast<std::uint16_t>(data[offset]) << 8) |
                static_cast<std::uint16_t>(data[offset + 1]);

            offset += 2;

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
 * @param record_start_offset Offset where the record starts (points to size field)
 * @param end_offset [OUT] Last byte offset of the record (inclusive), unchanged on error
 * @return true if successful, false if record is incomplete/invalid
 */
inline bool
get_record_end_offset(
    boost::iostreams::mapped_file_source const& mmap,
    std::uint16_t key_size,
    std::uint64_t record_start_offset,
    std::uint64_t& end_offset)
{
    auto const* data = reinterpret_cast<std::uint8_t const*>(mmap.data());
    std::uint64_t file_size = mmap.size();

    // Check we can read the 6-byte size field
    if (record_start_offset + 6 > file_size)
        return false;

    // Read size (big-endian)
    std::uint64_t value_size =
        (static_cast<std::uint64_t>(data[record_start_offset + 0]) << 40) |
        (static_cast<std::uint64_t>(data[record_start_offset + 1]) << 32) |
        (static_cast<std::uint64_t>(data[record_start_offset + 2]) << 24) |
        (static_cast<std::uint64_t>(data[record_start_offset + 3]) << 16) |
        (static_cast<std::uint64_t>(data[record_start_offset + 4]) << 8) |
        static_cast<std::uint64_t>(data[record_start_offset + 5]);

    // Spill record, not a data record
    if (value_size == 0)
        return false;

    // Check record is complete: size(6) + key + value
    std::uint64_t record_total_size = 6 + key_size + value_size;
    if (record_start_offset + record_total_size > file_size)
        return false;  // Incomplete record

    // Calculate end (last byte, inclusive)
    end_offset = record_start_offset + record_total_size - 1;
    return true;
}

} // nudbutil

#endif
