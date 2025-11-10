//
// Slice Store Format Definitions
// Part of nudbview/view - Read-only slice database implementation
//

#ifndef NUDBVIEW_VIEW_FORMAT_HPP
#define NUDBVIEW_VIEW_FORMAT_HPP

#include <nudbview/error.hpp>
#include <nudbview/type_traits.hpp>
#include <nudbview/detail/buffer.hpp>
#include <nudbview/detail/endian.hpp>
#include <nudbview/detail/field.hpp>
#include <nudbview/detail/stream.hpp>
#include <nudbview/detail/format.hpp>
#include <array>
#include <cstdint>
#include <cstring>
#include <string>

namespace nudbview {
namespace view {

// Current version for slice meta files
static std::size_t constexpr slice_meta_version = 1;

/**
 * Slice Meta File Header
 *
 * The slice meta file contains:
 *  1. This header (256 bytes)
 *  2. Index section (record_number -> dat_offset mappings)
 *  3. Spill section (bucket overflow records)
 *
 * A slice is a contiguous range of records from a .dat file,
 * with its own optimized key file and metadata.
 */
struct slice_meta_header
{
    static std::size_t constexpr size = 256;

    // File identification
    char type[16];                      // "nudb.slice.meta" (16 bytes for alignment)
    std::size_t version;                // Version = 1

    // Must match source .dat file
    std::uint64_t uid;                  // Database UID
    std::uint64_t appnum;               // Application number
    nsize_t key_size;                   // Key size in bytes

    // Slice boundaries (byte offsets in original .dat file)
    noff_t slice_start_offset;          // First byte of first record
    noff_t slice_end_offset;            // Last byte of last record (inclusive)

    // Statistics
    std::uint64_t key_count;            // Total keys in this slice

    // Index configuration
    std::uint64_t index_interval;       // Index every N records (e.g., 10000)
    std::uint64_t index_count;          // Number of index entries
    noff_t index_section_offset;        // Byte offset where index starts (after header)

    // Spill records
    // Spills are written here instead of .dat file (which is read-only)
    noff_t spill_section_offset;        // Byte offset where spills start
    std::uint64_t spill_count;          // Number of spill records

    // Reserved for future expansion
    char reserved[128];
};

/**
 * Index Entry
 *
 * Maps a record number to its byte offset in the .dat file.
 * Stored in the index section of the meta file.
 *
 * Index is sparse - only every Nth record is indexed.
 */
struct index_entry
{
    static std::size_t constexpr size = 16;

    std::uint64_t record_number;        // Sequential record number (0-based)
    noff_t dat_offset;                  // Byte offset in .dat file
};

//------------------------------------------------------------------------------
// Header I/O Functions
//------------------------------------------------------------------------------

/**
 * Read slice meta header from stream
 */
template<class = void>
void
read(detail::istream& is, slice_meta_header& smh)
{
    detail::read(is, smh.type, sizeof(smh.type));
    detail::read<std::uint16_t>(is, smh.version);
    detail::read<std::uint64_t>(is, smh.uid);
    detail::read<std::uint64_t>(is, smh.appnum);
    detail::read<std::uint16_t>(is, smh.key_size);
    detail::read<std::uint64_t>(is, smh.slice_start_offset);
    detail::read<std::uint64_t>(is, smh.slice_end_offset);
    detail::read<std::uint64_t>(is, smh.key_count);
    detail::read<std::uint64_t>(is, smh.index_interval);
    detail::read<std::uint64_t>(is, smh.index_count);
    detail::read<std::uint64_t>(is, smh.index_section_offset);
    detail::read<std::uint64_t>(is, smh.spill_section_offset);
    detail::read<std::uint64_t>(is, smh.spill_count);
    std::array<std::uint8_t, 128> reserved;
    detail::read(is, reserved.data(), reserved.size());
}

/**
 * Read slice meta header from file
 */
template<class File>
void
read(File& f, slice_meta_header& smh, error_code& ec)
{
    std::array<std::uint8_t, slice_meta_header::size> buf;
    f.read(0, buf.data(), buf.size(), ec);
    if(ec)
        return;
    detail::istream is{buf};
    read(is, smh);
}

/**
 * Write slice meta header to stream
 */
template<class = void>
void
write(detail::ostream& os, slice_meta_header const& smh)
{
    detail::write(os, "nudb.slice.meta", 16);
    detail::write<std::uint16_t>(os, smh.version);
    detail::write<std::uint64_t>(os, smh.uid);
    detail::write<std::uint64_t>(os, smh.appnum);
    detail::write<std::uint16_t>(os, smh.key_size);
    detail::write<std::uint64_t>(os, smh.slice_start_offset);
    detail::write<std::uint64_t>(os, smh.slice_end_offset);
    detail::write<std::uint64_t>(os, smh.key_count);
    detail::write<std::uint64_t>(os, smh.index_interval);
    detail::write<std::uint64_t>(os, smh.index_count);
    detail::write<std::uint64_t>(os, smh.index_section_offset);
    detail::write<std::uint64_t>(os, smh.spill_section_offset);
    detail::write<std::uint64_t>(os, smh.spill_count);
    std::array<std::uint8_t, 128> reserved;
    reserved.fill(0);
    detail::write(os, reserved.data(), reserved.size());
}

/**
 * Write slice meta header to file
 */
template<class File>
void
write(File& f, slice_meta_header const& smh, error_code& ec)
{
    std::array<std::uint8_t, slice_meta_header::size> buf;
    detail::ostream os{buf};
    write(os, smh);
    f.write(0, buf.data(), buf.size(), ec);
}

//------------------------------------------------------------------------------
// Index Entry I/O Functions
//------------------------------------------------------------------------------

/**
 * Read index entry from stream
 */
template<class = void>
void
read(detail::istream& is, index_entry& ie)
{
    detail::read<std::uint64_t>(is, ie.record_number);
    detail::read<std::uint64_t>(is, ie.dat_offset);
}

/**
 * Write index entry to stream
 */
template<class = void>
void
write(detail::ostream& os, index_entry const& ie)
{
    detail::write<std::uint64_t>(os, ie.record_number);
    detail::write<std::uint64_t>(os, ie.dat_offset);
}

//------------------------------------------------------------------------------
// Verification Functions
//------------------------------------------------------------------------------

/**
 * Verify slice meta header contents
 */
template<class = void>
void
verify(slice_meta_header const& smh, error_code& ec)
{
    std::string const type{smh.type, 16};
    if(type.substr(0, 15) != "nudb.slice.meta")
    {
        ec = error::not_data_file;  // VFALCO: Add slice-specific error codes?
        return;
    }
    if(smh.version != slice_meta_version)
    {
        ec = error::different_version;
        return;
    }
    if(smh.key_size < 1)
    {
        ec = error::invalid_key_size;
        return;
    }
    if(smh.slice_end_offset <= smh.slice_start_offset)
    {
        ec = error::invalid_key_size;  // VFALCO: Need better error code
        return;
    }
    if(smh.key_count < 1)
    {
        ec = error::invalid_key_size;  // VFALCO: Need better error code
        return;
    }
    if(smh.index_interval < 1)
    {
        ec = error::invalid_key_size;  // VFALCO: Need better error code
        return;
    }
}

/**
 * Verify slice meta header matches dat file header
 */
template<class = void>
void
verify(detail::dat_file_header const& dh,
    slice_meta_header const& smh, error_code& ec)
{
    verify(smh, ec);
    if(ec)
        return;
    if(smh.uid != dh.uid)
    {
        ec = error::uid_mismatch;
        return;
    }
    if(smh.appnum != dh.appnum)
    {
        ec = error::appnum_mismatch;
        return;
    }
    if(smh.key_size != dh.key_size)
    {
        ec = error::key_size_mismatch;
        return;
    }
}

/**
 * Verify slice meta header matches key file header
 */
template<class = void>
void
verify(detail::key_file_header const& kh,
    slice_meta_header const& smh, error_code& ec)
{
    verify(smh, ec);
    if(ec)
        return;
    if(smh.uid != kh.uid)
    {
        ec = error::uid_mismatch;
        return;
    }
    if(smh.appnum != kh.appnum)
    {
        ec = error::appnum_mismatch;
        return;
    }
    if(smh.key_size != kh.key_size)
    {
        ec = error::key_size_mismatch;
        return;
    }
}

} // view
} // nudbview

#endif
