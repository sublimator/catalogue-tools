//
// Index Reader - Helper for loading and querying .index files
//

#ifndef NUDB_UTIL_INDEX_READER_HPP
#define NUDB_UTIL_INDEX_READER_HPP

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <catl/core/logger.h>
#include <cstdint>
#include <nudbview/detail/format.hpp>
#include <nudbview/view/index_format.hpp>
#include <string>

namespace nudbutil {

/**
 * Helper class for loading and querying .index files
 *
 * Provides convenient access to index data:
 * - Load and validate index files
 * - Translate record numbers to byte offsets
 * - Verify index matches dat file
 */
class IndexReader
{
private:
    boost::iostreams::mapped_file_source mmap_;
    nudbview::view::index_file_header header_;
    nudbview::noff_t const* offset_array_;
    bool loaded_;

public:
    IndexReader() : offset_array_(nullptr), loaded_(false)
    {
    }

    ~IndexReader()
    {
        close();
    }

    /**
     * Load index file from path
     *
     * @param index_path Path to .index file
     * @param ec Error code output
     * @return true if loaded successfully
     */
    bool
    load(std::string const& index_path, nudbview::error_code& ec)
    {
        namespace fs = boost::filesystem;

        // Check file exists
        if (!fs::exists(index_path))
        {
            ec = nudbview::make_error_code(nudbview::error::not_data_file);
            return false;
        }

        // Memory-map the index file
        try
        {
            mmap_.open(index_path);
        }
        catch (const std::exception& e)
        {
            LOGE("Failed to mmap index file: ", e.what());
            ec = nudbview::make_error_code(nudbview::error::short_read);
            return false;
        }

        if (!mmap_.is_open())
        {
            ec = nudbview::make_error_code(nudbview::error::short_read);
            return false;
        }

        auto const* data = reinterpret_cast<const std::uint8_t*>(mmap_.data());
        std::uint64_t file_size = mmap_.size();

        // Read header
        if (file_size < nudbview::view::index_file_header::size)
        {
            ec = nudbview::error_code{nudbview::error::short_read};
            return false;
        }

        nudbview::detail::istream is{
            data, nudbview::view::index_file_header::size};
        nudbview::view::read(is, header_);

        // Verify header
        nudbview::view::verify(header_, ec);
        if (ec)
            return false;

        // Verify file size matches header
        std::uint64_t expected_size =
            nudbview::view::index_file_header::size + (header_.entry_count * 8);
        if (file_size < expected_size)
        {
            ec = nudbview::error_code{nudbview::error::short_read};
            return false;
        }

        // Point to offset array
        offset_array_ = reinterpret_cast<nudbview::noff_t const*>(
            data + nudbview::view::index_file_header::size);

        loaded_ = true;
        return true;
    }

    /**
     * Verify this index matches a dat file header
     */
    bool
    verify_match(
        nudbview::detail::dat_file_header const& dh,
        nudbview::error_code& ec) const
    {
        if (!loaded_)
        {
            ec = nudbview::make_error_code(nudbview::error::short_read);
            return false;
        }

        nudbview::view::verify(dh, header_, ec);
        return !ec;
    }

    /**
     * Translate record number to byte offset
     *
     * Uses index to find closest indexed record, then tells you how many
     * records to scan forward from that offset.
     *
     * @param record_num Data record number (zero-based)
     * @param[out] byte_offset Byte offset of closest indexed record
     * @param[out] records_to_skip How many records to scan from byte_offset
     * @return true if successful
     */
    bool
    lookup_record_start_offset(
        std::uint64_t record_num,
        nudbview::noff_t& byte_offset,
        std::uint64_t& records_to_skip) const
    {
        if (!loaded_)
            return false;

        // Read offset from memory-mapped array
        // Note: offsets are stored in BIG ENDIAN!
        std::uint64_t array_index = record_num / header_.index_interval;

        if (array_index >= header_.entry_count)
            array_index = header_.entry_count - 1;

        // Read 8-byte big-endian offset
        auto const* offset_bytes =
            reinterpret_cast<std::uint8_t const*>(offset_array_ + array_index);

        byte_offset = (static_cast<std::uint64_t>(offset_bytes[0]) << 56) |
            (static_cast<std::uint64_t>(offset_bytes[1]) << 48) |
            (static_cast<std::uint64_t>(offset_bytes[2]) << 40) |
            (static_cast<std::uint64_t>(offset_bytes[3]) << 32) |
            (static_cast<std::uint64_t>(offset_bytes[4]) << 24) |
            (static_cast<std::uint64_t>(offset_bytes[5]) << 16) |
            (static_cast<std::uint64_t>(offset_bytes[6]) << 8) |
            static_cast<std::uint64_t>(offset_bytes[7]);

        std::uint64_t indexed_record = array_index * header_.index_interval;
        records_to_skip = record_num - indexed_record;

        return true;
    }

    void
    close()
    {
        if (mmap_.is_open())
            mmap_.close();
        offset_array_ = nullptr;
        loaded_ = false;
    }

    // Accessors
    bool
    is_loaded() const
    {
        return loaded_;
    }
    nudbview::view::index_file_header const&
    header() const
    {
        return header_;
    }
    std::uint64_t
    total_records() const
    {
        return header_.total_records_indexed;
    }
    std::uint64_t
    index_interval() const
    {
        return header_.index_interval;
    }
    std::uint64_t
    entry_count() const
    {
        return header_.entry_count;
    }

    // Debug helpers
    void
    dump_entries(std::ostream& os, std::size_t max_entries = 100) const
    {
        if (!loaded_)
        {
            os << "Index not loaded" << std::endl;
            return;
        }

        os << "Index: " << header_.entry_count
           << " entries, interval=" << header_.index_interval
           << ", total_records=" << header_.total_records_indexed << std::endl;

        std::size_t to_print = std::min(
            static_cast<std::size_t>(header_.entry_count), max_entries);
        for (std::size_t i = 0; i < to_print; ++i)
        {
            // Read big-endian offset
            auto const* offset_bytes =
                reinterpret_cast<std::uint8_t const*>(offset_array_ + i);
            std::uint64_t offset =
                (static_cast<std::uint64_t>(offset_bytes[0]) << 56) |
                (static_cast<std::uint64_t>(offset_bytes[1]) << 48) |
                (static_cast<std::uint64_t>(offset_bytes[2]) << 40) |
                (static_cast<std::uint64_t>(offset_bytes[3]) << 32) |
                (static_cast<std::uint64_t>(offset_bytes[4]) << 24) |
                (static_cast<std::uint64_t>(offset_bytes[5]) << 16) |
                (static_cast<std::uint64_t>(offset_bytes[6]) << 8) |
                static_cast<std::uint64_t>(offset_bytes[7]);

            std::uint64_t record_num = i * header_.index_interval;
            os << "  [" << i << "] record " << record_num << " -> offset "
               << offset << std::endl;
        }
        if (header_.entry_count > to_print)
        {
            os << "  ... (" << (header_.entry_count - to_print)
               << " more entries)" << std::endl;
        }
    }
};

}  // namespace nudbutil

#endif
