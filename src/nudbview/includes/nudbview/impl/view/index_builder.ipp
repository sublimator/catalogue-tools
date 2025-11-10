#ifndef NUDBVIEW_IMPL_VIEW_INDEX_BUILDER_IPP
#define NUDBVIEW_IMPL_VIEW_INDEX_BUILDER_IPP

#include <nudbview/view/dat_scanner.hpp>
#include <nudbview/view/index_format.hpp>
#include <nudbview/view/index_reader.hpp>
#include <nudbview/view/slice_rekey.hpp>
#include <nudbview/detail/format.hpp>
#include <nudbview/native_file.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/filesystem.hpp>
#include <chrono>
#include <vector>

namespace fs = boost::filesystem;

namespace nudbutil {

IndexBuildResult
IndexBuilder::build(
    std::string const& dat_path,
    std::string const& index_path,
    IndexBuildOptions const& options)
{
    return build_internal(dat_path, index_path, options, false);
}

IndexBuildResult
IndexBuilder::extend(
    std::string const& dat_path,
    std::string const& index_path,
    IndexBuildOptions const& options)
{
    return build_internal(dat_path, index_path, options, true);
}

IndexBuildResult
IndexBuilder::build_internal(
    std::string const& dat_path,
    std::string const& index_path,
    IndexBuildOptions const& options,
    bool extend_mode)
{
    IndexBuildResult result;

    // Check dat file exists
    if (!fs::exists(dat_path))
    {
        result.error_message = "Database file not found: " + dat_path;
        return result;
    }

    // Check if index file already exists (error for build, required for extend)
    bool index_exists = fs::exists(index_path);
    if (!extend_mode && index_exists)
    {
        result.error_message = "Index file already exists: " + index_path;
        return result;
    }
    if (extend_mode && !index_exists)
    {
        result.error_message = "Index file not found for extend: " + index_path;
        return result;
    }

    // Memory-map the dat file
    boost::iostreams::mapped_file_source dat_mmap;
    try
    {
        dat_mmap.open(dat_path);
    }
    catch (const std::exception& e)
    {
        result.error_message = std::string("Failed to mmap dat file: ") + e.what();
        return result;
    }

    if (!dat_mmap.is_open())
    {
        result.error_message = "Failed to open memory-mapped file";
        return result;
    }

    auto const* dat_data = reinterpret_cast<const std::uint8_t*>(dat_mmap.data());
    std::uint64_t file_size = dat_mmap.size();

    // Read dat file header
    if (file_size < nudbview::detail::dat_file_header::size)
    {
        result.error_message = "File too small to contain header";
        return result;
    }

    nudbview::detail::dat_file_header dh;
    nudbview::detail::istream is{dat_data, nudbview::detail::dat_file_header::size};
    nudbview::detail::read(is, dh);

    nudbview::error_code ec;
    nudbview::detail::verify(dh, ec);
    if (ec)
    {
        result.error_message = "Invalid dat file header: " + ec.message();
        return result;
    }

    // Variables for extend mode
    std::uint64_t start_offset = nudbview::detail::dat_file_header::size;
    std::uint64_t start_record_num = 0;
    std::uint64_t existing_total_records = 0;

    // Collect index entries
    std::vector<nudbview::noff_t> offsets;
    offsets.reserve(100000);

    // If extending, read existing index
    if (extend_mode)
    {
        boost::iostreams::mapped_file_source index_mmap;
        try
        {
            index_mmap.open(index_path);
        }
        catch (const std::exception& e)
        {
            result.error_message = std::string("Failed to mmap existing index: ") + e.what();
            return result;
        }

        auto const* index_data = reinterpret_cast<const std::uint8_t*>(index_mmap.data());
        std::uint64_t index_file_size = index_mmap.size();

        if (index_file_size < nudbview::view::index_file_header::size)
        {
            result.error_message = "Existing index file too small";
            return result;
        }

        nudbview::view::index_file_header existing_ifh;
        nudbview::detail::istream ifh_is{index_data, nudbview::view::index_file_header::size};
        nudbview::view::read(ifh_is, existing_ifh);

        nudbview::view::verify(dh, existing_ifh, ec);
        if (ec)
        {
            result.error_message = "Existing index doesn't match dat file: " + ec.message();
            return result;
        }

        if (existing_ifh.index_interval != options.index_interval)
        {
            result.error_message = "Index interval mismatch";
            return result;
        }

        existing_total_records = existing_ifh.total_records;

        // Read existing offsets
        offsets.reserve(existing_ifh.entry_count + 100000);
        std::uint64_t offset_array_offset = nudbview::view::index_file_header::size;

        for (std::uint64_t i = 0; i < existing_ifh.entry_count; ++i)
        {
            if (offset_array_offset + 8 > index_file_size)
            {
                result.error_message = "Index file truncated";
                return result;
            }

            nudbview::detail::istream offset_is{index_data + offset_array_offset, 8};
            nudbview::noff_t offset;
            nudbview::view::read_offset(offset_is, offset);
            offsets.push_back(offset);
            offset_array_offset += 8;
        }

        if (!offsets.empty())
        {
            start_offset = offsets.back();
            start_record_num = (offsets.size() - 1) * options.index_interval;
        }

        index_mmap.close();
    }

    // Scan records
    auto scan_start = std::chrono::high_resolution_clock::now();

    std::uint64_t total_records = scan_dat_records(
        dat_mmap, dh.key_size,
        [&](std::uint64_t record_num, std::uint64_t offset, std::uint64_t /* size */)
        {
            // In extend mode, skip records that were already indexed
            bool should_index = (record_num % options.index_interval == 0);
            if (extend_mode && should_index && record_num < existing_total_records)
            {
                should_index = false;  // Already indexed in original
            }

            if (should_index)
            {
                offsets.push_back(offset);
            }

            if (options.progress_callback)
            {
                options.progress_callback(offset - start_offset, file_size - start_offset);
            }
        },
        start_offset,
        start_record_num);

    auto scan_end = std::chrono::high_resolution_clock::now();
    result.scan_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        scan_end - scan_start).count();

    // CRITICAL: Truncate offsets to only include complete interval boundaries!
    // If we scanned 105 records with interval=26, we collected offsets for: 0, 26, 52, 78, 104
    // But we should only report up to the last complete interval: (105/26)*26 = 104 records (0-103)
    // So we can only keep offsets for records: 0, 26, 52, 78 (NOT 104!)
    std::uint64_t rounded_total_records = (total_records / options.index_interval) * options.index_interval;
    std::size_t expected_entries = rounded_total_records / options.index_interval;

    if (offsets.size() > expected_entries)
    {
        offsets.resize(expected_entries);
    }

    // Write index file
    auto write_start = std::chrono::high_resolution_clock::now();

    // In extend mode, delete old file first
    if (extend_mode && fs::exists(index_path))
    {
        fs::remove(index_path);
    }

    nudbview::native_file f;
    f.create(nudbview::file_mode::write, index_path, ec);
    if (ec)
    {
        result.error_message = "Failed to create index file: " + ec.message();
        return result;
    }

    // Prepare header (write last as commit point)
    nudbview::view::index_file_header ifh;
    ifh.version = nudbview::view::index_file_version;
    ifh.uid = dh.uid;
    ifh.appnum = dh.appnum;
    ifh.key_size = dh.key_size;
    ifh.total_records = total_records;
    ifh.index_interval = options.index_interval;
    ifh.entry_count = offsets.size();

    // Write offsets in batches
    constexpr std::size_t batch_size = 8192;
    std::vector<std::uint8_t> batch_buf(batch_size * 8);

    nudbview::noff_t file_offset = nudbview::view::index_file_header::size;
    std::size_t batch_count = 0;
    std::size_t batch_offset = 0;

    for (auto offset : offsets)
    {
        nudbview::detail::ostream os{batch_buf.data() + batch_offset, 8};
        nudbview::view::write_offset(os, offset);
        batch_offset += 8;
        ++batch_count;

        if (batch_count >= batch_size)
        {
            f.write(file_offset, batch_buf.data(), batch_offset, ec);
            if (ec)
            {
                result.error_message = "Failed to write offset batch: " + ec.message();
                return result;
            }
            file_offset += batch_offset;
            batch_count = 0;
            batch_offset = 0;
        }
    }

    // Flush remaining
    if (batch_count > 0)
    {
        f.write(file_offset, batch_buf.data(), batch_offset, ec);
        if (ec)
        {
            result.error_message = "Failed to write final batch: " + ec.message();
            return result;
        }
    }

    // Write header as commit point
    nudbview::view::write(f, ifh, ec);
    if (ec)
    {
        result.error_message = "Failed to write header: " + ec.message();
        return result;
    }

    f.sync(ec);
    if (ec)
    {
        result.error_message = "Failed to sync: " + ec.message();
        return result;
    }

    auto write_end = std::chrono::high_resolution_clock::now();
    result.write_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        write_end - write_start).count();

    // CRITICAL: Only report complete interval boundaries!
    // We may have scanned additional records beyond the last interval boundary,
    // but since we can only slice at interval boundaries, we should only report
    // records up to the last complete interval.
    result.total_records = (total_records / options.index_interval) * options.index_interval;
    result.entry_count = offsets.size();
    result.success = true;

    return result;
}

bool
IndexBuilder::verify(
    std::string const& dat_path,
    std::string const& index_path,
    nudbview::error_code& ec)
{
    // TODO: Implement verification
    // - Load index
    // - For each offset, scan_dat_records to verify it points to a valid record
    // - Count records and verify matches index header
    return false;
}

bool
IndexBuilder::create_slice_from_index(
    std::string const& dat_path,
    std::string const& index_path,
    std::uint64_t start_record,
    std::uint64_t end_record,
    std::string const& slice_key_path,
    std::string const& slice_meta_path,
    nudbview::error_code& ec)
{
    // Load index
    IndexReader index_reader;
    if (!index_reader.load(index_path, ec))
        return false;

    std::uint64_t interval = index_reader.index_interval();
    std::uint64_t total_records = index_reader.total_records();

    // CRITICAL: Validate boundaries are at interval multiples!
    // You CANNOT slice to arbitrary record numbers on live files.
    if (start_record % interval != 0)
    {
        ec = nudbview::make_error_code(nudbview::error::invalid_key_size);
        return false;
    }

    if (end_record % interval != 0)
    {
        ec = nudbview::make_error_code(nudbview::error::invalid_key_size);
        return false;
    }

    // Validate range is within indexed records
    if (end_record > total_records)
    {
        ec = nudbview::make_error_code(nudbview::error::invalid_key_size);
        return false;
    }

    if (start_record >= end_record)
    {
        ec = nudbview::make_error_code(nudbview::error::invalid_key_size);
        return false;
    }

    // Get exact byte offsets from index (no scanning needed at boundaries!)
    nudbview::noff_t start_offset;
    std::uint64_t records_to_skip_start;
    if (!index_reader.lookup_record(start_record, start_offset, records_to_skip_start))
    {
        ec = nudbview::make_error_code(nudbview::error::short_read);
        return false;
    }

    // Should be zero since we're at a boundary
    if (records_to_skip_start != 0)
    {
        ec = nudbview::make_error_code(nudbview::error::invalid_key_size);
        return false;
    }

    // Get end boundary (start of next record after our range)
    nudbview::noff_t end_boundary;
    std::uint64_t records_to_skip_end;
    if (!index_reader.lookup_record(end_record, end_boundary, records_to_skip_end))
    {
        ec = nudbview::make_error_code(nudbview::error::short_read);
        return false;
    }

    // Should be zero since we're at a boundary
    if (records_to_skip_end != 0)
    {
        ec = nudbview::make_error_code(nudbview::error::invalid_key_size);
        return false;
    }

    // rekey_slice expects [start, end] INCLUSIVE, so subtract 1
    nudbview::noff_t end_offset = end_boundary - 1;

    // Calculate exact record count - we already have this from the index!
    std::uint64_t record_count = end_record - start_record;

    // TODO: Read block_size and load_factor from original .dat file header!
    // These should match the original database settings, not use hardcoded defaults.
    // For now using sensible defaults: 4096 block size, 0.5 load factor

    // Create the slice using low-level rekey_slice
    // OPTIMIZATION: Pass record_count to skip Pass 1 (we already scanned to build the index!)
    nudbview::view::rekey_slice<nudbview::xxhasher, nudbview::native_file>(
        dat_path,
        start_offset,
        end_offset,
        slice_key_path,
        slice_meta_path,
        4096,      // TODO: Read from dat_file_header
        0.5f,      // TODO: Read from key_file_header
        interval,  // Use same interval as the index
        8192,      // buffer_size
        ec,
        [](std::uint64_t, std::uint64_t) {},  // no-op progress callback
        record_count  // Skip counting pass - we know the count!
    );

    return !ec;
}

} // namespace nudbutil
#endif
