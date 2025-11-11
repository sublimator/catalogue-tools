#ifndef NUDBVIEW_IMPL_VIEW_INDEX_BUILDER_IPP
#define NUDBVIEW_IMPL_VIEW_INDEX_BUILDER_IPP

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <chrono>
#include <nudbview/detail/format.hpp>
#include <nudbview/native_file.hpp>
#include <nudbview/view/dat_scanner.hpp>
#include <nudbview/view/index_format.hpp>
#include <nudbview/view/index_reader.hpp>
#include <nudbview/view/rekey_slice.hpp>
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
        result.error_message =
            std::string("Failed to mmap dat file: ") + e.what();
        return result;
    }

    if (!dat_mmap.is_open())
    {
        result.error_message = "Failed to open memory-mapped file";
        return result;
    }

    auto const* dat_data =
        reinterpret_cast<const std::uint8_t*>(dat_mmap.data());
    std::uint64_t file_size = dat_mmap.size();

    // Read dat file header
    if (file_size < nudbview::detail::dat_file_header::size)
    {
        result.error_message = "File too small to contain header";
        return result;
    }

    nudbview::detail::dat_file_header dh;
    nudbview::detail::istream is{
        dat_data, nudbview::detail::dat_file_header::size};
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
            result.error_message =
                std::string("Failed to mmap existing index: ") + e.what();
            return result;
        }

        auto const* index_data =
            reinterpret_cast<const std::uint8_t*>(index_mmap.data());
        std::uint64_t index_file_size = index_mmap.size();

        if (index_file_size < nudbview::view::index_file_header::size)
        {
            result.error_message = "Existing index file too small";
            return result;
        }

        nudbview::view::index_file_header existing_ifh;
        nudbview::detail::istream ifh_is{
            index_data, nudbview::view::index_file_header::size};
        nudbview::view::read(ifh_is, existing_ifh);

        nudbview::view::verify(dh, existing_ifh, ec);
        if (ec)
        {
            result.error_message =
                "Existing index doesn't match dat file: " + ec.message();
            return result;
        }

        if (existing_ifh.index_interval != options.index_interval)
        {
            result.error_message = "Index interval mismatch";
            return result;
        }

        existing_total_records = existing_ifh.total_records_indexed;

        // Read existing offsets
        offsets.reserve(existing_ifh.entry_count + 100000);
        std::uint64_t offset_array_offset =
            nudbview::view::index_file_header::size;

        for (std::uint64_t i = 0; i < existing_ifh.entry_count; ++i)
        {
            if (offset_array_offset + 8 > index_file_size)
            {
                result.error_message = "Index file truncated";
                return result;
            }

            nudbview::detail::istream offset_is{
                index_data + offset_array_offset, 8};
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
        dat_mmap,
        dh.key_size,
        [&](std::uint64_t record_num,
            std::uint64_t offset,
            std::uint64_t /* size */) {
            // In extend mode, skip records that were already indexed
            bool should_index = (record_num % options.index_interval == 0);
            if (extend_mode && should_index &&
                record_num < existing_total_records)
            {
                should_index = false;  // Already indexed in original
            }

            if (should_index)
            {
                offsets.push_back(offset);
            }

            if (options.progress_callback)
            {
                options.progress_callback(
                    offset - start_offset, file_size - start_offset);
            }
        },
        start_offset,
        start_record_num);

    auto scan_end = std::chrono::high_resolution_clock::now();
    result.scan_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              scan_end - scan_start)
                              .count();

    // CRITICAL: Truncate offsets to only include complete interval boundaries!
    // If we scanned 105 records with interval=26, we collected offsets for: 0,
    // 26, 52, 78, 104 But we should only report up to the last complete
    // interval: (105/26)*26 = 104 records (0-103) So we can only keep offsets
    // for records: 0, 26, 52, 78 (NOT 104!)
    std::uint64_t rounded_total_records =
        (total_records / options.index_interval) * options.index_interval;
    std::size_t expected_entries =
        rounded_total_records / options.index_interval;

    if (offsets.size() > expected_entries)
    {
        offsets.resize(expected_entries);
    }

    // CRITICAL: Use the ROUNDED total_records for consistency!
    // We can only index/slice at interval boundaries, so report that
    std::uint64_t rounded_total_records_for_header = rounded_total_records;

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
    ifh.total_records_indexed =
        rounded_total_records_for_header;  // Use ROUNDED count!
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
                result.error_message =
                    "Failed to write offset batch: " + ec.message();
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
            result.error_message =
                "Failed to write final batch: " + ec.message();
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
    result.write_time_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            write_end - write_start)
            .count();

    // Report the same rounded count we wrote to the header
    result.total_records = rounded_total_records_for_header;
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
    std::uint64_t start_record_incl,
    std::uint64_t end_record_excl,
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
    std::uint64_t entry_count = index_reader.entry_count();

    // Calculate the last indexed record number
    // With N entries at interval I, we have entries for records: 0, I, 2I, ...,
    // (N-1)*I The last indexed record is (N-1) * I
    std::uint64_t last_indexed_record =
        (entry_count > 0) ? (entry_count - 1) * interval : 0;
    std::uint64_t max_end_record = last_indexed_record + interval;

    // CRITICAL: Validate boundaries are at interval multiples!
    // You CANNOT slice to arbitrary record numbers on live files.
    if (start_record_incl % interval != 0)
    {
        ec = nudbview::make_error_code(nudbview::error::invalid_slice_boundary);
        return false;
    }

    if (end_record_excl % interval != 0)
    {
        ec = nudbview::make_error_code(nudbview::error::invalid_slice_boundary);
        return false;
    }

    // Validate range
    if (start_record_incl >= end_record_excl)
    {
        ec = nudbview::make_error_code(nudbview::error::invalid_slice_boundary);
        return false;
    }

    // CRITICAL: Can only slice up to last INDEXED record!
    // With N entries, we can lookup records 0, I, 2I, ..., (N-1)*I
    // To slice [start, end), we need to lookup BOTH start and end in the index
    // So end must be <= (N-1)*I + I (one past the last indexed record)
    if (end_record_excl > max_end_record)
    {
        ec = nudbview::make_error_code(nudbview::error::invalid_slice_boundary);
        return false;
    }

    // Also check against total_records (though max_end_record check is
    // stricter)
    if (end_record_excl > total_records)
    {
        ec = nudbview::make_error_code(nudbview::error::invalid_slice_boundary);
        return false;
    }

    // Get exact byte offsets from index (no scanning needed at boundaries!)
    nudbview::noff_t start_offset;
    std::uint64_t records_to_skip_start;
    if (!index_reader.lookup_record_start_offset(
            start_record_incl, start_offset, records_to_skip_start))
    {
        ec = nudbview::make_error_code(nudbview::error::short_read);
        return false;
    }

    // Should be zero since we're at a boundary
    if (records_to_skip_start != 0)
    {
        ec = nudbview::make_error_code(nudbview::error::invalid_slice_boundary);
        return false;
    }

    // Get end boundary
    nudbview::noff_t end_offset;

    // Special case: if slicing to total_records (end of indexed range), find
    // actual end
    if (end_record_excl == total_records)
    {
        // We need to find the END of the last indexed record
        // Get the last index entry position
        std::uint64_t last_entry = entry_count - 1;
        std::uint64_t last_entry_record = last_entry * interval;

        // Get offset of the last index entry
        nudbview::noff_t last_entry_offset;
        std::uint64_t dummy;
        if (!index_reader.lookup_record_start_offset(
                last_entry_record, last_entry_offset, dummy))
        {
            ec = nudbview::make_error_code(
                nudbview::error::invalid_slice_boundary);
            return false;
        }

        // Now scan forward to find the end of the last record in the interval
        boost::iostreams::mapped_file_source dat_mmap(dat_path);

        // Read dat header to get key_size
        auto const* dat_data =
            reinterpret_cast<const std::uint8_t*>(dat_mmap.data());
        nudbview::detail::dat_file_header dh;
        std::memcpy(&dh, dat_data, sizeof(dh));

        // Use dat_scanner to scan the last interval and find the last record
        nudbview::noff_t last_record_end = last_entry_offset;
        std::uint64_t records_found = 0;

        // We want records [0, end_record_excl) so the LAST record is
        // end_record_excl - 1
        std::uint64_t last_record_we_want = end_record_excl - 1;

        nudbutil::scan_dat_records(
            dat_mmap,
            dh.key_size,
            [&](std::uint64_t record_num,
                std::uint64_t offset,
                std::uint64_t size) {
                // We only care about records in our last interval
                if (record_num < last_entry_record)
                    return;  // Skip records before our interval

                if (record_num > last_record_we_want)
                    return;  // Stop - we've gone past the last record we want

                // Keep updating until we find the END of the last record we
                // want
                std::uint64_t this_end;
                if (nudbutil::get_record_end_offset_incl(
                        dat_mmap, dh.key_size, offset, this_end))
                {
                    last_record_end = this_end;  // Update to this record's end
                    records_found++;

                    // If we found the exact record we want, we can stop
                    if (record_num == last_record_we_want)
                        return;
                }
            },
            last_entry_offset,
            last_entry_record);

        if (records_found == 0)
        {
            // No records found in the last interval?
            ec = nudbview::make_error_code(
                nudbview::error::invalid_slice_boundary);
            return false;
        }

        end_offset =
            last_record_end;  // Already inclusive from get_record_end_offset
    }
    else
    {
        // Normal case: We want records [0, end_record_excl) so last record is
        // end_record_excl - 1 We need to find the END of that last record
        std::uint64_t last_record_we_want = end_record_excl - 1;

        // Find which interval contains this record
        std::uint64_t interval_for_last =
            (last_record_we_want / interval) * interval;

        nudbview::noff_t interval_offset;
        std::uint64_t records_to_skip;
        if (!index_reader.lookup_record_start_offset(
                interval_for_last, interval_offset, records_to_skip))
        {
            ec = nudbview::make_error_code(
                nudbview::error::invalid_slice_boundary);
            return false;
        }

        // Now scan forward to find the END of last_record_we_want
        boost::iostreams::mapped_file_source dat_mmap(dat_path);
        auto const* dat_data =
            reinterpret_cast<const std::uint8_t*>(dat_mmap.data());
        nudbview::detail::dat_file_header dh;
        std::memcpy(&dh, dat_data, sizeof(dh));

        nudbview::noff_t last_record_end = interval_offset;

        nudbutil::scan_dat_records(
            dat_mmap,
            dh.key_size,
            [&](std::uint64_t record_num,
                std::uint64_t offset,
                std::uint64_t size) {
                if (record_num > last_record_we_want)
                    return;  // Stop - we've gone past the last record we want

                // Update the end offset for each record until we hit our target
                std::uint64_t this_end;
                if (nudbutil::get_record_end_offset_incl(
                        dat_mmap, dh.key_size, offset, this_end))
                {
                    last_record_end = this_end;

                    if (record_num == last_record_we_want)
                        return;  // Found it!
                }
            },
            interval_offset,
            interval_for_last);

        end_offset = last_record_end;  // Already inclusive
    }

    // Now we have the correct byte range [start_offset, end_offset] that
    // contains exactly the records we want [start_record_incl, end_record_excl)

    // Read block_size and salt from key file
    std::uint64_t salt = 1;
    float load_factor = 0.5f;
    std::size_t block_size = 4096;

    {
        // Need to get salt from the .key file for proper hashing
        std::string key_path = dat_path;
        size_t pos = key_path.rfind(".dat");
        if (pos != std::string::npos)
        {
            key_path.replace(pos, 4, ".key");
        }

        nudbview::native_file kf;
        nudbview::error_code kf_ec;
        kf.open(nudbview::file_mode::read, key_path, kf_ec);
        if (!kf_ec)
        {
            nudbview::detail::key_file_header kfh;
            nudbview::detail::read(kf, kfh, kf_ec);
            if (!kf_ec)
            {
                salt = kfh.salt;
                // load_factor is stored as uint16_t normalized to [0, 65536]
                load_factor = static_cast<float>(kfh.load_factor) / 65536.0f;
                block_size = kfh.block_size;
            }
            kf.close();
        }
    }

    // Create the slice using low-level rekey_slice
    // DON'T pass expected record count - let it scan and count actual records
    // in the range
    nudbview::view::rekey_slice<nudbview::xxhasher, nudbview::native_file>(
        dat_path,
        start_offset,
        end_offset,
        slice_key_path,
        slice_meta_path,
        block_size,
        load_factor,
        interval,  // Use same interval as the index
        8192,      // buffer_size
        ec,
        [](std::uint64_t, std::uint64_t) {}  // no-op progress callback
        // NO expected_record_count - let it scan!
    );

    return !ec;
}

}  // namespace nudbutil
#endif
