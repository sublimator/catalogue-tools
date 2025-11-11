//
// Slice Rekey Implementation
// Part of nudbview/view - Read-only slice database implementation
//

#ifndef NUDBVIEW_IMPL_VIEW_SLICE_REKEY_IPP
#define NUDBVIEW_IMPL_VIEW_SLICE_REKEY_IPP

#include <nudbview/concepts.hpp>
#include <nudbview/create.hpp>
#include <nudbview/detail/bucket.hpp>
#include <nudbview/detail/bulkio.hpp>
#include <nudbview/detail/format.hpp>
#include <nudbview/view/format.hpp>
#include <nudbview/view/dat_scanner.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <cmath>
#include <vector>

namespace nudbview {
namespace view {

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
    std::uint64_t expected_record_count,
    Args&&... args)
{
    static_assert(is_File<File>::value,
        "File requirements not met");
    static_assert(is_Hasher<Hasher>::value,
        "Hasher requirements not met");
    static_assert(is_Progress<Progress>::value,
        "Progress requirements not met");

    using namespace detail;

    // Validate inputs
    if(end_offset <= start_offset)
    {
        ec = error::slice_invalid_range;
        return;
    }
    if(index_interval < 1)
    {
        ec = error::slice_invalid_interval;
        return;
    }

    auto const writeSize = 16 * blockSize;

    // Open data file briefly to read header
    // (we'll use mmap for actual data reading)
    File df{args...};
    df.open(file_mode::read, dat_path, ec);
    if(ec)
        return;

    // Read and verify dat file header
    dat_file_header dh;
    read(df, dh, ec);
    if(ec)
        return;
    verify(dh, ec);
    if(ec)
        return;

    auto const dataFileSize = df.size(ec);
    if(ec)
        return;

    // Close file - we'll use mmap for reading data
    df.close();

    // Validate slice boundaries
    if(start_offset < dat_file_header::size)
    {
        ec = error::slice_start_before_header;
        return;
    }
    if(end_offset >= dataFileSize)
    {
        ec = error::slice_end_exceeds_file;
        return;
    }

    // =========================================================================
    // PASS 1: Scan slice range to count keys and collect index offsets
    // =========================================================================
    //
    // OPTIMIZATION: Skip if expected_record_count provided
    // -----------------------------------------------------
    // If caller already has an index and knows the exact record count (common case
    // when using IndexBuilder), they can pass expected_record_count to skip this pass.
    // We'll validate the count during Pass 2.
    //
    // IMPORTANT: Handling partial records at tail (live .dat files)
    // --------------------------------------------------------------
    // We use memory-mapped I/O (mmap) to scan the .dat file, same approach as
    // IndexBuilder. If end_offset points into a PARTIAL record (incomplete write),
    // scan_dat_records will stop early when it detects the record extends beyond
    // end_offset.
    //
    // The caller is responsible for ensuring end_offset points to a complete
    // record boundary. Use IndexBuilder to scan the file first and determine
    // safe bounds. If the scan stops early, your end_offset is wrong!
    //
    // For stress testing concurrent writes, see tests/nudbview/slice-stress-gtest.cpp

    std::uint64_t itemCount = 0;
    std::vector<index_entry> index_entries;

    // Memory-map the dat file
    boost::iostreams::mapped_file_source dat_mmap;
    try
    {
        dat_mmap.open(dat_path);
    }
    catch (const std::exception& e)
    {
        ec = error::short_read;  // VFALCO: Need better error for mmap failure
        return;
    }

    if (!dat_mmap.is_open())
    {
        ec = error::short_read;  // VFALCO: Need better error for mmap failure
        return;
    }

    auto const* dat_data = reinterpret_cast<const std::uint8_t*>(dat_mmap.data());
    std::uint64_t dat_file_size = dat_mmap.size();

    // Validate end_offset doesn't exceed file
    if (end_offset >= dat_file_size)
    {
        ec = error::slice_end_exceeds_file;
        return;
    }

    progress(0, 2 * (end_offset - start_offset));  // 2 passes total

    if (expected_record_count > 0)
    {
        // OPTIMIZATION: Skip counting pass, use provided count
        itemCount = expected_record_count;

        // Pre-allocate index entries based on expected count
        std::uint64_t expected_entries = (itemCount + index_interval - 1) / index_interval;
        index_entries.reserve(expected_entries);
    }
    else
    {
        // Scan records using mmap to count and build index
        itemCount = nudbutil::scan_dat_records(
            dat_mmap,
            dh.key_size,
            [&](std::uint64_t record_num, std::uint64_t offset, std::uint64_t size) {
                // Check if we've passed the end
                if (offset > end_offset)
                    return;

                // Record index entry if this is an interval boundary
                if (record_num % index_interval == 0)
                {
                    index_entry ie;
                    ie.record_number = record_num;
                    ie.dat_offset = offset;
                    index_entries.push_back(ie);
                }

                progress(offset - start_offset, 2 * (end_offset - start_offset));
            },
            start_offset,
            0
        );
    }

    if(itemCount == 0)
    {
        ec = error::slice_empty;
        return;
    }

    // =========================================================================
    // Set up key file header
    // =========================================================================

    key_file_header kh;
    kh.version = currentVersion;
    kh.uid = dh.uid;
    kh.appnum = dh.appnum;
    kh.key_size = dh.key_size;
    kh.salt = make_salt();
    kh.pepper = pepper<Hasher>(kh.salt);
    kh.block_size = blockSize;
    kh.load_factor = std::min<std::size_t>(
        static_cast<std::size_t>(65536.0f * loadFactor), 65535);
    kh.buckets = static_cast<std::size_t>(
        std::ceil(itemCount / (
            bucket_capacity(kh.block_size) * loadFactor)));
    kh.modulus = ceil_pow2(kh.buckets);
    kh.capacity = bucket_capacity(kh.block_size);

    // =========================================================================
    // Create key file
    // =========================================================================

    File kf{args...};
    kf.create(file_mode::write, slice_key_path, ec);
    if(ec)
        return;

    // Write key file header
    {
        std::array<std::uint8_t, key_file_header::size> buf;
        ostream os{buf.data(), buf.size()};
        write(os, kh);
        kf.write(0, buf.data(), buf.size(), ec);
        if(ec)
            return;
        kf.sync(ec);
        if(ec)
            return;
    }

    // Pre-allocate space for the entire key file
    buffer buf{kh.block_size};
    {
        std::memset(buf.get(), 0, kh.block_size);
        ostream os{buf.get(), kh.block_size};
        write(os, kh);
        kf.write(0, buf.get(), buf.size(), ec);
        if(ec)
            return;
        kf.sync(ec);
        if(ec)
            return;

        // Pre-allocate full key file
        std::uint8_t zero = 0;
        kf.write(
            static_cast<noff_t>(kh.buckets + 1) * kh.block_size - 1,
                &zero, 1, ec);
        if(ec)
            return;
        kf.sync(ec);
        if(ec)
            return;
    }

    // =========================================================================
    // Create meta file
    // =========================================================================

    File mf{args...};
    mf.create(file_mode::write, slice_meta_path, ec);
    if(ec)
        return;

    // Prepare meta header (we'll update it later with spill info)
    slice_meta_header smh;
    smh.version = slice_meta_version;
    smh.uid = dh.uid;
    smh.appnum = dh.appnum;
    smh.key_size = dh.key_size;
    smh.slice_start_offset = start_offset;
    smh.slice_end_offset = end_offset;
    smh.key_count = itemCount;
    smh.index_interval = index_interval;
    smh.index_count = index_entries.size();
    smh.index_section_offset = slice_meta_header::size;
    smh.spill_section_offset = slice_meta_header::size +
        index_entries.size() * index_entry::size;
    smh.spill_count = 0;  // Will update after pass 2

    // Write initial meta header
    write(mf, smh, ec);
    if(ec)
        return;

    // Write index section
    {
        noff_t offset = smh.index_section_offset;
        for(auto const& ie : index_entries)
        {
            std::array<std::uint8_t, index_entry::size> ie_buf;
            ostream os{ie_buf};
            write(os, ie);
            mf.write(offset, ie_buf.data(), ie_buf.size(), ec);
            if(ec)
                return;
            offset += index_entry::size;
        }
        mf.sync(ec);
        if(ec)
            return;
    }

    // =========================================================================
    // PASS 2: Build key file buckets and write spills to meta file
    // =========================================================================

    auto const chunkSize = std::max<std::size_t>(1,
        bufferSize / kh.block_size);

    buf.reserve(chunkSize * kh.block_size);

    // Bulk writer for spills (writes to meta file, not dat file!)
    bulk_writer<File> mw{mf, smh.spill_section_offset, writeSize};
    std::uint64_t spill_count = 0;

    for(nbuck_t b0 = 0; b0 < kh.buckets; b0 += chunkSize)
    {
        auto const b1 = std::min<std::size_t>(b0 + chunkSize, kh.buckets);
        auto const bn = b1 - b0;

        // Create empty buckets
        for(std::size_t i = 0; i < bn; ++i)
            bucket b{kh.block_size,
                buf.get() + i * kh.block_size, empty};

        // Insert all keys into buckets using mmap
        // If expected_record_count was provided, we also validate the actual count
        std::uint64_t actual_count = 0;

        nudbutil::scan_dat_records(
            dat_mmap,
            dh.key_size,
            [&](std::uint64_t record_num, std::uint64_t record_offset, std::uint64_t size) {
                if (record_offset > end_offset)
                    return;

                // Count records if we need to validate
                if (expected_record_count > 0)
                    ++actual_count;

                progress((end_offset - start_offset) +
                    (b0 / chunkSize) * (end_offset - start_offset) /
                    ((kh.buckets + chunkSize - 1) / chunkSize),
                    2 * (end_offset - start_offset));

                // Read key directly from mmap (skip 6-byte size field)
                std::uint8_t const* const key = dat_data + record_offset + field<uint48_t>::size;

                auto const h = hash<Hasher>(key, dh.key_size, kh.salt);
                auto const n = bucket_index(h, kh.buckets, kh.modulus);

                if(n < b0 || n >= b1)
                    return;

                // Collect index entries if we skipped Pass 1
                if (expected_record_count > 0 && record_num % index_interval == 0)
                {
                    index_entry ie;
                    ie.record_number = record_num;
                    ie.dat_offset = record_offset;
                    index_entries.push_back(ie);
                }

                bucket b{kh.block_size, buf.get() + (n - b0) * kh.block_size};

                // Check if bucket is full - need to spill
                if(b.size() >= kh.capacity)
                {
                    // Write spill record to meta file
                    auto os = mw.prepare(
                        field<uint48_t>::size +     // Size = 0
                        field<std::uint16_t>::size + // Bucket size
                        b.actual_size(), ec);
                    if(ec)
                        return;

                    write<uint48_t>(os, 0);  // Spill marker
                    write<std::uint16_t>(os, b.actual_size());
                    b.write(os);

                    ++spill_count;

                    // Clear bucket and set spill pointer
                    b = bucket{kh.block_size, buf.get() + (n - b0) * kh.block_size, empty};
                    b.spill(mw.offset() - b.actual_size() -
                        field<uint48_t>::size - field<std::uint16_t>::size);
                }

                b.insert(record_offset, size, h);
            },
            start_offset,
            0
        );

        // CRITICAL: Validate expected count matches actual
        if (expected_record_count > 0 && actual_count != expected_record_count)
        {
            ec = error::slice_record_count_mismatch;
            return;
        }

        // Check for errors that may have occurred in the callback
        if (ec)
            return;

        // Write buckets to key file
        kf.write((b0 + 1) * kh.block_size, buf.get(),
            static_cast<std::size_t>(bn * kh.block_size), ec);
        if(ec)
            return;
    }

    // Flush any remaining spills
    mw.flush(ec);
    if(ec)
        return;

    // =========================================================================
    // Finalize meta file with updated spill count
    // =========================================================================

    smh.spill_count = spill_count;
    write(mf, smh, ec);
    if(ec)
        return;

    // Sync everything
    kf.sync(ec);
    if(ec)
        return;

    mf.sync(ec);
    if(ec)
        return;

    progress(2 * (end_offset - start_offset), 2 * (end_offset - start_offset));
}

} // view
} // nudbview

#endif
