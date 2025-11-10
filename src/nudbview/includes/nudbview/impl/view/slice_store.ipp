//
// Slice Store Implementation
// Part of nudbview/view - Read-only slice database implementation
//

#ifndef NUDBVIEW_IMPL_VIEW_SLICE_STORE_IPP
#define NUDBVIEW_IMPL_VIEW_SLICE_STORE_IPP

#include <nudbview/concepts.hpp>
#include <boost/assert.hpp>
#include <cstring>

namespace nudbview {
namespace view {

template<class Hasher, class File>
slice_store<Hasher, File>::
~slice_store()
{
    error_code ec;
    close(ec);
}

template<class Hasher, class File>
path_type const&
slice_store<Hasher, File>::
dat_path() const
{
    BOOST_ASSERT(is_open());
    return dat_path_;
}

template<class Hasher, class File>
path_type const&
slice_store<Hasher, File>::
key_path() const
{
    BOOST_ASSERT(is_open());
    return key_path_;
}

template<class Hasher, class File>
path_type const&
slice_store<Hasher, File>::
meta_path() const
{
    BOOST_ASSERT(is_open());
    return meta_path_;
}

template<class Hasher, class File>
std::uint64_t
slice_store<Hasher, File>::
appnum() const
{
    BOOST_ASSERT(is_open());
    return dh_.appnum;
}

template<class Hasher, class File>
std::size_t
slice_store<Hasher, File>::
key_size() const
{
    BOOST_ASSERT(is_open());
    return dh_.key_size;
}

template<class Hasher, class File>
std::size_t
slice_store<Hasher, File>::
block_size() const
{
    BOOST_ASSERT(is_open());
    return kh_.block_size;
}

template<class Hasher, class File>
std::uint64_t
slice_store<Hasher, File>::
key_count() const
{
    BOOST_ASSERT(is_open());
    return smh_.key_count;
}

template<class Hasher, class File>
noff_t
slice_store<Hasher, File>::
slice_start_offset() const
{
    BOOST_ASSERT(is_open());
    return smh_.slice_start_offset;
}

template<class Hasher, class File>
noff_t
slice_store<Hasher, File>::
slice_end_offset() const
{
    BOOST_ASSERT(is_open());
    return smh_.slice_end_offset;
}

template<class Hasher, class File>
template<class... Args>
void
slice_store<Hasher, File>::
open(
    path_type const& dat_path,
    path_type const& slice_key_path,
    path_type const& slice_meta_path,
    error_code& ec,
    Args&&... args)
{
    static_assert(is_Hasher<Hasher>::value,
        "Hasher requirements not met");
    static_assert(is_File<File>::value,
        "File requirements not met");

    using namespace detail;

    BOOST_ASSERT(! is_open());

    // Save paths
    dat_path_ = dat_path;
    key_path_ = slice_key_path;
    meta_path_ = slice_meta_path;

    // Open .dat file with mmap (read-only)
    try
    {
        dat_mmap_.open(dat_path);
    }
    catch(std::exception const& e)
    {
        ec = error::short_read;  // File could not be opened
        return;
    }

    if(!dat_mmap_.is_open())
    {
        ec = error::short_read;  // File could not be opened
        return;
    }

    // Read dat file header from mmap
    if(dat_mmap_.size() < dat_file_header::size)
    {
        ec = error::short_read;
        return;
    }

    {
        istream is{dat_mmap_.data(), dat_file_header::size};
        read(is, dh_);
    }

    verify(dh_, ec);
    if(ec)
        return;

    // Open slice key file
    kf_ = File{args...};
    kf_.open(file_mode::read, slice_key_path, ec);
    if(ec)
        return;

    read(kf_, kh_, ec);
    if(ec)
        return;

    verify<Hasher>(kh_, ec);
    if(ec)
        return;

    // Open .meta file with mmap (read-only)
    try
    {
        meta_mmap_.open(slice_meta_path);
    }
    catch(std::exception const& e)
    {
        ec = error::short_read;  // File could not be opened
        return;
    }

    if(!meta_mmap_.is_open())
    {
        ec = error::short_read;  // File could not be opened
        return;
    }

    // Read meta file header from mmap
    if(meta_mmap_.size() < slice_meta_header::size)
    {
        ec = error::short_read;
        return;
    }

    {
        istream is{meta_mmap_.data(), slice_meta_header::size};
        read(is, smh_);
    }

    verify(smh_, ec);
    if(ec)
        return;

    // Verify all headers match
    verify(dh_, smh_, ec);
    if(ec)
        return;

    verify(kh_, smh_, ec);
    if(ec)
        return;

    // Initialize hasher with salt from key file
    hasher_ = Hasher{kh_.salt};

    open_ = true;
}

template<class Hasher, class File>
void
slice_store<Hasher, File>::
close(error_code& ec)
{
    if(open_)
    {
        open_ = false;

        // Close mmap files
        if(dat_mmap_.is_open())
            dat_mmap_.close();
        if(meta_mmap_.is_open())
            meta_mmap_.close();

        // Close key file
        kf_.close();
    }
    ec = {};
}

template<class Hasher, class File>
template<class Callback>
void
slice_store<Hasher, File>::
fetch(
    void const* key,
    Callback&& callback,
    error_code& ec)
{
    using namespace detail;

    BOOST_ASSERT(is_open());

    // Hash the key
    auto const h = hash(key, dh_.key_size, hasher_);

    // Find bucket in key file
    auto const n = bucket_index(h, kh_.buckets, kh_.modulus);

    // Read bucket from key file
    buffer buf{kh_.block_size};
    bucket b{kh_.block_size, buf.get()};
    b.read(kf_, (n + 1) * kh_.block_size, ec);
    if(ec)
        return;

    // Search in bucket and spills
    fetch(h, key, b, callback, ec);
}

// Fetch key in loaded bucket b or its spills
template<class Hasher, class File>
template<class Callback>
void
slice_store<Hasher, File>::
fetch(
    detail::nhash_t h,
    void const* key,
    detail::bucket b,
    Callback&& callback,
    error_code& ec)
{
    using namespace detail;

    buffer buf0;
    buffer buf1;

    for(;;)
    {
        // Search bucket for matching hash
        for(auto i = b.lower_bound(h); i < b.size(); ++i)
        {
            auto const item = b[i];
            if(item.hash != h)
                break;

            // Found matching hash - check if key matches
            // Read from mmap'd .dat file (zero-copy!)
            noff_t const record_offset = item.offset;

            // Validate offset is in bounds
            if(record_offset < smh_.slice_start_offset ||
               record_offset > smh_.slice_end_offset)
            {
                // This shouldn't happen - indicates corruption
                ec = error::invalid_key_size;  // VFALCO: Better error
                return;
            }

            // Calculate offset in mmap
            noff_t const mmap_offset = record_offset;
            if(mmap_offset + field<uint48_t>::size + dh_.key_size > dat_mmap_.size())
            {
                ec = error::short_read;
                return;
            }

            // Read record from mmap (skip size field, read key + value)
            auto const* data_ptr = reinterpret_cast<std::uint8_t const*>(
                dat_mmap_.data()) + mmap_offset + field<uint48_t>::size;

            // Check if key matches
            if(std::memcmp(data_ptr, key, dh_.key_size) == 0)
            {
                // Key matches! Return value to callback
                callback(data_ptr + dh_.key_size, item.size);
                return;
            }
        }

        // Check for spill
        auto const spill = b.spill();
        if(! spill)
            break;

        // Spill records are in the META file, not DAT file!
        // Read spill bucket from meta mmap

        // Spill offset is relative to start of spill section
        noff_t const spill_offset = spill;

        // Validate spill offset
        if(spill_offset + field<uint48_t>::size + field<std::uint16_t>::size >
           meta_mmap_.size())
        {
            ec = error::short_read;
            return;
        }

        // Read spill record from meta mmap
        auto const* spill_ptr = reinterpret_cast<std::uint8_t const*>(
            meta_mmap_.data()) + spill_offset;

        // Read spill header (size = 0, bucket_size)
        istream is{spill_ptr,
            meta_mmap_.size() - spill_offset};

        nsize_t spill_size;
        read_size48(is, spill_size);
        if(spill_size != 0)
        {
            // Not a spill record!
            ec = error::invalid_key_size;  // VFALCO: Better error
            return;
        }

        std::uint16_t bucket_size;
        read<std::uint16_t>(is, bucket_size);

        // Load spill bucket
        buf1.reserve(bucket_size);
        std::memcpy(buf1.get(), is.data(bucket_size), bucket_size);

        b = bucket{kh_.block_size, buf1.get()};
    }

    // Key not found
    ec = error::key_not_found;
}

} // view
} // nudbview

#endif
