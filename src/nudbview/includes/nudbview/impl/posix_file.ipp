//
// Copyright (c) 2015-2016 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef NUDB_IMPL_POSIX_FILE_IPP
#define NUDB_IMPL_POSIX_FILE_IPP

#include <boost/assert.hpp>
#include <algorithm>
#include <cstdlib>
#include <limits.h>

namespace nudbview {

inline
posix_file::
~posix_file()
{
    close();
}

inline
posix_file::
posix_file(posix_file &&other)
    : fd_(other.fd_)
{
    std::lock_guard<std::mutex> l{other.remap_mutex_};
    maps_ = std::move(other.maps_);
    map_.store(other.map_.exchange(nullptr));
    other.fd_ = -1;
}

inline
posix_file&
posix_file::
operator=(posix_file&& other)
{
    if(&other == this)
        return *this;
    close();
    std::lock_guard<std::mutex> l{other.remap_mutex_};
    fd_ = other.fd_;
    maps_ = std::move(other.maps_);
    map_.store(other.map_.exchange(nullptr));
    other.fd_ = -1;
    return *this;
}

inline
void
posix_file::
drop_mappings()
{
    // Callers guarantee no concurrent reader: close and trunc are not
    // thread safe with respect to reads by contract.
    map_.store(nullptr);
    known_size_.store(0);
    std::lock_guard<std::mutex> l{remap_mutex_};
    maps_.clear();
}

inline
void
posix_file::
close()
{
    drop_mappings();
    if(fd_ != -1)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

inline
posix_file::mapping const*
posix_file::
remap() const
{
    std::lock_guard<std::mutex> l{remap_mutex_};
    // Another reader may have remapped while we waited.
    struct stat st;
    if(::fstat(fd_, &st) != 0 || st.st_size <= 0)
        return nullptr;
    auto const size = static_cast<std::size_t>(st.st_size);
    auto const* cur = map_.load();
    if(cur && cur->len >= size)
        return cur;
    // Map with headroom so a file that grows a block at a time, as the
    // key file does on every bucket split, remaps logarithmically rather
    // than per block. Pages past the end of the file are never touched:
    // the store only reads what it has written.
    //
    // TODO(nudbview): doubling is uncapped and superseded mappings are
    // kept until close. At a terabyte that reserves twice the file in
    // address space and retains every earlier mapping. Cap the headroom
    // at a few GB and retire old mappings once no reader can hold them,
    // using the store's generation drain rather than a lock per read.
    auto len = cur ? std::max(size, cur->len * 2) : size;
    len = std::max<std::size_t>(len, std::size_t{16} << 20);
    void* const p = ::mmap(nullptr, len, PROT_READ, MAP_SHARED, fd_, 0);
    if(p == MAP_FAILED)
        return nullptr;
    maps_.push_back(std::make_unique<mapping const>(p, len));
    auto const* m = maps_.back().get();
    map_.store(m);
    return m;
}

inline
void
posix_file::
create(file_mode mode, path_type const& path, error_code& ec)
{
    auto const result = flags(mode);
    BOOST_ASSERT(! is_open());
    fd_ = ::open(path.c_str(), result.first | O_CREAT | O_EXCL, 0644);
    if(fd_ == -1)
        return last_err(ec);
#ifndef __APPLE__
    if(::posix_fadvise(fd_, 0, 0, result.second) != 0)
        return last_err(ec);
#endif
}

inline
void
posix_file::
open(file_mode mode, path_type const& path, error_code& ec)
{
    BOOST_ASSERT(! is_open());
    auto const result = flags(mode);
    fd_ = ::open(path.c_str(), result.first);
    if(fd_ == -1)
        return last_err(ec);
#ifndef __APPLE__
    if(::posix_fadvise(fd_, 0, 0, result.second) != 0)
        return last_err(ec);
#endif
}

inline
void
posix_file::
erase(path_type const& path, error_code& ec)
{
    if(::unlink(path.c_str()) != 0)
    {
        int const ev = errno;
        return err(ev, ec);
    }
}

inline
std::uint64_t
posix_file::
size(error_code& ec) const
{
    static_assert(sizeof(stat::st_size) == sizeof(std::uint64_t), "");
    struct stat st;
    if(::fstat(fd_, &st) != 0)
    {
        last_err(ec);
        return 0;
    }
    return st.st_size;
}
inline
void
posix_file::
read(std::uint64_t offset,
     void* buffer, std::size_t bytes, error_code& ec)
{
    static_assert(sizeof(off_t) >= sizeof(offset), "");
    if(bytes == 0)
        return;
    // Mapped fast path: copy when the range is inside the mapping,
    // refreshing the mapping once if the file has grown past it.
    // NUDBVIEW_NO_MMAP=1 in the environment disables it, for A/B runs.
    static bool const no_mmap = std::getenv("NUDBVIEW_NO_MMAP") != nullptr;
    if(! no_mmap)
    {
        auto const* m = map_.load(std::memory_order_acquire);
        if(! m || offset + bytes > m->len)
            m = remap();
        if(m && offset + bytes <= m->len)
        {
            // Headroom past the file's end would fault; a read there is
            // a caller error and gets pread's short_read instead.
            if(offset + bytes > known_size_.load(std::memory_order_acquire))
            {
                struct stat st;
                if(::fstat(fd_, &st) == 0)
                    known_size_.store(static_cast<std::uint64_t>(st.st_size),
                        std::memory_order_release);
            }
            if(offset + bytes <= known_size_.load(std::memory_order_acquire))
            {
                std::memcpy(buffer,
                    static_cast<char const*>(m->p) + offset, bytes);
                return;
            }
        }
    }
    while(bytes > 0)
    {
        auto const amount = static_cast<ssize_t>(
            std::min(bytes, static_cast<std::size_t>(SSIZE_MAX)));
        auto const n = ::pread(fd_, buffer, amount, offset);
        if(n == -1)
        {
            auto const ev = errno;
            if(ev == EINTR)
                continue;
            return err(ev, ec);
        }
        if(n == 0)
        {
            ec = error::short_read;
            return;
        }
        offset += n;
        bytes -= n;
        buffer = reinterpret_cast<char*>(buffer) + n;
    }
}

inline
void
posix_file::
write(std::uint64_t offset,
      void const* buffer, std::size_t bytes, error_code& ec)
{
    static_assert(sizeof(off_t) >= sizeof(offset), "");
    while(bytes > 0)
    {
        auto const amount = static_cast<ssize_t>(
            std::min(bytes, static_cast<std::size_t>(SSIZE_MAX)));
        auto const n = ::pwrite(fd_, buffer, amount, offset);
        if(n == -1)
        {
            auto const ev = errno;
            if(ev == EINTR)
                continue;
            return err(ev, ec);
        }
        offset += n;
        bytes -= n;
        buffer = reinterpret_cast<char const*>(buffer) + n;
    }
}

inline
void
posix_file::
sync(error_code& ec)
{
    for(;;)
    {
        if(::fsync(fd_) == 0)
            break;
        auto const ev = errno;
        if(ev == EINTR)
            continue;
        return err(ev, ec);
    }
}

inline
void
posix_file::
trunc(std::uint64_t length, error_code& ec)
{
    // A mapping longer than the file would fault past the new end.
    drop_mappings();
    for(;;)
    {
        if(::ftruncate(fd_, length) == 0)
            break;
        auto const ev = errno;
        if(ev == EINTR)
            continue;
        return err(ev, ec);
    }
}

inline
std::pair<int, int>
posix_file::
flags(file_mode mode)
{
    std::pair<int, int> result;
    switch(mode)
    {
    case file_mode::scan:
        result.first =
            O_RDONLY;
    #ifndef __APPLE__
        result.second =
            POSIX_FADV_SEQUENTIAL;
    #endif
        break;
    case file_mode::read:
        result.first =
            O_RDONLY;
    #ifndef __APPLE__
        result.second =
            POSIX_FADV_RANDOM;
    #endif
        break;
    case file_mode::append:
        result.first =
            O_RDWR |
            O_APPEND;
    #ifndef __APPLE__
        result.second =
            POSIX_FADV_RANDOM;
    #endif
        break;
    case file_mode::write:
        result.first =
            O_RDWR;
    #ifndef __APPLE__
        result.second =
            POSIX_FADV_NORMAL;
    #endif
        break;
    }
    return result;
}

} // nudb

#endif
