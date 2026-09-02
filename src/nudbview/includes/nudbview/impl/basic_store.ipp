//
// Copyright (c) 2015-2016 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef NUDB_IMPL_BASIC_STORE_IPP
#define NUDB_IMPL_BASIC_STORE_IPP

#include <nudbview/concepts.hpp>
#include <nudbview/recover.hpp>
#include <boost/assert.hpp>
#include <cmath>
#include <memory>
#include <thread>

#ifndef NUDB_DEBUG_LOG
#define NUDB_DEBUG_LOG 0
#endif
#if NUDB_DEBUG_LOG
#include <boost/beast/_experimental/unit_test/dstream.hpp>
#include <iostream>
#endif

namespace nudbview {

template<class Hasher, class File>
basic_store<Hasher, File>::state::
state(File&& df_, File&& kf_, File&& lf_,
    path_type const& dp_, path_type const& kp_,
        path_type const& lp_,
            detail::key_file_header const& kh_)
    : df(std::move(df_))
    , kf(std::move(kf_))
    , lf(std::move(lf_))
    , dp(dp_)
    , kp(kp_)
    , lp(lp_)
    , hasher(kh_.salt)
    , kh(kh_)
{
    static_assert(is_File<File>::value,
        "File requirements not met");
    p0.reserve(nshards);
    p1.reserve(nshards);
    for(std::size_t i = 0; i < nshards; ++i)
    {
        p0.emplace_back(kh_.key_size, "p0");
        p1.emplace_back(kh_.key_size, "p1");
    }
}

//------------------------------------------------------------------------------

template<class Hasher, class File>
basic_store<Hasher, File>::
~basic_store()
{
    error_code ec;
    // We call close here to make sure data is intact
    // if an exception destroys the basic_store, but callers
    // should always call close manually to receive the
    // error code.
    close(ec);
}

template<class Hasher, class File>
path_type const&
basic_store<Hasher, File>::
dat_path() const
{
    BOOST_ASSERT(is_open());
    return s_->dp;
}

template<class Hasher, class File>
path_type const&
basic_store<Hasher, File>::
key_path() const
{
    BOOST_ASSERT(is_open());
    return s_->kp;
}

template<class Hasher, class File>
path_type const&
basic_store<Hasher, File>::
log_path() const
{
    BOOST_ASSERT(is_open());
    return s_->lp;
}

template<class Hasher, class File>
std::uint64_t
basic_store<Hasher, File>::
appnum() const
{
    BOOST_ASSERT(is_open());
    return s_->kh.appnum;
}

template<class Hasher, class File>
std::size_t
basic_store<Hasher, File>::
key_size() const
{
    BOOST_ASSERT(is_open());
    return s_->kh.key_size;
}

template<class Hasher, class File>
std::size_t
basic_store<Hasher, File>::
block_size() const
{
    BOOST_ASSERT(is_open());
    return s_->kh.block_size;
}

template<class Hasher, class File>
template<class... Args>
void
basic_store<Hasher, File>::
open(
    path_type const& dat_path,
    path_type const& key_path,
    path_type const& log_path,
    error_code& ec,
    Args&&... args)
{
    static_assert(is_Hasher<Hasher>::value,
        "Hasher requirements not met");
    using namespace detail;
    BOOST_ASSERT(! is_open());
    ec_ = {};
    ecb_.store(false);
    recover<Hasher, File>(
        dat_path, key_path, log_path, ec, args...);
    if(ec)
        return;
    File df(args...);
    File kf(args...);
    File lf(args...);
    df.open(file_mode::append, dat_path, ec);
    if(ec)
        return;
    kf.open(file_mode::write, key_path, ec);
    if(ec)
        return;
    lf.create(file_mode::append, log_path, ec);
    if(ec)
        return;
    // VFALCO TODO Erase empty log file if this
    //             function subsequently fails.
    dat_file_header dh;
    read(df, dh, ec);
    if(ec)
        return;
    verify(dh, ec);
    if(ec)
        return;
    key_file_header kh;
    read(kf, kh, ec);
    if(ec)
        return;
    verify<Hasher>(kh, ec);
    if(ec)
        return;
    verify<Hasher>(dh, kh, ec);
    if(ec)
        return;
    boost::optional<state> s;
    s.emplace(std::move(df), std::move(kf), std::move(lf),
        dat_path, key_path, log_path, kh);
    thresh_ = std::max<std::size_t>(65536UL,
        kh.load_factor * kh.capacity);
    frac_ = thresh_ / 2;
    buckets_ = kh.buckets;
    modulus_ = ceil_pow2(kh.buckets);
    // VFALCO TODO This could be better
    if(buckets_ < 1)
    {
        ec = error::short_key_file;
        return;
    }
    dataWriteSize_ = 32 * nudbview::block_size(dat_path);
    logWriteSize_ = 32 * nudbview::block_size(log_path);
    s_.emplace(std::move(*s));
    gen_.store(0);
    readers_[0].store(0);
    readers_[1].store(0);
    snap_[0] = snapshot{buckets_, modulus_, nullptr};
    snap_[1] = snapshot{};
    // Recovery has run: everything in the data file is durable.
    durable_size_.store(s_->df.size(ec));
    if(ec)
        return;
    open_ = true;
    ctx_->insert(*this);
}

template<class Hasher, class File>
void
basic_store<Hasher, File>::
close(error_code& ec)
{
    if(open_)
    {
        open_ = false;
        ctx_->erase(*this);
        if(! pool_empty())
        {
            std::size_t work;
            std::lock_guard<std::mutex> wl{w_};
            detail::unique_lock_type m{m_};
            commit(m, work, ec_);
            if (ec_)
                ecb_.store(true);
        }
        if(ecb_)
        {
            ec = ec_;
            return;
        }
        s_->lf.close();
        state s{std::move(*s_)};
        File::erase(s.lp, ec_);
        if(ec_)
            ec = ec_;
    }
}

template<class Hasher, class File>
template<class Callback>
void
basic_store<Hasher, File>::
fetch(
    void const* key,
    Callback && callback,
    error_code& ec)
{
    using namespace detail;
    BOOST_ASSERT(is_open());
    if(ecb_)
    {
        ec = ec_;
        return;
    }
    auto const h =
        hash(key, s_->kh.key_size, s_->hasher);
    if(! in_batch_.load(std::memory_order_acquire))
    {
        auto const sh = shard(h);
        std::lock_guard<std::mutex> l{sh_[sh]};
        auto iter = s_->p1[sh].find(key);
        if(iter == s_->p1[sh].end())
        {
            iter = s_->p0[sh].find(key);
            if(iter == s_->p0[sh].end())
                goto cont;
        }
        callback(iter->first.data, iter->first.size);
        return;
    }
cont:
    read_guard rg{*this};
    auto const n = bucket_index(h, rg.snap->buckets, rg.snap->modulus);
    if(rg.snap->c1)
    {
        auto const iter = rg.snap->c1->find(n);
        if(iter != rg.snap->c1->end())
            return fetch(h, key, iter->second, callback, ec);
    }
    buffer buf{s_->kh.block_size};
    // b constructs from uninitialized buf
    bucket b{s_->kh.block_size, buf.get()};
    b.read(s_->kf, (n + 1) * b.block_size(), ec);
    if(ec)
        return;
    fetch(h, key, b, callback, ec);
}

template<class Hasher, class File>
void
basic_store<Hasher, File>::
fetch_offset(
    void const* key,
    record_location& location,
    error_code& ec)
{
    using namespace detail;
    BOOST_ASSERT(is_open());
    if(ecb_)
    {
        ec = ec_;
        return;
    }
    auto const h =
        hash(key, s_->kh.key_size, s_->hasher);
    // A key still in a pool has not been assigned
    // a data file offset; commit assigns them.
    if(! in_batch_.load(std::memory_order_acquire))
    {
        auto const sh = shard(h);
        std::lock_guard<std::mutex> l{sh_[sh]};
        if(s_->p1[sh].find(key) != s_->p1[sh].end() ||
           s_->p0[sh].find(key) != s_->p0[sh].end())
        {
            ec = error::key_pending;
            return;
        }
    }
    read_guard rg{*this};
    auto const n = bucket_index(h, rg.snap->buckets, rg.snap->modulus);
    if(rg.snap->c1)
    {
        auto const iter = rg.snap->c1->find(n);
        if(iter != rg.snap->c1->end())
            return fetch_offset(h, key, iter->second, location, ec);
    }
    buffer buf{s_->kh.block_size};
    // b constructs from uninitialized buf
    bucket b{s_->kh.block_size, buf.get()};
    b.read(s_->kf, (n + 1) * b.block_size(), ec);
    if(ec)
        return;
    fetch_offset(h, key, b, location, ec);
}

template<class Hasher, class File>
void
basic_store<Hasher, File>::
insert(
    void const* key,
    void const* data,
    nsize_t size,
    error_code& ec)
{
    using namespace detail;
    using namespace std::chrono;
    BOOST_ASSERT(is_open());
    if(ecb_)
    {
        ec = ec_;
        return;
    }
    // Data Record
    BOOST_ASSERT(size > 0);                     // zero disallowed
    BOOST_ASSERT(size <= field<uint32_t>::max); // too large
    auto const h =
        hash(key, s_->kh.key_size, s_->hasher);
    auto const sh = shard(h);
    // The shard lock makes check-then-insert atomic for this key and
    // serializes only inserts that share the shard.
    std::lock_guard<std::mutex> u{sh_[sh]};
    {
        if(s_->p1[sh].find(key) != s_->p1[sh].end() ||
           s_->p0[sh].find(key) != s_->p0[sh].end())
        {
            ec = error::key_exists;
            return;
        }
        read_guard rg{*this};
        auto const n = bucket_index(h, rg.snap->buckets, rg.snap->modulus);
        bool found = false;
        typename detail::cache::iterator iter;
        bool cached = false;
        if(rg.snap->c1)
        {
            iter = rg.snap->c1->find(n);
            cached = iter != rg.snap->c1->end();
        }
        if(cached)
        {
            found = exists(h, key, nullptr, iter->second, ec);
        }
        else
        {
            buffer buf;
            buf.reserve(s_->kh.block_size);
            bucket b{s_->kh.block_size, buf.get()};
            b.read(s_->kf,
                   static_cast<noff_t>(n + 1) * s_->kh.block_size, ec);
            if(ec)
                return;
            found = exists(h, key, nullptr, b, ec);
        }
        if(ec)
            return;
        if(found)
        {
            ec = error::key_exists;
            return;
        }
    }
    // Perform insert, under the shard lock only.
    s_->p1[sh].insert(h, key, data, size);
    auto const pending_bytes = pending_bytes_.fetch_add(size) + size;
    auto const pending_count = pending_count_.fetch_add(1) + 1;
    bool sleep = false;
    {
        shared_lock_type m{m_};
        auto const now = clock_type::now();
        auto const elapsed = duration_cast<duration<float>>(
            now > s_->when ? now - s_->when : clock_type::duration{1});
        auto const work = pending_bytes +
            3 * pending_count * s_->kh.block_size;
        auto const rate = static_cast<std::size_t>(
            std::ceil(work / elapsed.count()));
        sleep = s_->rate && rate > s_->rate && work > s_->burst;
    }

    // The caller of insert must be blocked when the rate of insertion
    // (measured in approximate bytes per second) exceeds the maximum rate
    // that can be flushed and a burst of data is already in memory.
    // The precise sleep duration is not important.
    if(sleep)
        std::this_thread::sleep_for(milliseconds{25});
}

// Fetch key in loaded bucket b or its spills.
//
template<class Hasher, class File>
template<class Callback>
void
basic_store<Hasher, File>::
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
        for(auto i = b.lower_bound(h); i < b.size(); ++i)
        {
            auto const item = b[i];
            if(item.hash != h)
                break;
            // Data Record
            auto const len =
                s_->kh.key_size +       // Key
                item.size;              // Value
            buf0.reserve(len);
            s_->df.read(item.offset +
                field<uint48_t>::size,  // Size
                    buf0.get(), len, ec);
            if(ec)
                return;
            if(std::memcmp(buf0.get(), key,
                s_->kh.key_size) == 0)
            {
                callback(
                    buf0.get() + s_->kh.key_size, item.size);
                return;
            }
        }
        auto const spill = b.spill();
        if(! spill)
            break;
        buf1.reserve(s_->kh.block_size);
        b = bucket(s_->kh.block_size,
            buf1.get());
        b.read(s_->df, spill, ec);
        if(ec)
            return;
    }
    ec = error::key_not_found;
}

// Locate key in loaded bucket b or its spills.
// Reads only the key of each candidate record.
//
template<class Hasher, class File>
void
basic_store<Hasher, File>::
fetch_offset(
    detail::nhash_t h,
    void const* key,
    detail::bucket b,
    record_location& location,
    error_code& ec)
{
    using namespace detail;
    buffer buf0{s_->kh.key_size};
    buffer buf1;
    for(;;)
    {
        for(auto i = b.lower_bound(h); i < b.size(); ++i)
        {
            auto const item = b[i];
            if(item.hash != h)
                break;
            // Data Record
            s_->df.read(item.offset +
                field<uint48_t>::size,      // Size
                    buf0.get(), s_->kh.key_size, ec);   // Key
            if(ec)
                return;
            if(std::memcmp(buf0.get(), key,
                s_->kh.key_size) == 0)
            {
                // Published by a commit that has not
                // cleared its log yet: not durable.
                if(item.offset >= durable_size_.load())
                {
                    ec = error::key_pending;
                    return;
                }
                location.offset = item.offset;
                location.size = item.size;
                return;
            }
        }
        auto const spill = b.spill();
        if(! spill)
            break;
        buf1.reserve(s_->kh.block_size);
        b = bucket(s_->kh.block_size,
            buf1.get());
        b.read(s_->df, spill, ec);
        if(ec)
            return;
    }
    ec = error::key_not_found;
}

// Returns `true` if the key exists
// lock is unlocked after the first bucket processed
//
template<class Hasher, class File>
bool
basic_store<Hasher, File>::
exists(
    detail::nhash_t h,
    void const* key,
    detail::shared_lock_type* lock,
    detail::bucket b,
    error_code& ec)
{
    using namespace detail;
    buffer buf{s_->kh.key_size + s_->kh.block_size};
    void* pk = buf.get();
    void* pb = buf.get() + s_->kh.key_size;
    for(;;)
    {
        for(auto i = b.lower_bound(h); i < b.size(); ++i)
        {
            auto const item = b[i];
            if(item.hash != h)
                break;
            // Data Record
            s_->df.read(item.offset +
                field<uint48_t>::size,      // Size
                pk, s_->kh.key_size, ec);       // Key
            if(ec)
                return false;
            if(std::memcmp(pk, key, s_->kh.key_size) == 0)
                return true;
        }
        auto spill = b.spill();
        if(lock && lock->owns_lock())
            lock->unlock();
        if(! spill)
            break;
        b = bucket(s_->kh.block_size, pb);
        b.read(s_->df, spill, ec);
        if(ec)
            return false;
    }
    return false;
}

//  Set the burst size
//
template<class Hasher, class File>
void
basic_store<Hasher, File>::
set_burst(
    std::size_t burst_size)
{
    detail::unique_lock_type m{m_};
    s_->burst = burst_size;
}

//  Split the bucket in b1 to b2
//  b1 must be loaded
//  tmp is used as a temporary buffer
//  splits are written but not the new buckets
//
template<class Hasher, class File>
void
basic_store<Hasher, File>::
split(
    detail::bucket& b1,
    detail::bucket& b2,
    detail::bucket& tmp,
    nbuck_t n1,
    nbuck_t n2,
    nbuck_t buckets,
    nbuck_t modulus,
    detail::bulk_writer<File>& w,
    error_code& ec)
{
    using namespace detail;
    // Trivial case: split empty bucket
    if(b1.empty())
        return;
    // Split
    for(std::size_t i = 0; i < b1.size();)
    {
        auto const e = b1[i];
        auto const n = bucket_index(e.hash, buckets, modulus);
        (void)n1;
        (void)n2;
        BOOST_ASSERT(n==n1 || n==n2);
        if(n == n2)
        {
            b2.insert(e.offset, e.size, e.hash);
            b1.erase(i);
        }
        else
        {
            ++i;
        }
    }
    noff_t spill = b1.spill();
    if(spill)
    {
        b1.spill(0);
        do
        {
            // If any part of the spill record is
            // in the write buffer then flush first
            if(spill + bucket_size(s_->kh.capacity) >
               w.offset() - w.size())
            {
                w.flush(ec);
                if(ec)
                    return;
            }
            tmp.read(s_->df, spill, ec);
            if(ec)
                return;
            for(std::size_t i = 0; i < tmp.size(); ++i)
            {
                auto const e = tmp[i];
                auto const n = bucket_index(
                    e.hash, buckets, modulus);
                BOOST_ASSERT(n==n1 || n==n2);
                if(n == n2)
                {
                    maybe_spill(b2, w, ec);
                    if(ec)
                        return;
                    b2.insert(e.offset, e.size, e.hash);
                }
                else
                {
                    maybe_spill(b1, w, ec);
                    if(ec)
                        return;
                    b1.insert(e.offset, e.size, e.hash);
                }
            }
            spill = tmp.spill();
        }
        while(spill);
    }
}

template<class Hasher, class File>
detail::bucket
basic_store<Hasher, File>::
load(
    nbuck_t n,
    detail::cache& c1,
    detail::cache& c0,
    void* buf,
    error_code& ec)
{
    using namespace detail;
    auto iter = c1.find(n);
    if(iter != c1.end())
        return iter->second;
    iter = c0.find(n);
    if(iter != c0.end())
        return c1.insert(n, iter->second)->second;
    bucket tmp{s_->kh.block_size, buf};
    tmp.read(s_->kf,
             static_cast<noff_t>(n + 1) * s_->kh.block_size, ec);
    if(ec)
        return {};
    c0.insert(n, tmp);
    return c1.insert(n, tmp)->second;
}

// Write the log file header naming the current file sizes,
// and checkpoint it. Recovery truncates both files back to
// these sizes if the commit or batch that follows is torn.
//
template<class Hasher, class File>
void
basic_store<Hasher, File>::
write_log_header(error_code& ec)
{
    using namespace detail;
    log_file_header lh;
    lh.version = currentVersion;            // Version
    lh.uid = s_->kh.uid;                    // UID
    lh.appnum = s_->kh.appnum;              // Appnum
    lh.key_size = s_->kh.key_size;          // Key Size
    lh.salt = s_->kh.salt;                  // Salt
    lh.pepper = pepper<Hasher>(lh.salt);    // Pepper
    lh.block_size = s_->kh.block_size;      // Block Size
    lh.key_file_size = s_->kf.size(ec);     // Key File Size
    if(ec)
        return;
    lh.dat_file_size = s_->df.size(ec);     // Data File Size
    if(ec)
        return;
    write(s_->lf, lh, ec);
    if(ec)
        return;
    // Checkpoint
    s_->lf.sync(ec);
}

// Do inserts, splits, and build view of original and
// modified buckets for records already written to the
// data file through w. Spills are appended through w.
//
template<class Hasher, class File>
void
basic_store<Hasher, File>::
index_records(
    std::vector<record_entry> const& records,
    detail::bulk_writer<File>& w,
    detail::cache& c0,
    detail::cache& c1,
    detail::bucket& tmp,
    void* buf,
    nbuck_t& buckets,
    nbuck_t& modulus,
    error_code& ec)
{
    using namespace detail;
    for(auto const& e : records)
    {
        if(e.replace)
        {
            // Promotion of an existing key adds no entry, so it
            // must not advance the load accounting or split.
            auto const n = bucket_index(
                e.hash, buckets, modulus);
            auto b = load(n, c1, c0, buf, ec);
            if(ec)
                return;
            auto const swung = swing_entry(b, e, w, tmp, ec);
            if(ec)
                return;
            if(swung)
                continue;
            // No entry for this key yet: insert as new, below.
        }
        // VFALCO Should this be >= or > ?
        if((frac_ += 65536) >= thresh_)
        {
            // split
            frac_ -= thresh_;
            if(buckets == modulus)
                modulus *= 2;
            auto const n1 = buckets - (modulus / 2);
            auto const n2 = buckets++;
            auto b1 = load(n1, c1, c0, buf, ec);
            if(ec)
                return;
            auto b2 = c1.create(n2);
            // If split spills, the writer is
            // flushed which can amplify writes.
            split(b1, b2, tmp, n1, n2,
                buckets, modulus, w, ec);
            if(ec)
                return;
        }
        // Insert
        auto const n = bucket_index(
            e.hash, buckets, modulus);
        auto b = load(n, c1, c0, buf, ec);
        if(ec)
            return;
        // This can amplify writes if it spills.
        maybe_spill(b, w, ec);
        if(ec)
            return;
        b.insert(e.offset, e.size, e.hash);
    }
}

// Swing the bucket entry for e's exact key to {e.offset, e.size}.
// Returns true if an entry was found and swung, false if the key
// has no entry in b or its spill chain. If the stale entry lives
// in a spill record, which is immutable, the whole chain is pulled
// back into the block without it, the way split does.
//
template<class Hasher, class File>
bool
basic_store<Hasher, File>::
swing_entry(
    detail::bucket& b,
    record_entry const& e,
    detail::bulk_writer<File>& w,
    detail::bucket& tmp,
    error_code& ec)
{
    using namespace detail;
    auto const key_size = s_->kh.key_size;
    // The new record's key: it may still be in the writer's
    // buffer, so flush before reading it back.
    buffer keybuf{key_size};
    if(e.offset + field<uint48_t>::size + key_size >
        w.offset() - w.size())
    {
        w.flush(ec);
        if(ec)
            return false;
    }
    s_->df.read(e.offset + field<uint48_t>::size,
        keybuf.get(), key_size, ec);
    if(ec)
        return false;
    return promote_entry(s_->df, w, b, tmp, key_size,
        s_->kh.capacity, keybuf.get(), e.hash, e.offset, e.size, ec);
}

// Publish the new buckets to readers, log the old ones,
// write the new ones to the key file, sync, and clear
// the log. On return m is locked.
//
template<class Hasher, class File>
void
basic_store<Hasher, File>::
finish_commit(
    detail::unique_lock_type& m,
    detail::cache& c0,
    detail::cache& c1,
    nbuck_t buckets,
    nbuck_t modulus,
    error_code& ec,
    bool shards_held)
{
    using namespace detail;
    BOOST_ASSERT(! m.owns_lock());
    // Publish the new buckets to readers under a new generation. The
    // slot being reused belonged to the generation before last, whose
    // readers were drained by the previous commit, so it is free to
    // clear and refill.
    auto const g = gen_.load(std::memory_order_acquire);
    auto const ng = g + 1;
    auto& slot = s_->c1[ng & 1];
    slot.clear();
    swap(c1, slot);
    buckets_ = buckets;
    modulus_ = modulus;
    snap_[ng & 1] = snapshot{buckets, modulus, &slot};
    gen_.store(ng, std::memory_order_release);
    // Readers of the previous generation may still be inside a bucket
    // read from the key file or its cache; wait them out before the
    // key file is rewritten. Their critical sections are microseconds.
    while(readers_[g & 1].load(std::memory_order_acquire) != 0)
        std::this_thread::yield();
    // The committed records are now findable through the published
    // buckets, so the committing pools can go. Shard locks only, never
    // under the store lock.
    for(std::size_t i = 0; i < nshards; ++i)
    {
        if(shards_held)
        {
            s_->p0[i].clear();
            continue;
        }
        std::lock_guard<std::mutex> l{sh_[i]};
        s_->p0[i].clear();
    }
    // Write clean buckets to log file
    {
        auto const size = s_->lf.size(ec);
        if(ec)
            return;
        bulk_writer<File> w{s_->lf, size, logWriteSize_};
        for(auto const e : c0)
        {
            // Log Record
            auto os = w.prepare(
                field<std::uint64_t>::size +    // Index
                e.second.actual_size(), ec);    // Bucket
            if(ec)
                return;
            // Log Record
            write<std::uint64_t>(os, e.first);  // Index
            e.second.write(os);                 // Bucket
        }
        c0.clear();
        w.flush(ec);
        if(ec)
            return;
        s_->lf.sync(ec);
        if(ec)
            return;
    }
    // Write new buckets to key file. Current-generation readers take
    // every touched bucket from the published cache, so none of these
    // blocks is read from the file while it is being written.
    for(auto const e : slot)
    {
        e.second.write(s_->kf,
           (e.first + 1) * s_->kh.block_size, ec);
        if(ec)
            return;
    }
    // Finalize the commit
    s_->df.sync(ec);
    if(ec)
        return;
    s_->kf.sync(ec);
    if(ec)
        return;
    s_->lf.trunc(0, ec);
    if(ec)
        return;
    s_->lf.sync(ec);
    if(ec)
        return;
    // The log is gone, so recovery can no longer remove
    // anything appended by this commit: it is all durable.
    {
        auto const size = s_->df.size(ec);
        if(ec)
            return;
        durable_size_.store(size);
    }
    // The published cache stays until the next commit reuses its slot,
    // so a current-generation reader can never see it cleared.
    m.lock();
}

// Caller holds w_ for the whole call.
//
template<class Hasher, class File>
void
basic_store<Hasher, File>::
commit(detail::unique_lock_type& m,
    std::size_t& work, error_code& ec)
{
    using namespace detail;
    BOOST_ASSERT(m.owns_lock());
    BOOST_ASSERT(! pool_empty());
    // Lock order is shard then store (insert holds a shard lock while it
    // takes the store lock), so release the store lock before swapping.
    // Pool access is guarded by the shard locks alone.
    m.unlock();
    std::size_t total = 0;
    work = 0;
    for(std::size_t i = 0; i < nshards; ++i)
    {
        std::lock_guard<std::mutex> l{sh_[i]};
        swap(s_->p0[i], s_->p1[i]);
        total += s_->p0[i].size();
        work += s_->p0[i].data_size();
        // Subtract exactly what this shard moved, under its lock, so an
        // insert landing on an already swapped shard keeps its count.
        // Zeroing after the loop would count it away and let close()
        // skip the commit that carries it.
        pending_count_.fetch_sub(s_->p0[i].size());
        pending_bytes_.fetch_sub(s_->p0[i].data_size());
    }
    cache c0(s_->kh.key_size, s_->kh.block_size, "c0");
    cache c1(s_->kh.key_size, s_->kh.block_size, "c1");
    // 0.63212 ~= 1 - 1/e
    {
        auto const size = static_cast<std::size_t>(
            std::ceil(0.63212 * total));
        c0.reserve(size);
        c1.reserve(size);
    }
    buffer buf1{s_->kh.block_size};
    buffer buf2{s_->kh.block_size};
    bucket tmp{s_->kh.block_size, buf1.get()};
    // Prepare rollback information
    write_log_header(ec);
    if(ec)
        return;
    // Append data and spills to data file
    auto modulus = modulus_;
    auto buckets = buckets_;
    {
        // Bulk write to avoid write amplification
        auto const size = s_->df.size(ec);
        if(ec)
            return;
        bulk_writer<File> w{s_->df, size, dataWriteSize_};
        // Write inserted data to the data file
        std::vector<record_entry> records;
        records.reserve(total);
        for(std::size_t i = 0; i < nshards; ++i)
        for(auto const& e : s_->p0[i])
        {
            auto const offset = w.offset();
            auto os = w.prepare(value_size(
                e.first.size, s_->kh.key_size), ec);
            if(ec)
                return;
            // Data Record
            write<uint48_t>(os, e.first.size);          // Size
            write(os, e.first.key, s_->kh.key_size);    // Key
            write(os, e.first.data, e.first.size);      // Data
            records.push_back({e.first.hash, e.first.size, offset});
        }
        index_records(records, w, c0, c1, tmp,
            buf2.get(), buckets, modulus, ec);
        if(ec)
            return;
        w.flush(ec);
        if(ec)
            return;
    }
    work += s_->kh.block_size * (2 * c0.size() + c1.size());
    finish_commit(m, c0, c1, buckets, modulus, ec);
}

template<class Hasher, class File>
noff_t
basic_store<Hasher, File>::appender::
append(
    void const* key,
    void const* data,
    nsize_t size,
    error_code& ec)
{
    using namespace detail;
    BOOST_ASSERT(size > 0);
    if(ec_)
    {
        ec = ec_;
        return 0;
    }
    auto const offset = w_.offset();
    auto os = w_.prepare(value_size(size, key_size_), ec);
    if(ec)
    {
        ec_ = ec;
        return 0;
    }
    // Data Record
    write<uint48_t>(os, size);          // Size
    write(os, key, key_size_);          // Key
    write(os, data, size);              // Data
    records_.push_back({
        hash(key, key_size_, hasher_), size, offset, false});
    return offset;
}

template<class Hasher, class File>
noff_t
basic_store<Hasher, File>::appender::
replace(
    void const* key,
    void const* data,
    nsize_t size,
    error_code& ec)
{
    auto const offset = append(key, data, size, ec);
    if(ec)
        return 0;
    records_.back().replace = true;
    return offset;
}

template<class Hasher, class File>
template<class Fn>
void
basic_store<Hasher, File>::
batch(Fn&& fn, error_code& ec)
{
    using namespace detail;
    BOOST_ASSERT(is_open());
    if(ecb_)
    {
        ec = ec_;
        return;
    }
    // Lock order: w_ (commits), m_ (state). Inserts may continue during
    // the batch: they reach the data file only at a commit, and no
    // commit runs while w_ is held.
    std::lock_guard<std::mutex> wl{w_};
    unique_lock_type m{m_};
    // Pending inserts are committed first, so they
    // precede the batch in the data file.
    if(! pool_empty())
    {
        std::size_t work;
        commit(m, work, ec_);
        if(ec_)
        {
            ecb_.store(true);
            ec = ec_;
            return;
        }
        BOOST_ASSERT(m.owns_lock());
    }
    m.unlock();
    // Exclude inserts for the batch's duration: every shard lock, in
    // order. The pool is empty from here (committed above, nothing can
    // land), so readers skip it while in_batch_ is set.
    struct shard_guard
    {
        basic_store& st;
        explicit shard_guard(basic_store& s) : st(s)
        {
            for(auto& mx : st.sh_)
                mx.lock();
            st.in_batch_.store(true, std::memory_order_release);
        }
        ~shard_guard()
        {
            st.in_batch_.store(false, std::memory_order_release);
            for(auto it = st.sh_.rbegin(); it != st.sh_.rend(); ++it)
                it->unlock();
        }
    } shards{*this};
    cache c0(s_->kh.key_size, s_->kh.block_size, "c0");
    cache c1(s_->kh.key_size, s_->kh.block_size, "c1");
    buffer buf1{s_->kh.block_size};
    buffer buf2{s_->kh.block_size};
    bucket tmp{s_->kh.block_size, buf1.get()};
    // Prepare rollback information
    write_log_header(ec_);
    if(ec_)
    {
        ecb_.store(true);
        ec = ec_;
        return;
    }
    auto modulus = modulus_;
    auto buckets = buckets_;
    {
        auto const size = s_->df.size(ec_);
        if(ec_)
        {
            ecb_.store(true);
            ec = ec_;
            return;
        }
        bulk_writer<File> w{s_->df, size, dataWriteSize_};
        std::vector<record_entry> records;
        appender a{w, s_->hasher, s_->kh.key_size, records};
        try
        {
            fn(a);
        }
        catch(...)
        {
            ec_ = error::batch_aborted;
        }
        if(! ec_ && a.ec_)
            ec_ = a.ec_;
        if(! ec_ && records.empty())
        {
            // Nothing appended: clear the log header we wrote
            // and leave the store exactly as it was.
            s_->lf.trunc(0, ec_);
            if(! ec_)
                s_->lf.sync(ec_);
            if(ec_)
            {
                ecb_.store(true);
                ec = ec_;
            }
            return;
        }
        if(! ec_)
            index_records(records, w, c0, c1, tmp,
                buf2.get(), buckets, modulus, ec_);
        if(! ec_)
            w.flush(ec_);
        if(ec_)
        {
            ecb_.store(true);
            ec = ec_;
            return;
        }
    }
    finish_commit(m, c0, c1, buckets, modulus, ec_, true);
    if(ec_)
    {
        ecb_.store(true);
        ec = ec_;
    }
}

template<class Hasher, class File>
void
basic_store<Hasher, File>::
flush()
{
    using namespace std::chrono;
    using namespace detail;

#if NUDB_DEBUG_LOG
    beast::unit_test::dstream dout{std::cout};
#endif
    {
        std::lock_guard<std::mutex> wl{w_};
        unique_lock_type m{m_};
        s_->when = clock_type::now();
        if(! pool_empty())
        {
            std::size_t work;
            commit(m, work, ec_);
            if(ec_)
            {
                ecb_.store(true);
                return;
            }
            BOOST_ASSERT(m.owns_lock());
            auto const now = clock_type::now();
            auto const elapsed = duration_cast<duration<float>>(
                now > s_->when ? now - s_->when : clock_type::duration{1});
            auto const rate = static_cast<std::size_t>(
                std::ceil(work / elapsed.count()));

            // Writes below the burst size may be dominated by
            // overhead and give an artificially low write rate
            if (work > s_->burst)
                s_->rate = rate;

        #if NUDB_DEBUG_LOG
            dout <<
                "work=" << work <<
                ", time=" << elapsed.count() <<
                ", rate=" << s_->rate <<
                "\n";
        #endif
        }
        // Lock order is shard then store: release the store lock before
        // touching the shards, or an insert holding a shard lock while it
        // takes the store lock shared deadlocks against this.
        m.unlock();
        for(std::size_t i = 0; i < nshards; ++i)
        {
            std::lock_guard<std::mutex> l{sh_[i]};
            s_->p1[i].periodic_activity();
        }
    }
    if(ec_)
        ecb_.store(true);
}

} // nudb

#endif
