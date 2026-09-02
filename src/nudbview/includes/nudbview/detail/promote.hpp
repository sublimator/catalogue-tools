//
// Exact-key promotion: swing a key's single bucket entry to a
// newer data record. Shared by basic_store::batch indexing and
// by rekey, so both reproduce the same winner.
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef NUDB_DETAIL_PROMOTE_HPP
#define NUDB_DETAIL_PROMOTE_HPP

#include <nudbview/error.hpp>
#include <nudbview/type_traits.hpp>
#include <nudbview/detail/bucket.hpp>
#include <nudbview/detail/buffer.hpp>
#include <nudbview/detail/bulkio.hpp>
#include <nudbview/detail/field.hpp>
#include <nudbview/detail/format.hpp>
#include <cstring>
#include <vector>

namespace nudbview {
namespace detail {

// Swing the bucket entry whose data record has exactly `key`
// to {offset, size}. Returns true if an entry was found and
// swung, false if the key has no entry in b or its spill
// chain, in which case the caller inserts as usual.
//
// If the stale entry lives in a spill record, which is
// immutable, the whole chain is pulled back into the block
// without it and re-spilled through w as the block fills, the
// way split does. The old spill records become dead bytes.
//
// Reads of the data file at or past w's flushed extent flush
// w first, because those bytes are still in its buffer.
//
template<class File>
bool
promote_entry(
    File& df,
    bulk_writer<File>& w,
    bucket& b,
    bucket& tmp,
    nsize_t key_size,
    nkey_t capacity,
    void const* key,
    nhash_t h,
    noff_t offset,
    nsize_t size,
    error_code& ec)
{
    buffer keybuf{key_size};
    std::uint8_t* const have = keybuf.get();
    auto const ensure_readable = [&](noff_t at, std::size_t len)
    {
        if(at + len > w.offset() - w.size())
            w.flush(ec);
    };
    auto const same_key = [&](noff_t at) -> bool
    {
        ensure_readable(at + field<uint48_t>::size, key_size);
        if(ec)
            return false;
        df.read(at + field<uint48_t>::size, have, key_size, ec);
        if(ec)
            return false;
        return std::memcmp(key, have, key_size) == 0;
    };
    // In the block: overwrite in place.
    for(auto i = b.lower_bound(h); i < b.size(); ++i)
    {
        auto const item = b[i];
        if(item.hash != h)
            break;
        if(same_key(item.offset))
        {
            b.replace(i, offset, size);
            return true;
        }
        if(ec)
            return false;
    }
    // In the spill chain. Pass one: find the stale entry, one
    // scratch bucket at a time, remembering only its record
    // offset. Memory is one block regardless of chain length.
    noff_t const head = b.spill();
    if(! head)
        return false;
    noff_t stale = 0;
    bool found = false;
    for(noff_t spill = head; spill && ! found;)
    {
        ensure_readable(spill, bucket_size(capacity));
        if(ec)
            return false;
        tmp.read(df, spill, ec);
        if(ec)
            return false;
        for(std::size_t i = 0; i < tmp.size(); ++i)
        {
            auto const item = tmp[i];
            if(item.hash == h && same_key(item.offset))
            {
                stale = item.offset;
                found = true;
                break;
            }
            if(ec)
                return false;
        }
        spill = tmp.spill();
    }
    if(! found)
        return false;
    // Pass two: stream the old chain back into the block, skipping
    // the stale entry by its unique record offset. maybe_spill writes
    // fresh spill records through w as the block fills; the old chain
    // is read from the file and becomes dead bytes.
    b.spill(0);
    for(noff_t spill = head; spill;)
    {
        ensure_readable(spill, bucket_size(capacity));
        if(ec)
            return false;
        tmp.read(df, spill, ec);
        if(ec)
            return false;
        for(std::size_t i = 0; i < tmp.size(); ++i)
        {
            auto const item = tmp[i];
            if(item.offset == stale)
                continue;
            maybe_spill(b, w, ec);
            if(ec)
                return false;
            b.insert(item.offset, item.size, item.hash);
        }
        spill = tmp.spill();
    }
    maybe_spill(b, w, ec);
    if(ec)
        return false;
    b.insert(offset, size, h);
    return true;
}

} // detail
} // nudbview

#endif
