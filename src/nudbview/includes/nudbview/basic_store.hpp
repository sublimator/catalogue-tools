//
// Copyright (c) 2015-2016 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef NUDB_BASIC_STORE_HPP
#define NUDB_BASIC_STORE_HPP

#include <nudbview/context.hpp>
#include <nudbview/file.hpp>
#include <nudbview/type_traits.hpp>
#include <nudbview/detail/cache.hpp>
#include <nudbview/detail/gentex.hpp>
#include <nudbview/detail/mutex.hpp>
#include <nudbview/detail/pool.hpp>
#include <nudbview/detail/store_base.hpp>
#include <boost/optional.hpp>
#include <chrono>
#include <mutex>
#include <vector>

namespace nudbview {

/** A high performance, insert-only key/value database for SSDs.

    To create a database first call the @ref create
    free function. Then construct a @ref basic_store and
    call @ref open on it:

    @code
        error_code ec;
        create<xxhasher>(
            "db.dat", "db.key", "db.log",
                1, make_salt(), 8, 4096, 0.5f, ec);
        basic_store<xxhasher, native_file> db;
        db.open("db.dat", "db.key", "db.log", ec);
    @endcode

    @tparam Hasher The hash function to use. This type
    must meet the requirements of @b Hasher.

    @tparam File The type of File object to use. This type
    must meet the requirements of @b File.
*/
template<class Hasher, class File>
class basic_store
#if ! NUDB_DOXYGEN
        : private detail::store_base
#endif
{
public:
    using hash_type = Hasher;
    using file_type = File;

private:
    using clock_type =
        std::chrono::steady_clock;

    using time_point =
        typename clock_type::time_point;

#if ! NUDB_DOXYGEN
    friend class test::context_test;
#endif

    struct state
    {
        File df;
        File kf;
        File lf;
        path_type dp;
        path_type kp;
        path_type lp;
        Hasher hasher;
        detail::pool p0;
        detail::pool p1;
        detail::cache c1;
        detail::key_file_header kh;

        std::size_t rate = 0;
        std::size_t burst = 4 * 1024 * 1024;
        time_point when = clock_type::now();

        state(state const&) = delete;
        state& operator=(state const&) = delete;

        state(state&&) = default;
        state& operator=(state&&) = default;

        state(File&& df_, File&& kf_, File&& lf_,
            path_type const& dp_, path_type const& kp_,
                path_type const& lp_,
                    detail::key_file_header const& kh_);
    };

    bool open_ = false;

    // Use optional because some
    // members cannot be default-constructed.
    //
    boost::optional<state> s_;      // State of an open database

    std::size_t frac_;              // accumulates load
    std::size_t thresh_;            // split threshold
    nbuck_t buckets_;               // number of buckets
    nbuck_t modulus_;               // hash modulus

    std::mutex u_;                  // serializes insert()
    std::mutex w_;                  // serializes commit() and batch()
    detail::gentex g_;
    boost::shared_mutex m_;

    // A record written to the data file during a commit
    // or a batch, before its bucket entry exists.
    struct record_entry
    {
        detail::nhash_t hash;
        nsize_t size;
        noff_t offset;
    };

    error_code ec_;
    std::atomic<bool> ecb_;         // `true` when ec_ set

    // Data file size at the end of the last commit or batch
    // whose log was cleared. Records at or past it were
    // published to readers by a commit still in flight and
    // would be truncated by recovery if the process died now.
    std::atomic<noff_t> durable_size_{0};

    std::size_t dataWriteSize_;
    std::size_t logWriteSize_;

    struct deleter
    {
        deleter() = default;
        deleter(bool) : free_(true) {}
        void operator() (context* p) const
        {
            if (free_)
                delete p;
        }

        bool free_ = false;
    };

    std::unique_ptr<context, deleter> ctx_;

public:
    /** Default constructor.

        A default constructed database is initially closed.
    */
    basic_store() : ctx_(new context, true)
    {
        ctx_->start();
    }

    basic_store(context& ctx) : ctx_(&ctx) {}

    /// Copy constructor (disallowed)
    basic_store(basic_store const&) = delete;

    /// Copy assignment (disallowed)
    basic_store& operator=(basic_store const&) = delete;

    /** Destroy the database.

        Files are closed, memory is freed, and data that has not been
        committed is discarded. To ensure that all inserted data is
        written, it is necessary to call @ref close before destroying
        the @ref basic_store.

        This function ignores errors returned by @ref close; to receive
        those errors it is necessary to call @ref close before the
        @ref basic_store is destroyed.
    */
    ~basic_store();

    /** Returns `true` if the database is open.

        @par Thread safety

        Safe to call concurrently with any function
        except @ref open or @ref close.
    */
    bool
    is_open() const
    {
        return open_;
    }

    /** Return the path to the data file.

        @par Requirements

        The database must be open.

        @par Thread safety

        Safe to call concurrently with any function
        except @ref open or @ref close.

        @return The data file path.
    */
    path_type const&
    dat_path() const;

    /** Return the path to the key file.

        @par Requirements

        The database must be open.

        @par Thread safety

        Safe to call concurrently with any function
        except @ref open or @ref close.

        @return The key file path.
    */
    path_type const&
    key_path() const;

    /** Return the path to the log file.

        @par Requirements

        The database must be open.

        @par Thread safety

        Safe to call concurrently with any function
        except @ref open or @ref close.

        @return The log file path.
    */
    path_type const&
    log_path() const;

    /** Return the appnum associated with the database.

        This is an unsigned 64-bit integer associated with the
        database and defined by the application. It is set
        once when the database is created in a call to
        @ref create.

        @par Requirements

        The database must be open.

        @par Thread safety

        Safe to call concurrently with any function
        except @ref open or @ref close.

        @return The appnum.
    */
    std::uint64_t
    appnum() const;

    /** Return the key size associated with the database.

        The key size is defined by the application when the
        database is created in a call to @ref create. The
        key size cannot be changed on an existing database.

        @par Requirements

        The database must be open.

        @par Thread safety

        Safe to call concurrently with any function
        except @ref open or @ref close.

        @return The size of keys in the database.
    */
    std::size_t
    key_size() const;

    /** Return the block size associated with the database.

        The block size is defined by the application when the
        database is created in a call to @ref create or when a
        key file is regenerated in a call to @ref rekey. The
        block size cannot be changed on an existing key file.
        Instead, a new key file may be created with a different
        block size.

        @par Requirements

        The database must be open.

        @par Thread safety

        Safe to call concurrently with any function
        except @ref open or @ref close.

        @return The size of blocks in the key file.
    */
    std::size_t
    block_size() const;

    /** Close the database.

        All data is committed before closing.

        If an error occurs, the database is still closed.

        @par Requirements

        The database must be open.

        @par Thread safety

        Not thread safe. The caller is responsible for
        ensuring that no other member functions are
        called concurrently.

        @param ec Set to the error, if any occurred.
    */
    void
    close(error_code& ec);

    /** Open a database.

        The database identified by the specified data, key, and
        log file paths is opened. If a log file is present, the
        recovery mechanism is invoked to restore database integrity
        before the function returns.

        @par Requirements

        The database must be not be open.

        @par Thread safety

        Not thread safe. The caller is responsible for
        ensuring that no other member functions are
        called concurrently.

        @param dat_path The path to the data file.

        @param key_path The path to the key file.

        @param log_path The path to the log file.

        @param ec Set to the error, if any occurred.

        @param args Optional arguments passed to @b File constructors.

    */
    template<class... Args>
    void
    open(
        path_type const& dat_path,
        path_type const& key_path,
        path_type const& log_path,
        error_code& ec,
        Args&&... args);

    /** Fetch a value.

        The function checks the database for the specified
        key, and invokes the callback if it is found. If
        the key is not found, `ec` is set to @ref error::key_not_found.
        If any other errors occur, `ec` is set to the
        corresponding error.

        @par Requirements

        The database must be open.

        @par Thread safety

        Safe to call concurrently with any function except
        @ref close.

        @note If the implementation encounters an error while
        committing data to the database, this function will
        immediately return with `ec` set to the error which
        occurred. All subsequent calls to @ref fetch will
        return the same error until the database is closed.

        @param key A pointer to a memory buffer of at least
        @ref key_size() bytes, containing the key to be searched
        for.

        @param callback A function which will be called with the
        value data if the fetch is successful. The equivalent
        signature must be:
        @code
        void callback(
            void const* buffer, // A buffer holding the value
            std::size_t size    // The size of the value in bytes
        );
        @endcode
        The buffer provided to the callback remains valid
        until the callback returns, ownership is not transferred.

        @param ec Set to the error, if any occurred.
    */
    template<class Callback>
    void
    fetch(void const* key, Callback && callback, error_code& ec);

    /** A committed data record's location in the data file. */
    struct record_location
    {
        /// Offset of the data record's size field in the data file.
        noff_t offset;

        /// Size of the value in bytes.
        nsize_t size;
    };

    /** Locate a committed value in the data file.

        This performs the same lookup as @ref fetch but returns the
        data record's location instead of its bytes. The record at
        `offset` is laid out as `[size48][key][value]`, so the key
        starts at `offset + 6` and the value at
        `offset + 6 + key_size()`.

        A committed data record never moves within a data file:
        commits only append, and recovery only truncates the
        uncommitted tail. The returned offset is therefore a stable
        handle for as long as the data file is not rebuilt.

        Only durable records are located. A commit publishes its
        records to @ref fetch before its final syncs, but this
        function reports such a record as @ref error::key_pending
        until the commit that wrote it has cleared its log, because
        until then recovery would remove it. If the key is present
        but still in a pending pool it likewise has no offset yet
        and `ec` is @ref error::key_pending. If the key is not
        present, `ec` is set to @ref error::key_not_found.

        @par Requirements

        The database must be open.

        @par Thread safety

        Safe to call concurrently with any function except
        @ref close.

        @param key A pointer to a memory buffer of at least
        @ref key_size() bytes, containing the key to be searched
        for.

        @param location Set to the record's location on success.

        @param ec Set to the error, if any occurred.
    */
    void
    fetch_offset(void const* key,
        record_location& location, error_code& ec);

    /** Appends records to the data file inside a @ref batch.

        An appender is handed to the batch function and is valid
        only for the duration of that call.
    */
    class appender
    {
        friend class basic_store;

        detail::bulk_writer<File>& w_;
        Hasher const& hasher_;
        nsize_t key_size_;
        std::vector<record_entry>& records_;
        error_code ec_;

        appender(detail::bulk_writer<File>& w, Hasher const& hasher,
            nsize_t key_size, std::vector<record_entry>& records)
            : w_(w)
            , hasher_(hasher)
            , key_size_(key_size)
            , records_(records)
        {
        }

    public:
        appender(appender const&) = delete;
        appender& operator=(appender const&) = delete;

        /** Append a data record.

            The record is written as `[size48][key][value]` at the
            returned offset. No check is made for an existing key:
            a batch may deliberately write a second record under a
            key that already exists, and the store's fetch decides
            between them.

            The returned offset is provisional. It names real bytes
            in the data file and may be used to link records appended
            earlier in the same batch, but it is committed only when
            @ref batch returns without error. If the batch is torn,
            the offset names bytes that recovery removes.

            @param key A buffer of at least `key_size()` bytes.
            @param data The value bytes.
            @param size The value size, greater than 0.
            @param ec Set to the error, if any occurred.
            @return The offset of the record's size field.
        */
        noff_t
        append(void const* key, void const* data,
            nsize_t size, error_code& ec);

        /// The offset the next append will return.
        noff_t
        offset() const
        {
            return w_.offset();
        }

        /// Number of records appended so far in this batch.
        std::size_t
        size() const
        {
            return records_.size();
        }
    };

    /** Run a function that appends records as one commit.

        The function is called once with an @ref appender. Every
        record it appends is written to the data file in order, then
        indexed, published and synced exactly as @ref insert records
        are during a commit. Visibility and failure follow commit's:
        a batch interrupted at any point before its final sync is
        removed as a unit by recovery on the next open, and an error
        inside the batch poisons the store as a commit error does.

        For the window of the call no @ref insert lands between the
        batch's records, so records appended together are physically
        adjacent in the data file. Any records inserted before the
        batch and still pending are committed first, so they precede
        the batch in the data file.

        The function may call @ref fetch and @ref fetch_offset on this
        store: no commit is in flight while it runs, so every record
        they find is durable, and the batch's own records are not yet
        indexed so they are never found. It must not call @ref insert,
        @ref batch, or @ref close, which would deadlock. It must not
        throw; if it throws, `ec` is set to @ref error::batch_aborted
        and the store is poisoned.

        A function that appends nothing leaves the store unchanged.

        @par Requirements

        The database must be open.

        @par Thread safety

        Safe to call concurrently with any function except
        @ref close. Concurrent inserts block for the duration.

        @param fn A function with the equivalent signature
        `void fn(appender&)`.

        @param ec Set to the error, if any occurred.
    */
    template<class Fn>
    void
    batch(Fn&& fn, error_code& ec);

    /** Insert a value.

        This function attempts to insert the specified key/value
        pair into the database. If the key already exists,
        `ec` is set to @ref error::key_exists. If an error
        occurs, `ec` is set to the corresponding error.

        @par Requirements

        The database must be open.

        @par Thread safety

        Safe to call concurrently with any function except
        @ref close.

        @note If the implementation encounters an error while
        committing data to the database, this function will
        immediately return with `ec` set to the error which
        occurred. All subsequent calls to @ref insert will
        return the same error until the database is closed.

        @param key A buffer holding the key to be inserted. The
        size of the buffer should be at least the `key_size`
        associated with the open database.

        @param data A buffer holding the value to be inserted.

        @param bytes The size of the buffer holding the value
        data. This value must be greater than 0 and no more
        than 0xffffffff.

        @param ec Set to the error, if any occurred.
    */
    void
    insert(void const* key, void const* data,
        nsize_t bytes, error_code& ec);

    /** Set the burst size

        This function sets the amount of data that can be
        cached before writing threads are throttled if the
        sustained write flush rate is exceeded.

        @par Thread safety

        Safe to call concurrently with any function except
        @ref close.

        @param burst_size The number of bytes before throttling.
    */
    void
    set_burst(std::size_t burst_size);

private:
    template<class Callback>
    void
    fetch(detail::nhash_t h, void const* key,
        detail::bucket b, Callback && callback, error_code& ec);

    void
    fetch_offset(detail::nhash_t h, void const* key,
        detail::bucket b, record_location& location, error_code& ec);

    bool
    exists(detail::nhash_t h, void const* key,
        detail::shared_lock_type* lock, detail::bucket b, error_code& ec);

    void
    split(detail::bucket& b1, detail::bucket& b2,
        detail::bucket& tmp, nbuck_t n1, nbuck_t n2,
            nbuck_t buckets, nbuck_t modulus,
                detail::bulk_writer<File>& w, error_code& ec);

    detail::bucket
    load(nbuck_t n, detail::cache& c1,
        detail::cache& c0, void* buf, error_code& ec);

    void
    write_log_header(error_code& ec);

    void
    index_records(std::vector<record_entry> const& records,
        detail::bulk_writer<File>& w, detail::cache& c0,
        detail::cache& c1, detail::bucket& tmp, void* buf,
        nbuck_t& buckets, nbuck_t& modulus, error_code& ec);

    void
    finish_commit(detail::unique_lock_type& m,
        detail::cache& c0, detail::cache& c1,
        nbuck_t buckets, nbuck_t modulus, error_code& ec);

    void
    commit(detail::unique_lock_type& m,
        std::size_t& work, error_code& ec);

    void
    flush() override;
};

} // nudb

#include <nudbview/impl/basic_store.ipp>

#endif
