//
// Slice Store - Read-only slice database using mmap
// Part of nudbview/view - Read-only slice database implementation
//
// IMPORTANT: Record ordering in NuDB .dat files
// --------------------------------------------
// Records in the .dat file are NOT in insertion order. NuDB buffers inserts
// in a std::map sorted by lexicographic key order (memcmp of raw key bytes).
// On commit, records are written to the .dat file in this sorted key order.
//
// This means:
// - "Record N" = the Nth data record in physical file scan order
// - Record N is NOT necessarily the Nth inserted record
// - Sequential scans traverse records in sorted key order, not insertion order
// - Indexes map record numbers to byte offsets in this physical order
//

#ifndef NUDBVIEW_VIEW_SLICE_STORE_HPP
#define NUDBVIEW_VIEW_SLICE_STORE_HPP

#include <nudbview/error.hpp>
#include <nudbview/file.hpp>
#include <nudbview/type_traits.hpp>
#include <nudbview/detail/bucket.hpp>
#include <nudbview/detail/buffer.hpp>
#include <nudbview/detail/format.hpp>
#include <nudbview/view/format.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <cstddef>
#include <cstdint>

namespace nudbview {
namespace view {

/** A read-only slice database for fast access to a subset of a larger database.

    A slice database provides:
    - Fast hash-based key lookup via slice key file
    - Zero-copy access to data via mmap
    - Index for sequential access and seeking
    - Spill records stored in meta file (not in shared .dat)

    The slice shares the original .dat file (read-only) but has its own
    optimized key file and metadata. Multiple slices can share the same
    .dat file efficiently.

    Example usage:
    @code
        error_code ec;
        slice_store<xxhasher, native_file> ss;
        ss.open("db.dat", "slice-0001-1000.key", "slice-0001-1000.meta", ec);

        ss.fetch(key, [](void const* data, std::size_t size) {
            // Process value
        }, ec);

        ss.close(ec);
    @endcode

    @tparam Hasher The hash function to use. This type
    must meet the requirements of @b Hasher.

    @tparam File The type of File object to use. This type
    must meet the requirements of @b File.
*/
template<class Hasher, class File>
class slice_store
{
public:
    using hash_type = Hasher;
    using file_type = File;

private:
    // Opened files
    boost::iostreams::mapped_file_source dat_mmap_;  // mmap for .dat
    File kf_;                                         // Slice key file
    boost::iostreams::mapped_file_source meta_mmap_; // mmap for .meta

    // Headers
    detail::dat_file_header dh_;
    detail::key_file_header kh_;
    slice_meta_header smh_;

    // Hasher state
    Hasher hasher_;

    // File paths (for error reporting)
    path_type dat_path_;
    path_type key_path_;
    path_type meta_path_;

    // Open flag
    bool open_ = false;

public:
    /** Constructor with salt.

        @param salt The salt value to initialize the hasher
    */
    explicit slice_store(std::uint64_t salt)
        : hasher_(salt)
    {
    }

    /// Copy constructor (disallowed)
    slice_store(slice_store const&) = delete;

    /// Copy assignment (disallowed)
    slice_store& operator=(slice_store const&) = delete;

    /** Destroy the slice store.

        Files are closed. This is read-only so no data is lost.
    */
    ~slice_store();

    /** Returns `true` if the slice store is open.

        @par Thread safety

        Safe to call concurrently with @ref fetch.
    */
    bool
    is_open() const
    {
        return open_;
    }

    /** Return the path to the data file.

        @par Requirements

        The slice store must be open.

        @par Thread safety

        Safe to call concurrently with @ref fetch.

        @return The data file path.
    */
    path_type const&
    dat_path() const;

    /** Return the path to the slice key file.

        @par Requirements

        The slice store must be open.

        @par Thread safety

        Safe to call concurrently with @ref fetch.

        @return The slice key file path.
    */
    path_type const&
    key_path() const;

    /** Return the path to the slice meta file.

        @par Requirements

        The slice store must be open.

        @par Thread safety

        Safe to call concurrently with @ref fetch.

        @return The slice meta file path.
    */
    path_type const&
    meta_path() const;

    /** Return the appnum associated with the database.

        @par Requirements

        The slice store must be open.

        @par Thread safety

        Safe to call concurrently with @ref fetch.

        @return The appnum.
    */
    std::uint64_t
    appnum() const;

    /** Return the key size associated with the database.

        @par Requirements

        The slice store must be open.

        @par Thread safety

        Safe to call concurrently with @ref fetch.

        @return The size of keys in the database.
    */
    std::size_t
    key_size() const;

    /** Return the block size associated with the key file.

        @par Requirements

        The slice store must be open.

        @par Thread safety

        Safe to call concurrently with @ref fetch.

        @return The size of blocks in the key file.
    */
    std::size_t
    block_size() const;

    /** Return the number of keys in this slice.

        @par Requirements

        The slice store must be open.

        @par Thread safety

        Safe to call concurrently with @ref fetch.

        @return The number of keys in this slice.
    */
    std::uint64_t
    key_count() const;

    /** Return the slice start offset in the .dat file.

        @par Requirements

        The slice store must be open.

        @par Thread safety

        Safe to call concurrently with @ref fetch.

        @return Byte offset of first record in slice.
    */
    noff_t
    slice_start_offset() const;

    /** Return the slice end offset in the .dat file.

        @par Requirements

        The slice store must be open.

        @par Thread safety

        Safe to call concurrently with @ref fetch.

        @return Byte offset of last record in slice.
    */
    noff_t
    slice_end_offset() const;

    /** Close the slice store.

        If an error occurs, the slice store is still closed.

        @par Requirements

        The slice store must be open.

        @par Thread safety

        Not thread safe. The caller is responsible for
        ensuring that no other member functions are
        called concurrently.

        @param ec Set to the error, if any occurred.
    */
    void
    close(error_code& ec);

    /** Open a slice database.

        The slice database identified by the specified data, key, and
        meta file paths is opened.

        @par Requirements

        The slice store must not be open.

        @par Thread safety

        Not thread safe. The caller is responsible for
        ensuring that no other member functions are
        called concurrently.

        @param dat_path The path to the data file (shared, read-only).

        @param slice_key_path The path to the slice key file.

        @param slice_meta_path The path to the slice meta file.

        @param ec Set to the error, if any occurred.

        @param args Optional arguments passed to @b File constructors.
    */
    template<class... Args>
    void
    open(
        path_type const& dat_path,
        path_type const& slice_key_path,
        path_type const& slice_meta_path,
        error_code& ec,
        Args&&... args);

    /** Fetch a value.

        The function checks the slice database for the specified
        key, and invokes the callback if it is found. If
        the key is not found, `ec` is set to @ref error::key_not_found.
        If any other errors occur, `ec` is set to the
        corresponding error.

        @par Requirements

        The slice store must be open.

        @par Thread safety

        Safe to call concurrently.

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
    fetch(void const* key, Callback&& callback, error_code& ec);

private:
    // Fetch key in loaded bucket b or its spills (in meta file!)
    template<class Callback>
    void
    fetch(
        detail::nhash_t h,
        void const* key,
        detail::bucket b,
        Callback&& callback,
        error_code& ec);
};

} // view
} // nudbview

#include <nudbview/impl/view/slice_store.ipp>

#endif
