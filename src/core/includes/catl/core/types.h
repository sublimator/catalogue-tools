#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>

/**
 * Zero-copy reference to a data buffer.
 * Used to minimize copying when working with memory-mapped data.
 */
class Slice
{
private:
    const uint8_t* data_;
    size_t size_;

public:
    Slice() : data_(nullptr), size_(0)
    {
    }
    Slice(const uint8_t* data, size_t size) : data_(data), size_(size)
    {
    }

    const uint8_t*
    data() const
    {
        return data_;
    }
    size_t
    size() const
    {
        return size_;
    }
    bool
    empty() const
    {
        return size_ == 0;
    }
    std::span<const uint8_t>
    span() const
    {
        return {data_, size_};
    }

    Slice
    subslice(size_t pos) const
    {
        return {data_ + pos, size_ - pos};
    }

    bool
    eq(const Slice& other) const
    {
        return size_ == other.size_ &&
            std::memcmp(data_, other.data_, size_) == 0;
    }
};

/**
 * Utility function to convert a data slice to hexadecimal representation
 */
void
slice_hex(Slice sl, std::string& result);

/**
 * 256-bit hash value with utility methods
 */
class Hash256
{
private:
    std::array<uint8_t, 32> data_;

public:
    Hash256() : data_()
    {
        data_.fill(0);
    }
    explicit Hash256(const std::array<uint8_t, 32>& data) : data_(data)
    {
    }
    explicit Hash256(const uint8_t* data) : data_()
    {
        std::memcpy(data_.data(), data, 32);
    }

    uint8_t*
    data()
    {
        return data_.data();
    }
    const uint8_t*
    data() const
    {
        return data_.data();
    }
    static constexpr std::size_t
    size()
    {
        return 32;
    }

    static Hash256 const&
    zero()
    {
        static const Hash256 zero{};
        return zero;
    }

    bool
    operator==(const Hash256& other) const
    {
        return data_ == other.data_;
    }
    bool
    operator!=(const Hash256& other) const
    {
        return !(*this == other);
    }
    /// Compare against raw pointer (32 bytes assumed).
    bool
    matches(const uint8_t* ptr) const
    {
        return std::memcmp(data_.data(), ptr, 32) == 0;
    }

    /// Find this hash in a buffer. Returns byte offset or -1 if not found.
    int
    find_in(const uint8_t* data, size_t size) const
    {
        for (size_t i = 0; i + 32 <= size; ++i)
            if (matches(data + i))
                return static_cast<int>(i);
        return -1;
    }

    /// Find this hash in a Slice.
    int
    find_in(Slice const& s) const
    {
        return find_in(s.data(), s.size());
    }
    bool
    operator<(const Hash256& other) const
    {
        return data_ < other.data_;
    }

    // Return hex string representation of the hash
    [[nodiscard]] std::string
    hex() const;
};

/**
 * Reference to a 32-byte key in memory-mapped data
 */
class Key
{
private:
    const std::uint8_t* data_;

public:
    Key(const std::uint8_t* data) : data_(data)
    {
    }

    const std::uint8_t*
    data() const
    {
        return data_;
    }
    static constexpr std::size_t
    size()
    {
        return 32;
    }
    Hash256
    to_hash() const
    {
        return Hash256(data_);
    }
    std::string
    hex() const
    {
        return to_hash().hex();
    }

    bool
    operator==(const Key& other) const
    {
        return std::memcmp(data_, other.data_, 32) == 0;
    }
    bool
    operator!=(const Key& other) const
    {
        return !(*this == other);
    }

    // Add comparison operator for use in ordered containers
    bool
    operator<(const Key& other) const
    {
        return std::memcmp(data_, other.data_, 32) < 0;
    }
};

// TODO: make this all const
/**
 * Combines a key with its associated data from memory-mapped storage
 */
class MmapItem
{
private:
    Key key_;
    Slice data_;
    mutable std::atomic<int> refCount_{0};

public:
    MmapItem(
        const std::uint8_t* keyData,
        const std::uint8_t* data,
        std::size_t dataSize)
        : key_(keyData), data_(data, dataSize)
    {
    }

    virtual ~MmapItem() = default;

    const Key&
    key() const
    {
        return key_;
    }
    Slice
    key_slice() const
    {
        return {key_.data(), Key::size()};
    }
    const Slice&
    slice() const
    {
        return data_;
    }

    std::string
    hex() const;

    void
    intrusive_ptr_release(MmapItem* p);

    // Friend declarations needed for boost::intrusive_ptr
    friend void
    intrusive_ptr_add_ref(MmapItem* p);
    friend void
    intrusive_ptr_release(MmapItem* p);
};

/**
 * MmapItem subclass that owns its memory.
 * MmapItem itself borrows pointers (e.g. into mmap'd storage).
 * OwnedItem copies key + data into a single owned allocation.
 */
class OwnedItem : public MmapItem
{
    std::unique_ptr<uint8_t[]> storage_;

    OwnedItem(
        const uint8_t* key_ptr,
        const uint8_t* data_ptr,
        std::size_t data_size,
        std::unique_ptr<uint8_t[]> storage)
        : MmapItem(key_ptr, data_ptr, data_size), storage_(std::move(storage))
    {
    }

public:
    /// Create from separate key and data. Single allocation: [32-byte
    /// key][data]. Caller wraps in boost::intrusive_ptr<MmapItem> as needed.
    static OwnedItem*
    create(Hash256 const& key, Slice const& data)
    {
        auto storage = std::make_unique<uint8_t[]>(32 + data.size());
        std::memcpy(storage.get(), key.data(), 32);
        std::memcpy(storage.get() + 32, data.data(), data.size());
        auto* key_ptr = storage.get();
        auto* data_ptr = storage.get() + 32;
        return new OwnedItem(
            key_ptr, data_ptr, data.size(), std::move(storage));
    }
};

/**
 * MmapItem subclass for hash-only placeholder nodes in abbreviated trees.
 *
 * Carries a precomputed subtree hash + SHAMap position key inline.
 * Data slice is empty (size 0). When a SHAMap leaf is constructed from
 * a HashItem, it sets placeholder_=true and update_hash() becomes a
 * no-op — the hash is already known.
 *
 * Used for proof paths: off-path branches are replaced with HashItems
 * that preserve the tree's root hash without storing the actual data.
 */
class HashItem : public MmapItem
{
    uint8_t key_storage_[32];
    Hash256 hash_;

public:
    HashItem(Hash256 const& key, Hash256 const& subtree_hash)
        : MmapItem(
              key_storage_,
              key_storage_,
              0)  // empty slice, non-null pointer
        , hash_(subtree_hash)
    {
        std::memcpy(key_storage_, key.data(), 32);
    }

    Hash256 const&
    precomputed_hash() const
    {
        return hash_;
    }

    static HashItem*
    create(Hash256 const& key, Hash256 const& subtree_hash)
    {
        return new HashItem(key, subtree_hash);
    }
};
