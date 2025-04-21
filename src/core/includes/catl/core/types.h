#pragma once

#include <array>
#include <cstring>
#include <cstdint>
#include <string>
#include <atomic>

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
        static Hash256 zero;
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
};

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