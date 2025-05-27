#pragma once

#include "catl/core/types.h"  // For Slice
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>  // For std::pair

namespace catl::xdata {

// Custom exception for SliceCursor errors
class SliceCursorError : public std::runtime_error
{
public:
    explicit SliceCursorError(const std::string& msg)
        : std::runtime_error("SliceCursor: " + msg)
    {
    }
};

// Cursor for tracking position in a Slice
struct SliceCursor
{
    Slice data;
    size_t pos = 0;

    bool
    empty() const
    {
        return pos >= data.size();
    }
    size_t
    remaining_size() const
    {
        return data.size() - pos;
    }
    Slice
    remaining() const
    {
        return data.subslice(pos);
    }

    uint8_t
    peek_u8() const
    {
        if (pos >= data.size())
        {
            throw SliceCursorError("peek past end of data");
        }
        return data.data()[pos];
    }

    uint8_t
    read_u8()
    {
        if (pos >= data.size())
        {
            throw SliceCursorError("read_u8 past end of data");
        }
        return data.data()[pos++];
    }

    uint16_t
    read_uint16_be()
    {
        if (pos + 2 > data.size())
        {
            throw SliceCursorError("read_uint16_be past end of data");
        }
        uint16_t result = (static_cast<uint16_t>(data.data()[pos]) << 8) |
            data.data()[pos + 1];
        pos += 2;
        return result;
    }

    uint32_t
    read_uint32_be()
    {
        if (pos + 4 > data.size())
        {
            throw SliceCursorError("read_uint32_be past end of data");
        }
        uint32_t result = (static_cast<uint32_t>(data.data()[pos]) << 24) |
            (static_cast<uint32_t>(data.data()[pos + 1]) << 16) |
            (static_cast<uint32_t>(data.data()[pos + 2]) << 8) |
            data.data()[pos + 3];
        pos += 4;
        return result;
    }

    uint64_t
    read_uint64_be()
    {
        if (pos + 8 > data.size())
        {
            throw SliceCursorError("read_uint64_be past end of data");
        }
        uint64_t result = 0;
        for (int i = 0; i < 8; ++i)
        {
            result = (result << 8) | data.data()[pos + i];
        }
        pos += 8;
        return result;
    }

    void
    advance(size_t n)
    {
        pos += n;
    }
    Slice
    read_slice(size_t n)
    {
        if (pos + n > data.size())
        {
            throw SliceCursorError(
                "attempted to read " + std::to_string(n) + " bytes, only " +
                std::to_string(data.size() - pos) + " available");
        }
        Slice result(data.data() + pos, n);
        pos += n;
        return result;
    }
};

// Read field header and return field code with the header bytes
// Returns: pair of (field header slice, field code)
// The field code is 0 on error/end
// TODO: this should throw, wtf
inline std::pair<Slice, uint32_t>
read_field_header(SliceCursor& cursor)
{
    size_t start_pos = cursor.pos;

    if (cursor.empty())
    {
        return {Slice{}, 0};
    }

    // Read the tag byte
    uint8_t byte1 = cursor.read_u8();

    // Extract type bits from upper 4 bits
    uint32_t type = byte1 >> 4;

    // Extract field bits from lower 4 bits
    uint32_t field = byte1 & 0x0F;

    // If type bits are 0, type code is in next byte
    if (type == 0)
    {
        if (cursor.empty())
        {
            return {Slice{}, 0};
        }
        type = cursor.read_u8();
        if (type == 0 || type < 16)
        {
            return {Slice{}, 0};
        }
    }

    // If field bits are 0, field code is in next byte
    if (field == 0)
    {
        if (cursor.empty())
        {
            return {Slice{}, 0};
        }
        field = cursor.read_u8();
        if (field == 0 || field < 16)
        {
            return {Slice{}, 0};
        }
    }

    // Create slice containing the entire field header
    size_t header_size = cursor.pos - start_pos;
    Slice header_slice(cursor.data.data() + start_pos, header_size);

    // Return header slice and combined field code
    return {header_slice, (type << 16) | field};
}

// Read variable length prefix
inline size_t
read_vl_length(SliceCursor& cursor)
{
    uint8_t byte1 = cursor.read_u8();

    if (byte1 <= 192)
    {
        return byte1;
    }
    else if (byte1 <= 240)
    {
        uint8_t byte2 = cursor.read_u8();
        return 193 + ((byte1 - 193) * 256) + byte2;
    }
    else if (byte1 <= 254)
    {
        uint8_t byte2 = cursor.read_u8();
        uint8_t byte3 = cursor.read_u8();
        return 12481 + ((byte1 - 241) * 65536) + (byte2 * 256) + byte3;
    }

    // Invalid VL encoding
    throw SliceCursorError(
        "Invalid VL encoding: first byte = " +
        std::to_string(static_cast<int>(byte1)));
}

}  // namespace catl::xdata
