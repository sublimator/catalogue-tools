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

    uint8_t byte1 = cursor.read_u8();
    uint32_t type = byte1 >> 4;
    uint32_t field = byte1 & 0x0F;

    if (type == 0)
    {
        // Type in next byte
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
    else if (field == 0)
    {
        // Field in next byte
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

    size_t header_size = cursor.pos - start_pos;
    Slice header_slice(cursor.data.data() + start_pos, header_size);

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
