#pragma once

#include "catl/core/types.h"  // For Slice, Hash256, etc.
#include "catl/xdata/fields.h"
#include "catl/xdata/protocol.h"
#include "catl/xdata/types.h"
#include <array>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <vector>

namespace catl::xdata {

// TODO: move this to a separate file, slice-cursor.h
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

    // TODO: check for usages for this, I think I saw some need for it!?
    uint8_t
    peek_u8() const
    {
        if (pos >= data.size())
        {
            // TODO: create a better class for this
            throw std::runtime_error("SliceCursor: peek past end of data");
        }
        return data.data()[pos];
    }

    uint8_t
    read_u8()
    {
        if (pos >= data.size())
        {
            throw std::runtime_error("SliceCursor: read_u8 past end of data");
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
            throw std::runtime_error(
                "SliceCursor: attempted to read " + std::to_string(n) +
                " bytes, only " + std::to_string(data.size() - pos) +
                " available");
        }
        Slice result(data.data() + pos, n);
        pos += n;
        return result;
    }
};

// Path element for tracking location in parse tree
struct PathElement
{
    const FieldDef* field;
    // maybe just collapse this into a single int with -1 as not an array
    // element?
    size_t array_index = 0;  // Only used if inside an array
    bool is_array_element = false;
};

// Path through the parsed structure
using FieldPath = std::vector<PathElement>;

// A slice of field data with its metadata
struct FieldSlice
{
    // TODO: maybe just encode the field as a slice too? or as well as the
    // actual field def
    const FieldDef* field;
    Slice data;
};

// TODO: move to src/x-data/includes/catl/xdata/slice-visitor.h
// Visitor concept for compile-time interface checking
template <typename T>
concept SliceVisitor =
    requires(T v, const FieldPath& path, const FieldDef& f, Slice s, size_t idx)
{
    {
        v.visit_object_start(path, f)
    }
    ->std::convertible_to<bool>;
    {
        v.visit_object_end(path, f)
    }
    ->std::same_as<void>;
    {
        v.visit_array_start(path, f)
    }
    ->std::convertible_to<bool>;
    {
        v.visit_array_end(path, f)
    }
    ->std::same_as<void>;
    {
        v.visit_array_element(path, idx)
    }
    ->std::convertible_to<bool>;
    {
        v.visit_field(path, f, s)
    }
    ->std::same_as<void>;
};

// Forward declarations
inline void
skip_array(SliceCursor& cursor, const Protocol& protocol);

// TODO: move this to a separate file with the SliceCursor ? and other little
// utilslike read_vl_length Read field header and return field code
// TODO: return a std::pair<Slice, uint32_t> instead, the slice is the field
// header slice
inline uint32_t
read_field_header(SliceCursor& cursor)
{
    if (cursor.empty())
        return 0;  // TODO: just let cursor throw and clients guard

    uint8_t byte1 = cursor.read_u8();
    uint32_t type = byte1 >> 4;
    uint32_t field = byte1 & 0x0F;

    if (type == 0)
    {
        // Type in next byte
        if (cursor.empty())
            return 0;
        type = cursor.read_u8();
        if (type == 0 || type < 16)
            return 0;
    }
    else if (field == 0)
    {
        // Field in next byte
        if (cursor.empty())
            return 0;
        field = cursor.read_u8();
        if (field == 0 || field < 16)
            return 0;
    }

    return (type << 16) | field;
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
    throw std::runtime_error(
        "Invalid VL encoding: first byte = " +
        std::to_string(static_cast<int>(byte1)));
}

// TODO: move to src/x-data/includes/catl/xdata/types.h
// Get size for Amount type by peeking at first byte
inline size_t
get_amount_size(const SliceCursor& cursor)
{
    if (cursor.empty())
    {
        throw std::runtime_error(
            "Cannot determine Amount size: no data available");
    }
    uint8_t first_byte = cursor.peek_u8();
    bool is_iou = (first_byte & 0x80) != 0;
    return is_iou ? 48 : 8;  // IOU: 8 + 20 + 20, XRP: just 8
}

// Get fixed size for a type (returns 0 for variable/special types)
inline size_t
get_fixed_size(FieldType type)
{
    return type.fixed_size;
}

// TODO: move to src/x-data/includes/catl/xdata/types/pathset.h
// Skip a PathSet (has its own termination protocol)
inline void
skip_pathset(SliceCursor& cursor)
{
    constexpr uint8_t PATHSET_END_BYTE = 0x00;
    constexpr uint8_t PATH_SEPARATOR_BYTE = 0xFF;
    constexpr uint8_t TYPE_ACCOUNT = 0x01;
    constexpr uint8_t TYPE_CURRENCY = 0x10;
    constexpr uint8_t TYPE_ISSUER = 0x20;

    while (!cursor.empty())
    {
        uint8_t type_byte = cursor.read_u8();

        if (type_byte == PATHSET_END_BYTE)
        {
            break;  // End of PathSet
        }

        if (type_byte == PATH_SEPARATOR_BYTE)
        {
            continue;  // Path separator, next path
        }

        // It's a hop - type byte tells us what follows
        if (type_byte & TYPE_ACCOUNT)
        {
            cursor.advance(20);  // AccountID
        }
        if (type_byte & TYPE_CURRENCY)
        {
            cursor.advance(20);  // Currency
        }
        if (type_byte & TYPE_ISSUER)
        {
            cursor.advance(20);  // AccountID as issuer
        }
    }
}

// Skip an entire object (find end marker) - only used when visitor returns
// false
inline void
skip_object(SliceCursor& cursor, const Protocol& protocol)
{
    while (!cursor.empty())
    {
        uint32_t field_code = read_field_header(cursor);
        if (field_code == 0)
            break;  // Error or end

        const FieldDef* field = protocol.get_field_by_code(field_code);
        if (!field)
            break;  // Unknown field

        // Check for end marker
        if (field->meta.type == FieldTypes::STObject && field->meta.nth == 1)
        {
            break;  // ObjectEndMarker
        }

        // Skip field data
        if (field->meta.type == FieldTypes::STObject)
        {
            skip_object(cursor, protocol);
        }
        else if (field->meta.type == FieldTypes::STArray)
        {
            skip_array(cursor, protocol);
        }
        else if (field->meta.type == FieldTypes::PathSet)
        {
            // PathSet has its own termination protocol
            skip_pathset(cursor);
        }
        else if (field->meta.is_vl_encoded)
        {
            size_t length = read_vl_length(cursor);
            cursor.advance(length);
        }
        else if (field->meta.type == FieldTypes::Amount)
        {
            // Amount is special - size depends on first byte
            size_t amount_size = get_amount_size(cursor);
            cursor.advance(amount_size);
        }
        else
        {
            // Fixed size
            size_t fixed_size = get_fixed_size(field->meta.type);
            if (fixed_size == 0)
            {
                throw std::runtime_error(
                    "Unknown field type size: " +
                    std::string(field->meta.type.name));
            }
            cursor.advance(fixed_size);
        }
    }
}

// Skip an entire array - only used when visitor returns false
inline void
skip_array(SliceCursor& cursor, const Protocol& protocol)
{
    while (!cursor.empty())
    {
        uint32_t field_code = read_field_header(cursor);
        if (field_code == 0)
            break;

        const FieldDef* field = protocol.get_field_by_code(field_code);
        if (!field)
            break;

        // Check for end marker
        if (field->meta.type == FieldTypes::STArray && field->meta.nth == 1)
        {
            break;  // ArrayEndMarker
        }

        // Arrays contain objects
        skip_object(cursor, protocol);
    }
}

// Main parsing function with visitor
template <SliceVisitor Visitor>
void
parse_with_visitor(
    SliceCursor& cursor,
    const Protocol& protocol,
    Visitor&& visitor)
{
    FieldPath path;
    parse_with_visitor_impl(cursor, protocol, visitor, path);
}

// Implementation that maintains the path
template <SliceVisitor Visitor>
void
parse_with_visitor_impl(
    SliceCursor& cursor,
    const Protocol& protocol,
    Visitor&& visitor,
    FieldPath& path)
{
    while (!cursor.empty())
    {
        uint32_t field_code = read_field_header(cursor);
        if (field_code == 0)
            break;

        const FieldDef* field = protocol.get_field_by_code(field_code);
        if (!field)
        {
            // Unknown field - skip if possible
            continue;
        }

        // Check for end markers
        if ((field->meta.type == FieldTypes::STObject &&
             field->meta.nth == 1) ||
            (field->meta.type == FieldTypes::STArray && field->meta.nth == 1))
        {
            // End marker - we're done with this level
            break;
        }

        if (field->meta.type == FieldTypes::STObject)
        {
            // Ask visitor if they want to descend
            if (visitor.visit_object_start(path, *field))
            {
                // Add to path and recurse
                path.push_back({field, 0, false});
                parse_with_visitor_impl(cursor, protocol, visitor, path);
                path.pop_back();

                // Read end marker
                read_field_header(cursor);
            }
            else
            {
                // Skip the object
                skip_object(cursor, protocol);
            }
            visitor.visit_object_end(path, *field);
        }
        else if (field->meta.type == FieldTypes::STArray)
        {
            // Ask visitor if they want to descend
            if (visitor.visit_array_start(path, *field))
            {
                // Add to path
                path.push_back({field, 0, false});

                size_t element_index = 0;
                while (!cursor.empty())
                {
                    // Peek for end marker
                    size_t saved_pos = cursor.pos;
                    uint32_t peek_code = read_field_header(cursor);
                    cursor.pos = saved_pos;  // Rewind

                    const FieldDef* peek_field =
                        protocol.get_field_by_code(peek_code);
                    if (peek_field &&
                        peek_field->meta.type == FieldTypes::STArray &&
                        peek_field->meta.nth == 1)
                    {
                        break;  // Found ArrayEndMarker
                    }

                    // Update array index in path
                    path.back().array_index = element_index;
                    path.back().is_array_element = true;

                    if (visitor.visit_array_element(path, element_index))
                    {
                        parse_with_visitor_impl(
                            cursor, protocol, visitor, path);
                    }
                    else
                    {
                        skip_object(cursor, protocol);
                    }
                    element_index++;
                }

                path.pop_back();

                // Read end marker
                read_field_header(cursor);
            }
            else
            {
                // Skip the array
                skip_array(cursor, protocol);
            }
            visitor.visit_array_end(path, *field);
        }
        else
        {
            // Leaf field - determine size and read
            size_t field_size;

            if (field->meta.is_vl_encoded)
            {
                size_t length_start = cursor.pos;
                field_size = read_vl_length(cursor);
                size_t vl_prefix_size = cursor.pos - length_start;
                // Reset to include VL prefix in the slice
                cursor.pos = length_start;
                field_size += vl_prefix_size;  // Total size including prefix
            }
            else if (field->meta.type == FieldTypes::Amount)
            {
                // Amount is special - peek at first byte to determine size
                field_size = get_amount_size(cursor);
            }
            else if (field->meta.type == FieldTypes::PathSet)
            {
                // PathSet - find the end byte to get size
                size_t start_pos = cursor.pos;
                skip_pathset(cursor);
                field_size = cursor.pos - start_pos;
                cursor.pos = start_pos;  // Reset to read the data
            }
            else
            {
                field_size = get_fixed_size(field->meta.type);
                if (field_size == 0)
                {
                    throw std::runtime_error(
                        "Unknown field type size: " +
                        std::string(field->meta.type.name));
                }
            }

            Slice field_data = cursor.read_slice(field_size);
            visitor.visit_field(path, *field, field_data);
        }
    }
}

// TODO: move to src/x-data/includes/catl/xdata/slice-visitor.h
// Example visitor that just emits slices
class SimpleSliceEmitter
{
    std::function<void(const FieldSlice&)> emit_;

public:
    explicit SimpleSliceEmitter(std::function<void(const FieldSlice&)> emit)
        : emit_(emit)
    {
    }

    bool
    visit_object_start(const FieldPath&, const FieldDef&)
    {
        return true;  // Always descend
    }
    void
    visit_object_end(const FieldPath&, const FieldDef&)
    {
    }

    bool
    visit_array_start(const FieldPath&, const FieldDef&)
    {
        return true;
    }
    void
    visit_array_end(const FieldPath&, const FieldDef&)
    {
    }

    bool
    visit_array_element(const FieldPath&, size_t)
    {
        return true;
    }

    void
    visit_field(const FieldPath&, const FieldDef& field, Slice data)
    {
        emit_({&field, data});
    }
};

// TODO: move to src/x-data/includes/catl/xdata/slice-visitor.h
// Example visitor that only processes top-level fields
class TopLevelOnlyVisitor
{
    std::function<void(const FieldSlice&)> process_;

public:
    explicit TopLevelOnlyVisitor(std::function<void(const FieldSlice&)> proc)
        : process_(proc)
    {
    }

    bool
    visit_object_start(const FieldPath& path, const FieldDef&)
    {
        return path.empty();  // Only descend into root object
    }
    void
    visit_object_end(const FieldPath&, const FieldDef&)
    {
    }

    bool
    visit_array_start(const FieldPath&, const FieldDef&)
    {
        return false;  // Never descend into arrays
    }
    void
    visit_array_end(const FieldPath&, const FieldDef&)
    {
    }

    bool
    visit_array_element(const FieldPath&, size_t)
    {
        return false;
    }

    void
    visit_field(const FieldPath& path, const FieldDef& field, Slice data)
    {
        if (path.size() == 1)
        {  // Top-level field
            process_({&field, data});
        }
    }
};

}  // namespace catl::xdata
