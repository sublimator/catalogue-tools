#pragma once

#include "catl/core/types.h"  // For Slice, Hash256, etc.
#include "catl/xdata/fields.h"
#include "catl/xdata/parser-context.h"
#include "catl/xdata/parser-error.h"
#include "catl/xdata/protocol.h"
#include "catl/xdata/slice-cursor.h"
#include "catl/xdata/slice-visitor.h"
#include "catl/xdata/types.h"
#include "catl/xdata/types/amount.h"
#include "catl/xdata/types/pathset.h"
#include <array>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <vector>

namespace catl::xdata {

// Helper functions to check for end markers
inline bool
is_object_end_marker(const FieldDef* field)
{
    return field && field->meta.type == FieldTypes::STObject &&
        field->meta.nth == 1;
}

inline bool
is_array_end_marker(const FieldDef* field)
{
    return field && field->meta.type == FieldTypes::STArray &&
        field->meta.nth == 1;
}

// Forward declarations
inline void
skip_array(ParserContext& ctx, const Protocol& protocol);

// Get fixed size for a type (returns 0 for variable/special types)
inline size_t
get_fixed_size(FieldType type)
{
    return type.fixed_size;
}

// Skip an entire object (find end marker) - only used when visitor returns
// false
inline void
skip_object(ParserContext& ctx, const Protocol& protocol)
{
    while (!ctx.cursor.empty())
    {
        auto [header_slice, field_code] = read_field_header(ctx.cursor);
        if (field_code == 0)
            break;  // Error or end

        const FieldDef* field = protocol.get_field_by_code(field_code);
        if (!field)
            break;  // Unknown field

        // Check for end marker
        if (is_object_end_marker(field))
        {
            break;  // ObjectEndMarker
        }

        // Skip field data
        if (field->meta.type == FieldTypes::STObject)
        {
            skip_object(ctx, protocol);
        }
        else if (field->meta.type == FieldTypes::STArray)
        {
            skip_array(ctx, protocol);
        }
        else if (field->meta.type == FieldTypes::PathSet)
        {
            // PathSet has its own termination protocol
            skip_pathset(ctx);
        }
        else if (field->meta.is_vl_encoded)
        {
            size_t length = read_vl_length(ctx.cursor);
            ctx.cursor.advance(length);
        }
        else if (field->meta.type == FieldTypes::Amount)
        {
            // Amount is special - size depends on first byte
            size_t amount_size = get_amount_size(ctx.cursor.peek_u8());
            ctx.cursor.advance(amount_size);
        }
        else
        {
            // Fixed size
            size_t fixed_size = get_fixed_size(field->meta.type);
            if (fixed_size == 0)
            {
                throw ParserError(
                    "Unknown field type size: " +
                    std::string(field->meta.type.name));
            }
            ctx.cursor.advance(fixed_size);
        }
    }
}

// Skip an entire array - only used when visitor returns false
inline void
skip_array(ParserContext& ctx, const Protocol& protocol)
{
    while (!ctx.cursor.empty())
    {
        auto [header_slice, field_code] = read_field_header(ctx.cursor);
        if (field_code == 0)
            break;

        const FieldDef* field = protocol.get_field_by_code(field_code);
        if (!field)
            break;

        // Check for end marker
        if (is_array_end_marker(field))
        {
            break;  // ArrayEndMarker
        }

        // Arrays contain objects
        skip_object(ctx, protocol);
    }
}

// Main parsing function with visitor
template <SliceVisitor Visitor>
void
parse_with_visitor(
    ParserContext& ctx,
    const Protocol& protocol,
    Visitor&& visitor)
{
    FieldPath path;
    parse_with_visitor_impl(ctx, protocol, visitor, path);
}

// Implementation that maintains the path
template <SliceVisitor Visitor>
void
parse_with_visitor_impl(
    ParserContext& ctx,
    const Protocol& protocol,
    Visitor&& visitor,
    FieldPath& path)
{
    while (!ctx.cursor.empty())
    {
        auto [header_slice, field_code] = read_field_header(ctx.cursor);
        if (field_code == 0)
            break;

        const FieldDef* field = protocol.get_field_by_code(field_code);
        if (!field)
        {
            // Unknown field - skip if possible
            continue;
        }

        // Check for end markers
        if (is_object_end_marker(field) || is_array_end_marker(field))
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
                path.push_back({field, -1});
                parse_with_visitor_impl(ctx, protocol, visitor, path);
                path.pop_back();

                // Read end marker
                auto [end_header, end_code] = read_field_header(ctx.cursor);
                const FieldDef* end_field =
                    protocol.get_field_by_code(end_code);
                if (!is_object_end_marker(end_field))
                {
                    throw ParserError(
                        "Expected ObjectEndMarker but got something else");
                }
            }
            else
            {
                // Skip the object
                skip_object(ctx, protocol);
            }
            visitor.visit_object_end(path, *field);
        }
        else if (field->meta.type == FieldTypes::STArray)
        {
            // Ask visitor if they want to descend
            if (visitor.visit_array_start(path, *field))
            {
                // Add to path
                path.push_back({field, -1});

                size_t element_index = 0;
                while (!ctx.cursor.empty())
                {
                    // Peek for end marker
                    size_t saved_pos = ctx.cursor.pos;
                    auto [peek_header, peek_code] =
                        read_field_header(ctx.cursor);
                    ctx.cursor.pos = saved_pos;  // Rewind

                    const FieldDef* peek_field =
                        protocol.get_field_by_code(peek_code);
                    if (is_array_end_marker(peek_field))
                    {
                        break;  // Found ArrayEndMarker
                    }

                    // Update array index in path
                    path.back().array_index = element_index;

                    if (visitor.visit_array_element(path, element_index))
                    {
                        parse_with_visitor_impl(ctx, protocol, visitor, path);
                    }
                    else
                    {
                        skip_object(ctx, protocol);
                    }
                    element_index++;
                }

                path.pop_back();

                // Read end marker
                auto [end_header2, end_code2] = read_field_header(ctx.cursor);
                const FieldDef* end_field2 =
                    protocol.get_field_by_code(end_code2);
                if (!is_array_end_marker(end_field2))
                {
                    throw ParserError(
                        "Expected ArrayEndMarker but got something else");
                }
            }
            else
            {
                // Skip the array
                skip_array(ctx, protocol);
            }
            visitor.visit_array_end(path, *field);
        }
        else
        {
            // Leaf field - determine size and read
            size_t field_size;

            if (field->meta.is_vl_encoded)
            {
                size_t length_start = ctx.cursor.pos;
                field_size = read_vl_length(ctx.cursor);
                size_t vl_prefix_size = ctx.cursor.pos - length_start;
                // Reset to include VL prefix in the slice
                ctx.cursor.pos = length_start;
                field_size += vl_prefix_size;  // Total size including prefix
            }
            else if (field->meta.type == FieldTypes::Amount)
            {
                // Amount is special - peek at first byte to determine size
                field_size = get_amount_size(ctx.cursor.peek_u8());
            }
            else if (field->meta.type == FieldTypes::PathSet)
            {
                // PathSet - find the end byte to get size
                size_t start_pos = ctx.cursor.pos;
                skip_pathset(ctx);
                field_size = ctx.cursor.pos - start_pos;
                ctx.cursor.pos = start_pos;  // Reset to read the data
            }
            else
            {
                field_size = get_fixed_size(field->meta.type);
                if (field_size == 0)
                {
                    throw ParserError(
                        "Unknown field type size: " +
                        std::string(field->meta.type.name));
                }
            }

            Slice field_data = ctx.cursor.read_slice(field_size);
            path.push_back({field, -1});
            visitor.visit_field(
                path, FieldSlice{field, header_slice, field_data});
            path.pop_back();
        }
    }
}

}  // namespace catl::xdata
