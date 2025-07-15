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
#include "catl/xdata/types/issue.h"
#include "catl/xdata/types/number.h"
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
get_fixed_size(const FieldType& type)
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
        {
            // Ugh
            throw ParserError("Error or end");
        }

        const FieldDef* field = protocol.get_field_by_code(field_code);
        if (!field)
        {
            throw ParserError(
                "Unknown field code: " + std::to_string(field_code));
        }

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
        else if (field->meta.type == FieldTypes::Issue)
        {
            // Issue is special - 20 bytes for XRP, 40 bytes for non-XRP
            size_t issue_size = get_issue_size(ctx.cursor);
            ctx.cursor.advance(issue_size);
        }
        else if (field->meta.type == FieldTypes::Number)
        {
            // Number is always 12 bytes (8 bytes mantissa + 4 bytes exponent)
            ctx.cursor.advance(12);
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

        // Array elements are: Field header + STObject content + ObjectEndMarker
        // We already read the field header, now skip the object content
        if (field->meta.type == FieldTypes::STObject)
        {
            // skip_object will consume everything up to and including the
            // ObjectEndMarker
            skip_object(ctx, protocol);
        }
        else
        {
            throw ParserError("Array elements must be STObject type");
        }
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
            throw ParserError(
                "Unknown field code: " + std::to_string(field_code));
        }

        // Check for end markers
        if (is_object_end_marker(field) || is_array_end_marker(field))
        {
            // End marker - we're done with this level
            break;
        }

        if (field->meta.type == FieldTypes::STObject)
        {
            // Track start position for size calculation
            size_t start_pos = ctx.cursor.pos - header_slice.size();

            // Create FieldSlice with empty data (not parsed yet)
            FieldSlice start_slice{field, header_slice, Slice{}};

            // Ask visitor if they want to descend
            if (visitor.visit_object_start(path, start_slice))
            {
                // Add to path and recurse
                path.push_back({field, -1});
                parse_with_visitor_impl(ctx, protocol, visitor, path);
                path.pop_back();

                // parse_with_visitor_impl consumes the ObjectEndMarker when it
                // sees it
            }
            else
            {
                // Skip the object
                skip_object(ctx, protocol);
            }
            // Create FieldSlice with complete object data
            size_t end_pos = ctx.cursor.pos;
            size_t object_size = end_pos - start_pos - header_slice.size();
            Slice object_data{
                ctx.cursor.data.data() + start_pos + header_slice.size(),
                object_size};
            FieldSlice end_slice{field, header_slice, object_data};

            visitor.visit_object_end(path, end_slice);
        }
        else if (field->meta.type == FieldTypes::STArray)
        {
            // Track start position for size calculation
            size_t start_pos = ctx.cursor.pos - header_slice.size();

            // Create FieldSlice with empty data (not parsed yet)
            FieldSlice start_slice{field, header_slice, Slice{}};

            // Ask visitor if they want to descend
            if (visitor.visit_array_start(path, start_slice))
            {
                // Add to path
                path.push_back({field, -1});

                size_t element_index = 0;
                while (!ctx.cursor.empty())
                {
                    // Read the field header for this array element
                    auto [elem_header, elem_code] =
                        read_field_header(ctx.cursor);
                    if (elem_code == 0)
                        break;

                    const FieldDef* elem_field =
                        protocol.get_field_by_code(elem_code);
                    if (is_array_end_marker(elem_field))
                    {
                        // Rewind since we'll read it again outside the loop
                        ctx.cursor.pos -= elem_header.size();
                        break;  // Found ArrayEndMarker
                    }

                    // Update array index in path
                    path.back().array_index = element_index;

                    // Array elements in XRPL:
                    // - Field header (like CreatedNode) indicates element type
                    // - Followed by object contents (fields)
                    // - Terminated by ObjectEndMarker

                    if (elem_field->meta.type == FieldTypes::STObject)
                    {
                        // Add the element type field to path
                        path.push_back({elem_field, -1});

                        // Track start position for size calculation
                        size_t elem_start_pos =
                            ctx.cursor.pos - elem_header.size();

                        // Create FieldSlice with empty data (not parsed yet)
                        FieldSlice elem_start_slice{
                            elem_field, elem_header, Slice{}};

                        // Parse the STObject content
                        if (visitor.visit_object_start(path, elem_start_slice))
                        {
                            // Parse the object's fields
                            parse_with_visitor_impl(
                                ctx, protocol, visitor, path);
                        }
                        else
                        {
                            skip_object(ctx, protocol);
                        }

                        // Create FieldSlice with complete object data
                        size_t elem_end_pos = ctx.cursor.pos;
                        size_t elem_size =
                            elem_end_pos - elem_start_pos - elem_header.size();
                        Slice elem_data{
                            ctx.cursor.data.data() + elem_start_pos +
                                elem_header.size(),
                            elem_size};
                        FieldSlice elem_end_slice{
                            elem_field, elem_header, elem_data};

                        visitor.visit_object_end(path, elem_end_slice);

                        path.pop_back();
                    }
                    else
                    {
                        throw ParserError(
                            "Array elements must be STObject type, got: " +
                            std::string(elem_field->meta.type.name));
                    }
                    element_index++;
                }

                path.pop_back();

                // Read the ArrayEndMarker (we rewound when we saw it)
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
            // Create FieldSlice with complete array data
            size_t end_pos = ctx.cursor.pos;
            size_t array_size = end_pos - start_pos - header_slice.size();
            Slice array_data{
                ctx.cursor.data.data() + start_pos + header_slice.size(),
                array_size};
            FieldSlice end_slice{field, header_slice, array_data};

            visitor.visit_array_end(path, end_slice);
        }
        else
        {
            // Leaf field - determine size and read
            size_t field_size;

            if (field->meta.is_vl_encoded)
            {
                field_size = read_vl_length(ctx.cursor);
                // field_size is now the actual data size, cursor is positioned
                // after VL prefix
            }
            else if (field->meta.type == FieldTypes::Amount)
            {
                // Amount is special - peek at first byte to determine size
                field_size = get_amount_size(ctx.cursor.peek_u8());
            }
            else if (field->meta.type == FieldTypes::Issue)
            {
                // Issue is special - 20 bytes for XRP, 40 bytes for non-XRP
                field_size = get_issue_size(ctx.cursor);
            }
            else if (field->meta.type == FieldTypes::Number)
            {
                // Number is always 12 bytes (8 bytes mantissa + 4 bytes exponent)
                field_size = 12;
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
