#pragma once

#include "catl/xdata/slice-visitor.h"
#include <iomanip>
#include <iostream>
#include <string>

namespace catl::xdata {

// Debug visitor that prints a tree structure with field names and hex values
class DebugTreeVisitor
{
    std::ostream& out_;

    std::string
    get_indent(const FieldPath& path) const
    {
        return std::string(path.size() * 2, ' ');  // 2 spaces per level
    }

    std::string
    to_hex(const Slice& slice) const
    {
        std::ostringstream oss;
        oss << std::uppercase;
        for (size_t i = 0; i < slice.size(); ++i)
        {
            oss << std::hex << std::setw(2) << std::setfill('0')
                << static_cast<int>(
                       static_cast<unsigned char>(slice.data()[i]));
        }
        return oss.str();
    }

public:
    explicit DebugTreeVisitor(std::ostream& out = std::cout) : out_(out)
    {
    }

    bool
    visit_object_start(const FieldPath& path, const FieldDef& field)
    {
        out_ << get_indent(path) << field.name << " {\n";
        return true;  // Always descend
    }

    void
    visit_object_end(const FieldPath& path, const FieldDef&)
    {
        out_ << get_indent(path) << "}\n";
    }

    bool
    visit_array_start(const FieldPath& path, const FieldDef& field)
    {
        out_ << get_indent(path) << field.name << " [\n";
        return true;  // Always descend
    }

    void
    visit_array_end(const FieldPath& path, const FieldDef&)
    {
        out_ << get_indent(path) << "]\n";
    }

    bool
    visit_array_element(const FieldPath& path, size_t index)
    {
        out_ << get_indent(path) << "[" << index << "]:\n";
        return true;  // Always descend
    }

    void
    visit_field(const FieldPath& path, const FieldSlice& fs)
    {
        const FieldDef& field = fs.get_field();

        out_ << get_indent(path) << field.name << ": ";

        // Show header hex if present
        if (!fs.header.empty())
        {
            out_ << "header=" << to_hex(fs.header) << " ";
        }

        // Show data hex
        if (!fs.data.empty())
        {
            out_ << "data=" << to_hex(fs.data);
        }

        out_ << "\n";
    }
};

}  // namespace catl::xdata