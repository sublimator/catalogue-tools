#pragma once

#include "catl/core/types.h"  // For Slice
#include "catl/xdata/fields.h"
#include <concepts>
#include <functional>
#include <vector>

namespace catl::xdata {

// Path element for tracking location in parse tree
struct PathElement
{
    const FieldDef* field;
    int array_index = -1;  // -1 means not an array element

    bool
    is_array_element() const
    {
        return array_index >= 0;
    }
};

// Path through the parsed structure
using FieldPath = std::vector<PathElement>;

// A slice of field data with its metadata
struct FieldSlice
{
    const FieldDef* field;
    Slice data;
};

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