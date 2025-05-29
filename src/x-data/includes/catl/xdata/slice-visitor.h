#pragma once

#include "catl/core/types.h"  // For Slice
#include "catl/xdata/fields.h"
#include <concepts>
#include <functional>
#include <vector>

namespace catl::xdata {

/**
 * PathElement represents a single step in the path through a parsed XRPL
 * structure.
 *
 * Each element tracks:
 * - The field definition at this level
 * - If we're inside an array, which element index (-1 if not in an array)
 *
 * Example path for a field inside an array:
 * [
 *   {field: AffectedNodes, array_index: -1},  // The array field itself
 *   {field: AffectedNodes, array_index: 0},   // We're at element 0
 *   {field: CreatedNode, array_index: -1},    // The CreatedNode object
 *   {field: Balance, array_index: -1}          // A leaf field
 * ]
 */
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

/**
 * FieldPath tracks the complete path from the root to the current position
 * in the parse tree. This provides full context for each visited element.
 */
using FieldPath = std::vector<PathElement>;

/**
 * FieldSlice combines field metadata with the actual byte slices from the
 * serialized data.
 *
 * Components:
 * - field: Pointer to the field definition (type, name, etc.)
 * - header: The encoded field header bytes (contains type/field ID)
 * - data: The actual field data bytes
 *
 * For container start callbacks (objects/arrays), data will be empty since
 * we haven't parsed the contents yet. For end callbacks and leaf fields,
 * data contains the complete serialized content.
 */
struct FieldSlice
{
    const FieldDef* field;
    Slice header;  // The field header bytes (type/field encoding)
    Slice data;    // The field data bytes

    const FieldDef&
    get_field() const
    {
        return *field;
    }
};

/**
 * SliceVisitor concept defines the interface for traversing XRPL serialized
 * data.
 *
 * The visitor pattern allows different implementations to process the data in
 * various ways (statistics collection, debugging output, selective extraction,
 * etc.)
 *
 * Key design principles:
 * 1. Consistent interface: All methods use FieldSlice for uniformity
 * 2. Container callbacks: Objects and arrays get start/end notifications
 * 3. Leaf-only fields: visit_field is only called for leaf nodes (not
 * containers)
 * 4. Path context: Every callback includes the full path for context
 * 5. Size information: End callbacks include complete data for size analysis
 *
 * Callback behavior:
 * - Start callbacks: data slice is empty (not yet parsed)
 * - End callbacks: data slice contains the complete serialized content
 * - Field callbacks: data slice contains the field value
 * - Return values: bool indicates whether to descend into containers
 */
template <typename T>
concept SliceVisitor =
    requires(T v, const FieldPath& path, const FieldSlice& fs)
{
    /**
     * Called when entering an STObject field.
     *
     * @param path Current path in the parse tree
     * @param fs Field slice with empty data (content not yet parsed)
     * @return true to parse object contents, false to skip
     *
     * Note: For array elements like CreatedNode, check
     * path.back().is_array_element()
     */
    {
        v.visit_object_start(path, fs)
    }
    ->std::convertible_to<bool>;

    /**
     * Called when exiting an STObject field.
     *
     * @param path Current path in the parse tree
     * @param fs Field slice with complete object data (excluding end marker)
     */
    {
        v.visit_object_end(path, fs)
    }
    ->std::same_as<void>;

    /**
     * Called when entering an STArray field.
     *
     * @param path Current path in the parse tree
     * @param fs Field slice with empty data (content not yet parsed)
     * @return true to parse array contents, false to skip
     */
    {
        v.visit_array_start(path, fs)
    }
    ->std::convertible_to<bool>;

    /**
     * Called when exiting an STArray field.
     *
     * @param path Current path in the parse tree
     * @param fs Field slice with complete array data (excluding end marker)
     */
    {
        v.visit_array_end(path, fs)
    }
    ->std::same_as<void>;

    /**
     * Called for leaf fields only (not objects or arrays).
     *
     * @param path Current path in the parse tree
     * @param fs Field slice with complete field data
     *
     * Examples of leaf fields: UInt32, Hash256, AccountID, Amount, Blob, etc.
     */
    {
        v.visit_field(path, fs)
    }
    ->std::same_as<void>;
};

/**
 * SimpleSliceEmitter - Example visitor that emits all leaf field slices.
 *
 * This visitor always descends into all containers and emits every
 * leaf field it encounters. Useful for extracting all field data
 * from a structure.
 */
class SimpleSliceEmitter
{
    std::function<void(const FieldSlice&)> emit_;

public:
    explicit SimpleSliceEmitter(std::function<void(const FieldSlice&)> emit)
        : emit_(emit)
    {
    }

    bool
    visit_object_start(const FieldPath&, const FieldSlice&)
    {
        return true;  // Always descend
    }

    void
    visit_object_end(const FieldPath&, const FieldSlice&)
    {
        // Could use the complete object data here if needed
    }

    bool
    visit_array_start(const FieldPath&, const FieldSlice&)
    {
        return true;  // Always descend
    }

    void
    visit_array_end(const FieldPath&, const FieldSlice&)
    {
        // Could use the complete array data here if needed
    }

    void
    visit_field(const FieldPath&, const FieldSlice& fs)
    {
        emit_(fs);
    }
};

/**
 * TopLevelOnlyVisitor - Example visitor that only processes root-level fields.
 *
 * This visitor demonstrates selective traversal by only descending into
 * the root object and ignoring all nested structures. Useful for quickly
 * extracting top-level metadata like TransactionType or LedgerSequence.
 */
class TopLevelOnlyVisitor
{
    std::function<void(const FieldSlice&)> process_;

public:
    explicit TopLevelOnlyVisitor(std::function<void(const FieldSlice&)> proc)
        : process_(proc)
    {
    }

    bool
    visit_object_start(const FieldPath& path, const FieldSlice&)
    {
        return path.empty();  // Only descend into root object
    }

    void
    visit_object_end(const FieldPath&, const FieldSlice&)
    {
    }

    bool
    visit_array_start(const FieldPath&, const FieldSlice&)
    {
        return false;  // Never descend into arrays
    }

    void
    visit_array_end(const FieldPath&, const FieldSlice&)
    {
    }

    void
    visit_field(const FieldPath& path, const FieldSlice& fs)
    {
        if (path.size() == 1)
        {  // Top-level field
            process_(fs);
        }
    }
};

}  // namespace catl::xdata
