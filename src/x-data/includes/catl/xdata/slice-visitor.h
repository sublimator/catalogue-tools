#pragma once

#include "catl/core/types.h"  // For Slice
#include "catl/xdata/fields.h"
#include <concepts>
#include <functional>
#include <type_traits>
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
namespace detail {

// Each callback is OPTIONAL. A visitor implements only the ones it needs; the
// parser supplies a default for any it omits (see the call_* dispatchers).
// A callback that IS present must have the correct signature — these concepts
// check exactly that, so a wrong return type degrades to "absent" (default)
// rather than a hard error, and the call sites stay clean.

template <typename T>
concept HasObjectStart =
    requires(T& v, const FieldPath& p, const FieldSlice& fs) {
        { v.visit_object_start(p, fs) } -> std::convertible_to<bool>;
    };
template <typename T>
concept HasObjectEnd = requires(T& v, const FieldPath& p, const FieldSlice& fs) {
    v.visit_object_end(p, fs);
};
template <typename T>
concept HasArrayStart =
    requires(T& v, const FieldPath& p, const FieldSlice& fs) {
        { v.visit_array_start(p, fs) } -> std::convertible_to<bool>;
    };
template <typename T>
concept HasArrayEnd = requires(T& v, const FieldPath& p, const FieldSlice& fs) {
    v.visit_array_end(p, fs);
};
template <typename T>
concept HasField = requires(T& v, const FieldPath& p, const FieldSlice& fs) {
    v.visit_field(p, fs);
};

// A missing *_start defaults to "descend" (true) — the common case; a visitor
// prunes nested structures by implementing the callback and returning false.
// Missing *_end / field callbacks are no-ops.
template <typename V>
inline bool
call_object_start(V& v, const FieldPath& p, const FieldSlice& fs)
{
    if constexpr (HasObjectStart<V>)
        return v.visit_object_start(p, fs);
    else
        return true;
}
template <typename V>
inline void
call_object_end(V& v, const FieldPath& p, const FieldSlice& fs)
{
    if constexpr (HasObjectEnd<V>)
        v.visit_object_end(p, fs);
}
template <typename V>
inline bool
call_array_start(V& v, const FieldPath& p, const FieldSlice& fs)
{
    if constexpr (HasArrayStart<V>)
        return v.visit_array_start(p, fs);
    else
        return true;
}
template <typename V>
inline void
call_array_end(V& v, const FieldPath& p, const FieldSlice& fs)
{
    if constexpr (HasArrayEnd<V>)
        v.visit_array_end(p, fs);
}
template <typename V>
inline void
call_field(V& v, const FieldPath& p, const FieldSlice& fs)
{
    if constexpr (HasField<V>)
        v.visit_field(p, fs);
}

}  // namespace detail

/**
 * A SliceVisitor may implement ANY SUBSET of the five callbacks:
 *
 *   bool visit_object_start(const FieldPath&, const FieldSlice&)  // descend?
 *   void visit_object_end  (const FieldPath&, const FieldSlice&)
 *   bool visit_array_start (const FieldPath&, const FieldSlice&)  // descend?
 *   void visit_array_end   (const FieldPath&, const FieldSlice&)
 *   void visit_field       (const FieldPath&, const FieldSlice&)
 *
 * Whatever it omits gets a default: the *_start callbacks default to "descend"
 * (true); the rest are no-ops. A visitor that only inspects leaf fields needs
 * just visit_field. At least one recognized callback must be present — a type
 * with none is almost certainly a mistake (e.g. a misspelled method) that
 * would otherwise silently do nothing.
 */
template <typename T>
concept SliceVisitor =
    detail::HasObjectStart<std::remove_cvref_t<T>> ||
    detail::HasObjectEnd<std::remove_cvref_t<T>> ||
    detail::HasArrayStart<std::remove_cvref_t<T>> ||
    detail::HasArrayEnd<std::remove_cvref_t<T>> ||
    detail::HasField<std::remove_cvref_t<T>>;

/**
 * SimpleSliceEmitter - Example visitor that emits all leaf field slices.
 *
 * It emits every leaf field it encounters and relies on the default "descend"
 * behaviour for containers — so it only needs visit_field. This is the minimal
 * shape a visitor can take (contrast TopLevelOnlyVisitor below, which DOES
 * implement the *_start callbacks because it prunes nested structures).
 */
class SimpleSliceEmitter
{
    std::function<void(const FieldSlice&)> emit_;

public:
    explicit SimpleSliceEmitter(std::function<void(const FieldSlice&)> emit)
        : emit_(emit)
    {
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
