#pragma once

#include <concepts>
#include <cstdint>
#include <type_traits>

namespace catl::shamap::concepts {

// ============================================================================
// Core Hook Detection Concepts
// ============================================================================

/**
 * Concept for detecting if a trait has the on_copy_created hook.
 * This hook is called when a node is created via CoW copy operation.
 *
 * Note: Since traits are shared by both inner and leaf nodes, this hook
 * receives the original traits object, not the full node.
 */
template <typename T>
concept HasCopyCreatedHook = requires(T& t, const T& original, int version)
{
    {
        t.on_copy_created(original, version)
    }
    ->std::same_as<void>;
};

/**
 * Concept for detecting if a trait has the on_copy_from hook.
 * This hook establishes parent-child relationships for lineage tracking.
 */
template <typename T>
concept HasCopyFromHook = requires(T& t, const T& parent)
{
    {
        t.on_copy_from(parent)
    }
    ->std::same_as<void>;
};

/**
 * Concept for detecting pre-serialization hook.
 * Called before a node is written to disk.
 */
template <typename T>
concept HasPreSerializeHook = requires(T& t)
{
    {
        t.on_pre_serialize()
    }
    ->std::same_as<void>;
};

/**
 * Concept for detecting post-serialization hook.
 * Called after a node has been written to disk with its file offset.
 */
template <typename T>
concept HasPostSerializeHook = requires(T& t, std::uint64_t offset)
{
    {
        t.on_post_serialize(offset)
    }
    ->std::same_as<void>;
};

/**
 * Concept for validation hook.
 * Allows traits to validate copy operations.
 */
template <typename T>
concept HasValidationHook = requires(T& t)
{
    {
        t.on_validate_copy()
    }
    ->std::same_as<bool>;
};

// ============================================================================
// Node Type Awareness Concepts (Optional)
// ============================================================================

/**
 * Concept for traits that need to know what type of node they're in.
 * This allows hooks to behave differently for inner vs leaf nodes.
 */
template <typename T>
concept HasNodeTypeAwareness = requires(T& t, bool is_inner)
{
    {
        t.on_copy_created_with_type(t, 0, is_inner)
    }
    ->std::same_as<void>;
};

// ============================================================================
// Composite Concepts for Feature Detection
// ============================================================================

/**
 * Traits that have any copy-related hooks
 */
template <typename T>
concept HasCopyHooks = HasCopyCreatedHook<T> || HasCopyFromHook<T>;

/**
 * Traits that have any serialization-related hooks
 */
template <typename T>
concept HasSerializationHooks =
    HasPreSerializeHook<T> || HasPostSerializeHook<T>;

/**
 * Traits that have all copy hooks (full tracking)
 */
template <typename T>
concept HasAllCopyHooks = HasCopyCreatedHook<T>&& HasCopyFromHook<T>;

/**
 * Traits that have any hooks at all
 */
template <typename T>
concept HasAnyHooks =
    HasCopyHooks<T> || HasSerializationHooks<T> || HasValidationHook<T>;

/**
 * Lightweight tracking - small traits with minimal overhead
 */
template <typename T>
concept IsLightweightTracking = HasCopyCreatedHook<T> &&
    sizeof(T) <= 64 && std::is_trivially_copyable_v<T>;

/**
 * Full tracking capabilities
 */
template <typename T>
concept IsFullTracking = HasAllCopyHooks<T>&& HasSerializationHooks<T>;

}  // namespace catl::shamap::concepts

namespace catl::shamap::hooks {

// ============================================================================
// Hook Invocation Functions
// ============================================================================

/**
 * Invoke on_copy_created hook if present.
 * Called when a node is copied during CoW operations.
 *
 * @param new_copy The newly created copy (traits portion)
 * @param original The original node's traits
 * @param new_version The version number for the new copy
 */
template <typename Traits>
inline void
invoke_on_copy_created(
    Traits& new_copy,
    const Traits& original,
    int new_version)
{
    if constexpr (concepts::HasCopyCreatedHook<Traits>)
    {
        new_copy.on_copy_created(original, new_version);
    }
}

/**
 * Invoke on_copy_from hook if present.
 * Establishes lineage relationships between nodes.
 *
 * @param child The child node's traits
 * @param parent The parent node's traits
 */
template <typename Traits>
inline void
invoke_on_copy_from(Traits& child, const Traits& parent)
{
    if constexpr (concepts::HasCopyFromHook<Traits>)
    {
        child.on_copy_from(parent);
    }
}

/**
 * Invoke pre-serialization hook if present.
 * Called before writing a node to disk.
 */
template <typename Traits>
inline void
invoke_on_pre_serialize(Traits& traits)
{
    if constexpr (concepts::HasPreSerializeHook<Traits>)
    {
        traits.on_pre_serialize();
    }
}

/**
 * Invoke post-serialization hook if present.
 * Called after writing a node to disk with its offset.
 */
template <typename Traits>
inline void
invoke_on_post_serialize(Traits& traits, std::uint64_t file_offset)
{
    if constexpr (concepts::HasPostSerializeHook<Traits>)
    {
        traits.on_post_serialize(file_offset);
    }
}

/**
 * Invoke validation hook if present.
 * Returns true if validation passes (or if no validation hook exists).
 */
template <typename Traits>
inline bool
invoke_validation(Traits& traits)
{
    if constexpr (concepts::HasValidationHook<Traits>)
    {
        return traits.on_validate_copy();
    }
    return true;  // Default: validation passes
}

/**
 * Special invocation for node-type-aware hooks.
 * Allows traits to know if they're in an inner or leaf node.
 */
template <typename Traits>
inline void
invoke_on_copy_created_with_type(
    Traits& new_copy,
    const Traits& original,
    int new_version,
    bool is_inner_node)
{
    if constexpr (concepts::HasNodeTypeAwareness<Traits>)
    {
        new_copy.on_copy_created_with_type(
            original, new_version, is_inner_node);
    }
    else if constexpr (concepts::HasCopyCreatedHook<Traits>)
    {
        // Fall back to regular hook if type-aware version not available
        new_copy.on_copy_created(original, new_version);
    }
}

}  // namespace catl::shamap::hooks

namespace catl::shamap::algorithms {

// ============================================================================
// Algorithm Selection Based on Trait Capabilities
// ============================================================================

/**
 * Perform copy operation with appropriate hook invocations.
 * Selects the optimal strategy based on trait capabilities.
 *
 * Since traits are shared between inner and leaf nodes, this function
 * works with the traits portion only, not the full node.
 */
template <typename Traits>
inline void
perform_copy_operation(
    Traits& new_copy,
    const Traits& original,
    int new_version,
    bool is_inner_node = false)
{
    // Select algorithm based on trait capabilities
    if constexpr (concepts::IsLightweightTracking<Traits>)
    {
        // Fast path for simple tracking
        // Can use memcpy for trivially copyable types
        new_copy.on_copy_created(original, new_version);
    }
    else if constexpr (concepts::IsFullTracking<Traits>)
    {
        // Full tracking with all hooks
        new_copy.on_copy_created(original, new_version);
        new_copy.on_copy_from(original);

        // Validate if supported
        if constexpr (concepts::HasValidationHook<Traits>)
        {
            [[maybe_unused]] bool valid = new_copy.on_validate_copy();
            assert(valid && "Copy validation failed");
        }
    }
    else if constexpr (concepts::HasNodeTypeAwareness<Traits>)
    {
        // Node-type-aware tracking
        invoke_on_copy_created_with_type(
            new_copy, original, new_version, is_inner_node);
    }
    else if constexpr (concepts::HasCopyHooks<Traits>)
    {
        // Standard copy with basic hooks
        hooks::invoke_on_copy_created(new_copy, original, new_version);
        hooks::invoke_on_copy_from(new_copy, original);
    }
    // else: No hooks - no additional work needed
}

/**
 * Specialized copy operation for inner nodes.
 * Can include inner-node-specific logic if needed.
 */
template <typename Traits>
inline void
perform_inner_node_copy(
    Traits& new_copy,
    const Traits& original,
    int new_version)
{
    perform_copy_operation(new_copy, original, new_version, true);
}

/**
 * Specialized copy operation for leaf nodes.
 * Can include leaf-node-specific logic if needed.
 */
template <typename Traits>
inline void
perform_leaf_node_copy(
    Traits& new_copy,
    const Traits& original,
    int new_version)
{
    perform_copy_operation(new_copy, original, new_version, false);
}

}  // namespace catl::shamap::algorithms

// ============================================================================
// Diagnostic Utilities (for debugging and testing)
// ============================================================================

namespace catl::shamap::diagnostics {

/**
 * Check what capabilities a trait type has.
 * Useful for debugging and understanding trait capabilities.
 */
template <typename T>
struct TraitCapabilities
{
    static constexpr bool has_copy_created = concepts::HasCopyCreatedHook<T>;
    static constexpr bool has_copy_from = concepts::HasCopyFromHook<T>;
    static constexpr bool has_pre_serialize = concepts::HasPreSerializeHook<T>;
    static constexpr bool has_post_serialize =
        concepts::HasPostSerializeHook<T>;
    static constexpr bool has_validation = concepts::HasValidationHook<T>;
    static constexpr bool has_node_type_awareness =
        concepts::HasNodeTypeAwareness<T>;
    static constexpr bool is_lightweight = concepts::IsLightweightTracking<T>;
    static constexpr bool is_full_tracking = concepts::IsFullTracking<T>;

    static void
    print_capabilities(const char* trait_name = "Trait")
    {
        std::cout << "Capabilities for " << trait_name << ":\n";
        std::cout << "  Copy created hook: " << (has_copy_created ? "✓" : "✗")
                  << "\n";
        std::cout << "  Copy from hook: " << (has_copy_from ? "✓" : "✗")
                  << "\n";
        std::cout << "  Pre-serialize hook: " << (has_pre_serialize ? "✓" : "✗")
                  << "\n";
        std::cout << "  Post-serialize hook: "
                  << (has_post_serialize ? "✓" : "✗") << "\n";
        std::cout << "  Validation hook: " << (has_validation ? "✓" : "✗")
                  << "\n";
        std::cout << "  Node type awareness: "
                  << (has_node_type_awareness ? "✓" : "✗") << "\n";
        std::cout << "  Lightweight tracking: " << (is_lightweight ? "✓" : "✗")
                  << "\n";
        std::cout << "  Full tracking: " << (is_full_tracking ? "✓" : "✗")
                  << "\n";
    }
};

}  // namespace catl::shamap::diagnostics