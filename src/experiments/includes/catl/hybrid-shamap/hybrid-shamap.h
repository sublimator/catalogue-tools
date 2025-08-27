#pragma once
#include "catl/shamap/shamap-utils.h"
#include "catl/v2/catl-v2-reader.h"
#include "catl/v2/catl-v2-structs.h"
#include <array>
#include <memory>
#include <stdexcept>
#include <utility>

/*

The SHAMap should use tagged pointers for the child nodes ...
To start with we'll just allocate an array in the struct

 */

namespace catl::hybrid_shamap {
class HybridReader;

/**
 * Lightweight view for an inner node - just holds pointer to mmap data
 */
struct InnerNodeView
{
    catl::v2::MemPtr<catl::v2::InnerNodeHeader>
        header;  // Points directly into mmap

    // Get a child iterator on demand
    [[nodiscard]] catl::v2::ChildIterator
    get_child_iter() const
    {
        const auto* offsets_data =
            header.raw() + sizeof(catl::v2::InnerNodeHeader);
        // For the iterator, we need file offset - calculate from pointer
        // This is a bit hacky but avoids storing file_offset
        size_t offsets_file_base = reinterpret_cast<uintptr_t>(offsets_data);
        // Pass MemPtr to iterator
        return {header, offsets_data, offsets_file_base};
    }

    // Get child type for branch i (EMPTY, INNER, or LEAF)
    [[nodiscard]] catl::v2::ChildType
    get_child_type(int branch) const
    {
        if (branch < 0 || branch >= 16)
        {
            throw std::out_of_range(
                "Branch index " + std::to_string(branch) +
                " out of range [0,16)");
        }
        auto header_val = header.get();
        return header_val.get_child_type(branch);
    }

    // Get pointer to child at branch i
    [[nodiscard]] const uint8_t*
    get_child_ptr(int branch) const
    {
        if (branch < 0 || branch >= 16)
        {
            throw std::out_of_range(
                "Branch index " + std::to_string(branch) +
                " out of range [0,16)");
        }

        auto header_val = header.get();
        auto child_type = header_val.get_child_type(branch);
        if (child_type == catl::v2::ChildType::EMPTY)
        {
            throw std::runtime_error(
                "No child at branch " + std::to_string(branch));
        }

        // Count how many non-empty children come before this branch
        int offset_index = 0;
        for (int i = 0; i < branch; ++i)
        {
            if (header_val.get_child_type(i) != catl::v2::ChildType::EMPTY)
            {
                offset_index++;
            }
        }

        // Get pointer to the slot where this offset is stored
        const uint8_t* slot_ptr = header.raw() +
            sizeof(catl::v2::InnerNodeHeader) +
            offset_index * sizeof(catl::v2::rel_off_t);

        // Read the relative offset (safely)
        catl::v2::rel_off_t rel;
        std::memcpy(&rel, slot_ptr, sizeof(rel));

        // The absolute pointer is just slot + relative!
        return slot_ptr + rel;
    }

    // Find first leaf using depth-first traversal
    // This needs HybridReader to recurse, so it's better placed there
};

// Leaf view structure
struct LeafView
{
    Key key;
    Slice data;
};

/**
 * Wrapper around CatlV2Reader for hybrid shamap operations
 */
class HybridReader
{
private:
    std::shared_ptr<catl::v2::CatlV2Reader> reader_;

public:
    explicit HybridReader(std::shared_ptr<catl::v2::CatlV2Reader> reader)
        : reader_(std::move(reader))
    {
    }

    /**
     * Get an inner node view at the given offset
     * Returns lightweight view with pointer into mmap data
     */
    [[nodiscard]] InnerNodeView
    get_inner_node_at(size_t offset) const
    {
        // Create MemPtr to the header in mmap (no copy)
        catl::v2::MemPtr<catl::v2::InnerNodeHeader> header(
            reader_->data_at(offset));
        return InnerNodeView{header};
    }

    /**
     * Get an inner node view from a pointer
     */
    [[nodiscard]] InnerNodeView
    get_inner_node(const uint8_t* ptr) const
    {
        catl::v2::MemPtr<catl::v2::InnerNodeHeader> header(ptr);
        return InnerNodeView{header};
    }

    /**
     * Get the current state tree root as an inner node
     * (must be called after read_ledger_info)
     */
    [[nodiscard]] InnerNodeView
    get_state_root() const
    {
        return get_inner_node_at(reader_->current_offset());
    }

    /**
     * Get an inner child from a node view
     */
    [[nodiscard]] InnerNodeView
    get_inner_child(const InnerNodeView& parent, int branch) const
    {
        auto child_type = parent.get_child_type(branch);
        if (child_type != catl::v2::ChildType::INNER)
        {
            if (child_type == catl::v2::ChildType::EMPTY)
            {
                throw std::runtime_error(
                    "No child at branch " + std::to_string(branch));
            }
            throw std::runtime_error(
                "Child at branch " + std::to_string(branch) +
                " is a leaf, not an inner node");
        }
        return get_inner_node(parent.get_child_ptr(branch));
    }

    /**
     * Get a leaf child from a node view
     */
    [[nodiscard]] LeafView
    get_leaf_child(const InnerNodeView& parent, int branch) const
    {
        auto child_type = parent.get_child_type(branch);
        if (child_type != catl::v2::ChildType::LEAF)
        {
            if (child_type == catl::v2::ChildType::EMPTY)
            {
                throw std::runtime_error(
                    "No child at branch " + std::to_string(branch));
            }
            throw std::runtime_error(
                "Child at branch " + std::to_string(branch) +
                " is an inner node, not a leaf");
        }

        const uint8_t* leaf_ptr = parent.get_child_ptr(branch);

        // Load leaf header directly from pointer
        const auto* leaf_header =
            reinterpret_cast<const catl::v2::LeafHeader*>(leaf_ptr);

        return LeafView{
            Key(leaf_header->key.data()),
            Slice(
                leaf_ptr + sizeof(catl::v2::LeafHeader),
                leaf_header->data_size())};
    }

    /**
     * Lookup a key in the state tree starting from a given inner node
     * @param root The root node to start from
     * @param key The key to search for
     * @return LeafView if found
     * @throws std::runtime_error if key not found
     */
    [[nodiscard]] LeafView
    lookup_key(const InnerNodeView& root, const Key& key) const
    {
        InnerNodeView current = root;
        auto root_header = root.header.get();
        int depth = root_header.get_depth();

        // Walk down the tree following the key nibbles
        while (true)
        {
            // Use shamap utility to extract nibble at current depth
            int nibble = catl::shamap::select_branch(key, depth);

            // Check child type
            auto child_type = current.get_child_type(nibble);
            if (child_type == catl::v2::ChildType::EMPTY)
            {
                throw std::runtime_error(
                    "Key not found - no child at nibble " +
                    std::to_string(nibble) + " at depth " +
                    std::to_string(depth));
            }

            if (child_type == catl::v2::ChildType::LEAF)
            {
                // Found a leaf, verify it's the right key
                auto leaf = get_leaf_child(current, nibble);
                if (std::memcmp(leaf.key.data(), key.data(), 32) == 0)
                {
                    return leaf;
                }
                throw std::runtime_error("Key mismatch at leaf");
            }

            // It's an inner node, continue traversing
            current = get_inner_child(current, nibble);
            auto current_header = current.header.get();
            depth = current_header.get_depth();
        }
    }

    /**
     * Lookup a key in the current state tree
     * Convenience method that uses the current state root
     */
    [[nodiscard]] LeafView
    lookup_key_in_state(const Key& key) const
    {
        return lookup_key(get_state_root(), key);
    }

    /**
     * Find the first leaf in depth-first order starting from given node
     *
     * Note: Uses recursion which is optimal here because:
     * - Max depth is bounded by key size (64 nibbles = 64 levels max)
     * - Stack usage is tiny (~8KB worst case vs 8MB default stack)
     * - CPU call stack is faster than heap-allocated stack (better cache
     * locality)
     * - Code is cleaner and compiler can optimize
     *
     * @param node The inner node to start from
     * @return LeafView of the first leaf found
     * @throws std::runtime_error if no leaf found (malformed tree)
     */
    [[nodiscard]] LeafView
    first_leaf_depth_first(const InnerNodeView& node) const
    {
        // Check each branch in order
        for (int i = 0; i < 16; ++i)
        {
            auto child_type = node.get_child_type(i);
            if (child_type == catl::v2::ChildType::EMPTY)
            {
                continue;  // Skip empty branches
            }

            if (child_type == catl::v2::ChildType::LEAF)
            {
                // Found a leaf!
                return get_leaf_child(node, i);
            }

            // It's an inner node, recurse
            auto inner_child = get_inner_child(node, i);
            // Recursive call will throw if no leaf found in subtree
            return first_leaf_depth_first(inner_child);
        }

        // No children at all or all empty - malformed tree
        throw std::runtime_error("No leaf found - malformed tree");
    }

    // Forward key methods
    [[nodiscard]] const catl::common::LedgerInfo&
    read_ledger_info()
    {
        return reader_->read_ledger_info();
    }

    [[nodiscard]] size_t
    current_offset() const
    {
        return reader_->current_offset();
    }
};

class HmapPathFinder
{
};

class HMapNode
{
    Hash256 hash;
};

class HmapInnerNode : public HMapNode
{
public:
    // These will be any kind of pointers - for now just storing raw offsets
    std::array<void*, 16> children{};
};

class HmapLeafNode : public HMapNode
{
    Key key;
    std::vector<uint8_t> data;
    Hash256 hash;
};

class Hmap
{
    HmapInnerNode root;

    void
    materialize_root(catl::v2::InnerNodeHeader* header)
    {
    }
};
}  // namespace catl::hybrid_shamap
