#pragma once
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
    const catl::v2::InnerNodeHeader* header;  // Points directly into mmap

    // Get a child iterator on demand
    [[nodiscard]] catl::v2::ChildIterator
    get_child_iter() const
    {
        const auto* offsets_data = reinterpret_cast<const uint8_t*>(header + 1);
        // For the iterator, we need file offset - calculate from pointer
        // This is a bit hacky but avoids storing file_offset
        size_t offsets_file_base = reinterpret_cast<uintptr_t>(offsets_data);
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
        return header->get_child_type(branch);
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

        auto child_type = header->get_child_type(branch);
        if (child_type == catl::v2::ChildType::EMPTY)
        {
            throw std::runtime_error(
                "No child at branch " + std::to_string(branch));
        }

        // Count how many non-empty children come before this branch
        int offset_index = 0;
        for (int i = 0; i < branch; ++i)
        {
            if (header->get_child_type(i) != catl::v2::ChildType::EMPTY)
            {
                offset_index++;
            }
        }

        // Get pointer to the slot where this offset is stored
        const uint8_t* slot_ptr = reinterpret_cast<const uint8_t*>(header + 1) +
            offset_index * sizeof(catl::v2::rel_off_t);

        // Read the relative offset (safely)
        catl::v2::rel_off_t rel;
        std::memcpy(&rel, slot_ptr, sizeof(rel));

        // The absolute pointer is just slot + relative!
        return slot_ptr + rel;
    }
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
        // Get pointer to the header in mmap (no copy)
        const auto* header = reinterpret_cast<const catl::v2::InnerNodeHeader*>(
            reader_->data_at(offset));

        return InnerNodeView{header};
    }

    /**
     * Get an inner node view from a pointer
     */
    [[nodiscard]] InnerNodeView
    get_inner_node(const uint8_t* ptr) const
    {
        const auto* header =
            reinterpret_cast<const catl::v2::InnerNodeHeader*>(ptr);
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

class HMapNode
{
};

class HmapInnerNode : public HMapNode
{
public:
    // These will be any kind of pointers - for now just storing raw offsets
    std::array<void*, 16> children{};
};

class HmapLeafNode : public HMapNode
{
};

class Hmap
{
    HmapInnerNode root;
};
}  // namespace catl::hybrid_shamap
