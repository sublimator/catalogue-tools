#pragma once
#include "catl/shamap/shamap-utils.h"
#include "catl/v2/catl-v2-reader.h"
#include "catl/v2/catl-v2-structs.h"
#include <array>
#include <atomic>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

/*

The SHAMap should use tagged pointers for the child nodes ...
To start with we'll just allocate an array in the struct

 */

namespace catl::hybrid_shamap {
class HybridReader;

// Forward declarations
class HMapNode;
class HmapInnerNode;
class HmapLeafNode;
class HmapPlaceholder;

/**
 * Tag for pointer types - WHERE does this node live?
 */
enum class PtrTag : uint8_t {
    RAW_MEMORY = 0,   // Points into mmap (could be any mmap file)
    MATERIALIZED = 1  // Points to heap-allocated node
};

/**
 * Tagged pointer for hybrid SHAMap nodes
 *
 * Uses the lower 2 bits for tagging since pointers are 8-byte aligned.
 * This tells us WHERE the node lives (mmap vs heap), while the node's
 * class type tells us WHAT it is (Inner/Leaf/Placeholder).
 *
 * For MATERIALIZED nodes, this class manages reference counting via
 * intrusive_ptr semantics. For RAW_MEMORY nodes, no ref counting occurs.
 */
class TaggedPtr
{
private:
    uintptr_t ptr_;  // Lower 2 bits used for tag

    static constexpr uintptr_t TAG_MASK = 0x3;
    static constexpr uintptr_t PTR_MASK = ~TAG_MASK;

    // Helper to increment ref count if materialized
    void
    add_ref() const;

    // Helper to decrement ref count if materialized
    void
    release() const;

public:
    // Default constructor - creates empty/null pointer
    TaggedPtr() : ptr_(0)
    {
    }

    // Copy constructor - increments ref count if materialized
    TaggedPtr(const TaggedPtr& other) : ptr_(other.ptr_)
    {
        add_ref();
    }

    // Move constructor - takes ownership, no ref count change
    TaggedPtr(TaggedPtr&& other) noexcept : ptr_(other.ptr_)
    {
        other.ptr_ = 0;  // Clear source
    }

    // Destructor - decrements ref count if materialized
    ~TaggedPtr()
    {
        release();
    }

    // Copy assignment
    TaggedPtr&
    operator=(const TaggedPtr& other)
    {
        if (this != &other)
        {
            release();  // Release old
            ptr_ = other.ptr_;
            add_ref();  // Add ref to new
        }
        return *this;
    }

    // Move assignment
    TaggedPtr&
    operator=(TaggedPtr&& other) noexcept
    {
        if (this != &other)
        {
            release();  // Release old
            ptr_ = other.ptr_;
            other.ptr_ = 0;  // Clear source
        }
        return *this;
    }

    // Factory methods
    static TaggedPtr
    make_raw_memory(const void* p)
    {
        TaggedPtr tp;
        tp.ptr_ = reinterpret_cast<uintptr_t>(p) |
            static_cast<uintptr_t>(PtrTag::RAW_MEMORY);
        return tp;
    }

    static TaggedPtr
    make_materialized(HMapNode* p)
    {
        TaggedPtr tp;
        tp.ptr_ = reinterpret_cast<uintptr_t>(p) |
            static_cast<uintptr_t>(PtrTag::MATERIALIZED);
        // Note: Caller is responsible for ensuring proper ref count
        // This is a raw factory - use from_intrusive() for managed pointers
        return tp;
    }

    // Factory from intrusive_ptr - takes a ref-counted pointer
    static TaggedPtr
    from_intrusive(const boost::intrusive_ptr<HMapNode>& p);

    // Convert to intrusive_ptr (only valid for MATERIALIZED)
    [[nodiscard]] boost::intrusive_ptr<HMapNode>
    to_intrusive() const;

    static TaggedPtr
    make_empty()
    {
        return TaggedPtr();  // null
    }

    // Accessors
    [[nodiscard]] PtrTag
    get_tag() const
    {
        return static_cast<PtrTag>(ptr_ & TAG_MASK);
    }

    [[nodiscard]] void*
    get_raw_ptr() const
    {
        return reinterpret_cast<void*>(ptr_ & PTR_MASK);
    }

    [[nodiscard]] const uint8_t*
    get_raw_memory() const
    {
        assert(is_raw_memory());
        return reinterpret_cast<const uint8_t*>(ptr_ & PTR_MASK);
    }

    [[nodiscard]] HMapNode*
    get_materialized() const
    {
        assert(is_materialized());
        return reinterpret_cast<HMapNode*>(ptr_ & PTR_MASK);
    }

    // Type checks
    [[nodiscard]] bool
    is_raw_memory() const
    {
        return get_tag() == PtrTag::RAW_MEMORY;
    }

    [[nodiscard]] bool
    is_materialized() const
    {
        return get_tag() == PtrTag::MATERIALIZED;
    }

    [[nodiscard]] bool
    is_empty() const
    {
        return (ptr_ & PTR_MASK) == 0;
    }

    // Equality
    bool
    operator==(const TaggedPtr& other) const
    {
        return ptr_ == other.ptr_;
    }

    bool
    operator!=(const TaggedPtr& other) const
    {
        return ptr_ != other.ptr_;
    }

    // Bool conversion
    explicit operator bool() const
    {
        return !is_empty();
    }
};

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
            header.offset(sizeof(catl::v2::InnerNodeHeader)).raw();
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

    // Get pointer to child at branch i using SparseChildOffsets
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

        // Create SparseChildOffsets accessor
        const uint8_t* offsets_base =
            header.offset(sizeof(catl::v2::InnerNodeHeader)).raw();
        catl::v2::SparseChildOffsets offsets(
            offsets_base, header_val.child_types);

        // Get the child pointer (will return nullptr if empty)
        const uint8_t* child_ptr = offsets.get_child_ptr(branch);
        if (!child_ptr)
        {
            throw std::runtime_error(
                "No child at branch " + std::to_string(branch));
        }

        return child_ptr;
    }

    // Get a SparseChildOffsets accessor for this node
    [[nodiscard]] catl::v2::SparseChildOffsets
    get_sparse_offsets() const
    {
        auto header_val = header.get();
        const uint8_t* offsets_base =
            header.offset(sizeof(catl::v2::InnerNodeHeader)).raw();
        return catl::v2::SparseChildOffsets(
            offsets_base, header_val.child_types);
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

        // Load leaf header using MemPtr
        catl::v2::MemPtr<catl::v2::LeafHeader> leaf_header_ptr(leaf_ptr);
        const auto& leaf_header = leaf_header_ptr.get();  // Force ref binding

        return LeafView{
            Key(leaf_header.key.data()),  // Now safe - ref in UNSAFE mode!
            Slice(
                leaf_header_ptr.offset(sizeof(catl::v2::LeafHeader)).raw(),
                leaf_header.data_size())};
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

/**
 * Base class for all hybrid map nodes
 * The node type tells us WHAT the node is (Inner/Leaf/Placeholder)
 * while the TaggedPtr tells us WHERE it lives (mmap vs heap)
 *
 * Supports boost::intrusive_ptr for reference counting of heap nodes.
 */
class HMapNode
{
private:
    mutable std::atomic<int> ref_count_{0};

public:
    enum class Type : uint8_t { INNER, LEAF, PLACEHOLDER };

    virtual ~HMapNode() = default;
    virtual Type
    get_type() const = 0;

    // For debugging
    virtual std::string
    describe() const = 0;

    // Friend functions for intrusive_ptr support
    friend void
    intrusive_ptr_add_ref(const HMapNode* p)
    {
        p->ref_count_.fetch_add(1, std::memory_order_relaxed);
    }

    friend void
    intrusive_ptr_release(const HMapNode* p)
    {
        if (p->ref_count_.fetch_sub(1, std::memory_order_release) == 1)
        {
            std::atomic_thread_fence(std::memory_order_acquire);
            delete p;
        }
    }
};

/**
 * Inner node - has up to 16 children
 */
class HmapInnerNode : public HMapNode
{
private:
    std::array<TaggedPtr, 16> children_{};
    uint32_t child_types_ = 0;  // 2 bits × 16 children = 32 bits
    uint8_t depth_ = 0;
    Hash256 hash_;  // Cached hash (invalidated on modification)
    bool hash_valid_ = false;

public:
    HmapInnerNode() = default;
    explicit HmapInnerNode(uint8_t depth) : depth_(depth)
    {
    }

    Type
    get_type() const override
    {
        return Type::INNER;
    }

    // Child access
    [[nodiscard]] TaggedPtr
    get_child(int branch) const
    {
        assert(branch >= 0 && branch < 16);
        return children_[branch];
    }

    void
    set_child(int branch, TaggedPtr ptr)
    {
        assert(branch >= 0 && branch < 16);
        children_[branch] = ptr;
        hash_valid_ = false;  // Invalidate cached hash
    }

    // Child type management (EMPTY/INNER/LEAF/PLACEHOLDER)
    [[nodiscard]] catl::v2::ChildType
    get_child_type(int branch) const
    {
        assert(branch >= 0 && branch < 16);
        return static_cast<catl::v2::ChildType>(
            (child_types_ >> (branch * 2)) & 0x3);
    }

    void
    set_child_type(int branch, catl::v2::ChildType type)
    {
        assert(branch >= 0 && branch < 16);
        uint32_t mask = ~(0x3u << (branch * 2));
        child_types_ = (child_types_ & mask) |
            (static_cast<uint32_t>(type) << (branch * 2));
    }

    [[nodiscard]] uint8_t
    get_depth() const
    {
        return depth_;
    }
    void
    set_depth(uint8_t d)
    {
        depth_ = d;
    }

    // Count non-empty children
    [[nodiscard]] int
    count_children() const
    {
        int count = 0;
        for (const auto& child : children_)
        {
            if (child)
                count++;
        }
        return count;
    }

    std::string
    describe() const override
    {
        return "InnerNode(depth=" + std::to_string(depth_) +
            ", children=" + std::to_string(count_children()) + ")";
    }
};

/**
 * Leaf node - contains actual data
 */
class HmapLeafNode : public HMapNode
{
private:
    Key key_;
    std::vector<uint8_t> data_;  // Owned copy of data
    Hash256 hash_;               // Cached hash
    bool hash_valid_ = false;

public:
    HmapLeafNode(const Key& key, const Slice& data)
        : key_(key), data_(data.data(), data.data() + data.size())
    {
    }

    Type
    get_type() const override
    {
        return Type::LEAF;
    }

    [[nodiscard]] const Key&
    get_key() const
    {
        return key_;
    }
    [[nodiscard]] Slice
    get_data() const
    {
        return Slice(data_.data(), data_.size());
    }

    void
    set_data(const Slice& data)
    {
        data_.assign(data.data(), data.data() + data.size());
        hash_valid_ = false;
    }

    std::string
    describe() const override
    {
        return "LeafNode(key=" + key_.hex().substr(0, 8) +
            "..., size=" + std::to_string(data_.size()) + ")";
    }
};

/**
 * Placeholder node - just knows the hash, content not loaded yet
 */
class HmapPlaceholder : public HMapNode
{
private:
    Hash256 hash_;
    uint8_t depth_ = 0;  // Might need to know if it's inner or leaf

public:
    HmapPlaceholder() = default;
    explicit HmapPlaceholder(const Hash256& hash, uint8_t depth = 0)
        : hash_(hash), depth_(depth)
    {
    }

    Type
    get_type() const override
    {
        return Type::PLACEHOLDER;
    }

    [[nodiscard]] const Hash256&
    get_hash() const
    {
        return hash_;
    }
    [[nodiscard]] uint8_t
    get_depth() const
    {
        return depth_;
    }

    std::string
    describe() const override
    {
        return "Placeholder(hash=" + hash_.hex().substr(0, 8) + "...)";
    }
};

/**
 * PathFinder for navigating hybrid SHAMap trees
 * Can traverse through both RAW_MEMORY (mmap) and MATERIALIZED (heap) nodes
 */
class HmapPathFinder
{
private:
    HybridReader* reader_;  // For accessing mmap nodes
    Key target_key_;

    // Path we've traversed: pair of (node_ptr, branch_taken_to_get_here)
    // First element has branch=-1 (root)
    std::vector<std::pair<TaggedPtr, int>> path_;

    // Terminal node we found (if any)
    TaggedPtr found_leaf_;
    bool key_matches_ = false;

public:
    HmapPathFinder(HybridReader* reader, const Key& key)
        : reader_(reader), target_key_(key)
    {
    }

    /**
     * Find path to target key starting from root
     * Root can be either RAW_MEMORY or MATERIALIZED
     */
    void
    find_path(TaggedPtr root)
    {
        path_.clear();
        found_leaf_ = TaggedPtr::make_empty();
        key_matches_ = false;

        // Start at root
        path_.push_back({root, -1});

        TaggedPtr current = root;
        int depth = 0;

        while (current)
        {
            if (current.is_raw_memory())
            {
                // Navigate through raw memory node
                if (!navigate_raw_inner(current, depth))
                {
                    break;  // Hit leaf or empty
                }
            }
            else
            {
                // Navigate through materialized node
                assert(current.is_materialized());
                HMapNode* node = current.get_materialized();

                if (node->get_type() == HMapNode::Type::LEAF)
                {
                    // Found a leaf
                    auto* leaf = static_cast<HmapLeafNode*>(node);
                    found_leaf_ = current;
                    key_matches_ = (leaf->get_key() == target_key_);
                    break;
                }
                else if (node->get_type() == HMapNode::Type::PLACEHOLDER)
                {
                    // Can't navigate through placeholder yet
                    // Would need to fetch the actual node
                    throw std::runtime_error(
                        "Cannot navigate through placeholder nodes yet");
                }
                else
                {
                    // It's an inner node
                    auto* inner = static_cast<HmapInnerNode*>(node);
                    depth = inner->get_depth();

                    int branch =
                        catl::shamap::select_branch(target_key_, depth);
                    TaggedPtr child = inner->get_child(branch);

                    if (!child)
                    {
                        // Empty branch
                        break;
                    }

                    path_.push_back({child, branch});
                    current = child;
                    depth++;  // Move to next level
                }
            }
        }
    }

    /**
     * Materialize the path for modification
     * Converts RAW_MEMORY nodes to MATERIALIZED along the path
     */
    void
    materialize_path()
    {
        for (size_t i = 0; i < path_.size(); ++i)
        {
            auto& [node_ptr, branch_taken] = path_[i];

            if (node_ptr.is_raw_memory())
            {
                // Need to materialize this node
                const uint8_t* raw = node_ptr.get_raw_memory();

                // Determine node type from parent's child_types or context
                bool is_leaf = false;
                if (i > 0)
                {
                    // Get type from parent's child_types
                    auto& [parent_ptr, _] = path_[i - 1];
                    if (parent_ptr.is_materialized())
                    {
                        auto* parent_inner = static_cast<HmapInnerNode*>(
                            parent_ptr.get_materialized());
                        is_leaf =
                            (parent_inner->get_child_type(branch_taken) ==
                             catl::v2::ChildType::LEAF);
                    }
                    else
                    {
                        // Parent is still raw, check its header
                        const uint8_t* parent_raw = parent_ptr.get_raw_memory();
                        catl::v2::MemPtr<catl::v2::InnerNodeHeader> header(
                            parent_raw);
                        auto header_val = header.get();
                        is_leaf =
                            (header_val.get_child_type(branch_taken) ==
                             catl::v2::ChildType::LEAF);
                    }
                }
                else if (i == path_.size() - 1 && found_leaf_)
                {
                    // Last node in path and we found a leaf
                    is_leaf = true;
                }

                auto materialized = materialize_raw_node(raw, is_leaf);

                // Update this entry in the path using proper ref counting
                node_ptr = TaggedPtr::from_intrusive(materialized);

                // Update parent's child pointer if not root
                if (i > 0)
                {
                    auto& [parent_ptr, _] = path_[i - 1];
                    assert(parent_ptr.is_materialized());
                    auto* parent_inner = static_cast<HmapInnerNode*>(
                        parent_ptr.get_materialized());
                    parent_inner->set_child(branch_taken, node_ptr);
                }
            }
        }
    }

    // Getters
    [[nodiscard]] bool
    found_leaf() const
    {
        return static_cast<bool>(found_leaf_);
    }
    [[nodiscard]] bool
    key_matches() const
    {
        return key_matches_;
    }
    [[nodiscard]] TaggedPtr
    get_found_leaf() const
    {
        return found_leaf_;
    }
    [[nodiscard]] const std::vector<std::pair<TaggedPtr, int>>&
    get_path() const
    {
        return path_;
    }

    // Debug helper
    void
    print_path() const
    {
        std::cout << "Path to key " << target_key_.hex() << ":\n";
        for (size_t i = 0; i < path_.size(); ++i)
        {
            const auto& [node_ptr, branch] = path_[i];
            std::cout << "  [" << i << "] ";
            if (branch >= 0)
            {
                std::cout << "branch " << branch << " -> ";
            }
            if (node_ptr.is_raw_memory())
            {
                std::cout << "RAW_MEMORY @ " << node_ptr.get_raw_ptr();
            }
            else
            {
                auto* node = node_ptr.get_materialized();
                std::cout << "MATERIALIZED " << node->describe();
            }
            std::cout << "\n";
        }
        if (found_leaf_)
        {
            std::cout << "  Found leaf, key "
                      << (key_matches_ ? "MATCHES" : "does NOT match") << "\n";
        }
        else
        {
            std::cout << "  No leaf found\n";
        }
    }

private:
    /**
     * Navigate through a raw memory inner node
     * Returns true if we should continue, false if we hit a leaf/empty
     */
    bool
    navigate_raw_inner(TaggedPtr& current, int& depth)
    {
        const uint8_t* raw = current.get_raw_memory();
        InnerNodeView view{catl::v2::MemPtr<catl::v2::InnerNodeHeader>(raw)};

        auto header = view.header.get();
        depth = header.get_depth();

        int branch = catl::shamap::select_branch(target_key_, depth);
        auto child_type = header.get_child_type(branch);

        if (child_type == catl::v2::ChildType::EMPTY)
        {
            // Empty branch
            return false;
        }

        const uint8_t* child_ptr = view.get_child_ptr(branch);
        TaggedPtr child = TaggedPtr::make_raw_memory(child_ptr);

        if (child_type == catl::v2::ChildType::LEAF)
        {
            // It's a leaf - check the key
            catl::v2::MemPtr<catl::v2::LeafHeader> leaf_header_ptr(child_ptr);
            const auto& leaf_header = leaf_header_ptr.get();

            found_leaf_ = child;
            key_matches_ =
                (std::memcmp(leaf_header.key.data(), target_key_.data(), 32) ==
                 0);

            path_.emplace_back(child, branch);
            return false;  // Stop here
        }
        else
        {
            // It's another inner node
            path_.emplace_back(child, branch);
            current = child;
            depth++;
            return true;  // Continue
        }
    }

    /**
     * Materialize a raw node (convert from mmap to heap)
     * Returns intrusive_ptr for proper memory management
     */
    boost::intrusive_ptr<HMapNode>
    materialize_raw_node(const uint8_t* raw, bool is_leaf)
    {
        if (is_leaf)
        {
            // Materialize as leaf
            catl::v2::MemPtr<catl::v2::LeafHeader> leaf_header_ptr(raw);
            const auto& header = leaf_header_ptr.get();

            Key key(header.key.data());
            Slice data(raw + sizeof(catl::v2::LeafHeader), header.data_size());

            return boost::intrusive_ptr<HMapNode>(new HmapLeafNode(key, data));
        }
        else
        {
            // Materialize as inner
            catl::v2::MemPtr<catl::v2::InnerNodeHeader> inner_ptr(raw);
            const auto& header = inner_ptr.get();

            auto* inner = new HmapInnerNode(header.get_depth());
            boost::intrusive_ptr<HMapNode> inner_ptr_managed(inner);

            // Copy child_types from mmap header
            for (int i = 0; i < 16; ++i)
            {
                inner->set_child_type(i, header.get_child_type(i));
            }

            // Copy children pointers as RAW_MEMORY tagged pointers
            InnerNodeView view{inner_ptr};
            auto offsets = view.get_sparse_offsets();

            for (int i = 0; i < 16; ++i)
            {
                if (offsets.has_child(i))
                {
                    const uint8_t* child_raw = offsets.get_child_ptr(i);
                    inner->set_child(i, TaggedPtr::make_raw_memory(child_raw));
                }
            }

            return inner_ptr_managed;
        }
    }
};

class Hmap
{
private:
    TaggedPtr root_;  // Root can be any type of pointer
    std::shared_ptr<catl::v2::CatlV2Reader> reader_;  // Keeps mmap alive

public:
    Hmap() = default;

    // Constructor with reader for mmap lifetime management
    explicit Hmap(std::shared_ptr<catl::v2::CatlV2Reader> reader)
        : reader_(std::move(reader))
    {
    }

    // Initialize with a raw memory root (from mmap)
    void
    set_root_raw(const uint8_t* raw_root)
    {
        root_ = TaggedPtr::make_raw_memory(raw_root);
    }

    // Initialize with a materialized root
    void
    set_root_materialized(HmapInnerNode* node)
    {
        root_ = TaggedPtr::make_materialized(node);
    }

    [[nodiscard]] TaggedPtr
    get_root() const
    {
        return root_;
    }

    [[nodiscard]] std::shared_ptr<catl::v2::CatlV2Reader>
    get_reader() const
    {
        return reader_;
    }
};

// Implementation of TaggedPtr methods that depend on HMapNode

inline void
TaggedPtr::add_ref() const
{
    if (is_materialized() && !is_empty())
    {
        intrusive_ptr_add_ref(get_materialized());
    }
}

inline void
TaggedPtr::release() const
{
    if (is_materialized() && !is_empty())
    {
        intrusive_ptr_release(get_materialized());
    }
}

inline TaggedPtr
TaggedPtr::from_intrusive(const boost::intrusive_ptr<HMapNode>& p)
{
    if (!p)
    {
        return TaggedPtr::make_empty();
    }

    // Create tagged pointer and manually increment ref count
    TaggedPtr tp = TaggedPtr::make_materialized(p.get());
    intrusive_ptr_add_ref(p.get());
    return tp;
}

inline boost::intrusive_ptr<HMapNode>
TaggedPtr::to_intrusive() const
{
    if (!is_materialized() || is_empty())
    {
        return nullptr;
    }

    // Create intrusive_ptr which will increment ref count
    return boost::intrusive_ptr<HMapNode>(get_materialized());
}

}  // namespace catl::hybrid_shamap
