#pragma once
#include "catl/crypto/sha512-half-hasher.h"
#include "catl/shamap/shamap-hashprefix.h"
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
class MemTreeOps;

// Forward declarations
class HMapNode;
class HmapInnerNode;
class HmapLeafNode;
class HmapPlaceholder;

/**
 * PolyNodePtr - A polymorphic smart pointer to nodes that can be either:
 * - In mmap memory (no ref counting, non-owning)
 * - In heap memory (with ref counting, shared ownership)
 *
 * Acts like a shared_ptr for materialized nodes, and a raw pointer for mmap
 * nodes. It explicitly stores metadata since we can't use bit-tagging with
 * unaligned pointers from #pragma pack(1).
 *
 * Size: 16 bytes (same as std::shared_ptr) - not the original 8-byte TaggedPtr
 * dream, but necessary for explicit metadata since we can't use bit-tagging.
 *
 * Usage: PolyNodePtr instances typically have short lifetimes - created
 * temporarily during traversal/manipulation then destroyed. The main exceptions
 * are class members like Hmap::root_ which hold long-term references to the
 * tree structure.
 *
 * Note: HmapInnerNode stores raw pointers directly and creates PolyNodePtr
 * lazily via get_child(). This means HmapInnerNode must manually manage
 * reference counts in set_child() and its destructor to maintain ownership
 * correctly. The set_child() properly releases old children before setting
 * new ones, preventing leaks when overwriting children.
 */
class PolyNodePtr
{
private:
    void* ptr_;           // Raw pointer (could point anywhere)
    v2::ChildType type_;  // What type of node (empty/inner/leaf/hash)
    bool materialized_;   // true = heap (needs ref counting), false = mmap

    // Helper to increment ref count if materialized
    void
    add_ref() const;

    // Helper to decrement ref count if materialized
    void
    release() const;

public:
    // Default constructor - creates empty reference
    PolyNodePtr()
        : ptr_(nullptr), type_(v2::ChildType::EMPTY), materialized_(false)
    {
    }

    // Constructor with all fields
    PolyNodePtr(void* p, v2::ChildType t, bool m)
        : ptr_(p), type_(t), materialized_(m)
    {
        add_ref();
    }

    // Copy constructor - increments ref count if materialized
    PolyNodePtr(const PolyNodePtr& other)
        : ptr_(other.ptr_)
        , type_(other.type_)
        , materialized_(other.materialized_)
    {
        add_ref();
    }

    // Move constructor - takes ownership, no ref count change
    PolyNodePtr(PolyNodePtr&& other) noexcept
        : ptr_(other.ptr_)
        , type_(other.type_)
        , materialized_(other.materialized_)
    {
        other.ptr_ = nullptr;
        other.type_ = v2::ChildType::EMPTY;
        other.materialized_ = false;
    }

    // Destructor - decrements ref count if materialized
    ~PolyNodePtr()
    {
        release();
    }

    // Copy assignment
    PolyNodePtr&
    operator=(const PolyNodePtr& other)
    {
        if (this != &other)
        {
            release();  // Release old
            ptr_ = other.ptr_;
            type_ = other.type_;
            materialized_ = other.materialized_;
            add_ref();  // Add ref to new
        }
        return *this;
    }

    // Move assignment
    PolyNodePtr&
    operator=(PolyNodePtr&& other) noexcept
    {
        if (this != &other)
        {
            release();  // Release old
            ptr_ = other.ptr_;
            type_ = other.type_;
            materialized_ = other.materialized_;
            other.ptr_ = nullptr;
            other.type_ = v2::ChildType::EMPTY;
            other.materialized_ = false;
        }
        return *this;
    }

    // Factory methods
    static PolyNodePtr
    make_raw_memory(const void* p, v2::ChildType type = v2::ChildType::INNER)
    {
        // Note: does NOT call add_ref since it's not materialized
        PolyNodePtr tp;
        tp.ptr_ = const_cast<void*>(p);
        tp.type_ = type;
        tp.materialized_ = false;
        return tp;
    }

    static PolyNodePtr
    make_materialized(HMapNode* p, v2::ChildType type = v2::ChildType::INNER)
    {
        // Note: Caller is responsible for ensuring proper ref count
        // This is a raw factory - use from_intrusive() for managed pointers
        PolyNodePtr tp;
        tp.ptr_ = p;
        tp.type_ = type;
        tp.materialized_ = true;
        return tp;
    }

    // Factory from intrusive_ptr - takes a ref-counted pointer
    static PolyNodePtr
    from_intrusive(const boost::intrusive_ptr<HMapNode>& p);

    // Convert to intrusive_ptr (only valid for MATERIALIZED)
    [[nodiscard]] boost::intrusive_ptr<HMapNode>
    to_intrusive() const;

    static PolyNodePtr
    make_empty()
    {
        return {};  // null/empty reference
    }

    // Accessors
    [[nodiscard]] void*
    get_raw_ptr() const
    {
        return ptr_;
    }

    [[nodiscard]] const uint8_t*
    get_raw_memory() const
    {
        assert(!materialized_);  // Should be mmap memory
        return static_cast<const uint8_t*>(ptr_);
    }

    [[nodiscard]] HMapNode*
    get_materialized() const
    {
        assert(materialized_);  // Should be heap memory
        return static_cast<HMapNode*>(ptr_);
    }

    [[nodiscard]] v2::ChildType
    get_type() const
    {
        return type_;
    }

    // Type checks
    [[nodiscard]] bool
    is_raw_memory() const
    {
        return !materialized_ && ptr_ != nullptr;
    }

    [[nodiscard]] bool
    is_materialized() const
    {
        return materialized_;
    }

    [[nodiscard]] bool
    is_empty() const
    {
        return ptr_ == nullptr || type_ == v2::ChildType::EMPTY;
    }

    [[nodiscard]] bool
    is_inner() const
    {
        return type_ == v2::ChildType::INNER;
    }

    [[nodiscard]] bool
    is_leaf() const
    {
        return type_ == v2::ChildType::LEAF;
    }

    [[nodiscard]] bool
    is_placeholder() const
    {
        return type_ == v2::ChildType::PLACEHOLDER;
    }

    // Equality (based on pointer only, not metadata)
    bool
    operator==(const PolyNodePtr& other) const
    {
        return ptr_ == other.ptr_;
    }

    bool
    operator!=(const PolyNodePtr& other) const
    {
        return ptr_ != other.ptr_;
    }

    // Bool conversion
    explicit operator bool() const
    {
        return !is_empty();
    }

    /**
     * Copy hash to a buffer (works for all node types)
     * @param dest Buffer to copy 32 bytes to
     */
    void
    copy_hash_to(uint8_t* dest) const;

    /**
     * Get hash as Hash256 (involves a copy)
     */
    [[nodiscard]] Hash256
    get_hash() const;
};

/**
 * Lightweight view for an inner node - just holds pointer to mmap data
 */
struct InnerNodeView
{
    v2::MemPtr<v2::InnerNodeHeader> header;  // Points directly into mmap

    // Get a child iterator on demand
    [[nodiscard]] v2::ChildIterator
    get_child_iter() const
    {
        const auto* offsets_data =
            header.offset(sizeof(v2::InnerNodeHeader)).raw();
        // Simple: just pass the header and offset array pointer
        return {header, offsets_data};
    }

    // Get child type for branch i (EMPTY, INNER, or LEAF)
    [[nodiscard]] v2::ChildType
    get_child_type(int branch) const
    {
        if (branch < 0 || branch >= 16)
        {
            throw std::out_of_range(
                "Branch index " + std::to_string(branch) +
                " out of range [0,16)");
        }
        const auto& header_val = header.get_uncopyable();
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

        // const auto& header_val = header.get_uncopyable();
        //
        // // Create SparseChildOffsets accessor
        // const uint8_t* offsets_base =
        //     header.offset(sizeof(catl::v2::InnerNodeHeader)).raw();
        // catl::v2::SparseChildOffsets offsets(
        //     offsets_base, header_val.child_types);

        // Get the child pointer (will return nullptr if empty)
        const uint8_t* child_ptr = get_sparse_offsets().get_child_ptr(branch);
        if (!child_ptr)
        {
            throw std::runtime_error(
                "No child at branch " + std::to_string(branch));
        }

        return child_ptr;
    }

    // Get a SparseChildOffsets accessor for this node
    [[nodiscard]] v2::SparseChildOffsets
    get_sparse_offsets() const
    {
        const auto& header_val = header.get_uncopyable();
        const uint8_t* offsets_base =
            header.offset(sizeof(v2::InnerNodeHeader)).raw();
        return {offsets_base, header_val.child_types};
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

class MemTreeOps  // TODO:
{
public:
    /**
     * Get an inner node view from a pointer
     * Returns lightweight view with pointer into mmap data
     */
    [[nodiscard]] static InnerNodeView
    get_inner_node(const uint8_t* ptr)
    {
        return InnerNodeView{(v2::MemPtr<v2::InnerNodeHeader>(ptr))};
    }

    /**
     * Get an inner child from a node view
     */
    [[nodiscard]] static InnerNodeView
    get_inner_child(const InnerNodeView& parent, int branch)
    {
        auto child_type = parent.get_child_type(branch);
        if (child_type != v2::ChildType::INNER)
        {
            if (child_type == v2::ChildType::EMPTY)
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
    [[nodiscard]] static LeafView
    get_leaf_child(const InnerNodeView& parent, int branch)
    {
        auto child_type = parent.get_child_type(branch);
        if (child_type != v2::ChildType::LEAF)
        {
            if (child_type == v2::ChildType::EMPTY)
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
        v2::MemPtr<v2::LeafHeader> leaf_header_ptr(leaf_ptr);
        const auto& leaf_header =
            leaf_header_ptr.get_uncopyable();  // Force ref binding

        return LeafView{
            Key(leaf_header.key.data()),  // Now safe - ref in UNSAFE mode!
            Slice(
                leaf_header_ptr.offset(sizeof(v2::LeafHeader)).raw(),
                leaf_header.data_size())};
    }

    /**
     * Lookup a key in the state tree starting from a given inner node
     * @param root The root node to start from
     * @param key The key to search for
     * @return LeafView if found
     * @throws std::runtime_error if key not found
     */
    [[nodiscard]] static LeafView
    lookup_key(const InnerNodeView& root, const Key& key)
    {
        InnerNodeView current = root;
        const auto& root_header = root.header.get_uncopyable();
        int depth = root_header.get_depth();

        // Walk down the tree following the key nibbles
        while (true)
        {
            // Use shamap utility to extract nibble at current depth
            int nibble = shamap::select_branch(key, depth);

            // Check child type
            auto child_type = current.get_child_type(nibble);
            if (child_type == v2::ChildType::EMPTY)
            {
                throw std::runtime_error(
                    "Key not found - no child at nibble " +
                    std::to_string(nibble) + " at depth " +
                    std::to_string(depth));
            }

            if (child_type == v2::ChildType::LEAF)
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
            const auto& current_header = current.header.get_uncopyable();
            depth = current_header.get_depth();
        }
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
    [[nodiscard]] static LeafView
    first_leaf_depth_first(const InnerNodeView& node)
    {
        // Check each branch in order
        for (int i = 0; i < 16; ++i)
        {
            auto child_type = node.get_child_type(i);
            if (child_type == v2::ChildType::EMPTY)
            {
                continue;  // Skip empty branches
            }

            if (child_type == v2::ChildType::LEAF)
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

protected:
    Hash256 hash_;
    bool hash_valid_ = false;

public:
    enum class Type : uint8_t { INNER, LEAF, PLACEHOLDER };

    virtual ~HMapNode() = default;
    virtual Type
    get_type() const = 0;

    // Hash support
    virtual void
    update_hash() = 0;

    const Hash256&
    get_hash()
    {
        if (!hash_valid_)
        {
            update_hash();
            hash_valid_ = true;
        }
        return hash_;
    }

    void
    invalidate_hash()
    {
        hash_valid_ = false;
    }

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
class HmapInnerNode final : public HMapNode
{
private:
    std::array<void*, 16> children_{};  // Just raw pointers, no tagging
    uint32_t child_types_ =
        0;  // 2 bits × 16 children = 32 bits (WHAT: empty/inner/leaf/hash)
    uint16_t materialized_mask_ =
        0;  // 1 bit × 16 children (WHERE: mmap vs heap)
    uint8_t depth_ = 0;

public:
    HmapInnerNode() = default;
    explicit HmapInnerNode(uint8_t depth) : depth_(depth)
    {
    }

    ~HmapInnerNode()
    {
        // Release references to all materialized children
        for (int i = 0; i < 16; ++i)
        {
            if (is_child_materialized(i) && children_[i] != nullptr)
            {
                // Decrement reference count
                auto* node = static_cast<HMapNode*>(children_[i]);
                intrusive_ptr_release(node);
            }
        }
    }

    Type
    get_type() const override
    {
        return Type::INNER;
    }

    // Check if a child is materialized (heap-allocated)
    [[nodiscard]] bool
    is_child_materialized(int branch) const
    {
        assert(branch >= 0 && branch < 16);
        return (materialized_mask_ >> branch) & 1;
    }

    // Get child as PolyNodeRef (lazily generated view)
    [[nodiscard]] PolyNodePtr
    get_child(int branch) const
    {
        assert(branch >= 0 && branch < 16);

        // Lazily construct PolyNodeRef from our storage
        return PolyNodePtr(
            children_[branch],
            get_child_type(branch),
            is_child_materialized(branch));
    }

    [[nodiscard]] v2::ChildType
    get_child_type(int branch) const
    {
        assert(branch >= 0 && branch < 16);
        return static_cast<v2::ChildType>((child_types_ >> (branch * 2)) & 0x3);
    }

    // Set child from PolyNodeRef
    void
    _set_child(int branch, const PolyNodePtr& ref)
    {
        assert(branch >= 0 && branch < 16);

        // Release old child if it was materialized
        if (is_child_materialized(branch) && children_[branch] != nullptr)
        {
            auto* old_node = static_cast<HMapNode*>(children_[branch]);
            intrusive_ptr_release(old_node);
        }

        // Extract raw pointer and store it
        children_[branch] = ref.get_raw_ptr();

        // Add reference to new child if it's materialized
        if (ref.is_materialized() && ref.get_raw_ptr() != nullptr)
        {
            auto* new_node = static_cast<HMapNode*>(ref.get_raw_ptr());
            intrusive_ptr_add_ref(new_node);
        }

        // Update child type
        uint32_t type_mask = ~(0x3u << (branch * 2));
        child_types_ = (child_types_ & type_mask) |
            (static_cast<uint32_t>(ref.get_type()) << (branch * 2));

        // Update materialized bit
        if (ref.is_materialized())
        {
            materialized_mask_ |= (1u << branch);
        }
        else
        {
            materialized_mask_ &= ~(1u << branch);
        }

        invalidate_hash();  // Invalidate cached hash
    }

    // Overload for backward compatibility
    void
    set_child(int branch, const PolyNodePtr& ptr, v2::ChildType type)
    {
        // Create a new PolyNodeRef with the specified type
        PolyNodePtr ref(ptr.get_raw_ptr(), type, ptr.is_materialized());
        _set_child(branch, ref);
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

    void
    update_hash() override;
};

/**
 * Leaf node - contains actual data
 */
class HmapLeafNode final : public HMapNode
{
private:
    Key key_;
    std::vector<uint8_t> data_;  // Owned copy of data

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
        invalidate_hash();
    }

    std::string
    describe() const override
    {
        return "LeafNode(key=" + key_.hex().substr(0, 8) +
            "..., size=" + std::to_string(data_.size()) + ")";
    }

    void
    update_hash() override;
};

/**
 * Placeholder node - just knows the hash, content not loaded yet
 */
class HmapPlaceholder final : public HMapNode
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

    void
    update_hash() override
    {
        // Placeholder already has its hash
        // Do nothing - hash_ is already set
    }
};

/**
 * PathFinder for navigating hybrid SHAMap trees
 * Can traverse through both RAW_MEMORY (mmap) and MATERIALIZED (heap) nodes
 */
class HmapPathFinder
{
private:
    Key target_key_;

    // Path we've traversed: pair of (node_ptr, branch_taken_to_get_here)
    // First element has branch=-1 (root)
    // TODO: linked list!?
    std::vector<std::pair<PolyNodePtr, int>> path_;

    // Terminal node we found (if any)
    PolyNodePtr found_leaf_;
    bool key_matches_ = false;

public:
    explicit HmapPathFinder(const Key& key) : target_key_(key)
    {
    }

    /**
     * Find path to target key starting from root
     * Root can be either RAW_MEMORY or MATERIALIZED
     */
    void
    find_path(const PolyNodePtr& root)
    {
        path_.clear();
        found_leaf_ = PolyNodePtr::make_empty();
        key_matches_ = false;

        // Start at root
        path_.emplace_back(root, -1);

        PolyNodePtr current = root;
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
                    const auto* leaf = static_cast<HmapLeafNode*>(
                        node);  // NOLINT(*-pro-type-static-cast-downcast)
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
                    const auto* inner = static_cast<HmapInnerNode*>(
                        node);  // NOLINT(*-pro-type-static-cast-downcast)
                    depth = inner->get_depth();

                    int branch = shamap::select_branch(target_key_, depth);
                    PolyNodePtr child = inner->get_child(branch);

                    if (!child)
                    {
                        // Empty branch
                        break;
                    }

                    path_.emplace_back(child, branch);
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
                             v2::ChildType::LEAF);
                    }
                    else
                    {
                        // Parent is still raw, check its header
                        const uint8_t* parent_raw = parent_ptr.get_raw_memory();
                        v2::MemPtr<v2::InnerNodeHeader> header(parent_raw);
                        const auto& header_val = header.get_uncopyable();
                        is_leaf =
                            (header_val.get_child_type(branch_taken) ==
                             v2::ChildType::LEAF);
                    }
                }
                else if (i == path_.size() - 1 && found_leaf_)
                {
                    // Last node in path and we found a leaf
                    is_leaf = true;
                }

                auto materialized = materialize_raw_node(raw, is_leaf);

                // Update this entry in the path using proper ref counting
                node_ptr = PolyNodePtr::from_intrusive(materialized);

                // Update parent's child pointer if not root
                if (i > 0)
                {
                    auto& [parent_ptr, _] = path_[i - 1];
                    assert(parent_ptr.is_materialized());
                    auto* parent_inner = static_cast<HmapInnerNode*>(
                        parent_ptr.get_materialized());
                    // Determine child type based on whether it's a leaf
                    v2::ChildType child_type =
                        is_leaf ? v2::ChildType::LEAF : v2::ChildType::INNER;
                    parent_inner->set_child(branch_taken, node_ptr, child_type);
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
    [[nodiscard]] PolyNodePtr
    get_found_leaf() const
    {
        return found_leaf_;
    }
    [[nodiscard]] const std::vector<std::pair<PolyNodePtr, int>>&
    get_path() const
    {
        return path_;
    }

    // Debug helper
    void
    debug_path() const
    {
        LOGD("Path to key ", target_key_.hex());
        for (size_t i = 0; i < path_.size(); ++i)
        {
            const auto& [node_ptr, branch] = path_[i];
            std::stringstream ss;
            ss << "  [" << i << "] ";
            if (branch >= 0)
            {
                ss << "branch " << branch << " -> ";
            }

            // Print node info and hash
            if (node_ptr.is_raw_memory())
            {
                ss << "RAW_MEMORY @ " << node_ptr.get_raw_ptr();

                // Print hash from mmap header
                const uint8_t* raw = node_ptr.get_raw_memory();
                if (node_ptr.is_inner())
                {
                    v2::MemPtr<v2::InnerNodeHeader> header(raw);
                    const auto& h = header.get_uncopyable();
                    ss << " depth=" << (int)h.get_depth();
                    ss << " hash=";
                    for (int j = 0; j < 8; ++j)
                    {  // First 8 bytes
                        char buf[3];
                        sprintf(buf, "%02x", h.hash[j]);
                        ss << buf;
                    }
                    ss << "...";
                }
                else if (node_ptr.is_leaf())
                {
                    v2::MemPtr<v2::LeafHeader> header(raw);
                    const auto& h = header.get_uncopyable();
                    ss << " hash=";
                    for (int j = 0; j < 8; ++j)
                    {  // First 8 bytes
                        char buf[3];
                        sprintf(buf, "%02x", h.hash[j]);
                        ss << buf;
                    }
                    ss << "...";
                }
            }
            else
            {
                auto* node = node_ptr.get_materialized();
                ss << "MATERIALIZED " << node->describe();

                // Print hash if valid
                if (node->get_type() == HMapNode::Type::INNER)
                {
                    ss << " hash=" << node->get_hash().hex().substr(0, 16)
                       << "...";
                }
                else if (node->get_type() == HMapNode::Type::LEAF)
                {
                    ss << " hash=" << node->get_hash().hex().substr(0, 16)
                       << "...";
                }
            }
            LOGD(ss.str());
        }
        if (found_leaf_)
        {
            LOGD(
                "  Found leaf, key ",
                (key_matches_ ? "MATCHES" : "does NOT match"));
        }
        else
        {
            LOGD("  No leaf found");
        }
    }

private:
    /**
     * Navigate through a raw memory inner node
     * Returns true if we should continue, false if we hit a leaf/empty
     */
    bool
    navigate_raw_inner(PolyNodePtr& current, int& depth)
    {
        const uint8_t* raw = current.get_raw_memory();
        InnerNodeView view{catl::v2::MemPtr<v2::InnerNodeHeader>(raw)};

        const auto& header = view.header.get_uncopyable();
        depth = header.get_depth();

        int branch = shamap::select_branch(target_key_, depth);
        auto child_type = header.get_child_type(branch);

        if (child_type == v2::ChildType::EMPTY)
        {
            // Empty branch
            return false;
        }

        const uint8_t* child_ptr = view.get_child_ptr(branch);
        PolyNodePtr child = PolyNodePtr::make_raw_memory(child_ptr, child_type);

        if (child_type == v2::ChildType::LEAF)
        {
            // It's a leaf - check the key
            const v2::MemPtr<v2::LeafHeader> leaf_header_ptr(child_ptr);
            const auto& leaf_header = leaf_header_ptr.get_uncopyable();

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
    static boost::intrusive_ptr<HMapNode>
    materialize_raw_node(const uint8_t* raw, bool is_leaf)
    {
        if (is_leaf)
        {
            // Materialize as leaf
            v2::MemPtr<v2::LeafHeader> leaf_header_ptr(raw);
            const auto& header = leaf_header_ptr.get_uncopyable();

            Key key(header.key.data());
            Slice data(raw + sizeof(v2::LeafHeader), header.data_size());

            return {new HmapLeafNode(key, data)};
        }
        else
        {
            // Materialize as inner
            v2::MemPtr<v2::InnerNodeHeader> inner_ptr(raw);
            const auto& header = inner_ptr.get_uncopyable();

            // ReSharper disable once CppDFAMemoryLeak
            auto* inner = new HmapInnerNode(header.get_depth());
            boost::intrusive_ptr<HMapNode> inner_ptr_managed(inner);

            // Copy children (both types and pointers) from mmap header
            const InnerNodeView view{inner_ptr};
            const auto offsets = view.get_sparse_offsets();

            for (int i = 0; i < 16; ++i)
            {
                v2::ChildType child_type = header.get_child_type(i);
                if (child_type != v2::ChildType::EMPTY)
                {
                    const uint8_t* child_raw = offsets.get_child_ptr(i);
                    inner->set_child(
                        i,
                        PolyNodePtr::make_raw_memory(child_raw, child_type),
                        child_type);
                }
            }

            // ReSharper disable once CppDFAMemoryLeak
            return inner_ptr_managed;
        }
    }
};

class Hmap
{
private:
    PolyNodePtr root_;  // Root can be any type of pointer
    std::vector<std::shared_ptr<v2::MmapHolder>>
        mmap_holders_;  // Keeps all mmap files alive

public:
    Hmap() = default;

    // Constructor with single mmap holder
    explicit Hmap(std::shared_ptr<v2::MmapHolder> holder)
    {
        if (holder)
        {
            mmap_holders_.push_back(std::move(holder));
        }
    }

    // Constructor with vector of mmap holders
    explicit Hmap(std::vector<std::shared_ptr<v2::MmapHolder>> holders)
        : mmap_holders_(std::move(holders))
    {
    }

    // Add an mmap holder to keep a file alive
    void
    add_mmap_holder(std::shared_ptr<v2::MmapHolder> holder)
    {
        if (holder)
        {
            mmap_holders_.push_back(std::move(holder));
        }
    }

    // Initialize with a raw memory root (from mmap)
    void
    set_root_raw(const uint8_t* raw_root)
    {
        root_ = PolyNodePtr::make_raw_memory(raw_root);
    }

    // Initialize with a materialized root
    void
    set_root_materialized(HmapInnerNode* node)
    {
        // We need to be careful about ownership here!
        root_ = PolyNodePtr::make_materialized(node);
    }

    [[nodiscard]] PolyNodePtr
    get_root() const
    {
        return root_;
    }

    // Set the root to any PolyNodeRef
    void
    set_root(const PolyNodePtr& new_root)
    {
        root_ = new_root;
    }

    [[nodiscard]] const std::vector<std::shared_ptr<v2::MmapHolder>>&
    get_mmap_holders() const
    {
        return mmap_holders_;
    }
};

// Implementation of TaggedPtr methods that depend on HMapNode

inline void
PolyNodePtr::add_ref() const
{
    if (is_materialized() && !is_empty())
    {
        intrusive_ptr_add_ref(get_materialized());
    }
}

inline void
PolyNodePtr::release() const
{
    if (is_materialized() && !is_empty())
    {
        intrusive_ptr_release(get_materialized());
    }
}

inline PolyNodePtr
PolyNodePtr::from_intrusive(const boost::intrusive_ptr<HMapNode>& p)
{
    if (!p)
    {
        return make_empty();
    }

    // Determine the child type from the node type
    v2::ChildType type;
    switch (p->get_type())
    {
        case HMapNode::Type::INNER:
            type = v2::ChildType::INNER;
            break;
        case HMapNode::Type::LEAF:
            type = v2::ChildType::LEAF;
            break;
        case HMapNode::Type::PLACEHOLDER:
            type = v2::ChildType::PLACEHOLDER;
            break;
        default:
            type = v2::ChildType::EMPTY;
    }

    // Create PolyNodeRef and manually increment ref count
    PolyNodePtr tp = make_materialized(p.get(), type);
    intrusive_ptr_add_ref(p.get());
    return tp;
}

inline boost::intrusive_ptr<HMapNode>
PolyNodePtr::to_intrusive() const
{
    if (!is_materialized() || is_empty())
    {
        return nullptr;
    }

    // Create intrusive_ptr which will increment ref count
    return {get_materialized()};
}

inline void
PolyNodePtr::copy_hash_to(uint8_t* dest) const
{
    if (is_empty())
    {
        // Zero hash for empty
        std::memset(dest, 0, Hash256::size());
        return;
    }

    if (is_materialized())
    {
        // Get hash from materialized node
        auto* node = get_materialized();
        const auto& hash = node->get_hash();
        std::memcpy(dest, hash.data(), Hash256::size());
    }
    else
    {
        // Get perma-cached hash from mmap header
        const uint8_t* raw = get_raw_memory();
        if (is_inner())
        {
            v2::MemPtr<v2::InnerNodeHeader> header(raw);
            const auto& h = header.get_uncopyable();
            std::memcpy(dest, h.hash.data(), Hash256::size());
        }
        else if (is_leaf())
        {
            v2::MemPtr<v2::LeafHeader> header(raw);
            const auto& h = header.get_uncopyable();
            std::memcpy(dest, h.hash.data(), Hash256::size());
        }
        else if (is_placeholder())
        {
            // Placeholder should have its hash stored
            auto* placeholder =
                static_cast<HmapPlaceholder*>(get_materialized());
            const auto& hash = placeholder->get_hash();
            std::memcpy(dest, hash.data(), Hash256::size());
        }
        else
        {
            // Unknown type - zero hash
            std::memset(dest, 0, Hash256::size());
        }
    }
}

inline Hash256
PolyNodePtr::get_hash() const
{
    Hash256 result;
    copy_hash_to(result.data());
    return result;
}

}  // namespace catl::hybrid_shamap
