#pragma once
#include "catl/crypto/sha512-half-hasher.h"
#include "catl/shamap/shamap-hashprefix.h"
#include "catl/shamap/shamap-options.h"  // For SetMode and SetResult
#include "catl/shamap/shamap-utils.h"
#include "catl/v2/catl-v2-memtree.h"
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

// Import v2 types we'll use
using catl::v2::InnerNodeView;
using catl::v2::LeafView;
using catl::v2::MemTreeOps;

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

    /**
     * Get a MemPtr of specified type pointing to raw memory
     * Only valid for non-materialized (mmap) nodes
     * @tparam T The type to cast to (e.g., v2::LeafHeader, v2::InnerNodeHeader)
     * @return MemPtr<T> pointing to the raw memory
     */
    template <typename T>
    [[nodiscard]] v2::MemPtr<T>
    get_memptr() const
    {
        assert(!materialized_);  // Should only be used for mmap nodes
        assert(ptr_ != nullptr);
        return v2::MemPtr<T>(static_cast<const uint8_t*>(ptr_));
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

        LOGD(
            "[HmapPathFinder] Starting path finding for key: ",
            target_key_.hex());
        LOGD(
            "  Root is raw: ",
            root.is_raw_memory(),
            " materialized: ",
            root.is_materialized());

        // Start at root
        path_.emplace_back(root, -1);

        PolyNodePtr current = root;
        int depth = 0;
        int iteration = 0;

        while (current)
        {
            if (++iteration > 64)
            {
                LOGE("Path finding exceeded maximum depth!");
                throw std::runtime_error("Path finding exceeded maximum depth");
            }

            LOGD("  Iteration ", iteration, " at depth ", depth);

            if (current.is_raw_memory())
            {
                LOGD("    Navigating raw memory node...");
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

                    // Check if we've reached a leaf
                    if (child.is_leaf())
                    {
                        // Found a leaf - check if it matches our key
                        if (child.is_materialized())
                        {
                            auto* leaf = static_cast<HmapLeafNode*>(
                                child.get_materialized());
                            found_leaf_ = child;
                            key_matches_ = (leaf->get_key() == target_key_);
                        }
                        else
                        {
                            // Raw memory leaf
                            auto leaf_header_ptr = child.get_memptr<v2::LeafHeader>();
                            const auto& leaf_header = *leaf_header_ptr;
                            found_leaf_ = child;
                            key_matches_ =
                                (std::memcmp(
                                     leaf_header.key.data(),
                                     target_key_.data(),
                                     32) == 0);
                        }
                        path_.emplace_back(child, branch);
                        break;  // Stop navigation at leaf
                    }

                    // It's an inner node, continue navigation
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
                        auto header = parent_ptr.get_memptr<v2::InnerNodeHeader>();
                        const auto& header_val = *header;
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
                    const auto& h = *header;
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
                    const auto& h = *header;
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

        // Debug: validate the pointer
        if (!raw || reinterpret_cast<uintptr_t>(raw) < 0x1000)
        {
            LOGE("Invalid raw pointer in navigate_raw_inner: ", raw);
            throw std::runtime_error("Invalid raw pointer in tree navigation");
        }

        // Check for suspicious addresses (like our crash address)
        if (reinterpret_cast<uintptr_t>(raw) > 0x700000000000)
        {
            LOGE("Suspicious raw pointer (too large): ", raw);
            LOGE("  This indicates corrupt node data or wrong node type");
            throw std::runtime_error("Corrupt raw pointer detected");
        }

        InnerNodeView view{v2::MemPtr<v2::InnerNodeHeader>(raw)};

        const auto& header = *view.header_ptr;
        depth = header.get_depth();

        // Validate depth
        if (depth < 0 || depth >= 64)
        {
            LOGE("Invalid depth in inner node: ", depth);
            LOGE("  Raw pointer: ", raw);
            LOGE(
                "  This likely means we're reading wrong data type as inner "
                "node");
            throw std::runtime_error("Invalid depth in inner node");
        }

        int branch = shamap::select_branch(target_key_, depth);
        auto child_type = header.get_child_type(branch);

        if (child_type == v2::ChildType::EMPTY)
        {
            // Empty branch
            return false;
        }

        const uint8_t* child_ptr = view.get_child_ptr(branch);

        // Validate child pointer
        if (!child_ptr || reinterpret_cast<uintptr_t>(child_ptr) < 0x1000)
        {
            LOGE("Invalid child pointer from sparse offsets");
            LOGE("  Branch: ", branch, " Type: ", static_cast<int>(child_type));
            throw std::runtime_error("Invalid child pointer");
        }

        if (reinterpret_cast<uintptr_t>(child_ptr) > 0x700000000000)
        {
            LOGE("Suspicious child pointer (too large): ", child_ptr);
            LOGE("  Parent depth: ", depth, " branch: ", branch);
            throw std::runtime_error("Corrupt child pointer detected");
        }

        PolyNodePtr child = PolyNodePtr::make_raw_memory(child_ptr, child_type);

        if (child_type == v2::ChildType::LEAF)
        {
            // It's a leaf - check the key
            const v2::MemPtr<v2::LeafHeader> leaf_header_ptr(child_ptr);
            const auto& leaf_header = *leaf_header_ptr;

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
            const auto& header = *leaf_header_ptr;

            Key key(header.key.data());
            Slice data(raw + sizeof(v2::LeafHeader), header.data_size());

            return {new HmapLeafNode(key, data)};
        }
        else
        {
            // Materialize as inner
            v2::MemPtr<v2::InnerNodeHeader> inner_ptr(raw);
            const auto& header = *inner_ptr;

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

    [[nodiscard]] Hash256
    get_root_hash() const
    {
        if (!root_)
        {
            return Hash256::zero();
        }
        return root_.get_hash();
    }

    /**
     * Set (add or update) an item in the tree
     * Will materialize nodes along the path as needed
     *
     * @param key The key to set
     * @param data The data to store
     * @return true if item was added, false if updated
     */
    shamap::SetResult
    set_item(
        const Key& key,
        const Slice& data,
        shamap::SetMode mode = shamap::SetMode::ADD_OR_UPDATE)
    {
        // If we have no root, create one
        if (!root_)
        {
            // Create a new root at depth 0
            auto* new_root = new HmapInnerNode(0);
            root_ = PolyNodePtr::from_intrusive(
                boost::intrusive_ptr<HMapNode>(new_root));
        }

        // Find the path to where this key should go
        HmapPathFinder pathfinder(key);
        pathfinder.find_path(root_);

        // Materialize the path so we can modify it
        pathfinder.materialize_path();

        // Update root if it was materialized
        const auto& path = pathfinder.get_path();
        if (!path.empty())
        {
            root_ = path[0].first;  // Update root to materialized version
        }

        // Check if we found an existing leaf with this key
        if (pathfinder.found_leaf() && pathfinder.key_matches())
        {
            // UPDATE case - replace the old leaf with a new one
            // (Leaves are immutable, so we always create a new leaf)

            // Find the parent of the leaf
            for (size_t i = 0; i < path.size(); ++i)
            {
                if (i > 0 && path[i].first.is_leaf())
                {
                    // Previous node is the parent
                    auto& parent_node = path[i - 1].first;
                    assert(
                        parent_node.is_inner() &&
                        parent_node.is_materialized());

                    auto* parent = static_cast<HmapInnerNode*>(
                        parent_node.get_materialized());
                    int branch = path[i].second;

                    // Create new leaf with updated data
                    auto* new_leaf = new HmapLeafNode(key, data);
                    parent->set_child(
                        branch,
                        PolyNodePtr::from_intrusive(
                            boost::intrusive_ptr<HMapNode>(new_leaf)),
                        v2::ChildType::LEAF);

                    // Check mode constraints
                    if (mode == shamap::SetMode::ADD_ONLY)
                    {
                        // Item exists but ADD_ONLY was specified
                        return shamap::SetResult::FAILED;
                    }
                    return shamap::SetResult::UPDATE;
                }
            }

            // This shouldn't happen - we found a leaf but can't find its parent
            throw std::runtime_error(
                "Found leaf but couldn't find parent in path");
        }

        // ADD case - need to insert a new leaf

        // Get the last inner node in the path
        if (path.empty())
        {
            throw std::runtime_error(
                "Path should not be empty after materialization");
        }

        // Find the deepest inner node
        HmapInnerNode* insert_parent = nullptr;
        int insert_depth = 0;

        for (auto it = path.rbegin(); it != path.rend(); ++it)
        {
            if (it->first.is_inner() && it->first.is_materialized())
            {
                insert_parent =
                    static_cast<HmapInnerNode*>(it->first.get_materialized());
                insert_depth = insert_parent->get_depth();
                break;
            }
        }

        if (!insert_parent)
        {
            throw std::runtime_error("No inner node found in path");
        }

        // Determine which branch to use
        int branch = shamap::select_branch(key, insert_depth);

        // Check what's at that branch
        auto existing = insert_parent->get_child(branch);

        if (existing.is_empty())
        {
            // Simple case - empty branch, just insert the leaf
            auto* new_leaf = new HmapLeafNode(key, data);
            insert_parent->set_child(
                branch,
                PolyNodePtr::from_intrusive(
                    boost::intrusive_ptr<HMapNode>(new_leaf)),
                v2::ChildType::LEAF);
            // Check mode constraints
            if (mode == shamap::SetMode::UPDATE_ONLY)
            {
                // Item doesn't exist but UPDATE_ONLY was specified
                return shamap::SetResult::FAILED;
            }
            return shamap::SetResult::ADD;
        }
        else if (existing.is_leaf())
        {
            // Collision - need to create intermediate inner node(s)

            // Get the existing leaf's key
            Key existing_key = [&]() -> Key {
                if (existing.is_materialized())
                {
                    auto* existing_leaf =
                        static_cast<HmapLeafNode*>(existing.get_materialized());
                    return existing_leaf->get_key();
                }
                else
                {
                    // Read from mmap
                    auto header = existing.get_memptr<v2::LeafHeader>();
                    return Key(header->key.data());
                }
            }();

            // Find where they diverge using existing utility
            int divergence_depth = shamap::find_divergence_depth(
                key, existing_key, insert_depth + 1);

            // Create new inner node at divergence depth
            auto* divergence_node = new HmapInnerNode(divergence_depth);

            // Add the new leaf
            auto* new_leaf = new HmapLeafNode(key, data);
            divergence_node->set_child(
                shamap::select_branch(key, divergence_depth),
                PolyNodePtr::from_intrusive(
                    boost::intrusive_ptr<HMapNode>(new_leaf)),
                v2::ChildType::LEAF);

            // Add the existing leaf
            divergence_node->set_child(
                shamap::select_branch(existing_key, divergence_depth),
                existing,
                v2::ChildType::LEAF);

            // Replace the branch in the parent
            insert_parent->set_child(
                branch,
                PolyNodePtr::from_intrusive(
                    boost::intrusive_ptr<HMapNode>(divergence_node)),
                v2::ChildType::INNER);

            // Check mode constraints
            if (mode == shamap::SetMode::UPDATE_ONLY)
            {
                // Item doesn't exist but UPDATE_ONLY was specified
                return shamap::SetResult::FAILED;
            }
            return shamap::SetResult::ADD;
        }
        else
        {
            // Existing is an inner node - this shouldn't happen if pathfinder
            // worked correctly
            throw std::runtime_error(
                "Unexpected inner node at insertion point");
        }
    }

    /**
     * Remove an item from the tree
     *
     * @param key The key to remove
     * @return true if item was removed, false if not found
     */
    bool
    remove_item(const Key& key)
    {
        LOGD("[remove_item] Starting removal for key: ", key.hex());

        if (!root_)
        {
            LOGD("[remove_item] Empty tree, nothing to remove");
            return false;  // Empty tree
        }

        // Find the path to the key
        HmapPathFinder pathfinder(key);
        pathfinder.find_path(root_);

        // Check if we found the key
        if (!pathfinder.found_leaf() || !pathfinder.key_matches())
        {
            LOGD("[remove_item] Key not found: ", key.hex());
            return false;  // Key not found
        }

        LOGD(
            "[remove_item] Found key, materializing path of size ",
            pathfinder.get_path().size());

        // Materialize the path so we can modify it
        pathfinder.materialize_path();

        // Update root if it was materialized
        const auto& path = pathfinder.get_path();
        if (!path.empty())
        {
            assert(
                path[0].first.is_materialized() &&
                "Root should be materialized after materialize_path()");
            root_ = path[0].first;  // Update root to materialized version
        }

        // Verify all nodes in path are properly set up
        for (size_t i = 0; i < path.size(); ++i)
        {
            LOGD(
                "[remove_item] Path[",
                i,
                "] is_materialized=",
                path[i].first.is_materialized(),
                " is_leaf=",
                path[i].first.is_leaf(),
                " is_inner=",
                path[i].first.is_inner(),
                " branch=",
                path[i].second);
            if (i <
                path.size() - 1)  // All non-leaf nodes should be materialized
            {
                assert(
                    path[i].first.is_materialized() &&
                    "All inner nodes in path should be materialized");
                assert(
                    path[i].first.is_inner() &&
                    "Non-terminal path nodes should be inner nodes");
            }
        }

        // Find the parent of the leaf to remove
        HmapInnerNode* parent = nullptr;
        int branch_to_remove = -1;
        size_t leaf_index = 0;

        for (size_t i = 0; i < path.size(); ++i)
        {
            if (i > 0 && path[i].first.is_leaf())
            {
                // Previous node is the parent
                auto& parent_node = path[i - 1].first;
                assert(
                    parent_node.is_inner() &&
                    "Parent of leaf should be inner node");
                assert(
                    parent_node.is_materialized() &&
                    "Parent should be materialized");

                if (parent_node.is_inner() && parent_node.is_materialized())
                {
                    parent = static_cast<HmapInnerNode*>(
                        parent_node.get_materialized());
                    branch_to_remove = path[i].second;
                    leaf_index = i;
                    LOGD(
                        "[remove_item] Found leaf at path[",
                        i,
                        "], parent at [",
                        i - 1,
                        "], branch=",
                        branch_to_remove);
                    break;
                }
            }
        }

        if (!parent || branch_to_remove == -1)
        {
            LOGE("[remove_item] Couldn't find parent of leaf!");
            return false;  // Couldn't find parent
        }

        LOGD(
            "[remove_item] Removing leaf from parent at branch ",
            branch_to_remove);

        // Remove the leaf
        parent->set_child(
            branch_to_remove, PolyNodePtr::make_empty(), v2::ChildType::EMPTY);

        LOGD("[remove_item] Starting collapse phase");

        // Collapse path - promote single children up the tree
        // Start from the parent and work our way up
        for (int i = static_cast<int>(path.size()) - 2; i >= 0; --i)
        {
            LOGD("[remove_item] Checking collapse at path[", i, "]");

            // After materialize_path(), ALL inner nodes in path should be
            // materialized! (The leaf might still be raw, but we've already
            // removed it)
            if (i < static_cast<int>(leaf_index))
            {
                assert(
                    path[i].first.is_materialized() &&
                    "Inner path node not materialized after "
                    "materialize_path()!");
            }

            if (!path[i].first.is_inner())
            {
                LOGD("[remove_item] Path[", i, "] is not inner, skipping");
                continue;  // It's a leaf, skip
            }

            assert(
                path[i].first.is_materialized() &&
                "Inner node must be materialized for collapse");
            auto* inner =
                static_cast<HmapInnerNode*>(path[i].first.get_materialized());

            // Count children and find the single child if there is one
            PolyNodePtr single_child;
            int child_count = 0;
            int single_child_branch = -1;

            for (int branch = 0; branch < 16; ++branch)
            {
                auto child = inner->get_child(branch);
                if (!child.is_empty())
                {
                    child_count++;
                    if (child_count == 1)
                    {
                        single_child = child;
                        single_child_branch = branch;
                    }
                    else if (child_count > 1)
                    {
                        break;  // More than one child, can't collapse
                    }
                }
            }

            LOGD("[remove_item] Path[", i, "] has ", child_count, " children");
            if (child_count == 1)
            {
                LOGD(
                    "  Single child at branch ",
                    single_child_branch,
                    " is_leaf=",
                    single_child.is_leaf(),
                    " is_materialized=",
                    single_child.is_materialized());
            }

            // If this inner node has only one child and it's a leaf, promote it
            if (child_count == 1 && single_child.is_leaf() && i > 0)
            {
                LOGD(
                    "[remove_item] Collapsing: promoting single leaf child up "
                    "from path[",
                    i,
                    "]");

                // Get the parent of this inner node
                auto& parent_entry = path[i - 1];
                assert(parent_entry.first.is_inner() && "Parent must be inner");
                assert(
                    parent_entry.first.is_materialized() &&
                    "Parent must be materialized");

                if (parent_entry.first.is_inner() &&
                    parent_entry.first.is_materialized())
                {
                    auto* parent_inner = static_cast<HmapInnerNode*>(
                        parent_entry.first.get_materialized());
                    int branch_in_parent = path[i].second;

                    LOGD(
                        "  Replacing inner at parent's branch ",
                        branch_in_parent,
                        " with leaf (type=",
                        static_cast<int>(single_child.get_type()),
                        ")");

                    // Replace this inner node with its single leaf child
                    // IMPORTANT: preserve the actual type of the child
                    parent_inner->set_child(
                        branch_in_parent,
                        single_child,
                        single_child.get_type());
                }
            }
            else if (child_count == 1 && !single_child.is_leaf())
            {
                LOGD(
                    "[remove_item] Single child is inner node, stopping "
                    "collapse");
                break;  // Don't collapse inner nodes
            }
            else if (child_count > 1)
            {
                LOGD(
                    "[remove_item] Multiple children (",
                    child_count,
                    "), stopping collapse");
                break;
            }
            else if (child_count == 0)
            {
                LOGW(
                    "[remove_item] Inner node has NO children after removal! "
                    "This shouldn't happen");
            }
        }

        LOGD("[remove_item] Successfully removed key: ", key.hex());
        return true;
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
        if (is_inner())
        {
            auto header = get_memptr<v2::InnerNodeHeader>();
            std::memcpy(dest, header->hash.data(), Hash256::size());
        }
        else if (is_leaf())
        {
            auto header = get_memptr<v2::LeafHeader>();
            std::memcpy(dest, header->hash.data(), Hash256::size());
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
