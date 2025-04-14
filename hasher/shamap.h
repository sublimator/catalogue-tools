#pragma once

#include <array>
#include <atomic>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <memory>
#include <openssl/evp.h>
#include <stdexcept>
#include <string>
#include <vector>

#include "hasher/core-types.h"

enum SHAMapNodeType : uint8_t {
    tnINNER = 1,
    tnTRANSACTION_NM = 2,  // transaction, no metadata
    tnTRANSACTION_MD = 3,  // transaction, with metadata
    tnACCOUNT_STATE = 4,
    tnREMOVE = 254,
    tnTERMINAL = 255  // special type to mark the end of a serialization stream
};

//----------------------------------------------------------
// Custom Exception Classes
//----------------------------------------------------------
class SHAMapException : public std::runtime_error
{
public:
    explicit SHAMapException(const std::string& message);
};

class InvalidDepthException : public SHAMapException
{
public:
    explicit InvalidDepthException(int depth, size_t maxAllowed);
    int
    depth() const;
    size_t
    max_allowed() const;

private:
    int depth_;
    size_t maxAllowed_;
};

class InvalidBranchException : public SHAMapException
{
public:
    explicit InvalidBranchException(int branch);
    int
    branch() const;

private:
    int branch_;
};

class NullNodeException : public SHAMapException
{
public:
    explicit NullNodeException(const std::string& context);
};

class NullItemException : public SHAMapException
{
public:
    explicit NullItemException();
};

class HashCalculationException : public SHAMapException
{
public:
    explicit HashCalculationException(const std::string& reason);
};

/**
 * Helper function to select a branch based on a key and depth
 */
int
select_branch(const Key& key, int depth);

//----------------------------------------------------------
// SHAMap Node Classes
//----------------------------------------------------------

/**
 * Abstract base class for SHAMap tree nodes
 */
class SHAMapTreeNode
{
protected:
    Hash256 hash;
    bool hashValid = false;
    mutable std::atomic<int> refCount_{0};

public:
    virtual ~SHAMapTreeNode() = default;
    void
    invalidate_hash();
    virtual bool
    is_leaf() const = 0;
    virtual bool
    is_inner() const = 0;
    virtual void
    update_hash() = 0;  // NO LOGGING INSIDE IMPLEMENTATIONS
    const Hash256&
    get_hash();

    // friend declarations needed for boost::intrusive_ptr
    friend void
    intrusive_ptr_add_ref(const SHAMapTreeNode* p);
    friend void
    intrusive_ptr_release(const SHAMapTreeNode* p);
};

/**
 * Leaf node in the SHAMap tree
 */
class SHAMapLeafNode : public SHAMapTreeNode
{
private:
    boost::intrusive_ptr<MmapItem> item;
    SHAMapNodeType type;
    int version = -1;  // Version for CoW tracking

public:
    SHAMapLeafNode(boost::intrusive_ptr<MmapItem> i, SHAMapNodeType t);
    bool
    is_leaf() const override;
    bool
    is_inner() const override;
    void
    update_hash() override;
    boost::intrusive_ptr<MmapItem>
    get_item() const;
    SHAMapNodeType
    get_type() const;

protected:
    friend class PathFinder;
    friend class SHAMap;

    // CoW support - only accessible to friends
    boost::intrusive_ptr<SHAMapLeafNode>
    copy() const;
    int
    get_version() const
    {
        return version;
    }
    void
    set_version(int v)
    {
        version = v;
    }
};

/**
 * Memory-optimized container for SHAMapInnerNode children with iteration
 * support
 */
class NodeChildren
{
private:
    boost::intrusive_ptr<SHAMapTreeNode>* children_;  // Dynamic array
    uint16_t branchMask_ = 0;         // Bit mask of active branches
    uint8_t capacity_ = 0;            // Actual allocation size
    bool canonicalized_ = false;      // Has this been optimized?
    int8_t branchToIndex_[16] = {0};  // Maps branch to array index

public:
    // Iterator class for iterating through valid children
    class iterator
    {
    private:
        NodeChildren const* container_;
        int currentBranch_;

        // Find next valid branch
        void
        findNextValid()
        {
            while (currentBranch_ < 16 &&
                   !(container_->branchMask_ & (1 << currentBranch_)))
            {
                ++currentBranch_;
            }
        }

    public:
        // Standard iterator type definitions
        using iterator_category = std::forward_iterator_tag;
        using value_type = boost::intrusive_ptr<SHAMapTreeNode>;
        using difference_type = std::ptrdiff_t;
        using pointer = const value_type*;
        using reference = const value_type&;

        iterator(NodeChildren const* container, int branch)
            : container_(container), currentBranch_(branch)
        {
            findNextValid();
        }

        reference
        operator*() const
        {
            if (container_->canonicalized_)
            {
                return container_
                    ->children_[container_->branchToIndex_[currentBranch_]];
            }
            else
            {
                return container_->children_[currentBranch_];
            }
        }

        pointer
        operator->() const
        {
            if (container_->canonicalized_)
            {
                return &container_->children_
                            [container_->branchToIndex_[currentBranch_]];
            }
            else
            {
                return &container_->children_[currentBranch_];
            }
        }

        iterator&
        operator++()
        {
            ++currentBranch_;
            findNextValid();
            return *this;
        }

        iterator
        operator++(int)
        {
            iterator temp = *this;
            ++(*this);
            return temp;
        }

        bool
        operator==(const iterator& other) const
        {
            return container_ == other.container_ &&
                currentBranch_ == other.currentBranch_;
        }

        bool
        operator!=(const iterator& other) const
        {
            return !(*this == other);
        }

        // Get the current branch index (useful for knowing which child)
        int
        branch() const
        {
            return currentBranch_;
        }
    };

    // Constructor - always starts with full 16 slots
    NodeChildren();
    ~NodeChildren();

    // Core operations
    boost::intrusive_ptr<SHAMapTreeNode>
    getChild(int branch) const;
    void
    setChild(int branch, boost::intrusive_ptr<SHAMapTreeNode> child);
    bool
    hasChild(int branch) const
    {
        return (branchMask_ & (1 << branch)) != 0;
    }
    int
    getChildCount() const
    {
        return __builtin_popcount(branchMask_);
    }
    uint16_t
    getBranchMask() const
    {
        return branchMask_;
    }

    // Memory optimization
    void
    canonicalize();
    bool
    isCanonical() const
    {
        return canonicalized_;
    }

    // For Copy-on-Write
    std::unique_ptr<NodeChildren>
    copy() const;

    // No copying
    NodeChildren(const NodeChildren&) = delete;
    NodeChildren&
    operator=(const NodeChildren&) = delete;

    // Iteration support - iterates only through non-empty children
    iterator
    begin() const
    {
        return iterator(this, 0);
    }
    iterator
    end() const
    {
        return iterator(this, 16);
    }

    // Array-like access (both const and non-const versions)
    const boost::intrusive_ptr<SHAMapTreeNode>&
    operator[](int branch) const;
    boost::intrusive_ptr<SHAMapTreeNode>&
    operator[](int branch);
};

/**
 * Inner (branch) node in the SHAMap tree
 */
class SHAMapInnerNode : public SHAMapTreeNode
{
private:
    std::unique_ptr<NodeChildren> children_;
    uint8_t depth_ = 0;

    // CoW support
    std::atomic<int> version{0};
    bool do_cow_ = false;

public:
    explicit SHAMapInnerNode(uint8_t nodeDepth = 0);
    SHAMapInnerNode(bool isCopy, uint8_t nodeDepth, int initialVersion);
    bool
    is_leaf() const override;
    bool
    is_inner() const override;
    uint8_t
    getDepth() const;
    void
    setDepth(uint8_t newDepth);
    void
    update_hash() override;
    bool
    set_child(int branch, boost::intrusive_ptr<SHAMapTreeNode> const& child);
    boost::intrusive_ptr<SHAMapTreeNode>
    get_child(int branch) const;
    bool
    has_child(int branch) const;
    int
    get_branch_count() const;
    uint16_t
    get_branch_mask() const;
    boost::intrusive_ptr<SHAMapLeafNode>
    get_only_child_leaf() const;

protected:
    friend class PathFinder;
    friend class SHAMap;

    // CoW support - only accessible to friends
    int
    get_version() const
    {
        return version.load(std::memory_order_acquire);
    }
    void
    set_version(int v)
    {
        version.store(v, std::memory_order_release);
    }
    bool
    is_cow_enabled() const
    {
        return do_cow_;
    }
    void
    enable_cow(bool enable)
    {
        do_cow_ = enable;
    }
    boost::intrusive_ptr<SHAMapInnerNode>
    copy(int newVersion) const;
};
;

/**
 * Helper class to find paths in the tree with CoW support
 */
class PathFinder
{
private:
    const Key& targetKey;
    std::vector<boost::intrusive_ptr<SHAMapInnerNode>> inners;
    std::vector<int> branches;
    boost::intrusive_ptr<SHAMapLeafNode> foundLeaf = nullptr;
    bool leafKeyMatches = false;
    int terminalBranch = -1;

    void
    find_path(boost::intrusive_ptr<SHAMapInnerNode> root);
    bool
    maybe_copy_on_write() const;

protected:
    friend class SHAMap;
    boost::intrusive_ptr<SHAMapInnerNode> searchRoot;

public:
    PathFinder(boost::intrusive_ptr<SHAMapInnerNode>& root, const Key& key);
    bool
    has_leaf() const;
    bool
    did_leaf_key_match() const;
    bool
    ended_at_null_branch() const;
    boost::intrusive_ptr<const SHAMapLeafNode>
    get_leaf() const;
    boost::intrusive_ptr<SHAMapLeafNode>
    get_leaf_mutable();
    boost::intrusive_ptr<SHAMapInnerNode>
    get_parent_of_terminal();
    boost::intrusive_ptr<const SHAMapInnerNode>
    get_parent_of_terminal() const;
    int
    get_terminal_branch() const;
    void
    dirty_path() const;
    void
    collapse_path();

    // CoW support - used by SHAMap operations
    boost::intrusive_ptr<SHAMapInnerNode>
    dirty_or_copy_inners(int targetVersion);
    boost::intrusive_ptr<SHAMapLeafNode>
    invalidated_possibly_copied_leaf_for_updating(int targetVersion);
};

/**
 * Main SHAMap class implementing a pruned, binary prefix tree
 * with Copy-on-Write support for efficient snapshots
 */
class SHAMap
{
private:
    boost::intrusive_ptr<SHAMapInnerNode> root;
    SHAMapNodeType node_type_;

    // CoW support - all private
    std::shared_ptr<std::atomic<int>> version_counter_;
    int current_version_ = 0;
    bool cow_enabled_ = false;

    // Private methods
    void
    enable_cow(bool enable = true);
    bool
    is_cow_enabled() const
    {
        return cow_enabled_;
    }
    int
    get_version() const
    {
        return current_version_;
    }
    int
    new_version();

    // Private constructor for creating snapshots
    SHAMap(
        SHAMapNodeType type,
        boost::intrusive_ptr<SHAMapInnerNode> rootNode,
        std::shared_ptr<std::atomic<int>> vCounter,
        int version);

public:
    explicit SHAMap(SHAMapNodeType type = tnACCOUNT_STATE);
    bool
    add_item(boost::intrusive_ptr<MmapItem>& item, bool allowUpdate = true);
    bool
    remove_item(const Key& key);
    Hash256
    get_hash() const;

    // Only public CoW method - creates a snapshot
    std::shared_ptr<SHAMap>
    snapshot();
};