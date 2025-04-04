#pragma once

#include <array>
#include <atomic>
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
};

/**
 * Leaf node in the SHAMap tree
 */
class SHAMapLeafNode : public SHAMapTreeNode
{
private:
    std::shared_ptr<MmapItem> item;
    SHAMapNodeType type;
    int version = -1;  // Version for CoW tracking

public:
    SHAMapLeafNode(std::shared_ptr<MmapItem> i, SHAMapNodeType t);
    bool
    is_leaf() const override;
    bool
    is_inner() const override;
    void
    update_hash() override;
    std::shared_ptr<MmapItem>
    get_item() const;
    SHAMapNodeType
    get_type() const;

protected:
    friend class PathFinder;
    friend class SHAMap;

    // CoW support - only accessible to friends
    std::shared_ptr<SHAMapLeafNode>
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
 * Inner (branch) node in the SHAMap tree
 */
class SHAMapInnerNode : public SHAMapTreeNode
{
private:
    std::array<std::shared_ptr<SHAMapTreeNode>, 16> children_;
    uint16_t branch_mask_ = 0;
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
    set_child(int branch, std::shared_ptr<SHAMapTreeNode> const& child);
    std::shared_ptr<SHAMapTreeNode>
    get_child(int branch) const;
    bool
    has_child(int branch) const;
    int
    get_branch_count() const;
    uint16_t
    get_branch_mask() const;
    std::shared_ptr<SHAMapLeafNode>
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
    std::shared_ptr<SHAMapInnerNode>
    copy(int newVersion) const;
};

/**
 * Helper class to find paths in the tree with CoW support
 */
class PathFinder
{
private:
    const Key& targetKey;
    std::vector<std::shared_ptr<SHAMapInnerNode>> inners;
    std::vector<int> branches;
    std::shared_ptr<SHAMapLeafNode> foundLeaf = nullptr;
    bool leafKeyMatches = false;
    int terminalBranch = -1;

    void
    find_path(std::shared_ptr<SHAMapInnerNode> root);
    bool
    maybe_copy_on_write() const;

protected:
    friend class SHAMap;
    std::shared_ptr<SHAMapInnerNode> searchRoot;

public:
    PathFinder(std::shared_ptr<SHAMapInnerNode>& root, const Key& key);
    bool
    has_leaf() const;
    bool
    did_leaf_key_match() const;
    bool
    ended_at_null_branch() const;
    std::shared_ptr<const SHAMapLeafNode>
    get_leaf() const;
    std::shared_ptr<SHAMapLeafNode>
    get_leaf_mutable();
    std::shared_ptr<SHAMapInnerNode>
    get_parent_of_terminal();
    std::shared_ptr<const SHAMapInnerNode>
    get_parent_of_terminal() const;
    int
    get_terminal_branch() const;
    void
    dirty_path() const;
    void
    collapse_path();

    // CoW support - used by SHAMap operations
    std::shared_ptr<SHAMapInnerNode>
    dirty_or_copy_inners(int targetVersion);
    std::shared_ptr<SHAMapLeafNode>
    invalidated_possibly_copied_leaf_for_updating(int targetVersion);
};

/**
 * Main SHAMap class implementing a pruned, binary prefix tree
 * with Copy-on-Write support for efficient snapshots
 */
class SHAMap
{
private:
    std::shared_ptr<SHAMapInnerNode> root;
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
        std::shared_ptr<SHAMapInnerNode> rootNode,
        std::shared_ptr<std::atomic<int>> vCounter,
        int version);

public:
    explicit SHAMap(SHAMapNodeType type = tnACCOUNT_STATE);
    bool
    add_item(std::shared_ptr<MmapItem>& item, bool allowUpdate = true);
    bool
    remove_item(const Key& key);
    Hash256
    get_hash() const;

    // Only public CoW method - creates a snapshot
    std::shared_ptr<SHAMap>
    snapshot();
};