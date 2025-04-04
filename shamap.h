#pragma once

#include <memory>
#include <vector>
#include <array>
#include <stdexcept>
#include <string>

#include "core-types.h"
#include <openssl/evp.h>

//----------------------------------------------------------
// Shared Types
//----------------------------------------------------------

enum SHAMapNodeType : uint8_t {
    tnINNER = 1,
    tnTRANSACTION_NM = 2,
    tnTRANSACTION_MD = 3,
    tnACCOUNT_STATE = 4,
    // TODO: tnUPDATE ? It's context sensitive anyway
    tnREMOVE = 254,
    tnTERMINAL = 255
};


//----------------------------------------------------------
// Custom Exception Classes
//----------------------------------------------------------
class SHAMapException : public std::runtime_error {
public:
    explicit SHAMapException(const std::string &message);
};

class InvalidDepthException : public SHAMapException {
public:
    explicit InvalidDepthException(int depth, size_t maxAllowed);
    int depth() const;
    size_t maxAllowed() const;

private:
    int depth_;
    size_t maxAllowed_;
};

class InvalidBranchException : public SHAMapException {
public:
    explicit InvalidBranchException(int branch);
    int branch() const;

private:
    int branch_;
};

class NullNodeException : public SHAMapException {
public:
    explicit NullNodeException(const std::string &context);
};

class NullItemException : public SHAMapException {
public:
    explicit NullItemException();
};

class HashCalculationException : public SHAMapException {
public:
    explicit HashCalculationException(const std::string &reason);
};

/**
 * Helper function to select a branch based on a key and depth
 */
int selectBranch(const Key &key, int depth);

//----------------------------------------------------------
// SHAMap Node Classes
//----------------------------------------------------------

/**
 * Abstract base class for SHAMap tree nodes
 */
class SHAMapTreeNode {
protected:
    Hash256 hash;
    bool hashValid = false;

public:
    virtual ~SHAMapTreeNode() = default;
    void invalidateHash();
    virtual bool isLeaf() const = 0;
    virtual bool isInner() const = 0;
    virtual void updateHash() = 0; // NO LOGGING INSIDE IMPLEMENTATIONS
    const Hash256 &getHash();
};

/**
 * Inner (branch) node in the SHAMap tree
 */
class SHAMapInnerNode : public SHAMapTreeNode {
private:
    std::array<std::shared_ptr<SHAMapTreeNode>, 16> children;
    uint16_t branchMask = 0;
    uint8_t depth = 0;

public:
    explicit SHAMapInnerNode(uint8_t nodeDepth = 0);
    bool isLeaf() const override;
    bool isInner() const override;
    uint8_t getDepth() const;
    void setDepth(uint8_t newDepth);
    void updateHash() override;
    bool setChild(int branch, std::shared_ptr<SHAMapTreeNode> const &child);
    std::shared_ptr<SHAMapTreeNode> getChild(int branch) const;
    bool hasChild(int branch) const;
    int getBranchCount() const;
    uint16_t getBranchMask() const;
    std::shared_ptr<class SHAMapLeafNode> getOnlyChildLeaf() const;
};

/**
 * Leaf node in the SHAMap tree
 */
class SHAMapLeafNode : public SHAMapTreeNode {
private:
    std::shared_ptr<MmapItem> item;
    SHAMapNodeType type;

public:
    SHAMapLeafNode(std::shared_ptr<MmapItem> i, SHAMapNodeType t);
    bool isLeaf() const override;
    bool isInner() const override;
    void updateHash() override;
    std::shared_ptr<MmapItem> getItem() const;
    SHAMapNodeType getType() const;
};

/**
 * Helper class to find paths in the tree
 */
class PathFinder {
private:
    const Key &targetKey;
    std::shared_ptr<SHAMapInnerNode> searchRoot;
    std::vector<std::shared_ptr<SHAMapInnerNode>> inners;
    std::vector<int> branches;
    std::shared_ptr<SHAMapLeafNode> foundLeaf = nullptr;
    bool leafKeyMatches = false;
    int terminalBranch = -1;

    void findPath(std::shared_ptr<SHAMapInnerNode> root);

public:
    PathFinder(std::shared_ptr<SHAMapInnerNode> &root, const Key &key);
    bool hasLeaf() const;
    bool didLeafKeyMatch() const;
    bool endedAtNullBranch() const;
    std::shared_ptr<const SHAMapLeafNode> getLeaf() const;
    std::shared_ptr<SHAMapLeafNode> getLeafMutable();
    std::shared_ptr<SHAMapInnerNode> getParentOfTerminal();
    std::shared_ptr<const SHAMapInnerNode> getParentOfTerminal() const;
    int getTerminalBranch() const;
    void dirtyPath() const;
    void collapsePath();
};

/**
 * Main SHAMap class implementing a pruned, binary prefix tree
 */
class SHAMap {
private:
    std::shared_ptr<SHAMapInnerNode> root;
    SHAMapNodeType nodeType;

public:
    explicit SHAMap(SHAMapNodeType type = tnACCOUNT_STATE);
    Hash256 getChildHash(int ix);
    bool addItem(std::shared_ptr<MmapItem> &item, bool allowUpdate = true);
    bool removeItem(const Key &key);
    Hash256 getHash() const;
};