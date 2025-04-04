#include <iostream>
#include <fstream>
#include <vector>
#include <array>
#include <memory>
#include <string>
#include <stdexcept>
#include <map>
#include <chrono>

// For memory mapping
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/filesystem.hpp>

// For crypto
#include <openssl/evp.h>

// Constants
static constexpr uint32_t CATL = 0x4C544143UL; // "CATL" in LE
static constexpr uint16_t CATALOGUE_VERSION_MASK = 0x00FF;
static constexpr uint16_t CATALOGUE_COMPRESS_LEVEL_MASK = 0x0F00;

constexpr std::uint32_t
make_hash_prefix(char a, char b, char c) {
    return (static_cast<std::uint32_t>(a) << 24) +
           (static_cast<std::uint32_t>(b) << 16) +
           (static_cast<std::uint32_t>(c) << 8);
}


// Hash prefixes from rippled
namespace HashPrefix {
    constexpr std::array<unsigned char, 4> txNode = {'S', 'N', 'D', 0x00};
    constexpr std::array<unsigned char, 4> leafNode = {'M', 'L', 'N', 0x00};
    constexpr std::array<unsigned char, 4> innerNode = {'M', 'I', 'N', 0x00};
};

// SHAMap node types
enum SHAMapNodeType : uint8_t {
    tnINNER = 1,
    tnTRANSACTION_NM = 2,
    tnTRANSACTION_MD = 3,
    tnACCOUNT_STATE = 4,
    tnREMOVE = 254,
    tnTERMINAL = 255
};

// Header structures
#pragma pack(push, 1)
struct CATLHeader {
    uint32_t magic;
    uint32_t min_ledger;
    uint32_t max_ledger;
    uint16_t version;
    uint16_t network_id;
    uint64_t filesize;
    std::array<uint8_t, 64> hash;
};

struct LedgerInfo {
    uint32_t sequence;
    uint8_t hash[32];
    uint8_t txHash[32];
    uint8_t accountHash[32];
    uint8_t parentHash[32];
    uint64_t drops;
    uint32_t closeFlags;
    uint32_t closeTimeResolution;
    uint64_t closeTime;
    uint64_t parentCloseTime;
};
#pragma pack(pop)

//----------------------------------------------------------
// Custom Exception Classes
//----------------------------------------------------------

/**
 * @brief Base exception class for SHAMap errors
 */
class SHAMapException : public std::runtime_error {
public:
    explicit SHAMapException(const std::string &message)
        : std::runtime_error(message) {
    }
};

/**
 * @brief Exception for invalid depth when calculating branches
 */
class InvalidDepthException : public SHAMapException {
public:
    explicit InvalidDepthException(int depth, size_t maxAllowed)
        : SHAMapException("Invalid depth (" + std::to_string(depth) +
                          ") for key in selectBranch. Max allowed: " +
                          std::to_string(maxAllowed)),
          depth_(depth), maxAllowed_(maxAllowed) {
    }

    int depth() const { return depth_; }
    size_t maxAllowed() const { return maxAllowed_; }

private:
    int depth_;
    size_t maxAllowed_;
};

/**
 * @brief Exception for invalid branch indices
 */
class InvalidBranchException : public SHAMapException {
public:
    explicit InvalidBranchException(int branch)
        : SHAMapException("Invalid branch index: " + std::to_string(branch)),
          branch_(branch) {
    }

    int branch() const { return branch_; }

private:
    int branch_;
};

/**
 * @brief Exception for null node encountered where a node was expected
 */
class NullNodeException : public SHAMapException {
public:
    explicit NullNodeException(const std::string &context)
        : SHAMapException("Null node encountered: " + context) {
    }
};

/**
 * @brief Exception for leaf node with null item
 */
class NullItemException : public SHAMapException {
public:
    explicit NullItemException()
        : SHAMapException("Found leaf node with null item") {
    }
};

/**
 * @brief Exception for hash calculation errors
 */
class HashCalculationException : public SHAMapException {
public:
    explicit HashCalculationException(const std::string &reason)
        : SHAMapException("Hash calculation error: " + reason) {
    }
};

// Slice class to reference data without copying
class Slice {
private:
    const uint8_t *data_;
    size_t size_;

public:
    Slice() : data_(nullptr), size_(0) {
    }

    Slice(const uint8_t *data, size_t size) : data_(data), size_(size) {
    }

    const uint8_t *data() const { return data_; }
    size_t size() const { return size_; }

    bool empty() const { return size_ == 0; }
};

void slice_hex(const Slice sl, std::string &result) {
    static constexpr char hexChars[] = "0123456789abcdef";
    // Reserve space for the hexadecimal representation (2 chars per byte)
    result.reserve(sl.size() * 2);

    // Get a pointer to the raw data from the slice
    const uint8_t *bytes = sl.data();

    // Convert each byte to two hexadecimal characters
    for (size_t i = 0; i < sl.size(); ++i) {
        uint8_t byte = bytes[i];
        result.push_back(hexChars[(byte >> 4) & 0xF]);
        result.push_back(hexChars[byte & 0xF]);
    }
}

// 256-bit hash type
class Hash256 {
private:
    std::array<uint8_t, 32> data_;

public:
    Hash256() : data_() {
        data_.fill(0);
    }

    explicit Hash256(const std::array<uint8_t, 32> &data) : data_(data) {
    }

    explicit Hash256(const uint8_t *data) : data_() {
        std::memcpy(data_.data(), data, 32);
    }

    uint8_t *data() {
        return data_.data();
    }

    const uint8_t *data() const {
        return data_.data();
    }

    static constexpr std::size_t size() {
        return 32;
    }

    static Hash256 zero() {
        return {};
    }

    bool operator==(const Hash256 &other) const {
        return data_ == other.data_;
    }

    bool operator!=(const Hash256 &other) const {
        return !(*this == other);
    }

    [[nodiscard]] std::string hex() const {
        std::string result;
        slice_hex({data(), size()}, result);
        return result;
    }
};


// Improved Key class with equality operator
class Key {
private:
    const std::uint8_t *data_;

public:
    Key(const std::uint8_t *data) : data_(data) {
    }

    const std::uint8_t *data() const {
        return data_;
    }

    static constexpr std::size_t size() {
        return 32;
    }

    // Create a Hash256 from this key
    Hash256 toHash() const {
        return Hash256(data_);
    }

    // Convert to hex string
    std::string toString() const {
        return toHash().hex();
    }

    bool operator==(const Key &other) const {
        return std::memcmp(data_, other.data_, 32) == 0;
    }

    bool operator!=(const Key &other) const {
        return !(*this == other);
    }
};


// Memory-mapped item that references slices of the memory-mapped file
class MmapItem {
private:
    Key key_; // Key object, must be 32 bytes
    Slice data_; // Data slice, variable size

public:
    MmapItem(
        const std::uint8_t *keyData,
        const std::uint8_t *data,
        std::size_t dataSize)
        : key_(keyData), data_(data, dataSize) {
    }

    const Key &key() const { return key_; }

    Slice keySlice() const { return {key_.data(), Key::size()}; }

    const Slice &slice() const { return data_; }

    std::string hex() const {
        const auto sl = slice();
        std::string result;
        slice_hex(sl, result);
        return result;
    }
};

// Forward declarations for SHAMap components
class SHAMapTreeNode;
class SHAMapInnerNode;
class SHAMapLeafNode;
class SHAMap;

/**
 * @brief Selects the branch index (0-15) for a key at a given depth.
 * @param key The key to use.
 * @param depth The current depth in the tree (0 for root's children).
 * @return Branch index (0-15).
 * @throws InvalidDepthException if depth is invalid.
 */
int selectBranch(const Key &key, int depth) {
    int byteIdx = depth / 2;
    // Use Key::size() for bounds check
    if (byteIdx < 0 || byteIdx >= static_cast<int>(Key::size())) {
        throw InvalidDepthException(depth, Key::size());
    }
    int nibbleIdx = depth % 2;
    // Assuming key.data() returns const uint8_t*
    uint8_t byte_val = key.data()[byteIdx];
    return (nibbleIdx == 0) ? (byte_val >> 4) & 0xF : byte_val & 0xF;
}

// Base class for SHAMap nodes
class SHAMapTreeNode {
protected:
    Hash256 hash;
    bool hashValid = false;

public:
    void invalidateHash() {
        hashValid = false;
    }

    virtual ~SHAMapTreeNode() = default;

    virtual bool isLeaf() const = 0;

    virtual bool isInner() const = 0;

    virtual void updateHash() = 0;

    const Hash256 &getHash() {
        if (!hashValid) {
            updateHash();
            hashValid = true;
        }
        return hash;
    }
};

// Inner node implementation
class SHAMapInnerNode : public SHAMapTreeNode {
private:
    std::array<std::shared_ptr<SHAMapTreeNode>, 16> children;
    uint16_t branchMask = 0;
    uint8_t depth = 0; // Depth in the tree, useful for debugging and operations

public:
    SHAMapInnerNode(uint8_t nodeDepth = 0) : depth(nodeDepth) {
    }

    bool isLeaf() const override { return false; }
    bool isInner() const override { return true; }

    // Get/set depth
    uint8_t getDepth() const { return depth; }
    void setDepth(uint8_t newDepth) { depth = newDepth; }

    void updateHash() override {
        if (branchMask == 0) {
            hash = Hash256::zero();
            hashValid = true;
            return;
        }

        EVP_MD_CTX *ctx = EVP_MD_CTX_new();
        if (!ctx) throw HashCalculationException("Failed to create EVP_MD_CTX");

        if (EVP_DigestInit_ex(ctx, EVP_sha512(), nullptr) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to initialize SHA-512 digest");
        }

        auto prefix = HashPrefix::innerNode;
        if (EVP_DigestUpdate(ctx, &prefix, sizeof(HashPrefix::innerNode)) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to update digest with prefix");
        }

        Hash256 zeroHash = Hash256::zero();
        for (int i = 0; i < 16; i++) {
            const uint8_t *hashData = zeroHash.data();
            if (children[i]) {
                hashData = children[i]->getHash().data();
            }

            if (EVP_DigestUpdate(ctx, hashData, Hash256::size()) != 1) {
                EVP_MD_CTX_free(ctx);
                throw HashCalculationException(
                    "Failed to update digest with child data (branch " + std::to_string(i) + ")");
            }
        }

        std::array<uint8_t, 64> fullHash;
        unsigned int hashLen = 0;
        if (EVP_DigestFinal_ex(ctx, fullHash.data(), &hashLen) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to finalize digest");
        }

        EVP_MD_CTX_free(ctx);
        hash = Hash256(reinterpret_cast<const uint8_t *>(fullHash.data()));
        hashValid = true;
    }

    bool setChild(int branch, std::shared_ptr<SHAMapTreeNode> const &child) {
        if (branch < 0 || branch >= 16) {
            throw InvalidBranchException(branch);
        }

        if (child) {
            children[branch] = child;
            branchMask |= (1 << branch);

            // Update depth of child if it's an inner node
            if (child->isInner()) {
                auto innerChild = std::static_pointer_cast<SHAMapInnerNode>(child);
                innerChild->setDepth(depth + 1);
            }
        } else {
            children[branch] = nullptr;
            branchMask &= ~(1 << branch);
        }

        hashValid = false;
        return true;
    }

    std::shared_ptr<SHAMapTreeNode> getChild(int branch) const {
        if (branch < 0 || branch >= 16) {
            throw InvalidBranchException(branch);
        }
        return children[branch];
    }

    bool hasChild(int branch) const {
        if (branch < 0 || branch >= 16) {
            throw InvalidBranchException(branch);
        }
        return (branchMask & (1 << branch)) != 0;
    }

    int getBranchCount() const {
        int count = 0;
        for (int i = 0; i < 16; i++) {
            if (hasChild(i)) count++;
        }
        return count;
    }

    uint16_t getBranchMask() const {
        return branchMask;
    }

    /**
    * @brief Checks if this inner node contains exactly one child, which must be a leaf node.
    *
    * This method iterates through the children. If it finds an inner node, or
    * more than one leaf node, it immediately returns nullptr. If it finds
    * exactly one leaf node and no inner nodes, it returns a shared_ptr to that leaf.
    *
    * @return A shared_ptr to the single leaf node child if conditions are met,
    * otherwise returns nullptr.
    */
    std::shared_ptr<SHAMapLeafNode> getOnlyChildLeaf() const {
        // const because it doesn't modify the node
        std::shared_ptr<SHAMapLeafNode> resultLeaf = nullptr;
        int leafCount = 0;

        // Iterate through all 16 potential child branches
        for (const std::shared_ptr<SHAMapTreeNode> &childNodePtr: children) {
            // Check if a node exists at this branch
            if (childNodePtr) {
                // Check if the existing node is an inner node
                if (childNodePtr->isInner()) {
                    // If we find *any* inner node, this node cannot have an "only child leaf".
                    resultLeaf = nullptr; // Reset any potential leaf we found earlier
                    break; // Stop searching immediately
                } else {
                    // It's not null and not inner, so it must be a leaf.
                    leafCount++; // Increment the count of leaves found

                    if (leafCount == 1) {
                        // This is the first leaf encountered. Store it as the candidate result.
                        // Cast the base pointer to the specific leaf type.
                        resultLeaf = std::static_pointer_cast<SHAMapLeafNode>(childNodePtr);
                    } else {
                        // This is the second (or subsequent) leaf found.
                        // Therefore, it's not an "only child leaf" situation.
                        resultLeaf = nullptr; // Reset the result
                        break; // Stop searching immediately
                    }
                }
            }
        } // End for loop

        // After checking all branches (or breaking early):
        // - resultLeaf will be nullptr if we found any inner nodes or more than one leaf.
        // - resultLeaf will hold the pointer if exactly one leaf and zero inner nodes were found.
        return resultLeaf;
    }
};

// Leaf node implementation that uses MmapItem
class SHAMapLeafNode : public SHAMapTreeNode {
private:
    std::shared_ptr<MmapItem> item;
    SHAMapNodeType type;

public:
    SHAMapLeafNode(std::shared_ptr<MmapItem> i, SHAMapNodeType t)
        : item(std::move(i)), type(t) {
        if (!item) {
            throw NullItemException();
        }
    }

    bool isLeaf() const override { return true; }
    bool isInner() const override { return false; }

    void updateHash() override {
        // Select appropriate hash prefix based on node type
        std::array<unsigned char, 4> prefix = {0, 0, 0, 0};
        auto set = [&prefix](auto &from) {
            std::memcpy(prefix.data(), from.data(), 4);
        };
        switch (type) {
            case tnTRANSACTION_NM:
            case tnTRANSACTION_MD:
                set(HashPrefix::txNode);
                break;
            case tnACCOUNT_STATE:
                set(HashPrefix::leafNode);
                break;
            default:
                set(HashPrefix::leafNode);
        }

        // Create and initialize digest context
        EVP_MD_CTX *ctx = EVP_MD_CTX_new();
        if (ctx == nullptr) {
            throw HashCalculationException("Failed to create EVP_MD_CTX");
        }

        // Initialize with SHA-512
        if (EVP_DigestInit_ex(ctx, EVP_sha512(), nullptr) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to initialize SHA-512 digest");
        }

        // Add prefix
        if (EVP_DigestUpdate(ctx, &prefix, sizeof(prefix)) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to update digest with prefix");
        }

        // Add item data
        if (EVP_DigestUpdate(ctx, item->slice().data(), item->slice().size()) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to update digest with item data");
        }

        // Add item key
        if (EVP_DigestUpdate(ctx, item->key().data(), Key::size()) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to update digest with item key");
        }

        // Finalize hash
        std::array<unsigned char, 64> fullHash;
        unsigned int hashLen = 0;
        if (EVP_DigestFinal_ex(ctx, fullHash.data(), &hashLen) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to finalize digest");
        }

        // Free the context
        EVP_MD_CTX_free(ctx);

        // Create our hash from the first half of SHA-512
        hash = Hash256(reinterpret_cast<const uint8_t *>(fullHash.data()));
        hashValid = true;
    }

    std::shared_ptr<MmapItem> getItem() const {
        return item;
    }

    SHAMapNodeType getType() const {
        return type;
    }
};

class PathFinder {
private:
    // The key we're searching for
    const Key &targetKey;

    // Root of the tree (needed for invalidation)
    std::shared_ptr<SHAMapInnerNode> searchRoot;

    // Vector of inner nodes along the path, from root to leaf
    std::vector<std::shared_ptr<SHAMapInnerNode> > inners;

    // Vector of branches taken at each inner node
    std::vector<int> branches;

    // Leaf found at the end of the path (if any)
    std::shared_ptr<SHAMapLeafNode> foundLeaf = nullptr;

    // Whether the leaf's key matches our search index
    bool leafKeyMatches = false;

    // Cache of the terminal branch
    int terminalBranch = -1;

    /**
     * @brief Builds the path from root to the target key
     * @param root The root node to start from
     * @throws NullNodeException if the root is null
     */
    void findPath(std::shared_ptr<SHAMapInnerNode> root) {
        if (!root) {
            throw NullNodeException("PathFinder: null root node");
        }

        // Reset state
        searchRoot = root;
        inners.clear();
        branches.clear();
        foundLeaf = nullptr;
        leafKeyMatches = false;
        terminalBranch = -1;

        std::shared_ptr<SHAMapInnerNode> currentInner = root;

        while (true) {
            // Calculate branch for our key at this depth
            int branch = selectBranch(targetKey, currentInner->getDepth());

            // Get the child at this branch
            std::shared_ptr<SHAMapTreeNode> child = currentInner->getChild(branch);

            // If no child, path ends at null branch
            if (!child) {
                // Record the terminal branch
                terminalBranch = branch;
                // Add the innermost node
                inners.push_back(currentInner);
                // No leaf found
                break;
            }

            // If child is leaf, path ends at leaf
            if (child->isLeaf()) {
                // Record the terminal branch
                terminalBranch = branch;
                // Add the innermost node
                inners.push_back(currentInner);
                // Record the leaf
                foundLeaf = std::static_pointer_cast<SHAMapLeafNode>(child);
                // Check if leaf matches our search key
                if (foundLeaf->getItem()) {
                    leafKeyMatches = (foundLeaf->getItem()->key() == targetKey);
                } else {
                    throw NullItemException();
                }
                break;
            }

            // Add current inner node and branch to our path
            inners.push_back(currentInner);
            branches.push_back(branch);

            // Continue descent
            currentInner = std::static_pointer_cast<SHAMapInnerNode>(child);
        }
    }

public:
    /**
     * @brief Constructs a PathFinder by traversing the tree from root to targetKey
     * @param root Root node of the tree
     * @param key The key to find
     */
    PathFinder(std::shared_ptr<SHAMapInnerNode> &root, const Key &key)
        : targetKey(key) {
        findPath(root);
    }

    // ---- Status Methods (Original Interface) ----

    /** @return True if a leaf node was found at the end of the path. */
    bool hasLeaf() const {
        return foundLeaf != nullptr;
    }

    /** @return True if a leaf node was found AND its key matched the target search key. */
    bool didLeafKeyMatch() const {
        return leafKeyMatches;
    }

    /** @return True if the path ended because a null branch was encountered. */
    bool endedAtNullBranch() const {
        // Path ended if we recorded a terminal branch but didn't find a leaf
        return foundLeaf == nullptr && terminalBranch != -1;
    }

    // ---- Node Access Methods (Original Interface) ----

    /** @return Const shared_ptr to the found leaf node, or nullptr. */
    std::shared_ptr<const SHAMapLeafNode> getLeaf() const {
        return foundLeaf;
    }

    /**
     * @brief Gets a potentially modifiable shared_ptr to the found leaf node.
     * @warning Modifying the node or its item typically requires calling dirtyPath()
     * afterwards (and potentially invalidating the leaf itself) to ensure
     * correct future hash calculations.
     * @return Shared_ptr to the leaf node, or nullptr if no leaf was found.
     */
    std::shared_ptr<SHAMapLeafNode> getLeafMutable() {
        return foundLeaf;
    }

    /**
     * @brief Gets a potentially modifiable shared_ptr to the parent node of the
     * found leaf or null branch. This is the deepest inner node reached.
     * @warning Modifying this node or its children (e.g., via setChild) requires
     * calling dirtyPath() afterwards to ensure correct future hash calculations.
     * @return Shared pointer to the parent node, or nullptr if the path was empty (e.g., null root).
     */
    std::shared_ptr<SHAMapInnerNode> getParentOfTerminal() {
        return inners.empty() ? nullptr : inners.back();
    }

    /** @return Const shared_ptr to the parent node of the found leaf or null branch. */
    std::shared_ptr<const SHAMapInnerNode> getParentOfTerminal() const {
        return inners.empty() ? nullptr : inners.back();
    }

    /**
     * @brief Gets the branch index (0-15) taken from the parent node
     * that leads/led to the found leaf or the null branch where the path ended.
     * @return Branch index (0-15), or -1 if path finding failed or root was null.
     */
    int getTerminalBranch() const {
        return terminalBranch;
    }

    /**
     * @brief Invalidates the hash of all inner nodes along the found path,
     * including the root and the direct parent of the terminal node/leaf.
     * Call this *before* or *after* making modifications to ensure subsequent
     * getHash() calls will recalculate correctly. Marks the path as "dirty".
     * Does NOT invalidate the leaf node itself; handle that separately if needed.
     */
    void dirtyPath() {
        // Invalidate all inner nodes along the path
        for (auto &inner: inners) {
            inner->invalidateHash();
        }
    }

    /**
     * @brief Performs a collapse operation, promoting a leaf node up past ancestors
     * if those ancestors only contained the path to that single leaf after a modification.
     *
     * Assumes the modification (e.g., leaf removal via parent->setChild(branch, nullptr))
     * has already occurred and dirtyPath() has been called.
     */
    void collapsePath() {
        if (inners.size() <= 1) return;

        // Start with no leaf to promote
        std::shared_ptr<SHAMapLeafNode> onlyChild = nullptr;

        // First check innermost node (parent of terminal)
        auto innermost = inners.back();
        onlyChild = innermost->getOnlyChildLeaf();

        // Process remaining nodes from innermost to outermost
        for (int i = static_cast<int>(inners.size()) - 2; i >= 0; --i) {
            auto inner = inners[i];
            int branch = branches[i];

            // If we have a leaf to promote from a deeper node
            if (onlyChild) {
                // Replace child with the promoted leaf
                inner->setChild(branch, onlyChild);
            }

            // Check if this node itself has only one leaf child
            onlyChild = inner->getOnlyChildLeaf();

            // If no leaf to promote, we're done
            if (!onlyChild) break;
        }
    }
};

// Main SHAMap class
class SHAMap {
private:
    std::shared_ptr<SHAMapInnerNode> root;
    SHAMapNodeType nodeType;

public:
    SHAMap(SHAMapNodeType type = tnACCOUNT_STATE)
        : nodeType(type) {
        root = std::make_shared<SHAMapInnerNode>(0); // Root has depth 0
    }

    Hash256 getChildHash(int ix) {
        try {
            auto child = root->getChild(ix);
            if (child) {
                return child->getHash();
            } else {
                return Hash256::zero();
            }
        } catch (const InvalidBranchException &e) {
            // Handle this specific error case
            return Hash256::zero();
        }
    }

    bool addItem(std::shared_ptr<MmapItem> item, bool allowUpdate = true) {
        try {
            // Use PathFinder to find the path to where this item should be
            PathFinder pathFinder(root, item->key());

            // Case 1: Path ends at null branch - add the item as a new leaf
            // Case 2: Path ends at leaf with matching key - update if allowed
            if (pathFinder.endedAtNullBranch() || (
                    pathFinder.hasLeaf() && pathFinder.didLeafKeyMatch() && allowUpdate)) {
                auto parent = pathFinder.getParentOfTerminal();
                int branch = pathFinder.getTerminalBranch();

                // Create and add the new leaf node
                auto newLeaf = std::make_shared<SHAMapLeafNode>(item, nodeType);
                parent->setChild(branch, newLeaf);

                // Invalidate all hashes along the path
                pathFinder.dirtyPath();
                return true;
            }

            // Case 3: Path ends at leaf with different key - create collision nodes
            if (pathFinder.hasLeaf() && !pathFinder.didLeafKeyMatch()) {
                auto parent = pathFinder.getParentOfTerminal();
                int branch = pathFinder.getTerminalBranch();
                auto existingLeaf = pathFinder.getLeafMutable();
                auto existingItem = existingLeaf->getItem();

                // Iteratively build a path until branches diverge
                std::shared_ptr<SHAMapInnerNode> currentParent = parent;
                std::shared_ptr<SHAMapTreeNode> childToReplace = existingLeaf;
                int currentBranch = branch;

                // Create the first inner node and link it
                uint8_t currentDepth = parent->getDepth();
                auto newInner = std::make_shared<SHAMapInnerNode>(currentDepth + 1);
                currentParent->setChild(currentBranch, newInner);
                currentParent = newInner;
                currentDepth++;

                // Iteratively create deeper nodes until branches diverge
                while (true) {
                    int existingBranch = selectBranch(existingItem->key(), currentDepth);
                    int newBranch = selectBranch(item->key(), currentDepth);

                    if (existingBranch != newBranch) {
                        // Keys finally diverge - place both leaves
                        currentParent->setChild(existingBranch, existingLeaf);
                        auto newLeaf = std::make_shared<SHAMapLeafNode>(item, nodeType);
                        currentParent->setChild(newBranch, newLeaf);
                        break;
                    } else {
                        // Keys still collide - add another level
                        auto nextInner = std::make_shared<SHAMapInnerNode>(currentDepth + 1);
                        currentParent->setChild(existingBranch, nextInner);
                        currentParent = nextInner;
                        currentDepth++;
                    }
                }

                // Invalidate all hashes along the path
                pathFinder.dirtyPath();
                return true;
            }


            throw SHAMapException("Unexpected state in addItem - PathFinder logic error");
        } catch (const SHAMapException &e) {
            // Log the error
            std::cout << "Error adding item: " << e.what() << std::endl;
            return false;
        }
    }

    // UPDATED: Use PathFinder for item removal
    bool removeItem(const Key &key) {
        try {
            // Use PathFinder to find the path to the item to remove
            PathFinder pathFinder(root, key);

            // Check if we found a matching leaf
            if (!pathFinder.hasLeaf() || !pathFinder.didLeafKeyMatch()) {
                // Item not found - nothing to remove
                return false;
            }

            // We found the item to remove
            auto parent = pathFinder.getParentOfTerminal();
            int branch = pathFinder.getTerminalBranch();

            // Remove the leaf by setting the branch to null
            parent->setChild(branch, nullptr);

            // Invalidate all hashes along the path
            pathFinder.dirtyPath();

            // Perform tree compression/collapsing
            // This will promote single-child inner nodes where appropriate
            pathFinder.collapsePath();

            return true;
        } catch (const SHAMapException &e) {
            // Log the error
            std::cout << "Error removing item: " << e.what() << std::endl;
            return false;
        }
    }

    // Get the current hash of the entire map
    Hash256 getHash() const {
        if (!root) {
            return Hash256::zero();
        }
        return root->getHash();
    }

    // Set the node type for this map
    void setNodeType(SHAMapNodeType type) {
        nodeType = type;
    }
};

// Convert NetClock time to human-readable string
std::string format_ripple_time(uint64_t netClockTime) {
    // NetClock uses seconds since January 1st, 2000 (946684800)
    static const time_t rippleEpochOffset = 946684800;

    time_t unixTime = netClockTime + rippleEpochOffset;
    std::tm *tm = std::gmtime(&unixTime);
    if (!tm) return "Invalid time";

    char timeStr[30];
    std::strftime(timeStr, sizeof(timeStr), "%Y-%m-%d %H:%M:%S UTC", tm);
    return timeStr;
}

class CATLHasher {
private:
    boost::iostreams::mapped_file_source mmapFile;
    const uint8_t *data = nullptr;
    size_t fileSize = 0;
    CATLHeader header;
    bool verbose = true;

    // Maps for tracking state
    SHAMap stateMap;
    SHAMap txMap;

    // Statistics
    struct Stats {
        uint32_t ledgersProcessed = 0;
        uint32_t stateNodesTotal = 0;
        uint32_t txNodesTotal = 0;
        uint32_t stateRemovalsApplied = 0;
        uint32_t successfulHashVerifications = 0;
        uint32_t failedHashVerifications = 0;
    } stats;

    bool validateHeader() {
        if (fileSize < sizeof(CATLHeader)) {
            std::cout << "File too small to contain a valid header" << std::endl;
            return false;
        }

        std::memcpy(&header, data, sizeof(CATLHeader));

        // Basic validation
        if (header.magic != CATL) {
            std::cout << "Invalid magic value: expected 0x" << std::hex << CATL
                    << ", got 0x" << header.magic << std::dec << std::endl;
            return false;
        }

        // Check compression level
        uint8_t compressionLevel = (header.version & CATALOGUE_COMPRESS_LEVEL_MASK) >> 8;
        if (compressionLevel != 0) {
            std::cout << "Compressed files not supported. Level: " << static_cast<int>(compressionLevel) << std::endl;
            return false;
        }

        if (verbose) {
            std::cout << "CATL Header:\n"
                    << "  Magic: 0x" << std::hex << header.magic << std::dec << "\n"
                    << "  Ledger range: " << header.min_ledger << " - " << header.max_ledger << "\n"
                    << "  Network ID: " << header.network_id << "\n"
                    << "  File size: " << header.filesize << " bytes\n";
        }

        return true;
    }

    // Unified map processing function that handles both state and transaction maps
    size_t processMap(size_t offset, SHAMap &map, uint32_t &nodeCount, bool isStateMap = false) {
        nodeCount = 0;
        bool foundTerminal = false;

        while (offset < fileSize && !foundTerminal) {
            // Read node type - check bounds first
            if (offset >= fileSize) {
                std::cout << "Unexpected EOF reading node type in "
                        << (isStateMap ? "state" : "transaction") << " map" << std::endl;
                return offset;
            }
            uint8_t nodeType = data[offset++];

            if (nodeType == tnTERMINAL) {
                foundTerminal = true;
                break;
            }

            // Check for valid node type
            if (nodeType != tnINNER && nodeType != tnTRANSACTION_NM &&
                nodeType != tnTRANSACTION_MD && nodeType != tnACCOUNT_STATE &&
                nodeType != tnREMOVE) {
                std::cout << "Invalid node type: " << static_cast<int>(nodeType)
                        << " at offset " << (offset - 1) << std::endl;
                return offset - 1;
            }

            // Read key (32 bytes) - check bounds first
            if (offset + 32 > fileSize) {
                std::cout << "Unexpected EOF reading key in "
                        << (isStateMap ? "state" : "transaction") << " map" << std::endl;
                return offset;
            }
            const uint8_t *key = data + offset;
            offset += 32;

            // Handle removal operation (only valid for state maps)
            if (nodeType == tnREMOVE) {
                if (isStateMap) {
                    // Remove item from state map
                    Key itemKey(key);
                    if (verbose) {
                        std::cout << "Removing state item: " << itemKey.toString() << std::endl;
                    }
                    if (map.removeItem(itemKey)) {
                        stats.stateRemovalsApplied++;
                    }
                } else {
                    // Removals shouldn't be in transaction maps, just skip the key
                    if (verbose) {
                        std::cout << "WARNING: Found tnREMOVE in transaction map" << std::endl;
                    }
                }
                nodeCount++;
                continue;
            }

            // Read data size - check bounds first
            if (offset + sizeof(uint32_t) > fileSize) {
                std::cout << "Unexpected EOF reading data size in "
                        << (isStateMap ? "state" : "transaction") << " map" << std::endl;
                return offset;
            }
            uint32_t dataSize = 0;
            std::memcpy(&dataSize, data + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);

            // Validate data size (check bounds and sanity check)
            if (dataSize > 100 * 1024 * 1024 || offset + dataSize > fileSize) {
                std::cout << "Invalid data size: " << dataSize << " bytes at offset " << offset << std::endl;
                return offset;
            }

            // Create MmapItem pointing directly to the mapped memory
            const uint8_t *itemData = data + offset;
            offset += dataSize;

            auto item = std::make_shared<MmapItem>(key, itemData, dataSize);

            if (isStateMap && verbose && nodeCount < 5) {
                // Debug output for the first few state items
                std::cout << "Adding account state item with key="
                        << item->key().toString()
                        << " (data size: " << dataSize << " bytes)" << std::endl;
            }

            map.addItem(item);
            nodeCount++;
        }

        if (!foundTerminal && verbose) {
            std::cout << "WARNING: No terminal marker found for "
                    << (isStateMap ? "state" : "transaction") << " map" << std::endl;
        }

        return offset;
    }

    // Process a single ledger
    size_t processLedger(size_t offset) {
        // Check if we have enough data for the ledger info
        if (offset + sizeof(LedgerInfo) > fileSize) {
            std::cout << "Not enough data for ledger info at offset " << offset << std::endl;
            return offset;
        }

        // Read ledger info
        LedgerInfo info;
        std::memcpy(&info, data + offset, sizeof(LedgerInfo));
        offset += sizeof(LedgerInfo);

        // Basic sanity check on sequence number
        if (info.sequence < header.min_ledger || info.sequence > header.max_ledger) {
            std::cout << "WARNING: Ledger sequence " << info.sequence
                    << " outside expected range (" << header.min_ledger
                    << "-" << header.max_ledger << ")" << std::endl;
            // Continue anyway, but this could indicate file corruption
        }

        if (verbose) {
            std::cout << "\nProcessing ledger " << info.sequence << std::endl;
            std::cout << "  Hash: " << Hash256(info.hash).hex() << std::endl;
            std::cout << "  AccountHash: " << Hash256(info.accountHash).hex() << std::endl;
            std::cout << "  TxHash: " << Hash256(info.txHash).hex() << std::endl;
            std::cout << "  Close time: " << format_ripple_time(info.closeTime) << std::endl;
        }

        // Process account state map or delta
        bool isFirstLedger = (info.sequence == header.min_ledger);

        // Initialize state map for first ledger, otherwise use existing state
        if (isFirstLedger) {
            stateMap = SHAMap(tnACCOUNT_STATE);
        }

        // Process state map
        uint32_t stateNodes = 0;
        size_t newOffset = processMap(offset, stateMap, stateNodes, true); // true = isStateMap
        if (newOffset == offset) {
            std::cout << "Error processing state map for ledger " << info.sequence << std::endl;
            return offset;
        }
        offset = newOffset;
        stats.stateNodesTotal += stateNodes;

        // Process transaction map (always create fresh)
        uint32_t txNodes = 0;
        txMap = SHAMap(tnTRANSACTION_MD);
        newOffset = processMap(offset, txMap, txNodes, false); // false = not isStateMap
        if (newOffset == offset) {
            std::cout << "Error processing transaction map for ledger " << info.sequence << std::endl;
            return offset;
        }
        offset = newOffset;
        stats.txNodesTotal += txNodes;

        // Verify hashes
        verifyMapHash(stateMap, Hash256(info.accountHash), "state", info.sequence);
        verifyMapHash(txMap, Hash256(info.txHash), "transaction", info.sequence);

        stats.ledgersProcessed++;
        return offset;
    }

    // Helper function to verify map hashes
    void verifyMapHash(const SHAMap &map, const Hash256 &expectedHash,
                       const std::string &mapType, uint32_t ledgerSeq) {
        Hash256 computedHash = map.getHash();
        bool hashMatch = (computedHash == expectedHash);

        if (!hashMatch) {
            std::cout << "WARNING: Computed " << mapType << " hash doesn't match stored hash for ledger "
                    << ledgerSeq << std::endl;
            if (verbose) {
                std::cout << "  Computed hash: " << computedHash.hex() << std::endl;
                std::cout << "  Expected hash: " << expectedHash.hex() << std::endl;
            }
            stats.failedHashVerifications++;
        } else {
            if (verbose) {
                std::cout << "  " << mapType << " hash verified for ledger " << ledgerSeq << std::endl;
            }
            stats.successfulHashVerifications++;
        }
    }

public:
    CATLHasher(const std::string &filename, bool verboseOutput = false)
        : header(), verbose(verboseOutput) {
        try {
            // Try to open the file
            if (!boost::filesystem::exists(filename)) {
                throw std::runtime_error("File does not exist: " + filename);
            }

            boost::filesystem::path path(filename);
            boost::uintmax_t file_size = boost::filesystem::file_size(path);
            if (file_size == 0) {
                throw std::runtime_error("File is empty: " + filename);
            }

            // Memory map the file
            mmapFile.open(filename);
            if (!mmapFile.is_open()) {
                throw std::runtime_error("Failed to memory map file: " + filename);
            }

            data = reinterpret_cast<const uint8_t *>(mmapFile.data());
            fileSize = mmapFile.size();

            if (verbose) {
                std::cout << "File opened: " << filename << " (" << fileSize << " bytes)" << std::endl;
            }
        } catch (const std::exception &e) {
            std::cout << "Error opening file: " << e.what() << std::endl;
            throw;
        }
    }

    bool processFile() {
        try {
            // Ensure we have a valid data pointer and file size
            if (!data || fileSize == 0) {
                std::cout << "No data available - file may not be properly opened" << std::endl;
                return false;
            }

            if (!validateHeader()) {
                return false;
            }

            // Check file size against header's reported size
            if (header.filesize != fileSize) {
                std::cout << "WARNING: File size mismatch. Header indicates "
                        << header.filesize << " bytes, but actual file size is "
                        << fileSize << " bytes" << std::endl;
                // Continue anyway, but this could indicate a truncated file
            }

            // Initialize SHA maps
            stateMap = SHAMap(tnACCOUNT_STATE);
            txMap = SHAMap(tnTRANSACTION_MD);

            // Process all ledgers
            size_t offset = sizeof(CATLHeader);

            uint32_t ledgers(0);
            while (offset < fileSize) {
                ledgers++;
                size_t newOffset = processLedger(offset);
                // if (ledgers == 13) {
                //     break;
                // }

                if (newOffset == offset) {
                    std::cout << "No progress made processing ledger at offset " << offset << std::endl;
                    break;
                }
                offset = newOffset;
            }

            // Print summary
            std::cout << "\nProcessing complete!" << std::endl;
            std::cout << "Ledgers processed: " << stats.ledgersProcessed
                    << " (expected " << (header.max_ledger - header.min_ledger + 1) << ")" << std::endl;
            std::cout << "Total state nodes: " << stats.stateNodesTotal << std::endl;
            std::cout << "Total transaction nodes: " << stats.txNodesTotal << std::endl;
            std::cout << "State removals applied: " << stats.stateRemovalsApplied << std::endl;
            std::cout << "Hash verifications: "
                    << stats.successfulHashVerifications << " succeeded, "
                    << stats.failedHashVerifications << " failed" << std::endl;

            return true;
        } catch (const SHAMapException &e) {
            std::cout << "SHAMap error processing file: " << e.what() << std::endl;
            return false;
        } catch (const std::exception &e) {
            std::cout << "Error processing file: " << e.what() << std::endl;
            return false;
        }
    }
};

int main(int argc, char *argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <catalogue_file> [--verbose]" << std::endl;
        std::cerr << "\nThis tool processes CATL files from the XRP Ledger, building internal" << std::endl;
        std::cerr << "SHAMaps from the memory-mapped data and verifying the cryptographic hashes." << std::endl;
        return 1;
    }

    std::string inputFile = argv[1];
    bool verbose = true;

    for (int i = 2; i < argc; i++) {
        if (std::string(argv[i]) == "--verbose") {
            verbose = true;
        }
    }

    // Start timing
    auto startTime = std::chrono::high_resolution_clock::now();

    try {
        std::cout << "Processing CATL file: " << inputFile << std::endl;

        CATLHasher hasher(inputFile, verbose);
        bool result = hasher.processFile();

        // Calculate and display execution time
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        std::cout << "\nExecution completed in " << duration.count() / 1000.0
                << " seconds (" << duration.count() << " ms)" << std::endl;

        return result ? 0 : 1;
    } catch (const std::exception &e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
