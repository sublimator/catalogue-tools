#include <iostream>
#include <fstream>
#include <vector>
#include <array>
#include <memory>
#include <string>
#include <stdexcept>
#include <map>
#include <chrono>
#include <sstream> // Needed for logger

// For memory mapping
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/filesystem.hpp>

// For crypto
#include <openssl/evp.h>

//----------------------------------------------------------
// Logging System
//----------------------------------------------------------

enum class LogLevel {
    ERROR = 0,
    WARNING = 1,
    INFO = 2,
    DEBUG = 3
};

class Logger {
private:
    static LogLevel currentLevel; // Global log level threshold
    LogLevel messageLevel; // Level of the specific message being logged
    std::ostringstream buffer; // Buffer to build the log message
    bool active; // Is this message level active?

    // Helper to get the string prefix for a level
    static std::string levelPrefix(LogLevel level) {
        switch (level) {
            case LogLevel::ERROR: return "[ERROR] ";
            case LogLevel::WARNING: return "[WARN]  ";
            case LogLevel::INFO: return "[INFO]  ";
            case LogLevel::DEBUG: return "[DEBUG] ";
            default: return "[?????] ";
        }
    }

public:
    // Constructor: Check if this message should be logged based on global level
    Logger(LogLevel level) : messageLevel(level), active(level <= currentLevel) {
        if (active) {
            // Prepend the level prefix if active
            buffer << levelPrefix(messageLevel);
        }
    }

    // Destructor: Output the buffered message if it was active
    ~Logger() {
        if (active) {
            // Output the complete message with a newline
            // Use std::cerr for ERROR and WARNING, std::cout for INFO and DEBUG
            if (messageLevel <= LogLevel::WARNING) {
                std::cerr << buffer.str() << std::endl;
            } else {
                std::cout << buffer.str() << std::endl;
            }
        }
    }

    // Stream operator to append data to the buffer
    template<typename T>
    Logger &operator<<(const T &value) {
        if (active) {
            buffer << value;
        }
        return *this;
    }

    // Static method to set the global logging level
    static void setLevel(LogLevel level) {
        currentLevel = level;
        // Log the level change itself (useful for debugging the logger)
        Logger(LogLevel::INFO) << "Log level set to " << static_cast<int>(level);
    }

    static LogLevel getLevel() {
        return currentLevel;
    }
};

// Define and initialize the static currentLevel
// Default level set here (e.g., INFO)
LogLevel Logger::currentLevel = LogLevel::INFO;

// Logging Macros for convenience
// Efficient conditional logging macros
#define LOGE   Logger(LogLevel::ERROR)
#define LOGW    if(Logger::getLevel() >= LogLevel::WARNING) Logger(LogLevel::WARNING)
#define LOGI    if(Logger::getLevel() >= LogLevel::INFO) Logger(LogLevel::INFO)
#define LOGD   if(Logger::getLevel() >= LogLevel::DEBUG) Logger(LogLevel::DEBUG)

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
    std::array<uint8_t, 64> hash; // Note: This hash is often unused/zero in practice
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
// Custom Exception Classes (Unchanged)
//----------------------------------------------------------
class SHAMapException : public std::runtime_error {
public:
    explicit SHAMapException(const std::string &message)
        : std::runtime_error(message) {
    }
};

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

class NullNodeException : public SHAMapException {
public:
    explicit NullNodeException(const std::string &context)
        : SHAMapException("Null node encountered: " + context) {
    }
};

class NullItemException : public SHAMapException {
public:
    explicit NullItemException()
        : SHAMapException("Found leaf node with null item") {
    }
};

class HashCalculationException : public SHAMapException {
public:
    explicit HashCalculationException(const std::string &reason)
        : SHAMapException("Hash calculation error: " + reason) {
    }
};

// Slice class to reference data without copying (Unchanged)
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

// Convert Slice to hex string (Unchanged)
void slice_hex(const Slice sl, std::string &result) {
    static constexpr char hexChars[] = "0123456789abcdef";
    result.reserve(sl.size() * 2);
    const uint8_t *bytes = sl.data();
    for (size_t i = 0; i < sl.size(); ++i) {
        uint8_t byte = bytes[i];
        result.push_back(hexChars[(byte >> 4) & 0xF]);
        result.push_back(hexChars[byte & 0xF]);
    }
}

// 256-bit hash type (Unchanged)
class Hash256 {
private:
    std::array<uint8_t, 32> data_;

public:
    Hash256() : data_() { data_.fill(0); }

    explicit Hash256(const std::array<uint8_t, 32> &data) : data_(data) {
    }

    explicit Hash256(const uint8_t *data) : data_() { std::memcpy(data_.data(), data, 32); }
    uint8_t *data() { return data_.data(); }
    const uint8_t *data() const { return data_.data(); }
    static constexpr std::size_t size() { return 32; }
    static Hash256 zero() { return {}; }
    bool operator==(const Hash256 &other) const { return data_ == other.data_; }
    bool operator!=(const Hash256 &other) const { return !(*this == other); }

    [[nodiscard]] std::string toString() const {
        std::string result;
        slice_hex({data(), size()}, result);
        return result;
    }
};

// Improved Key class with equality operator (Unchanged)
class Key {
private:
    const std::uint8_t *data_;

public:
    Key(const std::uint8_t *data) : data_(data) {
    }

    const std::uint8_t *data() const { return data_; }
    static constexpr std::size_t size() { return 32; }
    Hash256 toHash() const { return Hash256(data_); }
    std::string toString() const { return toHash().toString(); }
    bool operator==(const Key &other) const { return std::memcmp(data_, other.data_, 32) == 0; }
    bool operator!=(const Key &other) const { return !(*this == other); }
};

// Memory-mapped item that references slices of the memory-mapped file (Unchanged)
class MmapItem {
private:
    Key key_;
    Slice data_;

public:
    MmapItem(const std::uint8_t *keyData, const std::uint8_t *data, std::size_t dataSize)
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

// selectBranch function (Unchanged)
int selectBranch(const Key &key, int depth) {
    int byteIdx = depth / 2;
    if (byteIdx < 0 || byteIdx >= static_cast<int>(Key::size())) {
        throw InvalidDepthException(depth, Key::size());
    }
    int nibbleIdx = depth % 2;
    uint8_t byte_val = key.data()[byteIdx];
    return (nibbleIdx == 0) ? (byte_val >> 4) & 0xF : byte_val & 0xF;
}

// Base class for SHAMap nodes (Unchanged)
class SHAMapTreeNode {
protected:
    Hash256 hash;
    bool hashValid = false;

public:
    void invalidateHash() { hashValid = false; }

    virtual ~SHAMapTreeNode() = default;

    virtual bool isLeaf() const = 0;

    virtual bool isInner() const = 0;

    virtual void updateHash() = 0;

    const Hash256 &getHash() {
        if (!hashValid) {
            // LOG_DEBUG << "Recalculating hash for node (Depth: " << (isInner() ? std::to_string(dynamic_cast<SHAMapInnerNode*>(this)->getDepth()) : "Leaf") << ")";
            updateHash();
            hashValid = true;
        }
        return hash;
    }
};

// Inner node implementation (Unchanged)
class SHAMapInnerNode : public SHAMapTreeNode {
private:
    std::array<std::shared_ptr<SHAMapTreeNode>, 16> children;
    uint16_t branchMask = 0;
    uint8_t depth = 0;

public:
    SHAMapInnerNode(uint8_t nodeDepth = 0) : depth(nodeDepth) {
    }

    bool isLeaf() const override { return false; }
    bool isInner() const override { return true; }
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
        if (branch < 0 || branch >= 16) throw InvalidBranchException(branch);
        bool changed = (children[branch] != child); // Basic check if pointer changes
        if (changed) {
            invalidateHash(); // Invalidate self if a child changes
            if (child) {
                children[branch] = child;
                branchMask |= (1 << branch);
                if (child->isInner()) {
                    auto innerChild = std::static_pointer_cast<SHAMapInnerNode>(child);
                    innerChild->setDepth(depth + 1);
                }
            } else {
                children[branch] = nullptr;
                branchMask &= ~(1 << branch);
            }
        }
        return changed; // Return true if the child actually changed
    }

    std::shared_ptr<SHAMapTreeNode> getChild(int branch) const {
        if (branch < 0 || branch >= 16) throw InvalidBranchException(branch);
        return children[branch];
    }

    bool hasChild(int branch) const {
        if (branch < 0 || branch >= 16) throw InvalidBranchException(branch);
        return (branchMask & (1 << branch)) != 0;
    }

    int getBranchCount() const {
        int count = 0;
        for (int i = 0; i < 16; i++) if (hasChild(i)) count++;
        return count;
    }

    uint16_t getBranchMask() const { return branchMask; }

    std::shared_ptr<SHAMapLeafNode> getOnlyChildLeaf() const {
        std::shared_ptr<SHAMapLeafNode> resultLeaf = nullptr;
        int leafCount = 0;
        for (const std::shared_ptr<SHAMapTreeNode> &childNodePtr: children) {
            if (childNodePtr) {
                if (childNodePtr->isInner()) return nullptr; // Found an inner node
                leafCount++;
                if (leafCount == 1) resultLeaf = std::static_pointer_cast<SHAMapLeafNode>(childNodePtr);
                else return nullptr; // Found more than one leaf
            }
        }
        return resultLeaf; // Null if 0 leaves, pointer if exactly 1 leaf
    }
};

// Leaf node implementation that uses MmapItem (Unchanged)
class SHAMapLeafNode : public SHAMapTreeNode {
private:
    std::shared_ptr<MmapItem> item;
    SHAMapNodeType type;

public:
    SHAMapLeafNode(std::shared_ptr<MmapItem> i, SHAMapNodeType t) : item(std::move(i)), type(t) {
        if (!item) throw NullItemException();
    }

    bool isLeaf() const override { return true; }
    bool isInner() const override { return false; }

    void updateHash() override {
        std::array<unsigned char, 4> prefix = {0, 0, 0, 0};
        auto set = [&prefix](auto &from) { std::memcpy(prefix.data(), from.data(), 4); };
        switch (type) {
            case tnTRANSACTION_NM:
            case tnTRANSACTION_MD: set(HashPrefix::txNode);
                break;
            case tnACCOUNT_STATE: set(HashPrefix::leafNode);
                break;
            default: set(HashPrefix::leafNode); // Default or throw?
        }
        EVP_MD_CTX *ctx = EVP_MD_CTX_new();
        if (!ctx) throw HashCalculationException("Failed to create EVP_MD_CTX");
        if (EVP_DigestInit_ex(ctx, EVP_sha512(), nullptr) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to initialize SHA-512 digest");
        }
        if (EVP_DigestUpdate(ctx, &prefix, sizeof(prefix)) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to update digest with prefix");
        }
        if (EVP_DigestUpdate(ctx, item->slice().data(), item->slice().size()) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to update digest with item data");
        }
        if (EVP_DigestUpdate(ctx, item->key().data(), Key::size()) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to update digest with item key");
        }
        std::array<unsigned char, 64> fullHash;
        unsigned int hashLen = 0;
        if (EVP_DigestFinal_ex(ctx, fullHash.data(), &hashLen) != 1) {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException("Failed to finalize digest");
        }
        EVP_MD_CTX_free(ctx);
        hash = Hash256(reinterpret_cast<const uint8_t *>(fullHash.data()));
        hashValid = true;
    }

    std::shared_ptr<MmapItem> getItem() const { return item; }
    SHAMapNodeType getType() const { return type; }
};

// PathFinder class (Unchanged internally, except for logging if needed)
class PathFinder {
private:
    const Key &targetKey;
    std::shared_ptr<SHAMapInnerNode> searchRoot;
    std::vector<std::shared_ptr<SHAMapInnerNode> > inners;
    std::vector<int> branches;
    std::shared_ptr<SHAMapLeafNode> foundLeaf = nullptr;
    bool leafKeyMatches = false;
    int terminalBranch = -1;

    void findPath(std::shared_ptr<SHAMapInnerNode> root) {
        if (!root) throw NullNodeException("PathFinder: null root node");
        searchRoot = root;
        inners.clear();
        branches.clear();
        foundLeaf = nullptr;
        leafKeyMatches = false;
        terminalBranch = -1;
        std::shared_ptr<SHAMapInnerNode> currentInner = root;
        while (true) {
            int branch = selectBranch(targetKey, currentInner->getDepth());
            std::shared_ptr<SHAMapTreeNode> child = currentInner->getChild(branch);
            if (!child) {
                terminalBranch = branch;
                inners.push_back(currentInner);
                break; // End at null branch
            }
            if (child->isLeaf()) {
                terminalBranch = branch;
                inners.push_back(currentInner);
                foundLeaf = std::static_pointer_cast<SHAMapLeafNode>(child);
                if (foundLeaf->getItem()) {
                    leafKeyMatches = (foundLeaf->getItem()->key() == targetKey);
                } else {
                    throw NullItemException(); // Should not happen if leaf constructor works
                }
                break; // End at leaf
            }
            inners.push_back(currentInner);
            branches.push_back(branch);
            currentInner = std::static_pointer_cast<SHAMapInnerNode>(child);
        }
    }

public:
    PathFinder(std::shared_ptr<SHAMapInnerNode> &root, const Key &key) : targetKey(key) { findPath(root); }
    bool hasLeaf() const { return foundLeaf != nullptr; }
    bool didLeafKeyMatch() const { return leafKeyMatches; }
    bool endedAtNullBranch() const { return foundLeaf == nullptr && terminalBranch != -1; }
    std::shared_ptr<const SHAMapLeafNode> getLeaf() const { return foundLeaf; }
    std::shared_ptr<SHAMapLeafNode> getLeafMutable() { return foundLeaf; }
    std::shared_ptr<SHAMapInnerNode> getParentOfTerminal() { return inners.empty() ? nullptr : inners.back(); }

    std::shared_ptr<const SHAMapInnerNode> getParentOfTerminal() const {
        return inners.empty() ? nullptr : inners.back();
    }

    int getTerminalBranch() const { return terminalBranch; }

    void dirtyPath() {
        for (auto &inner: inners) {
            inner->invalidateHash();
        }
        LOGD << "Dirtied path for key " << targetKey.toString() << " (depth " << inners.size() << ")";
    }

    void collapsePath() {
        if (inners.size() <= 1) return;
        std::shared_ptr<SHAMapLeafNode> onlyChild = nullptr;
        auto innermost = inners.back();
        onlyChild = innermost->getOnlyChildLeaf();

        // Only proceed if the innermost node *now* has only one child
        // (It might not if the removed item was part of a collision)
        if (!onlyChild && innermost->getBranchCount() > 1) {
            LOGD << "Collapse skipped: Innermost node still has multiple branches.";
            return;
        }
        // If innermost is now empty (last child removed), 'onlyChild' is null.

        LOGD << "Starting collapse check for path of depth " << inners.size();

        for (int i = static_cast<int>(inners.size()) - 2; i >= 0; --i) {
            auto inner = inners[i];
            int branch = branches[i]; // Branch taken *from* inner *to* get to the next deeper node

            LOGD << "Collapse checking node at depth " << inner->getDepth() << " (parent index " << i <<
                    ", branch " << branch << ")";

            // If we have a single leaf from a deeper node to promote
            if (onlyChild) {
                LOGD << "Promoting leaf " << onlyChild->getItem()->key().toString() <<
                        " to replace child at branch " << branch;
                inner->setChild(branch, onlyChild); // Replace the deeper inner node with the leaf
            } else {
                // If the node below became empty, we clear the branch leading to it
                // This is handled implicitly if setChild(branch, nullptr) was called before collapse
                LOGD << "No leaf to promote, checking if branch " << branch <<
                        " should be cleared (likely already done).";
                // Redundant check: if (inner->getChild(branch) != nullptr && inner->getChild(branch)->isInner() && std::static_pointer_cast<SHAMapInnerNode>(inner->getChild(branch))->getBranchCount() == 0) {
                //      LOG_DEBUG << "Explicitly clearing branch " << branch << " as child became empty.";
                //      inner->setChild(branch, nullptr);
                // }
            }

            // Now, check if *this* node (`inner`) qualifies for collapsing
            onlyChild = inner->getOnlyChildLeaf();

            // If this node now has only one leaf child, we keep 'onlyChild' set for the next iteration.
            // If it has multiple children, or an inner child, or is empty, 'onlyChild' becomes null, stopping the upward collapse.
            if (!onlyChild) {
                LOGD << "Collapse stopped at depth " << inner->getDepth() << ". Node has " << inner->
                        getBranchCount() << " children.";
                break;
            } else {
                LOGD << "Node at depth " << inner->getDepth() <<
                        " now has only one leaf. Continuing collapse upwards.";
            }
        }

        // Special case: If the root itself collapsed to a single leaf
        if (onlyChild && inners.size() > 0 && inners[0] == searchRoot) {
            LOGD << "Root node collapsed into a single leaf node.";
            // This case is rare and usually means the entire map has only one item left.
            // The root remains an InnerNode but contains only one leaf child.
            // If we wanted the root itself to *become* a leaf node, more complex logic
            // would be needed in the SHAMap class to handle replacing the root pointer.
        }
        LOGD << "Collapse finished.";
    }
};

// Main SHAMap class
class SHAMap {
private:
    std::shared_ptr<SHAMapInnerNode> root;
    SHAMapNodeType nodeType;

public:
    SHAMap(SHAMapNodeType type = tnACCOUNT_STATE) : nodeType(type) {
        root = std::make_shared<SHAMapInnerNode>(0); // Root has depth 0
        LOGD << "SHAMap created with type " << static_cast<int>(type);
    }

    Hash256 getChildHash(int ix) {
        // Added for completeness, but not used by CATLHasher logic
        try {
            if (!root) return Hash256::zero();
            auto child = root->getChild(ix);
            return child ? child->getHash() : Hash256::zero();
        } catch (const InvalidBranchException &) {
            return Hash256::zero();
        }
    }

    bool addItem(std::shared_ptr<MmapItem> item, bool allowUpdate = true) {
        if (!item) {
            LOGW << "Attempted to add null item to SHAMap.";
            return false;
        }
        const Key &key = item->key();
        LOGD << "addItem called for key: " << key.toString();

        try {
            PathFinder pathFinder(root, key);

            // Case 1: Path ends at null branch - add the item as a new leaf
            if (pathFinder.endedAtNullBranch()) {
                auto parent = pathFinder.getParentOfTerminal();
                int branch = pathFinder.getTerminalBranch();
                if (!parent) {
                    // Should only happen if root is null initially, which constructor prevents
                    LOGE << "addItem: Path ended at null branch but parent is null. This should not happen.";
                    return false; // Or throw
                }

                LOGD << "addItem: Adding new leaf at depth " << parent->getDepth() << ", branch " << branch;
                auto newLeaf = std::make_shared<SHAMapLeafNode>(item, nodeType);
                parent->setChild(branch, newLeaf);
                pathFinder.dirtyPath();
                return true;
            }

            // Case 2: Path ends at leaf with matching key - update if allowed
            if (pathFinder.hasLeaf() && pathFinder.didLeafKeyMatch()) {
                if (allowUpdate) {
                    auto parent = pathFinder.getParentOfTerminal();
                    int branch = pathFinder.getTerminalBranch();
                    if (!parent) {
                        LOGE << "addItem Update: Path ended at matching leaf but parent is null.";
                        return false;
                    }

                    LOGD << "addItem: Updating existing leaf at depth " << parent->getDepth() << ", branch " <<
                            branch;
                    auto newLeaf = std::make_shared<SHAMapLeafNode>(item, nodeType); // Create new leaf with new data
                    parent->setChild(branch, newLeaf); // Replace the old leaf
                    pathFinder.dirtyPath();
                    return true;
                } else {
                    LOGD << "addItem: Item with key " << key.toString() << " already exists, update not allowed.";
                    return false; // Item exists, but update not allowed
                }
            }

            // Case 3: Path ends at leaf with different key - create collision nodes
            if (pathFinder.hasLeaf() && !pathFinder.didLeafKeyMatch()) {
                auto parent = pathFinder.getParentOfTerminal();
                int branch = pathFinder.getTerminalBranch();
                auto existingLeaf = pathFinder.getLeafMutable(); // Get the existing leaf
                if (!parent || !existingLeaf) {
                    LOGE << "addItem Collision: Path ended at non-matching leaf but parent or leaf is null.";
                    return false;
                }

                auto existingItem = existingLeaf->getItem();
                if (!existingItem) throw NullItemException(); // Should be caught by leaf constructor

                LOGD << "addItem: Collision detected at depth " << parent->getDepth() << ", branch " << branch;
                LOGD << "  Existing Key: " << existingItem->key().toString();
                LOGD << "  New Key:      " << item->key().toString();

                // Start path invalidation *before* restructuring
                pathFinder.dirtyPath();

                // Take the existing leaf out temporarily
                parent->setChild(branch, nullptr);

                // Create the first new inner node to replace the original leaf's position
                uint8_t currentDepth = parent->getDepth() + 1;
                auto currentInner = std::make_shared<SHAMapInnerNode>(currentDepth);
                parent->setChild(branch, currentInner); // Link the new inner node

                // Iteratively add inner nodes until the keys diverge
                std::shared_ptr<SHAMapTreeNode> nodeToPlaceExisting = existingLeaf;
                std::shared_ptr<SHAMapTreeNode> nodeToPlaceNew = std::make_shared<SHAMapLeafNode>(item, nodeType);

                while (true) {
                    int existingBranch = selectBranch(existingItem->key(), currentDepth);
                    int newBranch = selectBranch(item->key(), currentDepth);

                    LOGD << "  Collision resolution at depth " << currentDepth << ": existing branch=" <<
                            existingBranch << ", new branch=" << newBranch;

                    if (existingBranch != newBranch) {
                        // Keys diverge, place both nodes under the current inner node
                        currentInner->setChild(existingBranch, nodeToPlaceExisting);
                        currentInner->setChild(newBranch, nodeToPlaceNew);
                        LOGD << "  Collision resolved. Placed existing at branch " << existingBranch <<
                                " and new at branch " << newBranch;
                        break; // Finished resolving collision
                    } else {
                        // Keys still collide at this depth, need another inner node
                        LOGD << "  Collision continues. Creating new inner node at branch " << newBranch;
                        auto nextInner = std::make_shared<SHAMapInnerNode>(currentDepth + 1);
                        currentInner->setChild(newBranch, nextInner); // Add the deeper inner node
                        currentInner = nextInner; // Descend into the new inner node
                        currentDepth++;
                        if (currentDepth >= 64) {
                            // Max depth check
                            LOGE << "addItem Collision: Maximum SHAMap depth (64) reached for key " << key.
                                    toString();
                            // Maybe try to revert changes? Complex. For now, signal failure.
                            // Consider throwing an exception here.
                            return false;
                        }
                    }
                }
                // No need to call dirtyPath again, it was done before restructuring.
                return true;
            }

            // Should not be reachable if PathFinder logic is correct
            LOGE << "addItem: Unexpected state reached after PathFinder for key " << key.toString();
            throw SHAMapException("Unexpected state in addItem - PathFinder logic error");
        } catch (const SHAMapException &e) {
            LOGE << "SHAMap error adding item with key " << item->key().toString() << ": " << e.what();
            return false;
        } catch (const std::exception &e) {
            LOGE << "Standard exception adding item with key " << item->key().toString() << ": " << e.what();
            return false;
        }
    }


    bool removeItem(const Key &key) {
        LOGD << "removeItem called for key: " << key.toString();
        try {
            PathFinder pathFinder(root, key);

            if (!pathFinder.hasLeaf() || !pathFinder.didLeafKeyMatch()) {
                LOGD << "removeItem: Key " << key.toString() << " not found.";
                return false; // Item not found
            }

            auto parent = pathFinder.getParentOfTerminal();
            int branch = pathFinder.getTerminalBranch();
            if (!parent) {
                LOGE << "removeItem: Found matching leaf but parent is null.";
                return false;
            }

            LOGD << "removeItem: Removing leaf at depth " << parent->getDepth() << ", branch " << branch;

            // Invalidate path *before* removing the node
            pathFinder.dirtyPath();

            // Remove the leaf
            parent->setChild(branch, nullptr);

            // Attempt to collapse the path upwards
            pathFinder.collapsePath();

            return true;
        } catch (const SHAMapException &e) {
            LOGE << "SHAMap error removing item with key " << key.toString() << ": " << e.what();
            return false;
        } catch (const std::exception &e) {
            LOGE << "Standard exception removing item with key " << key.toString() << ": " << e.what();
            return false;
        }
    }

    Hash256 getHash() {
        // Made const-correct
        if (!root) {
            return Hash256::zero();
        }
        LOGD << "Calculating root hash for SHAMap type " << static_cast<int>(nodeType);
        return root->getHash(); // Delegate to root node's getHash
    }

    void setNodeType(SHAMapNodeType type) {
        // Note: Changing type after adding items might lead to incorrect hash prefixes
        // if nodes aren't re-hashed. Best to set type at construction.
        LOGI << "Setting SHAMap node type to " << static_cast<int>(type);
        nodeType = type;
        // Optionally, invalidate all hashes if type changes mid-life?
        // if (root) root->invalidateHashRecursively(); // Would need this method
    }
};

// Convert NetClock time to human-readable string (Unchanged)
std::string formatRippleTime(uint64_t netClockTime) {
    static const time_t rippleEpochOffset = 946684800;
    time_t unixTime = netClockTime + rippleEpochOffset;
    // Use gmtime_r for thread safety if this were multithreaded
    std::tm timeinfo;
#ifdef _WIN32
        gmtime_s(&timeinfo, &unixTime);
#else
    gmtime_r(&unixTime, &timeinfo);
#endif
    if (unixTime < rippleEpochOffset && netClockTime != 0) return "Invalid time (before epoch)";
    char timeStr[30];
    std::strftime(timeStr, sizeof(timeStr), "%Y-%m-%d %H:%M:%S UTC", &timeinfo);
    return timeStr;
}

class CATLHasher {
private:
    boost::iostreams::mapped_file_source mmapFile;
    const uint8_t *data = nullptr;
    size_t fileSize = 0;
    CATLHeader header;
    // bool verbose = true; // Replaced by Logger::currentLevel

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
            LOGE << "File too small (" << fileSize << " bytes) to contain a valid CATL header (" << sizeof(
                CATLHeader) << " bytes)";
            return false;
        }

        std::memcpy(&header, data, sizeof(CATLHeader));

        // Basic validation
        if (header.magic != CATL) {
            LOGE << "Invalid magic value: expected 0x" << std::hex << CATL
                    << ", got 0x" << header.magic << std::dec;
            return false;
        }

        uint16_t catlVersion = header.version & CATALOGUE_VERSION_MASK;
        uint8_t compressionLevel = (header.version & CATALOGUE_COMPRESS_LEVEL_MASK) >> 8;

        LOGI << "CATL Header Validation:";
        LOGI << "  Magic: 0x" << std::hex << header.magic << std::dec << " (OK)";
        LOGI << "  Version Raw: 0x" << std::hex << header.version << std::dec;
        LOGI << "  Version Parsed: " << catlVersion;
        LOGI << "  Compression Level: " << static_cast<int>(compressionLevel);
        LOGI << "  Ledger range: " << header.min_ledger << " - " << header.max_ledger;
        LOGI << "  Network ID: " << header.network_id;
        LOGI << "  Reported Filesize: " << header.filesize << " bytes";
        // Header hash is often zero, maybe log if non-zero
        // std::string headerHashStr; slice_hex({header.hash.data(), 32}, headerHashStr); // Only first 32 bytes usually relevant if used
        // LOG_INFO << "  Header Hash Prefix: " << headerHashStr;


        if (compressionLevel != 0) {
            LOGE << "Compressed CATL files are not supported (Level: " << static_cast<int>(compressionLevel) <<
                    ")";
            return false;
        }

        return true;
    }

    // Unified map processing function that handles both state and transaction maps
    size_t processMap(size_t offset, SHAMap &map, uint32_t &nodeCount, bool isStateMap) {
        nodeCount = 0;
        bool foundTerminal = false;
        const char *mapTypeName = isStateMap ? "state" : "transaction";

        LOGD << "Processing " << mapTypeName << " map starting at offset " << offset;

        while (offset < fileSize && !foundTerminal) {
            // Read node type - check bounds first
            if (offset >= fileSize) {
                LOGW << "Unexpected EOF reading node type in " << mapTypeName << " map at offset " << offset;
                break; // Exit loop gracefully
            }
            uint8_t nodeTypeVal = data[offset++];
            SHAMapNodeType nodeType = static_cast<SHAMapNodeType>(nodeTypeVal);


            if (nodeType == tnTERMINAL) {
                foundTerminal = true;
                LOGD << "Found " << mapTypeName << " map terminal marker at offset " << (offset - 1);
                break;
            }

            // Check for valid storable node types or removal
            if (nodeType != tnTRANSACTION_NM && nodeType != tnTRANSACTION_MD &&
                nodeType != tnACCOUNT_STATE && nodeType != tnREMOVE) {
                LOGE << "Invalid node type " << static_cast<int>(nodeType) << " encountered in "
                        << mapTypeName << " map data stream at offset " << (offset - 1);
                return offset - 1; // Return error offset
            }

            // Read key (32 bytes) - check bounds first
            size_t keyEndOffset = offset + Key::size();
            if (keyEndOffset > fileSize) {
                LOGW << "Unexpected EOF reading key in " << mapTypeName << " map at offset " << offset;
                return offset; // Return current offset as error point
            }
            const uint8_t *keyData = data + offset;
            Key itemKey(keyData); // Create Key object immediately
            offset = keyEndOffset;


            if (nodeType == tnREMOVE) {
                if (isStateMap) {
                    LOGD << "Processing state removal for key: " << itemKey.toString();
                    if (map.removeItem(itemKey)) {
                        stats.stateRemovalsApplied++;
                        LOGD << "  Successfully removed item.";
                    } else {
                        LOGD << "  Item for removal not found in map."; // This can be normal
                    }
                } else {
                    LOGW << "Found tnREMOVE node type in transaction map stream for key "
                            << itemKey.toString() << " at offset " << (offset - Key::size() - 1) << ". Skipping.";
                }
                nodeCount++;
                continue; // Move to next node entry
            }

            // Read data size (4 bytes) - check bounds first
            size_t dataSizeEndOffset = offset + sizeof(uint32_t);
            if (dataSizeEndOffset > fileSize) {
                LOGW << "Unexpected EOF reading data size in " << mapTypeName << " map for key " << itemKey.
                        toString() << " at offset " << offset;
                return offset;
            }
            uint32_t dataSize = 0;
            std::memcpy(&dataSize, data + offset, sizeof(uint32_t));
            offset = dataSizeEndOffset;


            // Validate data size (check bounds and basic sanity check)
            // Allow zero size, but large sizes are suspicious
            const uint32_t MAX_REASONABLE_NODE_SIZE = 64 * 1024; // 64KB sanity limit per node
            size_t dataEndOffset = offset + dataSize;
            if (dataSize > MAX_REASONABLE_NODE_SIZE) {
                LOGW << "Unusually large data size (" << dataSize << " bytes) for node with key "
                        << itemKey.toString() << " in " << mapTypeName << " map at offset " << (
                            offset - sizeof(uint32_t))
                        << ". Potential corruption.";
                // Continue processing cautiously, but log it
            }
            if (dataEndOffset > fileSize) {
                LOGW << "Data size (" << dataSize << " bytes) exceeds file bounds for node with key "
                        << itemKey.toString() << " in " << mapTypeName << " map. File truncated? Offset: " << offset;
                return offset; // Return current offset as error point
            }

            // Create MmapItem pointing directly to the mapped memory
            const uint8_t *itemDataPtr = data + offset;
            offset = dataEndOffset; // Move offset past the data

            auto item = std::make_shared<MmapItem>(keyData, itemDataPtr, dataSize);

            // Only log detailed add if DEBUG level is enabled
            LOGD << "Adding " << mapTypeName << " item: Key=" << item->key().toString()
                    << ", DataSize=" << dataSize << ", Type=" << static_cast<int>(nodeType);

            // Assign correct SHAMap node type based on file data
            // The map itself should already have its base type set (tnACCOUNT_STATE or tnTRANSACTION_MD)
            // But the leaf node needs the specific type from the file stream.
            SHAMapNodeType leafNodeType = nodeType; // Use the type read from the file for this leaf

            // The addItem function needs the SHAMap's base type to correctly create the *leaf node object*.
            // Let's adjust addItem or pass the type explicitly.
            // Let's refine SHAMap::addItem to accept the node type for the leaf being added.
            // --> No, the SHAMap's internal type dictates the prefix. The file type is just metadata.
            // --> Revert: The leaf node constructor needs the type for its hash prefix calc.

            // Pass the specific node type read from the file to the addItem logic,
            // which will pass it to the SHAMapLeafNode constructor.
            // --> Let's modify SHAMap::addItem signature (Alternative: pass in leaf node directly)
            // --> Simplest: Pass the `leafNodeType` to the `SHAMapLeafNode` constructor inside `addItem`.
            // We need to adjust SHAMap::addItem to handle this... or assume the map's type IS the leaf type.
            // The current SHAMap::addItem uses `this->nodeType`. Let's stick with that convention for now.
            // It implies the CATL file should only contain node types matching the map type (state or tx).
            if (isStateMap && nodeType != tnACCOUNT_STATE) {
                LOGW << "Encountered non-ACCOUNT_STATE node type (" << static_cast<int>(nodeType)
                        << ") in state map stream for key " << itemKey.toString();
            } else if (!isStateMap && nodeType != tnTRANSACTION_NM && nodeType != tnTRANSACTION_MD) {
                LOGW << "Encountered non-TRANSACTION node type (" << static_cast<int>(nodeType)
                        << ") in transaction map stream for key " << itemKey.toString();
            }


            if (!map.addItem(item)) {
                // addItem logs its own errors, but we can add context here
                LOGE << "Failed to add item with key " << itemKey.toString() << " to " << mapTypeName << " map.";
                // Depending on severity, we might want to stop processing the map here.
                // For now, continue to the next node.
            }
            nodeCount++;
        }

        if (!foundTerminal && offset < fileSize) {
            // This means the loop exited due to an error reported above
            LOGE << "Processing " << mapTypeName << " map stopped prematurely at offset " << offset;
        } else if (!foundTerminal && offset >= fileSize) {
            // Loop finished because EOF was reached before finding terminal marker
            LOGW << "Reached EOF while processing " << mapTypeName << " map, terminal marker not found. Processed "
                    << nodeCount << " nodes.";
        } else {
            LOGI << "Finished processing " << mapTypeName << " map. Nodes processed: " << nodeCount;
        }


        return offset; // Return the offset *after* the processed map (or error offset)
    }

    // Process a single ledger
    size_t processLedger(size_t offset) {
        // Check if we have enough data for the ledger info
        size_t ledgerInfoEndOffset = offset + sizeof(LedgerInfo);
        if (ledgerInfoEndOffset > fileSize) {
            LOGW << "Not enough data remaining (" << (fileSize - offset) << " bytes) for LedgerInfo struct ("
                    << sizeof(LedgerInfo) << " bytes) at offset " << offset << ". Assuming end of file.";
            return fileSize; // Indicate processing should stop
        }

        // Read ledger info
        LedgerInfo info;
        std::memcpy(&info, data + offset, sizeof(LedgerInfo));
        offset = ledgerInfoEndOffset; // Move offset past ledger info

        // Basic sanity check on sequence number
        if (info.sequence < header.min_ledger || info.sequence > header.max_ledger) {
            LOGW << "Ledger sequence " << info.sequence
                    << " is outside the expected range defined in header (" << header.min_ledger
                    << "-" << header.max_ledger << ")";
            // Continue processing, but this is suspicious
        }

        LOGI << "---- Processing Ledger " << info.sequence << " ----";
        LOGD << "  Ledger Header Offset: " << (offset - sizeof(LedgerInfo));
        LOGD << "  Ledger Hash:   " << Hash256(info.hash).toString();
        LOGD << "  Account Hash:  " << Hash256(info.accountHash).toString();
        LOGD << "  Tx Hash:       " << Hash256(info.txHash).toString();
        LOGD << "  Parent Hash:   " << Hash256(info.parentHash).toString();
        LOGD << "  Close Time:    " << info.closeTime << " (" << formatRippleTime(info.closeTime) << ")";
        LOGD << "  Parent Close:  " << info.parentCloseTime << " (" << formatRippleTime(info.parentCloseTime) << ")";
        LOGD << "  Drops:         " << info.drops;
        LOGD << "  Close Flags:   " << info.closeFlags;


        // Process account state map or delta
        bool isFirstLedgerInFile = (info.sequence == header.min_ledger);

        // Initialize state map *only* for the first ledger in the *file*
        // Subsequent ledgers modify the existing state map
        if (isFirstLedgerInFile) {
            LOGI << "Initializing state map for the first ledger in file (" << info.sequence << ")";
            stateMap = SHAMap(tnACCOUNT_STATE); // Reset/create state map
        } else {
            LOGI << "Updating existing state map for ledger " << info.sequence;
        }

        // Process state map data (nodes/removals)
        uint32_t stateNodesProcessedThisLedger = 0;
        size_t offsetAfterStateMap = processMap(offset, stateMap, stateNodesProcessedThisLedger, true);
        // true = isStateMap
        if (offsetAfterStateMap == offset && stateNodesProcessedThisLedger == 0 && Logger::getLevel() >=
            LogLevel::DEBUG) {
            // If processMap returned the same offset AND processed 0 nodes, it might be an empty delta or an error. processMap logs errors.
            // Check if a terminal marker was expected but not found (processMap warns about this)
            LOGD << "State map processing returned same offset; likely an empty delta or error occurred.";
        } else if (offsetAfterStateMap <= offset) {
            // If offset didn't advance or went backward, it signifies an error reported by processMap.
            LOGE << "Error processing state map for ledger " << info.sequence << ". Halting ledger processing.";
            return offset; // Return error offset
        }
        offset = offsetAfterStateMap; // Advance offset past state map data
        stats.stateNodesTotal += stateNodesProcessedThisLedger;

        // Process transaction map (always create fresh for each ledger)
        LOGI << "Processing transaction map for ledger " << info.sequence;
        txMap = SHAMap(tnTRANSACTION_MD); // Create a new, empty transaction map
        uint32_t txNodesProcessedThisLedger = 0;
        size_t offsetAfterTxMap = processMap(offset, txMap, txNodesProcessedThisLedger, false);
        // false = not isStateMap
        if (offsetAfterTxMap == offset && txNodesProcessedThisLedger == 0 && Logger::getLevel() >= LogLevel::DEBUG) {
            LOGD << "Transaction map processing returned same offset; likely an empty map or error occurred.";
        } else if (offsetAfterTxMap <= offset) {
            LOGE << "Error processing transaction map for ledger " << info.sequence <<
                    ". Halting ledger processing.";
            return offset; // Return error offset
        }
        offset = offsetAfterTxMap; // Advance offset past tx map data
        stats.txNodesTotal += txNodesProcessedThisLedger;


        // Verify map hashes against ledger info header
        LOGI << "Verifying map hashes for ledger " << info.sequence;
        verifyMapHash(stateMap, Hash256(info.accountHash), "state", info.sequence);
        verifyMapHash(txMap, Hash256(info.txHash), "transaction", info.sequence);

        stats.ledgersProcessed++;
        LOGI << "---- Finished Ledger " << info.sequence << " ----";
        return offset; // Return offset after processing this ledger
    }

    // Helper function to verify map hashes
    void verifyMapHash(SHAMap &map, const Hash256 &expectedHash,
                       // Changed to non-const to allow getHash() to compute if needed
                       const std::string &mapType, uint32_t ledgerSeq) {
        LOGD << "Computing final " << mapType << " hash for ledger " << ledgerSeq;
        Hash256 computedHash = map.getHash(); // This might compute the hash if dirty
        bool hashMatch = (computedHash == expectedHash);

        if (!hashMatch) {
            LOGW << "Ledger " << ledgerSeq << ": Computed " << mapType << " hash MISMATCH!";
            LOGW << "  Computed: " << computedHash.toString();
            LOGW << "  Expected: " << expectedHash.toString();
            stats.failedHashVerifications++;
        } else {
            LOGI << "Ledger " << ledgerSeq << ": Computed " << mapType << " hash verified OK.";
            // LOG_DEBUG << "  Hash: " << computedHash.toString(); // Log hash value only if debugging
            stats.successfulHashVerifications++;
        }
    }

public:
    // Constructor uses initializer list and logs actions
    CATLHasher(const std::string &filename)
        : stateMap(tnACCOUNT_STATE), // Pre-initialize to avoid potential issues if file opening fails
          txMap(tnTRANSACTION_MD) {
        LOGI << "Attempting to open and map file: " << filename;
        try {
            if (!boost::filesystem::exists(filename)) {
                throw std::runtime_error("File does not exist");
            }

            boost::filesystem::path path(filename);
            boost::uintmax_t fs_size = boost::filesystem::file_size(path);
            if (fs_size == 0) {
                throw std::runtime_error("File is empty");
            }

            mmapFile.open(filename);
            if (!mmapFile.is_open()) {
                throw std::runtime_error("boost::iostreams::mapped_file_source failed to open");
            }

            data = reinterpret_cast<const uint8_t *>(mmapFile.data());
            fileSize = mmapFile.size(); // Use size from mapped file object

            if (static_cast<boost::uintmax_t>(fileSize) != fs_size) {
                LOGW << "Memory mapped size (" << fileSize << ") differs from filesystem size (" << fs_size << ")";
                // This might indicate an issue but isn't necessarily fatal
            }

            LOGI << "Successfully mapped file: " << filename << " (" << fileSize << " bytes)";
        } catch (const boost::filesystem::filesystem_error &e) {
            LOGE << "Filesystem error accessing file " << filename << ": " << e.what();
            throw std::runtime_error("Filesystem error: " + std::string(e.what()));
        }
        catch (const std::exception &e) {
            // Catch potential runtime_errors from checks or boost exceptions
            LOGE << "Error opening or mapping file " << filename << ": " << e.what();
            // Re-throw to signal failure to the caller (main)
            throw;
        }
    }

    // Destructor (optional, for cleanup if needed, e.g., explicitly close mmapFile)
    ~CATLHasher() {
        if (mmapFile.is_open()) {
            mmapFile.close();
            LOGD << "Closed memory mapped file.";
        }
    }


    bool processFile() {
        try {
            // Ensure we have a valid data pointer and file size
            if (!data || fileSize == 0) {
                LOGE << "No data available or file size is zero. Cannot process.";
                return false;
            }

            LOGI << "Starting CATL file processing...";

            if (!validateHeader()) {
                LOGE << "Header validation failed. Aborting processing.";
                return false;
            }

            // Check file size against header's reported size
            if (header.filesize != fileSize) {
                // This is common if the file was moved/copied improperly, especially on Windows
                LOGW << "Filesize mismatch: Header reports " << header.filesize
                        << " bytes, actual mapped size is " << fileSize << " bytes.";
                // Decide how critical this is. Maybe proceed but use actual fileSize for bounds.
                // Using fileSize (actual mapped size) is safer for bounds checks.
            }


            // Initialize SHAMaps (already done in constructor now)
            // stateMap = SHAMap(tnACCOUNT_STATE);
            // txMap = SHAMap(tnTRANSACTION_MD);

            // Process all ledgers starting after the header
            size_t currentOffset = sizeof(CATLHeader);
            uint32_t expectedLedgerCount = (header.max_ledger - header.min_ledger + 1);

            while (currentOffset < fileSize) {
                size_t nextOffset = processLedger(currentOffset);

                if (nextOffset == currentOffset) {
                    // No progress made - processLedger detected an error or end condition
                    LOGE << "Processing stopped: No progress made processing ledger data at offset " <<
                            currentOffset << ". Check previous errors.";
                    break; // Exit the loop
                }
                if (nextOffset < currentOffset) {
                    // Offset went backward - definite error reported by processLedger
                    LOGE << "Processing stopped: Offset moved backward after processing ledger data near offset "
                            << currentOffset << ". Check previous errors.";
                    break;
                }

                currentOffset = nextOffset; // Advance offset

                // Optional: Check if we've processed more ledgers than expected
                if (stats.ledgersProcessed > expectedLedgerCount) {
                    LOGW << "Processed " << stats.ledgersProcessed << " ledgers, which is more than the "
                            << expectedLedgerCount << " expected from the header range.";
                    // Continue, but flag this potential issue. Could be a bad header.
                }
            } // End while loop

            // Check if we stopped *before* reaching EOF unexpectedly
            if (currentOffset < fileSize) {
                LOGW << "Processing loop finished, but " << (fileSize - currentOffset)
                        << " bytes remain in the file (ended at offset " << currentOffset << " of " << fileSize << ").";
            } else {
                LOGI << "Reached end of file processing (Offset: " << currentOffset << ").";
            }


            // Print final summary
            LOGI << "================= Summary =================";
            // LOG_INFO << "File Processed: " << mmapFile.path(); // Assuming mmapFile stores path
            LOGI << "Ledger Range in Header: " << header.min_ledger << " - " << header.max_ledger;
            LOGI << "Ledgers Processed: " << stats.ledgersProcessed << " / " << expectedLedgerCount;
            LOGI << "Total State Nodes (Adds/Updates): " << stats.stateNodesTotal;
            LOGI << "Total State Removals Applied: " << stats.stateRemovalsApplied;
            LOGI << "Total Transaction Nodes: " << stats.txNodesTotal;
            LOGI << "Map Hash Verifications:";
            LOGI << "  Succeeded: " << stats.successfulHashVerifications;
            LOGI << "  Failed:    " << stats.failedHashVerifications;
            LOGI << "===========================================";


            // Return true if processing seemed mostly okay, false if major errors occurred
            // A simple heuristic: success if at least one ledger was processed and no hash mismatches,
            // or allow some mismatches if required.
            bool overallSuccess = (stats.ledgersProcessed > 0 && stats.failedHashVerifications == 0);
            if (!overallSuccess) {
                if (stats.ledgersProcessed == 0)
                    LOGE << "Overall Result: FAILURE (No ledgers processed).";
                if (stats.failedHashVerifications > 0)
                    LOGE << "Overall Result: FAILURE (Hash mismatches occurred).";
            } else {
                LOGI << "Overall Result: SUCCESS";
            }

            return overallSuccess;
        } catch (const SHAMapException &e) {
            LOGE << "Aborting due to SHAMap error: " << e.what();
            return false;
        } catch (const std::exception &e) {
            LOGE << "Aborting due to unexpected error: " << e.what();
            return false;
        }
    }
};

int main(int argc, char *argv[]) {
    if (argc < 2) {
        // Use std::cerr directly for usage message as logging might not be configured yet
        std::cerr << "Usage: " << argv[0] << " <catalogue_file> [--verbose | --debug | --warn | --info]" << std::endl;
        std::cerr << "\nProcesses XRP Ledger CATL history shard files." << std::endl;
        std::cerr << "  Verifies SHAMap hashes for account state and transactions." << std::endl;
        std::cerr << "Log Levels:" << std::endl;
        std::cerr << "  --debug    Show all messages (verbose debugging)." << std::endl;
        std::cerr << "  --verbose  Alias for --debug." << std::endl;
        std::cerr << "  --info     Show informational messages, warnings, errors (default)." << std::endl;
        std::cerr << "  --warn     Show only warnings and errors." << std::endl;
        return 1;
    }

    std::string inputFile = argv[1];

    // Set default log level
    Logger::setLevel(LogLevel::WARNING);

    // Parse log level argument
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--verbose" || arg == "--debug") {
            Logger::setLevel(LogLevel::DEBUG);
        } else if (arg == "--info") {
            Logger::setLevel(LogLevel::INFO);
        } else if (arg == "--warn") {
            Logger::setLevel(LogLevel::WARNING);
        } else {
            std::cerr << "Warning: Unknown argument '" << arg << "' ignored." << std::endl;
        }
    }

    // Start timing
    auto startTime = std::chrono::high_resolution_clock::now();
    int exitCode = 1; // Default to failure

    try {
        LOGI << "Starting CATLHasher for file: " << inputFile;

        // CATLHasher constructor might throw if file is invalid
        CATLHasher hasher(inputFile);

        // processFile handles its own logging and returns success/failure
        if (hasher.processFile()) {
            exitCode = 0; // Success
        } else {
            LOGE << "CATL file processing reported errors.";
            exitCode = 1; // Failure
        }
    } catch (const std::exception &e) {
        // Catch errors during CATLHasher construction (e.g., file not found)
        LOGE << "Initialization failed: " << e.what();
        exitCode = 1; // Failure
    } catch (...) {
        // Catch any other unexpected exceptions
        LOGE << "Caught unknown exception during processing.";
        exitCode = 1; // Failure
    }


    // Calculate and display execution time
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    LOGW << "Execution finished in " << duration.count() / 1000.0
            << " seconds (" << duration.count() << " ms). Exit code: " << exitCode;

    return exitCode;
}
