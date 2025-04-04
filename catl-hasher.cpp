#include <iostream>
#include <fstream>
#include <vector>
#include <array>
#include <memory>
#include <string>
#include <stdexcept>
#include <map>
#include <chrono>
#include <iomanip>

// For memory mapping
#include <boost/iostreams/device/mapped_file.hpp>
#include <boost/filesystem.hpp>

// For crypto
#include <openssl/evp.h>

#include "Logger.h"


// Macro for logging hashes efficiently (only formats if DEBUG is enabled)
#define LOGD_HASH(label, hash_obj)  if(Logger::getLevel() >= LogLevel::DEBUG) Logger::logWithFormat(LogLevel::DEBUG, \
                                            [](const std::string& lbl, const Hash256& h) { return lbl + h.toString(); }, label, hash_obj)
#define LOGD_KEY(label, key_obj)    if(Logger::getLevel() >= LogLevel::DEBUG) Logger::logWithFormat(LogLevel::DEBUG, \
                                            [](const std::string& lbl, const Key& k) { return lbl + k.toString(); }, label, key_obj)


// --- End of Logger Implementation ---


static constexpr uint32_t CATL = 0x4C544143UL; // "CATL" in LE
static constexpr uint16_t CATALOGUE_VERSION_MASK = 0x00FF;
static constexpr uint16_t CATALOGUE_COMPRESS_LEVEL_MASK = 0x0F00;

// TODO: reuse this
constexpr std::uint32_t
make_hash_prefix(char a, char b, char c) {
    return (static_cast<std::uint32_t>(a) << 24) +
           (static_cast<std::uint32_t>(b) << 16) +
           (static_cast<std::uint32_t>(c) << 8);
}

// Hash prefixes from rippled
namespace HashPrefix {
    // TODO: just use std::uint32_t enum but need to handle endian flip
    // when passing to hasher
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
    // TODO: tnUPDATE ? It's context sensitive anyway
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
    std::array<uint8_t, 64> hash; // Note: This hash is usually unused/zero in practice
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
    result.reserve(sl.size() * 2);
    const uint8_t *bytes = sl.data();
    for (size_t i = 0; i < sl.size(); ++i) {
        uint8_t byte = bytes[i];
        result.push_back(hexChars[(byte >> 4) & 0xF]);
        result.push_back(hexChars[byte & 0xF]);
    }
}

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

    [[nodiscard]] std::string hex() const {
        std::string result;
        slice_hex({data(), size()}, result);
        return result;
    }
};

class Key {
private:
    const std::uint8_t *data_;

public:
    Key(const std::uint8_t *data) : data_(data) {
    }

    const std::uint8_t *data() const { return data_; }
    static constexpr std::size_t size() { return 32; }
    Hash256 toHash() const { return Hash256(data_); }
    std::string toString() const { return toHash().hex(); }
    bool operator==(const Key &other) const { return std::memcmp(data_, other.data_, 32) == 0; }
    bool operator!=(const Key &other) const { return !(*this == other); }
};

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

class SHAMapTreeNode;
class SHAMapInnerNode;
class SHAMapLeafNode;
class SHAMap;

int selectBranch(const Key &key, int depth) {
    int byteIdx = depth / 2;
    if (byteIdx < 0 || byteIdx >= static_cast<int>(Key::size())) {
        throw InvalidDepthException(depth, Key::size());
    }
    int nibbleIdx = depth % 2;
    uint8_t byte_val = key.data()[byteIdx];
    return (nibbleIdx == 0) ? (byte_val >> 4) & 0xF : byte_val & 0xF;
}

// SHAMapTreeNode
class SHAMapTreeNode {
protected:
    Hash256 hash;
    bool hashValid = false;

public:
    void invalidateHash() { hashValid = false; }

    virtual ~SHAMapTreeNode() = default;

    virtual bool isLeaf() const = 0;

    virtual bool isInner() const = 0;

    virtual void updateHash() = 0; // NO LOGGING INSIDE IMPLEMENTATIONS
    const Hash256 &getHash() {
        if (!hashValid) {
            updateHash();
            hashValid = true;
        }
        return hash;
    }
};

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
                hashData = children[i]->getHash().data(); // Recursive call might trigger update
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
        if (branch < 0 || branch >= 16) { throw InvalidBranchException(branch); }
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
        invalidateHash(); // Mark self as invalid, not children
        return true;
    }

    std::shared_ptr<SHAMapTreeNode> getChild(int branch) const {
        if (branch < 0 || branch >= 16) { throw InvalidBranchException(branch); }
        return children[branch];
    }

    bool hasChild(int branch) const {
        if (branch < 0 || branch >= 16) { throw InvalidBranchException(branch); }
        return (branchMask & (1 << branch)) != 0;
    }

    int getBranchCount() const {
        int count = 0;
        for (int i = 0; i < 16; i++) { if (hasChild(i)) count++; }
        return count;
    }

    uint16_t getBranchMask() const { return branchMask; }

    std::shared_ptr<SHAMapLeafNode> getOnlyChildLeaf() const {
        std::shared_ptr<SHAMapLeafNode> resultLeaf = nullptr;
        int leafCount = 0;
        for (const std::shared_ptr<SHAMapTreeNode> &childNodePtr: children) {
            if (childNodePtr) {
                if (childNodePtr->isInner()) { return nullptr; } // Found inner node
                leafCount++;
                if (leafCount == 1) {
                    resultLeaf = std::static_pointer_cast<SHAMapLeafNode>(childNodePtr);
                } else {
                    return nullptr; // Found more than one leaf
                }
            }
        }
        return resultLeaf; // Returns the leaf if exactly one found, else nullptr
    }
};

class SHAMapLeafNode : public SHAMapTreeNode {
private:
    std::shared_ptr<MmapItem> item;
    SHAMapNodeType type;

public:
    SHAMapLeafNode(std::shared_ptr<MmapItem> i, SHAMapNodeType t)
        : item(std::move(i)), type(t) { if (!item) { throw NullItemException(); } }

    bool isLeaf() const override { return true; }
    bool isInner() const override { return false; }

    void updateHash() override {
        std::array<unsigned char, 4> prefix = {0, 0, 0, 0};
        auto set = [&prefix](auto &from) { std::memcpy(prefix.data(), from.data(), 4); };
        switch (type) {
            case tnTRANSACTION_NM:
            case tnTRANSACTION_MD: set(HashPrefix::txNode);
                break;
            case tnACCOUNT_STATE: default: set(HashPrefix::leafNode);
                break;
        }
        EVP_MD_CTX *ctx = EVP_MD_CTX_new();
        if (ctx == nullptr) { throw HashCalculationException("Failed to create EVP_MD_CTX"); }
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

// PathFinder
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
        if (!root) { throw NullNodeException("PathFinder: null root node"); }
        searchRoot = root;
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
                break;
            }
            if (child->isLeaf()) {
                terminalBranch = branch;
                inners.push_back(currentInner);
                foundLeaf = std::static_pointer_cast<SHAMapLeafNode>(child);
                if (foundLeaf->getItem()) {
                    leafKeyMatches = (foundLeaf->getItem()->key() == targetKey);
                } else { throw NullItemException(); }
                break;
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

    void dirtyPath() const {
        for (auto &inner: inners) { inner->invalidateHash(); }
    }

    void collapsePath() {
        if (inners.size() <= 1) return;
        std::shared_ptr<SHAMapLeafNode> onlyChild = nullptr;
        auto innermost = inners.back();
        onlyChild = innermost->getOnlyChildLeaf();
        for (int i = static_cast<int>(inners.size()) - 2; i >= 0; --i) {
            auto inner = inners[i];
            int branch = branches[i];
            if (onlyChild) { inner->setChild(branch, onlyChild); }
            onlyChild = inner->getOnlyChildLeaf();
            if (!onlyChild) break;
        }
    }
};


// SHAMap class (Integrate Logging for errors/debug)
class SHAMap {
private:
    std::shared_ptr<SHAMapInnerNode> root;
    SHAMapNodeType nodeType;

public:
    SHAMap(SHAMapNodeType type = tnACCOUNT_STATE) : nodeType(type) {
        root = std::make_shared<SHAMapInnerNode>(0); // Root has depth 0
        LOGD("SHAMap created with type: ", static_cast<int>(type));
    }

    // Added for completeness, might not be used externally in this tool
    Hash256 getChildHash(int ix) {
        try {
            if (!root) return Hash256::zero();
            auto child = root->getChild(ix);
            if (child) {
                return child->getHash();
            } else {
                return Hash256::zero();
            }
        } catch (const InvalidBranchException &e) {
            LOGW("Attempted to get child hash for invalid branch ", ix, " from root: ", e.what());
            return Hash256::zero();
        }
    }

    // TODO: add a custom return type for this, to distinguish between adding and updating
    // fail/added/updated <- return type, need more than just bool, need a tribool
    bool addItem(std::shared_ptr<MmapItem> &item, bool allowUpdate = true) {
        if (!item) {
            LOGW("Attempted to add null item to SHAMap.");
            return false;
        }
        LOGD_KEY("Attempting to add item with key: ", item->key());

        try {
            PathFinder pathFinder(root, item->key());

            if (pathFinder.endedAtNullBranch() || (
                    pathFinder.hasLeaf() && pathFinder.didLeafKeyMatch() && allowUpdate)) {
                auto parent = pathFinder.getParentOfTerminal();
                int branch = pathFinder.getTerminalBranch();
                if (!parent) { throw NullNodeException("addItem: null parent node (should be root)"); }

                LOGD("Adding/Updating leaf at depth ", parent->getDepth() + 1, " branch ", branch);
                auto newLeaf = std::make_shared<SHAMapLeafNode>(item, nodeType);
                parent->setChild(branch, newLeaf);
                pathFinder.dirtyPath();
                return true;
            }

            if (pathFinder.hasLeaf() && !pathFinder.didLeafKeyMatch()) {
                LOGD_KEY("Handling collision for key: ", item->key());
                auto parent = pathFinder.getParentOfTerminal();
                int branch = pathFinder.getTerminalBranch();
                if (!parent) { throw NullNodeException("addItem collision: null parent node (should be root)"); }
                auto existingLeaf = pathFinder.getLeafMutable();
                auto existingItem = existingLeaf->getItem();
                if (!existingItem) { throw NullItemException(); /* Should be caught by leaf constructor */ }

                std::shared_ptr<SHAMapInnerNode> currentParent = parent;
                int currentBranch = branch;
                uint8_t currentDepth = parent->getDepth() + 1; // Start depth below parent

                // Create first new inner node to replace the leaf
                auto newInner = std::make_shared<SHAMapInnerNode>(currentDepth);
                parent->setChild(currentBranch, newInner);
                currentParent = newInner;

                while (currentDepth < 64) {
                    // Max depth check
                    int existingBranch = selectBranch(existingItem->key(), currentDepth);
                    int newBranch = selectBranch(item->key(), currentDepth);

                    if (existingBranch != newBranch) {
                        LOGD("Collision resolved at depth ", currentDepth, ". Placing leaves at branches ",
                             existingBranch, " and ", newBranch);
                        auto newLeaf = std::make_shared<SHAMapLeafNode>(item, nodeType);
                        currentParent->setChild(existingBranch, existingLeaf);
                        currentParent->setChild(newBranch, newLeaf);
                        break; // Done
                    } else {
                        // Collision continues, create another inner node
                        LOGD("Collision continues at depth ", currentDepth, ", branch ", existingBranch,
                             ". Descending further.");
                        auto nextInner = std::make_shared<SHAMapInnerNode>(currentDepth + 1);
                        currentParent->setChild(existingBranch, nextInner);
                        currentParent = nextInner;
                        currentDepth++;
                    }
                }
                if (currentDepth >= 64) {
                    throw SHAMapException("Maximum SHAMap depth reached during collision resolution for key: " +
                                          item->key().toString());
                }

                pathFinder.dirtyPath();
                return true;
            }

            // Should ideally not be reached if PathFinder logic is correct
            LOGE("Unexpected state in addItem for key: ", item->key().toString(), ". PathFinder logic error?");
            throw SHAMapException("Unexpected state in addItem - PathFinder logic error");
        } catch (const SHAMapException &e) {
            LOGE("Error adding item with key ", item->key().toString(), ": ", e.what());
            return false;
        }
        catch (const std::exception &e) {
            LOGE("Standard exception adding item with key ", item->key().toString(), ": ", e.what());
            return false;
        }
    }

    bool removeItem(const Key &key) {
        LOGD_KEY("Attempting to remove item with key: ", key);
        try {
            PathFinder pathFinder(root, key);

            if (!pathFinder.hasLeaf() || !pathFinder.didLeafKeyMatch()) {
                LOGD_KEY("Item not found for removal, key: ", key);
                return false; // Item not found
            }

            auto parent = pathFinder.getParentOfTerminal();
            int branch = pathFinder.getTerminalBranch();
            if (!parent) { throw NullNodeException("removeItem: null parent node (should be root)"); }

            LOGD("Removing leaf at depth ", parent->getDepth() + 1, " branch ", branch);
            parent->setChild(branch, nullptr); // Remove the leaf
            pathFinder.dirtyPath();
            pathFinder.collapsePath(); // Compress path if possible
            LOGD_KEY("Item removed successfully, key: ", key);
            return true;
        } catch (const SHAMapException &e) {
            LOGE("Error removing item with key ", key.toString(), ": ", e.what());
            return false;
        }
        catch (const std::exception &e) {
            LOGE("Standard exception removing item with key ", key.toString(), ": ", e.what());
            return false;
        }
    }

    Hash256 getHash() const {
        if (!root) {
            LOGW("Attempting to get hash of a null root SHAMap.");
            return Hash256::zero();
        }
        // getHash() inside the node handles lazy calculation
        return root->getHash();
    }
};

// timeToString
std::string format_ripple_time(uint64_t netClockTime) {
    static constexpr time_t rippleEpochOffset = 946684800;
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

    // Maps for tracking state
    SHAMap stateMap;
    SHAMap txMap;

    // Statistics
    struct Stats {
        uint32_t ledgersProcessed = 0;
        uint32_t stateNodesAdded = 0;
        uint32_t txNodesAdded = 0;
        uint32_t stateRemovalsAttempted = 0;
        uint32_t stateRemovalsSucceeded = 0;
        uint32_t successfulHashVerifications = 0;
        uint32_t failedHashVerifications = 0;
        size_t currentOffset = 0;
    } stats;

    bool validateHeader() {
        stats.currentOffset = 0;
        if (fileSize < sizeof(CATLHeader)) {
            LOGE("File too small (", fileSize, " bytes) to contain a valid CATL header (", sizeof(CATLHeader),
                 " bytes)");
            return false;
        }

        std::memcpy(&header, data, sizeof(CATLHeader));
        stats.currentOffset = sizeof(CATLHeader);

        if (header.magic != CATL) {
            // Use std::hex manipulator directly in the log call
            std::ostringstream oss_magic;
            oss_magic << "Invalid magic value: expected 0x" << std::hex << CATL << ", got 0x" << header.magic <<
                    std::dec;
            LOGE(oss_magic.str());
            return false;
        }

        uint8_t compressionLevel = (header.version & CATALOGUE_COMPRESS_LEVEL_MASK) >> 8;
        if (compressionLevel != 0) {
            LOGE("Compressed CATL files are not supported. Compression level: ",
                 static_cast<int>(compressionLevel));
            return false;
        }

        // Log header info at INFO level
        LOGI("CATL Header Validated:");
        // Use std::hex manipulator for magic value display
        std::ostringstream oss_magic_info;
        oss_magic_info << "  Magic: 0x" << std::hex << header.magic << std::dec;
        LOGI(oss_magic_info.str());
        LOGI("  Ledger range: ", header.min_ledger, " - ", header.max_ledger);
        LOGI("  Version: ", (header.version & CATALOGUE_VERSION_MASK)); // Mask out compression bits
        LOGI("  Network ID: ", header.network_id);
        LOGI("  Header Filesize: ", header.filesize, " bytes"); // Note: Compare with actual later

        return true;
    }

    // Unified map processing function
    size_t processMap(size_t offset, SHAMap &map, uint32_t &nodesProcessedCount, bool isStateMap = false) {
        nodesProcessedCount = 0;
        bool foundTerminal = false;
        const std::string mapTypeName = isStateMap ? "state" : "transaction";
        size_t startOffset = offset;

        LOGD("Starting processing of ", mapTypeName, " map data at offset ", offset);

        while (offset < fileSize && !foundTerminal) {
            stats.currentOffset = offset; // Update global offset for error reporting

            // Read node type - check bounds first
            if (offset >= fileSize) {
                LOGE("Unexpected EOF reading node type in ", mapTypeName, " map at offset ", offset);
                return startOffset; // Return original offset on error
            }
            uint8_t nodeTypeVal = data[offset++];
            SHAMapNodeType nodeType = static_cast<SHAMapNodeType>(nodeTypeVal);

            if (nodeType == tnTERMINAL) {
                LOGD("Found terminal marker for ", mapTypeName, " map at offset ", offset -1);
                foundTerminal = true;
                break; // Exit loop successfully
            }

            // Validate node type BEFORE reading key/data
            if (nodeType != tnINNER && nodeType != tnTRANSACTION_NM &&
                nodeType != tnTRANSACTION_MD && nodeType != tnACCOUNT_STATE &&
                nodeType != tnREMOVE) {
                LOGE("Invalid node type encountered: ", static_cast<int>(nodeTypeVal), " in ", mapTypeName,
                     " map at offset ", offset - 1);
                return startOffset; // Indicate error by returning original offset
            }


            // Read key (32 bytes) - check bounds
            if (offset + Key::size() > fileSize) {
                LOGE("Unexpected EOF reading key (", Key::size(), " bytes) in ", mapTypeName,
                     " map. Current offset: ", offset, ", File size: ", fileSize);
                return startOffset;
            }
            const uint8_t *keyData = data + offset;
            Key itemKey(keyData); // Create Key object early for logging
            offset += Key::size();

            // Handle removal (only expected in state maps)
            if (nodeType == tnREMOVE) {
                if (isStateMap) {
                    LOGD_KEY("Processing tnREMOVE for key: ", itemKey);
                    stats.stateRemovalsAttempted++;
                    if (map.removeItem(itemKey)) {
                        stats.stateRemovalsSucceeded++;
                        nodesProcessedCount++;
                    } else {
                        // Log warning if removal failed (item might not have existed)
                        LOGE("Failed to remove state item (may not exist), key: ", itemKey.toString());
                        return startOffset; // error
                    }
                } else {
                    LOGW("Found unexpected tnREMOVE node in transaction map at offset ", offset - 1 - Key::size(),
                         " for key: ", itemKey.toString());
                    return startOffset; // error
                }
                continue; // Move to the next node
            }

            // Read data size (4 bytes) - check bounds
            if (offset + sizeof(uint32_t) > fileSize) {
                LOGE("Unexpected EOF reading data size (", sizeof(uint32_t), " bytes) in ", mapTypeName,
                     " map. Current offset: ", offset, ", File size: ", fileSize);
                return startOffset;
            }
            uint32_t dataSize = 0;
            std::memcpy(&dataSize, data + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);

            // Validate data size (check against remaining file size and sanity check)
            // Allow zero size data (e.g. some placeholder nodes)
            constexpr uint32_t MAX_REASONABLE_DATA_SIZE = 5 * 1024 * 1024; // 5 MiB sanity limit
            if (offset > fileSize || (dataSize > 0 && offset + dataSize > fileSize) || dataSize >
                MAX_REASONABLE_DATA_SIZE) {
                LOGE("Invalid data size (", dataSize, " bytes) or EOF reached in ", mapTypeName, " map. Offset: ",
                     offset, ", Remaining bytes: ", (fileSize > offset ? fileSize - offset : 0), ", File size: ",
                     fileSize);
                LOGD_KEY("Error occurred processing node with key: ", itemKey);
                return startOffset;
            }

            // Create MmapItem (zero-copy reference)
            const uint8_t *itemDataPtr = data + offset;
            auto item = std::make_shared<MmapItem>(keyData, itemDataPtr, dataSize);

            // Add item to the appropriate map
            if (map.addItem(item)) {
                // addItem handles logging internally now
                nodesProcessedCount++;
            } else {
                // addItem already logs errors, but we might want a higher level error here
                LOGE("Failed to add item from ", mapTypeName, " map to SHAMap, key: ", itemKey.toString(),
                     " at offset ", stats.currentOffset);
                // Consider if processing should stop here. Returning startOffset indicates failure.
                return startOffset;
            }

            // Advance offset past the data
            offset += dataSize;
        } // End while loop

        if (!foundTerminal) {
            LOGW("Processing ", mapTypeName,
                 " map ended without finding a terminal marker (tnTERMINAL). Reached offset ", offset);
            // This might be okay if it's the end of the file, or an error otherwise.
            if (offset < fileSize) {
                LOGE("Map processing stopped prematurely before EOF and without terminal marker. Offset: ",
                     offset);
                return startOffset; // Indicate error
            }
        }

        LOGD("Finished processing ", mapTypeName, " map. Processed ", nodesProcessedCount,
             " nodes. Final offset: ", offset);
        return offset; // Return the new offset after successful processing
    }


    // Process a single ledger
    size_t processLedger(size_t offset) {
        stats.currentOffset = offset;
        size_t initialOffset = offset;

        // Check bounds for LedgerInfo
        if (offset + sizeof(LedgerInfo) > fileSize) {
            LOGE("Not enough data remaining (", (fileSize > offset ? fileSize - offset : 0),
                 " bytes) for LedgerInfo structure (", sizeof(LedgerInfo), " bytes) at offset ", offset);
            return initialOffset; // Return original offset on error
        }

        LedgerInfo info;
        std::memcpy(&info, data + offset, sizeof(LedgerInfo));
        offset += sizeof(LedgerInfo);
        stats.currentOffset = offset;

        // Sanity check ledger sequence
        if (info.sequence < header.min_ledger || info.sequence > header.max_ledger) {
            LOGW("Ledger sequence ", info.sequence, " is outside the expected range [", header.min_ledger, ", ",
                 header.max_ledger, "] specified in the header.");
        }

        LOGI("--- Processing Ledger ", info.sequence, " ---");
        LOGI("  Ledger Hash:      ", Hash256(info.hash).hex()); // Using efficient macro
        LOGI("  Parent Hash:      ", Hash256(info.parentHash).hex());
        LOGI("  AccountState Hash:", Hash256(info.accountHash).hex());
        LOGI("  Transaction Hash: ", Hash256(info.txHash).hex());
        LOGI("  Close Time:       ", format_ripple_time(info.closeTime));
        LOGI("  Drops:            ", info.drops);
        LOGI("  Close Flags:      ", info.closeFlags);
        LOGI("  Offset at start:  ", initialOffset);


        // Process Account State Map
        bool isFirstLedger = (info.sequence == header.min_ledger);
        if (isFirstLedger) {
            LOGI("Initializing new State SHAMap for first ledger ", info.sequence);
            stateMap = SHAMap(tnACCOUNT_STATE); // Recreate for the first ledger
        } else {
            LOGI("Processing State Map delta for ledger ", info.sequence);
            // stateMap persists from previous ledger
        }

        uint32_t stateNodesProcessed = 0;
        size_t stateMapEndOffset = processMap(offset, stateMap, stateNodesProcessed, true); // true = isStateMap
        if (stateMapEndOffset == offset && stateNodesProcessed == 0 && offset != fileSize) {
            // Check if no progress was made and not EOF
            LOGE("Error processing state map data for ledger ", info.sequence, ". No progress made from offset ",
                 offset);
            return initialOffset; // Return original offset to signal failure
        }
        offset = stateMapEndOffset;
        stats.currentOffset = offset;
        stats.stateNodesAdded += stateNodesProcessed; // Accumulate stats
        LOGI("  State map processing finished. Nodes processed in this ledger: ", stateNodesProcessed,
             ". New offset: ", offset);


        // Process Transaction Map (always created fresh for each ledger)
        LOGI("Processing Transaction Map for ledger ", info.sequence);
        txMap = SHAMap(tnTRANSACTION_MD); // Create fresh transaction map
        uint32_t txNodesProcessed = 0;
        size_t txMapEndOffset = processMap(offset, txMap, txNodesProcessed, false); // false = not isStateMap
        if (txMapEndOffset == offset && txNodesProcessed == 0 && offset != fileSize) {
            LOGE("Error processing transaction map data for ledger ", info.sequence,
                 ". No progress made from offset ", offset);
            return initialOffset; // Signal failure
        }
        offset = txMapEndOffset;
        stats.currentOffset = offset;
        stats.txNodesAdded += txNodesProcessed; // Accumulate stats
        LOGI("  Transaction map processing finished. Nodes processed: ", txNodesProcessed,
             ". Final offset for ledger: ", offset);

        // Verify Hashes
        LOGI("Verifying map hashes for ledger ", info.sequence);
        verifyMapHash(stateMap, Hash256(info.accountHash), "AccountState", info.sequence);
        verifyMapHash(txMap, Hash256(info.txHash), "Transaction", info.sequence);

        stats.ledgersProcessed++;
        // LOG_INFO("--- Completed Ledger ", info.sequence, " ---");
        return offset; // Return the final offset for this ledger
    }

    // Helper to verify map hashes
    void verifyMapHash(const SHAMap &map, const Hash256 &expectedHash,
                       const std::string &mapType, uint32_t ledgerSeq) {
        Hash256 computedHash = map.getHash(); // getHash is const
        bool hashMatch = (computedHash == expectedHash);

        if (!hashMatch) {
            LOGW("HASH MISMATCH for ", mapType, " map in ledger ", ledgerSeq, "!");
            // Log details only at DEBUG level for performance
            if (Logger::getLevel() >= LogLevel::DEBUG) {
                LOGD("  Computed Hash: ", computedHash.hex());
                LOGD("  Expected Hash: ", expectedHash.hex());
            }
            stats.failedHashVerifications++;
        } else {
            LOGI("  ", mapType, " hash verified successfully for ledger ", ledgerSeq);
            stats.successfulHashVerifications++;
        }
    }

public:
    // Constructor uses initializer list and handles file opening errors
    CATLHasher(const std::string &filename)
        : stateMap(tnACCOUNT_STATE), // Initialize maps here
          txMap(tnTRANSACTION_MD) {
        LOGI("Attempting to open and map file: ", filename);
        try {
            if (!boost::filesystem::exists(filename)) {
                throw std::runtime_error("File does not exist: " + filename);
            }

            boost::filesystem::path path(filename);
            boost::uintmax_t actualFileSize = boost::filesystem::file_size(path);
            if (actualFileSize == 0) {
                throw std::runtime_error("File is empty: " + filename);
            }

            mmapFile.open(filename);
            if (!mmapFile.is_open()) {
                throw std::runtime_error("Boost failed to memory map file: " + filename);
            }

            data = reinterpret_cast<const uint8_t *>(mmapFile.data());
            fileSize = mmapFile.size(); // Get size from boost map

            if (fileSize != actualFileSize) {
                LOGW("Memory mapped size (", fileSize, ") differs from filesystem size (", actualFileSize,
                     "). Using mapped size.");
            }

            if (!data) {
                // This case should ideally be caught by mmapFile.is_open()
                throw std::runtime_error("Memory mapping succeeded but data pointer is null.");
            }

            LOGI("File mapped successfully: ", filename, " (", fileSize, " bytes)");
        } catch (const boost::filesystem::filesystem_error &e) {
            LOGE("Boost Filesystem error opening file '", filename, "': ", e.what());
            throw; // Re-throw to be caught by main
        }
        catch (const std::ios_base::failure &e) {
            LOGE("Boost IOStreams error mapping file '", filename, "': ", e.what());
            throw; // Re-throw
        }
        catch (const std::exception &e) {
            LOGE("Error during file setup '", filename, "': ", e.what());
            throw; // Re-throw standard exceptions
        }
    }

    // Destructor (optional, Boost handles unmapping)
    ~CATLHasher() {
        LOGD("CATLHasher destroyed, Boost will unmap the file.");
        if (mmapFile.is_open()) {
            mmapFile.close();
        }
    }


    bool processFile() {
        LOGI("Starting CATL file processing...");
        try {
            if (!data || fileSize == 0) {
                LOGE("No data available to process. File not mapped correctly?");
                return false;
            }

            if (!validateHeader()) {
                LOGE("CATL header validation failed. Aborting processing.");
                return false;
            }

            // Compare actual file size with header filesize
            if (header.filesize != fileSize) {
                LOGW("File size mismatch: Header reports ", header.filesize, " bytes, actual mapped size is ",
                     fileSize, " bytes. Processing based on actual size.");
                // Processing will continue, but this might indicate truncation or corruption.
            }

            // Process ledgers starting after the header
            size_t currentFileOffset = sizeof(CATLHeader);
            uint32_t expectedLedgerCount = (header.max_ledger - header.min_ledger + 1);
            LOGI("Expecting ", expectedLedgerCount, " ledgers in this file.");

            while (currentFileOffset < fileSize) {
                // Check if we might be reading padding or garbage at the end
                if (fileSize - currentFileOffset < sizeof(LedgerInfo)) {
                    LOGW("Only ", (fileSize - currentFileOffset), " bytes remaining, less than LedgerInfo size (",
                         sizeof(LedgerInfo), "). Assuming end of meaningful data at offset ", currentFileOffset);
                    break;
                }

                size_t nextOffset = processLedger(currentFileOffset);

                if (nextOffset == currentFileOffset) {
                    // No progress was made, indicates an error in processLedger
                    LOGE("Processing stalled at offset ", currentFileOffset, ". Error likely occurred in ledger ",
                         (stats.ledgersProcessed > 0 ? header.min_ledger + stats.ledgersProcessed : header.
                             min_ledger));
                    return false; // Abort processing
                }
                if (nextOffset < currentFileOffset) {
                    // This should ideally not happen, defensive check
                    LOGE("Offset went backwards from ", currentFileOffset, " to ", nextOffset, ". Aborting.");
                    return false;
                }
                currentFileOffset = nextOffset;
            }

            // Check if we processed up to the expected end of the file
            if (currentFileOffset != fileSize) {
                LOGW("Processing finished at offset ", currentFileOffset, " but file size is ", fileSize,
                     ". Potential trailing data or incomplete processing.");
            } else {
                LOGI("Processing reached the end of the mapped file (offset ", currentFileOffset, ").");
            }


            // Final Summary using INFO level
            LOGI("--- Processing Summary ---");
            LOGI("Ledgers processed:      ", stats.ledgersProcessed, " (Expected: ", expectedLedgerCount, ")");
            if (stats.ledgersProcessed != expectedLedgerCount) {
                LOGW("Mismatch between processed ledgers and expected count based on header range.");
            }
            LOGI("State map nodes added:  ", stats.stateNodesAdded);
            if (stats.stateRemovalsAttempted > 0 || stats.stateRemovalsSucceeded > 0) {
                LOGI("State map removals:   ", stats.stateRemovalsSucceeded, " succeeded out of ",
                     stats.stateRemovalsAttempted, " attempts");
            }
            LOGI("Transaction nodes added:", stats.txNodesAdded);
            LOGI("Hash Verifications:   ", stats.successfulHashVerifications, " Succeeded, ",
                 stats.failedHashVerifications, " Failed");
            LOGI("--- End Summary ---");

            // Return true if processing completed, potentially with warnings/hash failures
            // Return false only if a fatal error occurred preventing continuation.
            // Consider hash failures fatal? For this tool, maybe not, just report them.
            return true;
        } catch (const SHAMapException &e) {
            LOGE("Aborting due to SHAMap error at offset ~", stats.currentOffset, ": ", e.what());
            return false;
        } catch (const std::exception &e) {
            LOGE("Aborting due to standard error at offset ~", stats.currentOffset, ": ", e.what());
            return false;
        }
        catch (...) {
            LOGE("Aborting due to unknown exception at offset ~", stats.currentOffset);
            return false;
        }
    }
};


// Main function updated for Logger control
int main(int argc, char *argv[]) {
    if (argc < 2) {
        // Use std::cerr directly for usage message as Logger might not be configured yet
        std::cerr << "Usage: " << argv[0] << " <catalogue_file> [--level <level>]" << std::endl;
        std::cerr << "  <catalogue_file>: Path to the CATL file." << std::endl;
        std::cerr << "  --level <level>: Set log verbosity (optional)." << std::endl;
        std::cerr << "     Levels: error, warn, info (default), debug" << std::endl;
        std::cerr << "\nProcesses CATL files, builds SHAMaps, verifies hashes." << std::endl;
        return 1;
    }

    std::string inputFile = argv[1];
    LogLevel desiredLevel = LogLevel::INFO; // Default level

    // Parse command line arguments for log level
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--level" && i + 1 < argc) {
            std::string levelStr = argv[++i];
            if (levelStr == "error" || levelStr == "ERROR") {
                desiredLevel = LogLevel::ERROR;
            } else if (levelStr == "warn" || levelStr == "WARN" || levelStr == "warning" || levelStr == "WARNING") {
                desiredLevel = LogLevel::WARNING;
            } else if (levelStr == "info" || levelStr == "INFO") {
                desiredLevel = LogLevel::INFO;
            } else if (levelStr == "debug" || levelStr == "DEBUG") {
                desiredLevel = LogLevel::DEBUG;
            } else {
                std::cerr << "Warning: Unknown log level '" << levelStr << "'. Using default (info)." << std::endl;
            }
        }
        // Deprecated verbose flags (map to debug for backward compatibility)
        else if (arg == "--verbose" || arg == "--debug") {
            desiredLevel = LogLevel::DEBUG;
            std::cerr << "Warning: --verbose/--debug flags are deprecated. Use '--level debug'." << std::endl;
        } else {
            std::cerr << "Warning: Unknown argument '" << arg << "'." << std::endl;
        }
    }

    // Set the logger level *before* creating CATLHasher
    Logger::setLevel(desiredLevel);

    // Start timing
    auto startTime = std::chrono::high_resolution_clock::now();
    int exitCode = 1; // Default to failure

    try {
        // Pass only filename, verbose removed
        CATLHasher hasher(inputFile);
        bool result = hasher.processFile();
        exitCode = result ? 0 : 1; // 0 on success, 1 on failure
    } catch (const std::exception &e) {
        // Catch errors during CATLHasher construction (e.g., file not found)
        // Logger might already be set, so use it. If not, cerr is fallback.
        LOGE("Fatal error during initialization: ", e.what());
        exitCode = 1;
    }
    catch (...) {
        LOGE("Caught unknown fatal error during initialization.");
        exitCode = 1;
    }

    // Calculate and display execution time using Logger
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
    double seconds = duration.count() / 1000.0;

    // Use fixed/setprecision for consistent output format
    std::ostringstream timeOSS;
    timeOSS << "Execution completed in " << std::fixed << std::setprecision(3) << seconds
            << " seconds (" << duration.count() << " ms)";
    LOGW(timeOSS.str());


    return exitCode;
}
