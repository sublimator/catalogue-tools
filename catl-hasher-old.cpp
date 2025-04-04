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
#include <openssl/sha.h>

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
    // /** transaction plus signature to give transaction ID */
    // transactionID = make_hash_prefix('T', 'X', 'N'),
    //
    // /** transaction plus metadata */
    // txNode = make_hash_prefix('S', 'N', 'D'),
    constexpr std::array<unsigned char, 4> txNode = {'S', 'N', 'D', 0x00};

    //
    // /** account state */
    // leafNode = make_hash_prefix('M', 'L', 'N'),
    constexpr std::array<unsigned char, 4> leafNode = {'M', 'L', 'N', 0x00};
    //
    // /** inner node in V1 tree */
    constexpr std::array<unsigned char, 4> innerNode = {'M', 'I', 'N', 0x00};
    // innerNode = make_hash_prefix('M', 'I', 'N'),
    //
    // /** ledger master data for signing */
    // ledgerMaster = make_hash_prefix('L', 'W', 'R'),
    //
    // /** inner transaction to sign */
    // txSign = make_hash_prefix('S', 'T', 'X'),
    //
    // /** inner transaction to multi-sign */
    // txMultiSign = make_hash_prefix('S', 'M', 'T'),
    //
    // /** validation for signing */
    // validation = make_hash_prefix('V', 'A', 'L'),
    //
    // /** proposal for signing */
    // proposal = make_hash_prefix('P', 'R', 'P'),
    //
    // /** Manifest */
    // manifest = make_hash_prefix('M', 'A', 'N'),
    //
    // /** Payment Channel Claim */
    // paymentChannelClaim = make_hash_prefix('C', 'L', 'M'),
    //
    // /** shard info for signing */
    // shardInfo = make_hash_prefix('S', 'H', 'D'),
    //
    // /** Emit Transaction Nonce */
    // emitTxnNonce = make_hash_prefix('E', 'T', 'X'),
    //
    // /** Random entropy for hook developers to use */
    // hookNonce = make_hash_prefix('N', 'C', 'E'),
    //
    // /* Hash of a Hook's actual code */
    // hookDefinition = make_hash_prefix('W', 'S', 'M')
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

    std::array<uint8_t, 32> &asArray() {
        return data_;
    }

    const std::array<uint8_t, 32> &asArray() const {
        return data_;
    }

    static constexpr std::size_t size() {
        return 32;
    }

    static Hash256 zero() {
        return {};
    }

    [[nodiscard]] bool isZero() const {
        for (auto b: data_) {
            if (b != 0) return false;
        }
        return true;
    }

    [[nodiscard]] std::string hex() const {
        static auto hexChars = "0123456789abcdef";
        std::string result;
        result.reserve(64);

        for (const auto b: data_) {
            result.push_back(hexChars[(b >> 4) & 0xF]);
            result.push_back(hexChars[b & 0xF]);
        }
        return result;
    }

    bool operator==(const Hash256 &other) const {
        return data_ == other.data_;
    }

    bool operator!=(const Hash256 &other) const {
        return !(*this == other);
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

public:
    bool isLeaf() const override { return false; }
    bool isInner() const override { return true; }

    void updateHash() override {
        // Compute hash of inner node
        if (branchMask == 0) {
            hash = Hash256::zero();
            hashValid = true;
            return;
        }

        SHA512_CTX ctx;
        SHA512_Init(&ctx);

        // Add HashPrefix::innerNode
        auto prefix = HashPrefix::innerNode;
        SHA512_Update(&ctx, &prefix, sizeof(HashPrefix::innerNode));

        // Add all child hashes (or zeros if empty)
        Hash256 zeroHash = Hash256::zero();
        for (int i = 0; i < 16; i++) {
            if (children[i]) {
                const auto &childHash = children[i]->getHash();
                SHA512_Update(&ctx, childHash.data(), Hash256::size());
            } else {
                SHA512_Update(&ctx, zeroHash.data(), Hash256::size());
            }
        }

        // Finalize hash (taking first half of SHA512)
        std::array<uint8_t, 64> fullHash;
        SHA512_Final(fullHash.data(), &ctx);

        // Create our hash from the first half
        hash = Hash256(reinterpret_cast<const uint8_t *>(fullHash.data()));

        hashValid = true;
    }

    bool setChild(int branch, std::shared_ptr<SHAMapTreeNode> const &child) {
        if (branch < 0 || branch >= 16) return false;

        if (child) {
            children[branch] = child;
            branchMask |= (1 << branch);
        } else {
            children[branch] = nullptr;
            branchMask &= ~(1 << branch);
        }

        hashValid = false;
        return true;
    }

    std::shared_ptr<SHAMapTreeNode> getChild(int branch) const {
        if (branch < 0 || branch >= 16) return nullptr;
        return children[branch];
    }

    bool hasChild(int branch) const {
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
    }

    bool isLeaf() const override { return true; }
    bool isInner() const override { return false; }


    void updateHash() override {
        // Select appropriate hash prefix based on node type
        // auto prefix = HashPrefix::leafNode; // Default

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
            throw std::runtime_error("Failed to create EVP_MD_CTX");
        }

        // Initialize with SHA-512
        if (EVP_DigestInit_ex(ctx, EVP_sha512(), nullptr) != 1) {
            EVP_MD_CTX_free(ctx);
            throw std::runtime_error("Failed to initialize SHA-512 digest");
        }


        // Add prefix
        if (EVP_DigestUpdate(ctx, &prefix, sizeof(prefix)) != 1) {
            EVP_MD_CTX_free(ctx);
            throw std::runtime_error("Failed to update digest with prefix");
        }

        // Add item data
        if (EVP_DigestUpdate(ctx, item->slice().data(), item->slice().size()) != 1) {
            EVP_MD_CTX_free(ctx);
            throw std::runtime_error("Failed to update digest with item data");
        }

        // Add item key
        if (EVP_DigestUpdate(ctx, item->key().data(), Key::size()) != 1) {
            EVP_MD_CTX_free(ctx);
            throw std::runtime_error("Failed to update digest with item key");
        }

        // Finalize hash
        std::array<unsigned char, 64> fullHash;
        unsigned int hashLen = 0;
        if (EVP_DigestFinal_ex(ctx, fullHash.data(), &hashLen) != 1) {
            EVP_MD_CTX_free(ctx);
            throw std::runtime_error("Failed to finalize digest");
        }

        // Free the context
        EVP_MD_CTX_free(ctx);

        // Debug output
        if (type != tnTRANSACTION_MD && false) {
            std::string hex;
            slice_hex(Slice(fullHash.data(), 64), hex);

            std::cout << "Hashing Key: " << item->key().toString() << std::endl;
            std::cout << "Hashing Data: " << item->hex() << std::endl;
            // std::cout << "Hashing Prefix Value: 0x" <<  std::endl;
            std::cout << "Full hash: " << hex << std::endl;
        }

        // Create our hash from the first half of SHA-512
        hash = Hash256(reinterpret_cast<const uint8_t *>(fullHash.data()));

        hashValid = true;
    }

    // void updateHash() override {
    //     // Select appropriate hash prefix based on node type
    //     HashPrefix prefix;
    //     switch (type) {
    //         // case tnTRANSACTION_NM:
    //         // case tnTRANSACTION_MD:
    //         //     prefix = HashPrefix::txNode;
    //         //     break;
    //         // case tnACCOUNT_STATE:
    //         //     prefix = HashPrefix::leafNode;
    //         //     break;
    //         default:
    //             prefix = HashPrefix::leafNode; // Default
    //     }
    //
    //
    //     // Compute hash
    //     SHA512_CTX ctx;
    //     if (!SHA512_Init(&ctx)) {
    //         throw std::runtime_error("ok");
    //     }
    //
    //     // Add prefix
    //     SHA512_Update(&ctx, &prefix, sizeof(prefix));
    //
    //     // Add item data and key
    //     SHA512_Update(&ctx, item->slice().data(), item->slice().size());
    //     SHA512_Update(&ctx, item->key().data(), Key::size());
    //
    //     // Finalize hash (taking first half of SHA512)
    //     std::array<unsigned char, 64> fullHash;
    //     SHA512_Final(fullHash.data(), &ctx);
    //
    //
    //
    //     if (type != tnTRANSACTION_MD) {
    //         std::string hex;
    //         slice_hex(Slice(fullHash.data(), 64), hex);
    //
    //         std::cout << "Hashing Key: " << item->key().toString() << std::endl;
    //         std::cout << "Hashing Data: " << item->hex() << std::endl;
    //         std::cout << "Hashing Prefix Value: 0x" << std::hex << static_cast<uint32_t>(prefix) << std::dec << std::endl;
    //         std::cout << "Full hash: " << hex << std::endl;
    //     }
    //
    //
    //
    //     // Create our hash from the first half
    //     hash = Hash256(reinterpret_cast<const uint8_t *>(fullHash.data()));
    //
    //     hashValid = true;
    // }

    std::shared_ptr<MmapItem> getItem() const {
        return item;
    }

    SHAMapNodeType getType() const {
        return type;
    }
};

// Main SHAMap class
class SHAMap {
private:
    std::shared_ptr<SHAMapInnerNode> root;
    SHAMapNodeType nodeType;

    // Helper to get the branch for a key at a given depth
    int selectBranch(const Key &key, int depth) const {
        int byteIdx = depth / 2;
        int nibbleIdx = depth % 2;
        uint8_t byte = key.data()[byteIdx];

        return nibbleIdx == 0 ? (byte >> 4) & 0xF : byte & 0xF;
    }

    void collapseTree(
        // Vector of pairs: {Parent Inner Node, Branch index taken from Parent to reach the next node down}
        // Stack goes from Root's child down to the removed leaf's parent.
        std::vector<std::pair<std::shared_ptr<SHAMapInnerNode>, int> > &stack,
        std::shared_ptr<SHAMapInnerNode> &root // Pass root in
    ) {

    }

    // Helper to collapse the tree after removal
    // void collapseTree(std::vector<std::pair<std::shared_ptr<SHAMapInnerNode>, int> > &stack) {
    //     // Process the stack from leaf toward root
    //     std::cout << "--- DEBUG: collapseTree ---" << std::endl;
    //     std::cout << "Stack size (path length above removed node's parent): "
    //               << stack.size() << std::endl;
    //
    //     root->invalidateHash();
    //
    //     // Loop through all the parent nodes on the path recorded in the stack
    //     // Order doesn't strictly matter for just invalidation
    //     for (const auto& stack_entry : stack) {
    //         auto parent_node = stack_entry.first;
    //         int branch_taken = stack_entry.second; // For logging, if needed
    //
    //         if (parent_node) {
    //             std::cout << "Invalidating hash for parent node at branch "
    //                       << branch_taken << std::endl;
    //             parent_node->invalidateHash(); // Mark hash as invalid
    //         } else {
    //             std::cout << "WARNING: Null parent node found in stack!" << std::endl;
    //         }
    //     }
    //
    //     while (!stack.empty()) {
    //         auto [parentNode, parentBranch] = stack.back();
    //         stack.pop_back();
    //
    //         // Get the child at this branch
    //         auto child = parentNode->getChild(parentBranch);
    //         if (!child) continue; // Child was removed
    //
    //         // Only try to collapse if it's an inner node
    //         if (!child->isInner()) continue;
    //
    //         auto innerChild = std::static_pointer_cast<SHAMapInnerNode>(child);
    //
    //         // Count children of this inner node
    //         int childCount = innerChild->getBranchCount();
    //
    //         if (childCount == 1) {
    //             // Find the only child
    //             int onlyChildBranch = -1;
    //             std::shared_ptr<SHAMapTreeNode> onlyChild;
    //
    //             for (int i = 0; i < 16; i++) {
    //                 if (innerChild->hasChild(i)) {
    //                     onlyChildBranch = i;
    //                     onlyChild = innerChild->getChild(i);
    //                     break;
    //                 }
    //             }
    //
    //             if (onlyChild) {
    //                 // Replace the parent's reference to this node with its only child
    //                 parentNode->setChild(parentBranch, onlyChild);
    //             }
    //         } else if (childCount == 0) {
    //             // If this node now has no children, remove it entirely
    //             parentNode->setChild(parentBranch, nullptr);
    //         }
    //     }
    //
    //     // Special case - check if root itself needs collapsing
    //     // Note: we don't actually collapse the root to a leaf, but we could
    //     // count its children for checking tree consistency
    //     int rootChildCount = root->getBranchCount();
    //     if (rootChildCount == 0) {
    //         // Root has no children - we've emptied the whole tree
    //         static constexpr bool verbose = true;
    //         if (verbose) {
    //             std::cout << "Tree is now empty" << std::endl;
    //         }
    //     }
    // }

public:
    SHAMap(SHAMapNodeType type = tnACCOUNT_STATE)
        : nodeType(type) {
        root = std::make_shared<SHAMapInnerNode>();
    }

    Hash256 getChildHash(int ix) {
        auto child = root->getChild(ix);
        if (child) {
            return child->getHash();
        } else {
            return Hash256::zero();
        }
    }

    bool addItem(std::shared_ptr<MmapItem> item, bool allowUpdate = true) {
        int depth = 0;
        std::shared_ptr<SHAMapInnerNode> node = root;
        // Stack to track the path for invalidation
        std::vector<std::shared_ptr<SHAMapInnerNode> > path_stack;

        while (true) {
            // Record the path as we descend
            path_stack.push_back(node);

            int branch = selectBranch(item->key(), depth);

            if (!node->hasChild(branch)) {
                // === ADD Case ===
                auto leaf = std::make_shared<SHAMapLeafNode>(item, nodeType);
                // setChild invalidates the direct parent 'node'
                node->setChild(branch, leaf);
                // Also invalidate all ancestors recorded on the path stack
                // (Necessary because adding a node changes parent hashes all the way up)
                // Note: technically redundant IF getHash propagation works perfectly,
                // but explicit invalidation is clearer and more robust.
                for (auto &ancestor: path_stack) {
                    if (ancestor) ancestor->invalidateHash();
                }
                return true;
            }

            // Branch has a child, check if it's leaf or inner
            auto child = node->getChild(branch);
            if (child->isLeaf()) {
                auto leafNode = std::static_pointer_cast<SHAMapLeafNode>(child);
                auto existingItem = leafNode->getItem();

                // Check if it's the same key
                if (existingItem->key() == item->key()) {
                    // === UPDATE Case ===
                    if (allowUpdate) {
                        auto newLeaf = std::make_shared<SHAMapLeafNode>(item, nodeType);
                        // Replace the leaf. setChild invalidates the direct parent 'node'.
                        node->setChild(branch, newLeaf);

                        // *** Invalidate the entire path to the root ***
                        // Because the leaf hash changed, all ancestor inner nodes'
                        // hashes are now also invalid.
                        // std::cerr << "DEBUG: Invalidating path due to update. Path size: "
                        //           << path_stack.size() << std::endl;
                        for (auto &ancestor: path_stack) {
                            if (ancestor) ancestor->invalidateHash();
                        }
                        // *** End path invalidation ***

                        return true; // Update successful
                    }
                    // Update not allowed
                    path_stack.pop_back(); // Remove current node before returning
                    return false;
                }

                // === COLLISION Case (Different Keys) ===
                // Need to create a new inner node and insert both leaves below it.
                auto newInner = std::make_shared<SHAMapInnerNode>();
                int existingBranch = selectBranch(existingItem->key(), depth + 1);
                int newBranch = selectBranch(item->key(), depth + 1);

                if (existingBranch != newBranch) {
                    // Keys diverge immediately below
                    newInner->setChild(existingBranch, child); // Invalidates newInner
                    auto newLeaf = std::make_shared<SHAMapLeafNode>(item, nodeType);
                    newInner->setChild(newBranch, newLeaf); // Invalidates newInner again
                    // Replace leaf with newInner. setChild invalidates parent 'node'.
                    node->setChild(branch, newInner);
                    // Invalidate the rest of the path stack above 'node'.
                    for (auto &ancestor: path_stack) {
                        if (ancestor) ancestor->invalidateHash();
                    }
                    return true;
                } else {
                    // Keys still collide, need to go deeper
                    // Replace leaf with newInner. setChild invalidates parent 'node'.
                    node->setChild(branch, newInner);
                    // Keep existing leaf temporarily under newInner
                    newInner->setChild(existingBranch, child); // Invalidates newInner
                    // Descend into newInner to continue insertion of the new item
                    node = newInner;
                    depth++;
                    // Loop continues, path_stack tracks correctly
                }
            } else {
                // === INNER Node Case ===
                // Descend down the tree
                node = std::static_pointer_cast<SHAMapInnerNode>(child);
                depth++;
                // Loop continues, path_stack tracks correctly
            }
        } // End while(true) loop
    }


    // bool addItem(std::shared_ptr<MmapItem> item, bool allowUpdate = true) {
    //     int depth = 0;
    //     std::shared_ptr<SHAMapInnerNode> node = root;
    //
    //     while (true) {
    //         int branch = selectBranch(item->key(), depth);
    //
    //         if (!node->hasChild(branch)) {
    //             // No child here, add the item as a leaf
    //             auto leaf = std::make_shared<SHAMapLeafNode>(item, nodeType);
    //             node->setChild(branch, leaf);
    //             return true;
    //         }
    //
    //         auto child = node->getChild(branch);
    //         if (child->isLeaf()) {
    //             // Use static_pointer_cast since we already verified it's a leaf
    //             auto leafNode = std::static_pointer_cast<SHAMapLeafNode>(child);
    //             auto existingItem = leafNode->getItem();
    //
    //             // Check if it's the same key (update)
    //             if (existingItem->key() == item->key()) {
    //                 // Only replace if updates are allowed
    //                 if (allowUpdate) {
    //                     // Replace the leaf with our new item
    //                     auto newLeaf = std::make_shared<SHAMapLeafNode>(item, nodeType);
    //                     node->setChild(branch, newLeaf);
    //                     return true;
    //                 }
    //                 return false; // Update not allowed
    //             }
    //
    //             // Different keys at same position - need to create a new inner node
    //             auto newInner = std::make_shared<SHAMapInnerNode>();
    //
    //             // Find the next branch for each item
    //             int existingBranch = selectBranch(existingItem->key(), depth + 1);
    //             int newBranch = selectBranch(item->key(), depth + 1);
    //
    //             if (existingBranch != newBranch) {
    //                 // Keys diverge at the next level, place them in their respective branches
    //                 newInner->setChild(existingBranch, child);
    //                 auto newLeaf = std::make_shared<SHAMapLeafNode>(item, nodeType);
    //                 newInner->setChild(newBranch, newLeaf);
    //
    //                 // Put the new inner node in place of the current leaf
    //                 node->setChild(branch, newInner);
    //                 return true;
    //             }
    //
    //             // If branches are the same, we need to go deeper
    //             // Replace the current leaf with an inner node and continue from there
    //             node->setChild(branch, newInner);
    //
    //             // Create a temporary leaf node for the new item
    //             auto newLeaf = std::make_shared<SHAMapLeafNode>(item, nodeType);
    //
    //             // Put the existing leaf in the inner node temporarily
    //             newInner->setChild(existingBranch, child);
    //
    //             // Continue with the new inner node
    //             node = newInner;
    //             depth++;
    //
    //             // Now we're back at the start of the loop with the new node
    //             // The next iteration will process this case again until we find a difference
    //         } else {
    //             // It's an inner node, move down the tree
    //             node = std::static_pointer_cast<SHAMapInnerNode>(child);
    //             depth++;
    //         }
    //     }
    // }

    // Remove an item by key
    bool removeItem(const Key &key) {
        int depth = 0;
        std::shared_ptr<SHAMapInnerNode> node = root;
        std::vector<std::pair<std::shared_ptr<SHAMapInnerNode>, int> > stack;

        // Traverse to find the leaf to remove
        while (true) {
            int branch = selectBranch(key, depth);

            if (!node->hasChild(branch)) {
                // Node not found
                return false;
            }

            auto child = node->getChild(branch);
            if (child->isLeaf()) {
                // Use static_pointer_cast since we already verified it's a leaf
                auto leafNode = std::static_pointer_cast<SHAMapLeafNode>(child);
                auto item = leafNode->getItem();

                // Check if it's the right key
                if (!(item->key() == key)) {
                    return false;
                }

                // Remove the leaf
                node->setChild(branch, nullptr);
                stack.push_back({node, branch});
                collapseTree(stack, root);

                return true;
            }

            // Push this node and branch to stack for path compression later
            stack.push_back({node, branch});

            // Move down the tree
            node = std::static_pointer_cast<SHAMapInnerNode>(child);
            depth++;
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
            std::cerr << "File too small to contain a valid header" << std::endl;
            return false;
        }

        std::memcpy(&header, data, sizeof(CATLHeader));

        // Basic validation
        if (header.magic != CATL) {
            std::cerr << "Invalid magic value: expected 0x" << std::hex << CATL
                    << ", got 0x" << header.magic << std::dec << std::endl;
            return false;
        }

        // Check compression level
        uint8_t compressionLevel = (header.version & CATALOGUE_COMPRESS_LEVEL_MASK) >> 8;
        if (compressionLevel != 0) {
            std::cerr << "Compressed files not supported. Level: " << static_cast<int>(compressionLevel) << std::endl;
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

    // Process a single ledger
    size_t processLedger(size_t offset) {
        // Check if we have enough data for the ledger info
        if (offset + sizeof(LedgerInfo) > fileSize) {
            std::cerr << "Not enough data for ledger info at offset " << offset << std::endl;
            return offset;
        }

        // Read ledger info
        LedgerInfo info;
        std::memcpy(&info, data + offset, sizeof(LedgerInfo));
        offset += sizeof(LedgerInfo);

        // Basic sanity check on sequence number
        if (info.sequence < header.min_ledger || info.sequence > header.max_ledger) {
            std::cerr << "WARNING: Ledger sequence " << info.sequence
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

        // // Set the correct map types for each map
        // stateMap.setNodeType(tnACCOUNT_STATE);
        // txMap.setNodeType(tnTRANSACTION_MD);

        // Process state map (clean if first ledger)
        uint32_t stateNodes = 0;
        if (isFirstLedger) {
            // stateMap = SHAMap(tnACCOUNT_STATE);
        }
        size_t newOffset = processStateMap(offset, stateNodes);
        if (newOffset == offset) {
            std::cerr << "Error processing state map for ledger " << info.sequence << std::endl;
            return offset;
        }
        offset = newOffset;
        stats.stateNodesTotal += stateNodes;

        // Process transaction map (always clean)
        uint32_t txNodes = 0;
        txMap = SHAMap(tnTRANSACTION_MD);
        newOffset = processTxMap(offset, txNodes);
        if (newOffset == offset) {
            std::cerr << "Error processing transaction map for ledger " << info.sequence << std::endl;
            return offset;
        }
        offset = newOffset;
        stats.txNodesTotal += txNodes;

        // Verify state map hash matches ledger info
        Hash256 computedStateHash = stateMap.getHash();
        Hash256 ledgerStateHash(info.accountHash);
        bool stateHashMatch = (computedStateHash == ledgerStateHash);

        if (!stateHashMatch) {
            std::cerr << "WARNING: Computed state hash doesn't match stored hash for ledger "
                    << info.sequence << std::endl;
            if (verbose) {
                std::cout << "  Computed ASH: " << computedStateHash.hex() << std::endl;
                std::cout << "  Expected ASH: " << ledgerStateHash.hex() << std::endl;
            }
            stats.failedHashVerifications++;
        } else {
            if (verbose) {
                std::cout << "  State hash verified for ledger " << info.sequence << std::endl;
            }
            stats.successfulHashVerifications++;
        }

        // Verify transaction map hash matches ledger info
        Hash256 computedTxHash = txMap.getHash();
        Hash256 ledgerTxHash(info.txHash);
        bool txHashMatch = (computedTxHash == ledgerTxHash);

        if (!txHashMatch) {
            std::cerr << "WARNING: Computed transaction hash doesn't match stored hash for ledger "
                    << info.sequence << std::endl;
            if (verbose) {
                std::cout << "  Computed TXH: " << computedTxHash.hex() << std::endl;
                std::cout << "  Expected TXH: " << ledgerTxHash.hex() << std::endl;
            }
            stats.failedHashVerifications++;
        } else {
            if (verbose) {
                std::cout << "  Transaction hash verified for ledger " << info.sequence << std::endl;
            }
            stats.successfulHashVerifications++;
        }

        stats.ledgersProcessed++;
        return offset;
    }

    // Process state map (or delta)
    size_t processStateMap(size_t offset, uint32_t &nodeCount) {
        nodeCount = 0;
        bool foundTerminal = false;

        while (offset < fileSize && !foundTerminal) {
            // Read node type - check bounds first
            if (offset >= fileSize) {
                std::cerr << "Unexpected EOF reading node type in state map" << std::endl;
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
                std::cerr << "Invalid node type: " << static_cast<int>(nodeType)
                        << " at offset " << (offset - 1) << std::endl;
                return offset - 1;
            }

            // Read key (32 bytes) - check bounds first
            if (offset + 32 > fileSize) {
                std::cerr << "Unexpected EOF reading key in state map" << std::endl;
                return offset;
            }
            const uint8_t *key = data + offset;
            offset += 32;

            if (nodeType == tnREMOVE) {
                // Remove item from state map
                Key itemKey(key);
                if (verbose) {
                    std::cout << "Removing state item: " << itemKey.toString() << std::endl;
                }
                if (stateMap.removeItem(itemKey)) {
                    std::cout << "Removal item was successful " << std::endl;
                    stats.stateRemovalsApplied++;
                } else {
                    std::cout << "Removal item was not successful " << std::endl;
                }
                nodeCount++;
                continue;
            }

            // Read data size - check bounds first
            if (offset + sizeof(uint32_t) > fileSize) {
                std::cerr << "Unexpected EOF reading data size in state map" << std::endl;
                return offset;
            }
            uint32_t dataSize = 0;
            std::memcpy(&dataSize, data + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);

            // Validate data size (check bounds and sanity check)
            if (dataSize > 100 * 1024 * 1024 || offset + dataSize > fileSize) {
                std::cerr << "Invalid data size: " << dataSize << " bytes at offset " << offset << std::endl;
                return offset;
            }

            // Create MmapItem pointing directly to the mapped memory
            const uint8_t *itemData = data + offset;
            offset += dataSize;

            auto item = std::make_shared<MmapItem>(key, itemData, dataSize);
            // if (nodeCount == 0) {
            //     break;
            // }

            if (false) {
                std::cout << "adding account state item with key="
                        << item->key().toString()
                        << " and data= "
                        << item->hex() << std::endl;
            }

            stateMap.addItem(item);
            nodeCount++;
            // if (nodeCount == 5) {
            //     std::cerr << "wtf" << std::endl;
            //     break;
            // }
            // std::cerr << "nodeCount: " << nodeCount << std::endl;
        }

        std::cerr << "nodeCount: " << nodeCount << std::endl;
        std::cerr << "first child: " << stateMap.getChildHash(0).hex() << std::endl;

        if (!foundTerminal && verbose) {
            std::cerr << "WARNING: No terminal marker found for state map" << std::endl;
        }

        return offset;
    }

    // Process transaction map
    size_t processTxMap(size_t offset, uint32_t &nodeCount) {
        nodeCount = 0;
        bool foundTerminal = false;

        while (offset < fileSize && !foundTerminal) {
            // Read node type - check bounds first
            if (offset >= fileSize) {
                std::cerr << "Unexpected EOF reading node type in transaction map" << std::endl;
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
                std::cerr << "Invalid node type: " << static_cast<int>(nodeType)
                        << " at offset " << (offset - 1) << std::endl;
                return offset - 1;
            }

            // Skip tnREMOVE (shouldn't be in tx maps) - but check bounds first
            if (nodeType == tnREMOVE) {
                if (offset + 32 > fileSize) {
                    std::cerr << "Unexpected EOF reading REMOVE key in transaction map" << std::endl;
                    return offset;
                }
                offset += 32; // Skip key
                nodeCount++;
                continue;
            }

            // Read key (32 bytes) - check bounds first
            if (offset + 32 > fileSize) {
                std::cerr << "Unexpected EOF reading key in transaction map" << std::endl;
                return offset;
            }
            const uint8_t *key = data + offset;
            offset += 32;

            // Read data size - check bounds first
            if (offset + sizeof(uint32_t) > fileSize) {
                std::cerr << "Unexpected EOF reading data size in transaction map" << std::endl;
                return offset;
            }
            uint32_t dataSize = 0;
            std::memcpy(&dataSize, data + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);

            // Validate data size (check bounds and sanity check)
            if (dataSize > 100 * 1024 * 1024 || offset + dataSize > fileSize) {
                std::cerr << "Invalid data size: " << dataSize << " bytes at offset " << offset << std::endl;
                return offset;
            }

            // Create MmapItem pointing directly to the mapped memory
            const uint8_t *itemData = data + offset;
            offset += dataSize;

            auto item = std::make_shared<MmapItem>(key, itemData, dataSize);
            if (verbose) {
                // std::cout << "Adding item: " << item->hex() << std::endl;
            }
            txMap.addItem(item);
            nodeCount++;
        }

        if (!foundTerminal && verbose) {
            std::cerr << "WARNING: No terminal marker found for transaction map" << std::endl;
        }

        return offset;
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
            std::cerr << "Error opening file: " << e.what() << std::endl;
            throw;
        }
    }

    bool processFile() {
        try {
            // Ensure we have a valid data pointer and file size
            if (!data || fileSize == 0) {
                std::cerr << "No data available - file may not be properly opened" << std::endl;
                return false;
            }

            if (!validateHeader()) {
                return false;
            }

            // Check file size against header's reported size
            if (header.filesize != fileSize) {
                std::cerr << "WARNING: File size mismatch. Header indicates "
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
                if (ledgers == 13) {
                    break;
                }

                if (newOffset == offset) {
                    std::cerr << "No progress made processing ledger at offset " << offset << std::endl;
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
        } catch (const std::exception &e) {
            std::cerr << "Error processing file: " << e.what() << std::endl;
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
