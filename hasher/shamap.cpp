#include "hasher/shamap.h"

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "hasher/catalogue-consts.h"
#include "hasher/log-macros.h"
#include "hasher/logger.h"

//----------------------------------------------------------
// Exception Classes Implementation
//----------------------------------------------------------
SHAMapException::SHAMapException(const std::string& message)
    : std::runtime_error(message)
{
}

InvalidDepthException::InvalidDepthException(int depth, size_t maxAllowed)
    : SHAMapException(
          "Invalid depth (" + std::to_string(depth) +
          ") for key in selectBranch. Max allowed: " +
          std::to_string(maxAllowed))
    , depth_(depth)
    , maxAllowed_(maxAllowed)
{
}

int
InvalidDepthException::depth() const
{
    return depth_;
}

size_t
InvalidDepthException::max_allowed() const
{
    return maxAllowed_;
}

InvalidBranchException::InvalidBranchException(int branch)
    : SHAMapException("Invalid branch index: " + std::to_string(branch))
    , branch_(branch)
{
}

int
InvalidBranchException::branch() const
{
    return branch_;
}

NullNodeException::NullNodeException(const std::string& context)
    : SHAMapException("Null node encountered: " + context)
{
}

NullItemException::NullItemException()
    : SHAMapException("Found leaf node with null item")
{
}

HashCalculationException::HashCalculationException(const std::string& reason)
    : SHAMapException("Hash calculation error: " + reason)
{
}

//----------------------------------------------------------
// Helper Functions Implementation
//----------------------------------------------------------
int
select_branch(const Key& key, int depth)
{
    int byteIdx = depth / 2;
    if (byteIdx < 0 || byteIdx >= static_cast<int>(Key::size()))
    {
        throw InvalidDepthException(depth, Key::size());
    }
    int nibbleIdx = depth % 2;
    uint8_t byte_val = key.data()[byteIdx];
    return (nibbleIdx == 0) ? (byte_val >> 4) & 0xF : byte_val & 0xF;
}

//----------------------------------------------------------
// SHAMapTreeNode Implementation
//----------------------------------------------------------

void
intrusive_ptr_add_ref(const SHAMapTreeNode* p)
{
    p->refCount_.fetch_add(1, std::memory_order_relaxed);
}

void
intrusive_ptr_release(const SHAMapTreeNode* p)
{
    if (p->refCount_.fetch_sub(1, std::memory_order_release) == 1)
    {
        std::atomic_thread_fence(std::memory_order_acquire);
        delete p;
    }
}

void
SHAMapTreeNode::invalidate_hash()
{
    hashValid = false;
}

const Hash256&
SHAMapTreeNode::get_hash()
{
    if (!hashValid)
    {
        update_hash();
        hashValid = true;
    }
    return hash;
}

//----------------------------------------------------------
// SHAMapLeafNode Implementation
//----------------------------------------------------------
SHAMapLeafNode::SHAMapLeafNode(
    boost::intrusive_ptr<MmapItem> i,
    SHAMapNodeType t)
    : item(std::move(i)), type(t)
{
    if (!item)
    {
        throw NullItemException();
    }
}

bool
SHAMapLeafNode::is_leaf() const
{
    return true;
}

bool
SHAMapLeafNode::is_inner() const
{
    return false;
}

void
SHAMapLeafNode::update_hash()
{
    std::array<unsigned char, 4> prefix = {0, 0, 0, 0};
    auto set = [&prefix](auto& from) {
        std::memcpy(prefix.data(), from.data(), 4);
    };
    switch (type)
    {
        case tnTRANSACTION_NM:
        case tnTRANSACTION_MD:
            set(HashPrefix::txNode);
            break;
        case tnACCOUNT_STATE:
        default:
            set(HashPrefix::leafNode);
            break;
    }
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (ctx == nullptr)
    {
        throw HashCalculationException("Failed to create EVP_MD_CTX");
    }
    if (EVP_DigestInit_ex(ctx, EVP_sha512(), nullptr) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to initialize SHA-512 digest");
    }
    if (EVP_DigestUpdate(ctx, &prefix, sizeof(prefix)) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to update digest with prefix");
    }
    if (EVP_DigestUpdate(ctx, item->slice().data(), item->slice().size()) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException(
            "Failed to update digest with item data");
    }
    if (EVP_DigestUpdate(ctx, item->key().data(), Key::size()) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to update digest with item key");
    }
    std::array<unsigned char, 64> fullHash;
    unsigned int hashLen = 0;
    if (EVP_DigestFinal_ex(ctx, fullHash.data(), &hashLen) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to finalize digest");
    }
    EVP_MD_CTX_free(ctx);
    hash = Hash256(reinterpret_cast<const uint8_t*>(fullHash.data()));
    hashValid = true;
}

boost::intrusive_ptr<MmapItem>
SHAMapLeafNode::get_item() const
{
    return item;
}

SHAMapNodeType
SHAMapLeafNode::get_type() const
{
    return type;
}

boost::intrusive_ptr<SHAMapLeafNode>
SHAMapLeafNode::copy() const
{
    auto newLeaf = boost::intrusive_ptr(new SHAMapLeafNode(item, type));
    newLeaf->hash = hash;
    newLeaf->hashValid = hashValid;
    newLeaf->version = version;
    return newLeaf;
}

//----------------------------------------------------------
// NodeChildren Implementation
//----------------------------------------------------------

NodeChildren::NodeChildren() : capacity_(16), canonicalized_(false)
{
    // Allocate full array of 16 slots
    children_ = new boost::intrusive_ptr<SHAMapTreeNode>[16]();

    // Initialize branch mapping for direct indexing
    for (int i = 0; i < 16; i++)
    {
        branchToIndex_[i] = i;
    }
}

NodeChildren::~NodeChildren()
{
    delete[] children_;
}

boost::intrusive_ptr<SHAMapTreeNode>
NodeChildren::getChild(int branch) const
{
    if (branch < 0 || branch >= 16)
        return nullptr;

    if (!(branchMask_ & (1 << branch)))
        return nullptr;

    return children_[canonicalized_ ? branchToIndex_[branch] : branch];
}

void
NodeChildren::setChild(int branch, boost::intrusive_ptr<SHAMapTreeNode> child)
{
    if (branch < 0 || branch >= 16)
        return;

    if (canonicalized_)
    {
        // IMPORTANT: Canonicalized nodes are immutable!
        // This should never happen if used correctly
        throw std::runtime_error("Attempted to modify a canonicalized node");
    }

    // Only non-canonicalized nodes can be modified
    if (child)
    {
        children_[branch] = child;
        branchMask_ |= (1 << branch);
    }
    else if (branchMask_ & (1 << branch))
    {
        children_[branch] = nullptr;
        branchMask_ &= ~(1 << branch);
    }
}

void
NodeChildren::canonicalize()
{
    if (canonicalized_ || branchMask_ == 0)
        return;

    int childCount = __builtin_popcount(branchMask_);

    // No need to canonicalize if nearly full
    if (childCount >= 14)
        return;

    // Create optimally sized array
    auto newChildren = new boost::intrusive_ptr<SHAMapTreeNode>[childCount];

    // Initialize lookup table (all -1)
    for (int i = 0; i < 16; i++)
    {
        branchToIndex_[i] = -1;
    }

    // Copy only non-null children
    int newIndex = 0;
    for (int i = 0; i < 16; i++)
    {
        if (branchMask_ & (1 << i))
        {
            newChildren[newIndex] = children_[i];
            branchToIndex_[i] = newIndex++;
        }
    }

    // Replace storage
    delete[] children_;
    children_ = newChildren;
    capacity_ = childCount;
    canonicalized_ = true;
}

std::unique_ptr<NodeChildren>
NodeChildren::copy() const
{
    auto newChildren = std::make_unique<NodeChildren>();

    // Copy branch mask
    newChildren->branchMask_ = branchMask_;

    // Always create a full non-canonicalized copy
    for (int i = 0; i < 16; i++)
    {
        if (branchMask_ & (1 << i))
        {
            if (canonicalized_)
            {
                newChildren->children_[i] = children_[branchToIndex_[i]];
            }
            else
            {
                newChildren->children_[i] = children_[i];
            }
        }
    }

    // Never copy the canonicalized state!
    newChildren->canonicalized_ = false;

    return newChildren;
}

const boost::intrusive_ptr<SHAMapTreeNode>&
NodeChildren::operator[](int branch) const
{
    static boost::intrusive_ptr<SHAMapTreeNode> nullPtr;

    if (branch < 0 || branch >= 16 || !(branchMask_ & (1 << branch)))
        return nullPtr;

    return children_[canonicalized_ ? branchToIndex_[branch] : branch];
}

// NO NON-CONST OPERATOR[] - Canonicalized nodes are immutable!
// This forces modifications to go through setChild which enforces immutability

//----------------------------------------------------------
// SHAMapInnerNode Implementation
//----------------------------------------------------------

SHAMapInnerNode::SHAMapInnerNode(uint8_t nodeDepth)
    : depth_(nodeDepth), version(0), do_cow_(false)
{
    children_ = std::make_unique<NodeChildren>();
}

SHAMapInnerNode::SHAMapInnerNode(
    bool isCopy,
    uint8_t nodeDepth,
    int initialVersion)
    : depth_(nodeDepth), version(initialVersion), do_cow_(isCopy)
{
    children_ = std::make_unique<NodeChildren>();
}

bool
SHAMapInnerNode::is_leaf() const
{
    return false;
}

bool
SHAMapInnerNode::is_inner() const
{
    return true;
}

uint8_t
SHAMapInnerNode::getDepth() const
{
    return depth_;
}

void
SHAMapInnerNode::setDepth(uint8_t newDepth)
{
    depth_ = newDepth;
}

void
SHAMapInnerNode::update_hash()
{
    uint16_t branchMask = children_->getBranchMask();

    if (branchMask == 0)
    {
        hash = Hash256::zero();
        hashValid = true;
        return;
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx)
        throw HashCalculationException("Failed to create EVP_MD_CTX");

    if (EVP_DigestInit_ex(ctx, EVP_sha512(), nullptr) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to initialize SHA-512 digest");
    }

    auto prefix = HashPrefix::innerNode;
    if (EVP_DigestUpdate(ctx, &prefix, sizeof(HashPrefix::innerNode)) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to update digest with prefix");
    }

    Hash256 zeroHash = Hash256::zero();
    for (int i = 0; i < 16; i++)
    {
        const uint8_t* hashData = zeroHash.data();
        auto child = children_->getChild(i);
        if (child)
        {
            hashData = child->get_hash().data();  // Recursive hash calculation
        }

        if (EVP_DigestUpdate(ctx, hashData, Hash256::size()) != 1)
        {
            EVP_MD_CTX_free(ctx);
            throw HashCalculationException(
                "Failed to update digest with child data (branch " +
                std::to_string(i) + ")");
        }
    }

    std::array<uint8_t, 64> fullHash{};
    unsigned int hashLen = 0;
    if (EVP_DigestFinal_ex(ctx, fullHash.data(), &hashLen) != 1)
    {
        EVP_MD_CTX_free(ctx);
        throw HashCalculationException("Failed to finalize digest");
    }

    EVP_MD_CTX_free(ctx);
    hash = Hash256(reinterpret_cast<const uint8_t*>(fullHash.data()));
    hashValid = true;

    // Once hash is calculated, canonicalize to save memory
    // After this, the node becomes immutable until explicitly copied
    children_->canonicalize();
}

bool
SHAMapInnerNode::set_child(
    int branch,
    boost::intrusive_ptr<SHAMapTreeNode> const& child)
{
    if (branch < 0 || branch >= 16)
    {
        throw InvalidBranchException(branch);
    }

    // Check if node is canonicalized - if yes, we need to make a copy first
    if (children_->isCanonical())
    {
        // Create a non-canonicalized copy of children
        children_ = children_->copy();
    }

    // Now safe to modify
    children_->setChild(branch, child);
    invalidate_hash();  // Mark hash as invalid
    return true;
}

boost::intrusive_ptr<SHAMapTreeNode>
SHAMapInnerNode::get_child(int branch) const
{
    if (branch < 0 || branch >= 16)
    {
        throw InvalidBranchException(branch);
    }

    return children_->getChild(branch);
}

bool
SHAMapInnerNode::has_child(int branch) const
{
    if (branch < 0 || branch >= 16)
    {
        throw InvalidBranchException(branch);
    }

    return children_->hasChild(branch);
}

int
SHAMapInnerNode::get_branch_count() const
{
    return children_->getChildCount();
}

uint16_t
SHAMapInnerNode::get_branch_mask() const
{
    return children_->getBranchMask();
}

boost::intrusive_ptr<SHAMapLeafNode>
SHAMapInnerNode::get_only_child_leaf() const
{
    boost::intrusive_ptr<SHAMapLeafNode> resultLeaf = nullptr;
    int leafCount = 0;

    // Iterate through all branches
    for (int i = 0; i < 16; i++)
    {
        if (children_->hasChild(i))
        {
            auto child = children_->getChild(i);
            if (child->is_inner())
            {
                return nullptr;  // Found inner node, not a leaf-only node
            }

            leafCount++;
            if (leafCount == 1)
            {
                resultLeaf = boost::static_pointer_cast<SHAMapLeafNode>(child);
            }
            else
            {
                return nullptr;  // More than one leaf
            }
        }
    }

    return resultLeaf;  // Returns leaf if exactly one found, else nullptr
}

boost::intrusive_ptr<SHAMapInnerNode>
SHAMapInnerNode::copy(int newVersion) const
{
    // Create a new inner node with same depth
    auto newNode =
        boost::intrusive_ptr(new SHAMapInnerNode(true, depth_, newVersion));

    // Copy children - this creates a non-canonicalized copy
    newNode->children_ = children_->copy();

    // Copy other properties
    newNode->hash = hash;
    newNode->hashValid = hashValid;

    LOGD(
        "Cloned inner node from version ",
        get_version(),
        " to version ",
        newVersion);

    return newNode;
}

//----------------------------------------------------------
// PathFinder Implementation
//----------------------------------------------------------
PathFinder::PathFinder(
    boost::intrusive_ptr<SHAMapInnerNode>& root,
    const Key& key)
    : targetKey(key)
{
    find_path(root);
}

void
PathFinder::find_path(boost::intrusive_ptr<SHAMapInnerNode> root)
{
    if (!root)
    {
        throw NullNodeException("PathFinder: null root node");
    }
    searchRoot = root;
    foundLeaf = nullptr;
    leafKeyMatches = false;
    terminalBranch = -1;
    boost::intrusive_ptr<SHAMapInnerNode> currentInner = root;
    while (true)
    {
        int branch = select_branch(targetKey, currentInner->getDepth());
        boost::intrusive_ptr<SHAMapTreeNode> child =
            currentInner->get_child(branch);
        if (!child)
        {
            terminalBranch = branch;
            inners.push_back(currentInner);
            break;
        }
        if (child->is_leaf())
        {
            terminalBranch = branch;
            inners.push_back(currentInner);
            foundLeaf = boost::static_pointer_cast<SHAMapLeafNode>(child);
            if (foundLeaf->get_item())
            {
                leafKeyMatches = (foundLeaf->get_item()->key() == targetKey);
            }
            else
            {
                throw NullItemException();
            }
            break;
        }
        inners.push_back(currentInner);
        branches.push_back(branch);
        currentInner = boost::static_pointer_cast<SHAMapInnerNode>(child);
    }
}

bool
PathFinder::has_leaf() const
{
    return foundLeaf != nullptr;
}

bool
PathFinder::did_leaf_key_match() const
{
    return leafKeyMatches;
}

bool
PathFinder::ended_at_null_branch() const
{
    return foundLeaf == nullptr && terminalBranch != -1;
}

boost::intrusive_ptr<const SHAMapLeafNode>
PathFinder::get_leaf() const
{
    return foundLeaf;
}

boost::intrusive_ptr<SHAMapLeafNode>
PathFinder::get_leaf_mutable()
{
    return foundLeaf;
}

boost::intrusive_ptr<SHAMapInnerNode>
PathFinder::get_parent_of_terminal()
{
    return inners.empty() ? nullptr : inners.back();
}

boost::intrusive_ptr<const SHAMapInnerNode>
PathFinder::get_parent_of_terminal() const
{
    return inners.empty() ? nullptr : inners.back();
}

int
PathFinder::get_terminal_branch() const
{
    return terminalBranch;
}

void
PathFinder::dirty_path() const
{
    for (auto& inner : inners)
    {
        inner->invalidate_hash();
    }
}

void
PathFinder::collapse_path()
{
    if (inners.size() <= 1)
        return;
    boost::intrusive_ptr<SHAMapLeafNode> onlyChild = nullptr;
    auto innermost = inners.back();
    onlyChild = innermost->get_only_child_leaf();
    for (int i = static_cast<int>(inners.size()) - 2; i >= 0; --i)
    {
        auto inner = inners[i];
        int branch = branches[i];
        if (onlyChild)
        {
            inner->set_child(branch, onlyChild);
        }
        onlyChild = inner->get_only_child_leaf();
        if (!onlyChild)
            break;
    }
}

bool
PathFinder::maybe_copy_on_write() const
{
    return !inners.empty() && inners.back()->is_cow_enabled();
}

boost::intrusive_ptr<SHAMapInnerNode>
PathFinder::dirty_or_copy_inners(int targetVersion)
{
    if (inners.empty())
    {
        LOGW("No inner nodes in path to apply CoW");
        return nullptr;
    }

    // Start from the root and work downward
    // Remove the unused variable

    for (size_t i = 0; i < inners.size(); ++i)
    {
        auto& currentInner = inners[i];

        // Skip if already at target version
        if (currentInner->get_version() == targetVersion)
        {
            LOGD(
                "Node at index ",
                i,
                " already at target version ",
                targetVersion);
            continue;
        }

        // Skip nodes that don't have CoW enabled
        if (!currentInner->is_cow_enabled())
        {
            // Just update version
            LOGD(
                "Node at index ",
                i,
                " has CoW disabled, updating version from ",
                currentInner->get_version(),
                " to ",
                targetVersion);
            currentInner->set_version(targetVersion);
            continue;
        }

        // Need to create a copy (CoW)
        LOGD(
            "Creating CoW copy of node at index ",
            i,
            " version ",
            currentInner->get_version(),
            " to version ",
            targetVersion);

        // Create copy with new version
        auto copy = currentInner->copy(targetVersion);

        // If this is the root, update the search root
        if (i == 0)
        {
            searchRoot = copy;
        }

        // If not the root, update parent's child pointer to point to this copy
        if (i > 0)
        {
            const auto& parent = inners[i - 1];
            int branch = branches[i - 1];
            LOGD(
                "Updating parent at depth ",
                parent->getDepth(),
                " branch ",
                branch,
                " to point to new copy");
            parent->set_child(branch, copy);
        }

        // Replace in our path vector
        inners[i] = copy;
    }

    // Return the innermost node for further operations
    return inners.back();
}

boost::intrusive_ptr<SHAMapLeafNode>
PathFinder::invalidated_possibly_copied_leaf_for_updating(int targetVersion)
{
    if (!leafKeyMatches)
    {
        throw SHAMapException("Cannot update leaf - key mismatch");
    }

    // Make sure we've handled the inner nodes first
    auto terminal = dirty_or_copy_inners(targetVersion);
    if (!terminal)
    {
        throw SHAMapException("Failed to prepare path for leaf update");
    }

    boost::intrusive_ptr<SHAMapLeafNode> theLeaf = foundLeaf;

    // Check if we need to copy the leaf
    if (foundLeaf->get_version() != targetVersion)
    {
        theLeaf = foundLeaf->copy();
        theLeaf->set_version(targetVersion);
        terminal->set_child(terminalBranch, theLeaf);
        foundLeaf = theLeaf;  // Update our reference
    }

    theLeaf->invalidate_hash();
    return theLeaf;
}

//----------------------------------------------------------
// SHAMap Implementation
//----------------------------------------------------------
SHAMap::SHAMap(SHAMapNodeType type)
    : node_type_(type)
    , version_counter_(std::make_shared<std::atomic<int>>(0))
    , current_version_(0)
    , cow_enabled_(false)
{
    root = boost::intrusive_ptr(new SHAMapInnerNode(0));  // Root has depth 0
    LOGD("SHAMap created with type: ", static_cast<int>(type));
}

SHAMap::SHAMap(
    const SHAMapNodeType type,
    boost::intrusive_ptr<SHAMapInnerNode> rootNode,
    std::shared_ptr<std::atomic<int>> vCounter,
    const int version)
    : root(std::move(rootNode))
    , node_type_(type)
    , version_counter_(std::move(vCounter))
    , current_version_(version)
    , cow_enabled_(true)
{
    LOGD("Created SHAMap snapshot with version ", version);
}

void
SHAMap::enable_cow(bool enable)
{
    cow_enabled_ = enable;

    // Update root node if it exists
    if (root && enable)
    {
        root->enable_cow(true);

        // Set version if it's 0
        if (root->get_version() == 0)
        {
            root->set_version(current_version_);
        }
    }

    LOGD(
        "Copy-on-Write ",
        (enable ? "enabled" : "disabled"),
        " for SHAMap with version ",
        current_version_);
}

int
SHAMap::new_version()
{
    if (!version_counter_)
    {
        version_counter_ = std::make_shared<std::atomic<int>>(0);
    }
    // Increment shared counter and update current version
    int newVer = ++(*version_counter_);
    current_version_ = newVer;
    LOGD("Generated new SHAMap version: ", newVer);
    return newVer;
}

std::shared_ptr<SHAMap>
SHAMap::snapshot()
{
    if (!root)
    {
        LOGW("Attempted to snapshot a SHAMap with null root");
        return nullptr;
    }

    // Enable CoW if not already enabled
    if (!cow_enabled_)
    {
        enable_cow(!cow_enabled_);
    }

    // Create new version for both original and snapshot
    const int originalVersion = new_version();
    int snapshotVersion = new_version();

    LOGI(
        "Creating snapshot: original version ",
        originalVersion,
        ", snapshot version ",
        snapshotVersion);

    // Create a new SHAMap that shares the same root and version counter
    auto copy = std::make_shared<SHAMap>(
        SHAMap(node_type_, root, version_counter_, snapshotVersion));

    return copy;
}

bool
SHAMap::add_item(boost::intrusive_ptr<MmapItem>& item, bool allowUpdate)
{
    if (!item)
    {
        LOGW("Attempted to add null item to SHAMap.");
        return false;
    }
    LOGD_KEY("Attempting to add item with key: ", item->key());

    try
    {
        PathFinder pathFinder(root, item->key());

        // If CoW is enabled, handle versioning
        if (cow_enabled_)
        {
            // First generate a new version if needed
            if (current_version_ == 0)
            {
                new_version();
            }

            // Apply CoW to path
            auto innerNode = pathFinder.dirty_or_copy_inners(current_version_);
            if (!innerNode)
            {
                throw NullNodeException(
                    "addItem: CoW failed to return valid inner node");
            }

            // If root was copied, update our reference
            if (pathFinder.get_parent_of_terminal() != root)
            {
                root = pathFinder.searchRoot;
            }
        }

        if (pathFinder.ended_at_null_branch() ||
            (pathFinder.has_leaf() && pathFinder.did_leaf_key_match() &&
             allowUpdate))
        {
            auto parent = pathFinder.get_parent_of_terminal();
            int branch = pathFinder.get_terminal_branch();
            if (!parent)
            {
                throw NullNodeException(
                    "addItem: null parent node (should be root)");
            }

            LOGD(
                "Adding/Updating leaf at depth ",
                parent->getDepth() + 1,
                " branch ",
                branch);
            auto newLeaf =
                boost::intrusive_ptr(new SHAMapLeafNode(item, node_type_));
            if (cow_enabled_)
            {
                newLeaf->set_version(current_version_);
            }
            parent->set_child(branch, newLeaf);
            pathFinder.dirty_path();
            return true;
        }

        if (pathFinder.has_leaf() && !pathFinder.did_leaf_key_match())
        {
            LOGD_KEY("Handling collision for key: ", item->key());
            auto parent = pathFinder.get_parent_of_terminal();
            int branch = pathFinder.get_terminal_branch();
            if (!parent)
            {
                throw NullNodeException(
                    "addItem collision: null parent node (should be root)");
            }
            auto existingLeaf = pathFinder.get_leaf_mutable();
            auto existingItem = existingLeaf->get_item();
            if (!existingItem)
            {
                throw NullItemException(); /* Should be caught by leaf
                                              constructor */
            }

            boost::intrusive_ptr<SHAMapInnerNode> currentParent = parent;
            int currentBranch = branch;
            uint8_t currentDepth =
                parent->getDepth() + 1;  // Start depth below parent

            // Create first new inner node to replace the leaf
            auto newInner =
                boost::intrusive_ptr(new SHAMapInnerNode(currentDepth));
            if (cow_enabled_)
            {
                newInner->enable_cow(true);
                newInner->set_version(current_version_);
            }
            parent->set_child(currentBranch, newInner);
            currentParent = newInner;

            while (currentDepth < 64)
            {
                // Max depth check
                int existingBranch =
                    select_branch(existingItem->key(), currentDepth);
                int newBranch = select_branch(item->key(), currentDepth);

                if (existingBranch != newBranch)
                {
                    LOGD(
                        "Collision resolved at depth ",
                        currentDepth,
                        ". Placing leaves at branches ",
                        existingBranch,
                        " and ",
                        newBranch);
                    auto newLeaf = boost::intrusive_ptr(
                        new SHAMapLeafNode(item, node_type_));
                    if (cow_enabled_)
                    {
                        newLeaf->set_version(current_version_);
                        // May need to update existing leaf version as well
                        if (existingLeaf->get_version() != current_version_)
                        {
                            auto copiedLeaf = existingLeaf->copy();
                            copiedLeaf->set_version(current_version_);
                            existingLeaf = copiedLeaf;
                        }
                    }
                    currentParent->set_child(existingBranch, existingLeaf);
                    currentParent->set_child(newBranch, newLeaf);
                    break;  // Done
                }
                else
                {
                    // Collision continues, create another inner node
                    LOGD(
                        "Collision continues at depth ",
                        currentDepth,
                        ", branch ",
                        existingBranch,
                        ". Descending further.");
                    auto nextInner = boost::intrusive_ptr(
                        new SHAMapInnerNode(currentDepth + 1));
                    if (cow_enabled_)
                    {
                        nextInner->enable_cow(true);
                        nextInner->set_version(current_version_);
                    }
                    currentParent->set_child(existingBranch, nextInner);
                    currentParent = nextInner;
                    currentDepth++;
                }
            }
            if (currentDepth >= 64)
            {
                throw SHAMapException(
                    "Maximum SHAMap depth reached during collision resolution "
                    "for key: " +
                    item->key().hex());
            }

            pathFinder.dirty_path();
            return true;
        }

        // Should ideally not be reached if PathFinder logic is correct
        LOGE(
            "Unexpected state in addItem for key: ",
            item->key().hex(),
            ". PathFinder logic error?");
        throw SHAMapException(
            "Unexpected state in addItem - PathFinder logic error");
    }
    catch (const SHAMapException& e)
    {
        LOGE("Error adding item with key ", item->key().hex(), ": ", e.what());
        return false;
    }
    catch (const std::exception& e)
    {
        LOGE(
            "Standard exception adding item with key ",
            item->key().hex(),
            ": ",
            e.what());
        return false;
    }
}

bool
SHAMap::remove_item(const Key& key)
{
    LOGD_KEY("Attempting to remove item with key: ", key);
    try
    {
        PathFinder pathFinder(root, key);

        // If CoW is enabled, handle versioning
        if (cow_enabled_)
        {
            // First generate a new version if needed
            if (current_version_ == 0)
            {
                new_version();
            }

            // Apply CoW to path
            auto innerNode = pathFinder.dirty_or_copy_inners(current_version_);
            if (!innerNode)
            {
                throw NullNodeException(
                    "removeItem: CoW failed to return valid inner node");
            }

            // If root was copied, update our reference
            if (pathFinder.get_parent_of_terminal() != root)
            {
                root = pathFinder.searchRoot;
            }
        }

        if (!pathFinder.has_leaf() || !pathFinder.did_leaf_key_match())
        {
            LOGD_KEY("Item not found for removal, key: ", key);
            return false;  // Item not found
        }

        auto parent = pathFinder.get_parent_of_terminal();
        int branch = pathFinder.get_terminal_branch();
        if (!parent)
        {
            throw NullNodeException(
                "removeItem: null parent node (should be root)");
        }

        LOGD(
            "Removing leaf at depth ",
            parent->getDepth() + 1,
            " branch ",
            branch);
        parent->set_child(branch, nullptr);  // Remove the leaf
        pathFinder.dirty_path();
        pathFinder.collapse_path();  // Compress path if possible
        LOGD_KEY("Item removed successfully, key: ", key);
        return true;
    }
    catch (const SHAMapException& e)
    {
        LOGE("Error removing item with key ", key.hex(), ": ", e.what());
        return false;
    }
    catch (const std::exception& e)
    {
        LOGE(
            "Standard exception removing item with key ",
            key.hex(),
            ": ",
            e.what());
        return false;
    }
}

Hash256
SHAMap::get_hash() const
{
    if (!root)
    {
        LOGW("Attempting to get hash of a null root SHAMap.");
        return Hash256::zero();
    }
    // getHash() inside the node handles lazy calculation
    return root->get_hash();
}
