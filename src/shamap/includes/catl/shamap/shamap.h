#pragma once

#include <array>
#include <atomic>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <memory>
#include <openssl/evp.h>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "catl/core/logger.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap-errors.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-nodechildren.h"
#include "catl/shamap/shamap-nodeid.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-options.h"
#include "catl/shamap/shamap-pathfinder.h"
#include "catl/shamap/shamap-traits.h"
#include "catl/shamap/shamap-treenode.h"

namespace catl::shamap {

// LogPartition for walk_new_nodes tracking - enable with
// walk_nodes_log.enable(LogLevel::DEBUG)
extern LogPartition walk_nodes_log;
/**
 * Main SHAMap class implementing a pruned, binary prefix tree
 * with Copy-on-Write support for efficient snapshots
 */
template <typename Traits = DefaultNodeTraits>
class SHAMapT
{
private:
    static LogPartition log_partition_;
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>> root;
    SHAMapNodeType node_type_;
    SHAMapOptions options_;

    // CoW support - all private
    std::shared_ptr<std::atomic<int>> version_counter_;
    int current_version_ = 0;
    bool cow_enabled_ = false;
    bool needs_version_bump_on_write_ =
        false;  // For lazy version bumping after snapshot

    // Private methods
    void
    enable_cow();

    bool
    is_cow_enabled() const
    {
        return cow_enabled_;
    }

    int
    new_version(bool in_place = false);

    /**
     * Recursively collapses inner nodes that have only a single inner child
     * and no leaf children
     *
     * @param node The inner node to process
     */
    void
    collapse_inner_node(boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& node);

    /**
     * Finds a single inner child if this node has exactly one inner child
     * and no leaf children
     *
     * @param node The inner node to check
     * @return A pointer to the single inner child, or nullptr if condition not
     * met
     */
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
    find_only_single_inner_child(
        boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>& node);

    // Private constructor for creating snapshots
    SHAMapT(
        SHAMapNodeType type,
        boost::intrusive_ptr<SHAMapInnerNodeT<Traits>> rootNode,
        std::shared_ptr<std::atomic<int>> vCounter,
        int version,
        SHAMapOptions options);

protected:
    template <typename T>
    friend class PathFinderT;

    void
    handle_path_cow(PathFinderT<Traits>& path_finder);

    SetResult
    set_item_reference(
        boost::intrusive_ptr<MmapItem>& item,
        SetMode mode = SetMode::ADD_OR_UPDATE);

    SetResult
    set_item_collapsed(
        boost::intrusive_ptr<MmapItem>& item,
        SetMode mode = SetMode::ADD_OR_UPDATE);
    bool
    remove_item_reference(const Key& key);

public:
    explicit SHAMapT(
        SHAMapNodeType type = tnACCOUNT_STATE,
        SHAMapOptions options = SHAMapOptions());

    ~SHAMapT();

    SetResult
    set_item(
        boost::intrusive_ptr<MmapItem>& item,
        SetMode mode = SetMode::ADD_OR_UPDATE);

    SetResult
    add_item(boost::intrusive_ptr<MmapItem>& item);

    SetResult
    update_item(boost::intrusive_ptr<MmapItem>& item);

    bool
    remove_item(const Key& key);

    /// Place a leaf at a specific tree position (by nodeID, not key).
    /// Creates inner nodes down to the nodeID's depth.
    /// Unlike set_item(key) which pushes to max depth, this places the
    /// leaf at the EXACT depth encoded in the nodeID.
    /// Only available for abbreviated tree traits.
    SetResult
    set_item_at(SHAMapNodeID const& pos, boost::intrusive_ptr<MmapItem>& item)
        requires(has_placeholders_v<Traits>);

    /// Check if a placeholder is needed at this position.
    /// Walks the path without modifying the tree.
    /// Returns true if the position is an unfilled gap.
    /// Returns false if a real node or placeholder already covers it.
    bool
    needs_placeholder(SHAMapNodeID const& pos) const
        requires(has_placeholders_v<Traits>);

    /// Place a hash-only placeholder at a specific tree position.
    /// Only available when Traits::supports_placeholders is true.
    /// Creates inner nodes along the path as needed.
    /// No-op if the position already has a real child.
    /// Returns true if the placeholder was set, false if skipped.
    bool
    set_placeholder(SHAMapNodeID const& pos, Hash256 const& hash)
        requires(has_placeholders_v<Traits>);

    /// Walk the abbreviated tree, visiting every node (real and placeholder).
    ///
    /// Callback signature:
    ///   void(SHAMapNodeID const& id, Hash256 const& hash,
    ///        SHAMapTreeNodeT<Traits> const* node)
    ///
    /// For real nodes (inner or leaf): node != nullptr, hash = node's hash.
    /// For placeholders: node == nullptr, hash = placeholder hash.
    /// Empty branches are not visited.
    ///
    /// Walk is depth-first, pre-order. Only available for abbreviated trees.
    template <typename Fn>
    void
    walk_abbreviated(Fn&& callback) const
        requires(has_placeholders_v<Traits>)
    {
        if (!root)
            return;

        // Stack: (inner node, nodeID of that inner)
        struct Frame
        {
            SHAMapInnerNodeT<Traits> const* inner;
            SHAMapNodeID id;
        };
        std::vector<Frame> stack;
        stack.push_back({root.get(), SHAMapNodeID::root()});

        while (!stack.empty())
        {
            auto [inner, inner_id] = stack.back();
            stack.pop_back();

            auto children = inner->get_children();

            for (int branch = 0; branch < 16; ++branch)
            {
                auto child_id = inner_id.child(branch);
                auto child = children->get_child(branch);

                if (child)
                {
                    auto child_hash = child->get_hash(options_);
                    callback(child_id, child_hash, child.get());

                    if (child->is_inner())
                    {
                        stack.push_back(
                            {static_cast<SHAMapInnerNodeT<Traits> const*>(
                                 child.get()),
                             child_id});
                    }
                }
                else if (children->has_placeholder(branch))
                {
                    auto const& ph_hash = children->get_placeholder(branch);
                    callback(child_id, ph_hash, nullptr);
                }
                // Empty branches: not visited
            }
        }
    }

    [[nodiscard]] bool
    has_item(const Key& key) const;

    Hash256
    get_hash() const;

    // Parallel hashing support - partition work across threads
    // threadId: 0-based thread index (0 to totalThreads-1)
    // totalThreads: Must be power of 2 (1, 2, 4, 8, 16)
    // Returns: function that performs this thread's portion of hashing
    std::function<void()>
    get_hash_job(int threadId, int totalThreads) const;

    // Only public CoW method - creates a snapshot
    std::shared_ptr<SHAMapT<Traits>>
    snapshot();

    void
    trie_json(std::ostream& os, TrieJsonOptions = {}) const;

    std::string
    trie_json_string(TrieJsonOptions options = {}) const;

    /// Serialize the tree to compact binary trie format.
    std::vector<uint8_t>
    trie_binary() const;

    void
    visit_items(const std::function<void(const MmapItem&)>& visitor) const;

    void
    invalidate_hash_recursive();

    boost::json::array
    items_json() const;

    /**
     * Get an item by its key
     *
     * @param key The key to look up
     * @return The item if found, or nullptr if not found
     */
    boost::intrusive_ptr<MmapItem>
    get_item(const Key& key) const;

    /**
     * Walk only nodes that have a specific version
     * This is useful for flushing only the delta to storage, not the entire
     * tree.
     *
     * @param target_version The version to look for (use -1 for root's version)
     * @param visitor Callback that receives each matching node. Return true to
     * continue.
     */
    void
    walk_new_nodes(
        const std::function<bool(
            const boost::intrusive_ptr<SHAMapTreeNodeT<Traits>>&)>& visitor,
        int target_version = -1) const;

    /**
     * Get the root node of the SHAMap
     *
     * @return Root node pointer
     */
    boost::intrusive_ptr<SHAMapInnerNodeT<Traits>>
    get_root() const
    {
        return root;
    }

    /**
     * Get the SHAMap options
     *
     * @return Options structure
     */
    const SHAMapOptions&
    get_options() const
    {
        return options_;
    }

    static LogPartition&
    get_log_partition()
    {
        return log_partition_;
    }

    /**
     * Collapses the entire tree by removing single-child inner nodes
     * where possible to optimize the tree structure
     *
     * This method optimizes the in-memory representation of the SHAMap by
     * collapsing long chains of inner nodes that have only a single child.
     * The optimization preserves the logical structure and hash computation
     * while reducing memory consumption and improving traversal efficiency.
     *
     * Note: When using a collapsed tree, the hash computation process must
     * account for the skipped nodes to maintain the same hash outcome as
     * a non-collapsed tree. This is handled internally by the hash algorithms.
     */
    void
    collapse_tree();

    /**
     * Creates a shallow copy of the root node and replaces the current root
     * without engaging CoW machinery. This is useful for serialization
     * scenarios where you need to modify node traits without affecting the
     * original tree.
     */
    void
    set_new_copied_root();

    int
    get_version() const
    {
        return current_version_;
    }

    /// Callback for from_trie_json: given a leaf's key hex and data JSON,
    /// return the raw item bytes (excluding the 32-byte key suffix).
    using LeafFromJsonCallback = std::function<boost::intrusive_ptr<MmapItem>(
        std::string const& key_hex,
        boost::json::value const& data)>;

    /// Reconstruct an abbreviated SHAMap from a JSON trie.
    ///
    /// The trie format (per SPEC 2.3):
    ///   String (64 hex) = placeholder hash
    ///   Object (keys "0"-"F") = inner node
    ///   Array [key_hex, data] = leaf
    ///
    /// Recursively walks the JSON, building the path as it descends.
    /// Calls set_item_at for leaves and set_placeholder for hash strings.
    /// The on_leaf callback converts the leaf JSON data to an MmapItem.
    ///
    /// @param trie     The JSON trie value (root object)
    /// @param type     Node type (tnTRANSACTION_MD or tnACCOUNT_STATE)
    /// @param on_leaf  Callback to create MmapItem from leaf JSON
    /// @return The reconstructed abbreviated SHAMap
    static SHAMapT<Traits>
    from_trie_json(
        boost::json::value const& trie,
        SHAMapNodeType type,
        LeafFromJsonCallback on_leaf)
        requires(has_placeholders_v<Traits>)
    {
        SHAMapOptions opts;
        opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
        opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
        SHAMapT<Traits> map(type, opts);

        // Recursive walker
        struct Walker
        {
            SHAMapT<Traits>& map;
            LeafFromJsonCallback& on_leaf;

            static Hash256
            hash_from_hex(std::string const& hex)
            {
                Hash256 result;
                for (size_t i = 0; i < 32 && i * 2 + 1 < hex.size(); ++i)
                {
                    unsigned int byte;
                    std::sscanf(hex.c_str() + i * 2, "%2x", &byte);
                    result.data()[i] = static_cast<uint8_t>(byte);
                }
                return result;
            }

            static int
            nibble_from_char(char c)
            {
                if (c >= '0' && c <= '9')
                    return c - '0';
                if (c >= 'A' && c <= 'F')
                    return c - 'A' + 10;
                if (c >= 'a' && c <= 'f')
                    return c - 'a' + 10;
                return -1;
            }

            void
            walk(
                boost::json::value const& node,
                uint8_t depth,
                uint8_t path[32])
            {
                if (node.is_string())
                {
                    // Placeholder hash
                    auto hex = std::string(node.as_string());
                    if (hex.size() == 64)
                    {
                        auto hash = hash_from_hex(hex);
                        Hash256 path_hash(path);
                        SHAMapNodeID nid(path_hash, depth);
                        map.set_placeholder(nid, hash);
                    }
                }
                else if (node.is_array())
                {
                    // Leaf: [key_hex, data]
                    auto const& arr = node.as_array();
                    if (arr.size() >= 2 && arr[0].is_string())
                    {
                        auto key_hex = std::string(arr[0].as_string());
                        auto item = on_leaf(key_hex, arr[1]);
                        if (item)
                        {
                            Hash256 path_hash(path);
                            SHAMapNodeID nid(path_hash, depth);
                            map.set_item_at(nid, item);
                        }
                    }
                }
                else if (node.is_object())
                {
                    // Inner node — recurse into children
                    auto const& obj = node.as_object();
                    for (auto const& kv : obj)
                    {
                        if (kv.key() == "__depth__")
                            continue;
                        if (kv.key().size() != 1)
                            continue;

                        int nibble = nibble_from_char(kv.key()[0]);
                        if (nibble < 0)
                            continue;

                        // Set nibble in path at current depth
                        uint8_t child_path[32];
                        std::memcpy(child_path, path, 32);
                        int byte_idx = depth / 2;
                        if (depth % 2 == 0)
                        {
                            child_path[byte_idx] =
                                (child_path[byte_idx] & 0x0F) | (nibble << 4);
                        }
                        else
                        {
                            child_path[byte_idx] =
                                (child_path[byte_idx] & 0xF0) | (nibble & 0xF);
                        }

                        walk(kv.value(), depth + 1, child_path);
                    }
                }
            }
        };

        uint8_t root_path[32] = {};
        Walker walker{map, on_leaf};
        walker.walk(trie, 0, root_path);

        return map;
    }

    /// Reconstruct an abbreviated SHAMap from binary trie data.
    ///
    /// Binary trie format (per SPEC 3.4):
    ///   4-byte LE branch header (2 bits per branch)
    ///   Then for each non-empty branch:
    ///     01 (leaf): [key: 32][data_len: varint][data]
    ///     10 (inner): recurse
    ///     11 (hash): [32 bytes]
    static SHAMapT<Traits>
    from_trie_binary(std::span<const uint8_t> data, SHAMapNodeType type)
        requires(has_placeholders_v<Traits>)
    {
        SHAMapOptions opts;
        opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
        opts.reference_hash_impl = ReferenceHashImpl::recursive_simple;
        SHAMapT<Traits> map(type, opts);

        struct Walker
        {
            SHAMapT<Traits>& map;
            std::span<const uint8_t> data;
            size_t pos = 0;

            void
            walk(uint8_t depth, uint8_t path[32])
            {
                uint32_t header = read_u32_le(data, pos);

                for (int i = 0; i < 16; i++)
                {
                    auto bt = get_branch_type(header, i);
                    if (bt == BranchType::empty)
                        continue;

                    // Build child path
                    uint8_t child_path[32];
                    std::memcpy(child_path, path, 32);
                    int byte_idx = depth / 2;
                    if (depth % 2 == 0)
                    {
                        child_path[byte_idx] = (child_path[byte_idx] & 0x0F) |
                            (static_cast<uint8_t>(i) << 4);
                    }
                    else
                    {
                        child_path[byte_idx] = (child_path[byte_idx] & 0xF0) |
                            (static_cast<uint8_t>(i) & 0xF);
                    }

                    if (bt == BranchType::leaf)
                    {
                        // key: 32 bytes
                        if (pos + 32 > data.size())
                            throw std::runtime_error(
                                "binary trie: unexpected end reading leaf key");
                        Hash256 key(data.data() + pos);
                        pos += 32;
                        // data_len: varint
                        size_t data_len = leb128_decode(data, pos);
                        if (pos + data_len > data.size())
                            throw std::runtime_error(
                                "binary trie: unexpected end reading leaf "
                                "data");
                        // Create item
                        Slice item_data(data.data() + pos, data_len);
                        auto item = boost::intrusive_ptr<MmapItem>(
                            OwnedItem::create(key, item_data));
                        pos += data_len;

                        Hash256 path_hash(child_path);
                        SHAMapNodeID nid(path_hash, depth + 1);
                        map.set_item_at(nid, item);
                    }
                    else if (bt == BranchType::inner)
                    {
                        walk(depth + 1, child_path);
                    }
                    else if (bt == BranchType::hash)
                    {
                        if (pos + 32 > data.size())
                            throw std::runtime_error(
                                "binary trie: unexpected end reading hash");
                        Hash256 hash(data.data() + pos);
                        pos += 32;
                        Hash256 path_hash(child_path);
                        SHAMapNodeID nid(path_hash, depth + 1);
                        map.set_placeholder(nid, hash);
                    }
                }
            }
        };

        uint8_t root_path[32] = {};
        Walker walker{map, data, 0};
        walker.walk(0, root_path);

        return map;
    }
};

// Define the static log partition declaration for all template instantiations
template <typename Traits>
LogPartition SHAMapT<Traits>::log_partition_("SHAMap");

// Type alias for backward compatibility
using SHAMap = SHAMapT<DefaultNodeTraits>;

}  // namespace catl::shamap