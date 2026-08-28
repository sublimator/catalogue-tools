#pragma once

#include "catl-v2-structs.h"
#include "catl/common/ledger-info.h"
#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "shamap-custom-traits.h"

#include <boost/intrusive_ptr.hpp>
#include <cassert>
#include <cstring>
#include <fstream>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <stack>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <zstd.h>

namespace catl::v2 {

/**
 * Build child types bitmap from a SHAMapInnerNodeS
 */
inline std::uint32_t
build_child_types(const boost::intrusive_ptr<SHAMapInnerNodeS>& inner)
{
    std::uint32_t child_types = 0;

    for (int i = 0; i < 16; ++i)
    {
        auto child = inner->get_child(i);
        ChildType type;

        if (!child)
        {
            type = ChildType::EMPTY;
        }
        else if (child->is_inner())
        {
            type = ChildType::INNER;
        }
        else
        {
            type = ChildType::LEAF;
        }

        // Set 2 bits for this branch
        child_types |= (static_cast<std::uint32_t>(type) << (i * 2));
    }

    return child_types;
}

/**
 * Writer for CATL v2 format - multiple ledgers with canonical headers
 *
 * This writer creates a new catalogue format that:
 * - Stores multiple ledgers in a single file
 * - Uses canonical LedgerInfo format (compatible with rippled/xahaud)
 * - Supports incremental serialization via structural sharing
 * - Maintains an index for fast ledger lookup
 *
 * Key concepts (from serialized inner trees):
 * - **Structural sharing on disk**: Nodes written in previous snapshots are
 *   referenced by their file offset rather than re-written
 * - **Copy-on-Write aware**: Only writes nodes with processed=false
 * (new/modified)
 * - **Incremental serialization**: Each snapshot only adds its delta to the
 *   file
 * - **Parallel loading**: The root inner node provides natural parallelization
 *   points - its child_types bitmap shows which children exist, and the child
 *   offsets array points to each subtree that can be processed independently
 *
 * Workflow:
 * 1. First ledger: All nodes have processed=false, write everything
 * 2. Snapshot + modify: Creates new nodes with processed=false
 * 3. Next ledger: Skip processed=true nodes (use existing offsets),
 *    only write new nodes
 *
 * This achieves the same structural sharing as in-memory CoW but persisted
 * to disk, allowing efficient storage of ledger history where each ledger
 * only adds its changes rather than duplicating the entire state.
 *
 * File layout:
 * - CatlV2Header
 * - One canonical empty-root inner
 * - Ledger data (headers + trees)
 * - Ledger index
 * - Optional immutable node-by-hash index (published after sealing)
 */
class CatlV2Writer
{
public:
    enum class PhysicalNodeKind : std::uint8_t { inner = 1, leaf = 2 };

    struct PhysicalNodeObservation
    {
        std::array<std::uint8_t, 32> hash;
        std::uint64_t offset;
        PhysicalNodeKind kind;
    };

    using PhysicalNodeObserver =
        std::function<void(PhysicalNodeObservation const&)>;

    explicit CatlV2Writer(
        const std::string& filename,
        uint32_t network_id = 0,
        PhysicalNodeObserver node_observer = {})
        : output_(filename, std::ios::binary | std::ios::out | std::ios::trunc)
        , node_observer_(std::move(node_observer))
    {
        if (!output_)
        {
            throw std::runtime_error("Failed to open output file: " + filename);
        }

        // Set network ID in header
        header_.network_id = network_id;

        // Set endianness marker for the current platform
        header_.endianness = get_host_endianness();
        header_.ledger_data_offset =
            sizeof(CatlV2Header) + sizeof(InnerNodeHeader);

        // Write placeholder header
        write_file_header();
        auto const empty_offset = write_canonical_empty_tree();
        if (empty_offset != sizeof(CatlV2Header) ||
            current_offset() != header_.ledger_data_offset)
            throw std::runtime_error(
                "Canonical empty root violated CATL2 prelude layout");
    }

    ~CatlV2Writer()
    {
        if (output_.is_open())
        {
            output_.close();
        }
    }

    /**
     * Write a complete ledger (header + state tree + tx tree)
     *
     * @param ledger_info The canonical ledger header
     * @param state_map The state tree for this ledger
     * @param tx_map Transaction tree (logically present, may be empty)
     * @return true on success
     */
    bool
    write_ledger(
        const catl::common::LedgerInfo& ledger_info,
        const catl::v2::serialization::SHAMapS& state_map,
        const catl::v2::serialization::SHAMapS& tx_map)
    {
        try
        {
            if (!output_.is_open())
                throw std::runtime_error("CATL2 writer is not open");

            // Record ledger entry for index
            LedgerIndexEntry index_entry;
            index_entry.sequence = ledger_info.seq;
            index_entry.header_offset = current_offset();

            // Write the canonical ledger header
            append_bytes(&ledger_info, sizeof(ledger_info));

            // Reserve space for TreesHeader (we'll fill it in later)
            TreesHeader trees_header{};
            auto trees_header_offset = current_offset();
            append_bytes(&trees_header, sizeof(trees_header));

            // CRITICAL: Compute hashes for both trees before serialization
            // This populates the cached hash in every node
            state_map.get_hash();  // Trigger recursive hash computation on
                                   // state tree
            auto const tx_hash = tx_map.get_hash();  // Trigger recursive hash
                                                     // computation on tx tree

            auto state_root = state_map.get_root();
            if (!state_root)
            {
                LOGE("Cannot serialize ledger with null state root");
                return false;
            }

            // Write state-tree delta (now with hashes computed). Child-first
            // serialization means the root is generally the last new node,
            // and a fully shared tree may append no nodes at all, so only the
            // returned offset is authoritative.
            auto state_start = current_offset();
            index_entry.state_tree_offset = serialize_tree(state_root);
            trees_header.state_tree_size = current_offset() - state_start;

            // A zero transaction hash names the canonical empty tree. Keep
            // one physical empty inner per pack so root/diff/index consumers
            // can use the same node-shaped path as every other tree, without
            // turning the streaming writer into a general deduplication
            // cache. The pack prelude already emitted the canonical root, so
            // every empty tree contributes no ledger-local bytes.
            if (tx_hash == Hash256::zero())
            {
                auto const tx_start = current_offset();
                index_entry.tx_tree_offset = write_canonical_empty_tree();
                trees_header.tx_tree_size = current_offset() - tx_start;
            }
            else
            {
                auto tx_root = tx_map.get_root();
                if (!tx_root)
                {
                    LOGE("Cannot serialize non-empty ledger with null tx root");
                    return false;
                }

                // Write transaction-tree delta (now with hashes computed).
                auto tx_start = current_offset();
                index_entry.tx_tree_offset = serialize_tree(tx_root);
                trees_header.tx_tree_size = current_offset() - tx_start;
            }

            // Go back and write the actual tree sizes
            write_at(trees_header_offset, &trees_header, sizeof(trees_header));

            // Add to ledger index
            ledger_index_.push_back(index_entry);
            ledger_count_++;

            // Update sequence range
            if (ledger_count_ == 1)
            {
                first_ledger_seq_ = ledger_info.seq;
            }
            last_ledger_seq_ = ledger_info.seq;

            return true;
        }
        catch (const std::exception& e)
        {
            LOGE("Failed to write ledger: ", e.what());
            return false;
        }
    }

    /**
     * Finalize the file by writing the index and updating the header
     */
    bool
    finalize()
    {
        try
        {
            // Record where the index starts
            std::uint64_t index_offset = current_offset();

            // Write the ledger index
            for (const auto& entry : ledger_index_)
            {
                append_bytes(&entry, sizeof(entry));
            }

            // Update and rewrite the file header
            finalize_file_header(index_offset);

            auto const stream_position = output_.tellp();
            if (stream_position < 0 ||
                static_cast<std::uint64_t>(stream_position) != append_offset_)
                throw std::runtime_error(
                    "CATL2 append cursor disagrees with output stream");

            output_.flush();
            if (!output_)
                throw std::runtime_error("Failed to flush finalized CATL2");
            output_.close();
            if (output_.fail())
                throw std::runtime_error("Failed to close finalized CATL2");
            return true;
        }
        catch (const std::exception& e)
        {
            LOGE("Failed to finalize file: ", e.what());
            return false;
        }
    }

    /**
     * Get current file position
     */
    std::uint64_t
    current_offset() const
    {
        return append_offset_;
    }

    /**
     * Get serialization statistics
     */
    struct Stats
    {
        std::uint64_t inner_nodes_written = 0;
        std::uint64_t leaf_nodes_written = 0;
        std::uint64_t total_bytes_written = 0;
        std::uint64_t compressed_leaves = 0;
        std::uint64_t uncompressed_size = 0;
        std::uint64_t compressed_size = 0;
        std::uint64_t inner_bytes_written = 0;  // Total bytes for inner nodes
        std::uint64_t leaf_bytes_written = 0;   // Total bytes for leaf nodes
    };

    const Stats&
    stats() const
    {
        return stats_;
    }

private:
    std::ofstream output_;
    Stats stats_;
    std::vector<LedgerIndexEntry> ledger_index_;
    std::uint64_t ledger_count_ = 0;
    std::uint64_t first_ledger_seq_ = 0;
    std::uint64_t last_ledger_seq_ = 0;
    CatlV2Header header_;  // Store header to preserve network_id
    PhysicalNodeObserver node_observer_;
    abs_off_t canonical_empty_tree_offset_ = 0;
    abs_off_t append_offset_ = 0;

    /**
     * Append bytes at the one authoritative logical output cursor.
     *
     * Random-access header patches restore the stream to this cursor without
     * changing it. A cold tellp() check at finalization validates the tracked
     * position without paying a tellp()/lseek cost for every physical node.
     */
    void
    append_bytes(const void* data, std::size_t size)
    {
        if (size > static_cast<std::size_t>(
                       std::numeric_limits<std::streamsize>::max()) ||
            size > std::numeric_limits<abs_off_t>::max() - append_offset_)
            throw std::runtime_error("CATL2 append size overflow");
        output_.write(
            reinterpret_cast<const char*>(data),
            static_cast<std::streamsize>(size));
        if (!output_)
            throw std::runtime_error("Failed to append CATL2 output");
        append_offset_ += size;
        stats_.total_bytes_written += size;
    }

    /**
     * Return the one canonical physical empty-root node for this pack.
     *
     * This deliberately caches one distinguished representation. It is not
     * arbitrary body deduplication: all ordinary nodes continue to stream,
     * and the sealed node index remains responsible for collapsing repeated
     * CAS identities.
     */
    abs_off_t
    write_canonical_empty_tree()
    {
        if (canonical_empty_tree_offset_ != 0)
            return canonical_empty_tree_offset_;

        canonical_empty_tree_offset_ = current_offset();

        InnerNodeHeader header{};
        header.child_types = 0;
        header.set_depth(0);
        header.set_rfu(0);
        header.overlay_mask = 0;
        header.hash.fill(0);

        append_bytes(&header, sizeof(header));
        stats_.inner_bytes_written += sizeof(header);
        stats_.inner_nodes_written++;

        if (node_observer_)
        {
            node_observer_(PhysicalNodeObservation{
                header.hash,
                canonical_empty_tree_offset_,
                PhysicalNodeKind::inner});
        }

        return canonical_empty_tree_offset_;
    }

    /**
     * Write file header (placeholder - updated at end)
     */
    void
    write_file_header()
    {
        append_bytes(&header_, sizeof(header_));
    }

    /**
     * Update header with final values
     */
    void
    finalize_file_header(std::uint64_t index_offset)
    {
        header_.ledger_count = ledger_count_;
        header_.first_ledger_seq = first_ledger_seq_;
        header_.last_ledger_seq = last_ledger_seq_;
        header_.ledger_index_offset = index_offset;

        write_at(0, &header_, sizeof(header_));
    }

    /**
     * Write data at specific offset (for updating headers)
     */
    void
    write_at(std::uint64_t offset, const void* data, size_t size)
    {
        output_.seekp(offset);
        output_.write(reinterpret_cast<const char*>(data), size);
        output_.seekp(append_offset_);
        if (!output_)
            throw std::runtime_error("Failed to patch CATL2 output");
    }

    /**
     * Write a leaf node and return its offset
     *
     * Writes a leaf node to the output file with optional compression.
     * The leaf format is:
     *   [LeafHeader (68 bytes)][data (variable length)]
     *
     * Where LeafHeader contains:
     *   - 32-byte key
     *   - 32-byte perma-cached hash (first 256 bits of SHA512)
     *   - 4-byte packed size_and_flags field:
     *     - Bits 0-23: data size (compressed or uncompressed)
     *     - Bits 24-27: compression type (see CompressionType enum)
     *     - Bits 28-31: reserved for future compression params
     *
     * Compression notes from experiments:
     *   - Individual leaf ZSTD compression only gets ~1.5x (disappointing)
     *   - Stream ZSTD achieves 7-10x due to 128MB rolling window
     *   - 32-byte keys repeat millions of times (prime dictionary candidate)
     *   - 20-byte accounts highly repetitive
     *   - Object templates/patterns are exploitable
     *   - Future: dictionary/template based compression per leaf
     *
     * @param leaf The leaf node (to get its hash)
     * @param key The 32-byte key identifying this leaf
     * @param data The leaf data (SLE - Serialized Ledger Entry)
     * @param compress Whether to attempt compression (default: false)
     * @return File offset where this leaf was written
     */
    std::uint64_t
    write_leaf_node(
        const boost::intrusive_ptr<SHAMapLeafNodeS>& leaf,
        const Key& key,
        const Slice& data,
        bool compress = false)
    {
        auto offset = current_offset();

        // Build leaf header with perma-cached hash
        LeafHeader header;
        std::memcpy(header.key.data(), key.data(), 32);

        // Copy the node's hash (computed earlier via map.get_hash())
        auto node_hash = leaf->valid_hash_or_throw();
        std::memcpy(header.hash.data(), node_hash.data(), 32);

        header.size_and_flags = 0;
        header.set_data_size(data.size());
        header.set_compression_type(
            compress ? CompressionType::ZSTD : CompressionType::NONE);

        // Write header
        append_bytes(&header, sizeof(header));
        stats_.leaf_bytes_written += sizeof(header);

        if (compress)
        {
            // Compress with zstd
            size_t const max_compressed_size = ZSTD_compressBound(data.size());
            std::vector<uint8_t> compressed_buffer(max_compressed_size);

            size_t const compressed_size = ZSTD_compress(
                compressed_buffer.data(),
                max_compressed_size,
                data.data(),
                data.size(),
                22  // Compression level (1-22, 3 is default)
            );

            if (ZSTD_isError(compressed_size))
            {
                LOGE(
                    "ZSTD compression failed: ",
                    ZSTD_getErrorName(compressed_size));
                // Fall back to uncompressed
                header.set_compression_type(CompressionType::NONE);
                // Rewrite header with updated compression type
                write_at(offset, &header, sizeof(header));
                append_bytes(data.data(), data.size());
                stats_.leaf_bytes_written += data.size();
            }
            else if (compressed_size >= data.size())
            {
                // Compression didn't help, write uncompressed
                header.set_compression_type(CompressionType::NONE);
                // Rewrite header with updated compression type
                write_at(offset, &header, sizeof(header));
                append_bytes(data.data(), data.size());
                stats_.leaf_bytes_written += data.size();
            }
            else
            {
                // Compression successful, update header with compressed size
                header.set_data_size(compressed_size);
                // Rewrite header with correct compressed size
                write_at(offset, &header, sizeof(header));
                append_bytes(compressed_buffer.data(), compressed_size);
                stats_.leaf_bytes_written += compressed_size;

                // Track compression statistics
                stats_.compressed_leaves++;
                stats_.uncompressed_size += data.size();
                stats_.compressed_size += compressed_size;
            }
        }
        else
        {
            // Write raw data
            append_bytes(data.data(), data.size());
            stats_.leaf_bytes_written += data.size();
        }

        stats_.leaf_nodes_written++;
        if (node_observer_)
        {
            node_observer_(PhysicalNodeObservation{
                header.hash, offset, PhysicalNodeKind::leaf});
        }
        return offset;
    }

    /**
     * Write an inner node and return its offset
     */
    std::uint64_t
    write_inner_node(
        const boost::intrusive_ptr<SHAMapInnerNodeS>& inner,
        const std::vector<std::uint64_t>& child_offsets_abs)
    {
        auto offset = current_offset();

        // Build inner node header with perma-cached hash
        InnerNodeHeader header{};
        header.set_depth(inner->get_depth());
        header.set_rfu(0);
        header.child_types = build_child_types(inner);
        header.overlay_mask = 0;  // overlay disabled in this experimental phase

        // Copy the node's hash (computed earlier via map.get_hash())
        auto node_hash = inner->valid_hash_or_throw();
        std::memcpy(header.hash.data(), node_hash.data(), 32);

        auto const child_count =
            static_cast<std::size_t>(header.count_children());
        if (child_offsets_abs.size() != child_count)
            throw std::runtime_error(
                "Resolved CATL2 child-offset population disagrees with "
                "child bitmap");

        auto const offsets_position = offset + sizeof(InnerNodeHeader);
        std::vector<rel_off_t> relative_offsets(child_count);
        for (std::size_t i = 0; i < child_count; ++i)
        {
            if (child_offsets_abs[i] == 0)
                throw std::runtime_error(
                    "Cannot write CATL2 inner with an unresolved child");
            relative_offsets[i] = rel_from_abs(
                child_offsets_abs[i], slot_from_index(offsets_position, i));
        }

        // Append the complete node in one stream write. The parent follows
        // every newly written child, so no placeholder or seek-back is needed.
        auto const offsets_size = child_count * sizeof(rel_off_t);
        std::vector<std::uint8_t> serialized(sizeof(header) + offsets_size);
        std::memcpy(serialized.data(), &header, sizeof(header));
        if (offsets_size != 0)
            std::memcpy(
                serialized.data() + sizeof(header),
                relative_offsets.data(),
                offsets_size);
        append_bytes(serialized.data(), serialized.size());
        stats_.inner_bytes_written += serialized.size();

        stats_.inner_nodes_written++;
        if (node_observer_)
        {
            node_observer_(PhysicalNodeObservation{
                header.hash, offset, PhysicalNodeKind::inner});
        }
        return offset;
    }

    /**
     * Main serialization logic - depth first traversal using explicit stack
     *
     * ## Copy-on-Write (CoW) and Structural Sharing
     *
     * This method implements incremental serialization that leverages SHAMap's
     * Copy-on-Write semantics to achieve structural sharing on disk:
     *
     * 1. **Initial State**: When reading a ledger from CATL v1:
     *    - All nodes start with processed=false, node_offset=0
     *    - First serialize_tree() call writes entire tree to disk
     *    - After writing, each node has processed=true and a valid node_offset
     *
     * 2. **Snapshot & Modify**: For the next ledger:
     *    - state_map.snapshot() makes current tree immutable
     *    - Applying state deltas (via set_item) triggers CoW:
     *      - Creates new nodes from root to modified leaf
     *      - New nodes have processed=false, node_offset=0 (SerializedNode
     * defaults)
     *      - Unchanged subtrees retain their processed=true nodes
     *
     * 3. **Incremental Write**: When serialize_tree() is called again:
     *    - Nodes with processed=true are skipped (already on disk)
     *    - Only new/modified nodes (processed=false) are written
     *    - Parent nodes reference children by their file offsets
     *
     * ## Child Types Bitmap
     *
     * Each inner node has a 32-bit child_types field encoding all 16 children:
     * - 2 bits per child: 00=EMPTY, 01=INNER, 02=LEAF, 11=RFU
     * - Example: 0x55555555 = all 16 children are INNER nodes
     * - Child offsets array only includes non-empty children
     *
     * ## Algorithm
     *
     * Uses explicit stack for iterative depth-first traversal:
     *
     * 1. **Inner Node First Visit**:
     *    - If processed=true: use existing node_offset, skip subtree
     *    - If processed=false:
     *      - Reserve an in-memory vector for N resolved child offsets
     *      - Push each unprocessed child onto the stack
     *
     * 2. **Child Processing**:
     *    - Process children in order (0-15)
     *    - Collect offsets as children complete
     *    - CRITICAL: Offsets must be stored in bitmap order!
     *
     * 3. **Inner Node Completion**:
     *    - Convert every resolved absolute child offset to a signed,
     *      self-relative offset
     *    - Append the complete header and offsets once
     *    - Publish node_offset and processed only after the append and
     *      physical-node observation complete
     *
     * ## File Writing Pattern
     *
     * The traversal is depth-first post-order. New descendants precede their
     * parent, while already processed subtrees are referenced at their stored
     * offsets without traversal or rewriting.
     *
     * ## Example
     *
     * Inner node with child_types=0x0000A802:
     * - Binary: 00000000 00000000 10101000 00000010
     * - Children at: 1(LEAF), 11(INNER), 13(LEAF), 15(LEAF)
     * - Offsets array has 4 entries: [child1_offset, child11_offset, ...]
     *
     * ## Invariants
     *
     * - Nodes are written exactly once (first time processed=false)
     * - New children are written before their new parent
     * - Child offsets are ordered by their position in the bitmap
     * - Zero offset means unresolved and is never serialized
     */
    std::uint64_t
    serialize_tree(const boost::intrusive_ptr<SHAMapTreeNodeS>& root)
    {
        struct StackEntry
        {
            explicit StackEntry(
                boost::intrusive_ptr<SHAMapTreeNodeS> node_,
                std::optional<std::size_t> parent_slot_ = std::nullopt)
                : node(std::move(node_)), parent_slot(parent_slot_)
            {
            }

            boost::intrusive_ptr<SHAMapTreeNodeS> node;
            boost::intrusive_ptr<SHAMapInnerNodeS> inner;
            std::vector<std::uint64_t> child_offsets;
            int next_child_branch = 0;
            std::size_t next_child_slot = 0;
            std::optional<std::size_t> parent_slot;
        };

        if (!root)
        {
            return 0;
        }

        std::stack<StackEntry> stack;
        stack.emplace(root);

        std::uint64_t root_offset = 0;

        auto complete_top = [&](std::uint64_t offset) {
            auto const parent_slot = stack.top().parent_slot;
            stack.pop();
            if (!parent_slot)
            {
                if (!stack.empty())
                    throw std::runtime_error(
                        "CATL2 traversal lost a parent slot");
                root_offset = offset;
                return;
            }
            if (stack.empty() ||
                *parent_slot >= stack.top().child_offsets.size())
                throw std::runtime_error(
                    "CATL2 traversal produced an invalid parent slot");
            stack.top().child_offsets[*parent_slot] = offset;
        };

        LOGD("Starting serialize_tree traversal");

        while (!stack.empty())
        {
            auto& entry = stack.top();

            if (entry.node->processed)
            {
                auto const offset = entry.node->node_offset;
                if (offset == 0)
                    throw std::runtime_error(
                        "Processed CATL2 node has no physical offset");
                complete_top(offset);
                continue;
            }

            if (entry.node->is_leaf())
            {
                auto leaf =
                    boost::static_pointer_cast<SHAMapLeafNodeS>(entry.node);
                auto item = leaf->get_item();
                if (!item)
                    throw std::runtime_error("Leaf node has null item");

                auto const leaf_offset =
                    write_leaf_node(leaf, item->key(), item->slice(), false);
                entry.node->node_offset = leaf_offset;
                entry.node->processed = true;
                complete_top(leaf_offset);
                continue;
            }

            if (!entry.inner)
            {
                entry.inner =
                    boost::static_pointer_cast<SHAMapInnerNodeS>(entry.node);
                std::size_t child_count = 0;
                for (int branch = 0; branch < 16; ++branch)
                    if (entry.inner->get_child(branch))
                        ++child_count;
                entry.child_offsets.resize(child_count, 0);
            }

            bool pushed_child = false;
            while (entry.next_child_branch < 16)
            {
                auto const branch = entry.next_child_branch++;
                auto child = entry.inner->get_child(branch);
                if (!child)
                    continue;

                auto const slot = entry.next_child_slot++;
                if (slot >= entry.child_offsets.size())
                    throw std::runtime_error(
                        "CATL2 traversal child population overflow");
                if (child->processed)
                {
                    if (child->node_offset == 0)
                        throw std::runtime_error(
                            "Processed CATL2 child has no physical offset");
                    entry.child_offsets[slot] = child->node_offset;
                    continue;
                }

                stack.emplace(std::move(child), slot);
                pushed_child = true;
                break;
            }

            if (pushed_child)
                continue;

            if (entry.next_child_slot != entry.child_offsets.size())
                throw std::runtime_error(
                    "CATL2 traversal child population underflow");

            auto const inner_offset =
                write_inner_node(entry.inner, entry.child_offsets);
            entry.node->node_offset = inner_offset;
            entry.node->processed = true;
            complete_top(inner_offset);
        }

        LOGD("serialize_tree complete, root offset = ", root_offset);
        return root_offset;
    }
};

}  // namespace catl::v2
