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
#include <memory>
#include <stack>
#include <stdexcept>
#include <string>
#include <vector>
#include <zstd.h>

namespace catl::v2 {

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
 * - Ledger data (headers + trees)
 * - Ledger index (at end for easy appending)
 */
class CatlV2Writer
{
public:
    explicit CatlV2Writer(const std::string& filename)
        : output_(filename, std::ios::binary | std::ios::out | std::ios::trunc)
    {
        if (!output_)
        {
            throw std::runtime_error("Failed to open output file: " + filename);
        }

        // Write placeholder header
        write_file_header();
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
     * @param tx_map Transaction tree (always present, may be empty)
     * @return true on success
     */
    bool
    write_ledger(
        const catl::common::LedgerInfo& ledger_info,
        const SHAMapS& state_map,
        const SHAMapS& tx_map)
    {
        try
        {
            // Record ledger entry for index
            LedgerIndexEntry index_entry;
            index_entry.sequence = ledger_info.seq;
            index_entry.header_offset = current_offset();

            // Write the canonical ledger header
            output_.write(
                reinterpret_cast<const char*>(&ledger_info),
                sizeof(ledger_info));
            stats_.total_bytes_written += sizeof(ledger_info);

            // Reserve space for TreesHeader (we'll fill it in later)
            TreesHeader trees_header{};
            auto trees_header_offset = current_offset();
            output_.write(
                reinterpret_cast<const char*>(&trees_header),
                sizeof(trees_header));
            stats_.total_bytes_written += sizeof(trees_header);

            // Write state tree
            auto state_root = state_map.get_root();
            if (!state_root)
            {
                LOGE("Cannot serialize ledger with null state root");
                return false;
            }
            index_entry.state_tree_offset = current_offset();
            auto state_start = current_offset();
            serialize_tree(state_root);
            trees_header.state_tree_size = current_offset() - state_start;

            // Write transaction tree (always present)
            auto tx_root = tx_map.get_root();
            if (!tx_root)
            {
                LOGE("Cannot serialize ledger with null tx root");
                return false;
            }
            index_entry.tx_tree_offset = current_offset();
            auto tx_start = current_offset();
            serialize_tree(tx_root);
            trees_header.tx_tree_size = current_offset() - tx_start;

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

            output_.flush();
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
                output_.write(
                    reinterpret_cast<const char*>(&entry), sizeof(entry));
                stats_.total_bytes_written += sizeof(entry);
            }

            // Update and rewrite the file header
            finalize_file_header(index_offset);

            output_.flush();
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
    current_offset()
    {
        return static_cast<std::uint64_t>(output_.tellp());
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

    /**
     * Write file header (placeholder - updated at end)
     */
    void
    write_file_header()
    {
        CatlV2Header header;
        output_.write(reinterpret_cast<const char*>(&header), sizeof(header));
        stats_.total_bytes_written += sizeof(header);
    }

    /**
     * Update header with final values
     */
    void
    finalize_file_header(std::uint64_t index_offset)
    {
        CatlV2Header header;
        header.ledger_count = ledger_count_;
        header.first_ledger_seq = first_ledger_seq_;
        header.last_ledger_seq = last_ledger_seq_;
        header.ledger_index_offset = index_offset;

        // Seek to beginning and rewrite header
        auto current_pos = output_.tellp();
        output_.seekp(0);
        output_.write(reinterpret_cast<const char*>(&header), sizeof(header));
        output_.seekp(current_pos);
    }

    /**
     * Write data at specific offset (for updating headers)
     */
    void
    write_at(std::uint64_t offset, const void* data, size_t size)
    {
        auto current_pos = output_.tellp();
        output_.seekp(offset);
        output_.write(reinterpret_cast<const char*>(data), size);
        output_.seekp(current_pos);
    }

    /**
     * Write a leaf node and return its offset
     *
     * Writes a leaf node to the output file with optional compression.
     * The leaf format is:
     *   [LeafHeader (36 bytes)][data (variable length)]
     *
     * Where LeafHeader contains:
     *   - 32-byte key
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
     * @param key The 32-byte key identifying this leaf
     * @param data The leaf data (SLE - Serialized Ledger Entry)
     * @param compress Whether to attempt compression (default: false)
     * @return File offset where this leaf was written
     */
    std::uint64_t
    write_leaf_node(const Key& key, const Slice& data, bool compress = false)
    {
        auto offset = current_offset();

        // Build leaf header
        LeafHeader header;
        std::memcpy(header.key.data(), key.data(), 32);
        header.size_and_flags = 0;
        header.set_data_size(data.size());
        header.set_compression_type(
            compress ? CompressionType::ZSTD : CompressionType::NONE);

        // Write header
        output_.write(reinterpret_cast<const char*>(&header), sizeof(header));
        stats_.total_bytes_written += sizeof(header);
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
                output_.write(
                    reinterpret_cast<const char*>(data.data()), data.size());
                stats_.total_bytes_written += data.size();
                stats_.leaf_bytes_written += data.size();
            }
            else if (compressed_size >= data.size())
            {
                // Compression didn't help, write uncompressed
                header.set_compression_type(CompressionType::NONE);
                // Rewrite header with updated compression type
                write_at(offset, &header, sizeof(header));
                output_.write(
                    reinterpret_cast<const char*>(data.data()), data.size());
                stats_.total_bytes_written += data.size();
                stats_.leaf_bytes_written += data.size();
            }
            else
            {
                // Compression successful, update header with compressed size
                header.set_data_size(compressed_size);
                // Rewrite header with correct compressed size
                write_at(offset, &header, sizeof(header));
                output_.write(
                    reinterpret_cast<const char*>(compressed_buffer.data()),
                    compressed_size);
                stats_.total_bytes_written += compressed_size;
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
            output_.write(
                reinterpret_cast<const char*>(data.data()), data.size());
            stats_.total_bytes_written += data.size();
            stats_.leaf_bytes_written += data.size();
        }

        stats_.leaf_nodes_written++;
        return offset;
    }

    /**
     * Write an inner node and return its offset
     */
    std::uint64_t
    write_inner_node(
        const boost::intrusive_ptr<SHAMapInnerNodeS>& inner,
        std::vector<std::uint64_t>& child_offsets)
    {
        auto offset = current_offset();

        // Build inner node header
        InnerNodeHeader header;
        header.bits.depth = inner->get_depth();
        header.bits.rfu = 0;
        header.child_types = build_child_types(inner);

        // Count non-empty children
        int child_count = header.count_children();

        // Write header
        output_.write(reinterpret_cast<const char*>(&header), sizeof(header));
        stats_.total_bytes_written += sizeof(header);
        stats_.inner_bytes_written += sizeof(header);

        // Write child offsets (8 bytes each for non-empty children)
        child_offsets.resize(child_count);
        size_t offsets_size = child_count * sizeof(std::uint64_t);
        output_.write(
            reinterpret_cast<const char*>(child_offsets.data()), offsets_size);
        stats_.total_bytes_written += offsets_size;
        stats_.inner_bytes_written += offsets_size;

        stats_.inner_nodes_written++;
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
     *      - Write header with child_types bitmap
     *      - Write placeholder offsets (zeros) for N children (N = non-empty)
     *      - Mark processed=true, save node_offset
     *      - Push children onto stack for processing
     *
     * 2. **Child Processing**:
     *    - Process children in order (0-15)
     *    - Collect offsets as children complete
     *    - CRITICAL: Offsets must be stored in bitmap order!
     *
     * 3. **Inner Node Completion**:
     *    - After all children processed, seek back to offset array location
     *    - Overwrite placeholder zeros with actual child offsets
     *    - Seek back to current write position
     *
     * ## File Writing Pattern
     *
     * The algorithm uses a two-phase approach for inner nodes:
     * - Phase 1: Write header + placeholder offsets, continue depth-first
     * - Phase 2: After children written, seek back and update offsets
     *
     * This requires file seeking but maintains depth-first layout for optimal
     * cache locality during reads.
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
     * - Inner node headers written before children (offsets updated later)
     * - Child offsets are ordered by their position in the bitmap
     * - Zero offset means bug (all valid nodes have offset > sizeof(header))
     */
    std::uint64_t
    serialize_tree(const boost::intrusive_ptr<SHAMapTreeNodeS>& root)
    {
        struct StackEntry
        {
            boost::intrusive_ptr<SHAMapTreeNodeS> node;
            int parent_depth;
            bool is_first_visit;
            // For inner nodes:
            boost::intrusive_ptr<SHAMapInnerNodeS> inner;
            std::vector<std::uint64_t> child_offsets;
            std::uint64_t inner_offset;
            int next_child_index;
            int child_count;  // Total number of non-empty children
            std::vector<int> child_positions;  // Maps child branch (0-15) to
                                               // offset array position
        };

        if (!root)
        {
            return 0;
        }

        std::stack<StackEntry> stack;
        stack.push({root, -1, true, nullptr, {}, 0, 0, 0, {}});

        std::uint64_t root_offset = 0;

        LOGD("Starting serialize_tree traversal");

        while (!stack.empty())
        {
            auto& entry = stack.top();

            if (entry.node->is_leaf())
            {
                // Check if already processed
                if (entry.node->processed)
                {
                    // Already written - use existing offset
                    std::uint64_t leaf_offset = entry.node->node_offset;

                    LOGD(
                        "Leaf already processed, using existing offset: ",
                        leaf_offset);
                    assert(
                        leaf_offset > sizeof(CatlV2Header) &&
                        "Invalid leaf offset");

                    if (stack.size() == 1)
                    {
                        root_offset = leaf_offset;
                    }

                    stack.pop();

                    // Update parent's child offset if needed
                    if (!stack.empty() && stack.top().inner)
                    {
                        auto& parent = stack.top();
                        // Find which child branch we just completed
                        int branch = -1;
                        for (int i = 0; i < 16; ++i)
                        {
                            if (parent.inner->get_child(i) == entry.node)
                            {
                                branch = i;
                                break;
                            }
                        }
                        assert(
                            branch >= 0 && branch < 16 &&
                            "Child not found in parent");

                        int offset_index = parent.child_positions[branch];
                        assert(
                            offset_index >= 0 &&
                            offset_index < parent.child_count &&
                            "Invalid offset index");

                        parent.child_offsets[offset_index] = leaf_offset;
                        LOGD(
                            "Set child offset[",
                            offset_index,
                            "] = ",
                            leaf_offset,
                            " for branch ",
                            branch);
                    }
                }
                else
                {
                    // Process leaf node
                    auto leaf =
                        boost::static_pointer_cast<SHAMapLeafNodeS>(entry.node);
                    auto item = leaf->get_item();
                    if (!item)
                    {
                        throw std::runtime_error("Leaf node has null item");
                    }

                    std::uint64_t leaf_offset = write_leaf_node(
                        item->key(),
                        item->slice(),
                        false);  // Default to no compression

                    LOGD("Wrote new leaf at offset: ", leaf_offset);
                    assert(
                        leaf_offset > sizeof(CatlV2Header) &&
                        "Invalid leaf offset");

                    // Mark as processed and save offset
                    entry.node->processed = true;
                    entry.node->node_offset = leaf_offset;

                    if (stack.size() == 1)
                    {
                        root_offset = leaf_offset;
                    }

                    stack.pop();

                    // Update parent's child offset if needed
                    if (!stack.empty() && stack.top().inner)
                    {
                        auto& parent = stack.top();
                        // Find which child branch we just completed
                        int branch = -1;
                        for (int i = 0; i < 16; ++i)
                        {
                            if (parent.inner->get_child(i) == entry.node)
                            {
                                branch = i;
                                break;
                            }
                        }
                        assert(
                            branch >= 0 && branch < 16 &&
                            "Child not found in parent");

                        int offset_index = parent.child_positions[branch];
                        assert(
                            offset_index >= 0 &&
                            offset_index < parent.child_count &&
                            "Invalid offset index");

                        parent.child_offsets[offset_index] = leaf_offset;
                        LOGD(
                            "Set child offset[",
                            offset_index,
                            "] = ",
                            leaf_offset,
                            " for branch ",
                            branch);
                    }
                }

                // Continue processing parent's children
                continue;
            }
            else
            {
                // Process inner node
                if (entry.is_first_visit)
                {
                    // Check if already processed
                    if (entry.node->processed)
                    {
                        // Already written - use existing offset
                        std::uint64_t inner_offset = entry.node->node_offset;

                        LOGD(
                            "Inner node already processed, using existing "
                            "offset: ",
                            inner_offset);
                        assert(
                            inner_offset > sizeof(CatlV2Header) &&
                            "Invalid inner offset");

                        if (stack.size() == 1)
                        {
                            root_offset = inner_offset;
                        }

                        stack.pop();

                        // Update parent's child offset if needed
                        if (!stack.empty() && stack.top().inner)
                        {
                            auto& parent = stack.top();
                            // Find which child branch we just completed
                            int branch = -1;
                            for (int i = 0; i < 16; ++i)
                            {
                                if (parent.inner->get_child(i) == entry.node)
                                {
                                    branch = i;
                                    break;
                                }
                            }
                            assert(
                                branch >= 0 && branch < 16 &&
                                "Child not found in parent");

                            int offset_index = parent.child_positions[branch];
                            assert(
                                offset_index >= 0 &&
                                offset_index < parent.child_count &&
                                "Invalid offset index");

                            parent.child_offsets[offset_index] = inner_offset;
                            LOGD(
                                "Set child offset[",
                                offset_index,
                                "] = ",
                                inner_offset,
                                " for branch ",
                                branch);
                        }
                    }
                    else
                    {
                        // First visit: write header and prepare for children
                        entry.inner =
                            boost::static_pointer_cast<SHAMapInnerNodeS>(
                                entry.node);

                        LOGD(
                            "Processing new inner node at depth ",
                            entry.inner->get_depth());

                        // Build child positions mapping
                        entry.child_positions.resize(16, -1);
                        entry.child_count = 0;
                        for (int i = 0; i < 16; ++i)
                        {
                            if (entry.inner->get_child(i))
                            {
                                entry.child_positions[i] = entry.child_count++;
                            }
                        }

                        LOGD(
                            "Inner node has ",
                            entry.child_count,
                            " non-empty children");
                        assert(
                            entry.child_count > 0 &&
                            "Inner node with no children");

                        // Resize child_offsets to match child count
                        entry.child_offsets.resize(entry.child_count, 0);

                        // Write inner node header with placeholder offsets
                        entry.inner_offset =
                            write_inner_node(entry.inner, entry.child_offsets);

                        LOGD(
                            "Wrote inner node header at offset: ",
                            entry.inner_offset);
                        assert(
                            entry.inner_offset > sizeof(CatlV2Header) &&
                            "Invalid inner offset");

                        // Mark as processed and save offset
                        entry.node->processed = true;
                        entry.node->node_offset = entry.inner_offset;

                        if (stack.size() == 1)
                        {
                            root_offset = entry.inner_offset;
                        }

                        entry.is_first_visit = false;
                        entry.next_child_index = 0;

                        // Don't push first child here - let the main loop
                        // handle it
                    }
                }

                // After first visit OR returning from a child - process next
                // child
                if (!entry.is_first_visit)
                {
                    // Look for next child to process
                    bool found_next = false;

                    for (int i = entry.next_child_index; i < 16; ++i)
                    {
                        auto child = entry.inner->get_child(i);
                        if (child)
                        {
                            entry.next_child_index = i + 1;
                            LOGD("Pushing child at branch ", i);

                            // Check if child is already processed
                            if (child->processed)
                            {
                                LOGD(
                                    "Child at branch ",
                                    i,
                                    " already processed with offset ",
                                    child->node_offset);

                                // Update offset directly without pushing to
                                // stack
                                int offset_index = entry.child_positions[i];
                                assert(
                                    offset_index >= 0 &&
                                    offset_index < entry.child_count &&
                                    "Invalid offset index");
                                entry.child_offsets[offset_index] =
                                    child->node_offset;

                                // Continue loop to find next child
                                continue;
                            }

                            // Child needs processing - push to stack
                            stack.push(
                                {child,
                                 entry.inner->get_depth(),
                                 true,
                                 nullptr,
                                 {},
                                 0,
                                 0,
                                 0,
                                 {}});
                            found_next = true;
                            break;
                        }
                    }

                    if (!found_next)
                    {
                        // All children processed - verify and update offsets
                        LOGD(
                            "All children processed for inner node at offset ",
                            entry.inner_offset);

                        // Verify all child offsets have been filled
                        for (int i = 0; i < entry.child_count; ++i)
                        {
                            assert(
                                entry.child_offsets[i] != 0 &&
                                "Child offset not set - this is a bug!");
                            LOGD(
                                "  Child offset[",
                                i,
                                "] = ",
                                entry.child_offsets[i]);
                        }

                        auto offset_position =
                            entry.inner_offset + sizeof(InnerNodeHeader);
                        write_at(
                            offset_position,
                            entry.child_offsets.data(),
                            entry.child_offsets.size() * sizeof(std::uint64_t));

                        stack.pop();

                        // Update parent's child offset if needed
                        if (!stack.empty() && stack.top().inner)
                        {
                            auto& parent = stack.top();
                            // Find which child branch we just completed
                            int branch = -1;
                            for (int i = 0; i < 16; ++i)
                            {
                                if (parent.inner->get_child(i) == entry.node)
                                {
                                    branch = i;
                                    break;
                                }
                            }
                            assert(
                                branch >= 0 && branch < 16 &&
                                "Child not found in parent");

                            int offset_index = parent.child_positions[branch];
                            assert(
                                offset_index >= 0 &&
                                offset_index < parent.child_count &&
                                "Invalid offset index");

                            parent.child_offsets[offset_index] =
                                entry.inner_offset;
                            LOGD(
                                "Set parent's child offset[",
                                offset_index,
                                "] = ",
                                entry.inner_offset,
                                " for branch ",
                                branch);
                        }
                    }
                }
            }
        }

        LOGD("serialize_tree complete, root offset = ", root_offset);
        return root_offset;
    }
};

}  // namespace catl::v2