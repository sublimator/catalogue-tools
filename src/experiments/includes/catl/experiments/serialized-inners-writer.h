#pragma once

#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/experiments/serialized-inners-structs.h"
#include "catl/experiments/shamap-custom-traits.h"

#include <boost/intrusive_ptr.hpp>
#include <cstring>
#include <fstream>
#include <memory>
#include <stack>
#include <stdexcept>
#include <string>
#include <vector>
#include <zstd.h>

namespace catl::experiments {

/**
 * Writer for serialized inner node trees with on-disk structural sharing
 *
 * This writer enables incremental serialization of SHAMap snapshots by only
 * writing nodes that have changed since the last serialization. It uses the
 * 'processed' flag and 'node_offset' fields in SerializedNode to track which
 * nodes have already been written to disk.
 *
 * Key concepts:
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
 * 1. First map: All nodes have processed=false, write everything
 * 2. Snapshot + modify: Creates new nodes with processed=false
 * 3. Second map: Skip processed=true nodes (use existing offsets),
 *    only write new nodes
 *
 * This achieves the same structural sharing as in-memory CoW but persisted
 * to disk, allowing efficient storage of ledger history where each ledger
 * only adds its changes rather than duplicating the entire state.
 */
class SerializedInnerWriter
{
public:
    explicit SerializedInnerWriter(const std::string& filename)
        : output_(filename, std::ios::binary | std::ios::out | std::ios::trunc)
    {
        if (!output_)
        {
            throw std::runtime_error("Failed to open output file: " + filename);
        }

        // Write placeholder header
        write_header();
    }

    ~SerializedInnerWriter()
    {
        if (output_.is_open())
        {
            output_.close();
        }
    }

    /**
     * Serialize a SHAMapS to file
     *
     * @param map The map to serialize
     * @return true on success
     */
    bool
    serialize_map(const SHAMapS& map)
    {
        try
        {
            auto root = map.get_root();
            if (!root)
            {
                LOGE("Cannot serialize map with null root");
                return false;
            }

            // Serialize the tree
            serialize_tree(root);

            // Update header with final values
            finalize_header(map.get_hash());

            output_.flush();
            return true;
        }
        catch (const std::exception& e)
        {
            LOGE("Serialization failed: ", e.what());
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
    };

    const Stats&
    stats() const
    {
        return stats_;
    }

private:
    std::ofstream output_;
    Stats stats_;

    /**
     * Write file header (placeholder - updated at end)
     */
    void
    write_header()
    {
        SerializedTreeHeader header;
        output_.write(reinterpret_cast<const char*>(&header), sizeof(header));
        stats_.total_bytes_written += sizeof(header);
    }

    /**
     * Update header with final values
     */
    void
    finalize_header(const Hash256& root_hash)
    {
        SerializedTreeHeader header;
        header.root_offset =
            sizeof(SerializedTreeHeader);  // Root starts after header
        header.total_inners = stats_.inner_nodes_written;
        header.total_leaves = stats_.leaf_nodes_written;
        std::memcpy(header.root_hash.data(), root_hash.data(), 32);

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
     * Writes a leaf node to the output file with optional zstd compression.
     * The leaf format is:
     *   [LeafHeader (36 bytes)][data (variable length)]
     *
     * Where LeafHeader contains:
     *   - 32-byte key
     *   - 4-byte packed size_and_flags field:
     *     - Bits 0-23: data size (compressed or uncompressed)
     *     - Bit 24: is_compressed flag
     *     - Bits 25-31: reserved
     *
     * Compression behavior:
     *   - Only compresses if requested AND compression reduces size
     *   - Falls back to uncompressed if compression fails or doesn't help
     *   - Uses zstd level 3 (good balance of speed/compression)
     *   - Tracks compression statistics for analysis
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
        header.set_compressed(compress);

        // Write header
        output_.write(reinterpret_cast<const char*>(&header), sizeof(header));
        stats_.total_bytes_written += sizeof(header);

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
                header.set_compressed(false);
                // Rewrite header with updated flag
                write_at(offset, &header, sizeof(header));
                output_.write(
                    reinterpret_cast<const char*>(data.data()), data.size());
                stats_.total_bytes_written += data.size();
            }
            else if (compressed_size >= data.size())
            {
                // Compression didn't help, write uncompressed
                header.set_compressed(false);
                // Rewrite header with updated flag
                write_at(offset, &header, sizeof(header));
                output_.write(
                    reinterpret_cast<const char*>(data.data()), data.size());
                stats_.total_bytes_written += data.size();
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

        // Write child offsets (8 bytes each for non-empty children)
        child_offsets.resize(child_count);
        output_.write(
            reinterpret_cast<const char*>(child_offsets.data()),
            child_count * sizeof(std::uint64_t));
        stats_.total_bytes_written += child_count * sizeof(std::uint64_t);

        stats_.inner_nodes_written++;
        return offset;
    }

    /**
     * Main serialization logic - depth first traversal using explicit stack
     *
     * This method respects the 'processed' flag for incremental serialization:
     * - Nodes with processed=true are skipped (already written to disk)
     * - Their existing node_offset is used in parent references
     * - Only nodes with processed=false are written (new/modified nodes)
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
        };

        if (!root)
        {
            return 0;
        }

        std::stack<StackEntry> stack;
        stack.push({root, -1, true, nullptr, {}, 0, 0});

        std::uint64_t root_offset = 0;

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

                    if (stack.size() == 1)
                    {
                        root_offset = leaf_offset;
                    }

                    stack.pop();

                    // Update parent's child offset if needed
                    if (!stack.empty() && stack.top().inner)
                    {
                        auto& parent = stack.top();
                        parent.child_offsets.push_back(leaf_offset);
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
                        true);  // Enable compression

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
                        parent.child_offsets.push_back(leaf_offset);
                    }
                }
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

                        if (stack.size() == 1)
                        {
                            root_offset = inner_offset;
                        }

                        stack.pop();

                        // Update parent's child offset if needed
                        if (!stack.empty() && stack.top().inner)
                        {
                            auto& parent = stack.top();
                            parent.child_offsets.push_back(inner_offset);
                        }
                    }
                    else
                    {
                        // First visit: write header and prepare for children
                        entry.inner =
                            boost::static_pointer_cast<SHAMapInnerNodeS>(
                                entry.node);

                        // Write inner node header with placeholder offsets
                        entry.inner_offset =
                            write_inner_node(entry.inner, entry.child_offsets);

                        // Mark as processed and save offset
                        entry.node->processed = true;
                        entry.node->node_offset = entry.inner_offset;

                        if (stack.size() == 1)
                        {
                            root_offset = entry.inner_offset;
                        }

                        entry.is_first_visit = false;
                        entry.next_child_index = 0;

                        // Find and push first child
                        for (int i = 0; i < 16; ++i)
                        {
                            auto child = entry.inner->get_child(i);
                            if (child)
                            {
                                entry.next_child_index = i + 1;
                                stack.push(
                                    {child,
                                     entry.inner->get_depth(),
                                     true,
                                     nullptr,
                                     {},
                                     0,
                                     0});
                                break;
                            }
                        }
                    }
                }
                else
                {
                    // Returning from a child - check if we have more children
                    bool found_next = false;

                    for (int i = entry.next_child_index; i < 16; ++i)
                    {
                        auto child = entry.inner->get_child(i);
                        if (child)
                        {
                            entry.next_child_index = i + 1;
                            stack.push(
                                {child,
                                 entry.inner->get_depth(),
                                 true,
                                 nullptr,
                                 {},
                                 0,
                                 0});
                            found_next = true;
                            break;
                        }
                    }

                    if (!found_next)
                    {
                        // All children processed - update offsets and pop
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
                            parent.child_offsets.push_back(entry.inner_offset);
                        }
                    }
                }
            }
        }

        return root_offset;
    }
};

}  // namespace catl::experiments