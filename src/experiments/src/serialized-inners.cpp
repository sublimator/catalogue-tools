/**
 * serialized-inners.cpp
 *
 * An experimental tool for exploring efficient serialization approaches for
 * SHAMap inner nodes. This tool evaluates strategies for compactly representing
 * the inner node structure while maintaining structural sharing capabilities
 * for Copy-on-Write.
 *
 * Key Design Goals:
 * 1. Compact binary representation of inner nodes (6 bytes per inner)
 * 2. Support for depth-first serialization with structural sharing
 * 3. Efficient deserialization with potential for parallel loading
 * 4. Integration with Copy-on-Write for memory-efficient snapshots
 */

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stack>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/experiments/serialized-inners-structs.h"
#include "catl/experiments/serialized-inners-writer.h"
#include "catl/experiments/shamap-custom-traits.h"
#include "catl/v1/catl-v1-reader.h"

using namespace catl::v1;
using namespace catl::experiments;
namespace po = boost::program_options;
namespace fs = boost::filesystem;

//----------------------------------------------------------
// File Format Structures
//----------------------------------------------------------

/**
 * Ledger metadata with offsets to serialized maps
 */
struct Ledger
{
    LedgerInfo ledger_info;
    std::uint64_t account_map_offset;
    std::uint64_t transaction_map_offset;
};

/**
 * Binary-searchable ledger index entry
 * 128 bits for cache line efficiency
 */
struct LedgerLookupEntry
{
    std::uint32_t ledger_index;
    std::uint64_t ledger_offset;  // Points to Ledger struct
    std::uint32_t rfu_padding;    // Maintains 16-byte alignment
};

/**
 * Proposed File Format:
 *
 * [Header]
 *   - Magic number, version, metadata
 *   - Body size
 *   - Footer size
 *
 * [Body]
 *   For each ledger:
 *     - LedgerInfo
 *     - AccountMap (depth-first serialized)
 *       - Inner nodes with child offsets
 *       - Leaf nodes inline with their parent's region
 *     - TransactionMap (same structure)
 *
 * [Footer]
 *   - LedgerLookupTable (sorted array for binary search)
 *   - Optional: Key-prefix index for random access
 *
 * Serialization Strategy:
 * - Depth-first order maximizes cache efficiency
 * - Leaves stored near their parent inner nodes
 * - Structural sharing via offset references
 * - Parallel deserialization possible with bookmark offsets
 */

//----------------------------------------------------------
// Serialization Implementation
//----------------------------------------------------------

/**
 * Serialize a tree in depth-first order, maintaining locality of reference
 *
 * Strategy:
 * 1. Process inner nodes depth-first
 * 2. Serialize each inner node followed by its direct leaf children
 * 3. Maintain offset bookkeeping for structural sharing
 *
 * @param root The root of the tree to serialize
 * @param output Buffer to write serialized data (future implementation)
 * @param current_offset Tracks position in serialized output
 * @return Number of nodes serialized (inners + leaves)
 */
size_t
serialize_depth_first_stack(
    const boost::intrusive_ptr<SHAMapInnerNodeS>& root,
    std::vector<uint8_t>& output
    [[maybe_unused]],  // Reserved for actual serialization
    std::uint64_t& current_offset)
{
    if (!root)
    {
        return 0;
    }

    size_t nodes_serialized = 0;
    std::stack<boost::intrusive_ptr<SHAMapInnerNodeS>> node_stack;

    // Start with the root node
    node_stack.push(root);

    while (!node_stack.empty())
    {
        auto current_node = node_stack.top();
        node_stack.pop();

        // Skip already processed nodes (for DAG support)
        if (!current_node || current_node->processed)
        {
            continue;
        }

        // Mark as processed and record offset
        current_node->processed = true;
        current_node->node_offset = current_offset;

        // Reserve space for InnerNodeHeader structure
        current_offset += sizeof(InnerNodeHeader);
        nodes_serialized++;

        LOGI("Serializing inner node at offset: ", current_node->node_offset);

        // Serialize all direct leaf children immediately after this inner
        // This maintains locality of reference for tree traversal
        for (int i = 0; i < 16; i++)
        {
            auto child = current_node->get_child(i);
            if (child && child->is_leaf())
            {
                LOGI("Serializing leaf child at offset: ", current_offset);

                auto item =
                    static_cast<SHAMapLeafNodeS*>(child.get())->get_item();
                // auto key = item->key();  // Reserved for future use
                auto size = item->slice().size();

                // Leaf format: [32-byte key][variable data]
                current_offset += 32 + size;
                nodes_serialized++;
            }
        }

        // Queue inner children for processing (reverse order for consistent
        // DFS)
        for (int i = 15; i >= 0; i--)
        {
            auto child = current_node->get_child(i);
            if (child && child->is_inner())
            {
                auto inner_child = boost::intrusive_ptr<SHAMapInnerNodeS>(
                    static_cast<SHAMapInnerNodeS*>(child.get()));
                node_stack.push(inner_child);
            }
        }
    }

    return nodes_serialized;
}

//----------------------------------------------------------
// Main Processing Logic
//----------------------------------------------------------

/**
 * Process multiple ledgers, demonstrating serialization concepts
 */
void
process_all_ledgers(const std::string& filename)
{
    Reader reader(filename);
    auto header = reader.header();

    LOGI(
        "Processing ledgers from ",
        header.min_ledger,
        " to ",
        header.max_ledger);

    // Initialize state map with CoW support
    SHAMapS map(catl::shamap::tnACCOUNT_STATE);
    map.snapshot();  // Enable CoW

    std::vector<uint8_t> serialized_output;
    std::uint64_t current_offset = 0;

    // Create a writer for actual binary output
    SerializedInnerWriter writer("test-serialized.bin");

    // Process subset for experimentation
    auto n_ledgers = header.min_ledger + 15000;
    auto max_ledger =
        std::min(static_cast<uint32_t>(n_ledgers), header.max_ledger);

    for (uint32_t ledger_seq = header.min_ledger; ledger_seq <= max_ledger;
         ledger_seq++)
    {
        LOGI("Processing ledger: ", ledger_seq);

        // Read ledger header
        reader.read_ledger_info();  // Skip for now

        // Read state map using owned items for proper CoW behavior
        map.snapshot();  // Create snapshot point
        reader.read_map_with_shamap_owned_items(
            map, catl::shamap::tnACCOUNT_STATE, true);

        // Demonstrate serialization concept
        auto root = map.get_root();
        if (root)
        {
            LOGI("Root node processed status: ", root->processed);

            // Serialize the tree structure (old method for comparison)
            size_t total_serialized = serialize_depth_first_stack(
                root, serialized_output, current_offset);

            LOGI(
                "Serialized ",
                total_serialized,
                " nodes for ledger ",
                ledger_seq);

            // Now actually write it to disk!
            if (ledger_seq == header.min_ledger)
            {  // Just do the first one for now
                LOGI("Writing ledger ", ledger_seq, " to binary file");
                if (writer.serialize_map(map))
                {
                    auto stats = writer.stats();
                    LOGI("Wrote ", stats.inner_nodes_written, " inner nodes");
                    LOGI("Wrote ", stats.leaf_nodes_written, " leaf nodes");
                    LOGI("Total bytes written: ", stats.total_bytes_written);
                }
            }
        }

        // Skip transaction map
        reader.skip_map(catl::shamap::tnTRANSACTION_MD);
    }

    LOGI("Total serialized size would be: ", current_offset, " bytes");
}

/**
 * Main entry point
 */
int
main([[maybe_unused]] int argc, [[maybe_unused]] char* argv[])
{
    Logger::set_level(LogLevel::INFO);
    LOGI("Starting serialized inner node experiment");

    // TODO: Accept filename as command line argument
    process_all_ledgers(
        "/Users/nicholasdudfield/projects/xahau-history/"
        "cat.2000000-2010000.compression-0.catl");

    return 0;
}