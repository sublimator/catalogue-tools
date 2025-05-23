/**
 * serialized-inners.cpp
 *
 * An experimental tool for exploring serialization approaches for SHAMap inner
 * nodes. This tool is used to evaluate different strategies for efficiently
 * representing and serializing the inner node structure of SHAMaps.
 */

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/v1/catl-v1-writer.h"
#include "shamap-custom-traits.h"

using namespace catl::v1;
namespace po = boost::program_options;
namespace fs = boost::filesystem;

enum class ChildType : std::uint8_t {
    INNER = 0,
    LEAF = 1,
    EMPTY = 2,
    RFU = 3  //
};

// We decided to use 6 bytes per inner but it should potentially benefit
// from better "alignment" optimization than using 5 bytes per inner
// "Using 6 bytes (2+4) instead of 5 bytes (1+4) improves CPU memory access
// efficiency through better alignment, reduces padding waste in arrays,
// optimizes cache line usage (10 nodes per line), and provides 8 additional
// flag bits with only a 20% size increase."
// TODO: get some stats from large ledgers and see how many inner nodes change
// per ledger
struct DepthAndFlags
{
    std::uint16_t depth : 6;  // 6 bits for depth (0-63)
    std::uint16_t rfu : 10;   // 10 bits reserved for future use
};

struct InnerNode
{
    union
    {
        std::uint16_t depth_plus;  // Access as raw byte for serialization
        DepthAndFlags bits;        // Access as structured fields
    };
    std::uint32_t
        child_types;  // 2 bits per child × 16 children = 32 bits total
};

struct ChildSlot
{
    // 640 kilobytes ought to be enough for anyone, that damning quote of the
    // ages If you start at the root node, then write out all the child nodes
    // and their slots one depth at a time, then you'd have children not far
    // from each other, and perhaps you could actually "get away" with using a
    // smaller type for the offset? 32 bits? This needs consideration. We looked
    // at this and decide that depth first is the way to go, and ALSO, because
    // we'll be pointing to nodes from distant maps past then we're going to
    // need the full 64 bits.
    std::uint64_t offset;  // Offset of child node in body
};
;
struct Ledger
{
    LedgerInfo ledger_info;
    std::uint64_t account_map_offset;
    std::uint64_t transaction_map_offset;
};

// 128 bits
struct LedgerLookupEntry
{
    std::uint32_t ledger_index;
    std::uint64_t ledger_offset;  // to Ledger Struct
    std::uint32_t rfu_padding;    // rfu
};

// File format
// Header
//    Body size
//    Footer size
// Body
//    Ledgers:
//    LedgerInfo
//    AccountMap
//    Leaf nodes
//    Root Node (potentially with body offset pointers to previous ledger inner
//    nodes, which can point to previous ledger leaf nodes)
//    TransactionMap
//    Leaf nodes
//    Inner nodes
// Footer
// LedgerLookupTable ( is a bisectable index of the ledgers in the file )

// Starts at root node
// You'd serialize the `InnerNode` struct, then leave offset slots for each
// child that is present i.e. if there are only 5 children, then you'd leave 5
// empty slots what is a slot? it's a relative pointer to where the child node
// is serialized in the body so of course you'd need to know the offset of the
// body it's probably easiest to write a recursive function that returns the
// offset of the serialized node afterwards converting it into a stack based
// implementation, but recursive is easier to start with stack overflows are a
// problem so it's probably easiest to use a stack straight of the bat but
// conceptually it's the same thing

// Do not try and interleave the serialization of the inners and leaves
// Separate them, so which goes first? The leaves first means you'd have the
// offsets ready to write for the inners For inners you'd do breadth first
// serialization which may help with locality of reference?! Is that actaully
// true though, actually, it does sound true Ordering of Nodes Leaves: Sorted by
// key order (lexicographically), which enables efficient binary search, range
// scans, and predictable access patterns. This organization allows for quick
// location of data when accessing by key directly, independent of tree
// traversal. Inners: Depth-first traversal order, which keeps nodes in the same
// subtree close together in memory. This maximizes cache efficiency when
// traversing paths for keys with common prefixes and performing operations on
// subtrees. It matches the natural recursive structure of tree algorithms and
// provides the best overall performance for tree-based operations.

// Actually, if we put all the leaves first we aren't going to be able to make
// easy use of parallelism

/**
 * Stack-based depth-first serialization: serialize inner node + its direct
 * leaves, then recurse Returns the total number of nodes serialized (inners +
 * leaves)
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

        // Skip if already processed or null
        if (!current_node || current_node->processed)
        {
            continue;
        }

        // Serialize this inner node first
        current_node->processed = true;
        current_node->node_offset = current_offset;
        current_offset += sizeof(InnerNode);
        nodes_serialized++;
        LOGI("Serializing inner node at offset: ", current_node->node_offset);

        // Serialize all direct leaf children immediately after this inner
        for (int i = 0; i < 16; i++)
        {
            auto child = current_node->get_child(i);
            if (child && child->is_leaf())
            {
                LOGI("Serializing leaf child at offset: ", current_offset);

                auto item = boost::static_pointer_cast<SHAMapLeafNodeS>(child)
                                ->get_item();
                // auto key = item->key();  // Reserved for future use
                auto size = item->slice().size();

                // Serialize leaf: 32-byte key + data
                current_offset += 32 + size;
                nodes_serialized++;
            }
        }

        // Push inner children onto stack for processing (in reverse order for
        // consistent traversal)
        for (int i = 15; i >= 0; i--)
        {
            auto child = current_node->get_child(i);
            if (child && child->is_inner())
            {
                auto inner_child =
                    boost::static_pointer_cast<SHAMapInnerNodeS>(child);
                node_stack.push(inner_child);
            }
        }
    }

    return nodes_serialized;
}

/**
 * Process all ledgers in the CATL file
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

    auto map = SHAMapS(catl::shamap::tnACCOUNT_STATE);
    map.snapshot();

    std::vector<uint8_t> serialized_output;
    std::uint64_t current_offset = 0;

    auto n_ledgers = header.min_ledger + 15000;
    auto max_ledger =
        std::min(static_cast<uint32_t>(n_ledgers), header.max_ledger);
    // Process each ledger in sequence
    for (uint32_t ledger_seq = header.min_ledger; ledger_seq <= max_ledger;
         ledger_seq++)
    {
        LOGI("Processing ledger: ", ledger_seq);

        // Read ledger info
        reader.read_ledger_info();  // Skip the ledger info for now

        // Read the account state map (allow delta updates)
        // Using the new owned items approach - much better for CoW!
        // Each item owns its memory, so CoW can properly garbage collect
        // when references are dropped. No more storage vector issues!
        map.snapshot();
        reader.read_map_with_shamap_owned_items(
            map, catl::shamap::tnACCOUNT_STATE, true);

        // Get root and serialize depth-first: inner + direct leaves, then
        // recurse
        auto root = map.get_root();
        if (root)
        {
            LOGI("Root node has been processed? ", root->processed);

            // Single depth-first pass: inner nodes with their direct leaves
            size_t total_serialized = serialize_depth_first_stack(
                root, serialized_output, current_offset);
            LOGI(
                "Depth-first: Serialized ",
                total_serialized,
                " nodes total for ledger ",
                ledger_seq);
        }

        // Skip transaction map for now
        reader.skip_map(catl::shamap::tnTRANSACTION_MD);
    }

    LOGI("Total serialized size: ", serialized_output.size(), " bytes");
}

/**
 * Main entry point
 */
int
main([[maybe_unused]] int argc, [[maybe_unused]] char* argv[])
{
    Logger::set_level(LogLevel::INFO);
    LOGI("Experiment COMPLETELY set up now bruh!");

    // Process all ledgers in the file
    process_all_ledgers(
        "/Users/nicholasdudfield/projects/xahau-history/"
        "cat.2000000-2010000.compression-0.catl");
}
