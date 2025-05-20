/**
 * serialized-inners.cpp
 * 
 * An experimental tool for exploring serialization approaches for SHAMap inner nodes.
 * This tool is used to evaluate different strategies for efficiently representing
 * and serializing the inner node structure of SHAMaps.
 */

#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <chrono>
#include <iomanip>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>

#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-writer.h"

using namespace catl::v1;
namespace po = boost::program_options;
namespace fs = boost::filesystem;


enum class ChildType : std::uint8_t {
    INNER = 0,
    LEAF = 1,
    EMPTY = 2,
    RFU = 3 //
};

// We decided to use 6 bytes per inner but it should potentially benefit
// from better "alignment" optimization than using 5 bytes per inner
// "Using 6 bytes (2+4) instead of 5 bytes (1+4) improves CPU memory access efficiency through
// better alignment, reduces padding waste in arrays, optimizes cache line usage (10 nodes per line),
// and provides 8 additional flag bits with only a 20% size increase."
// TODO: get some stats from large ledgers and see how many inner nodes change per ledger
struct DepthAndFlags {
    std::uint16_t depth : 6;  // 6 bits for depth (0-63)
    std::uint16_t rfu   : 10;  // 10 bits reserved for future use
};

struct InnerNode {
    union {
        std::uint16_t depth_plus; // Access as raw byte for serialization
        DepthAndFlags bits; // Access as structured fields
    };
    std::uint32_t child_types; // 2 bits per child × 16 children = 32 bits total
};

struct ChildSlot {
    // 640 kilobytes ought to be enough for anyone, that damning quote of the ages
    // If you start at the root node, then write out all the child nodes and their slots
    // one depth at a time, then you'd have children not far from each other, and perhaps you
    // could actually "get away" with using a smaller type for the offset? 32 bits?
    // This needs consideration.
    // We looked at this and decide that depth first is the way to go, and ALSO, because we'll be
    // pointing to nodes from distant maps past then we're going to need the full 64 bits.
    std::uint64_t offset; // Offset of child node in body
};



class MapNodeSerializer {
    // If you were processing thousands of trees and calculating deltas, then this should only
    // ever have offsets to the tree nodes that are actually currently in the tree for the position
    // in the stream. So you would need to clear_offset(p) when you delete a node from the tree
    // or when an inner node is not reused (essentially deleted) - different words for the same thing
private:
    std::map<void*, std::uint64_t> offsets;
public:
    std::uint64_t get_offset(void* node);
    void set_offset(void* node, std::uint64_t offset);
    void clear_offset(void* node); // clear when we see a deleted node in the tree
};


struct Ledger {
    LedgerInfo ledger_info;
    std::uint64_t account_map_offset;
    std::uint64_t transaction_map_offset;
};


// 128 bits
struct LedgerLookupEntry {
    std::uint32_t ledger_index;
    std::uint64_t ledger_offset; // to Ledger Struct
    std::uint32_t rfu_padding; // rfu
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
        //    Root Node (potentially with body offset pointers to previous ledger inner nodes, which can point to previous ledger leaf nodes)
    //    TransactionMap
        //    Leaf nodes
        //    Inner nodes
// Footer
    // LedgerLookupTable ( is a bisectable index of the ledgers in the file )

// Starts at root node
// You'd serialize the `InnerNode` struct, then leave offset slots for each child that is present
// i.e. if there are only 5 children, then you'd leave 5 empty slots
// what is a slot? it's a relative pointer to where the child node is serialized in the body
// so of course you'd need to know the offset of the body
// it's probably easiest to write a recursive function that returns the offset of the serialized node
// afterwards converting it into a stack based implementation, but recursive is easier to start with
// stack overflows are a problem so it's probably easiest to use a stack straight of the bat
// but conceptually it's the same thing


// Do not try and interleave the serialization of the inners and leaves
// Separate them, so which goes first? The leaves first means you'd have the offsets ready to write for the inners
// For inners you'd do breadth first serialization which may help with locality of reference?!
// Is that actaully true though, actually, it does sound true
// Ordering of Nodes
// Leaves:
// Sorted by key order (lexicographically), which enables efficient binary search, range scans, and predictable access patterns. This organization allows for quick location of data when accessing by key directly, independent of tree traversal.
// Inners:
// Depth-first traversal order, which keeps nodes in the same subtree close together in memory. This maximizes cache efficiency when traversing paths for keys with common prefixes and performing operations on subtrees. It matches the natural recursive structure of tree algorithms and provides the best overall performance for tree-based operations.


/**
 * Main entry point
 */
int main(int argc, char* argv[]) 
{
    Logger::set_level(LogLevel::INFO);
    LOGI("Experiment set up");
}
