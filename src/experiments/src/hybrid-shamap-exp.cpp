/**
 * Hybrid SHAMap experiment
 *
 * This program explores a hybrid merkle tree architecture that can use
 * mmap'd nodes, memory nodes, and placeholder nodes.
 */

#include "../../../src/shamap/src/pretty-print-json.h"
#include "catl/core/logger.h"
#include "catl/hybrid-shamap/hybrid-shamap.h"
#include "catl/v2/catl-v2-reader.h"
#include "catl/v2/catl-v2-structs.h"
#include "catl/xdata/json-visitor.h"
#include "catl/xdata/parser-context.h"
#include "catl/xdata/parser.h"
#include "catl/xdata/protocol.h"
#include <boost/json.hpp>
#include <iostream>
#include <memory>
#include <optional>
#include <string>

/**
 * Helper class to convert LeafView data to JSON using appropriate protocol
 */
class LeafJsonConverter
{
private:
    catl::xdata::Protocol protocol_;

public:
    explicit LeafJsonConverter(uint32_t network_id)
        : protocol_(
              (network_id == 0)
                  ? catl::xdata::Protocol::load_embedded_xrpl_protocol()
                  : catl::xdata::Protocol::load_embedded_xahau_protocol())
    {
    }

    [[nodiscard]] boost::json::value
    to_json(const catl::hybrid_shamap::LeafView& leaf) const
    {
        catl::xdata::JsonVisitor visitor(protocol_);
        catl::xdata::ParserContext ctx(leaf.data);
        catl::xdata::parse_with_visitor(ctx, protocol_, visitor);
        return visitor.get_result();
    }

    void
    pretty_print(std::ostream& os, const catl::hybrid_shamap::LeafView& leaf)
        const
    {
        auto json = to_json(leaf);
        pretty_print_json(os, json);
    }
};

void test_tagged_ptr() {
    LOGI("=== Testing TaggedPtr ===");
    
    // Test 1: Default construction
    LOGD("Test 1: Default construction");
    catl::hybrid_shamap::TaggedPtr tp1;
    LOGI("  Default ptr is_empty: ", tp1.is_empty());
    LOGI("  Default ptr raw: ", tp1.get_raw_ptr());
    
    // Test 2: Create RAW_MEMORY pointer
    LOGD("Test 2: RAW_MEMORY pointer");
    const void* test_ptr = reinterpret_cast<const void*>(0x105c66599);  // Use the branch 13 pointer
    LOGI("  Test pointer: ", test_ptr);
    LOGI("  Pointer & 0x7 (lower 3 bits): ", (reinterpret_cast<uintptr_t>(test_ptr) & 0x7));
    LOGI("  Is 8-byte aligned? ", ((reinterpret_cast<uintptr_t>(test_ptr) & 0x7) == 0));
    auto tp2 = catl::hybrid_shamap::TaggedPtr::make_raw_memory(test_ptr);
    LOGI("  RAW ptr is_raw_memory: ", tp2.is_raw_memory());
    LOGI("  RAW ptr get_raw_ptr: ", tp2.get_raw_ptr());
    
    // Test 3: Assignment to array element
    LOGD("Test 3: Assignment to array elements");
    std::array<catl::hybrid_shamap::TaggedPtr, 16> test_array{};
    LOGI("  Array[0] before assignment: ", test_array[0].get_raw_ptr());
    test_array[0] = tp2;
    LOGI("  Array[0] after assignment: ", test_array[0].get_raw_ptr());
    
    // Test 4: Assignment to element 13 specifically
    LOGD("Test 4: Assignment to element 13");
    LOGI("  Array[13] before assignment: ", test_array[13].get_raw_ptr());
    test_array[13] = tp2;
    LOGI("  Array[13] after assignment: ", test_array[13].get_raw_ptr());
    
    // Test 5: Multiple assignments
    LOGD("Test 5: Assigning all 16 elements");
    for (int i = 0; i < 16; i++) {
        auto ptr_val = reinterpret_cast<const void*>(0x100000000 + i * 0x1000);
        auto tp = catl::hybrid_shamap::TaggedPtr::make_raw_memory(ptr_val);
        test_array[i] = tp;
        LOGD("  Assigned ", ptr_val, " to index ", i);
    }
    LOGI("  All 16 assignments completed");
    
    // Test 6: Test in a class similar to HmapInnerNode
    LOGD("Test 6: Test in mock inner node");
    struct MockInner {
        std::array<catl::hybrid_shamap::TaggedPtr, 16> children_{};
        uint32_t child_types_ = 0;
        
        void set_child(int branch, catl::hybrid_shamap::TaggedPtr ptr) {
            LOGD("    MockInner::set_child(", branch, ", ", ptr.get_raw_ptr(), ")");
            children_[branch] = ptr;
            LOGD("    Assignment completed");
        }
    };
    
    MockInner mock;
    for (int i = 0; i < 16; i++) {
        auto ptr_val = reinterpret_cast<const void*>(0x100000000 + i * 0x1000);
        auto tp = catl::hybrid_shamap::TaggedPtr::make_raw_memory(ptr_val);
        LOGD("  Setting child ", i);
        mock.set_child(i, tp);
    }
    
    LOGI("=== TaggedPtr tests completed ===");
}

int
main(int argc, char* argv[])
{
    // Set log level to DEBUG for maximum visibility
    Logger::set_level(LogLevel::DEBUG);
    
    // Enable v2-structs partition for debugging
    catl::v2::get_v2_structs_log_partition().set_level(LogLevel::DEBUG);
    
    if (argc != 2)
    {
        LOGE("Usage: ", argv[0], " <catl-v2-file>");
        return 1;
    }

    const std::string filename = argv[1];
    LOGI("Starting hybrid-shamap experiment with file: ", filename);

    try
    {
        LOGD("Creating CatlV2Reader for file: ", filename);
        // Create reader for the v2 file
        auto reader = catl::v2::CatlV2Reader::create(filename);

        LOGI("Successfully opened: ", filename);

        // Print file header info
        const auto& header = reader->header();
        LOGI("File Header:");
        LOGI("  Version: ", header.version);
        LOGI("  Ledger count: ", header.ledger_count);
        LOGI("  First ledger: ", header.first_ledger_seq);
        LOGI("  Last ledger: ", header.last_ledger_seq);
        LOGI("  Index offset: ", header.ledger_index_offset, " bytes");

        // Read the first ledger
        LOGD("Checking for ledgers in file");
        if (reader->at_end_of_ledgers())
        {
            LOGE("No ledgers in file!");
            return 1;
        }

        LOGD("Reading first ledger info");
        const auto& ledger_info = reader->read_ledger_info();

        LOGI("First Ledger Header:");
        LOGI("  Sequence: ", ledger_info.seq);
        LOGI("  Drops: ", ledger_info.drops);
        LOGI("  Parent hash: ", ledger_info.parent_hash.hex());
        LOGI("  Tx hash: ", ledger_info.tx_hash.hex());
        LOGI("  Account hash: ", ledger_info.account_hash.hex());
        LOGI("  Parent close: ", ledger_info.parent_close_time);
        LOGI("  Close time: ", ledger_info.close_time);
        LOGI("  Close time res: ", (int)ledger_info.close_time_resolution);
        LOGI("  Close flags: ", (int)ledger_info.close_flags);
        if (ledger_info.hash.has_value())
        {
            LOGI("  Ledger hash: ", ledger_info.hash->hex());
        }
        else
        {
            LOGI("  Ledger hash: (not present)");
        }
        auto ptr = reader->data_at(reader->current_offset());
        LOGI("Direct pointer from reader: ", static_cast<const void*>(ptr));

        // Now use HybridReader to get the state tree root
        LOGD("Creating HybridReader");
        catl::hybrid_shamap::HybridReader hybrid_reader(reader);


        // Get the state tree root as an InnerNodeView
        LOGD("Getting state tree root view");
        auto root_view = hybrid_reader.get_state_root();

        LOGI("State Tree Root Node:");
        LOGD("  Header pointer from HybridReader: ", static_cast<const void*>(root_view.header.raw()));
        
        // Let's also create a MemPtr directly and compare
        catl::v2::MemPtr<catl::v2::InnerNodeHeader> direct_header(ptr);
        LOGD("  Direct MemPtr raw: ", static_cast<const void*>(direct_header.raw()));
        const auto& direct_header_val = direct_header.get_uncopyable();
        LOGI("  Direct header depth: ", (int)direct_header_val.get_depth());
        LOGI("  Direct header child_types: 0x", std::hex, direct_header_val.child_types, std::dec);
        const auto& root_header = root_view.header.get_uncopyable();
        LOGI("  Depth: ", (int)root_header.get_depth());
        LOGI("  Child types: 0x", std::hex, root_header.child_types, std::dec);
        LOGI("  Non-empty children: ", root_header.count_children());

        // Create an HmapInnerNode and populate with child offsets
        LOGD("Creating HmapInnerNode at depth 0");
        auto hybrid_root_heap = new catl::hybrid_shamap::HmapInnerNode(0);  // depth 0
        // catl::hybrid_shamap::HmapInnerNode hybrid_root(0);  // depth 0
        auto& hybrid_root = *hybrid_root_heap;

        // Get child iterator and load offsets
        LOGD("Getting child iterator from root view");
        LOGD("  root_view.header.raw() = ", static_cast<const void*>(root_view.header.raw()));
        auto child_iter = root_view.get_child_iter();
        LOGD("  child_iter.header.raw() = ", static_cast<const void*>(child_iter.header.raw()));
        LOGD("  child_iter.offsets_start = ", static_cast<const void*>(child_iter.offsets_start));

        LOGI("Loading child offsets into hybrid node:");
        LOGI("Initial remaining_mask: 0x", std::hex, child_iter.remaining_mask, std::dec);
        
        // Let's also test creating an iterator directly
        LOGD("Testing direct iterator creation:");
        const uint8_t* offsets_ptr = ptr + sizeof(catl::v2::InnerNodeHeader);
        catl::v2::ChildIterator direct_iter(direct_header, offsets_ptr);
        LOGI("Direct iterator remaining_mask: 0x", std::hex, direct_iter.remaining_mask, std::dec);
        
        // Test iterating with direct iterator and save results
        LOGD("Testing direct iteration (GOLD STANDARD):");
        std::vector<std::pair<int, const void*>> gold_results;
        int test_count = 0;
        while (direct_iter.has_next() && test_count < 20) {
            auto test_child = direct_iter.next();
            gold_results.push_back({test_child.branch, static_cast<const void*>(test_child.ptr)});
            uintptr_t addr = reinterpret_cast<uintptr_t>(test_child.ptr);
            LOGD("  GOLD[", test_count, "]: branch=", test_child.branch, 
                 ", ptr=", static_cast<const void*>(test_child.ptr),
                 ", align=", (addr & 0x7));
            test_count++;
        }
        LOGI("Direct iteration completed, count=", test_count);
        
        // Check minimum alignment
        int min_align = 7;
        for (auto& [branch, ptr] : gold_results) {
            uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
            int align = __builtin_ctz(addr | 0x8);  // Count trailing zeros (or 3 max)
            if (align < min_align) min_align = align;
        }
        LOGI("Minimum alignment across all pointers: ", min_align, " bits");
        LOGI("Can use ", min_align, " bits for tagging");
        
        int branch_count = 0;
        int safety_counter = 0;
        while (true)
        {
            LOGD("Checking has_next() with remaining_mask=0x", 
                 std::hex, child_iter.remaining_mask, std::dec);
            if (!child_iter.has_next())
            {
                LOGD("has_next() returned false, exiting loop");
                break;
            }

            if (++safety_counter > 20)
            {
                LOGE("Iterator stuck in infinite loop after ", safety_counter, " iterations!");
                LOGE("Remaining mask: 0x", std::hex, child_iter.remaining_mask, std::dec);
                break;
            }

            LOGD("Calling child_iter.next() for iteration ", safety_counter);
            auto child = child_iter.next();
            
            // Compare with gold results
            if (branch_count < gold_results.size()) {
                auto& gold = gold_results[branch_count];
                bool match = (gold.first == child.branch && gold.second == static_cast<const void*>(child.ptr));
                if (!match) {
                    LOGE("MISMATCH at index ", branch_count, "!");
                    LOGE("  GOLD: branch=", gold.first, ", ptr=", gold.second);
                    LOGE("  OUR:  branch=", child.branch, ", ptr=", static_cast<const void*>(child.ptr));
                } else {
                    LOGI("  MATCH[", branch_count, "]: branch=", child.branch, 
                         ", ptr=", static_cast<const void*>(child.ptr));
                }
            }
            
            branch_count++;
            
            // SKIP set_child operations entirely for now
            LOGD("SKIPPING set_child for branch ", child.branch);
        }

        LOGI("Finished loading children, branch_count = ", branch_count);

        // Print summary
        LOGI("Hybrid root node populated:");
        LOGI("Loaded ", branch_count, " branches from iterator");
        LOGI("Total populated children: ", hybrid_root.count_children());

        // Keep first_leaf key in outer scope for later use
        std::optional<Key> first_leaf_key;

        // Find first available key to test with IN STATE TREE
        LOGI("=== STATE TREE ===");
        LOGD("Finding first leaf in state tree");
        try
        {
            LOGD("Calling first_leaf_depth_first");
            auto first_leaf = hybrid_reader.first_leaf_depth_first(root_view);
            first_leaf_key = first_leaf.key;
            LOGI("  Found first leaf with key: ", first_leaf.key.hex());

            // Now test lookup with the key we found
            LOGI("Testing key lookup:");
            LOGI("  Looking for key: ", first_leaf.key.hex());

            // Lookup the key using our simplified traversal
            LOGD("Calling lookup_key_in_state");
            auto leaf = hybrid_reader.lookup_key_in_state(first_leaf.key);
            LOGI("  Found leaf!");
            LOGI("  Data size: ", leaf.data.size(), " bytes");

            // Parse and display as JSON using the converter
            try
            {
                // Create converter with the appropriate protocol based on
                // network ID
                LOGD("Creating LeafJsonConverter for network ID: ", reader->header().network_id);
                LeafJsonConverter converter(reader->header().network_id);

                LOGI("Parsed state object as JSON:");
                converter.pretty_print(std::cout, leaf);
            }
            catch (const std::exception& e)
            {
                LOGW("Failed to parse as JSON: ", e.what());
                std::string hex;
                slice_hex(leaf.data, hex);
                LOGD("Raw hex data: ", hex);
            }
        }
        catch (const std::exception& e)
        {
            LOGE("  State key lookup failed: ", e.what());
        }

        // Now let's look at the TRANSACTION TREE
        LOGI("=== TRANSACTION TREE ===");

        // Skip the state tree to get to the tx tree
        LOGD("Skipping state tree");
        auto state_tree_size = reader->skip_state_map();
        LOGI("Skipped state tree (", state_tree_size, " bytes)");

        // Now we're at the tx tree - get it as an InnerNodeView
        LOGD("Getting transaction tree root view");
        auto tx_root_view =
            hybrid_reader.get_inner_node(reader->current_data());

        LOGI("Transaction Tree Root Node:");
        const auto& tx_root_header = tx_root_view.header.get_uncopyable();
        LOGI("  Depth: ", (int)tx_root_header.get_depth());
        LOGI("  Child types: 0x", std::hex, tx_root_header.child_types, std::dec);
        LOGI("  Non-empty children: ", tx_root_header.count_children());

        // Find first transaction
        LOGI("Finding first transaction:");
        try
        {
            LOGD("Calling first_leaf_depth_first on tx tree");
            auto first_tx = hybrid_reader.first_leaf_depth_first(tx_root_view);
            LOGI("  Found first transaction with ID: ", first_tx.key.hex());
            LOGI("  Transaction data size: ", first_tx.data.size(), " bytes");

            // Parse transaction + metadata
            try
            {
                // Parse as transaction with metadata (VL-encoded tx +
                // VL-encoded metadata)
                catl::xdata::ParserContext ctx(first_tx.data);

                // Create root object to hold both tx and meta
                boost::json::object root;

                // First: Parse VL-encoded transaction
                size_t tx_vl_length = catl::xdata::read_vl_length(ctx.cursor);
                Slice tx_data = ctx.cursor.read_slice(tx_vl_length);

                {
                    catl::xdata::Protocol protocol =
                        (reader->header().network_id == 0)
                        ? catl::xdata::Protocol::load_embedded_xrpl_protocol()
                        : catl::xdata::Protocol::load_embedded_xahau_protocol();
                    catl::xdata::JsonVisitor tx_visitor(protocol);
                    catl::xdata::ParserContext tx_ctx(tx_data);
                    catl::xdata::parse_with_visitor(
                        tx_ctx, protocol, tx_visitor);
                    root["tx"] = tx_visitor.get_result();
                }

                // Second: Parse VL-encoded metadata
                size_t meta_vl_length = catl::xdata::read_vl_length(ctx.cursor);
                Slice meta_data = ctx.cursor.read_slice(meta_vl_length);

                {
                    catl::xdata::Protocol protocol =
                        (reader->header().network_id == 0)
                        ? catl::xdata::Protocol::load_embedded_xrpl_protocol()
                        : catl::xdata::Protocol::load_embedded_xahau_protocol();
                    catl::xdata::JsonVisitor meta_visitor(protocol);
                    catl::xdata::ParserContext meta_ctx(meta_data);
                    catl::xdata::parse_with_visitor(
                        meta_ctx, protocol, meta_visitor);
                    root["meta"] = meta_visitor.get_result();
                }

                LOGI("Parsed transaction as JSON:");
                pretty_print_json(std::cout, boost::json::value(root));
            }
            catch (const std::exception& e)
            {
                LOGW("Failed to parse transaction: ", e.what());
                std::string hex;
                slice_hex(first_tx.data, hex);
                LOGD("Raw hex data: ", hex);
            }
        }
        catch (const std::exception& e)
        {
            LOGE("  Transaction lookup failed: ", e.what());
        }

        // Now test the HYBRID PATHFINDER
        LOGI("=== HYBRID PATHFINDER TEST ===");

        // Reset reader position back to state tree
        LOGD("Seeking back to ledger ", ledger_info.seq);
        reader->seek_to_ledger(ledger_info.seq);
        reader->read_ledger_info();  // Re-read to position at state tree

        // Create an Hmap with reader for mmap lifetime management
        LOGD("Creating Hmap with reader");
        catl::hybrid_shamap::Hmap hmap(reader);
        const uint8_t* state_root_raw = reader->current_data();
        hmap.set_root_raw(state_root_raw);

        LOGI("Created Hmap with RAW_MEMORY root at: ",
             static_cast<const void*>(state_root_raw));

        // Use the first leaf key we found earlier
        if (!first_leaf_key)
        {
            LOGW("No leaf key found to test with!");
            return 0;
        }

        LOGI("Finding path to key: ", first_leaf_key->hex());

        LOGD("Creating HmapPathFinder");
        catl::hybrid_shamap::HmapPathFinder pathfinder(
            &hybrid_reader, *first_leaf_key);
        LOGD("Calling find_path");
        pathfinder.find_path(hmap.get_root());

        LOGI("Path traversal result:");
        pathfinder.print_path();

        // Now materialize the path
        LOGI("Materializing path for modification...");
        pathfinder.materialize_path();

        LOGI("Path after materialization:");
        pathfinder.print_path();

        // Verify no memory leaks by checking the nodes are properly managed
        LOGI("[Memory management check: Using boost::intrusive_ptr]");
        LOGI("Materialized nodes will be automatically deleted when path goes out of scope");

        LOGI("[Hybrid SHAMap experiment completed successfully]");

        return 0;
    }
    catch (const std::exception& e)
    {
        LOGE("Error: ", e.what());
        return 1;
    }
}