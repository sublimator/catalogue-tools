/**
 * Hybrid SHAMap experiment
 *
 * This program explores a hybrid merkle tree architecture that can use
 * mmap'd nodes, memory nodes, and placeholder nodes.
 */

#include "../../../src/shamap/src/pretty-print-json.h"
#include "catl/core/log-macros.h"
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

int
main(int argc, char* argv[])
{
    if (argc != 2)
    {
        std::cerr << "Usage: " << argv[0] << " <catl-v2-file>" << std::endl;
        return 1;
    }

    const std::string filename = argv[1];

    try
    {
        // Create reader for the v2 file
        auto reader = catl::v2::CatlV2Reader::create(filename);

        std::cout << "Successfully opened: " << filename << std::endl;

        // Print file header info
        const auto& header = reader->header();
        std::cout << "\nFile Header:" << std::endl;
        std::cout << "  Version: " << header.version << std::endl;
        std::cout << "  Ledger count: " << header.ledger_count << std::endl;
        std::cout << "  First ledger: " << header.first_ledger_seq << std::endl;
        std::cout << "  Last ledger: " << header.last_ledger_seq << std::endl;
        std::cout << "  Index offset: " << header.ledger_index_offset
                  << " bytes" << std::endl;

        // Read the first ledger
        if (reader->at_end_of_ledgers())
        {
            std::cerr << "No ledgers in file!" << std::endl;
            return 1;
        }

        const auto& ledger_info = reader->read_ledger_info();

        std::cout << "\nFirst Ledger Header:" << std::endl;
        std::cout << "  Sequence: " << ledger_info.seq << std::endl;
        std::cout << "  Drops: " << ledger_info.drops << std::endl;
        std::cout << "  Parent hash: " << ledger_info.parent_hash.hex()
                  << std::endl;
        std::cout << "  Tx hash: " << ledger_info.tx_hash.hex() << std::endl;
        std::cout << "  Account hash: " << ledger_info.account_hash.hex()
                  << std::endl;
        std::cout << "  Parent close: " << ledger_info.parent_close_time
                  << std::endl;
        std::cout << "  Close time: " << ledger_info.close_time << std::endl;
        std::cout << "  Close time res: "
                  << (int)ledger_info.close_time_resolution << std::endl;
        std::cout << "  Close flags: " << (int)ledger_info.close_flags
                  << std::endl;
        if (ledger_info.hash.has_value())
        {
            std::cout << "  Ledger hash: " << ledger_info.hash->hex()
                      << std::endl;
        }
        else
        {
            std::cout << "  Ledger hash: (not present)" << std::endl;
        }

        // Now use HybridReader to get the state tree root
        catl::hybrid_shamap::HybridReader hybrid_reader(reader);

        // Get the state tree root as an InnerNodeView
        auto root_view = hybrid_reader.get_state_root();

        std::cout << "\nState Tree Root Node:" << std::endl;
        std::cout << "  Header pointer: "
                  << static_cast<const void*>(root_view.header.raw())
                  << std::endl;
        auto root_header = root_view.header.get();
        std::cout << "  Depth: " << (int)root_header.get_depth() << std::endl;
        std::cout << "  Child types: 0x" << std::hex << root_header.child_types
                  << std::dec << std::endl;
        std::cout << "  Non-empty children: " << root_header.count_children()
                  << std::endl;

        // Create an HmapInnerNode and populate with child offsets
        catl::hybrid_shamap::HmapInnerNode hybrid_root;

        // Get child iterator and load offsets
        auto child_iter = root_view.get_child_iter();

        std::cout << "\nLoading child offsets into hybrid node:" << std::endl;
        while (child_iter.has_next())
        {
            auto child = child_iter.next();

            // Store the absolute offset as a void* (for now, no tagging yet)
            hybrid_root.children[child.branch] =
                reinterpret_cast<void*>(child.offset);

            std::cout << "  Branch[" << child.branch << "]: "
                      << (child.type == catl::v2::ChildType::INNER ? "INNER"
                                                                   : "LEAF")
                      << " at offset 0x" << std::hex << child.offset << std::dec
                      << std::endl;
        }

        // Print summary
        std::cout << "\nHybrid root node populated:" << std::endl;
        int populated_count = 0;
        for (int i = 0; i < 16; ++i)
        {
            if (hybrid_root.children[i] != nullptr)
            {
                populated_count++;
            }
        }
        std::cout << "Total populated children: " << populated_count
                  << std::endl;

        // Find first available key to test with IN STATE TREE
        std::cout << "\n=== STATE TREE ===" << std::endl;
        std::cout << "Finding first leaf in state tree:" << std::endl;
        try
        {
            auto first_leaf = hybrid_reader.first_leaf_depth_first(root_view);
            std::cout << "  Found first leaf with key: " << first_leaf.key.hex()
                      << std::endl;

            // Now test lookup with the key we found
            std::cout << "\nTesting key lookup:" << std::endl;
            std::cout << "  Looking for key: " << first_leaf.key.hex()
                      << std::endl;

            // Lookup the key using our simplified traversal
            auto leaf = hybrid_reader.lookup_key_in_state(first_leaf.key);
            std::cout << "  Found leaf!" << std::endl;
            std::cout << "  Data size: " << leaf.data.size() << " bytes"
                      << std::endl;

            // Parse and display as JSON using the converter
            try
            {
                // Create converter with the appropriate protocol based on
                // network ID
                LeafJsonConverter converter(reader->header().network_id);

                std::cout << "\nParsed state object as JSON:" << std::endl;
                converter.pretty_print(std::cout, leaf);
            }
            catch (const std::exception& e)
            {
                std::cout << "Failed to parse as JSON: " << e.what()
                          << std::endl;
                std::cout << "Raw hex data: ";
                std::string hex;
                slice_hex(leaf.data, hex);
                std::cout << hex << std::endl;
            }
        }
        catch (const std::exception& e)
        {
            std::cout << "  State key lookup failed: " << e.what() << std::endl;
        }

        // Now let's look at the TRANSACTION TREE
        std::cout << "\n=== TRANSACTION TREE ===" << std::endl;
        
        // Skip the state tree to get to the tx tree
        auto state_tree_size = reader->skip_state_map();
        std::cout << "Skipped state tree (" << state_tree_size << " bytes)" << std::endl;
        
        // Now we're at the tx tree - get it as an InnerNodeView
        auto tx_root_view = hybrid_reader.get_inner_node_at(reader->current_offset());
        
        std::cout << "\nTransaction Tree Root Node:" << std::endl;
        auto tx_root_header = tx_root_view.header.get();
        std::cout << "  Depth: " << (int)tx_root_header.get_depth() << std::endl;
        std::cout << "  Child types: 0x" << std::hex << tx_root_header.child_types
                  << std::dec << std::endl;
        std::cout << "  Non-empty children: " << tx_root_header.count_children()
                  << std::endl;

        // Find first transaction
        std::cout << "\nFinding first transaction:" << std::endl;
        try
        {
            auto first_tx = hybrid_reader.first_leaf_depth_first(tx_root_view);
            std::cout << "  Found first transaction with ID: " << first_tx.key.hex()
                      << std::endl;
            std::cout << "  Transaction data size: " << first_tx.data.size() << " bytes"
                      << std::endl;

            // Parse transaction + metadata
            try
            {
                // Parse as transaction with metadata (VL-encoded tx + VL-encoded metadata)
                catl::xdata::ParserContext ctx(first_tx.data);
                
                // Create root object to hold both tx and meta
                boost::json::object root;
                
                // First: Parse VL-encoded transaction
                size_t tx_vl_length = catl::xdata::read_vl_length(ctx.cursor);
                Slice tx_data = ctx.cursor.read_slice(tx_vl_length);
                
                {
                    catl::xdata::Protocol protocol = (reader->header().network_id == 0)
                        ? catl::xdata::Protocol::load_embedded_xrpl_protocol()
                        : catl::xdata::Protocol::load_embedded_xahau_protocol();
                    catl::xdata::JsonVisitor tx_visitor(protocol);
                    catl::xdata::ParserContext tx_ctx(tx_data);
                    catl::xdata::parse_with_visitor(tx_ctx, protocol, tx_visitor);
                    root["tx"] = tx_visitor.get_result();
                }
                
                // Second: Parse VL-encoded metadata
                size_t meta_vl_length = catl::xdata::read_vl_length(ctx.cursor);
                Slice meta_data = ctx.cursor.read_slice(meta_vl_length);
                
                {
                    catl::xdata::Protocol protocol = (reader->header().network_id == 0)
                        ? catl::xdata::Protocol::load_embedded_xrpl_protocol()
                        : catl::xdata::Protocol::load_embedded_xahau_protocol();
                    catl::xdata::JsonVisitor meta_visitor(protocol);
                    catl::xdata::ParserContext meta_ctx(meta_data);
                    catl::xdata::parse_with_visitor(meta_ctx, protocol, meta_visitor);
                    root["meta"] = meta_visitor.get_result();
                }
                
                std::cout << "\nParsed transaction as JSON:" << std::endl;
                pretty_print_json(std::cout, boost::json::value(root));
            }
            catch (const std::exception& e)
            {
                std::cout << "Failed to parse transaction: " << e.what()
                          << std::endl;
                std::cout << "Raw hex data: ";
                std::string hex;
                slice_hex(first_tx.data, hex);
                std::cout << hex << std::endl;
            }
        }
        catch (const std::exception& e)
        {
            std::cout << "  Transaction lookup failed: " << e.what() << std::endl;
        }

        std::cout << "\n[Hybrid SHAMap experiment completed successfully]"
                  << std::endl;

        return 0;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}