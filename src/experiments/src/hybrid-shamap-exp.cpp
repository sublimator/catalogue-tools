/**
 * Hybrid SHAMap experiment
 *
 * This program explores a hybrid merkle tree architecture that can use
 * mmap'd nodes, memory nodes, and placeholder nodes.
 */

#include "catl/core/log-macros.h"
#include "catl/hybrid-shamap/hybrid-shamap.h"
#include "catl/v2/catl-v2-reader.h"
#include "catl/v2/catl-v2-structs.h"
#include "catl/xdata/json-visitor.h"
#include "catl/xdata/parser-context.h"
#include "catl/xdata/parser.h"
#include "catl/xdata/protocol.h"
#include "../../../src/shamap/src/pretty-print-json.h"
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
        : protocol_((network_id == 0) 
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
    pretty_print(std::ostream& os, const catl::hybrid_shamap::LeafView& leaf) const
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
        std::cout << "  Header pointer: " << root_view.header << std::endl;
        std::cout << "  Depth: " << (int)root_view.header->get_depth()
                  << std::endl;
        std::cout << "  Child types: 0x" << std::hex
                  << root_view.header->child_types << std::dec << std::endl;
        std::cout << "  Non-empty children: "
                  << root_view.header->count_children() << std::endl;

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

        // Find first available key to test with
        std::cout << "\nFinding first leaf in tree:" << std::endl;
        try
        {
            auto first_leaf = hybrid_reader.first_leaf_depth_first(root_view);
            std::cout << "  Found first leaf with key: " << first_leaf.key.hex() << std::endl;
            
            // Now test lookup with the key we found
            std::cout << "\nTesting key lookup:" << std::endl;
            std::cout << "  Looking for key: " << first_leaf.key.hex() << std::endl;
            
            // Lookup the key using our simplified traversal
            auto leaf = hybrid_reader.lookup_key_in_state(first_leaf.key);
            std::cout << "  Found leaf!" << std::endl;
            std::cout << "  Data size: " << leaf.data.size() << " bytes" << std::endl;
            
            // Parse and display as JSON using the converter
            try
            {
                // Create converter with the appropriate protocol based on network ID
                LeafJsonConverter converter(reader->header().network_id);
                
                std::cout << "\nParsed object as JSON:" << std::endl;
                converter.pretty_print(std::cout, leaf);
            }
            catch (const std::exception& e)
            {
                std::cout << "Failed to parse as JSON: " << e.what() << std::endl;
                std::cout << "Raw hex data: ";
                std::string hex;
                slice_hex(leaf.data, hex);
                std::cout << hex << std::endl;
            }
        }
        catch (const std::exception& e)
        {
            std::cout << "  Key lookup failed: " << e.what() << std::endl;
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