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
#include <iostream>
#include <memory>
#include <string>

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
        std::cout << "  File offset: " << root_view.file_offset << std::endl;
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