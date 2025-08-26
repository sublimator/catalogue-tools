/**
 * Hybrid SHAMap experiment
 *
 * This program explores a hybrid merkle tree architecture that can use
 * mmap'd nodes, memory nodes, and placeholder nodes.
 */

#include "catl/core/log-macros.h"
#include "catl/v2/catl-v2-reader.h"
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

        // TODO: Next steps
        // 1. Walk the state tree and collect statistics
        // 2. Experiment with creating a hybrid node structure
        // 3. Test CoW operations on hybrid trees

        std::cout << "\n[Hybrid SHAMap experiment initialized successfully]"
                  << std::endl;

        return 0;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}