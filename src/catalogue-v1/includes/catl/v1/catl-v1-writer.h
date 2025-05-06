#pragma once

#include "catl/core/types.h"
#include "catl/shamap/shamap.h"
#include "catl/v1/catl-v1-structs.h"
#include <fstream>
#include <memory>
#include <string>
#include <vector>

namespace catl {
namespace v1 {

/**
 * Writer for CATL v1 files
 *
 * This class creates CATL files from a series of ledgers and their associated
 * state and transaction maps.
 */
class Writer
{
public:
    /**
     * Constructor
     *
     * @param output_path Path to the output CATL file
     * @param network_id Network ID to use in the file header
     */
    Writer(const std::string& output_path, uint32_t network_id);

    /**
     * Destructor
     */
    ~Writer();

    /**
     * Write file header
     *
     * @param min_ledger First ledger sequence to include
     * @param max_ledger Last ledger sequence to include
     * @return true if successful
     */
    bool
    writeHeader(uint32_t min_ledger, uint32_t max_ledger);

    /**
     * Write a complete ledger and its maps
     *
     * @param header Ledger header information
     * @param state_map State map for the ledger
     * @param tx_map Transaction map for the ledger
     * @return true if successful
     */
    bool
    writeLedger(
        const LedgerInfo& header,
        const SHAMap& state_map,
        const SHAMap& tx_map);

    /**
     * Write delta between two state maps
     *
     * @param base_map Base state map
     * @param new_map New state map
     * @return true if successful
     */
    bool
    writeStateDelta(const SHAMap& base_map, const SHAMap& new_map);

    /**
     * Finalize the file
     * Updates header with final file size and closes file
     *
     * @return true if successful
     */
    bool
    finalize();

private:
    std::ofstream file_;
    std::string filepath_;
    uint32_t network_id_;
    uint64_t file_position_ = 0;
    bool header_written_ = false;
    bool finalized_ = false;

    /**
     * Write an item to the file
     *
     * @param node_type Type of node
     * @param key Key of the item
     * @param data Data buffer
     * @param size Size of the data
     * @return true if successful
     */
    bool
    writeItem(
        SHAMapNodeType node_type,
        const Key& key,
        const uint8_t* data,
        uint32_t size);

    /**
     * Write a terminal marker for a map
     *
     * @return true if successful
     */
    bool
    writeTerminal();
};

}  // namespace v1
}  // namespace catl