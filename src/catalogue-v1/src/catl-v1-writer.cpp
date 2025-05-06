#include "catl/v1/catl-v1-writer.h"
#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/shamap/shamap-pathfinder.h"
#include "catl/shamap/shamap.h"
#include "catl/v1/catl-v1-errors.h"
#include "catl/v1/catl-v1-structs.h"
#include "catl/v1/catl-v1-utils.h"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <map>
#include <vector>

namespace catl {
namespace v1 {

Writer::Writer(const std::string& output_path, uint32_t network_id)
    : filepath_(output_path), network_id_(network_id)
{
    // Open file in binary mode
    file_.open(filepath_, std::ios::binary | std::ios::out | std::ios::trunc);

    if (!file_.is_open())
    {
        throw CatlV1Error("Failed to open output file: " + filepath_);
    }

    LOGI("Created CATL writer for file: ", filepath_);
}

Writer::~Writer()
{
    // Ensure the file is properly closed
    if (file_.is_open())
    {
        // If not finalized, try to finalize
        if (!finalized_ && header_written_)
        {
            try
            {
                finalize();
            }
            catch (const std::exception& e)
            {
                LOGE("Error finalizing file during destruction: ", e.what());
            }
        }

        file_.close();
    }
}

bool
Writer::writeHeader(uint32_t min_ledger, uint32_t max_ledger)
{
    if (header_written_)
    {
        LOGE("Header already written");
        return false;
    }

    if (min_ledger > max_ledger)
    {
        LOGE("Invalid ledger range: min > max");
        return false;
    }

    // Create header structure
    CatlHeader header;
    header.magic = CATL_MAGIC;
    header.version = BASE_CATALOGUE_VERSION;  // No compression
    header.network_id = network_id_;
    header.min_ledger = min_ledger;
    header.max_ledger = max_ledger;
    header.filesize = 0;  // Will update this in finalize()

    // Write header to file
    file_.write(reinterpret_cast<const char*>(&header), sizeof(CatlHeader));

    if (file_.fail())
    {
        LOGE("Failed to write header");
        return false;
    }

    file_position_ = sizeof(CatlHeader);
    header_written_ = true;

    LOGI("Wrote CATL header: ledger range ", min_ledger, "-", max_ledger);
    return true;
}

bool
Writer::writeLedger(
    const LedgerInfo& header,
    const SHAMap& state_map,
    const SHAMap& tx_map)
{
    if (!header_written_)
    {
        LOGE("Cannot write ledger before header");
        return false;
    }

    if (finalized_)
    {
        LOGE("Cannot write ledger after finalization");
        return false;
    }

    // Write ledger header
    file_.write(reinterpret_cast<const char*>(&header), sizeof(LedgerInfo));

    if (file_.fail())
    {
        LOGE("Failed to write ledger header");
        return false;
    }

    file_position_ += sizeof(LedgerInfo);

    // Write state map (full, not delta)
    LOGI("Writing state map for ledger ", header.sequence);

    // Count of items in the state map
    uint32_t state_items_count = 0;

    // Use the visitor pattern to write all items
    state_map.visit_items([&](const MmapItem& item) {
        // Since we don't have direct access to the node type, use
        // tnACCOUNT_STATE as default for state map
        if (!writeItem(
                tnACCOUNT_STATE,  // State map uses this type
                item.key(),
                item.slice().data(),
                item.slice().size()))
        {
            LOGE("Failed to write state map item: ", item.key().hex());
        }
        else
        {
            state_items_count++;
        }
    });

    // Write terminal marker for state map
    if (!writeTerminal())
    {
        LOGE("Failed to write state map terminal marker");
        return false;
    }

    // Write transaction map
    LOGI("Writing transaction map for ledger ", header.sequence);

    // Count of items in the transaction map
    uint32_t tx_items_count = 0;

    // Use the visitor pattern to write all items
    tx_map.visit_items([&](const MmapItem& item) {
        // Since we don't have direct access to the node type, use
        // tnTRANSACTION_MD as default for tx map
        if (!writeItem(
                tnTRANSACTION_MD,  // Transaction map uses this type
                item.key(),
                item.slice().data(),
                item.slice().size()))
        {
            LOGE("Failed to write transaction map item: ", item.key().hex());
        }
        else
        {
            tx_items_count++;
        }
    });

    // Write terminal marker for transaction map
    if (!writeTerminal())
    {
        LOGE("Failed to write transaction map terminal marker");
        return false;
    }

    LOGI(
        "Successfully wrote ledger ",
        header.sequence,
        " with ",
        state_items_count,
        " state items and ",
        tx_items_count,
        " transaction items");

    return true;
}

bool
Writer::writeStateDelta(const SHAMap& base_map, const SHAMap& new_map)
{
    if (!header_written_)
    {
        LOGE("Cannot write state delta before header");
        return false;
    }

    if (finalized_)
    {
        LOGE("Cannot write state delta after finalization");
        return false;
    }

    // Track items and their existence in both maps
    std::map<Key, bool> base_items_map;
    std::map<Key, const MmapItem*> new_items_map;

    // Fill the maps with items from both trees
    base_map.visit_items(
        [&](const MmapItem& item) { base_items_map[item.key()] = true; });

    new_map.visit_items(
        [&](const MmapItem& item) { new_items_map[item.key()] = &item; });

    // Count of changes written
    uint32_t changes = 0;

    // Process removals (items in base but not in new)
    for (const auto& [key, _] : base_items_map)
    {
        if (new_items_map.find(key) == new_items_map.end())
        {
            // This key exists in base but not in new, so it was removed
            if (!writeItem(tnREMOVE, key, nullptr, 0))
            {
                LOGE("Failed to write removal for key: ", key.hex());
                return false;
            }
            changes++;
        }
    }

    // Process additions and modifications (items in new but not in base, or
    // different)
    for (const auto& [key, item_ptr] : new_items_map)
    {
        bool is_new = base_items_map.find(key) == base_items_map.end();

        if (is_new)
        {
            // This is a new item, add it as an account state item
            if (!writeItem(
                    tnACCOUNT_STATE,  // Assume state map items
                    item_ptr->key(),
                    item_ptr->slice().data(),
                    item_ptr->slice().size()))
            {
                LOGE("Failed to write addition for key: ", key.hex());
                return false;
            }
            changes++;
        }
        else
        {
            // This item exists in both maps, we need to check if it changed
            // But we don't have a direct way to compare the items
            // For now, let's assume it changed and write it
            if (!writeItem(
                    tnACCOUNT_STATE,  // Assume state map items
                    item_ptr->key(),
                    item_ptr->slice().data(),
                    item_ptr->slice().size()))
            {
                LOGE("Failed to write modification for key: ", key.hex());
                return false;
            }
            changes++;
        }
    }

    // Write terminal marker
    if (!writeTerminal())
    {
        LOGE("Failed to write state delta terminal marker");
        return false;
    }

    LOGI("Wrote state delta with ", changes, " changes");
    return true;
}

bool
Writer::finalize()
{
    if (!header_written_)
    {
        LOGE("Cannot finalize file before writing header");
        return false;
    }

    if (finalized_)
    {
        LOGE("File already finalized");
        return false;
    }

    // Update file size in header
    file_.flush();

    // Get current file size
    auto current_pos = file_.tellp();

    if (current_pos < 0)
    {
        LOGE("Failed to get current file position");
        return false;
    }

    // Seek to the filesize field in the header
    file_.seekp(offsetof(CatlHeader, filesize));

    if (file_.fail())
    {
        LOGE("Failed to seek to header filesize field");
        return false;
    }

    // Write the file size
    uint64_t filesize = static_cast<uint64_t>(current_pos);
    file_.write(reinterpret_cast<const char*>(&filesize), sizeof(filesize));

    if (file_.fail())
    {
        LOGE("Failed to update header filesize");
        return false;
    }

    // Return to the end of the file
    file_.seekp(0, std::ios::end);

    if (file_.fail())
    {
        LOGE("Failed to return to end of file");
        return false;
    }

    file_.flush();
    finalized_ = true;

    LOGI("Finalized CATL file with size ", filesize, " bytes");
    return true;
}

bool
Writer::writeItem(
    SHAMapNodeType node_type,
    const Key& key,
    const uint8_t* data,
    uint32_t size)
{
    // Write node type
    uint8_t type_byte = static_cast<uint8_t>(node_type);
    file_.write(reinterpret_cast<const char*>(&type_byte), sizeof(type_byte));

    if (file_.fail())
    {
        LOGE("Failed to write node type");
        return false;
    }

    file_position_ += sizeof(type_byte);

    // Write key
    file_.write(reinterpret_cast<const char*>(key.data()), Key::size());

    if (file_.fail())
    {
        LOGE("Failed to write key");
        return false;
    }

    file_position_ += Key::size();

    // For node types other than tnREMOVE, write data size and data
    if (node_type != tnREMOVE)
    {
        // Write data size
        file_.write(reinterpret_cast<const char*>(&size), sizeof(size));

        if (file_.fail())
        {
            LOGE("Failed to write data size");
            return false;
        }

        file_position_ += sizeof(size);

        // Write data if present
        if (size > 0 && data != nullptr)
        {
            file_.write(reinterpret_cast<const char*>(data), size);

            if (file_.fail())
            {
                LOGE("Failed to write item data");
                return false;
            }

            file_position_ += size;
        }
    }

    return true;
}

bool
Writer::writeTerminal()
{
    // Write the terminal node type
    uint8_t terminal = static_cast<uint8_t>(tnTERMINAL);
    file_.write(reinterpret_cast<const char*>(&terminal), sizeof(terminal));

    if (file_.fail())
    {
        LOGE("Failed to write terminal marker");
        return false;
    }

    file_position_ += sizeof(terminal);
    return true;
}

}  // namespace v1
}  // namespace catl