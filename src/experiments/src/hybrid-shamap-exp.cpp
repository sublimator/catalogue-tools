/**
 * Hybrid SHAMap experiment
 *
 * This program explores a hybrid merkle tree architecture that can use
 * mmap'd nodes, memory nodes, and placeholder nodes.
 */

#include "../../shamap/src/pretty-print-json.h"
#include "catl/core/logger.h"
#include "catl/hybrid-shamap-v2/hybrid-shamap.h"
#include "catl/hybrid-shamap-v2/tree-walker.h"
#include "catl/v2/catl-v2-memtree-diff.h"
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
#include <unordered_set>
#include <vector>

#include "catl/shamap/shamap.h"
#include "catl/v1/catl-v1-ledger-info-view.h"
// #include "catl/v2/shamap-custom-traits.h"

using namespace catl::hybrid_shamap;

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
    to_json(const LeafView& leaf) const
    {
        catl::xdata::JsonVisitor visitor(protocol_);
        catl::xdata::ParserContext ctx(leaf.data);
        catl::xdata::parse_with_visitor(ctx, protocol_, visitor);
        return visitor.get_result();
    }

    void
    pretty_print(std::ostream& os, const LeafView& leaf) const
    {
        auto json = to_json(leaf);
        pretty_print_json(os, json);
    }
};

std::shared_ptr<catl::shamap::SHAMap>
create_state_map(InnerNodeView state_view)
{
    auto _state_map =
        std::make_shared<catl::shamap::SHAMap>(catl::shamap::tnACCOUNT_STATE);
    MemTreeOps::walk_leaves(state_view, [&](const Key& k, const Slice& data) {
        boost::intrusive_ptr<MmapItem> mmap_item(
            new MmapItem(k.data(), data.data(), data.size()));
        _state_map->add_item(mmap_item);
        return true;
    });
    return _state_map;
}

void
test_diff(
    catl::v2::CatlV2Reader& reader,
    const catl::common::LedgerInfo& ledger_info)
{
    auto state_view = MemTreeOps::get_inner_node(reader.current_data());
    auto state_map = *create_state_map(state_view);

    if (ledger_info.account_hash != state_map.get_hash())
    {
        throw std::runtime_error("Account state hash mismatch, for ledger 1");
    }
    reader.seek_to_ledger(ledger_info.seq + 9998);
    auto second_ledger_info = reader.read_ledger_info();
    auto second_state_view = MemTreeOps::get_inner_node(reader.current_data());
    // auto second_state_map = *create_state_map(second_state_view);
    // confirming that our catl packs and readers are working correctly
    // we can walk an InnerNodeView and get the hash of the state tree using our
    // gold SHAMap impl
    // if (second_ledger_info.account_hash != second_state_map.get_hash())
    // {
    //     throw std::runtime_error(
    //         "Account state hash mismatch, for ledger 2 full rebuild");
    // }

    // This means the error must be in the diff code

    auto i = 0;
    size_t added = 0, modified = 0, deleted = 0;
    std::unordered_set<std::string> processed_keys;
    auto callback = [&](const Key& key,
                        catl::v2::DiffOp op,
                        const Slice& old_data,
                        const Slice& new_data) {
        // Check for duplicate processing
        auto key_str = key.hex();
        if (!processed_keys.insert(key_str).second)
        {
            LOGW(
                "DUPLICATE: Key processed multiple times: ",
                key.hex(),
                " op=",
                op == catl::v2::DiffOp::Added
                    ? "Added"
                    : op == catl::v2::DiffOp::Modified ? "Modified"
                                                       : "Deleted");
            // Let's check what the key looks like in both trees
            auto check_a = MemTreeOps::lookup_key_optional(state_view, key);
            auto check_b =
                MemTreeOps::lookup_key_optional(second_state_view, key);
            LOGW(
                "  -> Key in tree A: ",
                check_a.has_value() ? "EXISTS" : "NOT FOUND");
            LOGW(
                "  -> Key in tree B: ",
                check_b.has_value() ? "EXISTS" : "NOT FOUND");
        }

        boost::intrusive_ptr mmap_item(
            new MmapItem(key.data(), new_data.data(), new_data.size()));

        if (op == catl::v2::DiffOp::Added)
        {
            added++;
            // Use set_item with ADD_OR_UPDATE mode to handle potential
            // duplicates
            auto result =
                state_map.set_item(mmap_item, catl::shamap::SetMode::ADD_ONLY);
            if (result != catl::shamap::SetResult::ADD)
            {
                LOGW("Item reported as Added but already existed: ", key.hex());
                // Debug: check if this item exists in the original tree
                auto check = MemTreeOps::lookup_key_optional(state_view, key);
                if (check)
                {
                    LOGW("  -> This key DOES exist in original tree A!");
                }
            }
        }
        else if (op == catl::v2::DiffOp::Deleted)
        {
            deleted++;
            if (!state_map.remove_item(key))
            {
                LOGW("Failed to delete item: ", key.hex());
            }
        }
        else if (op == catl::v2::DiffOp::Modified)
        {
            modified++;
            auto result = state_map.set_item(
                mmap_item, catl::shamap::SetMode::UPDATE_ONLY);
            if (result != catl::shamap::SetResult::UPDATE)
            {
                LOGW("Failed to modify item: ", key.hex());
            }
        }
        i++;
        return true;
    };
    LOGI("Diffing ledgers...");
    diff_memtree_nodes(state_view, second_state_view, callback);
    LOGI(
        "Diff complete: ",
        i,
        " changes (Added: ",
        added,
        ", Modified: ",
        modified,
        ", Deleted: ",
        deleted,
        ")");
    auto final_hash = state_map.get_hash();
    if (second_ledger_info.account_hash != final_hash)
    {
        LOGE("Hash mismatch for ledger 2:");
        LOGE("  Expected: ", second_ledger_info.account_hash.hex());
        LOGE("  Got:      ", final_hash.hex());
        throw std::runtime_error("Account state hash mismatch, for ledger 2");
    }
    LOGI("✓ Hash verified successfully!");
}

int
main(int argc, char* argv[])
{
    // Set log level to DEBUG for maximum visibility
    Logger::set_level(LogLevel::INFO);

    // Disable v2-structs partition logging
    catl::v2::get_v2_memtree_log_partition().set_level(LogLevel::NONE);

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

        test_diff(*reader, ledger_info);

        return 0;
    }
    catch (const std::exception& e)
    {
        LOGE("Error: ", e.what());
        return 1;
    }
}
