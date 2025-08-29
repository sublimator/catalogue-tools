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
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "catl/hybrid-shamap-v2/hmap.h"
#include "catl/shamap/shamap-utils.h"
#include "catl/v1/catl-v1-ledger-info-view.h"

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
    to_json(const catl::v2::LeafView& leaf) const
    {
        catl::xdata::JsonVisitor visitor(protocol_);
        catl::xdata::ParserContext ctx(leaf.data);
        catl::xdata::parse_with_visitor(ctx, protocol_, visitor);
        return visitor.get_result();
    }

    void
    pretty_print(std::ostream& os, const catl::v2::LeafView& leaf) const
    {
        auto json = to_json(leaf);
        pretty_print_json(os, json);
    }
};

std::unique_ptr<Hmap>
create_hybrid_map_from_mmap(
    const uint8_t* root_ptr,
    std::shared_ptr<catl::v2::MmapHolder> holder)
{
    auto hmap = std::make_unique<Hmap>(holder);
    hmap->set_root_raw(root_ptr);
    return hmap;
}

void
test_diff(
    catl::v2::CatlV2Reader& reader,
    const catl::common::LedgerInfo& first_ledger_info,
    int delta_n,
    bool materialize_only = false)
{
    // Create hybrid map from the first ledger's mmap'd data
    auto hmap = create_hybrid_map_from_mmap(
        reader.current_data(), reader.mmap_holder());

    // Verify initial hash
    if (first_ledger_info.account_hash != hmap->get_root_hash())
    {
        LOGE("Initial hash mismatch:");
        LOGE("  Expected: ", first_ledger_info.account_hash.hex());
        LOGE("  Got:      ", hmap->get_root_hash().hex());
        throw std::runtime_error(
            "Account state hash mismatch, for first ledger");
    }

    LOGI("Starting from ledger ", first_ledger_info.seq);
    LOGI(
        "✓ Initial hash: ",
        first_ledger_info.account_hash.hex().substr(0, 16),
        "...");

    // Get the first ledger's state view for diffing
    auto first_state_view =
        catl::v2::MemTreeOps::get_inner_node(reader.current_data());

    // Seek to target ledger (first + delta_n)
    uint32_t target_seq = first_ledger_info.seq + delta_n;
    LOGI("Seeking to ledger ", target_seq, " (delta=", delta_n, ")");
    reader.seek_to_ledger(target_seq);
    auto second_ledger_info = reader.read_ledger_info();
    auto second_state_view =
        catl::v2::MemTreeOps::get_inner_node(reader.current_data());

    LOGI("Applying diff: ledger ", first_ledger_info.seq, " -> ", target_seq);
    if (materialize_only)
    {
        LOGI("MODE: Materialize-only (no modifications)");
    }

    auto i = 0;
    size_t added = 0, modified = 0, deleted = 0;
    std::unordered_set<std::string> processed_keys;

    // Log all keys to understand branch patterns
    LOGI("Keys in diff (showing first 4 hex chars = first 2 branches):");
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
        }

        // Log key with branch info (first byte = branch at depth 0, etc.)
        std::string branches;
        for (int d = 0; d < 4; ++d)
        {
            int branch = catl::shamap::select_branch(key, d);
            char hex = (branch < 10) ? ('0' + branch) : ('A' + branch - 10);
            branches += hex;
        }
        LOGI(
            "  [",
            i + 1,
            "] Key: ",
            key_str.substr(0, 16),
            "... branches: ",
            branches,
            " (",
            op == catl::v2::DiffOp::Added
                ? "ADD"
                : op == catl::v2::DiffOp::Modified ? "MOD" : "DEL",
            ")");

        if (materialize_only)
        {
            // Just materialize the path and check hash
            if (op == catl::v2::DiffOp::Added)
                added++;
            else if (op == catl::v2::DiffOp::Modified)
                modified++;
            else if (op == catl::v2::DiffOp::Deleted)
                deleted++;

            hmap->materialize_path(key);

            // Check hash after each materialization
            auto current_hash = hmap->get_root_hash();
            LOGI(
                "  [",
                i + 1,
                "] After materializing key ",
                key.hex().substr(0, 16),
                "... (",
                op == catl::v2::DiffOp::Added
                    ? "ADD"
                    : op == catl::v2::DiffOp::Modified ? "MOD" : "DEL",
                "), hash: ",
                current_hash.hex().substr(0, 16),
                "...");
        }
        else
        {
            // Normal mode - apply actual modifications
            if (op == catl::v2::DiffOp::Added)
            {
                added++;
                auto result = hmap->set_item(
                    key, new_data, catl::shamap::SetMode::ADD_ONLY);
                if (result != catl::shamap::SetResult::ADD)
                {
                    LOGW(
                        "Item reported as Added but already existed: ",
                        key.hex());
                }
            }
            else if (op == catl::v2::DiffOp::Deleted)
            {
                deleted++;
                if (!hmap->remove_item(key))
                {
                    LOGW("Failed to delete item: ", key.hex());
                }
            }
            else if (op == catl::v2::DiffOp::Modified)
            {
                modified++;
                auto result = hmap->set_item(
                    key, new_data, catl::shamap::SetMode::UPDATE_ONLY);
                if (result != catl::shamap::SetResult::UPDATE)
                {
                    LOGW("Failed to modify item: ", key.hex());
                }
            }
        }
        i++;
        return true;
    };
    // Apply the diff between the two ledgers
    diff_memtree_nodes(first_state_view, second_state_view, callback);
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

    // Get the final hash from the hybrid map
    auto final_hash = hmap->get_root_hash();

    if (materialize_only)
    {
        // In materialize-only mode, the hash should remain unchanged
        LOGI("Final hash after materializing ", i, " paths:");
        LOGI("  Initial:  ", first_ledger_info.account_hash.hex());
        LOGI("  Current:  ", final_hash.hex());
        if (first_ledger_info.account_hash != final_hash)
        {
            LOGE("ERROR: Hash changed after materialization!");
            LOGE("  This indicates a bug in materialization");
        }
        else
        {
            LOGI("✓ Hash unchanged after materialization (as expected)");
        }
    }
    else
    {
        // Normal mode - check against expected hash
        if (second_ledger_info.account_hash != final_hash)
        {
            LOGE("Hash mismatch for ledger 2:");
            LOGE("  Expected: ", second_ledger_info.account_hash.hex());
            LOGE("  Got:      ", final_hash.hex());
            throw std::runtime_error(
                "Account state hash mismatch, for ledger 2");
        }
        LOGI("✓ Hash verified successfully after applying ", i, " diffs!");
        LOGI("  Final hash: ", final_hash.hex().substr(0, 16), "...");
    }
}

int
main(int argc, char* argv[])
{
    // Check for LOG_LEVEL environment variable
    const char* log_level_env = std::getenv("LOG_LEVEL");
    if (log_level_env)
    {
        std::string level_str(log_level_env);
        if (level_str == "DEBUG")
            Logger::set_level(LogLevel::DEBUG);
        else if (level_str == "INFO")
            Logger::set_level(LogLevel::INFO);
        else if (level_str == "WARN" || level_str == "WARNING")
            Logger::set_level(LogLevel::WARNING);
        else if (level_str == "ERROR")
            Logger::set_level(LogLevel::ERROR);
        else
            Logger::set_level(LogLevel::INFO);  // Default
    }
    else
    {
        // Set log level to DEBUG for maximum visibility
        Logger::set_level(LogLevel::DEBUG);  // Changed to DEBUG for now
    }

    // Disable v2-structs partition logging
    catl::v2::get_v2_memtree_log_partition().set_level(LogLevel::NONE);

    // TODO: Use boost::program_options for better argument parsing
    if (argc < 2 || argc > 4)
    {
        LOGE(
            "Usage: ",
            argv[0],
            " <catl-v2-file> [delta-n] [--materialize-only]");
        LOGE("  delta-n: number of ledgers to skip (default: 1, max: 99)");
        LOGE(
            "  --materialize-only: just materialize paths without "
            "modifications");
        return 1;
    }

    const std::string filename = argv[1];
    int delta_n = 1;
    bool materialize_only = false;

    // Parse arguments (crude for now, use boost::program_options later)
    for (int i = 2; i < argc; ++i)
    {
        std::string arg = argv[i];
        if (arg == "--materialize-only")
        {
            materialize_only = true;
        }
        else if (std::isdigit(arg[0]))
        {
            delta_n = std::atoi(arg.c_str());
            if (delta_n < 1 || delta_n > 99)
            {
                LOGE("Delta must be between 1 and 99");
                return 1;
            }
        }
    }
    LOGI("Starting hybrid-shamap experiment with file: ", filename);
    LOGI("Delta: ", delta_n, " ledgers");
    if (materialize_only)
    {
        LOGI("Mode: MATERIALIZE-ONLY (no modifications)");
    }

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

        test_diff(*reader, ledger_info, delta_n, materialize_only);

        return 0;
    }
    catch (const std::exception& e)
    {
        LOGE("Error: ", e.what());
        return 1;
    }
}
