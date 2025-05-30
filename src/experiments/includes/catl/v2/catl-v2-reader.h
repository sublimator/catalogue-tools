#pragma once

#include "catl/common/ledger-info.h"
#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/v2/catl-v2-ledger-index-view.h"
#include "catl/v2/catl-v2-structs.h"
#include "shamap-custom-traits.h"

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <cstring>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace catl::v2 {

/**
 * MMAP-based reader for CATL v2 format
 *
 * This reader provides high-performance access to ledgers stored in the CATL v2
 * format using memory-mapped I/O via Boost. It supports:
 * - Zero-copy reading of ledger headers (canonical LedgerInfo format)
 * - Fast skipping over state/tx maps
 * - Direct memory access to all data structures
 *
 * The reader is designed for streaming access patterns where you process
 * ledgers sequentially, but also supports using the index for random access
 * when needed.
 */
class CatlV2Reader
{
public:
    explicit CatlV2Reader(const std::string& filename) : filename_(filename)
    {
        try
        {
            // Check if file exists
            if (!boost::filesystem::exists(filename_))
            {
                throw std::runtime_error("File does not exist: " + filename_);
            }

            // Check file size
            boost::uintmax_t file_size =
                boost::filesystem::file_size(filename_);
            if (file_size == 0)
            {
                throw std::runtime_error("File is empty: " + filename_);
            }

            // Open the memory-mapped file
            mmap_file_.open(filename_);
            if (!mmap_file_.is_open())
            {
                throw std::runtime_error(
                    "Failed to memory map file: " + filename_);
            }

            // Set up internal state
            data_ = reinterpret_cast<const uint8_t*>(mmap_file_.data());
            file_size_ = mmap_file_.size();

            if (!data_)
            {
                throw std::runtime_error(
                    "Memory mapping succeeded but data pointer is null");
            }

            // Read and validate header
            read_and_validate_header();

            // Start reading position after header
            current_pos_ = sizeof(CatlV2Header);
        }
        catch (const boost::filesystem::filesystem_error& e)
        {
            throw std::runtime_error(
                "Filesystem error: " + std::string(e.what()));
        }
        catch (const std::ios_base::failure& e)
        {
            throw std::runtime_error("I/O error: " + std::string(e.what()));
        }
    }

    ~CatlV2Reader()
    {
        if (mmap_file_.is_open())
        {
            mmap_file_.close();
        }
    }

    // Delete copy operations
    CatlV2Reader(const CatlV2Reader&) = delete;
    CatlV2Reader&
    operator=(const CatlV2Reader&) = delete;

    /**
     * Get the file header
     */
    const CatlV2Header&
    header() const
    {
        return header_;
    }

    /**
     * Read the next ledger info from current position
     *
     * Also reads the TreesHeader that follows it, making tree sizes
     * available for skipping.
     *
     * @return The canonical ledger info
     * @throws std::runtime_error if at EOF or read error
     */
    const catl::common::LedgerInfo&
    read_ledger_info()
    {
        if (current_pos_ + sizeof(catl::common::LedgerInfo) +
                sizeof(TreesHeader) >
            file_size_)
        {
            throw std::runtime_error("Attempted to read past end of file");
        }

        auto info = reinterpret_cast<const catl::common::LedgerInfo*>(
            data_ + current_pos_);
        current_pos_ += sizeof(catl::common::LedgerInfo);

        // Read the trees header that follows
        current_trees_header_ =
            *reinterpret_cast<const TreesHeader*>(data_ + current_pos_);
        current_pos_ += sizeof(TreesHeader);

        current_ledger_seq_ = info->seq;
        return *info;
    }

    /**
     * Skip the state map
     *
     * Uses the tree size from the most recent read_ledger_info().
     *
     * @return Number of bytes skipped
     */
    std::uint64_t
    skip_state_map()
    {
        current_pos_ += current_trees_header_.state_tree_size;
        return current_trees_header_.state_tree_size;
    }

    /**
     * Skip the transaction map
     *
     * Uses the tree size from the most recent read_ledger_info().
     *
     * @return Number of bytes skipped
     */
    std::uint64_t
    skip_tx_map()
    {
        current_pos_ += current_trees_header_.tx_tree_size;
        return current_trees_header_.tx_tree_size;
    }

    /**
     * Get current file position
     */
    std::uint64_t
    current_offset() const
    {
        return current_pos_;
    }

    /**
     * Check if we've reached end of ledgers
     * (but before the index)
     */
    bool
    at_end_of_ledgers() const
    {
        return current_pos_ >= header_.ledger_index_offset;
    }

    /**
     * Get direct pointer to data at current position
     * (for zero-copy operations)
     */
    const uint8_t*
    current_data() const
    {
        return data_ + current_pos_;
    }

    /**
     * Get direct pointer to data at specific offset
     */
    const uint8_t*
    data_at(size_t offset) const
    {
        if (offset >= file_size_)
        {
            throw std::runtime_error("Requested offset is beyond file bounds");
        }
        return data_ + offset;
    }

    /**
     * Look up a key in the current state tree
     *
     * Must be called after read_ledger_info() to have tree offsets.
     * Returns the leaf data as a Slice, or empty optional if not found.
     *
     * @param key The key to search for
     * @return Optional Slice containing the leaf data
     */
    std::optional<Slice>
    lookup_key_in_state(const Key& key)
    {
        // The state tree starts at current_pos_ after read_ledger_info()
        // read_ledger_info() reads: LedgerInfo + TreesHeader
        // and positions current_pos_ right at the start of the state tree
        size_t tree_offset = current_pos_;
        LOGD(
            "State tree lookup - tree offset: ",
            tree_offset,
            ", current_pos: ",
            current_pos_,
            ", state_tree_size: ",
            current_trees_header_.state_tree_size);
        return lookup_key_at_node(key, tree_offset, 0);
    }

    /**
     * Get the ledger index view
     *
     * Loads the index on first access (lazy loading).
     *
     * @return View into the ledger index
     */
    const LedgerIndexView&
    get_ledger_index()
    {
        if (!ledger_index_.has_value())
        {
            load_ledger_index();
        }
        return ledger_index_.value();
    }

    /**
     * Seek to a specific ledger by sequence number
     *
     * Uses the ledger index to jump directly to the ledger.
     *
     * @param sequence Ledger sequence to seek to
     * @return true if found and positioned at ledger header
     */
    bool
    seek_to_ledger(uint32_t sequence)
    {
        const auto& index = get_ledger_index();
        const auto* entry = index.find_ledger(sequence);

        if (!entry)
        {
            return false;
        }

        current_pos_ = entry->header_offset;
        return true;
    }

private:
    std::string filename_;
    boost::iostreams::mapped_file_source mmap_file_;
    const uint8_t* data_ = nullptr;
    size_t file_size_ = 0;
    size_t current_pos_ = 0;

    CatlV2Header header_;
    std::uint32_t current_ledger_seq_ = 0;
    TreesHeader current_trees_header_{};
    std::optional<LedgerIndexView> ledger_index_;

    /**
     * Read and validate the file header
     */
    void
    read_and_validate_header()
    {
        if (file_size_ < sizeof(CatlV2Header))
        {
            throw std::runtime_error("File too small to contain header");
        }

        // Copy header from mmap data
        std::memcpy(&header_, data_, sizeof(CatlV2Header));

        // Validate magic
        if (header_.magic != std::array<char, 4>{'C', 'A', 'T', '2'})
        {
            throw std::runtime_error("Invalid file magic");
        }

        // Validate version
        if (header_.version != 1)
        {
            throw std::runtime_error(
                "Unsupported file version: " + std::to_string(header_.version));
        }
    }

    /**
     * Load the ledger index from the end of the file
     */
    void
    load_ledger_index()
    {
        if (header_.ledger_index_offset +
                header_.ledger_count * sizeof(LedgerIndexEntry) >
            file_size_)
        {
            throw std::runtime_error("Invalid ledger index offset or size");
        }

        // Create a view directly into the mmap'd index data
        auto index_data = reinterpret_cast<const LedgerIndexEntry*>(
            data_ + header_.ledger_index_offset);

        ledger_index_.emplace(index_data, header_.ledger_count);
    }

    /**
     * Lookup a key in the tree using stack-based traversal
     *
     * @param key The key to search for
     * @param root_offset Offset to the root node
     * @param start_depth Starting depth (usually 0)
     * @return Optional Slice containing the leaf data
     */
    std::optional<Slice>
    lookup_key_at_node(const Key& key, size_t root_offset, int start_depth)
    {
        struct StackEntry
        {
            size_t node_offset;
            int depth;
        };

        LOGD("=== Starting key lookup ===");
        LOGD("Target key: ", key.hex());
        LOGD("Root offset: ", root_offset);
        LOGD("Start depth: ", start_depth);

        // Use a fixed-size stack (max tree depth is 64)
        StackEntry stack[64];
        int stack_top = 0;

        // Push root
        stack[stack_top++] = {root_offset, start_depth};
        LOGD("Pushed root to stack");

        int nodes_visited = 0;
        while (stack_top > 0)
        {
            auto [node_offset, depth] = stack[--stack_top];
            nodes_visited++;

            LOGD("--- Node ", nodes_visited, " ---");
            LOGD("Node offset: ", node_offset);
            LOGD("Depth: ", depth);

            if (node_offset + 2 > file_size_)
            {
                LOGE("Node offset exceeds file size!");
                return std::nullopt;
            }

            // Check if this is an inner node by looking at depth
            std::uint16_t first_two_bytes;
            std::memcpy(&first_two_bytes, data_ + node_offset, 2);
            LOGD("First two bytes: ", first_two_bytes);

            if (first_two_bytes < 64)  // It's an inner node
            {
                LOGD("Processing INNER node");

                // Read the inner node header
                if (node_offset + sizeof(InnerNodeHeader) > file_size_)
                {
                    LOGE("Inner node header exceeds file size!");
                    return std::nullopt;
                }

                auto inner_header = reinterpret_cast<const InnerNodeHeader*>(
                    data_ + node_offset);

                LOGD(
                    "Inner node depth from header: ",
                    (int)inner_header->bits.depth);
                LOGD(
                    "Child types bitmap: 0x",
                    std::hex,
                    inner_header->child_types,
                    std::dec);

                // Determine which branch to follow based on the key nibble
                int nibble_idx = inner_header->bits.depth;
                if (nibble_idx >= 64)  // Sanity check
                {
                    LOGE("Invalid nibble index: ", nibble_idx);
                    return std::nullopt;
                }

                // Extract the nibble from the key
                uint8_t byte = key.data()[nibble_idx / 2];
                uint8_t nibble = (nibble_idx & 1) ? (byte & 0x0F) : (byte >> 4);
                LOGD(
                    "Key byte[",
                    nibble_idx / 2,
                    "] = 0x",
                    std::hex,
                    (int)byte,
                    std::dec);
                LOGD("Extracted nibble[", nibble_idx, "] = ", (int)nibble);

                // Check if this child exists
                ChildType child_type = inner_header->get_child_type(nibble);
                LOGD(
                    "Child type for nibble ",
                    (int)nibble,
                    ": ",
                    (child_type == ChildType::EMPTY
                         ? "EMPTY"
                         : (child_type == ChildType::INNER ? "INNER"
                                                           : "LEAF")));

                if (child_type == ChildType::EMPTY)
                {
                    LOGW(
                        "No child at nibble ", (int)nibble, " - key not found");
                    return std::nullopt;
                }

                // Find the child's index among non-empty children
                int child_index = 0;
                for (int i = 0; i < nibble; ++i)
                {
                    if (inner_header->get_child_type(i) != ChildType::EMPTY)
                    {
                        child_index++;
                    }
                }
                LOGD("Child index among non-empty children: ", child_index);

                // Count total non-empty children for debugging
                int total_children = 0;
                for (int i = 0; i < 16; ++i)
                {
                    if (inner_header->get_child_type(i) != ChildType::EMPTY)
                    {
                        total_children++;
                    }
                }
                LOGD("Total non-empty children: ", total_children);

                // Read the child offset
                size_t offsets_start = node_offset + sizeof(InnerNodeHeader);
                if (offsets_start + (child_index + 1) * sizeof(std::uint64_t) >
                    file_size_)
                {
                    LOGE("Child offset exceeds file size!");
                    return std::nullopt;
                }

                std::uint64_t child_offset;
                std::memcpy(
                    &child_offset,
                    data_ + offsets_start + child_index * sizeof(std::uint64_t),
                    sizeof(child_offset));
                LOGD("Child offset: ", child_offset);

                // Push child to stack
                if (stack_top >= 64)  // Stack overflow protection
                {
                    throw std::runtime_error("Tree depth exceeds 64");
                }
                stack[stack_top++] = {
                    child_offset, inner_header->bits.depth + 1};
                LOGD("Pushed child to stack, new stack top: ", stack_top);
            }
            else  // It's a leaf node
            {
                LOGD("Processing LEAF node");

                // Read the leaf header
                if (node_offset + sizeof(LeafHeader) > file_size_)
                {
                    LOGE("Leaf header exceeds file size!");
                    return std::nullopt;
                }

                auto leaf_header =
                    reinterpret_cast<const LeafHeader*>(data_ + node_offset);

                // Convert leaf key to hex for logging
                Hash256 leaf_key_hash(leaf_header->key.data());
                LOGD("Leaf key: ", leaf_key_hash.hex());

                // Check if this is the key we're looking for
                bool key_match =
                    std::memcmp(leaf_header->key.data(), key.data(), 32) == 0;
                LOGD("Key match: ", key_match ? "YES!" : "no");

                if (key_match)
                {
                    // Found it! Return the data
                    size_t data_offset = node_offset + sizeof(LeafHeader);
                    size_t data_size = leaf_header->data_size();
                    LOGD("Found key! Data size: ", data_size, " bytes");

                    if (data_offset + data_size > file_size_)
                    {
                        LOGE("Leaf data exceeds file size!");
                        return std::nullopt;
                    }

                    LOGD("=== Key lookup successful! ===");
                    LOGD("Total nodes visited: ", nodes_visited);
                    return Slice(data_ + data_offset, data_size);
                }

                // Not the key we're looking for, continue
                LOGD("Not our key, continuing search...");
            }
        }

        LOGW("=== Key lookup failed - key not found ===");
        LOGW("Total nodes visited: ", nodes_visited);
        return std::nullopt;  // Key not found
    }
};

}  // namespace catl::v2