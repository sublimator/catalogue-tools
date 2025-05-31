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
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../../../hasher-v1/includes/catl/hasher-v1/ledger.h"

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
private:
    /**
     * Private constructor that takes raw memory
     * Used by create() and share() methods
     */
    CatlV2Reader(const uint8_t* data, size_t size)
        : data_(data), file_size_(size)
    {
        // Read and validate header
        read_and_validate_header();

        // Start reading position after header
        current_pos_ = sizeof(CatlV2Header);
    }

public:
    /**
     * Create a reader from a file
     * Handles all file I/O and memory mapping
     *
     * @param filename Path to CATL v2 file
     * @return Shared pointer to reader
     */
    static std::shared_ptr<CatlV2Reader>
    create(const std::string& filename)
    {
        // Store the mmap object in a shared structure
        struct MmapHolder
        {
            boost::iostreams::mapped_file_source mmap_file;
            std::string filename;
        };

        auto holder = std::make_shared<MmapHolder>();
        holder->filename = filename;

        try
        {
            // Check if file exists
            if (!boost::filesystem::exists(filename))
            {
                throw std::runtime_error("File does not exist: " + filename);
            }

            // Check file size
            boost::uintmax_t file_size = boost::filesystem::file_size(filename);
            if (file_size == 0)
            {
                throw std::runtime_error("File is empty: " + filename);
            }

            // Open the memory-mapped file
            holder->mmap_file.open(filename);
            if (!holder->mmap_file.is_open())
            {
                throw std::runtime_error(
                    "Failed to memory map file: " + filename);
            }

            // Get data pointer and size
            const uint8_t* data =
                reinterpret_cast<const uint8_t*>(holder->mmap_file.data());
            size_t size = holder->mmap_file.size();

            if (!data)
            {
                throw std::runtime_error(
                    "Memory mapping succeeded but data pointer is null");
            }

            // Create reader with custom deleter that keeps mmap alive
            return std::shared_ptr<CatlV2Reader>(
                new CatlV2Reader(data, size), [holder](CatlV2Reader* reader) {
                    delete reader;
                    // holder destructor will close mmap
                });
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

    /**
     * Create a new reader sharing the same memory
     * Each reader has its own traversal state (current_pos_, etc)
     *
     * @return New reader instance sharing same memory
     */
    std::shared_ptr<CatlV2Reader>
    share() const
    {
        // Simply create a new reader with the same memory
        // It will re-parse the header (48 bytes) but that's negligible
        return std::shared_ptr<CatlV2Reader>(
            new CatlV2Reader(data_, file_size_));
    }

    ~CatlV2Reader() = default;

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
     * Look up a key in the current transaction tree
     *
     * Must be called after read_ledger_info() to have tree offsets.
     * Returns the leaf data as a Slice, or empty optional if not found.
     *
     * @param key The key to search for (transaction hash)
     * @return Optional Slice containing the transaction + metadata
     */
    std::optional<Slice>
    lookup_key_in_tx(const Key& key)
    {
        // The tx tree starts after the state tree
        // current_pos_ is at the start of state tree after read_ledger_info()
        size_t tree_offset =
            current_pos_ + current_trees_header_.state_tree_size;
        LOGD(
            "Tx tree lookup - tree offset: ",
            tree_offset,
            ", current_pos: ",
            current_pos_,
            ", state_tree_size: ",
            current_trees_header_.state_tree_size,
            ", tx_tree_size: ",
            current_trees_header_.tx_tree_size);
        return lookup_key_at_node(key, tree_offset, 0);
    }

    /**
     * Walk all items in the current state tree
     *
     * Performs a depth-first traversal of the tree, calling the callback
     * for each leaf node with the key and data.
     *
     * @param callback Function called for each item: (Key, Slice) -> bool
     *                 Return false to stop iteration
     * @return Number of items visited
     */
    template <typename Callback>
    size_t
    walk_state_items(Callback&& callback)
    {
        size_t tree_offset = current_pos_;
        LOGD(
            "walk_state_items - tree_offset: ",
            tree_offset,
            ", state_tree_size: ",
            current_trees_header_.state_tree_size);
        return walk_items_at_node(
            tree_offset, 0, std::forward<Callback>(callback));
    }

    /**
     * Walk all items in the current transaction tree
     *
     * @param callback Function called for each item: (Key, Slice) -> bool
     *                 Return false to stop iteration
     * @return Number of items visited
     */
    template <typename Callback>
    size_t
    walk_tx_items(Callback&& callback)
    {
        size_t tree_offset =
            current_pos_ + current_trees_header_.state_tree_size;
        LOGD(
            "walk_tx_items - tree_offset: ",
            tree_offset,
            ", tx_tree_size: ",
            current_trees_header_.tx_tree_size);
        return walk_items_at_node(
            tree_offset, 0, std::forward<Callback>(callback));
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
    [[nodiscard]] std::optional<Slice>
    lookup_key_at_node(const Key& key, size_t root_offset, int start_depth)
        const
    {
        struct StackEntry
        {
            size_t node_offset;
            int depth;
            bool is_leaf;  // Determined by parent's child_types bitmap
        };

        LOGD("=== Starting key lookup ===");
        LOGD("Target key: ", key.hex());
        LOGD("Root offset: ", root_offset);
        LOGD("Start depth: ", start_depth);

        // Use a fixed-size stack (max tree depth is 64)
        StackEntry stack[64];
        int stack_top = 0;

        // Push root
        stack[stack_top++] = {root_offset, start_depth, false};
        LOGD("Pushed root to stack");

        int nodes_visited = 0;
        while (stack_top > 0)
        {
            auto [node_offset, depth, is_leaf] = stack[--stack_top];
            nodes_visited++;

            LOGD("--- Node ", nodes_visited, " ---");
            LOGD("Node offset: ", node_offset);
            LOGD("Depth: ", depth);
            LOGD("Is leaf: ", is_leaf);

            if (!is_leaf)  // It's an inner node
            {
                LOGD("Processing INNER node");

                // Read the inner node header
                if (node_offset + sizeof(InnerNodeHeader) > file_size_)
                {
                    LOGE("Inner node header exceeds file size!");
                    throw std::runtime_error(
                        "Inner node header exceeds file size");
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
                    throw std::runtime_error("Invalid nibble index");
                }

                // Extract the nibble from the key
                uint8_t byte = key.data()[nibble_idx / 2];
                uint8_t nibble = (nibble_idx & 1) ? (byte & 0x0F) : (byte >> 4);
                LOGD(
                    "Key byte[",
                    nibble_idx / 2,
                    "] = 0x",
                    std::hex,
                    static_cast<int>(byte),
                    std::dec);
                LOGD(
                    "Extracted nibble[",
                    nibble_idx,
                    "] = ",
                    static_cast<int>(nibble));

                // Check if this child exists
                ChildType child_type = inner_header->get_child_type(nibble);
                LOGD(
                    "Child type for nibble ",
                    static_cast<int>(nibble),
                    ": ",
                    (child_type == ChildType::EMPTY
                         ? "EMPTY"
                         : (child_type == ChildType::INNER ? "INNER"
                                                           : "LEAF")));

                if (child_type == ChildType::EMPTY)
                {
                    LOGW(
                        "No child at nibble ",
                        static_cast<int>(nibble),
                        " - key not found");
                    return std::nullopt;
                }

                // Use iterator to find the specific child
                size_t offsets_start = node_offset + sizeof(InnerNodeHeader);
                ChildIterator child_iter(inner_header, data_ + offsets_start);

                std::uint64_t child_offset = 0;
                bool found = false;

                while (child_iter.has_next())
                {
                    auto child = child_iter.next();
                    if (child.branch == nibble)
                    {
                        child_offset = child.offset;
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    LOGE(
                        "Failed to find child at nibble ",
                        static_cast<int>(nibble));
                    return std::nullopt;
                }

                LOGD("Child offset: ", child_offset);

                // Push child to stack with proper type information
                if (stack_top >= 64)  // Stack overflow protection
                {
                    throw std::runtime_error("Tree depth exceeds 64");
                }

                bool child_is_leaf = (child_type == ChildType::LEAF);
                stack[stack_top++] = {
                    child_offset, inner_header->bits.depth + 1, child_is_leaf};
                LOGD(
                    "Pushed child to stack, new stack top: ",
                    stack_top,
                    ", child is ",
                    child_is_leaf ? "LEAF" : "INNER");
            }
            else  // It's a leaf node
            {
                LOGD("Processing LEAF node");

                // Leaf nodes start directly with LeafHeader (no marker)
                size_t leaf_header_offset = node_offset;

                // Read the leaf header
                if (leaf_header_offset + sizeof(LeafHeader) > file_size_)
                {
                    LOGE("Leaf header exceeds file size!");
                    throw std::runtime_error("Leaf header exceeds file size");
                }

                auto leaf_header = reinterpret_cast<const LeafHeader*>(
                    data_ + leaf_header_offset);

                // Convert leaf key to hex for logging
                LOGD("Leaf key: ", Hash256(leaf_header->key.data()).hex());

                // Check if this is the key we're looking for
                bool key_match =
                    std::memcmp(leaf_header->key.data(), key.data(), 32) == 0;
                LOGD("Key match: ", key_match ? "YES!" : "no");

                if (key_match)
                {
                    // Found it! Return the data (after LeafHeader)
                    size_t data_offset =
                        leaf_header_offset + sizeof(LeafHeader);
                    size_t data_size = leaf_header->data_size();
                    LOGD("Found key! Data size: ", data_size, " bytes");

                    if (data_offset + data_size > file_size_)
                    {
                        LOGE("Leaf data exceeds file size!");
                        throw std::runtime_error("Leaf data exceeds file size");
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

    /**
     * Walk all items in a tree using iterative traversal
     *
     * @param root_offset Offset to the root node
     * @param start_depth Starting depth (usually 0)
     * @param callback Function to call for each leaf: (Key, Slice) -> bool
     * @return Number of items visited
     */
    template <typename Callback>
    size_t
    walk_items_at_node(size_t root_offset, int start_depth, Callback&& callback)
    {
        struct StackEntry
        {
            size_t node_offset;
            int depth;
            bool is_leaf;  // Determined by parent's child_types bitmap

            // For inner nodes, we store iteration state
            bool is_processing_children;
            uint32_t
                remaining_children_mask;  // Bitmask of children yet to process
            int offset_index;  // Current index in sparse offset array

            StackEntry()
                : node_offset(0)
                , depth(0)
                , is_leaf(false)
                , is_processing_children(false)
                , remaining_children_mask(0)
                , offset_index(0)
            {
            }
            StackEntry(size_t offset, int d, bool leaf)
                : node_offset(offset)
                , depth(d)
                , is_leaf(leaf)
                , is_processing_children(false)
                , remaining_children_mask(0)
                , offset_index(0)
            {
            }
        };

        LOGD(
            "walk_items_at_node - root_offset: ",
            root_offset,
            ", start_depth: ",
            start_depth);

        // Use a fixed-size stack (max tree depth is 64)
        StackEntry stack[64];
        int stack_top = 0;

        // Push root
        // TODO: should be able to read the inner node here to determine the
        // depth if this function should support also walking a leaf node, that
        // would have to be passed in as an argument to the function, because
        // it's not possible to determine a leaf from an inner node.
        stack[stack_top++] = StackEntry(root_offset, start_depth, false);

        size_t items_visited = 0;
        size_t iterations = 0;
        const size_t MAX_ITERATIONS = 100'000'000;  // Safety limit

        while (stack_top > 0 && iterations < MAX_ITERATIONS)
        {
            iterations++;

            // Peek at top of stack (don't pop yet)
            auto& entry = stack[stack_top - 1];

            LOGD(
                "Iteration ",
                iterations,
                " - Stack[",
                stack_top - 1,
                "]: offset=",
                entry.node_offset,
                ", depth=",
                entry.depth,
                ", is_leaf=",
                entry.is_leaf,
                ", is_processing=",
                entry.is_processing_children);

            if (entry.is_leaf)
            {
                // Process leaf node - no marker, starts directly with
                // LeafHeader
                if (entry.node_offset + sizeof(LeafHeader) > file_size_)
                {
                    LOGE(
                        "Leaf header at offset ",
                        entry.node_offset,
                        " exceeds file size");
                    throw std::runtime_error("Leaf header exceeds file bounds");
                }

                auto leaf_header = reinterpret_cast<const LeafHeader*>(
                    data_ + entry.node_offset);
                Key leaf_key(leaf_header->key.data());

                LOGD(
                    "Processing leaf at offset ",
                    entry.node_offset,
                    " with key: ",
                    leaf_key.hex());

                // Get leaf data
                size_t data_offset = entry.node_offset + sizeof(LeafHeader);
                size_t data_size = leaf_header->data_size();

                if (data_offset + data_size > file_size_)
                {
                    LOGE(
                        "Leaf data at offset ",
                        data_offset,
                        " with size ",
                        data_size,
                        " exceeds file size");
                    throw std::runtime_error("Leaf data exceeds file bounds");
                }

                Slice leaf_data(data_ + data_offset, data_size);

                // Call the callback
                items_visited++;
                if (!callback(leaf_key, leaf_data))
                {
                    LOGD("Callback requested early termination");
                    break;
                }

                // Pop the leaf from stack
                stack_top--;
            }
            else
            {
                // Process inner node
                if (!entry.is_processing_children)
                {
                    // First time visiting this inner node
                    if (entry.node_offset + sizeof(InnerNodeHeader) >
                        file_size_)
                    {
                        LOGE(
                            "Inner node header at offset ",
                            entry.node_offset,
                            " exceeds file size");
                        throw std::runtime_error(
                            "Inner node header exceeds file bounds");
                    }

                    auto inner_header =
                        reinterpret_cast<const InnerNodeHeader*>(
                            data_ + entry.node_offset);
                    LOGD(
                        "Processing inner node at offset ",
                        entry.node_offset,
                        ", depth=",
                        entry.depth,
                        ", header depth=",
                        (int)inner_header->bits.depth);

                    if (inner_header->bits.depth != entry.depth)
                    {
                        LOGW(
                            "Depth mismatch: expected ",
                            entry.depth,
                            " but header says ",
                            (int)inner_header->bits.depth);
                    }

                    // Show all children for debugging
                    LOGD("Inner node at depth ", entry.depth, " has children:");
                    for (int i = 0; i < 16; ++i)
                    {
                        ChildType ct = inner_header->get_child_type(i);
                        if (ct != ChildType::EMPTY)
                        {
                            LOGD(
                                "  Branch ",
                                i,
                                ": ",
                                ct == ChildType::INNER
                                    ? "INNER"
                                    : ct == ChildType::LEAF ? "LEAF"
                                                            : "UNKNOWN");
                        }
                    }

                    int child_count = inner_header->count_children();
                    LOGD("Total non-empty children: ", child_count);

                    if (child_count == 0)
                    {
                        LOGW(
                            "Inner node with no children at offset ",
                            entry.node_offset);
                        stack_top--;  // Pop this node
                        continue;
                    }

                    // Build mask of non-empty children
                    entry.remaining_children_mask = 0;
                    for (int i = 0; i < 16; ++i)
                    {
                        if (inner_header->get_child_type(i) != ChildType::EMPTY)
                        {
                            entry.remaining_children_mask |= (1u << i);
                        }
                    }
                    entry.offset_index = 0;
                    entry.is_processing_children = true;
                }

                // Process next child
                if (entry.remaining_children_mask != 0)
                {
                    // Find next child (least significant bit)
                    int branch = __builtin_ctz(entry.remaining_children_mask);

                    // Get inner header again
                    auto inner_header =
                        reinterpret_cast<const InnerNodeHeader*>(
                            data_ + entry.node_offset);
                    ChildType child_type = inner_header->get_child_type(branch);

                    // Get offset array
                    size_t offsets_start =
                        entry.node_offset + sizeof(InnerNodeHeader);
                    const uint64_t* offsets = reinterpret_cast<const uint64_t*>(
                        data_ + offsets_start);
                    uint64_t child_offset = offsets[entry.offset_index];

                    bool child_is_leaf = (child_type == ChildType::LEAF);
                    LOGD(
                        "Processing child: branch=",
                        branch,
                        ", type=",
                        child_is_leaf ? "LEAF" : "INNER",
                        ", offset=",
                        child_offset);

                    // Basic sanity check on offset
                    if (child_offset < sizeof(CatlV2Header) ||
                        child_offset >= file_size_)
                    {
                        LOGE(
                            "Invalid child offset: ",
                            child_offset,
                            " (file size: ",
                            file_size_,
                            ")");
                        throw std::runtime_error("Invalid child offset");
                    }

                    // Clear this bit from mask
                    entry.remaining_children_mask &= ~(1u << branch);
                    ++entry.offset_index;

                    // Push child onto stack
                    if (stack_top >= 64)
                    {
                        throw std::runtime_error(
                            "Stack overflow - tree depth exceeds 64");
                    }

                    LOGD(
                        "Pushing child: offset=",
                        child_offset,
                        ", depth=",
                        entry.depth + 1,
                        ", is_leaf=",
                        child_is_leaf);
                    stack[stack_top++] = StackEntry(
                        child_offset, entry.depth + 1, child_is_leaf);
                }
                else
                {
                    // No more children, pop this inner node
                    LOGD(
                        "Inner node at offset ",
                        entry.node_offset,
                        " has no more children, popping");
                    stack_top--;
                }
            }
        }

        if (iterations >= MAX_ITERATIONS)
        {
            LOGE(
                "Walk aborted after ",
                MAX_ITERATIONS,
                " iterations - possible infinite loop");
            throw std::runtime_error("Walk iteration limit exceeded");
        }

        LOGD(
            "Walk complete - visited ",
            items_visited,
            " items in ",
            iterations,
            " iterations");
        return items_visited;
    }
};

}  // namespace catl::v2