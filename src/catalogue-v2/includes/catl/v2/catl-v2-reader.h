#pragma once

#include "catl/common/ledger-info.h"
#include "catl/core/bit-utils.h"
#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/v2/catl-v2-ledger-index-view.h"
#include "catl/v2/catl-v2-memtree.h"
#include "catl/v2/catl-v2-structs.h"

#include <atomic>
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <chrono>
#include <cstring>
#include <errno.h>  // TODO: use cerrno
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#if defined(__APPLE__) || defined(__linux__)
#include <sys/mman.h>
#endif

// #include "catl/hasher-v1/ledger.h"

namespace catl::v2 {

struct MmapHolder
{
    boost::iostreams::mapped_file_source mmap_file;
    std::string filename;
};

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
    /**
     * Private constructor that takes raw memory and ownership
     * Used by create() and share() methods
     */
    CatlV2Reader(
        const uint8_t* data,
        size_t size,
        std::shared_ptr<MmapHolder> holder = nullptr)
        : data_(data), file_size_(size), mmap_holder_(holder)
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

            // Create reader with the holder for lifetime management
            // Can't use make_shared due to private constructor
            return std::shared_ptr<CatlV2Reader>(
                new CatlV2Reader(data, size, holder));
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
     * The memory mapping stays alive as long as any reader references it.
     *
     * @return New reader instance sharing same memory
     */
    std::shared_ptr<CatlV2Reader>
    share() const
    {
        if (!mmap_holder_)
        {
            // Fallback for readers created without mmap holder (e.g., from raw
            // memory)
            return std::shared_ptr<CatlV2Reader>(
                new CatlV2Reader(data_, file_size_, nullptr));
        }
        // Share the mmap holder to keep memory alive
        return std::shared_ptr<CatlV2Reader>(
            new CatlV2Reader(data_, file_size_, mmap_holder_));
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

        // TODO: this is not using load_pod
        // Should this just use MemPtr ?
        auto info = reinterpret_cast<const catl::common::LedgerInfo*>(
            data_ + current_pos_);
        current_pos_ += sizeof(catl::common::LedgerInfo);

        // Read the trees header that follows
        current_trees_header_ =
            load_pod<TreesHeader>(data_, current_pos_, file_size_);
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
     * Load a POD type from the current position
     * Uses the safe/unsafe loading based on CATL_UNSAFE_POD_LOADS flag
     */
    template <typename T>
    T
    load_pod_at_current() const
    {
        return load_pod<T>(data_, current_pos_, file_size_);
    }

    /**
     * Get the file size for bounds checking
     */
    size_t
    file_size() const
    {
        return file_size_;
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
        const uint8_t* tree_ptr = data_ + current_pos_;
        LOGD(
            "State tree lookup - tree ptr: ",
            static_cast<const void*>(tree_ptr),
            ", current_pos: ",
            current_pos_,
            ", state_tree_size: ",
            current_trees_header_.state_tree_size);
        return lookup_key_at_node(key, tree_ptr);
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
        const uint8_t* tree_ptr =
            data_ + current_pos_ + current_trees_header_.state_tree_size;
        LOGD(
            "Tx tree lookup - tree ptr: ",
            static_cast<const void*>(tree_ptr),
            ", current_pos: ",
            current_pos_,
            ", state_tree_size: ",
            current_trees_header_.state_tree_size,
            ", tx_tree_size: ",
            current_trees_header_.tx_tree_size);
        return lookup_key_at_node(key, tree_ptr);
    }

    std::shared_ptr<MmapHolder>
    mmap_holder()
    {
        return mmap_holder_;
    }

    /**
     * Options for controlling tree traversal behavior
     */
    struct WalkOptions
    {
        bool parallel;       // Use parallel processing with thread pool
        bool prefetch;       // Do prefetch pass before parallel (experimental)
        size_t num_threads;  // Number of worker threads for parallel mode

        // Default constructor - sequential mode
        WalkOptions() : parallel(false), prefetch(false), num_threads(8)
        {
        }

        // Constructor for custom settings
        WalkOptions(bool p, bool pf, size_t nt)
            : parallel(p), prefetch(pf), num_threads(nt)
        {
        }

        // Convenience constructors
        static WalkOptions
        sequential()
        {
            return WalkOptions();
        }
        static WalkOptions
        parallel_only()
        {
            return WalkOptions(true, false, 8);
        }
        static WalkOptions
        parallel_with_prefetch()
        {
            return WalkOptions(true, true, 8);
        }
    };

    /**
     * Walk all items in the current state tree
     *
     * Performs a depth-first traversal of the tree, calling the callback
     * for each leaf node with the key and data.
     *
     * @param callback Function called for each item: (Key, Slice) -> bool
     *                 Return false to stop iteration
     * @param options Controls traversal behavior (parallel, prefetch, etc)
     * @return Number of items visited
     */
    template <typename Callback>
    size_t
    walk_state_items(Callback&& callback, const WalkOptions& options = {})
    {
        const uint8_t* tree_ptr = data_ + current_pos_;
        LOGD(
            "walk_state_items - tree_ptr: ",
            static_cast<const void*>(tree_ptr),
            ", current_pos: ",
            current_pos_,
            ", state_tree_size: ",
            current_trees_header_.state_tree_size,
            ", parallel: ",
            options.parallel,
            ", prefetch: ",
            options.prefetch,
            ", num_threads: ",
            options.num_threads);

        if (!options.parallel)
        {
            return walk_items_at_node(
                tree_ptr, std::forward<Callback>(callback));
        }
        else
        {
            return walk_items_parallel(
                tree_ptr, std::forward<Callback>(callback), options);
        }
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
        const uint8_t* tree_ptr =
            data_ + current_pos_ + current_trees_header_.state_tree_size;
        LOGD(
            "walk_tx_items - tree_ptr: ",
            static_cast<const void*>(tree_ptr),
            ", tx_tree_size: ",
            current_trees_header_.tx_tree_size);
        return walk_items_at_node(tree_ptr, std::forward<Callback>(callback));
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
    abs_off_t current_pos_ = 0;  // Current position in file (absolute offset)
    std::shared_ptr<MmapHolder> mmap_holder_;  // Keeps mmap alive

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

        // Validate version (experimental - only version 1 supported)
        if (header_.version != 1)
        {
            throw std::runtime_error(
                "Unsupported file version: " + std::to_string(header_.version) +
                " (experimental code only supports version 1)");
        }

        // Check endianness compatibility
        std::uint32_t host_endian = get_host_endianness();
        if (header_.endianness != host_endian)
        {
            const char* file_endian = (header_.endianness == 0x01020304)
                ? "big-endian"
                : "little-endian";
            const char* host_type =
                (host_endian == 0x01020304) ? "big-endian" : "little-endian";
            throw std::runtime_error(
                std::string("Endianness mismatch: file is ") + file_endian +
                ", but host is " + host_type +
                ". Cannot mmap files created on different endian systems.");
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
     * Lookup a key in the tree using MemTreeOps
     *
     * @param key The key to search for
     * @param root_ptr Pointer to the root node
     * @return Optional Slice containing the leaf data
     */
    [[nodiscard]] std::optional<Slice>
    lookup_key_at_node(const Key& key, const uint8_t* root_ptr) const
    {
        LOGD("=== Starting key lookup ===");
        LOGD("Target key: ", key.hex());
        LOGD("Root ptr: ", static_cast<const void*>(root_ptr));

        try
        {
            // Use MemTreeOps to do the lookup
            auto root_view = MemTreeOps::get_inner_node(root_ptr);
            auto leaf = MemTreeOps::lookup_key(root_view, key);

            LOGD("=== Key lookup successful! ===");
            LOGD("Found key! Data size: ", leaf.data.size(), " bytes");

            // Return the leaf data as a Slice
            return Slice(leaf.data.data(), leaf.data.size());
        }
        catch (const std::runtime_error& e)
        {
            LOGW("=== Key lookup failed ===");
            LOGW("Error: ", e.what());
            return std::nullopt;  // Key not found
        }
    }

    /**
     * Walk all items in a tree using MemTreeOps
     *
     * @param root_ptr Pointer to the root node
     * @param callback Function to call for each leaf: (Key, Slice) -> bool
     * @return Number of items visited
     */
    template <typename Callback>
    size_t
    walk_items_at_node(const uint8_t* root_ptr, Callback&& callback)
    {
        LOGD(
            "walk_items_at_node - root_ptr: ",
            static_cast<const void*>(root_ptr));

        // Use MemTreeOps to walk the leaves
        size_t items_visited = MemTreeOps::walk_leaves_from_ptr(
            root_ptr, std::forward<Callback>(callback));

        LOGD("Walk complete - visited ", items_visited, " items");
        return items_visited;
    }

    /**
     * Walk items in parallel using a thread pool
     *
     * @param root_ptr Pointer to the root node
     * @param callback Function to call for each leaf: (Key, Slice) -> bool
     * @param options Walk options including thread count and prefetch settings
     * @return Number of items visited
     */
    template <typename Callback>
    size_t
    walk_items_parallel(
        const uint8_t* root_ptr,
        Callback&& callback,
        const WalkOptions& options)
    {
        const size_t NUM_THREADS = options.num_threads;

        std::stringstream ss;
        ss << std::this_thread::get_id();
        LOGI(
            "walk_items_parallel START - root_ptr: ",
            static_cast<const void*>(root_ptr),
            ", main thread: ",
            ss.str(),
            ", using ",
            NUM_THREADS,
            " threads");

        // First, read the root node to get its children
        MemPtr<InnerNodeHeader> root_header_ptr(root_ptr);
        const auto& root_header = root_header_ptr.get_uncopyable();

        LOGI(
            "Root node depth: ",
            static_cast<int>(root_header.get_depth()),
            ", child count: ",
            root_header.count_children());

        // Collect all direct children
        struct ChildInfo
        {
            const uint8_t* ptr;  // Direct memory pointer
            int branch;
            bool is_leaf;
        };
        std::vector<ChildInfo> children;

        const std::uint8_t* rel_base = root_ptr + sizeof(InnerNodeHeader);
        int offset_index = 0;

        for (int branch = 0; branch < 16; ++branch)
        {
            ChildType child_type = root_header.get_child_type(branch);
            if (child_type != ChildType::EMPTY)
            {
                ChildInfo info;
                // Resolve self-relative offset to get child pointer
                const uint8_t* child_ptr =
                    resolve_self_relative(rel_base, offset_index);
                ++offset_index;

                info.ptr = child_ptr;
                info.branch = branch;
                info.is_leaf = (child_type == ChildType::LEAF);
                children.push_back(info);

                LOGD(
                    "Root child[",
                    branch,
                    "]: ptr=",
                    static_cast<const void*>(info.ptr),
                    ", type=",
                    info.is_leaf ? "LEAF" : "INNER");
            }
        }

        // Thread-safe callback wrapper and item counter
        std::mutex callback_mutex;
        std::atomic<size_t> total_items{0};
        std::atomic<bool> should_stop{false};

        auto thread_safe_callback = [&](const Key& key,
                                        const Slice& data) -> bool {
            if (should_stop.load())
                return false;

            // std::lock_guard<std::mutex> lock(callback_mutex);
            bool continue_walking = callback(key, data);
            if (!continue_walking)
            {
                should_stop.store(true);
            }
            return continue_walking;
        };

        // Create work queue for children to process
        std::mutex work_mutex;
        size_t next_child_idx = 0;

        // Worker function that processes 2 children at a time
        auto worker = [&]() {
            std::stringstream tid;
            tid << std::this_thread::get_id();
            LOGI("Worker thread ", tid.str(), " started");

            while (true)
            {
                // Get next 2 children to process
                std::vector<ChildInfo> my_children;
                {
                    std::lock_guard<std::mutex> lock(work_mutex);
                    if (next_child_idx >= children.size())
                        break;

                    // Take up to 2 children
                    size_t end = std::min(next_child_idx + 2, children.size());
                    for (size_t i = next_child_idx; i < end; ++i)
                    {
                        my_children.push_back(children[i]);
                    }
                    next_child_idx = end;

                    LOGI(
                        "Thread ",
                        tid.str(),
                        " took children ",
                        next_child_idx - my_children.size(),
                        " to ",
                        next_child_idx - 1);
                }

                // Process the children we took
                for (const auto& child : my_children)
                {
                    if (child.is_leaf)
                    {
                        // Process leaf directly
                        LOGI(
                            "Thread ",
                            tid.str(),
                            " processing leaf child[",
                            child.branch,
                            "]");

                        MemPtr<LeafHeader> leaf_ptr(child.ptr);
                        const auto& leaf_header = leaf_ptr.get_uncopyable();
                        Key leaf_key(leaf_header.key.data());

                        const uint8_t* data_ptr =
                            child.ptr + sizeof(LeafHeader);
                        size_t data_size = leaf_header.data_size();

                        Slice leaf_data(data_ptr, data_size);

                        if (thread_safe_callback(leaf_key, leaf_data))
                        {
                            total_items.fetch_add(1);
                        }
                    }
                    else
                    {
                        // Process inner node subtree
                        LOGI(
                            "Thread ",
                            tid.str(),
                            " processing inner child[",
                            child.branch,
                            "]");

                        try
                        {
                            size_t items = walk_items_at_node(
                                child.ptr, thread_safe_callback);
                            total_items.fetch_add(items);

                            LOGI(
                                "Thread ",
                                tid.str(),
                                " completed child[",
                                child.branch,
                                "], items: ",
                                items);
                        }
                        catch (const std::exception& e)
                        {
                            LOGE(
                                "Thread error in child[",
                                child.branch,
                                "]: ",
                                e.what());
                        }
                    }
                }
            }

            LOGI("Worker thread ", tid.str(), " finished");
        };

        // Create thread vector
        std::vector<std::thread> threads;
        threads.reserve(NUM_THREADS);

        // If prefetching, run the prefetch pass first and wait for completion
        if (options.prefetch)
        {
            LOGI("Starting prefetch pass");
            auto start_time = std::chrono::high_resolution_clock::now();

            size_t prefetched = 0;

            // First, madvise the entire tree region!
            size_t tree_size = current_trees_header_.state_tree_size;

            LOGI(
                "Calling madvise(MADV_WILLNEED) on ",
                tree_size,
                " bytes at ptr ",
                static_cast<const void*>(root_ptr));

            // We don't actually know if madvise is too expensive!
            // #if defined(__APPLE__) || defined(__linux__)
            // if (madvise(const_cast<uint8_t*>(tree_start), tree_size,
            // MADV_WILLNEED) != 0)
            // {
            //     LOGW("madvise(MADV_WILLNEED) failed: ", strerror(errno));
            // }
            // else
            // {
            //     LOGI("madvise(MADV_WILLNEED) succeeded!");
            // }
            //
            // // Also try MADV_SEQUENTIAL for good measure
            // if (madvise(const_cast<uint8_t*>(tree_start), tree_size,
            // MADV_SEQUENTIAL) != 0)
            // {
            //     LOGW("madvise(MADV_SEQUENTIAL) failed: ", strerror(errno));
            // }
            // #endif

            // Now walk and touch pages to ensure they're actually loaded
            walk_items_at_node(
                root_ptr, [&prefetched](const Key& key, const Slice& data) {
                    (void)key;
                    (void)data;
                    // Just touch first byte of key and data to trigger page
                    // faults volatile uint8_t k = key.data()[0]; volatile
                    // uint8_t d = data.data()[0]; (void)k; (void)d;

                    // NO PER-ITEM MADVISE - it's too expensive!

                    prefetched++;
                    // if (prefetched % 10000 == 0) {
                    //     LOGI("Prefetched ", prefetched, " items");
                    // }
                    //
                    return true;
                });

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    end_time - start_time);

            LOGW(
                "Prefetch complete: ",
                prefetched,
                " items in ",
                duration.count(),
                "ms");
            LOGW("Pausing for 2 seconds to let you see the timing...");

            std::this_thread::sleep_for(std::chrono::seconds(2));

            LOGW("Resume! Now starting parallel processing with warm cache...");
        }

        // Start worker threads

        for (size_t i = 0; i < NUM_THREADS; ++i)
        {
            threads.emplace_back(worker);
        }

        // Wait for all threads to complete
        for (auto& thread : threads)
        {
            thread.join();
        }

        size_t final_count = total_items.load();
        LOGD("Parallel walk complete - total items: ", final_count);
        return final_count;
    }
};
}  // namespace catl::v2