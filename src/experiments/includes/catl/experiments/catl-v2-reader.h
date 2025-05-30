#pragma once

#include "catl/common/ledger-info.h"
#include "catl/core/log-macros.h"
#include "catl/core/types.h"
#include "catl/experiments/catl-v2-ledger-index-view.h"
#include "catl/experiments/catl-v2-structs.h"
#include "catl/experiments/shamap-custom-traits.h"

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/mapped_file.hpp>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace catl::experiments {

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
};

}  // namespace catl::experiments