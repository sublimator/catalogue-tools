#pragma once

#include "catl/core/types.h"
#include "catl/utils/slicer/arg-options.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-writer.h"
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace catl::utils::slicer {

/**
 * Statistics collected during the slicing operation
 */
struct SliceStats {
    uint32_t start_ledger = 0;
    uint32_t end_ledger = 0;
    size_t bytes_processed = 0;
    size_t bytes_written = 0;
    size_t state_items_processed = 0;
    bool start_snapshot_used = false;
    bool end_snapshot_created = false;
    double elapsed_seconds = 0.0;
};

/**
 * In-memory representation of the state map
 */
class InMemoryStateMap {
private:
    std::map<Key, std::vector<uint8_t>> items_;

public:
    /**
     * Add or update an item in the map
     * 
     * @param key The key of the item
     * @param data Pointer to the data
     * @param size Size of the data in bytes
     */
    void set_item(const Key& key, const uint8_t* data, uint32_t size);

    /**
     * Remove an item from the map
     * 
     * @param key The key of the item to remove
     * @return true if the item was found and removed, false otherwise
     */
    bool remove_item(const Key& key);

    /**
     * Get the number of items in the map
     * 
     * @return Item count
     */
    size_t size() const;

    /**
     * Serialize the state map to a stream using the CATL v1 format
     * 
     * @param writer The writer to use for serialization
     * @return Number of items written
     */
    size_t serialize(catl::v1::Writer& writer) const;

    /**
     * Access to the internal map for iteration
     * 
     * @return Reference to the internal map
     */
    const std::map<Key, std::vector<uint8_t>>& items() const;
};

/**
 * Main class for the CATL slicing operation
 */
class Slicer {
private:
    CommandLineOptions options_;
    std::unique_ptr<catl::v1::Reader> reader_;
    std::unique_ptr<catl::v1::Writer> writer_;
    std::unique_ptr<InMemoryStateMap> state_map_;
    SliceStats stats_;
    std::string snapshots_path_;

    /**
     * Initialize the input reader and output writer
     */
    void initialize();

    /**
     * Process the first ledger of the slice
     * 
     * @return true if successful, false otherwise
     */
    bool process_first_ledger();

    /**
     * Attempt to use a snapshot for the first ledger
     * 
     * @return true if a snapshot was successfully used, false otherwise
     */
    bool try_use_start_snapshot();

    /**
     * Fast-forward through input ledgers to build state for the first ledger
     * 
     * @return true if successful, false otherwise
     */
    bool fast_forward_to_start();

    /**
     * Process subsequent ledgers in the slice
     * 
     * @return true if successful, false otherwise
     */
    bool process_subsequent_ledgers();

    /**
     * Create a snapshot for the next slice if enabled
     * 
     * @return true if successful, false otherwise
     */
    bool create_next_slice_snapshot();

    /**
     * Validate ledger ranges against the input file header
     * 
     * @return true if valid, false otherwise
     */
    bool validate_ledger_ranges();

    /**
     * Get the expected snapshot filename for a given ledger
     * 
     * @param ledger_seq Ledger sequence number
     * @return Full path to the expected snapshot file
     */
    std::string get_snapshot_filename(uint32_t ledger_seq) const;

public:
    /**
     * Constructor
     * 
     * @param options Command line options
     */
    explicit Slicer(const CommandLineOptions& options);

    /**
     * Run the slicing operation
     * 
     * @return true if successful, false otherwise
     */
    bool run();

    /**
     * Get statistics about the slicing operation
     * 
     * @return Statistics structure
     */
    const SliceStats& stats() const;
};

} // namespace catl::utils::slicer