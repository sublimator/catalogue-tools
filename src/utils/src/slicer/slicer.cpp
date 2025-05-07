#include "catl/utils/slicer/slicer.h"
#include "catl/core/log-macros.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/v1/catl-v1-structs.h"
#include "catl/v1/catl-v1-utils.h"
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>

namespace fs = std::filesystem;
namespace io = boost::iostreams;

namespace catl::utils::slicer {

// Implementation of InMemoryStateMap methods
void
InMemoryStateMap::set_item(const Key& key, const uint8_t* data, uint32_t size)
{
    std::vector<uint8_t> value_data(data, data + size);
    items_[key] = std::move(value_data);
}

bool
InMemoryStateMap::remove_item(const Key& key)
{
    auto it = items_.find(key);
    if (it != items_.end())
    {
        items_.erase(it);
        return true;
    }
    return false;
}

size_t
InMemoryStateMap::size() const
{
    return items_.size();
}

size_t
InMemoryStateMap::serialize(catl::v1::Writer& writer) const
{
    size_t item_count = 0;

    for (const auto& [key, data] : items_)
    {
        // Write each item as: tnACCOUNT_STATE + key + data
        uint8_t type = SHAMapNodeType::tnACCOUNT_STATE;
        writer.write_raw_data(&type, sizeof(type));
        writer.write_raw_data(key.data(), Key::size());

        // Write data length as uint32_t
        uint32_t data_size = static_cast<uint32_t>(data.size());
        writer.write_raw_data(
            reinterpret_cast<const uint8_t*>(&data_size), sizeof(data_size));

        // Write actual data
        writer.write_raw_data(data.data(), data_size);

        item_count++;
    }

    // Write terminal marker
    uint8_t terminal = SHAMapNodeType::tnTERMINAL;
    writer.write_raw_data(&terminal, sizeof(terminal));

    return item_count;
}

const std::map<Key, std::vector<uint8_t>>&
InMemoryStateMap::items() const
{
    return items_;
}

// Implementation of Slicer methods
Slicer::Slicer(const CommandLineOptions& options)
    : options_(options), state_map_(nullptr)
{
    // Ensure snapshots path is set
    if (options_.snapshots_path)
    {
        snapshots_path_ = *options_.snapshots_path;
    }
    else
    {
        fs::path output_dir =
            fs::path(options_.output_file.value()).parent_path();
        snapshots_path_ = (output_dir / "catl_snapshots").string();
    }

    // Initialize statistics
    stats_.start_ledger = options_.start_ledger.value_or(0);
    stats_.end_ledger = options_.end_ledger.value_or(0);
}

bool
Slicer::run()
{
    auto start_time = std::chrono::high_resolution_clock::now();

    try
    {
        // Initialize reader and writer
        initialize();

        // Validate ledger ranges
        if (!validate_ledger_ranges())
        {
            return false;
        }

        // Create snapshots directory if needed
        if (options_.create_next_slice_state_snapshot ||
            options_.use_start_snapshot)
        {
            if (!fs::exists(snapshots_path_))
            {
                fs::create_directories(snapshots_path_);
                LOGI("Created snapshots directory:", snapshots_path_);
            }
        }

        // Process the first ledger of the slice
        if (!process_first_ledger())
        {
            return false;
        }

        // Process the rest of the ledgers in the slice
        if (!process_subsequent_ledgers())
        {
            return false;
        }

        // Create snapshot for next slice if requested
        if (options_.create_next_slice_state_snapshot)
        {
            if (!create_next_slice_snapshot())
            {
                LOGW(
                    "Failed to create snapshot for next slice, but slice "
                    "creation was successful");
            }
        }

        // Finalize the output file
        writer_->finalize();

        // Calculate elapsed time
        auto end_time = std::chrono::high_resolution_clock::now();
        stats_.elapsed_seconds =
            std::chrono::duration<double>(end_time - start_time).count();

        // Print stats
        LOGI("Slice creation complete:");
        LOGI("  Ledger range:", stats_.start_ledger, "-", stats_.end_ledger);
        LOGI("  Bytes processed:", stats_.bytes_processed);
        LOGI("  Bytes written:", stats_.bytes_written);
        LOGI("  State items processed:", stats_.state_items_processed);
        LOGI(
            "  Start snapshot used:",
            stats_.start_snapshot_used ? "Yes" : "No");
        LOGI(
            "  End snapshot created:",
            stats_.end_snapshot_created ? "Yes" : "No");
        LOGI("  Time taken:", stats_.elapsed_seconds, "seconds");

        return true;
    }
    catch (const std::exception& e)
    {
        LOGE("Error during slicing:", e.what());
        return false;
    }
}

void
Slicer::initialize()
{
    // Create reader for input file
    if (!options_.input_file)
    {
        throw std::runtime_error("Input file not specified");
    }
    reader_ = std::make_unique<catl::v1::Reader>(*options_.input_file);

    // Create writer for output file
    if (!options_.output_file)
    {
        throw std::runtime_error("Output file not specified");
    }

    // Check if output file exists and handle accordingly
    if (fs::exists(*options_.output_file) && !options_.force_overwrite)
    {
        std::string answer;
        std::cout << "Output file '" << *options_.output_file
                  << "' already exists. Overwrite? [y/N]: ";
        std::getline(std::cin, answer);
        if (answer != "y" && answer != "Y")
        {
            throw std::runtime_error("Operation cancelled by user");
        }
    }

    // Create writer with desired compression level
    catl::v1::WriterOptions writer_options;
    writer_options.compression_level = options_.compression_level;
    writer_options.network_id = reader_->header().network_id;

    writer_ = catl::v1::Writer::for_file(*options_.output_file, writer_options);
}

bool
Slicer::validate_ledger_ranges()
{
    const auto& header = reader_->header();

    if (!options_.start_ledger || !options_.end_ledger)
    {
        LOGE("Start and end ledger must be specified");
        return false;
    }

    uint32_t start = options_.start_ledger.value();
    uint32_t end = options_.end_ledger.value();

    if (start < header.min_ledger || end > header.max_ledger)
    {
        LOGE(
            "Requested ledger range [",
            start,
            "-",
            end,
            "] is outside the input file's range [",
            header.min_ledger,
            "-",
            header.max_ledger,
            "]");
        return false;
    }

    if (start > end)
    {
        LOGE(
            "Start ledger (",
            start,
            ") must be less than or equal to end ledger (",
            end,
            ")");
        return false;
    }

    // Write header for output slice with adjusted min/max ledger
    writer_->write_header(start, end);

    return true;
}

bool
Slicer::process_first_ledger()
{
    uint32_t start_ledger = options_.start_ledger.value();

    LOGI("Processing first ledger of slice (", start_ledger, ")");

    // Try to use a snapshot if enabled
    bool use_snapshot_success = false;
    if (options_.use_start_snapshot)
    {
        use_snapshot_success = try_use_start_snapshot();
        stats_.start_snapshot_used = use_snapshot_success;
    }

    // If snapshot wasn't used, fast-forward through input
    if (!use_snapshot_success)
    {
        LOGI(
            "No suitable snapshot found or snapshot usage disabled, "
            "fast-forwarding through input");
        if (!fast_forward_to_start())
        {
            LOGE("Failed to fast-forward to start ledger");
            return false;
        }
    }

    return true;
}

bool
Slicer::try_use_start_snapshot()
{
    uint32_t start_ledger = options_.start_ledger.value();
    std::string snapshot_file = get_snapshot_filename(start_ledger);

    if (!fs::exists(snapshot_file))
    {
        LOGI("No snapshot found for ledger", start_ledger);
        return false;
    }

    try
    {
        LOGI("Found snapshot for ledger", start_ledger, ":", snapshot_file);

        // Read LedgerInfo for start ledger from input
        bool found_start_ledger = false;
        catl::v1::LedgerInfo ledger_info;

        // Skip through ledgers until we find our start ledger
        while (!found_start_ledger)
        {
            try
            {
                ledger_info = reader_->read_ledger_info();
                if (ledger_info.sequence == start_ledger)
                {
                    found_start_ledger = true;
                    LOGD("Found start ledger header in input file");
                }
            }
            catch (const std::exception& e)
            {
                LOGE(
                    "Failed to read ledger info while looking for start "
                    "ledger:",
                    e.what());
                return false;
            }
        }

        // Write ledger header to output
        writer_->write_ledger_header(ledger_info);

        // Open the snapshot file
        std::ifstream snapshot_stream(snapshot_file, std::ios::binary);
        if (!snapshot_stream)
        {
            LOGE("Failed to open snapshot file:", snapshot_file);
            return false;
        }

        // Set up decompression if needed
        io::filtering_istream decomp_stream;
        decomp_stream.push(io::zlib_decompressor());
        decomp_stream.push(snapshot_stream);

        // Create in-memory state map if we need to save the next slice's
        // snapshot
        if (options_.create_next_slice_state_snapshot)
        {
            state_map_ = std::make_unique<InMemoryStateMap>();
        }

        // Read and copy the decompressed stream content to output
        // If state_map_ is not null, also parse items to populate it
        const size_t buffer_size = 8192;
        std::vector<uint8_t> buffer(buffer_size);
        size_t total_bytes = 0;

        while (decomp_stream)
        {
            decomp_stream.read(
                reinterpret_cast<char*>(buffer.data()), buffer_size);
            size_t bytes_read = decomp_stream.gcount();
            if (bytes_read > 0)
            {
                // Write the data to the output file
                writer_->write_raw_data(buffer.data(), bytes_read);
                total_bytes += bytes_read;

                // If we need the state map, we must also parse the content
                if (state_map_)
                {
                    // This is simplistic - in reality, we would need to keep
                    // track of partial nodes and accumulate data across buffer
                    // boundaries. This is just to illustrate the concept.
                    LOGW(
                        "Parsing snapshot stream to populate state map not "
                        "fully implemented");
                    // TODO: Implement proper parsing of the snapshot data to
                    // populate state_map_
                }
            }
        }

        LOGI("Copied", total_bytes, "bytes from snapshot to output");
        stats_.bytes_written += total_bytes;

        // Now we need to find and copy the transaction map for this ledger
        // First, we need to skip past the account state map in the input file
        bool found_terminal = false;
        uint8_t node_type;

        while (!found_terminal)
        {
            if (reader_->read_raw_data(&node_type, 1) != 1)
            {
                LOGE(
                    "Unexpected end of file while looking for terminal marker");
                return false;
            }

            if (node_type == SHAMapNodeType::tnTERMINAL)
            {
                found_terminal = true;
                LOGD("Found state map terminal marker in input file");
            }
            else
            {
                // Skip this node - read key, length, and data
                uint8_t key_data[32];
                if (reader_->read_raw_data(key_data, 32) != 32)
                {
                    LOGE("Unexpected end of file while reading key");
                    return false;
                }

                uint32_t data_length;
                if (reader_->read_raw_data(
                        reinterpret_cast<uint8_t*>(&data_length), 4) != 4)
                {
                    LOGE("Unexpected end of file while reading data length");
                    return false;
                }

                // Skip the data
                std::vector<uint8_t> skip_buffer(data_length);
                if (reader_->read_raw_data(skip_buffer.data(), data_length) !=
                    data_length)
                {
                    LOGE("Unexpected end of file while skipping data");
                    return false;
                }

                stats_.state_items_processed++;
            }
        }

        // Now copy the transaction map from input to output
        found_terminal = false;
        total_bytes = 0;

        while (!found_terminal)
        {
            if (reader_->read_raw_data(&node_type, 1) != 1)
            {
                LOGE("Unexpected end of file while copying tx map");
                return false;
            }

            // Write the node type to output
            writer_->write_raw_data(&node_type, 1);
            total_bytes += 1;

            if (node_type == SHAMapNodeType::tnTERMINAL)
            {
                found_terminal = true;
                LOGD("Found tx map terminal marker in input file");
            }
            else if (
                node_type == SHAMapNodeType::tnTRANSACTION_NM ||
                node_type == SHAMapNodeType::tnTRANSACTION_MD)
            {
                // Copy key
                uint8_t key_data[32];
                if (reader_->read_raw_data(key_data, 32) != 32)
                {
                    LOGE("Unexpected end of file while reading tx key");
                    return false;
                }
                writer_->write_raw_data(key_data, 32);
                total_bytes += 32;

                // Copy data length and data
                uint32_t data_length;
                if (reader_->read_raw_data(
                        reinterpret_cast<uint8_t*>(&data_length), 4) != 4)
                {
                    LOGE("Unexpected end of file while reading tx data length");
                    return false;
                }
                writer_->write_raw_data(
                    reinterpret_cast<uint8_t*>(&data_length), 4);
                total_bytes += 4;

                // Copy the data
                std::vector<uint8_t> data_buffer(data_length);
                if (reader_->read_raw_data(data_buffer.data(), data_length) !=
                    data_length)
                {
                    LOGE("Unexpected end of file while reading tx data");
                    return false;
                }
                writer_->write_raw_data(data_buffer.data(), data_length);
                total_bytes += data_length;
            }
            else
            {
                LOGE(
                    "Unexpected node type in tx map:",
                    static_cast<int>(node_type));
                return false;
            }
        }

        LOGI("Copied", total_bytes, "bytes of transaction map to output");
        stats_.bytes_written += total_bytes;

        return true;
    }
    catch (const std::exception& e)
    {
        LOGE("Error using start snapshot:", e.what());
        return false;
    }
}

bool
Slicer::fast_forward_to_start()
{
    uint32_t input_min_ledger = reader_->header().min_ledger;
    uint32_t start_ledger = options_.start_ledger.value();

    LOGI("Fast-forwarding from ledger", input_min_ledger, "to", start_ledger);

    // Create state map to track state
    state_map_ = std::make_unique<InMemoryStateMap>();

    try
    {
        // Process ledgers from input_min_ledger to start_ledger-1
        for (uint32_t current_seq = input_min_ledger;
             current_seq < start_ledger;
             current_seq++)
        {
            LOGI("Fast-forwarding through ledger", current_seq);

            // Read ledger header
            catl::v1::LedgerInfo ledger_info = reader_->read_ledger_info();
            if (ledger_info.sequence != current_seq)
            {
                LOGE(
                    "Expected ledger",
                    current_seq,
                    "but found",
                    ledger_info.sequence);
                return false;
            }

            // Process state map delta
            bool state_map_done = false;
            while (!state_map_done)
            {
                uint8_t node_type;
                if (reader_->read_raw_data(&node_type, 1) != 1)
                {
                    LOGE("Unexpected end of file while reading state map");
                    return false;
                }

                if (node_type == SHAMapNodeType::tnTERMINAL)
                {
                    state_map_done = true;
                }
                else if (node_type == SHAMapNodeType::tnACCOUNT_STATE)
                {
                    // Read key
                    uint8_t key_data[32];
                    if (reader_->read_raw_data(key_data, 32) != 32)
                    {
                        LOGE("Unexpected end of file while reading key");
                        return false;
                    }
                    Key key(key_data);

                    // Read data length and data
                    uint32_t data_length;
                    if (reader_->read_raw_data(
                            reinterpret_cast<uint8_t*>(&data_length), 4) != 4)
                    {
                        LOGE(
                            "Unexpected end of file while reading data length");
                        return false;
                    }

                    std::vector<uint8_t> data(data_length);
                    if (reader_->read_raw_data(data.data(), data_length) !=
                        data_length)
                    {
                        LOGE("Unexpected end of file while reading data");
                        return false;
                    }

                    // Update state map
                    state_map_->set_item(key, data.data(), data_length);
                    stats_.state_items_processed++;
                }
                else if (node_type == SHAMapNodeType::tnREMOVE)
                {
                    // Read key of item to remove
                    uint8_t key_data[32];
                    if (reader_->read_raw_data(key_data, 32) != 32)
                    {
                        LOGE("Unexpected end of file while reading remove key");
                        return false;
                    }
                    Key key(key_data);

                    // Remove from state map
                    state_map_->remove_item(key);
                }
                else
                {
                    LOGE(
                        "Unexpected node type in state map:",
                        static_cast<int>(node_type));
                    return false;
                }
            }

            // Skip transaction map (we don't need it during fast-forward)
            bool tx_map_done = false;
            while (!tx_map_done)
            {
                uint8_t node_type;
                if (reader_->read_raw_data(&node_type, 1) != 1)
                {
                    LOGE("Unexpected end of file while skipping tx map");
                    return false;
                }

                if (node_type == SHAMapNodeType::tnTERMINAL)
                {
                    tx_map_done = true;
                }
                else if (
                    node_type == SHAMapNodeType::tnTRANSACTION_NM ||
                    node_type == SHAMapNodeType::tnTRANSACTION_MD)
                {
                    // Skip key
                    uint8_t key_data[32];
                    if (reader_->read_raw_data(key_data, 32) != 32)
                    {
                        LOGE("Unexpected end of file while skipping tx key");
                        return false;
                    }

                    // Skip data
                    uint32_t data_length;
                    if (reader_->read_raw_data(
                            reinterpret_cast<uint8_t*>(&data_length), 4) != 4)
                    {
                        LOGE(
                            "Unexpected end of file while reading tx data "
                            "length");
                        return false;
                    }

                    std::vector<uint8_t> skip_buffer(data_length);
                    if (reader_->read_raw_data(
                            skip_buffer.data(), data_length) != data_length)
                    {
                        LOGE("Unexpected end of file while skipping tx data");
                        return false;
                    }
                }
            }
        }

        // Now process the start ledger
        LOGI("Processing start ledger", start_ledger);

        // Read ledger header for start ledger
        catl::v1::LedgerInfo ledger_info = reader_->read_ledger_info();
        if (ledger_info.sequence != start_ledger)
        {
            LOGE(
                "Expected ledger",
                start_ledger,
                "but found",
                ledger_info.sequence);
            return false;
        }

        // Write ledger header to output
        writer_->write_ledger_header(ledger_info);

        // Process state map delta for start ledger and apply to state_map_
        bool state_map_done = false;
        while (!state_map_done)
        {
            uint8_t node_type;
            if (reader_->read_raw_data(&node_type, 1) != 1)
            {
                LOGE("Unexpected end of file while reading state map");
                return false;
            }

            if (node_type == SHAMapNodeType::tnTERMINAL)
            {
                state_map_done = true;
            }
            else if (node_type == SHAMapNodeType::tnACCOUNT_STATE)
            {
                // Read key
                uint8_t key_data[32];
                if (reader_->read_raw_data(key_data, 32) != 32)
                {
                    LOGE("Unexpected end of file while reading key");
                    return false;
                }
                Key key(key_data);

                // Read data length and data
                uint32_t data_length;
                if (reader_->read_raw_data(
                        reinterpret_cast<uint8_t*>(&data_length), 4) != 4)
                {
                    LOGE("Unexpected end of file while reading data length");
                    return false;
                }

                std::vector<uint8_t> data(data_length);
                if (reader_->read_raw_data(data.data(), data_length) !=
                    data_length)
                {
                    LOGE("Unexpected end of file while reading data");
                    return false;
                }

                // Update state map
                state_map_->set_item(key, data.data(), data_length);
                stats_.state_items_processed++;
            }
            else if (node_type == SHAMapNodeType::tnREMOVE)
            {
                // Read key of item to remove
                uint8_t key_data[32];
                if (reader_->read_raw_data(key_data, 32) != 32)
                {
                    LOGE("Unexpected end of file while reading remove key");
                    return false;
                }
                Key key(key_data);

                // Remove from state map
                state_map_->remove_item(key);
            }
            else
            {
                LOGE(
                    "Unexpected node type in state map:",
                    static_cast<int>(node_type));
                return false;
            }
        }

        // Now serialize state_map_ to output as a full state map
        LOGI(
            "Writing full state map for ledger",
            start_ledger,
            "with",
            state_map_->size(),
            "items");
        size_t items_written = state_map_->serialize(*writer_);
        LOGI("Wrote", items_written, "state items to output");

        // Copy transaction map from input to output
        bool tx_map_done = false;
        size_t tx_bytes = 0;

        while (!tx_map_done)
        {
            uint8_t node_type;
            if (reader_->read_raw_data(&node_type, 1) != 1)
            {
                LOGE("Unexpected end of file while copying tx map");
                return false;
            }

            // Write node type to output
            writer_->write_raw_data(&node_type, 1);
            tx_bytes += 1;

            if (node_type == SHAMapNodeType::tnTERMINAL)
            {
                tx_map_done = true;
            }
            else if (
                node_type == SHAMapNodeType::tnTRANSACTION_NM ||
                node_type == SHAMapNodeType::tnTRANSACTION_MD)
            {
                // Copy key
                uint8_t key_data[32];
                if (reader_->read_raw_data(key_data, 32) != 32)
                {
                    LOGE("Unexpected end of file while reading tx key");
                    return false;
                }
                writer_->write_raw_data(key_data, 32);
                tx_bytes += 32;

                // Copy data length and data
                uint32_t data_length;
                if (reader_->read_raw_data(
                        reinterpret_cast<uint8_t*>(&data_length), 4) != 4)
                {
                    LOGE("Unexpected end of file while reading tx data length");
                    return false;
                }
                writer_->write_raw_data(
                    reinterpret_cast<uint8_t*>(&data_length), 4);
                tx_bytes += 4;

                std::vector<uint8_t> data_buffer(data_length);
                if (reader_->read_raw_data(data_buffer.data(), data_length) !=
                    data_length)
                {
                    LOGE("Unexpected end of file while reading tx data");
                    return false;
                }
                writer_->write_raw_data(data_buffer.data(), data_length);
                tx_bytes += data_length;
            }
        }

        LOGI("Copied", tx_bytes, "bytes of transaction map to output");
        stats_.bytes_written += tx_bytes;

        return true;
    }
    catch (const std::exception& e)
    {
        LOGE("Error during fast-forward:", e.what());
        return false;
    }
}

bool
Slicer::process_subsequent_ledgers()
{
    uint32_t start_ledger = options_.start_ledger.value();
    uint32_t end_ledger = options_.end_ledger.value();

    // Skip if there's only one ledger in the slice
    if (start_ledger == end_ledger)
    {
        LOGI(
            "Slice contains only one ledger, no subsequent ledgers to process");
        return true;
    }

    LOGI("Processing subsequent ledgers:", start_ledger + 1, "to", end_ledger);

    try
    {
        for (uint32_t current_seq = start_ledger + 1; current_seq <= end_ledger;
             current_seq++)
        {
            LOGI("Processing ledger", current_seq);

            // Read ledger header
            catl::v1::LedgerInfo ledger_info = reader_->read_ledger_info();
            if (ledger_info.sequence != current_seq)
            {
                LOGE(
                    "Expected ledger",
                    current_seq,
                    "but found",
                    ledger_info.sequence);
                return false;
            }

            // Write ledger header to output
            writer_->write_ledger_header(ledger_info);

            // Byte-naively copy the state map delta from input to output
            size_t state_bytes = copy_state_map_delta(current_seq);
            LOGI("Copied", state_bytes, "bytes of state map delta to output");
            stats_.bytes_written += state_bytes;

            // Byte-naively copy the transaction map from input to output
            size_t tx_bytes = copy_transaction_map();
            LOGI("Copied", tx_bytes, "bytes of transaction map to output");
            stats_.bytes_written += tx_bytes;
        }

        return true;
    }
    catch (const std::exception& e)
    {
        LOGE("Error processing subsequent ledgers:", e.what());
        return false;
    }
}

size_t
Slicer::copy_state_map_delta(uint32_t ledger_seq)
{
    size_t bytes_copied = 0;
    bool done = false;

    while (!done)
    {
        uint8_t node_type;
        if (reader_->read_raw_data(&node_type, 1) != 1)
        {
            throw std::runtime_error(
                "Unexpected end of file while copying state map delta");
        }

        // Write node type to output
        writer_->write_raw_data(&node_type, 1);
        bytes_copied += 1;

        if (node_type == SHAMapNodeType::tnTERMINAL)
        {
            done = true;
        }
        else if (node_type == SHAMapNodeType::tnACCOUNT_STATE)
        {
            // Copy key
            uint8_t key_data[32];
            if (reader_->read_raw_data(key_data, 32) != 32)
            {
                throw std::runtime_error(
                    "Unexpected end of file while reading state key");
            }
            writer_->write_raw_data(key_data, 32);
            bytes_copied += 32;

            // Copy data length and data
            uint32_t data_length;
            if (reader_->read_raw_data(
                    reinterpret_cast<uint8_t*>(&data_length), 4) != 4)
            {
                throw std::runtime_error(
                    "Unexpected end of file while reading state data length");
            }
            writer_->write_raw_data(
                reinterpret_cast<uint8_t*>(&data_length), 4);
            bytes_copied += 4;

            std::vector<uint8_t> data_buffer(data_length);
            if (reader_->read_raw_data(data_buffer.data(), data_length) !=
                data_length)
            {
                throw std::runtime_error(
                    "Unexpected end of file while reading state data");
            }
            writer_->write_raw_data(data_buffer.data(), data_length);
            bytes_copied += data_length;

            // Update in-memory state map if needed for next slice snapshot
            if (options_.create_next_slice_state_snapshot && state_map_)
            {
                Key key(key_data);
                state_map_->set_item(key, data_buffer.data(), data_length);
            }

            stats_.state_items_processed++;
        }
        else if (node_type == SHAMapNodeType::tnREMOVE)
        {
            // Copy key
            uint8_t key_data[32];
            if (reader_->read_raw_data(key_data, 32) != 32)
            {
                throw std::runtime_error(
                    "Unexpected end of file while reading remove key");
            }
            writer_->write_raw_data(key_data, 32);
            bytes_copied += 32;

            // Update in-memory state map if needed
            if (options_.create_next_slice_state_snapshot && state_map_)
            {
                Key key(key_data);
                state_map_->remove_item(key);
            }
        }
        else
        {
            throw std::runtime_error(
                "Unexpected node type in state map: " +
                std::to_string(static_cast<int>(node_type)));
        }
    }

    return bytes_copied;
}

size_t
Slicer::copy_transaction_map()
{
    size_t bytes_copied = 0;
    bool done = false;

    while (!done)
    {
        uint8_t node_type;
        if (reader_->read_raw_data(&node_type, 1) != 1)
        {
            throw std::runtime_error(
                "Unexpected end of file while copying tx map");
        }

        // Write node type to output
        writer_->write_raw_data(&node_type, 1);
        bytes_copied += 1;

        if (node_type == SHAMapNodeType::tnTERMINAL)
        {
            done = true;
        }
        else if (
            node_type == SHAMapNodeType::tnTRANSACTION_NM ||
            node_type == SHAMapNodeType::tnTRANSACTION_MD)
        {
            // Copy key
            uint8_t key_data[32];
            if (reader_->read_raw_data(key_data, 32) != 32)
            {
                throw std::runtime_error(
                    "Unexpected end of file while reading tx key");
            }
            writer_->write_raw_data(key_data, 32);
            bytes_copied += 32;

            // Copy data length and data
            uint32_t data_length;
            if (reader_->read_raw_data(
                    reinterpret_cast<uint8_t*>(&data_length), 4) != 4)
            {
                throw std::runtime_error(
                    "Unexpected end of file while reading tx data length");
            }
            writer_->write_raw_data(
                reinterpret_cast<uint8_t*>(&data_length), 4);
            bytes_copied += 4;

            std::vector<uint8_t> data_buffer(data_length);
            if (reader_->read_raw_data(data_buffer.data(), data_length) !=
                data_length)
            {
                throw std::runtime_error(
                    "Unexpected end of file while reading tx data");
            }
            writer_->write_raw_data(data_buffer.data(), data_length);
            bytes_copied += data_length;
        }
        else
        {
            throw std::runtime_error(
                "Unexpected node type in tx map: " +
                std::to_string(static_cast<int>(node_type)));
        }
    }

    return bytes_copied;
}

bool
Slicer::create_next_slice_snapshot()
{
    if (!state_map_)
    {
        LOGE("Cannot create next slice snapshot: state map not populated");
        return false;
    }

    uint32_t next_ledger = options_.end_ledger.value() + 1;
    std::string snapshot_file = get_snapshot_filename(next_ledger);

    LOGI("Creating snapshot for ledger", next_ledger, ":", snapshot_file);

    // Check if snapshot file exists and handle accordingly
    if (fs::exists(snapshot_file) && !options_.force_overwrite)
    {
        std::string answer;
        std::cout << "Snapshot file '" << snapshot_file
                  << "' already exists. Overwrite? [y/N]: ";
        std::getline(std::cin, answer);
        if (answer != "y" && answer != "Y")
        {
            LOGI("Snapshot creation cancelled by user");
            return false;
        }
    }

    try
    {
        // Read delta for next ledger from input and apply to state_map_
        LOGI(
            "Reading state delta for ledger",
            next_ledger,
            "to create snapshot");

        // Read ledger header for next ledger
        catl::v1::LedgerInfo ledger_info = reader_->read_ledger_info();
        if (ledger_info.sequence != next_ledger)
        {
            LOGE(
                "Expected ledger",
                next_ledger,
                "but found",
                ledger_info.sequence);
            return false;
        }

        // Apply state map delta for next ledger to in-memory state map
        bool done = false;
        size_t items_processed = 0;

        while (!done)
        {
            uint8_t node_type;
            if (reader_->read_raw_data(&node_type, 1) != 1)
            {
                LOGE(
                    "Unexpected end of file while reading next ledger state "
                    "map");
                return false;
            }

            if (node_type == SHAMapNodeType::tnTERMINAL)
            {
                done = true;
            }
            else if (node_type == SHAMapNodeType::tnACCOUNT_STATE)
            {
                // Read key
                uint8_t key_data[32];
                if (reader_->read_raw_data(key_data, 32) != 32)
                {
                    LOGE(
                        "Unexpected end of file while reading next ledger key");
                    return false;
                }
                Key key(key_data);

                // Read data length and data
                uint32_t data_length;
                if (reader_->read_raw_data(
                        reinterpret_cast<uint8_t*>(&data_length), 4) != 4)
                {
                    LOGE(
                        "Unexpected end of file while reading next ledger data "
                        "length");
                    return false;
                }

                std::vector<uint8_t> data(data_length);
                if (reader_->read_raw_data(data.data(), data_length) !=
                    data_length)
                {
                    LOGE(
                        "Unexpected end of file while reading next ledger "
                        "data");
                    return false;
                }

                // Update state map
                state_map_->set_item(key, data.data(), data_length);
                items_processed++;
            }
            else if (node_type == SHAMapNodeType::tnREMOVE)
            {
                // Read key of item to remove
                uint8_t key_data[32];
                if (reader_->read_raw_data(key_data, 32) != 32)
                {
                    LOGE(
                        "Unexpected end of file while reading next ledger "
                        "remove key");
                    return false;
                }
                Key key(key_data);

                // Remove from state map
                state_map_->remove_item(key);
                items_processed++;
            }
            else
            {
                LOGE(
                    "Unexpected node type in next ledger state map:",
                    static_cast<int>(node_type));
                return false;
            }
        }

        LOGI(
            "Applied",
            items_processed,
            "state items from ledger",
            next_ledger,
            "to state map");

        // Skip transaction map (not needed for snapshot)
        bool tx_map_done = false;
        while (!tx_map_done)
        {
            uint8_t node_type;
            if (reader_->read_raw_data(&node_type, 1) != 1)
            {
                LOGE(
                    "Unexpected end of file while skipping next ledger tx map");
                return false;
            }

            if (node_type == SHAMapNodeType::tnTERMINAL)
            {
                tx_map_done = true;
            }
            else if (
                node_type == SHAMapNodeType::tnTRANSACTION_NM ||
                node_type == SHAMapNodeType::tnTRANSACTION_MD)
            {
                // Skip key
                uint8_t key_data[32];
                if (reader_->read_raw_data(key_data, 32) != 32)
                {
                    LOGE(
                        "Unexpected end of file while skipping next ledger tx "
                        "key");
                    return false;
                }

                // Skip data
                uint32_t data_length;
                if (reader_->read_raw_data(
                        reinterpret_cast<uint8_t*>(&data_length), 4) != 4)
                {
                    LOGE(
                        "Unexpected end of file while reading next ledger tx "
                        "data length");
                    return false;
                }

                std::vector<uint8_t> skip_buffer(data_length);
                if (reader_->read_raw_data(skip_buffer.data(), data_length) !=
                    data_length)
                {
                    LOGE(
                        "Unexpected end of file while skipping next ledger tx "
                        "data");
                    return false;
                }
            }
            else
            {
                LOGE(
                    "Unexpected node type in next ledger tx map:",
                    static_cast<int>(node_type));
                return false;
            }
        }

        // Create snapshot file
        std::ofstream file(snapshot_file, std::ios::binary);
        if (!file)
        {
            LOGE("Failed to create snapshot file:", snapshot_file);
            return false;
        }

        // Set up compression
        io::filtering_ostream comp_stream;
        comp_stream.push(io::zlib_compressor(
            io::zlib::best_compression));  // Use max compression
        comp_stream.push(file);

        // Serialize the state map to the compressed stream
        LOGI("Writing", state_map_->size(), "items to snapshot file");

        // Manually serialize items to compressed stream
        size_t items_written = 0;
        for (const auto& [key, data] : state_map_->items())
        {
            // Write each item as: tnACCOUNT_STATE + key + data
            uint8_t type = SHAMapNodeType::tnACCOUNT_STATE;
            comp_stream.write(
                reinterpret_cast<const char*>(&type), sizeof(type));

            // Write key
            comp_stream.write(
                reinterpret_cast<const char*>(key.data()), Key::size());

            // Write data length as uint32_t
            uint32_t data_size = static_cast<uint32_t>(data.size());
            comp_stream.write(
                reinterpret_cast<const char*>(&data_size), sizeof(data_size));

            // Write actual data
            comp_stream.write(
                reinterpret_cast<const char*>(data.data()), data.size());

            items_written++;
        }

        // Write terminal marker
        uint8_t terminal = SHAMapNodeType::tnTERMINAL;
        comp_stream.write(
            reinterpret_cast<const char*>(&terminal), sizeof(terminal));

        comp_stream.flush();

        LOGI(
            "Wrote",
            items_written,
            "items to snapshot file for ledger",
            next_ledger);
        stats_.end_snapshot_created = true;

        return true;
    }
    catch (const std::exception& e)
    {
        LOGE("Error creating snapshot:", e.what());
        return false;
    }
}

std::string
Slicer::get_snapshot_filename(uint32_t ledger_seq) const
{
    std::ostringstream oss;
    oss << snapshots_path_ << "/state_snapshot_for_ledger_" << ledger_seq
        << ".dat.zst";
    return oss.str();
}

const SliceStats&
Slicer::stats() const
{
    return stats_;
}

}  // namespace catl::utils::slicer