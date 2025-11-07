#include "catl/utils-v1/nudb/catl1-to-nudb-arg-options.h"

#include <boost/program_options.hpp>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace po = boost::program_options;
namespace catl::v1::utils::nudb {

Catl1ToNudbOptions
parse_catl1_to_nudb_argv(int argc, char* argv[])
{
    Catl1ToNudbOptions options;

    // Define command line options with Boost
    po::options_description desc("Allowed options");
    desc.add_options()("help,h", "Display this help message")(
        "input,i", po::value<std::string>(), "Path to the input CATL file")(
        "nudb-path,n",
        po::value<std::string>(),
        "Path to the output NuDB database directory")(
        "start-ledger,s",
        po::value<uint32_t>(),
        "Start ledger sequence number (optional, defaults to file's "
        "min_ledger)")(
        "end-ledger,e",
        po::value<uint32_t>(),
        "End ledger sequence number (optional, defaults to file's max_ledger)")(
        "force,f",
        po::bool_switch(),
        "Force overwrite of existing database without prompting")(
        "create-database",
        po::bool_switch()->default_value(true),
        "Create database if it doesn't exist")(
        "key-size",
        po::value<uint32_t>()->default_value(32),
        "NuDB key size in bytes (default: 32 for 256-bit hashes)")(
        "block-size",
        po::value<uint32_t>()->default_value(4096),
        "NuDB block size (default: 4096)")(
        "load-factor,F",
        po::value<double>()->default_value(0.5),
        "NuDB load factor 0.0-1.0 (default: 0.5) - lower = faster, higher = "
        "more space efficient")(
        "nudb-factor", po::value<double>(), "Alias for --load-factor")(
        "log-level,l",
        po::value<std::string>()->default_value("info"),
        "Log level (error, warn, info, debug)")(
        "test-snapshots",
        po::bool_switch(),
        "Test snapshot memory usage (reads file and creates snapshots without "
        "pipeline)")(
        "hasher-threads",
        po::value<int>()->default_value(1),
        "Number of threads for parallel hashing (must be power of 2: 1, 2, 4, "
        "8, 16) - Default 1 (best performance)")(
        "compressor-threads",
        po::value<int>()->default_value(2),
        "Number of threads for parallel compression (default: 2)")(
        "max-write-queue-mb",
        po::value<uint32_t>()->default_value(2048),
        "Max write queue size in megabytes (default: 2048 MB = 2 GB)")(
        "enable-debug-partitions",
        po::bool_switch(),
        "Enable verbose debug log partitions (MAP_OPS, WALK_NODES, "
        "VERSION_TRACK, PIPE_VERSION)")(
        "walk-nodes-ledger",
        po::value<uint32_t>(),
        "Enable WALK_NODES logging only for the specified ledger number "
        "(useful for debugging specific ledger issues)")(
        "walk-nodes-debug-key",
        po::value<std::string>(),
        "Debug key prefix (hex) to print detailed info for matching keys "
        "during walk_nodes (e.g., '567D5DABE2E1AF17')")(
        "nudb-mock",
        po::value<std::string>(),
        "Mock NuDB mode for performance testing. Options: 'noop' or 'memory' "
        "(skip all I/O), 'disk' (buffered append-only file), 'nudb' (regular "
        "NuDB inserts, no bulk writer)")(
        "verify-keys",
        po::bool_switch(),
        "Verify all inserted keys are readable after import (8 threads)")(
        "no-dedupe",
        po::bool_switch(),
        "Skip deduplication tracking for faster writes (disables "
        "verification)");

    // Generate the help text
    std::ostringstream help_stream;
    help_stream
        << "CATL to NuDB Converter Tool" << std::endl
        << "---------------------------" << std::endl
        << "Converts a CATL v1 file into a NuDB database" << std::endl
        << std::endl
        << "Usage: " << (argc > 0 ? argv[0] : "catl1-to-nudb")
        << " --input <catl_file> --nudb-path <db_directory> [options]"
        << std::endl
        << desc << std::endl
        << "This tool reads ledger data from a CATL file and stores it "
           "in a NuDB database"
        << std::endl
        << "for efficient key-value lookups. If start/end ledgers are not "
           "specified,"
        << std::endl
        << "it will process the entire file." << std::endl;
    options.help_text = help_stream.str();

    try
    {
        // Parse command line
        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
        po::notify(vm);

        // Check for help flag
        if (vm.count("help"))
        {
            options.show_help = true;
            return options;
        }

        // Check for required input file
        if (vm.count("input"))
        {
            options.input_file = vm["input"].as<std::string>();
        }
        else
        {
            options.valid = false;
            options.error_message = "No input file specified (--input)";
            return options;
        }

        // Check for test-snapshots mode
        if (vm.count("test-snapshots"))
        {
            options.test_snapshots = vm["test-snapshots"].as<bool>();
        }

        // Check for debug partitions flag
        if (vm.count("enable-debug-partitions"))
        {
            options.enable_debug_partitions =
                vm["enable-debug-partitions"].as<bool>();
        }

        // Check for walk-nodes-ledger
        if (vm.count("walk-nodes-ledger"))
        {
            options.walk_nodes_ledger = vm["walk-nodes-ledger"].as<uint32_t>();
        }

        // Check for walk-nodes-debug-key
        if (vm.count("walk-nodes-debug-key"))
        {
            options.walk_nodes_debug_key =
                vm["walk-nodes-debug-key"].as<std::string>();
        }

        // Check for nudb-mock mode
        if (vm.count("nudb-mock"))
        {
            std::string mock_mode = vm["nudb-mock"].as<std::string>();
            // Validate mock mode
            if (mock_mode != "noop" && mock_mode != "memory" &&
                mock_mode != "disk" && mock_mode != "nudb")
            {
                options.valid = false;
                options.error_message =
                    "nudb-mock must be one of: noop, memory, disk, nudb";
                return options;
            }
            options.nudb_mock = mock_mode;
        }

        // Check for verify-keys flag
        if (vm.count("verify-keys"))
        {
            options.verify_keys = vm["verify-keys"].as<bool>();
        }

        // Check for no-dedupe flag
        if (vm.count("no-dedupe"))
        {
            options.no_dedupe = vm["no-dedupe"].as<bool>();
        }

        if (vm.count("hasher-threads"))
        {
            int threads = vm["hasher-threads"].as<int>();
            // Validate it's a power of 2
            if (threads <= 0 || (threads & (threads - 1)) != 0 || threads > 16)
            {
                options.valid = false;
                options.error_message =
                    "hasher-threads must be a power of 2 (1, 2, 4, 8, or 16)";
                return options;
            }
            options.hasher_threads = threads;
        }

        if (vm.count("compressor-threads"))
        {
            int threads = vm["compressor-threads"].as<int>();
            if (threads <= 0 || threads > 32)
            {
                options.valid = false;
                options.error_message =
                    "compressor-threads must be between 1 and 32";
                return options;
            }
            options.compressor_threads = threads;
        }

        if (vm.count("max-write-queue-mb"))
        {
            options.max_write_queue_mb =
                vm["max-write-queue-mb"].as<uint32_t>();
            if (options.max_write_queue_mb == 0)
            {
                options.valid = false;
                options.error_message =
                    "max-write-queue-mb must be greater than 0";
                return options;
            }
        }

        // Check for required nudb path (not required in test-snapshots mode)
        if (vm.count("nudb-path"))
        {
            options.nudb_path = vm["nudb-path"].as<std::string>();
        }
        else if (!options.test_snapshots)
        {
            options.valid = false;
            options.error_message = "No NuDB path specified (--nudb-path)";
            return options;
        }

        // Check for optional start ledger
        if (vm.count("start-ledger"))
        {
            options.start_ledger = vm["start-ledger"].as<uint32_t>();
        }

        // Check for optional end ledger
        if (vm.count("end-ledger"))
        {
            options.end_ledger = vm["end-ledger"].as<uint32_t>();

            // Validate end_ledger is >= start_ledger if both specified
            if (options.start_ledger &&
                *options.end_ledger < *options.start_ledger)
            {
                options.valid = false;
                options.error_message = "End ledger must be >= start ledger";
                return options;
            }
        }

        // Check force overwrite flag
        if (vm.count("force"))
        {
            options.force_overwrite = vm["force"].as<bool>();
        }

        // Check create database flag
        if (vm.count("create-database"))
        {
            options.create_database = vm["create-database"].as<bool>();
        }

        // Get NuDB parameters
        if (vm.count("key-size"))
        {
            options.key_size = vm["key-size"].as<uint32_t>();
            if (options.key_size == 0)
            {
                options.valid = false;
                options.error_message = "Key size must be greater than 0";
                return options;
            }
        }

        if (vm.count("block-size"))
        {
            options.block_size = vm["block-size"].as<uint32_t>();
            if (options.block_size == 0)
            {
                options.valid = false;
                options.error_message = "Block size must be greater than 0";
                return options;
            }
        }

        // Support both --load-factor and --nudb-factor (alias)
        if (vm.count("nudb-factor"))
        {
            options.load_factor = vm["nudb-factor"].as<double>();
        }
        else if (vm.count("load-factor"))
        {
            options.load_factor = vm["load-factor"].as<double>();
        }

        if (options.load_factor <= 0.0 || options.load_factor > 1.0)
        {
            options.valid = false;
            options.error_message = "Load factor must be between 0.0 and 1.0";
            return options;
        }

        // Get log level
        if (vm.count("log-level"))
        {
            std::string level = vm["log-level"].as<std::string>();
            // Validate log level
            if (level != "error" && level != "warn" && level != "info" &&
                level != "debug")
            {
                options.valid = false;
                options.error_message =
                    "Log level must be one of: error, warn, info, debug";
                return options;
            }
            options.log_level = level;
        }
    }
    catch (const po::error& e)
    {
        options.valid = false;
        options.error_message = e.what();
    }
    catch (const std::exception& e)
    {
        options.valid = false;
        options.error_message = std::string("Unexpected error: ") + e.what();
    }

    return options;
}

}  // namespace catl::v1::utils::nudb