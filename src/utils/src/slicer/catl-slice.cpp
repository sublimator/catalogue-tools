#include "catl/core/logger.h"
#include "catl/utils/slicer/arg-options.h"
#include "catl/utils/slicer/slicer.h"
#include <iostream>

using namespace catl::utils::slicer;

int
main(int argc, char* argv[])
{
    // Parse command line arguments
    CommandLineOptions options = parse_argv(argc, argv);

    // Display help if requested or if argument parsing failed
    if (options.show_help || !options.valid)
    {
        if (options.error_message)
        {
            std::cerr << *options.error_message << std::endl << std::endl;
        }
        std::cout << options.help_text << std::endl;
        return options.valid ? 0 : 1;
    }

    // Set the log level
    Logger::set_level(convert_to_core_log_level(options.log_level));

    // Print the configuration
    LOGI("catl-slice: High-Performance CATL File Slicing Tool");
    LOGI("Input file:", *options.input_file);
    LOGI("Output file:", *options.output_file);
    LOGI("Ledger range:", *options.start_ledger, "to", *options.end_ledger);
    LOGI("Snapshots path:", *options.snapshots_path);
    LOGI("Compression level:", static_cast<int>(options.compression_level));
    LOGI("Use start snapshot:", options.use_start_snapshot ? "Yes" : "No");
    LOGI(
        "Create next slice snapshot:",
        options.create_next_slice_state_snapshot ? "Yes" : "No");

    try
    {
        // Create and run the slicer
        Slicer slicer(options);
        bool success = slicer.run();

        if (success)
        {
            LOGI("Slice operation completed successfully.");
            const auto& stats = slicer.stats();

            // Print summary
            std::cout << "\nSummary:" << std::endl;
            std::cout << "  Ledger range: " << stats.start_ledger << " to "
                      << stats.end_ledger << std::endl;
            std::cout << "  Bytes processed: " << stats.bytes_processed
                      << std::endl;
            std::cout << "  Bytes written: " << stats.bytes_written
                      << std::endl;
            std::cout << "  State items processed: "
                      << stats.state_items_processed << std::endl;
            std::cout << "  Start snapshot used: "
                      << (stats.start_snapshot_used ? "Yes" : "No")
                      << std::endl;
            std::cout << "  End snapshot created: "
                      << (stats.end_snapshot_created ? "Yes" : "No")
                      << std::endl;
            std::cout << "  Time taken: " << stats.elapsed_seconds << " seconds"
                      << std::endl;

            return 0;
        }
        else
        {
            LOGE("Slice operation failed.");
            return 1;
        }
    }
    catch (const std::exception& e)
    {
        LOGE("Error:", e.what());
        return 1;
    }
}
