#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <ftxui/component/component.hpp>
#include <ftxui/component/loop.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace catl::v1::utils::nudb {

/**
 * Real-time dashboard for catl1-to-nudb pipeline monitoring
 *
 * Features:
 * - Queue depth gauges (hasher, compression, dedupe, assembly, write)
 * - Throughput graphs (ledgers/sec, nodes/sec)
 * - Duplicate statistics
 * - Color-coded status indicators
 *
 * Usage:
 *   PipelineDashboard dashboard;
 *   dashboard.start();
 *
 *   // Update stats from pipeline thread:
 *   dashboard.update_stats(hasher_queue, compression_queue, ...);
 *
 *   // Stop dashboard when done:
 *   dashboard.stop();
 */
class PipelineDashboard
{
public:
    struct Stats
    {
        // Queue depths
        size_t hasher_queue = 0;
        size_t compression_queue = 0;
        size_t dedupe_queue = 0;
        size_t assembly_queue = 0;
        size_t write_queue = 0;

        // Progress counters
        uint32_t start_ledger = 0;
        uint32_t end_ledger = 0;
        uint32_t current_ledger = 0;
        uint64_t ledgers_processed = 0;
        uint64_t inner_nodes = 0;  // Total inner (state + tx)
        uint64_t leaf_nodes = 0;   // Total leaf (state + tx)
        uint64_t duplicates = 0;   // Total duplicates

        // Total nodes by type
        uint64_t total_state_inner = 0;
        uint64_t total_tx_inner = 0;
        uint64_t total_state_leaf = 0;
        uint64_t total_tx_leaf = 0;
        uint64_t total_ledger_headers = 0;

        // Duplicates by type
        uint64_t duplicates_state_inner = 0;
        uint64_t duplicates_tx_inner = 0;
        uint64_t duplicates_state_leaf = 0;

        // Status
        std::string status =
            "Processing";  // "Processing", "Draining", "Rekeying", "Complete"

        // Performance metrics
        double elapsed_sec = 0.0;
        double ledgers_per_sec = 0.0;
        double nodes_per_sec = 0.0;
        double catl_read_mb_per_sec = 0.0;
        double nudb_write_mb_per_sec = 0.0;
        uint64_t bytes_read = 0;
        uint64_t bytes_written = 0;
        uint64_t bytes_uncompressed = 0;
        double compression_ratio = 0.0;

        // Node operations
        uint64_t state_added = 0;
        uint64_t state_updated = 0;
        uint64_t state_deleted = 0;
        uint64_t tx_added = 0;

        // RocksDB stats (optional)
        uint64_t rocks_fast_path = 0;
        uint64_t rocks_slow_path = 0;
        uint64_t rocks_false_positives = 0;
    };

    PipelineDashboard();
    ~PipelineDashboard();

    /**
     * Start the dashboard UI in a separate thread
     * Non-blocking - dashboard runs in background
     */
    void
    start();

    /**
     * Stop the dashboard and wait for UI thread to exit
     */
    void
    stop();

    /**
     * Update dashboard stats (thread-safe)
     * Call this periodically from your pipeline
     */
    void
    update_stats(const Stats& stats);

    /**
     * Get current stats snapshot (thread-safe)
     */
    Stats
    get_stats() const;

    /**
     * Check if dashboard is still running
     */
    bool
    is_running() const
    {
        return running_;
    }

private:
    void
    run_ui();  // UI thread main loop

    // Stats storage (atomic for thread safety)
    std::atomic<size_t> hasher_queue_{0};
    std::atomic<size_t> compression_queue_{0};
    std::atomic<size_t> dedupe_queue_{0};
    std::atomic<size_t> assembly_queue_{0};
    std::atomic<size_t> write_queue_{0};

    std::atomic<uint32_t> start_ledger_{0};
    std::atomic<uint32_t> end_ledger_{0};
    std::atomic<uint32_t> current_ledger_{0};
    std::atomic<uint64_t> ledgers_processed_{0};
    std::atomic<uint64_t> inner_nodes_{0};  // Total inner (state + tx)
    std::atomic<uint64_t> leaf_nodes_{0};   // Total leaf (state + tx)
    std::atomic<uint64_t> duplicates_{0};   // Total duplicates

    // Total nodes by type
    std::atomic<uint64_t> total_state_inner_{0};
    std::atomic<uint64_t> total_tx_inner_{0};
    std::atomic<uint64_t> total_state_leaf_{0};
    std::atomic<uint64_t> total_tx_leaf_{0};
    std::atomic<uint64_t> total_ledger_headers_{0};

    // Duplicates by type
    std::atomic<uint64_t> duplicates_state_inner_{0};
    std::atomic<uint64_t> duplicates_tx_inner_{0};
    std::atomic<uint64_t> duplicates_state_leaf_{0};

    // Status (protected by mutex since it's a string)
    std::string status_{"Processing"};
    mutable std::mutex status_mutex_;

    // Performance metrics (using double in atomic is C++20, store as bits)
    std::atomic<double> elapsed_sec_{0.0};
    std::atomic<double> ledgers_per_sec_{0.0};
    std::atomic<double> nodes_per_sec_{0.0};
    std::atomic<double> catl_read_mb_per_sec_{0.0};
    std::atomic<double> nudb_write_mb_per_sec_{0.0};
    std::atomic<uint64_t> bytes_read_{0};
    std::atomic<uint64_t> bytes_written_{0};
    std::atomic<uint64_t> bytes_uncompressed_{0};
    std::atomic<double> compression_ratio_{0.0};

    // Node operations
    std::atomic<uint64_t> state_added_{0};
    std::atomic<uint64_t> state_updated_{0};
    std::atomic<uint64_t> state_deleted_{0};
    std::atomic<uint64_t> tx_added_{0};

    std::atomic<uint64_t> rocks_fast_path_{0};
    std::atomic<uint64_t> rocks_slow_path_{0};
    std::atomic<uint64_t> rocks_false_positives_{0};

    // Throughput tracking
    struct ThroughputSample
    {
        std::chrono::steady_clock::time_point timestamp;
        uint64_t ledgers;
        uint64_t nodes;
    };
    std::deque<ThroughputSample> throughput_history_;
    mutable std::mutex throughput_mutex_;

    // UI thread
    std::unique_ptr<std::thread> ui_thread_;
    std::atomic<bool> running_{false};
};

}  // namespace catl::v1::utils::nudb
