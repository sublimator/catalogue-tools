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
        uint64_t ledgers_processed = 0;
        uint64_t inner_nodes = 0;
        uint64_t leaf_nodes = 0;
        uint64_t duplicates = 0;

        // Throughput (updated by dashboard)
        double ledgers_per_sec = 0.0;
        double nodes_per_sec = 0.0;

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
    void start();

    /**
     * Stop the dashboard and wait for UI thread to exit
     */
    void stop();

    /**
     * Update dashboard stats (thread-safe)
     * Call this periodically from your pipeline
     */
    void update_stats(const Stats& stats);

    /**
     * Get current stats snapshot (thread-safe)
     */
    Stats get_stats() const;

    /**
     * Check if dashboard is still running
     */
    bool is_running() const
    {
        return running_;
    }

private:
    void run_ui();  // UI thread main loop

    // Stats storage (atomic for thread safety)
    std::atomic<size_t> hasher_queue_{0};
    std::atomic<size_t> compression_queue_{0};
    std::atomic<size_t> dedupe_queue_{0};
    std::atomic<size_t> assembly_queue_{0};
    std::atomic<size_t> write_queue_{0};

    std::atomic<uint64_t> ledgers_processed_{0};
    std::atomic<uint64_t> inner_nodes_{0};
    std::atomic<uint64_t> leaf_nodes_{0};
    std::atomic<uint64_t> duplicates_{0};

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
