#pragma once

#include <atomic>
#include <chrono>
#include <ftxui/component/component.hpp>
#include <ftxui/component/loop.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace nudbview {

/**
 * Real-time dashboard for keyfile analysis
 *
 * Shows live progress as buckets are scanned:
 * - Progress gauge (buckets scanned / total)
 * - Entry count histogram (updating live)
 * - Collision statistics
 * - Throughput metrics
 */
class KeyfileStatsDashboard
{
public:
    struct Stats
    {
        // Scan progress
        uint64_t buckets_scanned = 0;
        uint64_t total_buckets = 0;

        // Bucket statistics
        uint64_t empty_buckets = 0;
        uint64_t full_buckets = 0;
        uint64_t buckets_with_spills = 0;

        // Entry statistics
        uint64_t total_entries = 0;
        uint64_t max_entries_in_bucket = 0;

        // Collision statistics
        uint64_t total_collisions = 0;
        uint64_t buckets_with_collisions = 0;

        // Capacity utilization
        uint64_t capacity_per_bucket = 0;

        // Histogram (entry_count -> bucket_count)
        std::map<std::size_t, uint64_t> entry_count_histogram;

        // Collision histogram (collision_count -> bucket_count)
        std::map<std::size_t, uint64_t> collision_count_histogram;

        // Performance
        double elapsed_sec = 0.0;
        double buckets_per_sec = 0.0;

        // File info
        std::string key_file_path;
        uint64_t file_size_mb = 0;
        std::size_t block_size = 0;
        float load_factor = 0.0f;
    };

    KeyfileStatsDashboard();
    ~KeyfileStatsDashboard();

    /**
     * Start the dashboard UI in a separate thread
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
    std::atomic<uint64_t> buckets_scanned_{0};
    std::atomic<uint64_t> total_buckets_{0};
    std::atomic<uint64_t> empty_buckets_{0};
    std::atomic<uint64_t> full_buckets_{0};
    std::atomic<uint64_t> buckets_with_spills_{0};
    std::atomic<uint64_t> total_entries_{0};
    std::atomic<uint64_t> max_entries_in_bucket_{0};
    std::atomic<uint64_t> total_collisions_{0};
    std::atomic<uint64_t> buckets_with_collisions_{0};
    std::atomic<uint64_t> capacity_per_bucket_{0};
    std::atomic<double> elapsed_sec_{0.0};
    std::atomic<double> buckets_per_sec_{0.0};

    // Histograms (protected by mutex since they're complex)
    std::map<std::size_t, uint64_t> entry_count_histogram_;
    std::map<std::size_t, uint64_t> collision_count_histogram_;
    mutable std::mutex histogram_mutex_;

    // File info (strings need mutex)
    std::string key_file_path_;
    uint64_t file_size_mb_{0};
    std::size_t block_size_{0};
    float load_factor_{0.0f};
    mutable std::mutex file_info_mutex_;

    // UI thread
    std::unique_ptr<std::thread> ui_thread_;
    std::atomic<bool> running_{false};
};

}  // namespace nudbview
