#pragma once

#include <cstdint>
#include <memory>

namespace catl::v1::utils::nudb {

/**
 * Abstract interface for receiving pipeline statistics updates
 *
 * This allows the pipeline to report stats without knowing what consumes them.
 * Implementations can be: dashboard, logger, metrics exporter, etc.
 */
class StatsReportSink
{
public:
    virtual ~StatsReportSink() = default;

    struct QueueDepths
    {
        size_t hasher_queue = 0;
        size_t compression_queue = 0;
        size_t dedupe_queue = 0;
        size_t assembly_queue = 0;
        size_t write_queue = 0;
    };

    struct ProgressCounters
    {
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

        std::string status =
            "Processing";  // "Processing", "Draining", "Rekeying", "Complete"
    };

    struct PerformanceMetrics
    {
        double elapsed_sec = 0.0;
        double ledgers_per_sec = 0.0;
        double nodes_per_sec = 0.0;
        double catl_read_mb_per_sec = 0.0;
        double nudb_write_mb_per_sec = 0.0;
        uint64_t bytes_read = 0;
        uint64_t bytes_written = 0;
        uint64_t bytes_uncompressed = 0;
        double compression_ratio = 0.0;
    };

    struct NodeOperations
    {
        uint64_t state_added = 0;
        uint64_t state_updated = 0;
        uint64_t state_deleted = 0;
        uint64_t tx_added = 0;
    };

    struct DeduplicationStats
    {
        uint64_t fast_path_hits = 0;
        uint64_t slow_path_hits = 0;
        uint64_t false_positives = 0;
        uint64_t true_duplicates = 0;
    };

    /**
     * Called periodically to report current pipeline state
     */
    virtual void
    report_stats(
        const QueueDepths& queues,
        const ProgressCounters& progress,
        const PerformanceMetrics& perf,
        const NodeOperations& ops,
        const DeduplicationStats& dedup_stats) = 0;

    /**
     * Check if the sink wants to continue receiving stats
     * Return false to signal shutdown (e.g., user pressed 'q' in dashboard)
     */
    virtual bool
    is_active() const = 0;
};

/**
 * No-op sink that discards all stats
 */
class NullStatsReportSink : public StatsReportSink
{
public:
    void
    report_stats(
        const QueueDepths&,
        const ProgressCounters&,
        const PerformanceMetrics&,
        const NodeOperations&,
        const DeduplicationStats&) override
    {
        // Discard
    }

    bool
    is_active() const override
    {
        return true;
    }
};

}  // namespace catl::v1::utils::nudb
