#pragma once

#include "catl/utils-v1/nudb/pipeline-dashboard.h"
#include "catl/utils-v1/nudb/stats-report-sink.h"
#include <memory>

namespace catl::v1::utils::nudb {

/**
 * Adapter that connects the pipeline's StatsReportSink interface
 * to the FTXUI dashboard implementation
 */
class DashboardStatsReportSink : public StatsReportSink
{
public:
    DashboardStatsReportSink()
    {
        dashboard_ = std::make_unique<PipelineDashboard>();
        dashboard_->start();
    }

    ~DashboardStatsReportSink()
    {
        if (dashboard_)
        {
            dashboard_->stop();
        }
    }

    void
    report_stats(
        const QueueDepths& queues,
        const ProgressCounters& progress,
        const PerformanceMetrics& perf,
        const NodeOperations& ops,
        const DeduplicationStats& dedup_stats) override
    {
        if (!dashboard_)
        {
            return;
        }

        PipelineDashboard::Stats stats;
        stats.hasher_queue = queues.hasher_queue;
        stats.compression_queue = queues.compression_queue;
        stats.dedupe_queue = queues.dedupe_queue;
        stats.assembly_queue = queues.assembly_queue;
        stats.write_queue = queues.write_queue;

        stats.start_ledger = progress.start_ledger;
        stats.end_ledger = progress.end_ledger;
        stats.current_ledger = progress.current_ledger;
        stats.ledgers_processed = progress.ledgers_processed;
        stats.inner_nodes = progress.inner_nodes;
        stats.leaf_nodes = progress.leaf_nodes;
        stats.duplicates = progress.duplicates;
        stats.status = progress.status;

        stats.elapsed_sec = perf.elapsed_sec;
        stats.ledgers_per_sec = perf.ledgers_per_sec;
        stats.nodes_per_sec = perf.nodes_per_sec;
        stats.catl_read_mb_per_sec = perf.catl_read_mb_per_sec;
        stats.nudb_write_mb_per_sec = perf.nudb_write_mb_per_sec;
        stats.bytes_read = perf.bytes_read;
        stats.bytes_written = perf.bytes_written;
        stats.bytes_uncompressed = perf.bytes_uncompressed;
        stats.compression_ratio = perf.compression_ratio;

        stats.state_added = ops.state_added;
        stats.state_updated = ops.state_updated;
        stats.state_deleted = ops.state_deleted;
        stats.tx_added = ops.tx_added;

        stats.rocks_fast_path = dedup_stats.fast_path_hits;
        stats.rocks_slow_path = dedup_stats.slow_path_hits;
        stats.rocks_false_positives = dedup_stats.false_positives;

        dashboard_->update_stats(stats);
    }

    bool
    is_active() const override
    {
        return dashboard_ && dashboard_->is_running();
    }

private:
    std::unique_ptr<PipelineDashboard> dashboard_;
};

}  // namespace catl::v1::utils::nudb
