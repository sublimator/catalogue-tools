#include "catl/utils-v1/nudb/pipeline-dashboard.h"
#include <chrono>
#include <iostream>
#include <random>
#include <thread>

using namespace catl::v1::utils::nudb;

int main()
{
    std::cout << "Starting Pipeline Dashboard Demo...\n";
    std::cout << "Press 'q' in the dashboard to quit.\n\n";

    PipelineDashboard dashboard;
    dashboard.start();

    // Simulate pipeline activity
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> queue_dist(0, 500);
    std::uniform_int_distribution<> small_queue_dist(0, 100);

    PipelineDashboard::Stats stats{};
    uint64_t ledger_count = 0;
    uint64_t node_count = 0;
    uint64_t dup_count = 0;

    // Simulate pipeline running for 5 minutes or until user quits
    for (int i = 0; i < 300 && dashboard.is_running(); ++i)
    {
        // Simulate processing
        ledger_count += 10;
        node_count += 150;
        dup_count += 5;

        // Simulate varying queue depths
        stats.hasher_queue = queue_dist(gen);
        stats.compression_queue = queue_dist(gen);
        stats.dedupe_queue = queue_dist(gen);
        stats.assembly_queue = small_queue_dist(gen);
        stats.write_queue = small_queue_dist(gen);

        stats.ledgers_processed = ledger_count;
        stats.inner_nodes = node_count / 2;
        stats.leaf_nodes = node_count / 2;
        stats.duplicates = dup_count;

        // Simulate RocksDB stats
        stats.rocks_fast_path = node_count * 99 / 100;  // 99% fast path
        stats.rocks_slow_path = node_count / 100;       // 1% slow path
        stats.rocks_false_positives = stats.rocks_slow_path / 10;

        dashboard.update_stats(stats);

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    dashboard.stop();

    std::cout << "\nDashboard demo completed.\n";
    std::cout << "Final stats:\n";
    std::cout << "  Ledgers: " << ledger_count << "\n";
    std::cout << "  Nodes: " << node_count << "\n";
    std::cout << "  Duplicates: " << dup_count << "\n";

    return 0;
}
