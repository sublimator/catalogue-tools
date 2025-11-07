#include "catl/utils-v1/nudb/pipeline-dashboard.h"
#include <ftxui/screen/color.hpp>
#include <iomanip>
#include <sstream>

namespace catl::v1::utils::nudb {

using namespace ftxui;

PipelineDashboard::PipelineDashboard() = default;

PipelineDashboard::~PipelineDashboard()
{
    stop();
}

void
PipelineDashboard::start()
{
    if (running_.exchange(true))
    {
        return;  // Already running
    }

    ui_thread_ = std::make_unique<std::thread>([this] { run_ui(); });
}

void
PipelineDashboard::stop()
{
    if (!running_.exchange(false))
    {
        return;  // Not running
    }

    if (ui_thread_ && ui_thread_->joinable())
    {
        ui_thread_->join();
    }
}

void
PipelineDashboard::update_stats(const Stats& stats)
{
    hasher_queue_ = stats.hasher_queue;
    compression_queue_ = stats.compression_queue;
    dedupe_queue_ = stats.dedupe_queue;
    assembly_queue_ = stats.assembly_queue;
    write_queue_ = stats.write_queue;

    ledgers_processed_ = stats.ledgers_processed;
    inner_nodes_ = stats.inner_nodes;
    leaf_nodes_ = stats.leaf_nodes;
    duplicates_ = stats.duplicates;

    rocks_fast_path_ = stats.rocks_fast_path;
    rocks_slow_path_ = stats.rocks_slow_path;
    rocks_false_positives_ = stats.rocks_false_positives;

    // Update throughput history
    {
        std::lock_guard<std::mutex> lock(throughput_mutex_);
        auto now = std::chrono::steady_clock::now();
        uint64_t total_nodes = stats.inner_nodes + stats.leaf_nodes;

        throughput_history_.push_back(
            {now, stats.ledgers_processed, total_nodes});

        // Keep only last 60 seconds of samples
        while (throughput_history_.size() > 60)
        {
            throughput_history_.pop_front();
        }
    }
}

PipelineDashboard::Stats
PipelineDashboard::get_stats() const
{
    Stats stats;
    stats.hasher_queue = hasher_queue_;
    stats.compression_queue = compression_queue_;
    stats.dedupe_queue = dedupe_queue_;
    stats.assembly_queue = assembly_queue_;
    stats.write_queue = write_queue_;

    stats.ledgers_processed = ledgers_processed_;
    stats.inner_nodes = inner_nodes_;
    stats.leaf_nodes = leaf_nodes_;
    stats.duplicates = duplicates_;

    stats.rocks_fast_path = rocks_fast_path_;
    stats.rocks_slow_path = rocks_slow_path_;
    stats.rocks_false_positives = rocks_false_positives_;

    return stats;
}

void
PipelineDashboard::run_ui()
{
    auto screen = ScreenInteractive::Fullscreen();

    // Helper: Format large numbers with commas
    auto format_number = [](uint64_t num) -> std::string {
        std::stringstream ss;
        ss.imbue(std::locale(""));
        ss << std::fixed << num;
        return ss.str();
    };

    // Helper: Format throughput
    auto format_rate = [](double rate) -> std::string {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(1) << rate;
        return ss.str();
    };

    // Helper: Queue gauge with color coding
    auto queue_gauge_colored = [](size_t depth, size_t max_depth,
                                   const std::string& label) -> Element {
        float progress =
            max_depth > 0 ? static_cast<float>(depth) / max_depth : 0.0f;

        // Color coding: green (<50%), yellow (50-80%), red (>80%)
        Color gauge_color = Color::Green;
        if (progress > 0.8f)
        {
            gauge_color = Color::Red;
        }
        else if (progress > 0.5f)
        {
            gauge_color = Color::Yellow;
        }

        return hbox({
            text(label) | size(WIDTH, EQUAL, 20),
            gauge(progress) | flex | color(gauge_color),
            text(" " + std::to_string(depth)) | size(WIDTH, EQUAL, 10),
        });
    };

    // Helper: Throughput graph
    auto throughput_graph = [this](int width, int height,
                                   bool show_ledgers) -> std::vector<int> {
        std::lock_guard<std::mutex> lock(throughput_mutex_);

        std::vector<int> output(width, 0);

        if (throughput_history_.size() < 2)
        {
            return output;  // Not enough data
        }

        // Calculate rates for each time window
        std::vector<double> rates;
        for (size_t i = 1; i < throughput_history_.size(); ++i)
        {
            auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(
                          throughput_history_[i].timestamp -
                          throughput_history_[i - 1].timestamp)
                          .count();

            if (dt > 0)
            {
                double rate = 0.0;
                if (show_ledgers)
                {
                    rate = (throughput_history_[i].ledgers -
                            throughput_history_[i - 1].ledgers) *
                           1000.0 / dt;
                }
                else
                {
                    rate = (throughput_history_[i].nodes -
                            throughput_history_[i - 1].nodes) *
                           1000.0 / dt;
                }
                rates.push_back(rate);
            }
        }

        if (rates.empty())
        {
            return output;
        }

        // Find max rate for scaling
        double max_rate = 1.0;
        for (double rate : rates)
        {
            if (rate > max_rate)
            {
                max_rate = rate;
            }
        }

        // Map rates to output array
        size_t start_idx =
            rates.size() > static_cast<size_t>(width) ? rates.size() - width : 0;
        for (int i = 0; i < width && (start_idx + i) < rates.size(); ++i)
        {
            double normalized = rates[start_idx + i] / max_rate;
            output[i] = static_cast<int>(normalized * height);
        }

        return output;
    };

    // Main render component
    auto component = Renderer([&]() -> Element {
        // Calculate current throughput
        double ledgers_per_sec = 0.0;
        double nodes_per_sec = 0.0;
        {
            std::lock_guard<std::mutex> lock(throughput_mutex_);
            if (throughput_history_.size() >= 2)
            {
                auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(
                              throughput_history_.back().timestamp -
                              throughput_history_.front().timestamp)
                              .count();
                if (dt > 0)
                {
                    ledgers_per_sec =
                        (throughput_history_.back().ledgers -
                         throughput_history_.front().ledgers) *
                        1000.0 / dt;
                    nodes_per_sec = (throughput_history_.back().nodes -
                                     throughput_history_.front().nodes) *
                                    1000.0 / dt;
                }
            }
        }

        // Queue depths section
        auto queues_section = vbox({
            text("📦 QUEUE DEPTHS") | bold | color(Color::Cyan),
            separator(),
            queue_gauge_colored(
                hasher_queue_.load(), 500, "Hasher (ledgers)"),
            queue_gauge_colored(
                compression_queue_.load(), 500, "Compression (ledgers)"),
            queue_gauge_colored(dedupe_queue_.load(), 500, "Dedupe (jobs)"),
            queue_gauge_colored(
                assembly_queue_.load(), 500, "Assembly (jobs)"),
            queue_gauge_colored(write_queue_.load(), 100, "Write (batches)"),
        });

        // Progress stats section
        auto stats_section = vbox({
            text("📊 PROGRESS STATS") | bold | color(Color::Cyan),
            separator(),
            hbox({
                text("Ledgers:     "),
                text(format_number(ledgers_processed_.load())) | bold,
            }),
            hbox({
                text("Inner nodes: "),
                text(format_number(inner_nodes_.load())) | bold,
            }),
            hbox({
                text("Leaf nodes:  "),
                text(format_number(leaf_nodes_.load())) | bold,
            }),
            hbox({
                text("Duplicates:  "),
                text(format_number(duplicates_.load())) | bold |
                    color(Color::Red),
            }),
        });

        // Throughput section with graphs
        auto ledgers_graph_fn = [&](int w, int h) {
            return throughput_graph(w, h, true);
        };
        auto nodes_graph_fn = [&](int w, int h) {
            return throughput_graph(w, h, false);
        };

        auto throughput_section = vbox({
            text("⚡ THROUGHPUT") | bold | color(Color::Cyan),
            separator(),
            hbox({
                text("Ledgers/sec: "),
                text(format_rate(ledgers_per_sec)) | bold |
                    color(Color::GreenLight),
            }),
            hbox({
                text("Nodes/sec:   "),
                text(format_rate(nodes_per_sec)) | bold |
                    color(Color::GreenLight),
            }),
            separator(),
            text("Ledgers/sec (last 60s)") | hcenter,
            graph(std::ref(ledgers_graph_fn)) | color(Color::Yellow) | flex,
            separator(),
            text("Nodes/sec (last 60s)") | hcenter,
            graph(std::ref(nodes_graph_fn)) | color(Color::BlueLight) | flex,
        });

        // RocksDB dedup stats (if using parallel dedupe)
        auto rocksdb_section = vbox({});
        if (rocks_fast_path_.load() > 0 || rocks_slow_path_.load() > 0)
        {
            uint64_t total_checks =
                rocks_fast_path_.load() + rocks_slow_path_.load();
            double fast_path_pct =
                total_checks > 0
                    ? (rocks_fast_path_.load() * 100.0 / total_checks)
                    : 0.0;

            rocksdb_section = vbox({
                text("🗄️  ROCKSDB DEDUP") | bold | color(Color::Cyan),
                separator(),
                hbox({
                    text("Fast path:   "),
                    text(format_number(rocks_fast_path_.load())) | bold,
                    text(" (" + format_rate(fast_path_pct) + "%)") |
                        color(Color::Green),
                }),
                hbox({
                    text("Slow path:   "),
                    text(format_number(rocks_slow_path_.load())) | bold,
                }),
                hbox({
                    text("False pos:   "),
                    text(format_number(rocks_false_positives_.load())) | bold |
                        color(Color::Yellow),
                }),
            });
        }

        // Layout: 2 columns
        return vbox({
                   text("CATL1-to-NUDB Pipeline Monitor") | bold | hcenter |
                       color(Color::MagentaLight),
                   separator(),
                   hbox({
                       vbox({
                           queues_section,
                           separator(),
                           stats_section,
                           separator(),
                           rocksdb_section,
                       }) | border |
                           size(WIDTH, EQUAL, 50),
                       separator(),
                       throughput_section | border | flex,
                   }) | flex,
                   separator(),
                   text("Press 'q' to quit") | hcenter | dim,
               }) |
               border;
    });

    // Add quit on 'q' key
    component |= CatchEvent([&](Event event) {
        if (event.is_character() && event.character() == "q")
        {
            screen.Exit();
            return true;
        }
        return false;
    });

    // Run loop at 10 FPS
    Loop loop(&screen, component);
    while (!loop.HasQuitted() && running_)
    {
        screen.RequestAnimationFrame();
        loop.RunOnce();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

}  // namespace catl::v1::utils::nudb
