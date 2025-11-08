#include "catl/utils-v1/nudb/pipeline-dashboard.h"
#include <ftxui/screen/color.hpp>
#include <cmath>
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
    // Signal the UI thread to stop
    running_ = false;

    // Always join if thread exists, regardless of running state
    if (ui_thread_ && ui_thread_->joinable())
    {
        ui_thread_->join();
        ui_thread_.reset();
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

    start_ledger_ = stats.start_ledger;
    end_ledger_ = stats.end_ledger;
    current_ledger_ = stats.current_ledger;
    ledgers_processed_ = stats.ledgers_processed;
    inner_nodes_ = stats.inner_nodes;
    leaf_nodes_ = stats.leaf_nodes;
    duplicates_ = stats.duplicates;

    {
        std::lock_guard<std::mutex> lock(status_mutex_);
        status_ = stats.status;
    }

    elapsed_sec_ = stats.elapsed_sec;
    ledgers_per_sec_ = stats.ledgers_per_sec;
    nodes_per_sec_ = stats.nodes_per_sec;
    catl_read_mb_per_sec_ = stats.catl_read_mb_per_sec;
    nudb_write_mb_per_sec_ = stats.nudb_write_mb_per_sec;
    bytes_read_ = stats.bytes_read;
    bytes_written_ = stats.bytes_written;
    bytes_uncompressed_ = stats.bytes_uncompressed;
    compression_ratio_ = stats.compression_ratio;

    state_added_ = stats.state_added;
    state_updated_ = stats.state_updated;
    state_deleted_ = stats.state_deleted;
    tx_added_ = stats.tx_added;

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

        // Keep only last 60 seconds of samples (based on timestamp)
        auto cutoff_time = now - std::chrono::seconds(60);
        while (!throughput_history_.empty() &&
               throughput_history_.front().timestamp < cutoff_time)
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

    stats.start_ledger = start_ledger_;
    stats.end_ledger = end_ledger_;
    stats.current_ledger = current_ledger_;
    stats.ledgers_processed = ledgers_processed_;
    stats.inner_nodes = inner_nodes_;
    stats.leaf_nodes = leaf_nodes_;
    stats.duplicates = duplicates_;

    {
        std::lock_guard<std::mutex> lock(status_mutex_);
        stats.status = status_;
    }

    stats.elapsed_sec = elapsed_sec_;
    stats.ledgers_per_sec = ledgers_per_sec_;
    stats.nodes_per_sec = nodes_per_sec_;
    stats.catl_read_mb_per_sec = catl_read_mb_per_sec_;
    stats.nudb_write_mb_per_sec = nudb_write_mb_per_sec_;
    stats.bytes_read = bytes_read_;
    stats.bytes_written = bytes_written_;
    stats.bytes_uncompressed = bytes_uncompressed_;
    stats.compression_ratio = compression_ratio_;

    stats.state_added = state_added_;
    stats.state_updated = state_updated_;
    stats.state_deleted = state_deleted_;
    stats.tx_added = tx_added_;

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

    // Helper: Format throughput with commas
    auto format_rate = [](double rate) -> std::string {
        // Round to 1 decimal place first
        double rounded = std::round(rate * 10.0) / 10.0;

        // Extract whole and fractional parts
        uint64_t whole = static_cast<uint64_t>(rounded);
        int decimal_digit = static_cast<int>((rounded - whole) * 10.0 + 0.5);

        // Format whole part with commas
        std::string whole_str;
        std::string num_str = std::to_string(whole);
        int len = num_str.length();
        for (int i = 0; i < len; i++)
        {
            if (i > 0 && (len - i) % 3 == 0)
                whole_str += ',';
            whole_str += num_str[i];
        }

        return whole_str + "." + std::to_string(decimal_digit);
    };

    // Helper: Queue gauge with color coding
    auto queue_gauge_colored = [](size_t depth,
                                  size_t max_depth,
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
    auto throughput_graph =
        [this](int width, int height, bool show_ledgers) -> std::vector<int> {
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
        size_t start_idx = rates.size() > static_cast<size_t>(width)
            ? rates.size() - width
            : 0;
        for (int i = 0; i < width && (start_idx + i) < rates.size(); ++i)
        {
            double normalized = rates[start_idx + i] / max_rate;
            output[i] = static_cast<int>(normalized * height);
        }

        return output;
    };

    // Helper: Format elapsed time as HH:MM:SS
    auto format_elapsed = [](double seconds) -> std::string {
        int total_sec = static_cast<int>(seconds);
        int hours = total_sec / 3600;
        int minutes = (total_sec % 3600) / 60;
        int secs = total_sec % 60;

        std::stringstream ss;
        ss << std::setfill('0') << std::setw(2) << hours << ":"
           << std::setfill('0') << std::setw(2) << minutes << ":"
           << std::setfill('0') << std::setw(2) << secs;
        return ss.str();
    };

    // Spinner animation state
    static int spinner_frame = 0;
    auto get_spinner = [&]() -> std::string {
        const std::vector<std::string> frames = {"|", "/", "-", "\\"};
        spinner_frame = (spinner_frame + 1) % frames.size();
        return frames[spinner_frame];
    };

    // Main render component
    auto component = Renderer([&]() -> Element {
        // Get current status
        std::string current_status;
        {
            std::lock_guard<std::mutex> lock(status_mutex_);
            current_status = status_;
        }

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
                    ledgers_per_sec = (throughput_history_.back().ledgers -
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
            queue_gauge_colored(hasher_queue_.load(), 500, "Hasher (ledgers)"),
            queue_gauge_colored(
                compression_queue_.load(), 500, "Compression (ledgers)"),
            queue_gauge_colored(dedupe_queue_.load(), 500, "Dedupe (jobs)"),
            queue_gauge_colored(assembly_queue_.load(), 500, "Assembly (jobs)"),
            queue_gauge_colored(write_queue_.load(), 100, "Write (batches)"),
        });

        // Progress stats section
        auto stats_section = vbox({
            text("📊 PROGRESS") | bold | color(Color::Cyan),
            separator(),
            hbox({
                text("Range:       "),
                text(
                    format_number(start_ledger_.load()) + " → " +
                    format_number(end_ledger_.load())) |
                    bold,
            }),
            hbox({
                text("Current:     "),
                text(format_number(current_ledger_.load())) | bold |
                    color(Color::GreenLight),
            }),
            hbox({
                text("Processed:   "),
                text(format_number(ledgers_processed_.load())) | bold,
            }),
            hbox({
                text("Elapsed:     "),
                text(format_elapsed(elapsed_sec_.load())) | bold |
                    color(Color::Yellow),
            }),
            separator(),
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
            separator(),
            text("💾 I/O (avg since start)") | bold | color(Color::Cyan),
            separator(),
            hbox({
                text("CATL read:   "),
                text(format_rate(catl_read_mb_per_sec_.load()) + " MB/s") |
                    bold | color(Color::GreenLight),
            }),
            hbox({
                text("NuDB write:  "),
                text(format_rate(nudb_write_mb_per_sec_.load()) + " MB/s") |
                    bold | color(Color::GreenLight),
            }),
            hbox({
                text("Compression: "),
                text(format_rate(compression_ratio_.load() * 100.0) + "%") |
                    bold | color(Color::Yellow),
                text(" saved"),
            }),
            separator(),
            text("🗺️  NODE OPERATIONS") | bold | color(Color::Cyan),
            separator(),
            hbox({
                text("State added: "),
                text(format_number(state_added_.load())) | bold,
            }),
            hbox({
                text("State upd:   "),
                text(format_number(state_updated_.load())) | bold,
            }),
            hbox({
                text("State del:   "),
                text(format_number(state_deleted_.load())) | bold,
            }),
            hbox({
                text("Tx added:    "),
                text(format_number(tx_added_.load())) | bold,
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
            text("Ledgers/sec:") | bold,
            hbox({
                text("  Total avg:  "),
                text(format_rate(ledgers_per_sec_.load())) | bold |
                    color(Color::Cyan),
            }),
            hbox({
                text("  Recent 60s: "),
                text(format_rate(ledgers_per_sec)) | bold |
                    color(Color::GreenLight),
            }),
            separator(),
            text("Nodes/sec (recent 60s):") | bold,
            hbox({
                text("  "),
                text(format_rate(nodes_per_sec)) | bold |
                    color(Color::GreenLight),
            }),
            separator(),
            text("Ledgers/sec (last 60s)") | hcenter | dim,
            graph(std::ref(ledgers_graph_fn)) | color(Color::Yellow) | flex,
            separator(),
            text("Nodes/sec (last 60s)") | hcenter | dim,
            graph(std::ref(nodes_graph_fn)) | color(Color::BlueLight) | flex,
        });

        // RocksDB dedup stats (if using parallel dedupe)
        auto rocksdb_section = vbox({});
        if (rocks_fast_path_.load() > 0 || rocks_slow_path_.load() > 0)
        {
            uint64_t total_checks =
                rocks_fast_path_.load() + rocks_slow_path_.load();
            double fast_path_pct = total_checks > 0
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

        // Status color coding and spinner
        Color status_color = Color::GreenLight;
        std::string status_display = current_status;

        if (current_status == "Draining")
            status_color = Color::Yellow;
        else if (current_status == "Rekeying")
        {
            status_color = Color::Magenta;
            status_display = current_status + " " + get_spinner();
        }
        else if (current_status == "Complete")
            status_color = Color::Cyan;

        // Layout: 2 columns
        return vbox({
                   text("CATL1-to-NUDB Pipeline Monitor") | bold | hcenter |
                       color(Color::MagentaLight),
                   separator(),
                   hbox({
                       text("Status: "),
                       text(status_display) | bold | color(status_color),
                   }) | hcenter,
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

    // Add quit on 'q' key and ignore mouse events
    component |= CatchEvent([&](Event event) {
        if (event.is_character() && event.character() == "q")
        {
            screen.Exit();
            return true;
        }
        // Ignore all mouse events to prevent stalling
        if (event.is_mouse())
        {
            return true;  // Consume the event but do nothing
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

    // Mark as stopped so demo loop knows we've exited
    running_ = false;
}

}  // namespace catl::v1::utils::nudb
