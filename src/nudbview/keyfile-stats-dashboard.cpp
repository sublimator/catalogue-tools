#include <ftxui/screen/color.hpp>
#include <iomanip>
#include <nudbview/keyfile-stats-dashboard.h>
#include <sstream>

namespace nudbview {

using namespace ftxui;

KeyfileStatsDashboard::KeyfileStatsDashboard() = default;

KeyfileStatsDashboard::~KeyfileStatsDashboard()
{
    stop();
}

void
KeyfileStatsDashboard::start()
{
    if (running_.exchange(true))
    {
        return;  // Already running
    }

    ui_thread_ = std::make_unique<std::thread>([this] { run_ui(); });
}

void
KeyfileStatsDashboard::stop()
{
    running_ = false;

    if (ui_thread_ && ui_thread_->joinable())
    {
        ui_thread_->join();
        ui_thread_.reset();
    }
}

void
KeyfileStatsDashboard::update_stats(const Stats& stats)
{
    buckets_scanned_ = stats.buckets_scanned;
    total_buckets_ = stats.total_buckets;
    empty_buckets_ = stats.empty_buckets;
    full_buckets_ = stats.full_buckets;
    buckets_with_spills_ = stats.buckets_with_spills;
    total_entries_ = stats.total_entries;
    max_entries_in_bucket_ = stats.max_entries_in_bucket;
    total_collisions_ = stats.total_collisions;
    buckets_with_collisions_ = stats.buckets_with_collisions;
    capacity_per_bucket_ = stats.capacity_per_bucket;
    elapsed_sec_ = stats.elapsed_sec;
    buckets_per_sec_ = stats.buckets_per_sec;

    {
        std::lock_guard<std::mutex> lock(histogram_mutex_);
        entry_count_histogram_ = stats.entry_count_histogram;
        collision_count_histogram_ = stats.collision_count_histogram;
    }

    {
        std::lock_guard<std::mutex> lock(file_info_mutex_);
        key_file_path_ = stats.key_file_path;
        file_size_mb_ = stats.file_size_mb;
        block_size_ = stats.block_size;
        load_factor_ = stats.load_factor;
    }
}

KeyfileStatsDashboard::Stats
KeyfileStatsDashboard::get_stats() const
{
    Stats stats;
    stats.buckets_scanned = buckets_scanned_;
    stats.total_buckets = total_buckets_;
    stats.empty_buckets = empty_buckets_;
    stats.full_buckets = full_buckets_;
    stats.buckets_with_spills = buckets_with_spills_;
    stats.total_entries = total_entries_;
    stats.max_entries_in_bucket = max_entries_in_bucket_;
    stats.total_collisions = total_collisions_;
    stats.buckets_with_collisions = buckets_with_collisions_;
    stats.capacity_per_bucket = capacity_per_bucket_;
    stats.elapsed_sec = elapsed_sec_;
    stats.buckets_per_sec = buckets_per_sec_;

    {
        std::lock_guard<std::mutex> lock(histogram_mutex_);
        stats.entry_count_histogram = entry_count_histogram_;
        stats.collision_count_histogram = collision_count_histogram_;
    }

    {
        std::lock_guard<std::mutex> lock(file_info_mutex_);
        stats.key_file_path = key_file_path_;
        stats.file_size_mb = file_size_mb_;
        stats.block_size = block_size_;
        stats.load_factor = load_factor_;
    }

    return stats;
}

void
KeyfileStatsDashboard::run_ui()
{
    auto screen = ScreenInteractive::Fullscreen();

    // Helper: Format large numbers with commas
    auto format_number = [](uint64_t num) -> std::string {
        std::stringstream ss;
        ss.imbue(std::locale(""));
        ss << std::fixed << num;
        return ss.str();
    };

    // Helper: Format rate with 1 decimal
    auto format_rate = [](double rate) -> std::string {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(1) << rate;
        return ss.str();
    };

    // Helper: Format percentage
    auto format_percent = [](double pct) -> std::string {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(2) << pct << "%";
        return ss.str();
    };

    // Spinner animation
    static int spinner_frame = 0;
    auto get_spinner = [&]() -> std::string {
        const std::vector<std::string> frames = {
            "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"};
        spinner_frame = (spinner_frame + 1) % frames.size();
        return frames[spinner_frame];
    };

    auto component = Renderer([&]() -> Element {
        uint64_t scanned = buckets_scanned_.load();
        uint64_t total = total_buckets_.load();
        uint64_t empty = empty_buckets_.load();
        uint64_t full = full_buckets_.load();
        uint64_t with_spills = buckets_with_spills_.load();
        uint64_t entries = total_entries_.load();
        uint64_t max_entries = max_entries_in_bucket_.load();
        uint64_t collisions = total_collisions_.load();
        uint64_t collision_buckets = buckets_with_collisions_.load();
        uint64_t capacity = capacity_per_bucket_.load();
        double elapsed = elapsed_sec_.load();
        double rate = buckets_per_sec_.load();

        // Calculate progress
        float progress = total > 0 ? static_cast<float>(scanned) / total : 0.0f;
        bool scanning = scanned < total;

        // File info
        std::string file_path;
        uint64_t file_mb;
        std::size_t block_size;
        float load_factor;
        {
            std::lock_guard<std::mutex> lock(file_info_mutex_);
            file_path = key_file_path_;
            file_mb = file_size_mb_;
            block_size = block_size_;
            load_factor = load_factor_;
        }

        // Calculate stats
        double avg_entries =
            scanned > 0 ? static_cast<double>(entries) / scanned : 0.0;
        double utilization = capacity > 0 ? avg_entries / capacity : 0.0;
        double empty_pct =
            scanned > 0 ? (static_cast<double>(empty) / scanned) * 100.0 : 0.0;
        double collision_rate = entries > 0
            ? (static_cast<double>(collisions) / entries) * 100.0
            : 0.0;

        // Get histogram
        std::map<std::size_t, uint64_t> histogram;
        {
            std::lock_guard<std::mutex> lock(histogram_mutex_);
            histogram = entry_count_histogram_;
        }

        // File info section
        auto file_section = vbox({
            text("📁 KEY FILE INFO") | bold | color(Color::Cyan),
            separator(),
            hbox({
                text("Path: "),
                text(file_path) | bold,
            }),
            hbox({
                text("Size: "),
                text(format_number(file_mb) + " MB") | bold,
            }),
            hbox({
                text("Block size: "),
                text(format_number(block_size) + " bytes") | bold,
            }),
            hbox({
                text("Load factor: "),
                text(format_rate(load_factor)) | bold,
            }),
            hbox({
                text("Capacity/bucket: "),
                text(format_number(capacity) + " entries") | bold |
                    color(Color::Yellow),
            }),
        });

        // Progress section
        Color progress_color = scanning ? Color::GreenLight : Color::Cyan;
        std::string status =
            scanning ? get_spinner() + " Scanning..." : "✓ Complete";

        auto progress_section = vbox({
            text("⚡ SCAN PROGRESS") | bold | color(Color::Cyan),
            separator(),
            hbox({
                text("Status: "),
                text(status) | bold | color(progress_color),
            }),
            hbox({
                text("Buckets: "),
                text(format_number(scanned) + " / " + format_number(total)) |
                    bold,
            }),
            gauge(progress) | color(progress_color),
            hbox({
                text("Progress: "),
                text(format_percent(progress * 100.0)) | bold,
            }),
            hbox({
                text("Elapsed: "),
                text(format_rate(elapsed) + " sec") | bold,
            }),
            hbox({
                text("Rate: "),
                text(format_rate(rate) + " buckets/sec") | bold |
                    color(Color::GreenLight),
            }),
        });

        // Statistics section
        auto stats_section = vbox({
            text("📊 BUCKET STATISTICS") | bold | color(Color::Cyan),
            separator(),
            hbox({
                text("Total entries: "),
                text(format_number(entries)) | bold,
            }),
            hbox({
                text("Avg/bucket: "),
                text(format_rate(avg_entries)) | bold,
            }),
            hbox({
                text("Max in bucket: "),
                text(format_number(max_entries)) | bold | color(Color::Yellow),
            }),
            hbox({
                text("Utilization: "),
                text(format_percent(utilization * 100.0)) | bold |
                    color(Color::Magenta),
            }),
            separator(),
            hbox({
                text("Empty: "),
                text(
                    format_number(empty) + " (" + format_percent(empty_pct) +
                    ")") |
                    dim,
            }),
            hbox({
                text("Full: "),
                text(format_number(full)) | color(Color::Red),
            }),
            hbox({
                text("With spills: "),
                text(format_number(with_spills)) | color(Color::Yellow),
            }),
        });

        // Collision section
        auto collision_section = vbox({
            text("🔍 HASH COLLISIONS") | bold | color(Color::Cyan),
            separator(),
            hbox({
                text("Total: "),
                text(format_number(collisions)) | bold | color(Color::Red),
            }),
            hbox({
                text("Rate: "),
                text(format_percent(collision_rate)) | bold,
            }),
            hbox({
                text("Buckets affected: "),
                text(format_number(collision_buckets)) | bold,
            }),
        });

        // Histogram section (top 10 entry counts)
        Elements histogram_elements;
        histogram_elements.push_back(
            text("📈 ENTRY COUNT HISTOGRAM (Top 10)") | bold |
            color(Color::Cyan));
        histogram_elements.push_back(separator());

        if (histogram.empty())
        {
            histogram_elements.push_back(text("No data yet...") | dim);
        }
        else
        {
            // Find max count for scaling
            uint64_t max_count = 0;
            for (auto const& [entry_count, bucket_count] : histogram)
            {
                if (bucket_count > max_count)
                    max_count = bucket_count;
            }

            // Show top 10 by bucket count
            std::vector<std::pair<std::size_t, uint64_t>> sorted_histogram(
                histogram.begin(), histogram.end());
            std::sort(
                sorted_histogram.begin(),
                sorted_histogram.end(),
                [](auto const& a, auto const& b) {
                    return a.second > b.second;
                });

            int shown = 0;
            for (auto const& [entry_count, bucket_count] : sorted_histogram)
            {
                if (shown >= 10)
                    break;

                float bar_progress = max_count > 0
                    ? static_cast<float>(bucket_count) / max_count
                    : 0.0f;
                double pct = scanned > 0
                    ? (static_cast<double>(bucket_count) / scanned) * 100.0
                    : 0.0;

                histogram_elements.push_back(hbox({
                    text(std::to_string(entry_count) + " entries:") |
                        size(WIDTH, EQUAL, 15),
                    gauge(bar_progress) | flex | color(Color::Blue),
                    text(
                        " " + format_number(bucket_count) + " (" +
                        format_percent(pct) + ")") |
                        size(WIDTH, EQUAL, 20),
                }));
                ++shown;
            }
        }

        auto histogram_section = vbox(histogram_elements);

        // Layout
        return vbox({
                   text("NuDB Key File Analyzer") | bold | hcenter |
                       color(Color::MagentaLight),
                   separator(),
                   hbox({
                       vbox({
                           file_section,
                           separator(),
                           progress_section,
                           separator(),
                           stats_section,
                           separator(),
                           collision_section,
                       }) | border |
                           size(WIDTH, EQUAL, 50),
                       separator(),
                       histogram_section | border | flex,
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
        if (event.is_mouse())
        {
            return true;  // Ignore mouse events
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

    running_ = false;
}

}  // namespace nudbview
