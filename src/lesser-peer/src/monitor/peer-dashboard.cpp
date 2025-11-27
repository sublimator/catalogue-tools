#include <catl/peer/monitor/peer-dashboard.h>
#include <cstdio>
#include <ftxui/component/component.hpp>
#include <ftxui/component/loop.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>
#include <iomanip>
#include <iostream>  // Required for std::cerr
#include <sstream>

namespace catl::peer::monitor {

using namespace ftxui;

PeerDashboard::PeerDashboard() = default;

void
PeerDashboard::restore_terminal()
{
    // Show cursor, exit alternate screen, disable all mouse modes
    std::fputs(
        "\033[?25h"     // Show cursor
        "\033[?1049l"   // Exit alternate screen
        "\033[?1000l"   // Disable basic mouse
        "\033[?1002l"   // Disable button-event mouse
        "\033[?1003l"   // Disable any-event mouse
        "\033[?1006l",  // Disable SGR mouse extension
        stdout);
    std::fflush(stdout);
}

PeerDashboard::~PeerDashboard()
{
    stop();
    // Ensure terminal is restored even if FTXUI didn't clean up properly
    restore_terminal();
}

void
PeerDashboard::start()
{
    if (running_.exchange(true))
    {
        return;  // Already running
    }

    ui_thread_ = std::make_unique<std::thread>([this] { run_ui(); });
}

void
PeerDashboard::stop()
{
    // Signal UI thread to exit cleanly
    exit_requested_ = true;
    running_ = false;

    if (ui_thread_ && ui_thread_->joinable())
    {
        ui_thread_->join();
        ui_thread_.reset();
    }
}

void
PeerDashboard::update_stats(const Stats& stats)
{
    // Update using default peer ID for backward compatibility
    update_peer_stats(default_peer_id_, stats);

    // Also update the deprecated fields for backward compatibility
    peer_address_ = stats.peer_address;
    peer_version_ = stats.peer_version;
    network_id_ = stats.network_id;
    protocol_version_ = stats.protocol_version;
    connected_ = stats.connected;

    {
        std::lock_guard<std::mutex> lock(packet_mutex_);
        packet_counts_ = stats.packet_counts;
        packet_bytes_ = stats.packet_bytes;
    }

    total_packets_ = stats.total_packets;
    total_bytes_ = stats.total_bytes;
    elapsed_seconds_ = stats.elapsed_seconds;

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        connection_state_ = stats.connection_state;
        last_packet_time_ = stats.last_packet_time;
    }

    // Update throughput history
    {
        std::lock_guard<std::mutex> lock(throughput_mutex_);
        auto now = std::chrono::steady_clock::now();

        throughput_history_.push_back(
            {now, stats.total_packets, stats.total_bytes});

        // Keep only last 60 seconds
        auto cutoff = now - std::chrono::seconds(60);
        while (!throughput_history_.empty() &&
               (throughput_history_.front().timestamp < cutoff ||
                throughput_history_.size() > 200))
        {
            throughput_history_.pop_front();
        }
    }
}

void
PeerDashboard::update_peer_stats(const std::string& peer_id, const Stats& stats)
{
    uint64_t total_packets = 0;
    uint64_t total_bytes = 0;

    {
        std::lock_guard<std::mutex> lock(peers_mutex_);

        // Create a copy of stats and ensure peer_id is set
        Stats updated_stats = stats;
        updated_stats.peer_id = peer_id;

        // Update or insert the peer stats
        peer_stats_[peer_id] = updated_stats;

        // Aggregate totals from all peers for throughput tracking
        for (const auto& [id, peer_stats] : peer_stats_)
        {
            total_packets += peer_stats.total_packets;
            total_bytes += peer_stats.total_bytes;
        }
    }

    // Update throughput history with aggregated totals
    {
        std::lock_guard<std::mutex> lock(throughput_mutex_);
        auto now = std::chrono::steady_clock::now();

        throughput_history_.push_back({now, total_packets, total_bytes});

        // Keep only last 60 seconds
        auto cutoff = now - std::chrono::seconds(60);
        while (!throughput_history_.empty() &&
               (throughput_history_.front().timestamp < cutoff ||
                throughput_history_.size() > 200))
        {
            throughput_history_.pop_front();
        }
    }
}

void
PeerDashboard::remove_peer(const std::string& peer_id)
{
    std::lock_guard<std::mutex> lock(peers_mutex_);
    peer_stats_.erase(peer_id);
}

std::vector<PeerDashboard::Stats>
PeerDashboard::get_all_peers_stats() const
{
    std::lock_guard<std::mutex> lock(peers_mutex_);
    std::vector<Stats> all_stats;

    for (const auto& [peer_id, stats] : peer_stats_)
    {
        all_stats.push_back(stats);
    }

    return all_stats;
}

void
PeerDashboard::update_ledger_info(
    uint32_t sequence,
    const std::string& hash,
    uint32_t validation_count)
{
    std::lock_guard<std::mutex> lock(ledger_mutex_);

    // Update if this is a newer ledger or has more validations
    if (sequence > current_validated_ledger_.sequence ||
        (sequence == current_validated_ledger_.sequence &&
         validation_count > current_validated_ledger_.validation_count))
    {
        current_validated_ledger_.sequence = sequence;
        current_validated_ledger_.hash = hash;
        current_validated_ledger_.validation_count = validation_count;
        current_validated_ledger_.last_update =
            std::chrono::steady_clock::now();
    }

    // Track validation counts for recent ledgers
    ledger_validations_[sequence] = validation_count;

    // Keep only last 10 ledgers
    while (ledger_validations_.size() > 10)
    {
        ledger_validations_.erase(ledger_validations_.begin());
    }
}

PeerDashboard::LedgerInfo
PeerDashboard::get_current_ledger() const
{
    std::lock_guard<std::mutex> lock(ledger_mutex_);
    return current_validated_ledger_;
}

void
PeerDashboard::update_available_endpoints(
    std::vector<std::string> const& endpoints)
{
    std::lock_guard<std::mutex> lock(endpoints_mutex_);
    available_endpoints_ = endpoints;
}

std::vector<std::string>
PeerDashboard::get_available_endpoints() const
{
    std::lock_guard<std::mutex> lock(endpoints_mutex_);
    return available_endpoints_;
}

PeerDashboard::Stats
PeerDashboard::get_stats() const
{
    Stats stats;
    stats.peer_address = peer_address_;
    stats.peer_version = peer_version_;
    stats.network_id = network_id_;
    stats.protocol_version = protocol_version_;
    stats.connected = connected_;

    {
        std::lock_guard<std::mutex> lock(packet_mutex_);
        stats.packet_counts = packet_counts_;
        stats.packet_bytes = packet_bytes_;
    }

    stats.total_packets = total_packets_;
    stats.total_bytes = total_bytes_;
    stats.elapsed_seconds = elapsed_seconds_;

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        stats.connection_state = connection_state_;
        stats.last_packet_time = last_packet_time_;
    }

    // Calculate recent throughput
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
                stats.packets_per_sec = (throughput_history_.back().packets -
                                         throughput_history_.front().packets) *
                    1000.0 / dt;
                stats.bytes_per_sec = (throughput_history_.back().bytes -
                                       throughput_history_.front().bytes) *
                    1000.0 / dt;
            }
        }
    }

    return stats;
}

void
PeerDashboard::request_exit()
{
    exit_requested_ = true;
}

void
PeerDashboard::run_ui()
{
    try
    {
        auto screen = ScreenInteractive::Fullscreen();
        screen.TrackMouse(false);  // Disable mouse capture

        // Helpers
        auto format_number = [](uint64_t num) -> std::string {
            std::stringstream ss;
            ss.imbue(std::locale(""));
            ss << std::fixed << num;
            return ss.str();
        };

        auto format_bytes = [](double bytes) -> std::string {
            const char* suffixes[] = {"B", "K", "M", "G", "T"};
            int i = 0;
            while (bytes > 1024 && i < 4)
            {
                bytes /= 1024;
                i++;
            }
            std::stringstream ss;
            ss << std::fixed << std::setprecision(2) << bytes << " "
               << suffixes[i];
            return ss.str();
        };

        auto format_rate = [](double rate) -> std::string {
            std::stringstream ss;
            ss << std::fixed << std::setprecision(1) << rate;
            return ss.str();
        };

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

        // Spinner animation
        static int spinner_frame = 0;
        auto get_spinner = [&]() -> std::string {
            const std::vector<std::string> frames = {
                "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"};
            spinner_frame = (spinner_frame + 1) % frames.size();
            return frames[spinner_frame];
        };

        // Create throughput graph
        auto throughput_graph = [this](
                                    int width, int height) -> std::vector<int> {
            std::lock_guard<std::mutex> lock(throughput_mutex_);
            std::vector<int> output(width, 0);

            if (throughput_history_.size() < 2)
                return output;

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
                    double rate = (throughput_history_[i].packets -
                                   throughput_history_[i - 1].packets) *
                        1000.0 / dt;
                    rates.push_back(rate);
                }
            }

            if (rates.empty())
                return output;

            // Find max rate for scaling
            double max_rate = 1.0;
            for (double rate : rates)
            {
                if (rate > max_rate)
                    max_rate = rate;
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

        auto component = Renderer([&]() -> Element {
            ui_render_counter_++;  // Increment heartbeat counter for UI thread
            // Get all peer stats
            auto all_peers = get_all_peers_stats();

            // Get current validated ledger
            auto current_ledger = get_current_ledger();

            // For backward compatibility, get first peer as primary
            bool is_connected = false;
            std::string state = "No peers";
            std::string primary_address;
            std::string primary_version;
            std::string primary_protocol;
            std::string primary_network_id;
            std::chrono::steady_clock::time_point last_packet;

            if (!all_peers.empty())
            {
                const auto& primary_peer = all_peers[0];
                is_connected = primary_peer.connected;
                state = primary_peer.connection_state;
                last_packet = primary_peer.last_packet_time;
                primary_address = primary_peer.peer_address;
                primary_version = primary_peer.peer_version;
                primary_protocol = primary_peer.protocol_version;
                primary_network_id = primary_peer.network_id;
            }

            // Connection status color
            Color status_color = is_connected ? Color::GreenLight : Color::Red;
            std::string status_icon = is_connected ? "🟢" : "🔴";

            // Check if primary peer is receiving data
            auto now = std::chrono::steady_clock::now();
            bool receiving = false;
            if (!all_peers.empty())
            {
                auto since_last =
                    std::chrono::duration_cast<std::chrono::seconds>(
                        now - all_peers[0].last_packet_time)
                        .count();
                receiving = all_peers[0].connected && since_last < 5;
            }
            std::string activity =
                receiving ? get_spinner() + " Receiving" : "Idle";

            // Ledger info section
            Elements ledger_elements;
            ledger_elements.push_back(
                text("📜 VALIDATED LEDGER") | bold | color(Color::Cyan));
            ledger_elements.push_back(separator());

            if (current_ledger.sequence > 0)
            {
                ledger_elements.push_back(hbox({
                    text("Sequence: "),
                    text(std::to_string(current_ledger.sequence)) | bold |
                        color(Color::Yellow),
                }));

                if (!current_ledger.hash.empty())
                {
                    ledger_elements.push_back(hbox({
                        text("Hash: "),
                        text(current_ledger.hash.substr(0, 16) + "...") | bold,
                    }));
                }

                ledger_elements.push_back(hbox({
                    text("Validations: "),
                    text(std::to_string(current_ledger.validation_count)) |
                        bold | color(Color::GreenLight),
                }));

                auto ledger_age =
                    std::chrono::duration_cast<std::chrono::seconds>(
                        now - current_ledger.last_update)
                        .count();
                ledger_elements.push_back(hbox({
                    text("Last update: "),
                    text(std::to_string(ledger_age) + "s ago") | dim,
                }));
            }
            else
            {
                ledger_elements.push_back(
                    text("No validated ledgers yet") | dim);
            }

            auto ledger_section = vbox(ledger_elements);

            // Multiple peers section
            Elements peers_elements;
            peers_elements.push_back(
                text(
                    "👥 CONNECTED PEERS (" + std::to_string(all_peers.size()) +
                    ")") |
                bold | color(Color::Cyan));
            peers_elements.push_back(separator());

            if (all_peers.empty())
            {
                peers_elements.push_back(text("No connected peers") | dim);
            }
            else
            {
                for (size_t i = 0; i < all_peers.size() && i < 5;
                     ++i)  // Show max 5 peers
                {
                    const auto& peer = all_peers[i];
                    Color peer_status_color =
                        peer.connected ? Color::GreenLight : Color::Red;
                    std::string peer_num = std::to_string(i + 1) + ".";
                    std::string status_icon = peer.connected ? "●" : "○";

                    // Check if this peer is actively receiving
                    auto peer_since_last =
                        std::chrono::duration_cast<std::chrono::seconds>(
                            now - peer.last_packet_time)
                            .count();
                    bool peer_receiving = peer.connected && peer_since_last < 3;
                    std::string activity_icon =
                        peer_receiving ? get_spinner() : " ";

                    peers_elements.push_back(hbox({
                        text(peer_num) | size(WIDTH, EQUAL, 3) | dim,
                        text(status_icon) | color(peer_status_color),
                        text(activity_icon) | color(Color::Cyan),
                        text(" " + peer.peer_address) | bold,
                        text(" | "),
                        text(format_number(peer.total_packets)) | dim,
                        text(" pkts | "),
                        text(format_bytes(
                            static_cast<double>(peer.total_bytes))) |
                            dim,
                    }));
                }

                if (all_peers.size() > 5)
                {
                    peers_elements.push_back(
                        text(
                            "... and " + std::to_string(all_peers.size() - 5) +
                            " more") |
                        dim);
                }
            }

            auto peers_section = vbox(peers_elements);

            // Connection info section (for primary peer)
            auto connection_section = vbox({
                text("🌐 PRIMARY PEER (1)") | bold | color(Color::Cyan),
                separator(),
                hbox({
                    text("Status: "),
                    text(status_icon + " " + state) | bold |
                        color(status_color),
                }),
                hbox({
                    text("Peer: "),
                    text(primary_address) | bold,
                }),
                hbox({
                    text("Version: "),
                    text(primary_version) | bold | color(Color::Yellow),
                }),
                hbox({
                    text("Protocol: "),
                    text(primary_protocol) | bold,
                }),
                hbox({
                    text("Network ID: "),
                    text(
                        primary_network_id.empty() ? "none"
                                                   : primary_network_id) |
                        bold,
                }),
                hbox({
                    text("Activity: "),
                    text(activity) | bold |
                        color(receiving ? Color::GreenLight : Color::GrayDark),
                }),
            });

            // Overall stats section - aggregate from all peers
            uint64_t total_pkts = 0;
            uint64_t total_b = 0;
            double elapsed = 0;
            for (const auto& peer : all_peers)
            {
                total_pkts += peer.total_packets;
                total_b += peer.total_bytes;
                if (peer.elapsed_seconds > elapsed)
                    elapsed = peer.elapsed_seconds;
            }

            // Calculate current throughput
            double pps = 0.0, bps = 0.0;
            {
                std::lock_guard<std::mutex> lock(throughput_mutex_);
                if (throughput_history_.size() >= 2)
                {
                    auto dt =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            throughput_history_.back().timestamp -
                            throughput_history_.front().timestamp)
                            .count();

                    if (dt > 0)
                    {
                        pps = (throughput_history_.back().packets -
                               throughput_history_.front().packets) *
                            1000.0 / dt;
                        bps = (throughput_history_.back().bytes -
                               throughput_history_.front().bytes) *
                            1000.0 / dt;
                    }
                }
            }

            auto stats_section = vbox({
                text("📊 STATISTICS") | bold | color(Color::Cyan),
                separator(),
                hbox({
                    text("Uptime: "),
                    text(format_elapsed(elapsed)) | bold | color(Color::Yellow),
                }),
                hbox({
                    text("Total packets: "),
                    text(format_number(total_pkts)) | bold,
                }),
                hbox({
                    text("Total data: "),
                    text(format_bytes(static_cast<double>(total_b))) | bold,
                }),
                separator(),
                text("Current Throughput") | bold,
                hbox({
                    text("  Packets/sec: "),
                    text(format_rate(pps)) | bold | color(Color::GreenLight),
                }),
                hbox({
                    text("  Data rate: "),
                    text(format_bytes(bps) + "/s") | bold |
                        color(Color::GreenLight),
                }),
                separator(),
                text("Average (since start)") | bold,
                hbox({
                    text("  Packets/sec: "),
                    text(format_rate(elapsed > 0 ? total_pkts / elapsed : 0)) |
                        bold,
                }),
                hbox({
                    text("  Data rate: "),
                    text(
                        format_bytes(elapsed > 0 ? total_b / elapsed : 0) +
                        "/s") |
                        bold,
                }),
            });

            // Packet types section (top 10)
            Elements packet_elements;
            packet_elements.push_back(
                text("📦 PACKET TYPES") | bold | color(Color::Cyan));
            packet_elements.push_back(separator());

            // Aggregate packet counts from all peers
            std::map<std::string, uint64_t> counts;
            std::map<std::string, uint64_t> bytes;
            for (const auto& peer : all_peers)
            {
                for (const auto& [type, count] : peer.packet_counts)
                {
                    counts[type] += count;
                }
                for (const auto& [type, b] : peer.packet_bytes)
                {
                    bytes[type] += b;
                }
            }

            // Sort by count
            std::vector<std::pair<std::string, uint64_t>> sorted_packets(
                counts.begin(), counts.end());
            std::sort(
                sorted_packets.begin(),
                sorted_packets.end(),
                [](const auto& a, const auto& b) {
                    return a.second > b.second;
                });

            // Display top 10
            int shown = 0;
            for (const auto& [type, count] : sorted_packets)
            {
                if (shown >= 10)
                    break;

                double pct =
                    total_pkts > 0 ? (count * 100.0 / total_pkts) : 0.0;
                double rate = elapsed > 0 ? count / elapsed : 0.0;
                uint64_t type_bytes = bytes[type];

                packet_elements.push_back(hbox({
                    text(type) | size(WIDTH, EQUAL, 26) | color(Color::Yellow),
                    text(format_number(count)) | size(WIDTH, EQUAL, 12),
                    text(format_rate(rate) + "/s") | size(WIDTH, EQUAL, 10),
                    text(format_bytes(static_cast<double>(type_bytes))) |
                        size(WIDTH, EQUAL, 10),
                    gauge(pct / 100.0f) | flex | color(Color::Blue),
                    text(" " + format_rate(pct) + "%") | size(WIDTH, EQUAL, 8),
                }));
                shown++;
            }

            auto packet_section = vbox(packet_elements);

            // Throughput graph
            auto graph_fn = [&](int w, int h) {
                return throughput_graph(w, h);
            };

            auto throughput_section = vbox({
                text("📈 PACKET THROUGHPUT (last 60s)") | bold |
                    color(Color::Cyan),
                separator(),
                graph(std::ref(graph_fn)) | color(Color::GreenLight) | flex,
            });

            // Discovered endpoints section (bottom right)
            auto available_eps = get_available_endpoints();
            Elements endpoint_elements;
            endpoint_elements.push_back(
                text("🌍 DISCOVERED PEERS") | bold | color(Color::Cyan));
            endpoint_elements.push_back(separator());
            if (available_eps.empty())
            {
                endpoint_elements.push_back(text("None yet") | dim);
            }
            else
            {
                int shown = 0;
                for (auto const& ep : available_eps)
                {
                    if (shown >= 10)
                        break;
                    endpoint_elements.push_back(text("• " + ep));
                    shown++;
                }
                if (static_cast<int>(available_eps.size()) > 10)
                {
                    endpoint_elements.push_back(
                        text(
                            "... and " +
                            std::to_string(available_eps.size() - 10) +
                            " more") |
                        dim);
                }
            }
            auto endpoints_section = vbox(endpoint_elements);

            // Layout
            return vbox({
                       text("XRPL Peer Monitor Dashboard") | bold | hcenter |
                           color(Color::MagentaLight),
                       separator(),
                       hbox({
                           vbox({
                               ledger_section,
                               separator(),
                               peers_section,
                               separator(),
                               connection_section,
                               separator(),
                               stats_section,
                           }) | border |
                               flex,
                           separator(),
                           vbox({
                               packet_section,
                               separator(),
                               throughput_section,
                               separator(),
                               endpoints_section,
                           }) | border |
                               flex,
                       }) | flex,
                       separator(),
                       text("Press 'q' to quit | 'c' to clear stats") |
                           hcenter | dim,
                   }) |
                border;
        });

        // Handle keyboard events
        component |= CatchEvent([&](Event event) {
            if (event.is_character())
            {
                if (event.character() == "q")
                {
                    screen.Exit();
                    // Signal monitor to shut down
                    if (shutdown_callback_)
                    {
                        shutdown_callback_();
                    }
                    return true;
                }
                else if (event.character() == "c")
                {
                    // Clear stats
                    total_packets_ = 0;
                    total_bytes_ = 0;
                    {
                        std::lock_guard<std::mutex> lock(packet_mutex_);
                        packet_counts_.clear();
                        packet_bytes_.clear();
                    }
                    {
                        std::lock_guard<std::mutex> lock(throughput_mutex_);
                        throughput_history_.clear();
                    }
                    return true;
                }
            }
            // Ignore mouse events
            if (event.is_mouse())
            {
                return true;
            }
            return false;
        });

        // Run loop at 10 FPS
        Loop loop(&screen, component);
        while (!loop.HasQuitted() && running_ && !exit_requested_)
        {
            screen.RequestAnimationFrame();
            loop.RunOnce();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        // Ensure terminal is restored by calling Exit() if we fell out
        // due to running_=false or exit_requested_ (not user 'q' key)
        if (!loop.HasQuitted())
        {
            screen.Exit();
        }

        running_ = false;
        exit_requested_ = false;

        // Belt-and-suspenders: restore terminal after FTXUI cleanup
        restore_terminal();
    }
    catch (std::exception const& e)
    {
        // Restore terminal before logging - screen may be in bad state
        restore_terminal();
        std::cerr << "CRITICAL DASHBOARD ERROR: " << e.what() << std::endl;
        running_ = false;
    }
    catch (...)
    {
        restore_terminal();
        std::cerr << "CRITICAL DASHBOARD ERROR: Unknown exception" << std::endl;
        running_ = false;
    }
}

}  // namespace catl::peer::monitor
