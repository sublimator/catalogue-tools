#include <catl/core/logger.h>
#include <catl/peer/monitor/peer-dashboard.h>
#include <ftxui/component/component.hpp>
#include <ftxui/component/loop.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>

namespace catl::peer::monitor {

using namespace ftxui;
using ftxui::color;  // Prefer ftxui::color over catl::color (from logger.h)

static LogPartition&
dashboard_log()
{
    static LogPartition partition("DASHBOARD", LogLevel::WARNING);
    return partition;
}

PeerDashboard::PeerDashboard() = default;

void
PeerDashboard::load_protocol(
    std::string const& definitions_path,
    uint32_t network_id)
{
    xdata::ProtocolOptions opts;
    if (network_id > 0)
        opts.network_id = network_id;

    if (!definitions_path.empty())
    {
        // Load from file - let it throw if it fails
        protocol_ = xdata::Protocol::load_from_file(definitions_path, opts);
        protocol_source_ = definitions_path;
    }
    else
    {
        protocol_ = xdata::Protocol::load_embedded_xahau_protocol(opts);
        protocol_source_ = "embedded-xahau";
    }
}

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

    // Only join if we're not being called from the UI thread itself
    if (ui_thread_ && ui_thread_->joinable() &&
        ui_thread_->get_id() != std::this_thread::get_id())
    {
        ui_thread_->join();
        ui_thread_.reset();
    }
}

void
PeerDashboard::run_ui()
{
    try
    {
        auto screen = ScreenInteractive::Fullscreen();
        screen.TrackMouse(false);  // Disable mouse capture


        auto commands_component = commands_tab_.component();
        auto component = Renderer(commands_component, [&]() -> Element {
            auto frame_start = std::chrono::steady_clock::now();
            ui_render_counter_++;  // Increment heartbeat counter for UI thread
            // Get all peer stats
            auto all_peers = get_all_peers_stats();
            auto after_stats = std::chrono::steady_clock::now();

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

            // Get current time for all time-based checks
            auto now = std::chrono::steady_clock::now();

            RenderParams p{
                all_peers,
                now,
                current_ledger,
                is_connected,
                state,
                primary_address,
                primary_version,
                primary_protocol,
                primary_network_id,
                last_packet,
            };


            // Tab bar
            int tab = current_tab_.load();
            auto tab_bar = hbox({
                text(" [1] Main ") |
                    (tab == 0 ? bold | bgcolor(Color::Blue) : dim),
                text(" "),
                text(" [2] Proposals ") |
                    (tab == 1 ? bold | bgcolor(Color::Blue) : dim),
                text(" "),
                text(" [3] Peers ") |
                    (tab == 2 ? bold | bgcolor(Color::Blue) : dim),
                text(" "),
                text(" [4] Commands ") |
                    (tab == 3 ? bold | bgcolor(Color::Blue) : dim),
                filler(),
            });

            // Proposals tab - rendered by render_proposals_tab_()

            // Peers tab - grid view for per-peer stats
            // Only render the active tab's content
            static const char* tab_names[] = {
                "Main", "Proposals", "Peers", "Commands"};
            auto render_start = std::chrono::steady_clock::now();

            Element content;
            switch (tab)
            {
                case 0:
                    content = render_main_tab_(p);
                    break;
                case 1:
                    content = render_proposals_tab_(p) | flex;
                    break;
                case 2:
                    content = render_peers_tab_(p) | flex;
                    break;
                case 3: {
                    std::vector<std::pair<std::string, bool>> peer_list;
                    for (auto const& peer : all_peers)
                    {
                        peer_list.emplace_back(peer.peer_id, peer.connected);
                    }
                    commands_tab_.update_peers(peer_list);
                    content = commands_tab_.render();
                    break;
                }
                default:
                    content = render_main_tab_(p);
                    break;
            }

            auto render_end = std::chrono::steady_clock::now();
            auto stats_us =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    after_stats - frame_start)
                    .count();
            auto tab_us =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    render_end - render_start)
                    .count();
            auto total_us =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    render_end - frame_start)
                    .count();
            PLOGD(
                dashboard_log(),
                "render [",
                tab_names[tab < 4 ? tab : 0],
                "] stats=",
                stats_us,
                "us tab=",
                tab_us,
                "us total=",
                total_us,
                "us");

            // Layout - use explicit 50/50 width for main columns
            return vbox({
                       text("XRPL Peer Monitor Dashboard") | bold | hcenter |
                           color(Color::MagentaLight),
                       tab_bar,
                       separator(),
                       content | flex,
                       separator(),
                       text("'1'-'4' tabs | SPACE pause | 'q' quit | 'c' "
                            "clear") |
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
                    // Just exit the screen - the loop will exit and
                    // the monitor will detect shutdown via the callback
                    // after the UI thread finishes cleanly
                    exit_requested_ = true;
                    screen.Exit();
                    return true;
                }
                else if (event.character() == "c")
                {
                    // Clear all stats and consensus state
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
                    {
                        std::lock_guard<std::mutex> lock(peers_mutex_);
                        for (auto& [id, stats] : peer_stats_)
                        {
                            stats.total_packets = 0;
                            stats.total_bytes = 0;
                            stats.elapsed_seconds = 0;
                            stats.packets_per_sec = 0;
                            stats.bytes_per_sec = 0;
                            stats.packet_counts.clear();
                            stats.packet_bytes.clear();
                        }
                    }
                    {
                        std::lock_guard<std::mutex> lock(
                            per_peer_throughput_mutex_);
                        per_peer_throughput_.clear();
                    }
                    {
                        std::lock_guard<std::mutex> lock(consensus_mutex_);
                        reset_consensus_state();
                    }
                    return true;
                }
                else if (event.character() == "1")
                {
                    current_tab_ = 0;  // Main tab
                    return true;
                }
                else if (event.character() == "2")
                {
                    current_tab_ = 1;  // Proposals tab
                    return true;
                }
                else if (event.character() == "3")
                {
                    current_tab_ = 2;  // Peers tab
                    return true;
                }
                else if (event.character() == "4")
                {
                    current_tab_ = 3;  // Commands tab
                    return true;
                }
                else if (event.character() == " ")
                {
                    // Toggle pause on proposals tab
                    if (current_tab_ == 1)
                    {
                        bool was_paused =
                            proposals_paused_.exchange(!proposals_paused_);
                        if (!was_paused)
                        {
                            // Just paused - capture current and previous
                            // round's prev_hash
                            std::lock_guard<std::mutex> lock(consensus_mutex_);
                            std::lock_guard<std::mutex> plock(pause_mutex_);
                            // Find newest and second-newest rounds
                            const LedgerProposals* newest = nullptr;
                            const LedgerProposals* second = nullptr;
                            for (auto const& [hash, props] : proposal_rounds_)
                            {
                                if (!newest ||
                                    props.first_proposal >
                                        newest->first_proposal)
                                {
                                    second = newest;
                                    newest = &props;
                                }
                                else if (
                                    !second ||
                                    props.first_proposal >
                                        second->first_proposal)
                                {
                                    second = &props;
                                }
                            }
                            if (newest)
                                paused_prev_hash_ = newest->prev_ledger_hash;
                            if (second)
                                paused_last_prev_hash_ =
                                    second->prev_ledger_hash;
                        }
                    }
                    return true;
                }
            }
            // On Commands tab, let unhandled events propagate to interactive
            // components (checkboxes, radiobox, buttons)
            if (current_tab_.load() == 3)
            {
                return false;
            }

            // On other tabs, consume character events to prevent accidental
            // interaction with the commands component
            if (event.is_character())
            {
                return true;
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

        // Belt-and-suspenders: restore terminal after FTXUI cleanup
        restore_terminal();

        // If user requested exit (pressed 'q'), notify the monitor to shut down
        // Do this AFTER the loop exits to avoid deadlock
        if (exit_requested_ && shutdown_callback_)
        {
            shutdown_callback_();
        }
        exit_requested_ = false;
    }
    catch (...)
    {
        // Restore terminal on any error
        restore_terminal();
        running_ = false;
    }
}

}  // namespace catl::peer::monitor
