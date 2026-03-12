#include <catl/peer/monitor/dashboard-format.h>
#include <catl/peer/monitor/peer-dashboard.h>
#include <cstdio>
#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>
#include <ftxui/screen/terminal.hpp>

namespace catl::peer::monitor {

using namespace ftxui;
using ftxui::color;

ftxui::Element
PeerDashboard::render_peers_tab_(RenderParams const& p)
{
    auto const& peers = p.all_peers;

    if (peers.empty())
    {
        return vbox({
                   text("No peers connected") | dim | hcenter,
               }) |
            flex;
    }

    auto render_peer_card = [&](Stats const& peer) -> Element {
        bool is_reconnecting =
            peer.reconnect_at.time_since_epoch().count() > 0 &&
            peer.reconnect_at > p.now;
        Color status_color = peer.connected
            ? Color::GreenLight
            : (is_reconnecting ? Color::Yellow : Color::Red);
        std::string status_icon =
            peer.connected ? "●" : (is_reconnecting ? "●" : "●");

        std::string state_label;
        if (peer.connection_state.rfind("Error:", 0) == 0)
            state_label = peer.connection_state;
        else if (peer.connected)
            state_label = "Connected";
        else if (is_reconnecting)
        {
            auto remaining =
                std::chrono::duration_cast<std::chrono::seconds>(
                    peer.reconnect_at - p.now)
                    .count();
            if (remaining < 0)
                remaining = 0;
            state_label = "Reconn " + std::to_string(remaining) + "s";
        }
        else
            state_label = "Disconnected";

        std::string last_pkt = "-";
        if (peer.last_packet_time.time_since_epoch().count() > 0)
        {
            auto age = std::chrono::duration_cast<std::chrono::seconds>(
                           p.now - peer.last_packet_time)
                           .count();
            if (age < 0)
                age = 0;
            last_pkt = std::to_string(age) + "s";
        }

        std::string addr = peer.peer_address;
        if (addr.size() > 24)
            addr = addr.substr(0, 24) + "...";

        // Fixed packet type slots for cross-peer comparison
        static const std::vector<std::string> fixed_types = {
            "mtPING",
            "mtMANIFESTS",
            "mtCLUSTER",
            "mtENDPOINTS",
            "mtTRANSACTION",
            "mtGET_LEDGER",
            "mtLEDGER_DATA",
            "mtPROPOSE_LEDGER",
            "mtSTATUS_CHANGE",
            "mtHAVE_SET",
            "mtVALIDATION",
            "mtGET_OBJECTS",
            "mtVALIDATORLIST",
            "mtSQUELCH",
            "mtVALIDATORLISTCOLLECTION",
            "mtPROOF_PATH_REQ",
            "mtPROOF_PATH_RESPONSE",
            "mtREPLAY_DELTA_REQ",
            "mtREPLAY_DELTA_RESPONSE",
            "mtHAVE_TRANSACTIONS",
            "mtTRANSACTIONS",
            "mtGET_PEER_SHARD_INFO_V2",
            "mtPEER_SHARD_INFO_V2",
        };

        Elements left_col, right_col;
        for (size_t i = 0; i < fixed_types.size(); ++i)
        {
            auto it = peer.packet_counts.find(fixed_types[i]);
            uint64_t count =
                it != peer.packet_counts.end() ? it->second : 0;
            char buf[48];
            std::snprintf(
                buf,
                sizeof(buf),
                "  %-26s %6s",
                fixed_types[i].c_str(),
                fmt::number(count).c_str());
            if (i % 2 == 0)
                left_col.push_back(text(buf) | dim);
            else
                right_col.push_back(text(buf) | dim);
        }

        Elements lines;
        lines.push_back(hbox({
            text(status_icon + " ") | color(status_color),
            text(peer.peer_id) | bold | color(status_color),
        }));
        lines.push_back(hbox({
            text("Addr: ") | dim,
            text(addr.empty() ? "(unknown)" : addr),
        }));
        lines.push_back(hbox({
            text("State: ") | dim,
            text(state_label) | color(status_color),
        }));
        lines.push_back(hbox({
            text("Uptime: ") | dim,
            text(
                peer.connected ? fmt::elapsed(peer.elapsed_seconds)
                               : std::string("--:--:--")) |
                bold,
            text("  Last: ") | dim,
            text(last_pkt),
        }));
        lines.push_back(hbox({
            text("Pkts: ") | dim,
            text(fmt::number(peer.total_packets)) | bold,
            text("  Bytes: ") | dim,
            text(fmt::bytes(static_cast<double>(peer.total_bytes))) |
                bold,
        }));
        lines.push_back(hbox({
            text("Rate: ") | dim,
            text(
                fmt::rate(peer.connected ? peer.packets_per_sec : 0.0) +
                "/s") |
                color(Color::GreenLight),
            text("  ") | dim,
            text(
                fmt::bytes(peer.connected ? peer.bytes_per_sec : 0.0) +
                "/s") |
                color(Color::Cyan),
        }));
        lines.push_back(text("Packet types:") | dim);
        lines.push_back(hbox({
            vbox(left_col),
            text(" "),
            vbox(right_col),
        }));

        return vbox(lines) | border;
    };

    Elements cards;
    for (auto const& peer : peers)
        cards.push_back(render_peer_card(peer));

    // Build grid with 3 columns
    const int columns = 3;
    auto term_size = Terminal::Size();
    int available_width = term_size.dimx - 4;
    int card_width =
        std::max(20, (available_width - (columns - 1)) / columns);

    Elements rows_el;
    for (size_t i = 0; i < cards.size(); i += columns)
    {
        Elements row_cells;
        for (int c = 0; c < columns; ++c)
        {
            size_t idx = i + c;
            if (idx < cards.size())
                row_cells.push_back(
                    cards[idx] | size(WIDTH, EQUAL, card_width));
            else
                row_cells.push_back(
                    filler() | size(WIDTH, EQUAL, card_width));
            if (c < columns - 1)
                row_cells.push_back(separator());
        }
        rows_el.push_back(hbox(row_cells));
    }

    std::string header =
        "PEERS (" + std::to_string(peers.size()) + ")";

    return vbox({
        text(header) | bold | color(Color::Cyan),
        separator(),
        vbox(rows_el) | flex | yframe,
    });
}

}  // namespace catl::peer::monitor
