#include <catl/peer/monitor/peer-commands-tab.h>

#include "ripple.pb.h"
#include <catl/core/logger.h>
#include <ftxui/component/component.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>
#include <sstream>

namespace catl::peer::monitor {

using namespace ftxui;
using ftxui::color;

PeerCommandsTab::PeerCommandsTab()
{
    // Start with an empty peers container
    peers_container_ = Container::Vertical({});

    // Buttons
    auto send_btn = Button("Ping", [this] { send_ping(); });
    auto invert_btn = Button("Invert", [this] { invert_selection(); });
    auto all_btn = Button("All", [this] { select_all(); });
    auto none_btn = Button("None", [this] { select_none(); });

    // Left side: peers + selection buttons
    auto left_buttons = Container::Horizontal({invert_btn, all_btn, none_btn});

    auto left_side = Container::Vertical({
        peers_container_,
        left_buttons,
    });

    // Right side: send button only
    auto right_side = Container::Vertical({
        send_btn,
    });

    root_component_ = Container::Horizontal({
        left_side,
        right_side,
    });
}

ftxui::Component
PeerCommandsTab::component()
{
    return root_component_;
}

ftxui::Element
PeerCommandsTab::render()
{
    std::lock_guard<std::mutex> lock(mutex_);

    // --- Left column: peer checkboxes + selection buttons ---
    auto peer_checkboxes = peers_container_->Render();

    Element peers_section;
    if (peer_ids_.empty())
    {
        peers_section = text("No peers") | dim;
    }
    else
    {
        peers_section = peer_checkboxes | flex;
    }

    // Render selection buttons from component tree
    // root_component_ children: [left_side, right_side]
    // left_side children: [peers_container_, left_buttons]
    auto& left_side = root_component_->ChildAt(0);
    auto& left_buttons = left_side->ChildAt(1);

    auto left_col = vbox({
        text("PEERS") | bold | color(Color::Cyan),
        separator(),
        peers_section | flex,
        separator(),
        left_buttons->Render(),
    });

    // --- Right column: ping button + log ---
    auto& right_side = root_component_->ChildAt(1);

    auto right_top = vbox({
        text("COMMAND") | bold | color(Color::Cyan),
        separator(),
        right_side->Render(),
    });

    // Log
    Elements log_lines;
    for (auto const& entry : command_log_)
    {
        log_lines.push_back(text(entry));
    }
    if (log_lines.empty())
    {
        log_lines.push_back(text("No commands sent yet") | dim);
    }

    auto right_bottom = vbox({
        text("LOG") | bold | color(Color::Cyan),
        separator(),
        vbox(log_lines) | flex | yframe,
    });

    auto right_col = vbox({
        right_top,
        separator(),
        right_bottom | flex,
    });

    return hbox({
               left_col | size(WIDTH, EQUAL, 35) | border,
               separator(),
               right_col | flex | border,
           }) |
        flex;
}

void
PeerCommandsTab::update_peers(
    std::vector<std::pair<std::string, bool>> const& peers)
{
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if the peer list actually changed (avoid needless rebuild)
    bool changed = (peers.size() != peer_ids_.size());
    if (!changed)
    {
        for (size_t i = 0; i < peers.size(); ++i)
        {
            if (peers[i].first != peer_ids_[i] ||
                peers[i].second != peer_connected_[i])
            {
                changed = true;
                break;
            }
        }
    }

    if (!changed)
        return;

    // Save old selection state
    std::map<std::string, bool> old_selection;
    for (size_t i = 0; i < peer_ids_.size(); ++i)
    {
        old_selection[peer_ids_[i]] = peer_selected_[i];
    }

    // Detach old checkboxes BEFORE clearing the deques they point into
    peers_container_->DetachAllChildren();

    peer_ids_.clear();
    peer_connected_.clear();
    peer_selected_.clear();
    peer_labels_.clear();

    for (auto const& [id, connected] : peers)
    {
        peer_ids_.push_back(id);
        peer_connected_.push_back(connected);
        auto it = old_selection.find(id);
        peer_selected_.push_back(
            (it != old_selection.end()) ? it->second : false);
        peer_labels_.push_back(
            id + (connected ? " (Connected)" : " (Reconnect)"));
    }

    // Add new checkboxes pointing at the fresh deque storage
    for (size_t i = 0; i < peer_labels_.size(); ++i)
    {
        peers_container_->Add(Checkbox(&peer_labels_[i], &peer_selected_[i]));
    }
}

void
PeerCommandsTab::invert_selection()
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& sel : peer_selected_)
    {
        sel = !sel;
    }
}

void
PeerCommandsTab::select_all()
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& sel : peer_selected_)
    {
        sel = true;
    }
}

void
PeerCommandsTab::select_none()
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& sel : peer_selected_)
    {
        sel = false;
    }
}

void
PeerCommandsTab::send_ping()
{
    if (!send_callback_)
        return;

    // Gather selected peers under lock
    std::vector<std::string> targets;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (size_t i = 0; i < peer_ids_.size(); ++i)
        {
            if (peer_selected_[i] && peer_connected_[i])
                targets.push_back(peer_ids_[i]);
        }
    }

    if (targets.empty())
    {
        std::lock_guard<std::mutex> lock(mutex_);
        command_log_.push_back("No connected peers selected");
        if (command_log_.size() > MAX_LOG_LINES)
            command_log_.pop_front();
        return;
    }

    static std::atomic<uint32_t> ping_seq{1000};  // start high to avoid
                                                  // colliding with heartbeat
    auto now = std::chrono::steady_clock::now();

    for (auto const& peer_id : targets)
    {
        uint32_t seq = ping_seq++;

        protocol::TMPing ping;
        ping.set_type(protocol::TMPing_pingType_ptPING);
        ping.set_seq(seq);
        std::vector<std::uint8_t> payload(ping.ByteSizeLong());
        ping.SerializeToArray(payload.data(), payload.size());

        send_callback_(peer_id, packet_type::ping, payload);

        std::lock_guard<std::mutex> lock(mutex_);
        pending_pings_[seq] = {peer_id, now};
        command_log_.push_back(
            "Ping seq=" + std::to_string(seq) + " -> " + peer_id);
        if (command_log_.size() > MAX_LOG_LINES)
            command_log_.pop_front();
    }

    // Prune old pending pings (>30s)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto cutoff = now - std::chrono::seconds(30);
        for (auto it = pending_pings_.begin(); it != pending_pings_.end();)
        {
            if (it->second.second < cutoff)
                it = pending_pings_.erase(it);
            else
                ++it;
        }
    }
}

void
PeerCommandsTab::record_pong(std::string const& peer_id, uint32_t seq)
{
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = pending_pings_.find(seq);
    if (it == pending_pings_.end())
        return;  // not one of ours (heartbeat pong etc.)

    auto rtt = std::chrono::steady_clock::now() - it->second.second;
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(rtt).count();
    pending_pings_.erase(it);

    std::ostringstream oss;
    oss << "Pong seq=" << seq << " <- " << peer_id << " (" << ms << "ms)";
    command_log_.push_back(oss.str());
    if (command_log_.size() > MAX_LOG_LINES)
        command_log_.pop_front();
}

}  // namespace catl::peer::monitor
