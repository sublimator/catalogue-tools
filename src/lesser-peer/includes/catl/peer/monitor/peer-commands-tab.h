#pragma once

#include <catl/peer/types.h>
#include <chrono>
#include <deque>
#include <ftxui/component/component.hpp>
#include <ftxui/dom/elements.hpp>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace catl::peer::monitor {

// Callback: (peer_id, packet_type, serialized_payload)
using send_callback_t =
    std::function<void(std::string, packet_type, std::vector<std::uint8_t>)>;

class PeerCommandsTab
{
public:
    PeerCommandsTab();

    // Set the callback used to send packets through the monitor
    void
    set_send_callback(send_callback_t cb)
    {
        send_callback_ = std::move(cb);
    }

    // Return the top-level FTXUI component (receives focus/events)
    ftxui::Component
    component();

    // Render the tab layout (called from the dashboard Renderer)
    ftxui::Element
    render();

    // Reconcile peer list from dashboard stats, preserving selection state
    void
    update_peers(
        std::vector<std::pair<std::string, bool>> const& peers);  // (id,
                                                                  // connected)

    // Selection helpers
    void
    invert_selection();
    void
    select_all();
    void
    select_none();

    // Send ping to all selected peers
    void
    send_ping();

    // Called when a pong is received — logs RTT
    void
    record_pong(std::string const& peer_id, uint32_t seq);

private:
    std::mutex mutex_;

    // Peer data — parallel vectors for FTXUI binding
    std::vector<std::string> peer_ids_;
    std::deque<bool> peer_connected_;
    std::deque<bool> peer_selected_;
    std::vector<std::string> peer_labels_;  // display labels for checkboxes

    // Command log
    static constexpr size_t MAX_LOG_LINES = 50;
    std::deque<std::string> command_log_;

    send_callback_t send_callback_;

    // Ping correlation: seq -> (peer_id, send_time)
    std::map<
        uint32_t,
        std::pair<std::string, std::chrono::steady_clock::time_point>>
        pending_pings_;

    // FTXUI components
    ftxui::Component peers_container_;  // rebuilt when peer list changes
    ftxui::Component root_component_;   // top-level container
};

}  // namespace catl::peer::monitor
