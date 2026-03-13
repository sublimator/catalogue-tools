#include <catl/peer/monitor/dashboard-format.h>
#include <catl/peer/monitor/peer-dashboard.h>
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>

namespace catl::peer::monitor {

using namespace ftxui;
using ftxui::color;

ftxui::Element
PeerDashboard::render_main_tab_(RenderParams const& p)
{
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

    // Connection status color - check for reconnecting state
    bool is_reconnecting = !p.all_peers.empty() &&
        p.all_peers[0].reconnect_at.time_since_epoch().count() > 0 &&
        p.all_peers[0].reconnect_at > p.now;
    Color status_color;
    std::string status_icon;

    if (p.is_connected)
    {
        status_color = Color::GreenLight;
        status_icon = "🟢";
    }
    else if (is_reconnecting)
    {
        status_color = Color::Yellow;
        status_icon = "🟡";
    }
    else
    {
        status_color = Color::Red;
        status_icon = "🔴";
    }

    // Check if primary peer is receiving data
    bool receiving = false;
    if (!p.all_peers.empty())
    {
        auto since_last =
            std::chrono::duration_cast<std::chrono::seconds>(
                p.now - p.all_peers[0].last_packet_time)
                .count();
        receiving = p.all_peers[0].connected && since_last < 5;
    }
    std::string activity;
    if (receiving)
    {
        activity = fmt::spinner() + " Receiving";
    }
    else if (is_reconnecting)
    {
        // Compute live countdown for primary peer
        auto remaining =
            std::chrono::duration_cast<std::chrono::seconds>(
                p.all_peers[0].reconnect_at - p.now)
                .count();
        if (remaining < 0)
            remaining = 0;
        activity = fmt::spinner() + " " + p.state + " in " +
            std::to_string(remaining) + "s";
    }
    else
    {
        activity = "Idle";
    }

    // Get consensus tracking info
    auto last_validated = get_last_validated();
    auto validating = get_validating();
    auto known_validators = get_known_validator_count();

    // Last Validated Ledger section
    Elements last_validated_elements;
    last_validated_elements.push_back(
        text("✓ LAST VALIDATED") | bold | color(Color::GreenLight));
    last_validated_elements.push_back(separator());

    if (last_validated && last_validated->sequence > 0)
    {
        // Alternate color based on even/odd sequence
        Color seq_color = (last_validated->sequence % 2 == 0)
            ? Color::Yellow
            : Color::Cyan;
        last_validated_elements.push_back(hbox({
            text("Sequence: "),
            text(fmt::number(last_validated->sequence)) | bold |
                color(seq_color),
        }));

        if (!last_validated->hash.empty())
        {
            last_validated_elements.push_back(hbox({
                text("Hash: "),
                text(last_validated->hash.substr(0, 16) + "...") |
                    bold,
            }));
        }

        size_t val_count = last_validated->validators.size();
        last_validated_elements.push_back(hbox({
            text("Validators: "),
            text(
                std::to_string(val_count) + "/" +
                std::to_string(known_validators)) |
                bold | color(Color::GreenLight),
        }));

        auto age = std::chrono::duration_cast<std::chrono::seconds>(
                       p.now - last_validated->first_seen)
                       .count();
        last_validated_elements.push_back(hbox({
            text("Closed: "),
            text(std::to_string(age) + "s ago") | dim,
        }));
    }
    else
    {
        last_validated_elements.push_back(
            text("Waiting for quorum...") | dim);
    }

    // Validating section (in progress)
    Elements validating_elements;
    validating_elements.push_back(
        text("⏳ VALIDATING...") | bold | color(Color::Cyan));
    validating_elements.push_back(separator());

    if (validating && validating->sequence > 0)
    {
        // Alternate color based on even/odd sequence
        Color seq_color = (validating->sequence % 2 == 0)
            ? Color::Yellow
            : Color::Cyan;
        validating_elements.push_back(hbox({
            text("Sequence: "),
            text(fmt::number(validating->sequence)) | bold |
                color(seq_color),
        }));

        if (!validating->hash.empty())
        {
            validating_elements.push_back(hbox({
                text("Hash: "),
                text(validating->hash.substr(0, 16) + "...") | bold,
            }));
        }

        size_t val_count = validating->validators.size();
        size_t quorum =
            static_cast<size_t>(std::ceil(known_validators * 0.8));
        float progress = known_validators > 0
            ? static_cast<float>(val_count) / known_validators
            : 0.0f;

        validating_elements.push_back(hbox({
            text("Progress: "),
            text(
                std::to_string(val_count) + "/" +
                std::to_string(known_validators) + " (need " +
                std::to_string(quorum) + ")") |
                bold,
        }));

        validating_elements.push_back(
            gauge(progress) | color(Color::Blue));

        // Show all validators
        for (auto const& [key, info] : validating->validators)
        {
            std::string short_key =
                key.size() > 12 ? key.substr(0, 12) + "..." : key;
            // Build compact peer list: "1,2,3" instead of
            // "peer-1,peer-2,peer-3"
            std::string peers_str;
            for (auto const& p : info.seen_from_peers)
            {
                if (!peers_str.empty())
                    peers_str += ",";
                // Extract just the number from "peer-N"
                auto pos = p.find('-');
                if (pos != std::string::npos && pos + 1 < p.size())
                    peers_str += p.substr(pos + 1);
                else
                    peers_str += p.substr(0, 2);
            }
            validating_elements.push_back(hbox({
                text("  ✓ ") | color(Color::GreenLight),
                text(short_key) | dim,
                text(" [") | dim,
                text(peers_str) | dim,
                text("]") | dim,
            }));
        }
    }
    else
    {
        validating_elements.push_back(
            text("Waiting for validations...") | dim);
    }

    // Helper to render proposal list
    auto render_proposals =
        [&](std::optional<LedgerProposals> const& proposals,
            std::string const& title,
            Color title_color) -> Elements {
        Elements elements;
        elements.push_back(text(title) | bold | color(title_color));
        elements.push_back(separator());

        if (proposals && !proposals->timeline.empty())
        {
            // First pass: find each validator's LATEST position
            std::map<std::string, std::pair<uint32_t, std::string>>
                validator_latest;  // validator -> (seq, hash)
            for (auto const& event : proposals->timeline)
            {
                auto& [seq, hash] =
                    validator_latest[event.validator_key];
                if (event.propose_seq >= seq)
                {
                    seq = event.propose_seq;
                    hash = event.tx_set_hash;
                }
            }

            // Count unique validators and their CURRENT tx sets
            std::set<std::string> unique_validators;
            std::set<std::string> unique_txsets;
            for (auto const& [validator, seq_hash] :
                 validator_latest)
            {
                unique_validators.insert(validator);
                unique_txsets.insert(seq_hash.second);
            }

            std::string seq_str = proposals->ledger_seq > 0
                ? fmt::number(proposals->ledger_seq)
                : "?";
            std::string prev_short =
                proposals->prev_ledger_hash.size() > 8
                ? proposals->prev_ledger_hash.substr(0, 8) + "..."
                : proposals->prev_ledger_hash;
            elements.push_back(hbox({
                text("Ledger "),
                text(seq_str) | bold | color(title_color),
                text(" | "),
                text(
                    std::to_string(unique_validators.size()) +
                    " proposers"),
                text(" | "),
                text(
                    std::to_string(unique_txsets.size()) +
                    " tx sets"),
                text(" | prev=") | dim,
                text(prev_short) | dim,
            }));
            elements.push_back(separator());

            // Group by validator
            struct ValidatorProposals
            {
                std::string latest_hash;
                uint32_t latest_seq = 0;
                std::set<std::string> all_peers;
                std::vector<double> timings;
            };
            std::map<std::string, ValidatorProposals> by_validator;

            for (auto const& event : proposals->timeline)
            {
                auto& vp = by_validator[event.validator_key];
                // Only update hash/seq if this is a higher
                // proposeSeq (handles out-of-order arrival)
                if (event.propose_seq >= vp.latest_seq)
                {
                    vp.latest_hash = event.tx_set_hash;
                    vp.latest_seq = event.propose_seq;
                }
                // Always accumulate peers and timings
                for (auto const& p : event.seen_from_peers)
                    vp.all_peers.insert(p);
                auto delta_ms = std::chrono::duration_cast<
                                    std::chrono::milliseconds>(
                                    event.received_at -
                                    proposals->first_proposal)
                                    .count();
                vp.timings.push_back(delta_ms / 1000.0);
            }

            // Find majority hash
            std::map<std::string, size_t> hash_counts;
            for (auto const& [key, vp] : by_validator)
                hash_counts[vp.latest_hash]++;
            std::string majority_hash;
            size_t max_count = 0;
            for (auto const& [hash, count] : hash_counts)
            {
                if (count > max_count)
                {
                    max_count = count;
                    majority_hash = hash;
                }
            }

            // Render all validators
            for (auto const& [key, vp] : by_validator)
            {
                std::string short_key = key.size() > 10
                    ? key.substr(0, 10) + "..."
                    : key;
                std::string short_hash = vp.latest_hash.size() > 8
                    ? vp.latest_hash.substr(0, 8)
                    : vp.latest_hash;

                std::string peers_str;
                for (auto const& p : vp.all_peers)
                {
                    if (!peers_str.empty())
                        peers_str += ",";
                    auto pos = p.find('-');
                    if (pos != std::string::npos &&
                        pos + 1 < p.size())
                        peers_str += p.substr(pos + 1);
                    else
                        peers_str += p.substr(0, 2);
                }

                std::string timings_str;
                for (auto t : vp.timings)
                {
                    if (!timings_str.empty())
                        timings_str += ",";
                    char buf[16];
                    std::snprintf(buf, sizeof(buf), "%.1f", t);
                    timings_str += buf;
                }

                bool is_different = !majority_hash.empty() &&
                    vp.latest_hash != majority_hash;
                Color hash_color = is_different ? Color::RedLight
                                                : Color::GreenLight;

                elements.push_back(hbox({
                    text(short_key) | dim,
                    text(" "),
                    text(short_hash) | color(hash_color),
                    text(" seq=") | dim,
                    text(std::to_string(vp.latest_seq)) |
                        (vp.latest_seq > 0 ? color(Color::Yellow)
                                           : nothing),
                    text(" [") | dim,
                    text(peers_str) | dim,
                    text("] [") | dim,
                    text(timings_str + "s") | dim,
                    text("]") | dim,
                    is_different
                        ? (text(" !") | color(Color::RedLight))
                        : text(""),
                }));
            }
        }
        else
        {
            elements.push_back(text("Waiting...") | dim);
        }
        return elements;
    };

    // Render both LAST PROPOSED and PROPOSING
    auto last_proposals = get_last_proposals();
    auto current_proposals = get_current_proposals();

    auto last_proposed_elements = render_proposals(
        last_proposals, "✓ LAST PROPOSED", Color::GreenLight);
    auto proposing_elements = render_proposals(
        current_proposals, "📋 PROPOSING", Color::Yellow);

    // Add debug counters to proposing
    proposing_elements.push_back(separator());
    proposing_elements.push_back(hbox({
        text("rx=") | dim,
        text(std::to_string(proposals_received_.load())) |
            color(Color::Cyan),
        text(" s0=") | dim,
        text(std::to_string(proposals_seq0_.load())) |
            color(Color::GreenLight),
        text(" s>0=") | dim,
        text(std::to_string(proposals_seq_gt0_.load())) |
            color(Color::Yellow),
        text(" rnds=") | dim,
        text(std::to_string(proposal_rounds_count_.load())) |
            color(Color::Magenta),
    }));

    // Calculate fixed heights based on peer count
    // Validation section: header(1) + sep(1) + 4 info lines +
    // validators Proposal section: header(1) + sep(1) + summary(1)
    // + sep(1) + validators
    size_t peer_count = p.all_peers.size();
    int validation_height = 6 + static_cast<int>(peer_count);
    int proposal_height = 4 + static_cast<int>(peer_count);

    // Render txset panel - show recent acquired txsets (WINNERS
    // ONLY)
    auto render_txset = [&]() -> Elements {
        Elements elements;
        elements.push_back(
            text("📦 ACQUIRED TX SETS") | bold |
            color(Color::Magenta));
        elements.push_back(separator());

        // Get winning txsets (marked as winners when ledger
        // validated)
        std::vector<TxSetInfo> all_txsets;
        {
            std::lock_guard<std::mutex> lock(consensus_mutex_);
            for (auto const& [hash, info] : txset_acquisitions_)
            {
                if (info.is_winner)
                {
                    all_txsets.push_back(info);
                }
            }
        }

        if (all_txsets.empty())
        {
            elements.push_back(
                text("No tx sets acquired yet") | dim);
            return elements;
        }

        // Sort by started_at (most recent first)
        std::sort(
            all_txsets.begin(),
            all_txsets.end(),
            [](auto const& a, auto const& b) {
                return a.started_at > b.started_at;
            });

        // Helper to get tx type name from protocol or fallback to
        // numeric
        auto get_tx_type_name =
            [this](uint16_t type) -> std::string {
            if (protocol_)
            {
                auto name =
                    protocol_->get_transaction_type_name(type);
                if (name)
                    return *name;
            }
            return "T" + std::to_string(type);
        };

        // Show up to 5 most recent
        size_t to_show = std::min(all_txsets.size(), size_t(5));
        for (size_t i = 0; i < to_show; ++i)
        {
            auto const& txset = all_txsets[i];

            // Status
            std::string status_str;
            Color status_color = Color::White;
            switch (txset.status)
            {
                case TxSetStatus::Pending:
                    status_str = "Pending";
                    status_color = Color::GrayLight;
                    break;
                case TxSetStatus::Acquiring:
                    status_str = "...";
                    status_color = Color::Yellow;
                    break;
                case TxSetStatus::Complete:
                    status_str = "✓";
                    status_color = Color::GreenLight;
                    break;
                case TxSetStatus::Failed:
                    status_str = "✗";
                    status_color = Color::Red;
                    break;
            }

            // Compact single-line format: L{seq} hash [status]
            // count txns: type1=N
            std::string types_str;
            for (auto const& [type, count] : txset.type_histogram)
            {
                std::string name = get_tx_type_name(type);
                if (!types_str.empty())
                    types_str += ", ";
                types_str += name + "=" + std::to_string(count);

                // For Shuffle (88), append LS histogram and PH
                if (type == 88)
                {
                    if (!txset.shuffle_ledger_seqs.empty())
                    {
                        types_str += " (LS={";
                        bool first = true;
                        for (auto const& [ls, cnt] :
                             txset.shuffle_ledger_seqs)
                        {
                            if (!first)
                                types_str += ",";
                            types_str += std::to_string(ls) + ":" +
                                std::to_string(cnt);
                            first = false;
                        }
                        types_str += "}";
                        // Add parent hashes if present
                        if (!txset.shuffle_parent_hashes.empty())
                        {
                            types_str += " PH={";
                            first = true;
                            for (auto const& [ph, cnt] :
                                 txset.shuffle_parent_hashes)
                            {
                                if (!first)
                                    types_str += ",";
                                types_str +=
                                    ph + ":" + std::to_string(cnt);
                                first = false;
                            }
                            types_str += "}";
                        }
                        types_str += ")";
                    }
                }
            }

            // Ledger seq prefix
            std::string seq_str = txset.ledger_seq > 0
                ? "L" + std::to_string(txset.ledger_seq) + " "
                : "";

            // Format depends on status:
            // - Acquiring: show cumulative r{sent}/{received} to
            // show activity
            // - Complete: show n{nodes} since
            // unique_req==unique_recv
            std::string io_str;
            if (txset.status == TxSetStatus::Acquiring)
            {
                // Show cumulative counts during acquisition
                io_str = "r" + std::to_string(txset.requests_sent) +
                    "/" + std::to_string(txset.replies_received);
            }
            else
            {
                // Show final unique node count for completed/failed
                io_str =
                    "n" + std::to_string(txset.unique_received);
            }
            if (txset.peers_used > 0)
                io_str += "/p" + std::to_string(txset.peers_used);
            if (txset.errors > 0)
                io_str += "/e" + std::to_string(txset.errors);

            elements.push_back(hbox({
                text(seq_str) | color(Color::Yellow),
                text(txset.tx_set_hash.substr(0, 12) + "...") |
                    color(Color::Cyan),
                text(" [") | dim,
                text(status_str) | color(status_color),
                text("] ") | dim,
                text(io_str) | color(Color::GrayLight),
                text(" ") | dim,
                text(std::to_string(txset.total_txns)) | bold,
                text(" txns") | dim,
                text(types_str.empty() ? "" : ": " + types_str) |
                    dim,
            }));

            // Show computed hash on second line for failed txsets
            if (txset.status == TxSetStatus::Failed &&
                !txset.computed_hash.empty())
            {
                elements.push_back(hbox({
                    text("    ") | dim,
                    text("exp=") | dim,
                    text(txset.tx_set_hash.substr(0, 16)) |
                        color(Color::Green),
                    text(" got=") | dim,
                    text(txset.computed_hash.substr(0, 16)) |
                        color(Color::Red),
                }));
            }
        }

        return elements;
    };

    auto txset_elements = render_txset();

    // Combine into layout - validations on top, proposals below
    // Calculate widths: main columns are 50/50, inner panels are
    // 50/50 of that
    auto term_size = Terminal::Size();
    int main_half =
        (term_size.dimx - 4) / 2;  // -4 for borders/separators
    int inner_half = (main_half - 1) / 2;  // -1 for inner separator

    auto consensus_section = vbox({
        hbox({
            vbox(last_validated_elements) |
                size(WIDTH, EQUAL, inner_half) |
                size(HEIGHT, EQUAL, validation_height),
            separator(),
            vbox(validating_elements) |
                size(WIDTH, EQUAL, inner_half) |
                size(HEIGHT, EQUAL, validation_height),
        }),
        separator(),
        hbox({
            vbox(last_proposed_elements) |
                size(WIDTH, EQUAL, inner_half) |
                size(HEIGHT, EQUAL, proposal_height),
            separator(),
            vbox(proposing_elements) |
                size(WIDTH, EQUAL, inner_half) |
                size(HEIGHT, EQUAL, proposal_height),
        }),
        separator(),
        vbox(txset_elements) | size(HEIGHT, LESS_THAN, 12),
    });

    // Multiple peers section
    Elements peers_elements;
    peers_elements.push_back(
        text(
            "👥 CONNECTED PEERS (" +
            std::to_string(p.all_peers.size()) + ")") |
        bold | color(Color::Cyan));
    peers_elements.push_back(separator());

    if (p.all_peers.empty())
    {
        peers_elements.push_back(text("No connected peers") | dim);
    }
    else
    {
        for (size_t i = 0; i < p.all_peers.size(); ++i)
        {
            const auto& peer = p.all_peers[i];

            // Determine status based on reconnect_at time
            bool is_reconnecting =
                peer.reconnect_at.time_since_epoch().count() > 0 &&
                peer.reconnect_at > p.now;
            Color peer_status_color;
            std::string status_icon;

            if (peer.connected)
            {
                peer_status_color = Color::GreenLight;
                status_icon = "●";
            }
            else if (is_reconnecting)
            {
                peer_status_color = Color::Yellow;
                status_icon = "◐";
            }
            else
            {
                peer_status_color = Color::Red;
                status_icon = "○";
            }

            std::string peer_num = std::to_string(i) + ".";

            // Check if this peer is actively receiving or
            // reconnecting
            auto peer_since_last =
                std::chrono::duration_cast<std::chrono::seconds>(
                    p.now - peer.last_packet_time)
                    .count();
            bool peer_receiving =
                peer.connected && peer_since_last < 3;
            std::string activity_icon =
                (peer_receiving || is_reconnecting) ? fmt::spinner()
                                                    : " ";

            // Show address if connected, otherwise show connection
            // state
            std::string display_text;
            if (peer.connected)
            {
                display_text = peer.peer_address;
            }
            else if (is_reconnecting)
            {
                // Compute live countdown
                auto remaining = std::chrono::duration_cast<
                                     std::chrono::seconds>(
                                     peer.reconnect_at - p.now)
                                     .count();
                if (remaining < 0)
                    remaining = 0;
                display_text = peer.connection_state + " in " +
                    std::to_string(remaining) + "s";
            }
            else
            {
                display_text = peer.connection_state;
            }

            peers_elements.push_back(hbox({
                text(peer_num) | size(WIDTH, EQUAL, 3) | dim,
                text(status_icon) | color(peer_status_color),
                text(activity_icon) | color(Color::Cyan),
                text(" " + display_text) | bold,
                text(" | "),
                text(fmt::number(peer.total_packets)) | dim,
                text(" pkts | "),
                text(fmt::bytes(
                    static_cast<double>(peer.total_bytes))) |
                    dim,
            }));
        }
    }

    auto peers_section = vbox(peers_elements);

    // Connection info section (for primary peer)
    auto connection_section = vbox({
        text("🌐 PRIMARY PEER (0)") | bold | color(Color::Cyan),
        separator(),
        hbox({
            text("Status: "),
            text(status_icon + " " + p.state) | bold |
                color(status_color),
        }),
        hbox({
            text("Peer: "),
            text(p.primary_address) | bold,
        }),
        hbox({
            text("Version: "),
            text(p.primary_version) | bold | color(Color::Yellow),
        }),
        hbox({
            text("Protocol: "),
            text(p.primary_protocol) | bold,
        }),
        hbox({
            text("Network ID: "),
            text(
                p.primary_network_id.empty() ? "none"
                                             : p.primary_network_id) |
                bold,
        }),
        hbox({
            text("Activity: "),
            text(activity) | bold |
                color(
                    receiving ? Color::GreenLight
                              : Color::GrayDark),
        }),
    });

    // Overall stats section - aggregate from all peers
    uint64_t total_pkts = 0;
    uint64_t total_b = 0;
    double elapsed = 0;
    for (const auto& peer : p.all_peers)
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
            auto dt = std::chrono::duration_cast<
                          std::chrono::milliseconds>(
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
            text(fmt::elapsed(elapsed)) | bold |
                color(Color::Yellow),
        }),
        hbox({
            text("Total packets: "),
            text(fmt::number(total_pkts)) | bold,
        }),
        hbox({
            text("Total data: "),
            text(fmt::bytes(static_cast<double>(total_b))) | bold,
        }),
        separator(),
        text("Current Throughput") | bold,
        hbox({
            text("  Packets/sec: "),
            text(fmt::rate(pps)) | bold |
                color(Color::GreenLight),
        }),
        hbox({
            text("  Data rate: "),
            text(fmt::bytes(bps) + "/s") | bold |
                color(Color::GreenLight),
        }),
        separator(),
        text("Average (since start)") | bold,
        hbox({
            text("  Packets/sec: "),
            text(fmt::rate(
                elapsed > 0 ? total_pkts / elapsed : 0)) |
                bold,
        }),
        hbox({
            text("  Data rate: "),
            text(
                fmt::bytes(elapsed > 0 ? total_b / elapsed : 0) +
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
    for (const auto& peer : p.all_peers)
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
            text(type) | size(WIDTH, EQUAL, 26) |
                color(Color::Yellow),
            text(fmt::number(count)) | size(WIDTH, EQUAL, 12),
            text(fmt::rate(rate) + "/s") | size(WIDTH, EQUAL, 10),
            text(fmt::bytes(static_cast<double>(type_bytes))) |
                size(WIDTH, EQUAL, 10),
            gauge(pct / 100.0f) | flex | color(Color::Blue),
            text(" " + fmt::rate(pct) + "%") |
                size(WIDTH, EQUAL, 8),
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

    // Debug section for protocol type resolution
    Elements debug_elements;
    debug_elements.push_back(
        text("🔧 DEBUG: Type Resolution") | bold |
        color(Color::Yellow));
    debug_elements.push_back(separator());
    if (protocol_)
    {
        auto name88 = protocol_->get_transaction_type_name(88);
        auto name89 = protocol_->get_transaction_type_name(89);
        debug_elements.push_back(
            text("Source: " + protocol_source_) | dim);
        debug_elements.push_back(
            text("Protocol: loaded") | color(Color::Green));
        debug_elements.push_back(text(
            "Type 88: " +
            (name88 ? *name88 : std::string("(nullopt)"))));
        debug_elements.push_back(text(
            "Type 89: " +
            (name89 ? *name89 : std::string("(nullopt)"))));
    }
    else
    {
        debug_elements.push_back(
            text("Protocol: NOT LOADED") | color(Color::Red));
    }
    auto debug_section = vbox(debug_elements);

    return hbox({
        vbox({
            consensus_section,
            separator(),
            peers_section,
            separator(),
            connection_section,
            separator(),
            stats_section,
        }) | size(WIDTH, EQUAL, main_half) |
            border,
        separator(),
        vbox({
            packet_section,
            separator(),
            throughput_section,
            separator(),
            endpoints_section,
            separator(),
            debug_section,
        }) | size(WIDTH, EQUAL, main_half) |
            border,
    });
}

}  // namespace catl::peer::monitor
