#include <catl/peer/monitor/peer-dashboard.h>
#include <algorithm>
#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>
#include <ftxui/screen/terminal.hpp>

namespace catl::peer::monitor {

using namespace ftxui;
using ftxui::color;

Element
PeerDashboard::render_proposals_tab_(RenderParams const& p)
{
    (void)p;

    // Helper to get tx type name
    auto get_tx_type_name = [this](uint16_t type) -> std::string {
        if (protocol_)
        {
            auto name = protocol_->get_transaction_type_name(type);
            if (name)
                return *name;
        }
        return "T" + std::to_string(type);
    };

    // Check if paused
    bool is_paused = proposals_paused_.load();
    std::string paused_hash;
    if (is_paused)
    {
        std::lock_guard<std::mutex> plock(pause_mutex_);
        paused_hash = paused_prev_hash_;
    }

    // Get last and current proposal rounds
    std::optional<LedgerProposals> last_props_opt;
    std::optional<LedgerProposals> curr_props_opt;
    std::map<std::string, TxSetInfo>
        txset_data;  // Copy of acquisitions
    {
        std::lock_guard<std::mutex> lock(consensus_mutex_);

        if (is_paused && !paused_hash.empty())
        {
            // Paused - show specific round
            auto it = proposal_rounds_.find(paused_hash);
            if (it != proposal_rounds_.end())
            {
                curr_props_opt = it->second;
                // Find the round before this one (by first_proposal
                // time)
                for (auto const& [hash, props] : proposal_rounds_)
                {
                    if (hash != paused_hash &&
                        props.first_proposal <
                            it->second.first_proposal)
                    {
                        if (!last_props_opt ||
                            props.first_proposal >
                                last_props_opt->first_proposal)
                        {
                            last_props_opt = props;
                        }
                    }
                }
            }
        }
        else
        {
            // Not paused - find two most recent rounds
            const LedgerProposals* newest = nullptr;
            const LedgerProposals* second = nullptr;
            for (auto const& [hash, props] : proposal_rounds_)
            {
                if (!newest ||
                    props.first_proposal > newest->first_proposal)
                {
                    second = newest;
                    newest = &props;
                }
                else if (
                    !second ||
                    props.first_proposal > second->first_proposal)
                {
                    second = &props;
                }
            }
            if (newest)
                curr_props_opt = *newest;
            if (second)
                last_props_opt = *second;
        }

        // Copy txset data (from unified acquisitions map)
        txset_data = txset_acquisitions_;
    }

    // Build validator key -> index mapping (from peer mapping)
    std::map<std::string, int> validator_index;
    if (peer_mapping_)
    {
        auto mappings = peer_mapping_->get_all();
        for (auto const& info : mappings)
        {
            if (!info.master_key.empty())
                validator_index[info.master_key] = info.index;
        }
    }
    // Fallback for validators not mapped to a connected peer
    {
        std::lock_guard<std::mutex> lock(consensus_mutex_);
        int next_idx = static_cast<int>(validator_index.size());
        for (auto const& vk : known_validators_)
        {
            if (validator_index.find(vk) == validator_index.end())
                validator_index[vk] = next_idx++;
        }
    }

    // Helper to format validator set as interval notation
    // e.g., validators {1, 2, 3, 5, 7} -> "V{1-3,5,7}"
    auto format_validator_set =
        [&](std::set<std::string> const& validators)
        -> std::string {
        std::vector<int> nums;
        for (auto const& v : validators)
        {
            auto it = validator_index.find(v);
            if (it != validator_index.end())
                nums.push_back(it->second);
        }
        if (nums.empty())
            return "V{}";
        std::sort(nums.begin(), nums.end());

        // Build interval notation
        std::string result = "V{";
        int start = nums[0], end = nums[0];
        for (size_t i = 1; i < nums.size(); ++i)
        {
            if (nums[i] == end + 1)
            {
                end = nums[i];
            }
            else
            {
                if (result.size() > 2)
                    result += ",";
                if (start == end)
                    result += std::to_string(start);
                else
                    result += std::to_string(start) + "-" +
                        std::to_string(end);
                start = end = nums[i];
            }
        }
        if (result.size() > 2)
            result += ",";
        if (start == end)
            result += std::to_string(start);
        else
            result +=
                std::to_string(start) + "-" + std::to_string(end);
        result += "}";
        return result;
    };

    // Timing info for first/last validator
    struct ValidatorTiming
    {
        std::string validator_key;
        int64_t delta_ms;
    };

    // Helper to render txset info
    // first/last: timing info for first and last validators
    auto render_txset_info =
        [&](std::string const& txset_hash,
            std::set<std::string> const& validators,
            bool is_winner,
            std::optional<ValidatorTiming> first = std::nullopt,
            std::optional<ValidatorTiming> last =
                std::nullopt) -> Elements {
        Elements els;
        Color hash_color =
            is_winner ? Color::GreenLight : Color::Yellow;

        std::string short_hash = txset_hash.size() > 12
            ? txset_hash.substr(0, 12)
            : txset_hash;

        std::string validator_str =
            format_validator_set(validators);

        // Format timing string: "n1=+100ms" or "n1=+100ms,
        // n3=+500ms"
        std::string timing_str;
        auto format_timing = [&](ValidatorTiming const& t) {
            auto it = validator_index.find(t.validator_key);
            int idx = it != validator_index.end() ? it->second : 0;
            std::string ms_str = t.delta_ms >= 0
                ? "+" + std::to_string(t.delta_ms) + "ms"
                : std::to_string(t.delta_ms) + "ms";
            return "n" + std::to_string(idx) + "=" + ms_str;
        };

        if (first.has_value())
        {
            timing_str = format_timing(first.value());
            if (last.has_value() &&
                last->validator_key != first->validator_key)
            {
                timing_str += ", " + format_timing(last.value());
            }
        }

        els.push_back(hbox({
            is_winner ? text("★ ") : text("  "),
            text(short_hash) | bold | color(hash_color),
            text(" ") | dim,
            text(validator_str) | color(hash_color),
            timing_str.empty() ? text("")
                               : text(" " + timing_str) | dim,
        }));

        auto txset_it = txset_data.find(txset_hash);
        if (txset_it != txset_data.end())
        {
            auto const& info = txset_it->second;
            std::string status_str;
            Color status_color;
            switch (info.status)
            {
                case TxSetStatus::Pending:
                    status_str = "pending";
                    status_color = Color::GrayLight;
                    break;
                case TxSetStatus::Acquiring:
                    status_str = "acquiring";
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

            els.push_back(hbox({
                text("  [") | dim,
                text(status_str) | color(status_color),
                text("] ") | dim,
                text(std::to_string(info.total_txns)) | bold,
                text(" txns") | dim,
            }));

            if (!info.type_histogram.empty())
            {
                for (auto const& [type, count] :
                     info.type_histogram)
                {
                    // Build LS string for Shuffle (type 88)
                    std::string ls_str;
                    if (type == 88 &&
                        !info.shuffle_ledger_seqs.empty())
                    {
                        ls_str = " LS={";
                        bool first = true;
                        for (auto const& [seq, cnt] :
                             info.shuffle_ledger_seqs)
                        {
                            if (!first)
                                ls_str += ",";
                            ls_str += std::to_string(seq) + ":" +
                                std::to_string(cnt);
                            first = false;
                        }
                        ls_str += "}";
                    }

                    els.push_back(hbox({
                        text("    ") | dim,
                        text(get_tx_type_name(type)) |
                            color(Color::Cyan),
                        text("=") | dim,
                        text(std::to_string(count)) | bold,
                        ls_str.empty() ? text("")
                                       : text(ls_str) |
                                color(Color::GreenLight),
                    }));
                }
            }
        }
        else
        {
            els.push_back(text("  [not tracked]") | dim);
        }
        els.push_back(text(""));
        return els;
    };

    // LEFT COLUMN: LAST LEDGER - show CONVERGED/WINNING txset only
    auto render_last_ledger = [&]() -> Element {
        Elements elements;
        elements.push_back(
            text("◀ LAST LEDGER (converged)") | bold |
            color(Color::GreenLight));
        elements.push_back(separator());

        if (!last_props_opt || last_props_opt->timeline.empty())
        {
            elements.push_back(text("No data yet...") | dim);
            return vbox(elements);
        }

        auto const& props = *last_props_opt;
        std::string seq_str = props.ledger_seq > 0
            ? std::to_string(props.ledger_seq)
            : "?";
        elements.push_back(hbox({
            text("L") | dim,
            text(seq_str) | bold | color(Color::GreenLight),
            is_paused ? text(" [P]") | bold | color(Color::Red)
                      : text(""),
        }));
        elements.push_back(separator());

        // Find the FINAL position of each validator (highest seq)
        std::map<std::string, std::pair<uint32_t, std::string>>
            validator_final;  // validator -> (max_seq, hash)
        for (auto const& event : props.timeline)
        {
            auto& [max_seq, hash] =
                validator_final[event.validator_key];
            if (event.propose_seq >= max_seq)
            {
                max_seq = event.propose_seq;
                hash = event.tx_set_hash;
            }
        }

        // Find the WINNING txset and collect its validators
        std::map<std::string, std::set<std::string>>
            txset_validators;
        for (auto const& [validator, seq_hash] : validator_final)
        {
            txset_validators[seq_hash.second].insert(validator);
        }

        std::string winner_hash;
        size_t max_count = 0;
        for (auto const& [hash, validators] : txset_validators)
        {
            if (validators.size() > max_count)
            {
                max_count = validators.size();
                winner_hash = hash;
            }
        }

        // Show ONLY the winning txset
        if (!winner_hash.empty())
        {
            auto& validators = txset_validators[winner_hash];
            auto info =
                render_txset_info(winner_hash, validators, true);
            for (auto& el : info)
                elements.push_back(std::move(el));
        }
        else
        {
            elements.push_back(text("No winner yet...") | dim);
        }

        return vbox(elements);
    };

    // RIGHT COLUMN: CURRENT LEDGER - show seq=0 proposals
    // (diversity)
    auto render_current_ledger = [&]() -> Element {
        Elements elements;
        elements.push_back(
            text("▶ CURRENT LEDGER (propSeq=0)") | bold |
            color(Color::Yellow));
        elements.push_back(separator());

        if (!curr_props_opt || curr_props_opt->timeline.empty())
        {
            elements.push_back(text("No proposals yet...") | dim);
            return vbox(elements);
        }

        auto const& props = *curr_props_opt;
        std::string seq_str = props.ledger_seq > 0
            ? std::to_string(props.ledger_seq)
            : "?";
        elements.push_back(hbox({
            text("L") | dim,
            text(seq_str) | bold | color(Color::Yellow),
            is_paused ? text(" [P]") | bold | color(Color::Red)
                      : text(""),
        }));
        elements.push_back(separator());

        // Get seq=0 proposals only - collect validators and
        // first/last seen times
        struct TxSetEntry
        {
            std::string hash;
            std::set<std::string> validators;
            std::chrono::steady_clock::time_point first_seen;
            std::chrono::steady_clock::time_point last_seen;
            std::string first_validator;
            std::string last_validator;
            size_t commit_count = 0;
            size_t reveal_count = 0;
        };
        std::map<std::string, TxSetEntry> txset_map;
        for (auto const& event : props.timeline)
        {
            if (event.propose_seq == 0)
            {
                auto& entry = txset_map[event.tx_set_hash];
                if (entry.hash.empty())
                {
                    entry.hash = event.tx_set_hash;
                    entry.first_seen = event.received_at;
                    entry.first_validator = event.validator_key;
                    entry.last_seen = event.received_at;
                    entry.last_validator = event.validator_key;
                }
                else
                {
                    // Update last seen if this is newer
                    if (event.received_at > entry.last_seen)
                    {
                        entry.last_seen = event.received_at;
                        entry.last_validator = event.validator_key;
                    }
                }
                entry.validators.insert(event.validator_key);
                if (event.has_commitment)
                    ++entry.commit_count;
                if (event.has_reveal)
                    ++entry.reveal_count;
            }
        }

        if (txset_map.empty())
        {
            elements.push_back(text("No seq=0 proposals") | dim);
            return vbox(elements);
        }

        // Sort by first seen time
        std::vector<TxSetEntry> txsets;
        for (auto& [_, entry] : txset_map)
            txsets.push_back(std::move(entry));
        std::sort(
            txsets.begin(),
            txsets.end(),
            [](auto const& a, auto const& b) {
                return a.first_seen < b.first_seen;
            });

        // Find majority for highlighting
        std::string majority_hash;
        size_t max_validators = 0;
        for (auto const& entry : txsets)
        {
            if (entry.validators.size() > max_validators)
            {
                max_validators = entry.validators.size();
                majority_hash = entry.hash;
            }
        }

        // Determine reference time for delta calculation
        // Use last_validated_at if set, otherwise first_proposal
        auto ref_time =
            props.last_validated_at.time_since_epoch().count() > 0
            ? props.last_validated_at
            : props.first_proposal;

        // Render each txset
        for (auto const& entry : txsets)
        {
            bool is_majority = (entry.hash == majority_hash);
            // Calculate first/last timing
            auto first_delta = std::chrono::duration_cast<
                                   std::chrono::milliseconds>(
                                   entry.first_seen - ref_time)
                                   .count();
            auto last_delta = std::chrono::duration_cast<
                                  std::chrono::milliseconds>(
                                  entry.last_seen - ref_time)
                                  .count();
            ValidatorTiming first{
                entry.first_validator, first_delta};
            ValidatorTiming last{entry.last_validator, last_delta};
            auto info = render_txset_info(
                entry.hash,
                entry.validators,
                is_majority,
                first,
                last);
            for (auto& el : info)
                elements.push_back(std::move(el));
            if (entry.commit_count > 0 || entry.reveal_count > 0)
            {
                elements.push_back(hbox({
                    text("  RNG ") | dim,
                    text(
                        "C:" + std::to_string(entry.commit_count)) |
                        color(Color::Cyan),
                    text(
                        " R:" +
                        std::to_string(entry.reveal_count)) |
                        color(Color::Magenta),
                }));
            }
        }

        return vbox(elements);
    };

    // Generic propSeq=N proposals column
    // Shows validators who actually sent a proposal at propSeq=N
    auto render_seq_proposals = [&](uint32_t seq_num) -> Element {
        Elements elements;
        elements.push_back(
            text("▶ propSeq=" + std::to_string(seq_num)) | bold |
            color(Color::Cyan));
        elements.push_back(separator());

        if (!curr_props_opt || curr_props_opt->timeline.empty())
        {
            elements.push_back(text("...") | dim);
            return vbox(elements);
        }

        auto const& props = *curr_props_opt;

        // Get seq=N proposals only - collect validators and
        // first/last seen times
        struct TxSetEntry
        {
            std::string hash;
            std::set<std::string> validators;
            std::chrono::steady_clock::time_point first_seen;
            std::chrono::steady_clock::time_point last_seen;
            std::string first_validator;
            std::string last_validator;
            size_t commit_count = 0;
            size_t reveal_count = 0;
        };
        std::map<std::string, TxSetEntry> txset_map;
        for (auto const& event : props.timeline)
        {
            if (event.propose_seq == seq_num)
            {
                auto& entry = txset_map[event.tx_set_hash];
                if (entry.hash.empty())
                {
                    entry.hash = event.tx_set_hash;
                    entry.first_seen = event.received_at;
                    entry.first_validator = event.validator_key;
                    entry.last_seen = event.received_at;
                    entry.last_validator = event.validator_key;
                }
                else
                {
                    if (event.received_at > entry.last_seen)
                    {
                        entry.last_seen = event.received_at;
                        entry.last_validator = event.validator_key;
                    }
                }
                entry.validators.insert(event.validator_key);
                if (event.has_commitment)
                    ++entry.commit_count;
                if (event.has_reveal)
                    ++entry.reveal_count;
            }
        }

        if (txset_map.empty())
        {
            elements.push_back(
                text("No propSeq=" + std::to_string(seq_num)) |
                dim);
            return vbox(elements);
        }

        // Sort by first seen time
        std::vector<TxSetEntry> txsets;
        for (auto& [_, entry] : txset_map)
            txsets.push_back(std::move(entry));
        std::sort(
            txsets.begin(),
            txsets.end(),
            [](auto const& a, auto const& b) {
                return a.first_seen < b.first_seen;
            });

        // Find majority
        std::string majority_hash;
        size_t max_validators = 0;
        for (auto const& entry : txsets)
        {
            if (entry.validators.size() > max_validators)
            {
                max_validators = entry.validators.size();
                majority_hash = entry.hash;
            }
        }

        // Determine reference time for delta calculation
        auto ref_time =
            props.last_validated_at.time_since_epoch().count() > 0
            ? props.last_validated_at
            : props.first_proposal;

        for (auto const& entry : txsets)
        {
            bool is_majority = (entry.hash == majority_hash);
            auto first_delta = std::chrono::duration_cast<
                                   std::chrono::milliseconds>(
                                   entry.first_seen - ref_time)
                                   .count();
            auto last_delta = std::chrono::duration_cast<
                                  std::chrono::milliseconds>(
                                  entry.last_seen - ref_time)
                                  .count();
            ValidatorTiming first{
                entry.first_validator, first_delta};
            ValidatorTiming last{entry.last_validator, last_delta};
            auto info = render_txset_info(
                entry.hash,
                entry.validators,
                is_majority,
                first,
                last);
            for (auto& el : info)
                elements.push_back(std::move(el));
            if (entry.commit_count > 0 || entry.reveal_count > 0)
            {
                elements.push_back(hbox({
                    text("  RNG ") | dim,
                    text(
                        "C:" + std::to_string(entry.commit_count)) |
                        color(Color::Cyan),
                    text(
                        " R:" +
                        std::to_string(entry.reveal_count)) |
                        color(Color::Magenta),
                }));
            }
        }

        return vbox(elements);
    };

    // Find max propSeq in current round (for "latest" display)
    uint32_t max_prop_seq = 0;
    if (curr_props_opt)
    {
        for (auto const& event : curr_props_opt->timeline)
        {
            if (event.propose_seq > max_prop_seq)
                max_prop_seq = event.propose_seq;
        }
    }

    // Render "latest" - shows highest propSeq (only when >= 6)
    auto render_latest_proposals = [&]() -> Element {
        Elements elements;
        std::string header = max_prop_seq >= 6
            ? "▶ propSeq=" + std::to_string(max_prop_seq)
            : "▶ propSeq=6+";
        elements.push_back(
            text(header) | bold | color(Color::Magenta));
        elements.push_back(separator());

        if (!curr_props_opt || curr_props_opt->timeline.empty() ||
            max_prop_seq < 6)
        {
            elements.push_back(text("...") | dim);
            return vbox(elements);
        }

        auto const& props = *curr_props_opt;

        struct TxSetEntry
        {
            std::string hash;
            std::set<std::string> validators;
            std::chrono::steady_clock::time_point first_seen;
            std::chrono::steady_clock::time_point last_seen;
            std::string first_validator;
            std::string last_validator;
            size_t commit_count = 0;
            size_t reveal_count = 0;
        };
        std::map<std::string, TxSetEntry> txset_map;
        for (auto const& event : props.timeline)
        {
            if (event.propose_seq == max_prop_seq)
            {
                auto& entry = txset_map[event.tx_set_hash];
                if (entry.hash.empty())
                {
                    entry.hash = event.tx_set_hash;
                    entry.first_seen = event.received_at;
                    entry.first_validator = event.validator_key;
                    entry.last_seen = event.received_at;
                    entry.last_validator = event.validator_key;
                }
                else
                {
                    if (event.received_at > entry.last_seen)
                    {
                        entry.last_seen = event.received_at;
                        entry.last_validator = event.validator_key;
                    }
                }
                entry.validators.insert(event.validator_key);
                if (event.has_commitment)
                    ++entry.commit_count;
                if (event.has_reveal)
                    ++entry.reveal_count;
            }
        }

        if (txset_map.empty())
        {
            elements.push_back(text("...") | dim);
            return vbox(elements);
        }

        std::vector<TxSetEntry> txsets;
        for (auto& [_, entry] : txset_map)
            txsets.push_back(std::move(entry));
        std::sort(
            txsets.begin(),
            txsets.end(),
            [](auto const& a, auto const& b) {
                return a.first_seen < b.first_seen;
            });

        std::string majority_hash;
        size_t max_validators = 0;
        for (auto const& entry : txsets)
        {
            if (entry.validators.size() > max_validators)
            {
                max_validators = entry.validators.size();
                majority_hash = entry.hash;
            }
        }

        auto ref_time =
            props.last_validated_at.time_since_epoch().count() > 0
            ? props.last_validated_at
            : props.first_proposal;

        for (auto const& entry : txsets)
        {
            bool is_majority = (entry.hash == majority_hash);
            auto first_delta = std::chrono::duration_cast<
                                   std::chrono::milliseconds>(
                                   entry.first_seen - ref_time)
                                   .count();
            auto last_delta = std::chrono::duration_cast<
                                  std::chrono::milliseconds>(
                                  entry.last_seen - ref_time)
                                  .count();
            ValidatorTiming first{
                entry.first_validator, first_delta};
            ValidatorTiming last{entry.last_validator, last_delta};
            auto info = render_txset_info(
                entry.hash,
                entry.validators,
                is_majority,
                first,
                last);
            for (auto& el : info)
                elements.push_back(std::move(el));
            if (entry.commit_count > 0 || entry.reveal_count > 0)
            {
                elements.push_back(hbox({
                    text("  RNG ") | dim,
                    text(
                        "C:" + std::to_string(entry.commit_count)) |
                        color(Color::Cyan),
                    text(
                        " R:" +
                        std::to_string(entry.reveal_count)) |
                        color(Color::Magenta),
                }));
            }
        }

        return vbox(elements);
    };

    // Render 5 columns with last 3 hsplit:
    // LAST | propSeq=0 | 1/2 | 3/4 | 5/latest
    auto left_col = render_last_ledger();
    auto seq0_col = render_current_ledger();

    // Get half height for hsplit columns
    auto term_height = Terminal::Size().dimy;
    int half_height =
        (term_height - 8) / 2;  // Account for header/footer

    // Column 3: propSeq=1 on top, propSeq=2 on bottom (50/50)
    auto col3 = vbox({
        render_seq_proposals(1) | size(HEIGHT, EQUAL, half_height),
        separator(),
        render_seq_proposals(2) | size(HEIGHT, EQUAL, half_height),
    });

    // Column 4: propSeq=3 on top, propSeq=4 on bottom (50/50)
    auto col4 = vbox({
        render_seq_proposals(3) | size(HEIGHT, EQUAL, half_height),
        separator(),
        render_seq_proposals(4) | size(HEIGHT, EQUAL, half_height),
    });

    // Column 5: propSeq=5 on top, latest on bottom (50/50)
    auto col5 = vbox({
        render_seq_proposals(5) | size(HEIGHT, EQUAL, half_height),
        separator(),
        render_latest_proposals() |
            size(HEIGHT, EQUAL, half_height),
    });

    auto term_size = Terminal::Size();
    int col_width = (term_size.dimx - 12) / 5;

    return hbox({
        left_col | size(WIDTH, EQUAL, col_width) | border,
        separator(),
        seq0_col | size(WIDTH, EQUAL, col_width) | border,
        separator(),
        col3 | size(WIDTH, EQUAL, col_width) | border,
        separator(),
        col4 | size(WIDTH, EQUAL, col_width) | border,
        separator(),
        col5 | size(WIDTH, EQUAL, col_width) | border,
    });
}

}  // namespace catl::peer::monitor
