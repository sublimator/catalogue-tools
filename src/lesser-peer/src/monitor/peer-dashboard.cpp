#include <algorithm>
#include <catl/core/logger.h>
#include <catl/peer/monitor/peer-dashboard.h>
#include <cmath>
#include <cstdio>
#include <ftxui/component/component.hpp>
#include <ftxui/component/loop.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>
#include <ftxui/screen/terminal.hpp>
#include <limits>
#include <sstream>

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
    double peer_pps = 0;
    double peer_bps = 0;
    auto now = std::chrono::steady_clock::now();

    {
        std::lock_guard<std::mutex> lock(peers_mutex_);

        // Create a copy of stats and ensure peer_id is set
        Stats updated_stats = stats;
        updated_stats.peer_id = peer_id;

        // Track natural order for peers tab (peer-0, peer-1, ..., peer-10)
        if (peer_stats_.find(peer_id) == peer_stats_.end())
        {
            // Insert in natural numeric order
            auto num = [](std::string const& id) -> int {
                auto pos = id.find('-');
                if (pos != std::string::npos && pos + 1 < id.size())
                {
                    try
                    {
                        return std::stoi(id.substr(pos + 1));
                    }
                    catch (...)
                    {
                    }
                }
                return std::numeric_limits<int>::max();
            };
            auto it = std::lower_bound(
                peer_order_.begin(),
                peer_order_.end(),
                peer_id,
                [&](std::string const& a, std::string const& b) {
                    return num(a) < num(b);
                });
            peer_order_.insert(it, peer_id);
        }

        // Update or insert the peer stats
        peer_stats_[peer_id] = updated_stats;

        // Aggregate totals from all peers for throughput tracking
        for (const auto& [id, peer_stats] : peer_stats_)
        {
            total_packets += peer_stats.total_packets;
            total_bytes += peer_stats.total_bytes;
        }
    }

    // Update per-peer throughput history and compute rates
    {
        std::lock_guard<std::mutex> lock(per_peer_throughput_mutex_);
        auto& history = per_peer_throughput_[peer_id];

        if (!stats.connected)
        {
            history.clear();
            peer_pps = 0;
            peer_bps = 0;
        }
        else
        {
            // If counters reset (e.g., reconnect), reset history to avoid
            // negative deltas.
            if (!history.empty() &&
                (stats.total_packets < history.back().packets ||
                 stats.total_bytes < history.back().bytes))
            {
                history.clear();
            }

            history.push_back({now, stats.total_packets, stats.total_bytes});

            auto cutoff = now - std::chrono::seconds(60);
            while (!history.empty() &&
                   (history.front().timestamp < cutoff || history.size() > 200))
            {
                history.pop_front();
            }

            if (history.size() >= 2)
            {
                auto dt =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        history.back().timestamp - history.front().timestamp)
                        .count();
                if (dt > 0)
                {
                    peer_pps =
                        (history.back().packets - history.front().packets) *
                        1000.0 / dt;
                    peer_bps = (history.back().bytes - history.front().bytes) *
                        1000.0 / dt;
                }
            }
        }
    }

    // Store per-peer rates
    {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        auto it = peer_stats_.find(peer_id);
        if (it != peer_stats_.end())
        {
            it->second.packets_per_sec = peer_pps;
            it->second.bytes_per_sec = peer_bps;
        }
    }

    // Update throughput history with aggregated totals
    {
        std::lock_guard<std::mutex> lock(throughput_mutex_);
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
    auto it = std::find(peer_order_.begin(), peer_order_.end(), peer_id);
    if (it != peer_order_.end())
        peer_order_.erase(it);
}

std::vector<PeerDashboard::Stats>
PeerDashboard::get_all_peers_stats() const
{
    std::lock_guard<std::mutex> lock(peers_mutex_);
    std::vector<Stats> all_stats;
    all_stats.reserve(peer_order_.size());

    for (auto const& id : peer_order_)
    {
        auto it = peer_stats_.find(id);
        if (it != peer_stats_.end())
            all_stats.push_back(it->second);
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

    // Track validation counts for recent ledgers (legacy)
    legacy_ledger_counts_[sequence] = validation_count;

    // Keep only last 10 ledgers
    while (legacy_ledger_counts_.size() > 10)
    {
        legacy_ledger_counts_.erase(legacy_ledger_counts_.begin());
    }
}

PeerDashboard::LedgerInfo
PeerDashboard::get_current_ledger() const
{
    std::lock_guard<std::mutex> lock(ledger_mutex_);
    return current_validated_ledger_;
}

void
PeerDashboard::record_validation(
    uint32_t ledger_seq,
    std::string const& ledger_hash,
    std::string const& validator_key,
    std::string const& ephemeral_key,
    std::string const& peer_id)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);
    auto now = std::chrono::steady_clock::now();

    // Add validator to known set
    known_validators_.insert(validator_key);

    // Always add to ledger_validations_ map - never reject based on sequence
    auto& consensus = ledger_validations_[ledger_seq];
    if (consensus.sequence == 0)
    {
        // First validation for this ledger
        consensus.sequence = ledger_seq;
        consensus.hash = ledger_hash;
        consensus.first_seen = now;
        consensus.is_validated = false;
    }

    // Add/update validator info
    auto& validators = consensus.validators;
    auto it = validators.find(validator_key);
    if (it == validators.end())
    {
        // New validator for this ledger - this peer delivered first
        ValidatorInfo info;
        info.master_key_hex = validator_key;
        info.ephemeral_key_hex = ephemeral_key;
        info.seen_from_peers.insert(peer_id);
        info.first_seen = now;
        validators[validator_key] = std::move(info);

        // Notify peer mapping: this peer was first to deliver this
        // validator's validation (likely the direct connection)
        if (peer_mapping_)
            peer_mapping_->on_first_validation(peer_id, validator_key);
    }
    else
    {
        // Already have this validator, just add the peer
        it->second.seen_from_peers.insert(peer_id);
    }

    // Detect testnet restart: if we're accumulating validations for a
    // sequence lower than our highest validated, the network has restarted.
    // Check before quorum so we reset early — remaining validations from
    // other peers will naturally re-accumulate in clean state.
    if (!ledger_validations_.empty())
    {
        for (auto it = ledger_validations_.rbegin();
             it != ledger_validations_.rend();
             ++it)
        {
            if (it->first > ledger_seq && it->second.is_validated)
            {
                LOGI(
                    "Network restart detected: validation for seq=",
                    ledger_seq,
                    " but already validated seq=",
                    it->first,
                    " — resetting state");
                reset_consensus_state();
                // Re-add this validation into fresh state and return.
                // Subsequent validations from other peers will accumulate
                // and hit quorum naturally.
                auto& fresh = ledger_validations_[ledger_seq];
                fresh.sequence = ledger_seq;
                fresh.hash = ledger_hash;
                fresh.first_seen = now;
                fresh.is_validated = false;
                ValidatorInfo info;
                info.master_key_hex = validator_key;
                info.ephemeral_key_hex = ephemeral_key;
                info.seen_from_peers.insert(peer_id);
                info.first_seen = now;
                fresh.validators[validator_key] = std::move(info);
                return;
            }
            break;  // Only check the highest entry
        }
    }

    // Check quorum: 80% of known validators (XRPL consensus threshold)
    size_t quorum =
        static_cast<size_t>(std::ceil(known_validators_.size() * 0.8));
    if (validators.size() >= quorum && !consensus.is_validated)
    {
        consensus.is_validated = true;

        // When ledger N validates, mark the winning txset that BUILT ledger N
        // That's in the proposal round where prev_hash == hash of ledger N-1
        auto prev_ledger_it = ledger_validations_.find(ledger_seq - 1);
        if (prev_ledger_it != ledger_validations_.end())
        {
            std::string prev_hash = prev_ledger_it->second.hash;
            auto round_it = proposal_rounds_.find(prev_hash);
            if (round_it != proposal_rounds_.end())
            {
                auto& round = round_it->second;

                // Find the winning txset for this round
                std::map<std::string, std::pair<uint32_t, std::string>>
                    validator_final;  // validator -> (max_seq, hash)
                for (auto const& event : round.timeline)
                {
                    auto& [max_seq, hash] =
                        validator_final[event.validator_key];
                    if (event.propose_seq >= max_seq)
                    {
                        max_seq = event.propose_seq;
                        hash = event.tx_set_hash;
                    }
                }

                // Count votes per txset
                std::map<std::string, size_t> txset_votes;
                for (auto const& [validator, seq_hash] : validator_final)
                {
                    txset_votes[seq_hash.second]++;
                }

                // Find winner (most votes)
                std::string winner;
                size_t max_votes = 0;
                for (auto const& [hash, votes] : txset_votes)
                {
                    if (votes > max_votes)
                    {
                        max_votes = votes;
                        winner = hash;
                    }
                }

                // Mark the winning txset
                if (!winner.empty())
                {
                    auto it = txset_acquisitions_.find(winner);
                    if (it != txset_acquisitions_.end())
                    {
                        it->second.is_winner = true;
                    }
                }
            }
        }

        // Also update last_validated_at for proposal rounds building ON this
        // ledger (for the delta timing display)
        for (auto& [prev_hash, round] : proposal_rounds_)
        {
            if (prev_hash == ledger_hash &&
                round.last_validated_at.time_since_epoch().count() == 0)
            {
                round.last_validated_at = now;
            }
        }
    }

    // LRU cleanup
    prune_old_rounds();
}

// Internal impl - must be called with consensus_mutex_ held
std::optional<LedgerConsensus>
PeerDashboard::get_last_validated_impl() const
{
    // Find highest validated sequence
    for (auto it = ledger_validations_.rbegin();
         it != ledger_validations_.rend();
         ++it)
    {
        if (it->second.is_validated)
            return it->second;
    }
    return std::nullopt;
}

// Internal impl - must be called with consensus_mutex_ held
std::optional<LedgerConsensus>
PeerDashboard::get_validating_impl() const
{
    auto last = get_last_validated_impl();
    if (!last)
    {
        // No validated ledger yet - return highest sequence being tracked
        if (!ledger_validations_.empty())
            return ledger_validations_.rbegin()->second;
        return std::nullopt;
    }

    // Return ledger after last validated (if exists)
    auto it = ledger_validations_.find(last->sequence + 1);
    if (it != ledger_validations_.end())
        return it->second;
    return std::nullopt;
}

std::optional<LedgerConsensus>
PeerDashboard::get_last_validated() const
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);
    return get_last_validated_impl();
}

std::optional<LedgerConsensus>
PeerDashboard::get_validating() const
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);
    return get_validating_impl();
}

size_t
PeerDashboard::get_known_validator_count() const
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);
    return known_validators_.size();
}

void
PeerDashboard::reconcile_key(
    std::string const& old_key,
    std::string const& new_key)
{
    if (old_key == new_key || old_key.empty() || new_key.empty())
        return;

    std::lock_guard<std::mutex> lock(consensus_mutex_);

    // 1. known_validators_: remove ephemeral, ensure master present
    if (known_validators_.erase(old_key))
    {
        known_validators_.insert(new_key);
        LOGD(
            "Reconcile: ",
            old_key.substr(0, 12),
            "... -> ",
            new_key.substr(0, 12),
            "...");
    }
    else
    {
        return;  // old_key wasn't tracked, nothing to reconcile
    }

    // 2. ledger_validations_: re-key validator entries
    for (auto& [seq, consensus] : ledger_validations_)
    {
        auto it = consensus.validators.find(old_key);
        if (it != consensus.validators.end())
        {
            auto info = std::move(it->second);
            info.master_key_hex = new_key;
            consensus.validators.erase(it);
            // Merge into existing master key entry if present
            auto master_it = consensus.validators.find(new_key);
            if (master_it != consensus.validators.end())
            {
                // Merge seen_from_peers
                master_it->second.seen_from_peers.insert(
                    info.seen_from_peers.begin(), info.seen_from_peers.end());
            }
            else
            {
                consensus.validators[new_key] = std::move(info);
            }
        }
    }

    // 3. proposal_rounds_: re-key timeline entries and proposing_validators
    for (auto& [hash, props] : proposal_rounds_)
    {
        for (auto& event : props.timeline)
        {
            if (event.validator_key == old_key)
                event.validator_key = new_key;
        }
    }
    for (auto& [hash, txset] : proposal_txsets_)
    {
        if (txset.proposing_validators.erase(old_key))
            txset.proposing_validators.insert(new_key);
    }

    // 4. peer_mapping_: transfer votes from old_key to new_key
    if (peer_mapping_)
    {
        peer_mapping_->reconcile_key(old_key, new_key);
    }
}

void
PeerDashboard::record_proposal(
    std::string const& prev_ledger_hash,
    std::string const& tx_set_hash,
    std::string const& validator_key,
    uint32_t propose_seq,
    std::string const& peer_id,
    bool has_commitment,
    bool has_reveal)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);
    auto now = std::chrono::steady_clock::now();

    // Debug counters
    ++proposals_received_;
    if (propose_seq == 0)
        ++proposals_seq0_;
    else
        ++proposals_seq_gt0_;

    // Always add to proposal_rounds_ map - keyed by prev_ledger_hash
    auto& round = proposal_rounds_[prev_ledger_hash];
    if (round.prev_ledger_hash.empty())
    {
        // First proposal for this round
        round.prev_ledger_hash = prev_ledger_hash;
        round.first_proposal = now;
        round.ledger_seq = infer_ledger_seq(prev_ledger_hash);
    }

    // Find the latest entry for this validator (highest proposeSeq)
    ProposalEvent* latest_entry = nullptr;
    for (auto& event : round.timeline)
    {
        if (event.validator_key == validator_key)
        {
            if (!latest_entry || event.propose_seq > latest_entry->propose_seq)
                latest_entry = &event;
        }
    }

    if (latest_entry)
    {
        if (propose_seq > latest_entry->propose_seq)
        {
            // New proposeSeq = validator changed position, add new entry
            ProposalEvent new_event;
            new_event.validator_key = validator_key;
            new_event.tx_set_hash = tx_set_hash;
            new_event.propose_seq = propose_seq;
            new_event.seen_from_peers.insert(peer_id);
            new_event.received_at = now;
            new_event.has_commitment = has_commitment;
            new_event.has_reveal = has_reveal;
            round.timeline.push_back(std::move(new_event));
        }
        else
        {
            // Same or older seq, just add peer to latest entry
            latest_entry->seen_from_peers.insert(peer_id);
        }
    }
    else
    {
        // New validator
        ProposalEvent event;
        event.validator_key = validator_key;
        event.tx_set_hash = tx_set_hash;
        event.propose_seq = propose_seq;
        event.seen_from_peers.insert(peer_id);
        event.received_at = now;
        event.has_commitment = has_commitment;
        event.has_reveal = has_reveal;
        round.timeline.push_back(std::move(event));
    }

    // Update debug counter
    proposal_rounds_count_ = proposal_rounds_.size();

    // LRU cleanup
    prune_old_rounds();
}

void
PeerDashboard::prune_old_rounds()
{
    // Keep only MAX_TRACKED_ROUNDS ledgers (already under lock)
    while (ledger_validations_.size() > MAX_TRACKED_ROUNDS)
    {
        ledger_validations_.erase(ledger_validations_.begin());
    }

    // Get paused hashes if paused (to pin them)
    std::string pinned_hash;
    std::string pinned_last_hash;
    if (proposals_paused_.load())
    {
        std::lock_guard<std::mutex> plock(pause_mutex_);
        pinned_hash = paused_prev_hash_;
        pinned_last_hash = paused_last_prev_hash_;
    }

    // Prune proposal rounds by age - keep only MAX_TRACKED_ROUNDS newest
    // Sort by first_proposal timestamp and keep the most recent ones
    if (proposal_rounds_.size() > MAX_TRACKED_ROUNDS)
    {
        // Find oldest proposals by first_proposal time
        std::vector<
            std::pair<std::chrono::steady_clock::time_point, std::string>>
            by_time;
        for (auto const& [hash, props] : proposal_rounds_)
        {
            // Skip pinned hashes - don't prune them
            if (!pinned_hash.empty() && hash == pinned_hash)
                continue;
            if (!pinned_last_hash.empty() && hash == pinned_last_hash)
                continue;
            by_time.emplace_back(props.first_proposal, hash);
        }
        std::sort(by_time.begin(), by_time.end());

        // Remove oldest until we're at limit
        size_t to_remove = proposal_rounds_.size() - MAX_TRACKED_ROUNDS;
        for (size_t i = 0; i < to_remove && i < by_time.size(); ++i)
        {
            proposal_rounds_.erase(by_time[i].second);
        }
    }
}

void
PeerDashboard::reset_consensus_state()
{
    // Called with consensus_mutex_ already held.
    ledger_validations_.clear();
    proposal_rounds_.clear();
    txset_acquisitions_.clear();
    proposal_txsets_.clear();
    known_validators_.clear();

    {
        std::lock_guard<std::mutex> llock(ledger_mutex_);
        legacy_ledger_counts_.clear();
        current_validated_ledger_ = {};
    }

    // Clear per-peer recent_ledgers
    {
        std::lock_guard<std::mutex> plock(peers_mutex_);
        for (auto& [_, stats] : peer_stats_)
            stats.recent_ledgers.clear();
    }

    {
        std::lock_guard<std::mutex> elock(endpoints_mutex_);
        available_endpoints_.clear();
    }

    // Reset peer mapping (vote history is stale)
    if (peer_mapping_)
        peer_mapping_->clear();

    // Unpause if paused (stale pinned hashes)
    if (proposals_paused_.load())
    {
        proposals_paused_.store(false);
        std::lock_guard<std::mutex> plock(pause_mutex_);
        paused_prev_hash_.clear();
        paused_last_prev_hash_.clear();
    }

    proposal_rounds_count_ = 0;

    // Clear debug counters
    proposals_received_ = 0;
    proposals_seq0_ = 0;
    proposals_seq_gt0_ = 0;
    proposals_ignored_ = 0;

    // Clear per-peer throughput history
    {
        std::lock_guard<std::mutex> tlock(per_peer_throughput_mutex_);
        per_peer_throughput_.clear();
    }

    // Notify external components (e.g. packet_processor)
    if (restart_callback_)
        restart_callback_();
}

uint32_t
PeerDashboard::infer_ledger_seq(std::string const& prev_hash) const
{
    // Find validation with this hash and return seq + 1
    for (auto const& [seq, consensus] : ledger_validations_)
    {
        if (consensus.hash == prev_hash)
            return seq + 1;
    }
    return 0;  // Unknown
}

std::optional<LedgerProposals>
PeerDashboard::get_proposals_for_ledger(uint32_t seq) const
{
    // Find proposals where prev_hash matches a ledger's hash
    auto consensus_it = ledger_validations_.find(seq - 1);
    if (consensus_it == ledger_validations_.end())
        return std::nullopt;

    std::string const& prev_hash = consensus_it->second.hash;
    auto prop_it = proposal_rounds_.find(prev_hash);
    if (prop_it != proposal_rounds_.end())
        return prop_it->second;
    return std::nullopt;
}

std::optional<LedgerProposals>
PeerDashboard::get_current_proposals() const
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    if (proposal_rounds_.empty())
        return std::nullopt;

    // Find the most recent proposal round by first_proposal timestamp
    const LedgerProposals* newest = nullptr;
    for (auto const& [hash, props] : proposal_rounds_)
    {
        if (!newest || props.first_proposal > newest->first_proposal)
            newest = &props;
    }

    return newest ? std::optional<LedgerProposals>(*newest) : std::nullopt;
}

std::optional<LedgerProposals>
PeerDashboard::get_last_proposals() const
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    if (proposal_rounds_.size() < 2)
        return std::nullopt;

    // Find the two most recent proposal rounds
    const LedgerProposals* newest = nullptr;
    const LedgerProposals* second = nullptr;

    for (auto const& [hash, props] : proposal_rounds_)
    {
        if (!newest || props.first_proposal > newest->first_proposal)
        {
            second = newest;
            newest = &props;
        }
        else if (!second || props.first_proposal > second->first_proposal)
        {
            second = &props;
        }
    }

    return second ? std::optional<LedgerProposals>(*second) : std::nullopt;
}

std::optional<std::pair<std::string, uint32_t>>
PeerDashboard::get_txset_to_acquire() const
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    // Get LAST PROPOSED (second most recent)
    if (proposal_rounds_.size() < 2)
        return std::nullopt;

    const LedgerProposals* newest = nullptr;
    const LedgerProposals* second = nullptr;

    for (auto const& [hash, props] : proposal_rounds_)
    {
        if (!newest || props.first_proposal > newest->first_proposal)
        {
            second = newest;
            newest = &props;
        }
        else if (!second || props.first_proposal > second->first_proposal)
        {
            second = &props;
        }
    }

    if (!second || second->timeline.empty())
        return std::nullopt;

    // Get LATEST tx_set_hash per validator (highest propose_seq)
    std::map<std::string, std::pair<uint32_t, std::string>>
        validator_latest;  // validator -> (seq, hash)
    for (auto const& event : second->timeline)
    {
        auto it = validator_latest.find(event.validator_key);
        if (it == validator_latest.end() ||
            event.propose_seq > it->second.first)
        {
            validator_latest[event.validator_key] = {
                event.propose_seq, event.tx_set_hash};
        }
    }

    // Count unique hashes among latest proposals
    std::map<std::string, size_t> hash_counts;
    for (auto const& [validator, seq_hash] : validator_latest)
    {
        hash_counts[seq_hash.second]++;
    }

    // Only return if all validators agree (1 unique hash)
    if (hash_counts.size() != 1)
        return std::nullopt;  // Not converged yet

    std::string best_hash = hash_counts.begin()->first;

    // Check if we already have it
    auto it = txset_acquisitions_.find(best_hash);
    if (it != txset_acquisitions_.end())
        return std::nullopt;  // Already tracking

    // Return hash and ledger_seq
    return std::make_pair(best_hash, second->ledger_seq);
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
PeerDashboard::record_txset_start(
    std::string const& tx_set_hash,
    std::string const& prev_ledger_hash)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    // Check if already tracking
    if (txset_acquisitions_.find(tx_set_hash) != txset_acquisitions_.end())
        return;

    // Create new entry
    TxSetInfo info;
    info.tx_set_hash = tx_set_hash;
    // Infer ledger_seq from prev_hash (this is the ledger being built)
    info.ledger_seq = infer_ledger_seq(prev_ledger_hash);
    info.status = TxSetStatus::Acquiring;
    info.started_at = std::chrono::steady_clock::now();
    txset_acquisitions_[tx_set_hash] = std::move(info);

    // LRU cleanup - remove oldest if over limit (skip when paused)
    if (!proposals_paused_.load())
    {
        while (txset_acquisitions_.size() > MAX_TRACKED_TXSETS)
        {
            auto oldest = txset_acquisitions_.begin();
            for (auto it = txset_acquisitions_.begin();
                 it != txset_acquisitions_.end();
                 ++it)
            {
                if (it->second.started_at < oldest->second.started_at)
                    oldest = it;
            }
            txset_acquisitions_.erase(oldest);
        }
    }
}

void
PeerDashboard::record_txset_transaction(
    std::string const& tx_set_hash,
    uint16_t tx_type)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = txset_acquisitions_.find(tx_set_hash);
    if (it == txset_acquisitions_.end())
        return;

    it->second.type_histogram[tx_type]++;
    it->second.total_txns++;
}

void
PeerDashboard::record_txset_shuffle_data(
    std::string const& tx_set_hash,
    uint32_t ledger_seq,
    std::string const& parent_hash)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = txset_acquisitions_.find(tx_set_hash);
    if (it == txset_acquisitions_.end())
        return;

    it->second.shuffle_ledger_seqs[ledger_seq]++;
    if (!parent_hash.empty())
    {
        // Store first 7 hex chars (like git short hash)
        std::string key =
            parent_hash.size() > 7 ? parent_hash.substr(0, 7) : parent_hash;
        it->second.shuffle_parent_hashes[key]++;
    }
}

void
PeerDashboard::record_txset_complete(
    std::string const& tx_set_hash,
    bool success)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = txset_acquisitions_.find(tx_set_hash);
    if (it == txset_acquisitions_.end())
        return;

    it->second.status = success ? TxSetStatus::Complete : TxSetStatus::Failed;
    it->second.completed_at = std::chrono::steady_clock::now();
}

void
PeerDashboard::record_txset_computed_hash(
    std::string const& tx_set_hash,
    std::string const& computed_hash)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = txset_acquisitions_.find(tx_set_hash);
    if (it == txset_acquisitions_.end())
        return;

    it->second.computed_hash = computed_hash;
}

void
PeerDashboard::record_txset_request(
    std::string const& tx_set_hash,
    size_t count)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = txset_acquisitions_.find(tx_set_hash);
    if (it == txset_acquisitions_.end())
        return;

    it->second.requests_sent += count;
}

void
PeerDashboard::record_txset_reply(std::string const& tx_set_hash, size_t count)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = txset_acquisitions_.find(tx_set_hash);
    if (it == txset_acquisitions_.end())
        return;

    it->second.replies_received += count;
}

void
PeerDashboard::record_txset_error(std::string const& tx_set_hash)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = txset_acquisitions_.find(tx_set_hash);
    if (it == txset_acquisitions_.end())
        return;

    it->second.errors++;
}

void
PeerDashboard::record_txset_peers(
    std::string const& tx_set_hash,
    uint32_t peers_used)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = txset_acquisitions_.find(tx_set_hash);
    if (it == txset_acquisitions_.end())
        return;

    it->second.peers_used = peers_used;
}

void
PeerDashboard::record_txset_unique_counts(
    std::string const& tx_set_hash,
    uint32_t unique_requested,
    uint32_t unique_received)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = txset_acquisitions_.find(tx_set_hash);
    if (it == txset_acquisitions_.end())
        return;

    it->second.unique_requested = unique_requested;
    it->second.unique_received = unique_received;
}

std::optional<TxSetInfo>
PeerDashboard::get_current_txset() const
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    if (txset_acquisitions_.empty())
        return std::nullopt;

    // Find most recent completed, or most recent acquiring
    const TxSetInfo* best = nullptr;
    for (auto const& [hash, info] : txset_acquisitions_)
    {
        if (!best)
        {
            best = &info;
            continue;
        }

        // Prefer completed over acquiring
        if (info.status == TxSetStatus::Complete &&
            best->status != TxSetStatus::Complete)
        {
            best = &info;
        }
        else if (info.status == best->status)
        {
            // Same status - prefer more recent
            if (info.started_at > best->started_at)
                best = &info;
        }
    }

    return best ? std::optional<TxSetInfo>(*best) : std::nullopt;
}

std::optional<TxSetInfo>
PeerDashboard::get_txset(std::string const& hash) const
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = txset_acquisitions_.find(hash);
    if (it != txset_acquisitions_.end())
        return it->second;
    return std::nullopt;
}

void
PeerDashboard::record_proposal_txset_start(
    std::string const& tx_set_hash,
    std::string const& prev_ledger_hash,
    std::string const& validator_key)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = proposal_txsets_.find(tx_set_hash);
    if (it == proposal_txsets_.end())
    {
        // New proposal txset
        ProposalTxSet info;
        info.tx_set_hash = tx_set_hash;
        info.prev_ledger_hash = prev_ledger_hash;
        info.status = TxSetStatus::Acquiring;
        info.first_seen = std::chrono::steady_clock::now();
        info.proposing_validators.insert(validator_key);
        proposal_txsets_[tx_set_hash] = std::move(info);
    }
    else if (it->second.prev_ledger_hash != prev_ledger_hash)
    {
        // Same txset hash but DIFFERENT ledger round - reset counters
        it->second.prev_ledger_hash = prev_ledger_hash;
        it->second.status = TxSetStatus::Acquiring;
        it->second.type_histogram.clear();
        it->second.total_txns = 0;
        it->second.proposing_validators.clear();
        it->second.proposing_validators.insert(validator_key);
        it->second.first_seen = std::chrono::steady_clock::now();
    }
    else
    {
        // Same txset, same round - just add validator
        it->second.proposing_validators.insert(validator_key);
    }

    // LRU cleanup - remove oldest if over limit (skip when paused)
    if (!proposals_paused_.load())
    {
        while (proposal_txsets_.size() > MAX_TRACKED_TXSETS * 2)
        {
            auto oldest = proposal_txsets_.begin();
            for (auto iter = proposal_txsets_.begin();
                 iter != proposal_txsets_.end();
                 ++iter)
            {
                if (iter->second.first_seen < oldest->second.first_seen)
                    oldest = iter;
            }
            proposal_txsets_.erase(oldest);
        }
    }
}

void
PeerDashboard::record_proposal_txset_transaction(
    std::string const& tx_set_hash,
    uint16_t tx_type,
    uint32_t ledger_seq)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = proposal_txsets_.find(tx_set_hash);
    if (it == proposal_txsets_.end())
        return;

    it->second.type_histogram[tx_type]++;
    it->second.total_txns++;
    if (ledger_seq > 0)
        it->second.ledger_seqs.insert(ledger_seq);
}

void
PeerDashboard::record_proposal_txset_complete(
    std::string const& tx_set_hash,
    bool success)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = proposal_txsets_.find(tx_set_hash);
    if (it == proposal_txsets_.end())
        return;

    it->second.status = success ? TxSetStatus::Complete : TxSetStatus::Failed;
    it->second.completed_at = std::chrono::steady_clock::now();
}

std::vector<ProposalTxSet>
PeerDashboard::get_proposal_txsets(std::string const& prev_ledger_hash) const
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    std::vector<ProposalTxSet> result;
    for (auto const& [hash, info] : proposal_txsets_)
    {
        if (info.prev_ledger_hash == prev_ledger_hash)
            result.push_back(info);
    }

    // Sort by total_txns descending
    std::sort(result.begin(), result.end(), [](auto const& a, auto const& b) {
        return a.total_txns > b.total_txns;
    });

    return result;
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
            // Format with thousands separators using manual insertion
            // (avoids std::locale("") which makes a system call each time)
            auto s = std::to_string(num);
            int pos = static_cast<int>(s.size()) - 3;
            while (pos > 0)
            {
                s.insert(pos, ",");
                pos -= 3;
            }
            return s;
        };

        auto format_bytes = [](double bytes) -> std::string {
            const char* suffixes[] = {"B", "K", "M", "G", "T"};
            int i = 0;
            while (bytes > 1024 && i < 4)
            {
                bytes /= 1024;
                i++;
            }
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%.2f %s", bytes, suffixes[i]);
            return buf;
        };

        auto format_rate = [](double rate) -> std::string {
            char buf[32];
            std::snprintf(buf, sizeof(buf), "%.1f", rate);
            return buf;
        };

        auto format_elapsed = [](double seconds) -> std::string {
            int total_sec = static_cast<int>(seconds);
            char buf[16];
            std::snprintf(
                buf,
                sizeof(buf),
                "%02d:%02d:%02d",
                total_sec / 3600,
                (total_sec % 3600) / 60,
                total_sec % 60);
            return buf;
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

            // Main tab content — wrapped in lambda to skip when not visible
            auto render_main_tab = [&]() -> Element {
                // Connection status color - check for reconnecting state
                bool is_reconnecting = !all_peers.empty() &&
                    all_peers[0].reconnect_at.time_since_epoch().count() > 0 &&
                    all_peers[0].reconnect_at > now;
                Color status_color;
                std::string status_icon;

                if (is_connected)
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
                if (!all_peers.empty())
                {
                    auto since_last =
                        std::chrono::duration_cast<std::chrono::seconds>(
                            now - all_peers[0].last_packet_time)
                            .count();
                    receiving = all_peers[0].connected && since_last < 5;
                }
                std::string activity;
                if (receiving)
                {
                    activity = get_spinner() + " Receiving";
                }
                else if (is_reconnecting)
                {
                    // Compute live countdown for primary peer
                    auto remaining =
                        std::chrono::duration_cast<std::chrono::seconds>(
                            all_peers[0].reconnect_at - now)
                            .count();
                    if (remaining < 0)
                        remaining = 0;
                    activity = get_spinner() + " " + state + " in " +
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
                        text(format_number(last_validated->sequence)) | bold |
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
                                   now - last_validated->first_seen)
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
                        text(format_number(validating->sequence)) | bold |
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
                            ? format_number(proposals->ledger_seq)
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
                size_t peer_count = all_peers.size();
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
                        std::to_string(all_peers.size()) + ")") |
                    bold | color(Color::Cyan));
                peers_elements.push_back(separator());

                if (all_peers.empty())
                {
                    peers_elements.push_back(text("No connected peers") | dim);
                }
                else
                {
                    for (size_t i = 0; i < all_peers.size(); ++i)
                    {
                        const auto& peer = all_peers[i];

                        // Determine status based on reconnect_at time
                        bool is_reconnecting =
                            peer.reconnect_at.time_since_epoch().count() > 0 &&
                            peer.reconnect_at > now;
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
                                now - peer.last_packet_time)
                                .count();
                        bool peer_receiving =
                            peer.connected && peer_since_last < 3;
                        std::string activity_icon =
                            (peer_receiving || is_reconnecting) ? get_spinner()
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
                                                 peer.reconnect_at - now)
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
                            text(format_number(peer.total_packets)) | dim,
                            text(" pkts | "),
                            text(format_bytes(
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
                            color(
                                receiving ? Color::GreenLight
                                          : Color::GrayDark),
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
                        text(format_elapsed(elapsed)) | bold |
                            color(Color::Yellow),
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
                        text(format_rate(pps)) | bold |
                            color(Color::GreenLight),
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
                        text(format_rate(
                            elapsed > 0 ? total_pkts / elapsed : 0)) |
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
                        text(type) | size(WIDTH, EQUAL, 26) |
                            color(Color::Yellow),
                        text(format_number(count)) | size(WIDTH, EQUAL, 12),
                        text(format_rate(rate) + "/s") | size(WIDTH, EQUAL, 10),
                        text(format_bytes(static_cast<double>(type_bytes))) |
                            size(WIDTH, EQUAL, 10),
                        gauge(pct / 100.0f) | flex | color(Color::Blue),
                        text(" " + format_rate(pct) + "%") |
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
            };  // end render_main_tab

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

            // Proposals tab - two columns: LAST LEDGER (left) | CURRENT LEDGER
            // (right)
            auto render_proposals_tab = [&]() -> Element {
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
            };

            // Peers tab - grid view for per-peer stats
            auto render_peers_tab = [&]() -> Element {
                auto const& peers = all_peers;

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
                        peer.reconnect_at > now;
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
                                peer.reconnect_at - now)
                                .count();
                        if (remaining < 0)
                            remaining = 0;
                        state_label =
                            "Reconn " + std::to_string(remaining) + "s";
                    }
                    else
                        state_label = "Disconnected";

                    std::string last_pkt = "-";
                    if (peer.last_packet_time.time_since_epoch().count() > 0)
                    {
                        auto age =
                            std::chrono::duration_cast<std::chrono::seconds>(
                                now - peer.last_packet_time)
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
                            format_number(count).c_str());
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
                            peer.connected
                                ? format_elapsed(peer.elapsed_seconds)
                                : std::string("--:--:--")) |
                            bold,
                        text("  Last: ") | dim,
                        text(last_pkt),
                    }));
                    lines.push_back(hbox({
                        text("Pkts: ") | dim,
                        text(format_number(peer.total_packets)) | bold,
                        text("  Bytes: ") | dim,
                        text(format_bytes(
                            static_cast<double>(peer.total_bytes))) |
                            bold,
                    }));
                    lines.push_back(hbox({
                        text("Rate: ") | dim,
                        text(
                            format_rate(
                                peer.connected ? peer.packets_per_sec : 0.0) +
                            "/s") |
                            color(Color::GreenLight),
                        text("  ") | dim,
                        text(
                            format_bytes(
                                peer.connected ? peer.bytes_per_sec : 0.0) +
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
            };

            // Only render the active tab's content
            static const char* tab_names[] = {
                "Main", "Proposals", "Peers", "Commands"};
            auto render_start = std::chrono::steady_clock::now();

            Element content;
            switch (tab)
            {
                case 0:
                    content = render_main_tab();
                    break;
                case 1:
                    content = render_proposals_tab() | flex;
                    break;
                case 2:
                    content = render_peers_tab() | flex;
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
                    content = render_main_tab();
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
