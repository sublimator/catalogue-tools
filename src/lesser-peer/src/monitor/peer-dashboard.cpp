#include <algorithm>
#include <catl/core/logger.h>
#include <catl/peer/monitor/peer-dashboard.h>
#include <cmath>
#include <ftxui/component/component.hpp>
#include <ftxui/component/loop.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>
#include <limits>

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
