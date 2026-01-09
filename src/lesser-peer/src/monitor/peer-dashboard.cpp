#include <catl/peer/monitor/peer-dashboard.h>
#include <cmath>
#include <cstdio>
#include <ftxui/component/component.hpp>
#include <ftxui/component/loop.hpp>
#include <ftxui/component/screen_interactive.hpp>
#include <ftxui/dom/elements.hpp>
#include <ftxui/screen/color.hpp>
#include <ftxui/screen/terminal.hpp>
#include <iomanip>
#include <sstream>

namespace catl::peer::monitor {

using namespace ftxui;

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
    }
    else
    {
        protocol_ = xdata::Protocol::load_embedded_xahau_protocol(opts);
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

    // Track which peer this validator's validations came from
    peer_to_validator_[peer_id] = validator_key;

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
        // New validator for this ledger
        ValidatorInfo info;
        info.master_key_hex = validator_key;
        info.ephemeral_key_hex = ephemeral_key;
        info.seen_from_peers.insert(peer_id);
        info.first_seen = now;
        validators[validator_key] = std::move(info);
    }
    else
    {
        // Already have this validator, just add the peer
        it->second.seen_from_peers.insert(peer_id);
    }

    // Check quorum: 80% of known validators (XRPL consensus threshold)
    size_t quorum =
        static_cast<size_t>(std::ceil(known_validators_.size() * 0.8));
    if (validators.size() >= quorum && !consensus.is_validated)
    {
        consensus.is_validated = true;
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
PeerDashboard::record_proposal(
    std::string const& prev_ledger_hash,
    std::string const& tx_set_hash,
    std::string const& validator_key,
    uint32_t propose_seq,
    std::string const& peer_id)
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
    uint32_t ledger_seq)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    // Check if already tracking
    if (txset_acquisitions_.find(tx_set_hash) != txset_acquisitions_.end())
        return;

    // Create new entry
    TxSetInfo info;
    info.tx_set_hash = tx_set_hash;
    info.ledger_seq = ledger_seq;
    info.status = TxSetStatus::Acquiring;
    info.started_at = std::chrono::steady_clock::now();
    txset_acquisitions_[tx_set_hash] = std::move(info);

    // LRU cleanup - remove oldest if over limit
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
PeerDashboard::record_txset_request(std::string const& tx_set_hash)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = txset_acquisitions_.find(tx_set_hash);
    if (it == txset_acquisitions_.end())
        return;

    it->second.requests_sent++;
}

void
PeerDashboard::record_txset_reply(std::string const& tx_set_hash)
{
    std::lock_guard<std::mutex> lock(consensus_mutex_);

    auto it = txset_acquisitions_.find(tx_set_hash);
    if (it == txset_acquisitions_.end())
        return;

    it->second.replies_received++;
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
                        text(last_validated->hash.substr(0, 16) + "...") | bold,
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
                    for (auto const& [validator, seq_hash] : validator_latest)
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
                        text(std::to_string(unique_txsets.size()) + " tx sets"),
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
                        // Only update hash/seq if this is a higher proposeSeq
                        // (handles out-of-order arrival)
                        if (event.propose_seq >= vp.latest_seq)
                        {
                            vp.latest_hash = event.tx_set_hash;
                            vp.latest_seq = event.propose_seq;
                        }
                        // Always accumulate peers and timings
                        for (auto const& p : event.seen_from_peers)
                            vp.all_peers.insert(p);
                        auto delta_ms =
                            std::chrono::duration_cast<
                                std::chrono::milliseconds>(
                                event.received_at - proposals->first_proposal)
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
                        std::string short_key =
                            key.size() > 10 ? key.substr(0, 10) + "..." : key;
                        std::string short_hash = vp.latest_hash.size() > 8
                            ? vp.latest_hash.substr(0, 8)
                            : vp.latest_hash;

                        std::string peers_str;
                        for (auto const& p : vp.all_peers)
                        {
                            if (!peers_str.empty())
                                peers_str += ",";
                            auto pos = p.find('-');
                            if (pos != std::string::npos && pos + 1 < p.size())
                                peers_str += p.substr(pos + 1);
                            else
                                peers_str += p.substr(0, 2);
                        }

                        std::string timings_str;
                        for (auto t : vp.timings)
                        {
                            if (!timings_str.empty())
                                timings_str += ",";
                            std::stringstream ss;
                            ss << std::fixed << std::setprecision(1) << t;
                            timings_str += ss.str();
                        }

                        bool is_different = !majority_hash.empty() &&
                            vp.latest_hash != majority_hash;
                        Color hash_color =
                            is_different ? Color::RedLight : Color::GreenLight;

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
                            is_different ? (text(" !") | color(Color::RedLight))
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
            // validators Proposal section: header(1) + sep(1) + summary(1) +
            // sep(1) + validators
            size_t peer_count = all_peers.size();
            int validation_height = 6 + static_cast<int>(peer_count);
            int proposal_height = 4 + static_cast<int>(peer_count);

            // Render txset panel - show recent acquired txsets
            auto render_txset = [&]() -> Elements {
                Elements elements;
                elements.push_back(
                    text("📦 ACQUIRED TX SETS") | bold | color(Color::Magenta));
                elements.push_back(separator());

                // Get all txsets, sorted by completion time (most recent first)
                std::vector<TxSetInfo> all_txsets;
                {
                    std::lock_guard<std::mutex> lock(consensus_mutex_);
                    for (auto const& [hash, info] : txset_acquisitions_)
                    {
                        all_txsets.push_back(info);
                    }
                }

                if (all_txsets.empty())
                {
                    elements.push_back(text("No tx sets acquired yet") | dim);
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
                auto get_tx_type_name = [this](uint16_t type) -> std::string {
                    if (protocol_)
                    {
                        auto name = protocol_->get_transaction_type_name(type);
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

                    // Compact single-line format: L{seq} hash [status] count
                    // txns: type1=N
                    std::string types_str;
                    for (auto const& [type, count] : txset.type_histogram)
                    {
                        std::string name = get_tx_type_name(type);
                        if (!types_str.empty())
                            types_str += ", ";
                        types_str += name + "=" + std::to_string(count);
                    }

                    // Ledger seq prefix
                    std::string seq_str = txset.ledger_seq > 0
                        ? "L" + std::to_string(txset.ledger_seq) + " "
                        : "";

                    // Format: req/reply stats + peers used + errors
                    std::string io_str = "r" +
                        std::to_string(txset.requests_sent) + "/" +
                        std::to_string(txset.replies_received);
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
                        text(types_str.empty() ? "" : ": " + types_str) | dim,
                    }));
                }

                return elements;
            };

            auto txset_elements = render_txset();

            // Combine into layout - validations on top, proposals below
            // Calculate widths: main columns are 50/50, inner panels are 50/50
            // of that
            auto term_size = Terminal::Size();
            int main_half =
                (term_size.dimx - 4) / 2;          // -4 for borders/separators
            int inner_half = (main_half - 1) / 2;  // -1 for inner separator

            auto consensus_section = vbox({
                hbox({
                    vbox(last_validated_elements) |
                        size(WIDTH, EQUAL, inner_half) |
                        size(HEIGHT, EQUAL, validation_height),
                    separator(),
                    vbox(validating_elements) | size(WIDTH, EQUAL, inner_half) |
                        size(HEIGHT, EQUAL, validation_height),
                }),
                separator(),
                hbox({
                    vbox(last_proposed_elements) |
                        size(WIDTH, EQUAL, inner_half) |
                        size(HEIGHT, EQUAL, proposal_height),
                    separator(),
                    vbox(proposing_elements) | size(WIDTH, EQUAL, inner_half) |
                        size(HEIGHT, EQUAL, proposal_height),
                }),
                separator(),
                vbox(txset_elements) | size(HEIGHT, LESS_THAN, 12),
            });

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
                for (size_t i = 0; i < all_peers.size(); ++i)
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

            // Layout - use explicit 50/50 width for main columns
            return vbox({
                       text("XRPL Peer Monitor Dashboard") | bold | hcenter |
                           color(Color::MagentaLight),
                       separator(),
                       hbox({
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
                           }) | size(WIDTH, EQUAL, main_half) |
                               border,
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
    catch (...)
    {
        // Restore terminal on any error
        restore_terminal();
        running_ = false;
    }
}

}  // namespace catl::peer::monitor
