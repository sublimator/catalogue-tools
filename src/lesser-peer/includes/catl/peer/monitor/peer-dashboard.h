#pragma once

#include <atomic>
#include <catl/xdata/protocol.h>
#include <chrono>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace catl::peer::monitor {

// Information about a validator that has validated a ledger
struct ValidatorInfo
{
    std::string master_key_hex;  // from manifest (or ephemeral if no manifest)
    std::string ephemeral_key_hex;  // signing key from validation
    std::set<std::string>
        seen_from_peers;  // peer_ids that relayed this validator's validation
    std::chrono::steady_clock::time_point first_seen;
};

// Consensus state for a single ledger
struct LedgerConsensus
{
    uint32_t sequence = 0;
    std::string hash;
    std::map<std::string, ValidatorInfo> validators;  // master_key -> info
    std::chrono::steady_clock::time_point first_seen;
    bool is_validated = false;  // hit quorum
};

// Single proposal event in the timeline
struct ProposalEvent
{
    std::string validator_key;  // master key (resolved) or ephemeral
    std::string tx_set_hash;    // what they're proposing
    uint32_t propose_seq;       // 0=initial, >0=changed position
    std::set<std::string> seen_from_peers;
    std::chrono::steady_clock::time_point received_at;
};

// All proposals for current open ledger
struct LedgerProposals
{
    uint32_t ledger_seq =
        0;  // sequence being built (inferred from validations)
    std::string prev_ledger_hash;         // identifies the ledger being built
    std::vector<ProposalEvent> timeline;  // ordered by received_at
    std::chrono::steady_clock::time_point first_proposal;
    std::chrono::steady_clock::time_point
        last_validated_at;  // for "since validated"
};

// Transaction set acquisition status
enum class TxSetStatus { Pending, Acquiring, Complete, Failed };

// Proposal txset info (for Proposals tab)
struct ProposalTxSet
{
    std::string tx_set_hash;
    std::string prev_ledger_hash;  // Which round this belongs to
    TxSetStatus status = TxSetStatus::Pending;
    std::map<uint16_t, uint32_t> type_histogram;  // tx_type -> count
    std::set<uint32_t> ledger_seqs;  // LedgerSequence values seen (debug)
    uint32_t total_txns = 0;
    std::set<std::string> proposing_validators;  // Who proposed this txset
    std::chrono::steady_clock::time_point first_seen;
    std::chrono::steady_clock::time_point completed_at;
};

// Acquired transaction set with type histogram
struct TxSetInfo
{
    std::string tx_set_hash;
    std::string computed_hash;  // Hash computed from acquired txns (for debug)
    uint32_t ledger_seq = 0;    // Ledger being built (inferred from proposals)
    TxSetStatus status = TxSetStatus::Pending;
    std::map<uint16_t, uint32_t> type_histogram;  // tx_type -> count
    uint32_t total_txns = 0;
    uint32_t requests_sent = 0;     // Cumulative requests (includes retries)
    uint32_t replies_received = 0;  // Cumulative replies
    uint32_t errors = 0;            // Number of errors
    uint32_t peers_used = 0;        // Number of unique peers used
    uint32_t unique_requested = 0;  // Unique nodes requested
    uint32_t unique_received = 0;   // Unique nodes received
    std::chrono::steady_clock::time_point started_at;
    std::chrono::steady_clock::time_point completed_at;

    // Winner flag - set when this txset wins its round (consensus converged)
    bool is_winner = false;

    // Shuffle transaction details (type 88)
    std::map<uint32_t, uint32_t>
        shuffle_ledger_seqs;  // LedgerSequence -> count
    std::map<std::string, uint32_t>
        shuffle_parent_hashes;  // ParentHash (12 chars) -> count
};

class PeerDashboard
{
public:
    struct LedgerInfo
    {
        uint32_t sequence = 0;
        std::string hash;
        uint32_t validation_count = 0;
        std::chrono::steady_clock::time_point last_update;
    };

    struct Stats
    {
        // Peer identification
        std::string peer_id;  // Unique ID for multi-peer tracking

        // Connection info
        std::string peer_address;
        std::string peer_version;
        std::string network_id;
        std::string protocol_version;
        bool connected = false;

        // Packet counters by type
        std::map<std::string, uint64_t> packet_counts;
        std::map<std::string, uint64_t> packet_bytes;

        // Overall stats
        uint64_t total_packets = 0;
        uint64_t total_bytes = 0;
        double elapsed_seconds = 0;

        // Recent activity (last 60s)
        double packets_per_sec = 0;
        double bytes_per_sec = 0;

        // Connection state
        std::string connection_state = "Disconnected";
        std::chrono::steady_clock::time_point last_packet_time;

        // Ledger tracking
        LedgerInfo current_ledger;
        std::map<uint32_t, uint32_t> recent_ledgers;  // seq -> validation count
    };

    PeerDashboard();
    ~PeerDashboard();

    // Load protocol definitions from file (must be called before start())
    void
    load_protocol(std::string const& definitions_path, uint32_t network_id = 0);

    // Get protocol (for parsing transactions)
    xdata::Protocol const*
    get_protocol() const
    {
        return protocol_ ? &*protocol_ : nullptr;
    }

    // Restore terminal state (cursor, alternate screen, mouse modes)
    // Safe to call multiple times. Use as safety net on abnormal exit.
    static void
    restore_terminal();

    void
    start();
    void
    stop();

    // Single peer methods (for backward compatibility)
    void
    update_stats(const Stats& stats);
    Stats
    get_stats() const;

    // Multi-peer methods
    void
    update_peer_stats(const std::string& peer_id, const Stats& stats);
    void
    remove_peer(const std::string& peer_id);
    std::vector<Stats>
    get_all_peers_stats() const;

    // Ledger tracking (legacy - kept for compatibility)
    void
    update_ledger_info(
        uint32_t sequence,
        const std::string& hash,
        uint32_t validation_count);
    LedgerInfo
    get_current_ledger() const;

    // Consensus tracking (new)
    void
    record_validation(
        uint32_t ledger_seq,
        std::string const& ledger_hash,
        std::string const& validator_key,  // master key (resolved) or ephemeral
        std::string const& ephemeral_key,
        std::string const& peer_id);

    std::optional<LedgerConsensus>
    get_last_validated() const;

    std::optional<LedgerConsensus>
    get_validating() const;

    size_t
    get_known_validator_count() const;

    // Proposal tracking
    void
    record_proposal(
        std::string const& prev_ledger_hash,
        std::string const& tx_set_hash,
        std::string const& validator_key,  // master key (resolved) or ephemeral
        uint32_t propose_seq,
        std::string const& peer_id);

    std::optional<LedgerProposals>
    get_current_proposals() const;

    std::optional<LedgerProposals>
    get_last_proposals() const;

    // Transaction set acquisition tracking
    void
    record_txset_start(
        std::string const& tx_set_hash,
        std::string const& prev_ledger_hash);
    void
    record_txset_transaction(std::string const& tx_set_hash, uint16_t tx_type);
    void
    record_txset_shuffle_data(
        std::string const& tx_set_hash,
        uint32_t ledger_seq,
        std::string const& parent_hash);  // First 12 hex chars
    void
    record_txset_complete(std::string const& tx_set_hash, bool success);
    void
    record_txset_computed_hash(
        std::string const& tx_set_hash,
        std::string const& computed_hash);
    void
    record_txset_request(std::string const& tx_set_hash, size_t count = 1);
    void
    record_txset_reply(std::string const& tx_set_hash, size_t count = 1);
    void
    record_txset_error(std::string const& tx_set_hash);
    void
    record_txset_peers(std::string const& tx_set_hash, uint32_t peers_used);
    void
    record_txset_unique_counts(
        std::string const& tx_set_hash,
        uint32_t unique_requested,
        uint32_t unique_received);
    std::optional<TxSetInfo>
    get_current_txset() const;  // Most recent completed or acquiring
    std::optional<TxSetInfo>
    get_txset(std::string const& hash) const;

    // Proposal txset tracking (for Proposals tab)
    void
    record_proposal_txset_start(
        std::string const& tx_set_hash,
        std::string const& prev_ledger_hash,
        std::string const& validator_key);
    void
    record_proposal_txset_transaction(
        std::string const& tx_set_hash,
        uint16_t tx_type,
        uint32_t ledger_seq);
    void
    record_proposal_txset_complete(
        std::string const& tx_set_hash,
        bool success);
    std::vector<ProposalTxSet>
    get_proposal_txsets(std::string const& prev_ledger_hash) const;

    // Discovered peer endpoints
    void
    update_available_endpoints(std::vector<std::string> const& endpoints);
    std::vector<std::string>
    get_available_endpoints() const;

    // Shutdown callback - called when user quits the dashboard
    using shutdown_callback_t = std::function<void()>;
    void
    set_shutdown_callback(shutdown_callback_t callback)
    {
        shutdown_callback_ = std::move(callback);
    }

    // Check if a txset needs to be acquired (returns hash and ledger_seq if
    // convergence detected and not yet acquired)
    std::optional<std::pair<std::string, uint32_t>>
    get_txset_to_acquire() const;

    std::atomic<uint64_t> ui_render_counter_{0};  // UI thread heartbeat

    // Debug counters for proposals
    std::atomic<uint64_t> proposals_received_{0};
    std::atomic<uint64_t> proposals_seq0_{0};
    std::atomic<uint64_t> proposals_seq_gt0_{0};
    std::atomic<uint64_t> proposals_ignored_{0};
    std::atomic<uint64_t> proposal_rounds_count_{
        0};  // Number of unique prev_hash rounds

    // Request UI thread to exit (thread-safe)
    void
    request_exit();

private:
    void
    run_ui();

    // Protocol definitions for transaction type names
    std::optional<xdata::Protocol> protocol_;
    std::string protocol_source_;  // Path or "embedded"

    // UI thread
    std::unique_ptr<std::thread> ui_thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> exit_requested_{false};

    // Multi-peer tracking
    mutable std::mutex peers_mutex_;
    std::map<std::string, Stats> peer_stats_;  // peer_id -> Stats

    // Backward compatibility - default peer
    std::string default_peer_id_{"default"};

    // Connection info (deprecated - kept for backward compatibility)
    std::string peer_address_;
    std::string peer_version_;
    std::string network_id_;
    std::string protocol_version_;
    std::atomic<bool> connected_{false};

    // Packet stats (deprecated - kept for backward compatibility)
    mutable std::mutex packet_mutex_;
    std::map<std::string, uint64_t> packet_counts_;
    std::map<std::string, uint64_t> packet_bytes_;

    // Overall stats (deprecated - kept for backward compatibility)
    std::atomic<uint64_t> total_packets_{0};
    std::atomic<uint64_t> total_bytes_{0};
    std::atomic<double> elapsed_seconds_{0};

    // Throughput tracking (deprecated - kept for backward compatibility)
    struct ThroughputSample
    {
        std::chrono::steady_clock::time_point timestamp;
        uint64_t packets;
        uint64_t bytes;
    };

    mutable std::mutex throughput_mutex_;
    std::deque<ThroughputSample> throughput_history_;

    // Connection state (deprecated - kept for backward compatibility)
    mutable std::mutex state_mutex_;
    std::string connection_state_{"Disconnected"};
    std::chrono::steady_clock::time_point last_packet_time_;

    // Global ledger tracking (legacy)
    mutable std::mutex ledger_mutex_;
    LedgerInfo current_validated_ledger_;
    std::map<uint32_t, uint32_t>
        legacy_ledger_counts_;  // seq -> count (legacy)

    // Consensus tracking - map-based storage for async-friendly accumulation
    mutable std::mutex consensus_mutex_;

    // LRU limit for tracked rounds
    static constexpr size_t MAX_TRACKED_ROUNDS = 10;

    // Storage for multiple ledger consensus rounds (keyed by sequence)
    // All validations accumulate here, display layer picks what to show
    std::map<uint32_t, LedgerConsensus>
        ledger_validations_;  // seq -> consensus

    // Storage for multiple proposal rounds (keyed by prev_ledger_hash)
    // All proposals accumulate here, display layer picks what to show
    std::map<std::string, LedgerProposals>
        proposal_rounds_;  // prev_hash -> proposals

    // Storage for transaction set acquisitions (keyed by tx_set_hash)
    static constexpr size_t MAX_TRACKED_TXSETS = 200;
    std::map<std::string, TxSetInfo> txset_acquisitions_;  // tx_hash -> info

    // Proposal txsets (for Proposals tab display)
    // Keyed by tx_set_hash, shows what txns each proposed set contains
    std::map<std::string, ProposalTxSet> proposal_txsets_;  // tx_hash -> info

    // Known validators (for quorum calculation)
    std::set<std::string>
        known_validators_;  // all validators seen (for quorum)
    std::map<std::string, std::string>
        peer_to_validator_;  // peer_id -> master_key (if detected)

    // LRU cleanup helper
    void
    prune_old_rounds();

    // Helper to infer ledger sequence from prev_hash
    uint32_t
    infer_ledger_seq(std::string const& prev_hash) const;

    // Helper to find proposals for a specific ledger
    std::optional<LedgerProposals>
    get_proposals_for_ledger(uint32_t seq) const;

    // Internal impl methods - must be called with consensus_mutex_ held
    std::optional<LedgerConsensus>
    get_last_validated_impl() const;
    std::optional<LedgerConsensus>
    get_validating_impl() const;

    // Known peer endpoints (mtENDPOINTS)
    mutable std::mutex endpoints_mutex_;
    std::vector<std::string> available_endpoints_;

    // Tab navigation
    enum class Tab { Main = 0, Proposals = 1 };
    std::atomic<int> current_tab_{0};

    // Proposals tab pause feature
    std::atomic<bool> proposals_paused_{false};
    mutable std::mutex pause_mutex_;
    std::string paused_prev_hash_;       // Current round we're paused on
    std::string paused_last_prev_hash_;  // Previous round (for LAST LEDGER)

    // Shutdown callback
    shutdown_callback_t shutdown_callback_;
};

}  // namespace catl::peer::monitor
