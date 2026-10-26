#pragma once

// Collects STValidation messages from the peer, grouped by ledger hash.
// Tracks which signing keys have validated each ledger. When a UNL is
// provided, signals quorum when enough UNL validators have validated
// the same ledger.

#include <catl/core/logger.h>
#include <catl/core/types.h>
#include <catl/vl-client/vl-client.h>
#include <catl/xdata/protocol.h>

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace xprv {

class ValidationCollector
{
public:
    enum class QuorumMode { live, proof };

    struct Entry
    {
        std::vector<uint8_t> raw;          // full STValidation bytes
        std::vector<uint8_t> signing_key;  // sfSigningPubKey
        std::string signing_key_hex;       // lowercase hex of sfSigningPubKey
        std::vector<uint8_t> signature;    // sfSignature
        Hash256 ledger_hash;               // sfLedgerHash
        uint32_t ledger_seq = 0;           // sfLedgerSequence
        bool sig_verified = false;
    };

    explicit ValidationCollector(
        catl::xdata::Protocol const& protocol,
        uint32_t network_id = 0);

    /// Set the UNL signing keys for quorum checking.
    void
    set_unl(std::vector<catl::vl::Manifest> const& validators);

    /// Keep the newest manifest of a publisher whose list was loaded, for
    /// held_manifest_bytes(). A list from a site serving an older publisher
    /// manifest doesn't replace a newer one. Empty raw bytes are ignored.
    /// Returns true when the held manifest changed.
    bool
    note_publisher_manifest(catl::vl::Manifest const& manifest);

    /// Call from unsolicited handler. Filters for type 41 (validation).
    void
    on_packet(uint16_t type, std::vector<uint8_t> const& data);

    /// Get all distinct validations currently available for the selected
    /// quorum ledger under the requested mode.
    std::vector<Entry>
    get_quorum(int percent = 90, QuorumMode mode = QuorumMode::live) const;

    /// Get the newest quorum whose signed ledger sequence does not exceed
    /// max_ledger_sequence. This lets proof-anchor selection compose
    /// validation authority with a separately observed transport boundary.
    std::vector<Entry>
    get_quorum_at_or_before(
        uint32_t max_ledger_sequence,
        int percent = 90,
        QuorumMode mode = QuorumMode::live) const;

    /// Get all distinct validations currently available for a specific ledger
    /// hash under the requested mode.
    std::vector<Entry>
    get_ledger_validations(
        Hash256 const& ledger_hash,
        QuorumMode mode = QuorumMode::live) const;

    /// Get distinct validations for one exact ledger identity. A signed
    /// sequence claim is part of that identity; callers must not combine
    /// contradictory sequence claims for the same hash into one quorum.
    std::vector<Entry>
    get_ledger_validations(
        Hash256 const& ledger_hash,
        uint32_t ledger_sequence,
        QuorumMode mode) const;

    /// Return the greatest sequence claimed by a validation that is usable
    /// under the requested quorum mode, without copying retained entries.
    std::optional<uint32_t>
    latest_ledger_sequence(
        Hash256 const& ledger_hash,
        QuorumMode mode = QuorumMode::live) const;

    /// Prune retained hashes to a bounded capacity derived from the fixed
    /// floor and the number of trusted identities.
    void
    prune_ledgers(std::size_t max_ledgers);

#ifdef XPRV_TESTING
    bool
    insert_entry_for_test(Entry entry)
    {
        return insert_entry(std::move(entry));
    }
#endif

    bool
    has_quorum(int percent = 90, QuorumMode mode = QuorumMode::live) const;

    bool
    has_stale_vl_manifests() const
    {
        return !stale_vl_masters_.empty();
    }

    /// Parse a raw protobuf TMValidation and extract the STValidation.
    static std::vector<uint8_t>
    extract_stvalidation(std::vector<uint8_t> const& data);

    /// Verify an STValidation's signature against its embedded SigningPubKey
    /// (sec #0053). Rebuilds the rippled signing serialization — the
    /// "VAL\0" hash prefix followed by the object with the sfSignature field
    /// excised — and checks sfSignature over it. Returns false on any parse
    /// failure, missing field, or signature mismatch. Static + self-contained
    /// so it can be unit-tested against captured validations.
    static bool
    verify_validation_signature(
        std::vector<uint8_t> const& stvalidation,
        catl::xdata::Protocol const& protocol);

    struct ManifestPacketResult
    {
        std::size_t total_entries = 0;
        std::size_t trusted_entries = 0;
        std::vector<catl::vl::Manifest> manifests;
    };

    /// Parse a manifests packet and authenticate at most one candidate per
    /// trusted master key. The result is safe to apply on the collector's
    /// owning strand.
    static ManifestPacketResult
    parse_trusted_manifests_packet(
        std::vector<uint8_t> const& data,
        std::set<std::string> const& trusted_master_keys,
        catl::xdata::Protocol const& protocol);

    std::set<std::string>
    trusted_master_keys() const
    {
        return unl_master_keys_;
    }

    void
    apply_peer_manifests(std::vector<catl::vl::Manifest> manifests);

    /// The §1.3 material this server holds, for verifiers on this server:
    /// per master, the newest manifest from any list this process has loaded
    /// or a peer (so a list-borne revocation reaches them too), plus the one
    /// conflicting pair for each equivocated master (so they drop the same
    /// validator), plus the newest manifest of each publisher whose list was
    /// loaded (so a superseded publisher key is refused there too).
    /// Not filtered by the current list: an older proof's own primary list
    /// can still name a master the current list dropped, and a verifier's
    /// rule 1 already filters by that proof's list. Peers' manifests
    /// are only stored for masters that were listed, so this stays bounded.
    std::vector<std::vector<uint8_t>>
    held_manifest_bytes() const
    {
        std::set<std::string> masters(unl_master_keys_);
        for (auto const& [master, _] : peer_manifests_by_master_)
            masters.insert(master);
        for (auto const& [master, _] : list_manifests_seen_)
            masters.insert(master);
        for (auto const& [master, _] : equivocation_evidence_)
            masters.insert(master);
        std::vector<std::vector<uint8_t>> out;
        for (auto const& master : masters)
        {
            // Newest of: any list this process has applied (kept across list
            // updates, so a list-borne revocation survives the master leaving
            // the list) and peers.
            std::optional<std::pair<uint32_t, std::vector<uint8_t>>> newest;
            auto const consider = [&](uint32_t seq, std::vector<uint8_t> const& raw) {
                if (!raw.empty() && (!newest || seq > newest->first))
                    newest.emplace(seq, raw);
            };
            if (auto it = list_manifests_seen_.find(master);
                it != list_manifests_seen_.end())
                consider(it->second.first, it->second.second);
            if (auto it = peer_manifests_by_master_.find(master);
                it != peer_manifests_by_master_.end())
                consider(it->second.sequence, it->second.raw);
            if (newest)
                out.push_back(newest->second);

            auto const evidence = equivocation_evidence_.find(master);
            if (evidence != equivocation_evidence_.end())
                out.insert(
                    out.end(), evidence->second.begin(), evidence->second.end());
        }
        for (auto const& [_, seen] : publisher_manifests_seen_)
            out.push_back(seen.second);
        return out;
    }

    // Public state
    std::map<Hash256, std::vector<Entry>> by_ledger;
    std::set<std::string> unl_signing_keys;
    int unl_size = 0;
    Hash256 quorum_hash;
    bool quorum_reached = false;
    int quorum_count = 0;

    /// When true, on_packet() keeps collecting after quorum is reached.
    /// Used by ValidationQuorumTracker for continuous collection.
    bool continuous = false;

private:
    // Remove every signing-key mapping of an equivocated master (§1.3 rule 2).
    void
    drop_equivocated_mappings()
    {
        for (auto* mapping : {&live_signing_to_master_, &vl_signing_to_master_})
        {
            for (auto it = mapping->begin(); it != mapping->end();)
            {
                if (equivocated_masters_.count(it->second))
                    it = mapping->erase(it);
                else
                    ++it;
            }
        }
    }

    catl::xdata::Protocol const& protocol_;
    std::string net_label_;
    std::set<std::string> unl_master_keys_;
    std::map<std::string, catl::vl::Manifest> peer_manifests_by_master_;
    std::map<std::string, std::string> vl_signing_to_master_;
    std::map<std::string, std::string> live_signing_to_master_;
    std::map<std::string, uint32_t> vl_manifest_sequence_by_master_;
    // Raw bytes of each master's manifest in the current list, per sequence,
    // to detect same-sequence equivocation against the list.
    std::map<std::string, std::vector<uint8_t>> vl_manifest_raw_by_master_;
    // Newest manifest (sequence, raw) each master has had in any list applied
    // in this process. Not cleared by set_unl, so a list-borne revocation stays
    // available to local verifiers after a newer list drops the master.
    std::map<std::string, std::pair<uint32_t, std::vector<uint8_t>>>
        list_manifests_seen_;
    // Newest manifest (sequence, raw) per publisher master, from the lists
    // loaded in this process. One publisher per network in practice (D5).
    std::map<std::string, std::pair<uint32_t, std::vector<uint8_t>>>
        publisher_manifests_seen_;
    // §1.3 rule 2: masters seen signing two different manifests at one
    // sequence. They stay in N and never vote again in this process. Exactly
    // one conflicting pair per master is kept (bounded; replays add nothing),
    // and it survives list refreshes so local verifiers keep dropping it.
    std::set<std::string> equivocated_masters_;
    std::map<std::string, std::vector<std::vector<uint8_t>>>
        equivocation_evidence_;
    std::map<std::string, uint32_t> manifest_sequence_by_master_;
    std::set<std::string> stale_vl_masters_;

    void
    filter_buffer_to_unl();

    void
    recompute_quorum_state(int percent = 90);

    bool
    insert_entry(Entry entry);

    void
    handle_manifests_packet(std::vector<uint8_t> const& data);

    bool
    apply_manifest(catl::vl::Manifest const& manifest, bool from_vl);

    std::string
    entry_key_hex(Entry const& entry, QuorumMode mode) const;

    int
    threshold_for(int percent) const;

    struct QuorumCandidate
    {
        Hash256 ledger_hash;
        uint32_t ledger_sequence = 0;
        int count = 0;
    };

    std::optional<QuorumCandidate>
    select_quorum_candidate(
        int percent,
        QuorumMode mode,
        std::optional<uint32_t> max_ledger_sequence = std::nullopt) const;

    static constexpr std::size_t kMaxRetainedVotesPerValidator = 4;
};

}  // namespace xprv
