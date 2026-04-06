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
#include <string>
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

    /// Call from unsolicited handler. Filters for type 41 (validation).
    void
    on_packet(uint16_t type, std::vector<uint8_t> const& data);

    /// Get the validations for the quorum ledger.
    std::vector<Entry>
    get_quorum(
        int percent = 90,
        QuorumMode mode = QuorumMode::live) const;

    bool
    has_quorum(
        int percent = 90,
        QuorumMode mode = QuorumMode::live) const;

    bool
    has_stale_vl_manifests() const
    {
        return !stale_vl_masters_.empty();
    }

    /// Parse a raw protobuf TMValidation and extract the STValidation.
    static std::vector<uint8_t>
    extract_stvalidation(std::vector<uint8_t> const& data);

    // Public state
    std::map<Hash256, std::vector<Entry>> by_ledger;
    std::set<std::string> unl_signing_keys;
    int unl_size = 0;
    Hash256 quorum_hash;
    bool quorum_reached = false;
    int quorum_count = 0;

    /// When true, on_packet() keeps collecting after quorum is reached.
    /// Used by ValidationBuffer for continuous collection.
    bool continuous = false;

private:
    catl::xdata::Protocol const& protocol_;
    std::string net_label_;
    std::set<std::string> unl_master_keys_;
    std::map<std::string, catl::vl::Manifest> peer_manifests_by_master_;
    std::map<std::string, std::string> vl_signing_to_master_;
    std::map<std::string, std::string> live_signing_to_master_;
    std::map<std::string, uint32_t> vl_manifest_sequence_by_master_;
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

    std::optional<Hash256>
    select_quorum_hash(int percent, QuorumMode mode) const;
};

}  // namespace xprv
