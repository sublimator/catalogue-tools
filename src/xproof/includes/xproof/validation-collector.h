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
#include <set>
#include <string>
#include <vector>

namespace xproof {

class ValidationCollector
{
public:
    struct Entry
    {
        std::vector<uint8_t> raw;          // full STValidation bytes
        std::vector<uint8_t> signing_key;  // sfSigningPubKey
        std::vector<uint8_t> signature;    // sfSignature
        Hash256 ledger_hash;               // sfLedgerHash
        uint32_t ledger_seq = 0;           // sfLedgerSequence
        bool sig_verified = false;
    };

    explicit ValidationCollector(catl::xdata::Protocol const& protocol);

    /// Set the UNL signing keys for quorum checking.
    void
    set_unl(std::vector<catl::vl::Manifest> const& validators);

    /// Call from unsolicited handler. Filters for type 41 (validation).
    void
    on_packet(uint16_t type, std::vector<uint8_t> const& data);

    /// Get the validations for the quorum ledger.
    std::vector<Entry> const&
    quorum_validations() const;

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

private:
    catl::xdata::Protocol const& protocol_;

    void
    check_all_for_quorum();

    void
    check_quorum_for(Hash256 const& hash);
};

}  // namespace xproof
