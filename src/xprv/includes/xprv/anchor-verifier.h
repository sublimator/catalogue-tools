#pragma once

// AnchorVerifier — verifies the anchor step of a proof chain.
//
// Given the anchor JSON object and a trusted VL publisher key,
// walks through every cryptographic step:
//   1. Publisher key matches trusted key
//   2. Publisher manifest signature (MAN\0 prefix, master key signs)
//   3. Blob signature (raw blob bytes, publisher signing key signs)
//   4. Each validator manifest in the UNL blob
//   5. Each STValidation signature (VAL\0 prefix, signing key signs)
//   6. Quorum check
//
// Appends detailed narrative to an explanation buffer.

#include <catl/core/types.h>
#include <catl/xdata/protocol.h>

#include <boost/json.hpp>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace xprv {

/// rippled's vfFullValidation. Only validations with this flag count toward a
/// ledger's quorum; a partial validation never does.
inline constexpr uint32_t kFullValidationFlag = 0x00000001;

bool
validator_list_active_at(
    uint32_t effective,
    uint32_t expiration,
    uint32_t ledger_close_time);

struct AnchorVerifyResult
{
    bool verified = false;
    int unl_size = 0;
    int unl_revoked = 0;   // revoked masters: counted in unl_size, never vote
    int unl_overlaid = 0;  // delegations updated by manifests held outside
    int unl_equivocated =
        0;  // masters with conflicting same-sequence manifests
    int validations_total = 0;
    int validations_verified = 0;
    int validations_partial = 0;
    int validations_matched_unl = 0;
    uint32_t vl_sequence = 0;
    uint32_t vl_effective = 0;
    uint32_t vl_expiration = 0;
    std::string error;
    std::vector<std::string> warnings;  // reported, never a rejection
};

/// A signed validator-list candidate held from outside the proof (§1.3).
/// Extracted from an XRPL VL site document (version 1 or version 2 `blobs_v2`).
struct ExternalValidatorList
{
    std::string public_key;  // optional hex master key from the site document
    std::vector<uint8_t> manifest;   // raw publisher manifest bytes
    std::vector<uint8_t> blob;       // raw signed list JSON bytes
    std::vector<uint8_t> signature;  // raw signature over blob
};

/// Expand an XRPL VL site document (version 1 or version 2), or a JSON array
/// of such documents, into candidate lists (§1.3 "Sources and packaging").
/// Throws std::runtime_error if the outer JSON is not a document object or
/// array of document objects; malformed individual candidates inside a
/// document are skipped so one broken entry cannot poison others.
std::vector<ExternalValidatorList>
parse_external_validator_lists(boost::json::value const& json);

class AnchorVerifier
{
public:
    /// Verify the anchor step.
    /// @param anchor    The anchor JSON object from the proof chain
    /// @param trusted_key  Hex string of the trusted VL publisher master key
    /// @param narrative Output: explanation lines appended here
    /// @param extra_manifests  Raw manifests held from outside the proof
    ///        (§1.3). They only update keys of masters the list names.
    /// @param extra_lists  Signed validator-list candidates held from outside
    ///        the proof (§1.3, §3.3).
    /// @param anchor_close_time  The matching anchor ledger header's close_time
    ///        when the proof carries descendants (§1.3 P3), or nullopt for an
    ///        anchor-only proof.
    /// @return Verification result
    static AnchorVerifyResult
    verify(
        boost::json::object const& anchor,
        std::string const& trusted_key,
        catl::xdata::Protocol const& protocol,
        std::vector<std::string>& narrative,
        std::vector<std::vector<uint8_t>> const& extra_manifests =
            std::vector<std::vector<uint8_t>>(),
        std::vector<ExternalValidatorList> const& extra_lists =
            std::vector<ExternalValidatorList>(),
        std::optional<uint32_t> anchor_close_time = std::nullopt);

    /// True when `raw` is a complete canonical XRPL `STValidation`: it
    /// round-trips under the XRPL codec and carries `LedgerHash`, a `Signature`
    /// field (the blob may be empty), and a 33-byte `SigningPubKey` whose
    /// prefix is `0xED`, `0x02`, or `0x03`.
    static bool
    is_canonical_stvalidation(std::vector<uint8_t> const& raw);

    /// Fingerprint of the definitions validations and manifests are read
    /// with: the XRPL table, on every network (definitions_fingerprint in
    /// proof-resolver.h).
    static std::string
    validation_definitions_fingerprint();

    /// The 33-byte `SigningPubKey` of a complete canonical `STValidation`,
    /// or nullopt when `raw` is not one.
    static std::optional<std::vector<uint8_t>>
    canonical_validation_signing_key(std::vector<uint8_t> const& raw);

#ifdef XPRV_TESTING
    /// §5.1 body: canonical bytes with only `Signature` removed. Empty when
    /// `raw` is not a canonical STValidation.
    static std::vector<uint8_t>
    validation_signing_body_for_test(std::vector<uint8_t> const& raw);
#endif

private:
    /// Parse a raw STValidation, strip sfSignature, return components.
    struct ParsedValidation
    {
        Hash256 ledger_hash;
        std::vector<uint8_t> signing_key;
        std::vector<uint8_t> signature;
        std::vector<uint8_t> without_signature;  // raw bytes sans sfSignature
        std::optional<uint32_t> flags;           // sfFlags, if present
        std::optional<uint32_t> ledger_seq;      // sfLedgerSequence, if present
        bool valid = false;
    };

    static ParsedValidation
    parse_validation(std::vector<uint8_t> const& raw);

    /// Decode hex string to bytes.
    static std::vector<uint8_t>
    from_hex(std::string_view hex);

    /// Bytes to uppercase hex string.
    static std::string
    to_hex(std::span<const uint8_t> data);
};

}  // namespace xprv
