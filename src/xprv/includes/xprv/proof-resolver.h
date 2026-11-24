#pragma once

// Proof chain resolver / verifier.
//
// Operates on ProofChain structs (format-agnostic).
// The caller loads from JSON via from_json_text() or binary via from_binary().
//
// Verifies each step:
//   - Anchor: UNL publisher key, blob signature, validation signatures
//   - Ledger headers: SHA512Half hash computation
//   - Map proofs: trie root hash reconstruction
//   - Skip lists: hash lookup between steps

#include "proof-chain.h"

#include <boost/json.hpp>
#include <catl/xdata/protocol.h>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace xprv {

/// A ledger header a proof authenticated, and what supported it (§7).
struct VerifiedHeader
{
    Hash256 ledger_hash;
    HeaderData header;
    std::string supported_by;  // "anchor", "parent_hash" or "ledger_hashes"
};

/// A leaf a proof authenticated (§7): its tree, its full key, the exact
/// bytes that were hashed, and every authenticated ledger whose root holds
/// it. A consumer binds its claim to these, not to anything a step says.
struct VerifiedLeaf
{
    bool is_tx = false;
    Hash256 key;
    std::vector<uint8_t> bytes;
    std::vector<std::pair<Hash256, uint32_t>> ledgers;  // hash, index
};

/// What verification established. The outputs are filled only when the
/// whole proof verified. A refusal carries its layer and reason, and
/// nothing a consumer could mistake for authenticated data.
struct VerifyReport
{
    bool verified = false;
    std::string layer;   // on refusal: "parse", "graph" or "trust"
    std::string reason;  // on refusal
    uint32_t network_id = 0;
    // Fingerprints of the loaded field definitions (§7): the table the
    // leaves were decoded with, and the table the validations were read
    // with. They differ on Xahau, whose validations use the XRPL table.
    std::string leaf_definitions;
    std::string validation_definitions;
    Hash256 anchor_hash;
    int votes = 0;      // list masters whose validation counted
    int list_size = 0;  // N
    std::vector<VerifiedHeader> headers;
    std::vector<VerifiedLeaf> leaves;
};

/// Resolve and verify a proof chain.
/// Verifies the anchor's UNL and validation signatures against the required
/// trusted publisher key.
/// Protocol is needed to serialize leaf JSON back to binary for hash
/// verification.
/// Returns true if all verifiable steps pass.
/// `extra_manifests` are raw validator/publisher manifests held from outside
/// the proof (§1.3 trust view); they only update keys of listed masters.
/// When `failure` is given, it receives the first refusal's reason. When
/// `report` is given, it receives the outputs, or the refusal's layer.
bool
resolve_proof_chain(
    ProofChain const& chain,
    catl::xdata::Protocol const& protocol,
    std::string const& trusted_publisher_key,
    std::vector<std::vector<uint8_t>> const& extra_manifests =
        std::vector<std::vector<uint8_t>>(),
    std::string* failure = nullptr,
    VerifyReport* report = nullptr);

/// What a verifier is configured to trust for one network (§1.3, §8): its
/// primary publisher key, the definitions it decodes that network with, and
/// the newer validator manifests it holds.
struct VerifyTrust
{
    std::string publisher_key;
    catl::xdata::Protocol const* protocol = nullptr;
    std::shared_ptr<std::vector<std::vector<uint8_t>> const> held_manifests;
};

/// The trust configured for a network. It returns nullopt, or throws with
/// its own reason, for a network the verifier is not configured for.
using TrustForNetworkFn =
    std::function<std::optional<VerifyTrust>(uint32_t network_id)>;

/// Read a proof in either form and verify it under configured trust. Never
/// throws: bad input is a refusal with a layer and a reason. This is the one
/// entry point the command line and the service share.
VerifyReport
verify_proof(std::span<const uint8_t> data, TrustForNetworkFn const& trust_for);

/// The report as JSON (§7): `verified`, then the outputs, or `layer` and
/// `reason`. `verifier` names the program and build that produced an
/// accepted report; it is kept apart from the definitions fingerprints.
boost::json::object
to_json(VerifyReport const& report, std::string_view verifier = "");

/// A fingerprint of a loaded set of definitions, so a report says which
/// table produced it: every field with its code, type and flags, the type
/// table, the transaction, ledger-entry, result and permission names, and
/// the native currency. Any change to what the table decodes changes it. It
/// is taken over the loaded table, not over a file, so formatting does not.
std::string
definitions_fingerprint(catl::xdata::Protocol const& protocol);

#ifdef XPRV_TESTING
/// Test-only entry point for exercising the post-anchor capability graph.
bool
resolve_proof_chain_with_pretrusted_anchor_for_testing(
    ProofChain const& chain,
    catl::xdata::Protocol const& protocol,
    std::string* failure = nullptr);
#endif

}  // namespace xprv
