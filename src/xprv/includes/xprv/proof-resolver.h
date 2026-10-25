#pragma once

// Proof chain resolver / verifier.
//
// Operates on ProofChain structs (format-agnostic).
// The caller loads from JSON via from_json() or binary (future).
//
// Verifies each step:
//   - Anchor: UNL publisher key, blob signature, validation signatures
//   - Ledger headers: SHA512Half hash computation
//   - Map proofs: trie root hash reconstruction
//   - Skip lists: hash lookup between steps

#include "proof-chain.h"

#include <catl/xdata/protocol.h>
#include <string>
#include <vector>

namespace xprv {

/// Resolve and verify a proof chain.
/// Verifies the anchor's UNL and validation signatures against the required
/// trusted publisher key.
/// Protocol is needed to serialize leaf JSON back to binary for hash
/// verification.
/// Returns true if all verifiable steps pass.
/// `extra_manifests` are raw validator/publisher manifests held from outside
/// the proof (§1.3 trust view); they only update keys of listed masters.
bool
resolve_proof_chain(
    ProofChain const& chain,
    catl::xdata::Protocol const& protocol,
    std::string const& trusted_publisher_key,
    std::vector<std::vector<uint8_t>> const& extra_manifests =
        std::vector<std::vector<uint8_t>>());

#ifdef XPRV_TESTING
/// Test-only entry point for exercising the post-anchor capability graph.
bool
resolve_proof_chain_with_pretrusted_anchor_for_testing(
    ProofChain const& chain, catl::xdata::Protocol const& protocol);
#endif

}  // namespace xprv
