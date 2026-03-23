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

namespace xproof {

/// Resolve and verify a proof chain.
/// If trusted_publisher_key is provided, verifies the anchor's UNL
/// and validation signatures against it.
/// Protocol is needed to serialize leaf JSON back to binary for hash
/// verification.
/// Returns true if all verifiable steps pass.
bool
resolve_proof_chain(
    ProofChain const& chain,
    catl::xdata::Protocol const& protocol,
    std::string const& trusted_publisher_key = "");

}  // namespace xproof
