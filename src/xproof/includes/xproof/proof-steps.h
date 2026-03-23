#pragma once

// Structured proof step records for narrative rendering.
//
// During verification, each step produces a ProofStep record.
// After verification, the narrative renderer walks the steps
// and produces human-readable output.

#include <string>
#include <variant>
#include <vector>

namespace xproof {

/// Verification outcome for a single check.
enum class Check { pass, fail, skip };

/// Step types in a proof chain.
enum class StepType {
    anchor,
    ledger_header,
    state_proof,
    tx_proof,
};

/// Role of a ledger header in the chain.
enum class HeaderRole {
    anchor,  // first header, verified against anchor hash
    flag,    // intermediate flag ledger (2-hop only)
    target,  // final target ledger containing the transaction
};

/// How a header was verified.
enum class HeaderVerifyMethod {
    anchor_hash,  // matched against trusted anchor hash
    skip_list,    // found in a skip list from previous state proof
    parent_hash,  // parent hash chain (consecutive headers)
};

/// Type of skip list.
enum class SkipListType {
    recent,  // last 256 ledger hashes (sfHashes at keylet::skip())
    flag,    // flag ledger hashes, every 256th (sfHashes at keylet::skip(seq))
};

//------------------------------------------------------------------------------
// Step records
//------------------------------------------------------------------------------

struct AnchorStep
{
    uint32_t ledger_index = 0;
    std::string ledger_hash;    // hex, truncated for display
    std::string publisher_key;  // hex, truncated
    Check publisher_key_check = Check::skip;
    Check blob_signature_check = Check::skip;
    int unl_size = 0;
    int validations_total = 0;
    int validations_verified = 0;
    int validations_matched = 0;
    Check quorum_check = Check::skip;
    std::vector<std::string> sub_steps;  // from AnchorVerifier narrative
};

struct HeaderStep
{
    uint32_t seq = 0;
    HeaderRole role = HeaderRole::target;
    std::string computed_hash;  // hex, truncated
    HeaderVerifyMethod method = HeaderVerifyMethod::anchor_hash;
    Check hash_check = Check::fail;
    std::string tx_hash;       // hex, truncated
    std::string account_hash;  // hex, truncated
    // For skip list verification:
    SkipListType skip_type = SkipListType::recent;
    int skip_list_size = 0;
};

struct MapProofStep
{
    bool is_state = false;
    int inners = 0;
    int leaves = 0;
    int placeholders = 0;
    int max_depth = 0;
    Check root_hash_check = Check::skip;
    std::string expected_root;  // hex, truncated
    std::string computed_root;  // hex, truncated
    // For state proofs:
    SkipListType skip_type = SkipListType::recent;
    int hashes_count = 0;
    // For tx proofs:
    std::string tx_type;
    std::string tx_account;
};

using ProofStepData = std::variant<AnchorStep, HeaderStep, MapProofStep>;

struct ProofStep
{
    StepType type;
    int step_number = 0;
    ProofStepData data;
};

//------------------------------------------------------------------------------
// Narrative renderer
//------------------------------------------------------------------------------

/// Render a sequence of proof steps into human-readable narrative lines.
std::vector<std::string>
render_narrative(std::vector<ProofStep> const& steps);

}  // namespace xproof
