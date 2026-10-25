#pragma once

// Canonical in-memory representation of a proof chain.
//
// Format-agnostic — serialized to JSON by proof-chain-json,
// and will be serialized to binary TLV in the future.
//
// The builder (proof-builder) produces a ProofChain.
// The resolver (proof-resolver) verifies a ProofChain.

#include <catl/core/types.h>

#include <boost/json.hpp>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace xprv {

//------------------------------------------------------------------------------
// Anchor — trust root with UNL data + validation signatures
//------------------------------------------------------------------------------

struct AnchorData
{
    Hash256 ledger_hash;
    uint32_t ledger_index = 0;

    // UNL data (raw bytes, self-contained for offline verification)
    std::string publisher_key_hex;
    std::vector<uint8_t> publisher_manifest;  // raw manifest bytes
    std::vector<uint8_t> blob;                // raw UNL blob bytes
    std::vector<uint8_t> blob_signature;      // publisher signs blob

    // Validations: signing_key_hex → raw STValidation bytes
    std::map<std::string, std::vector<uint8_t>> validations;
};

//------------------------------------------------------------------------------
// Ledger header — fixed 118-byte canonical layout
//------------------------------------------------------------------------------

struct HeaderData
{
    uint32_t seq = 0;
    uint64_t drops = 0;
    Hash256 parent_hash;
    Hash256 tx_hash;
    Hash256 account_hash;
    uint32_t parent_close_time = 0;
    uint32_t close_time = 0;
    uint8_t close_time_resolution = 0;
    uint8_t close_flags = 0;
};

//------------------------------------------------------------------------------
// Map proof — abbreviated SHAMap trie
//------------------------------------------------------------------------------

struct TrieData
{
    enum class TreeType { tx, state };
    TreeType tree = TreeType::tx;

    // Two representations of the same abbreviated SHAMap.
    // The builder populates both; serializers use whichever they need.
    // JSON form: human-readable, leaves are decoded XRPL objects.
    // Binary form: compact, leaves are raw canonical binary (no re-serialization
    //   needed for hash verification).
    boost::json::value trie_json;
    std::vector<uint8_t> trie_binary;

    // Non-empty when a leaf could not be decoded and re-encoded to its exact
    // bytes with this server's field definitions. JSON leaves are decoded only
    // (2055), so such a trie has no JSON form; trie_binary is still complete.
    std::string json_unavailable;
};

/// A proof asked for in JSON form has a trie with no JSON form (see
/// TrieData::json_unavailable). The binary form carries the same proof.
class JsonFormUnavailable : public std::runtime_error
{
public:
    explicit JsonFormUnavailable(std::string reason)
        : std::runtime_error(
              "proof has no JSON form: " + reason + ". " + std::string(hint))
        , reason_(std::move(reason))
    {
    }

    std::string const&
    reason() const
    {
        return reason_;
    }

    static constexpr std::string_view hint =
        "Request the binary proof instead (format=bin): its leaves are the "
        "exact ledger bytes, so it needs no field definitions to build or "
        "verify";

private:
    std::string reason_;
};

//------------------------------------------------------------------------------
// Proof chain — ordered sequence of steps
//------------------------------------------------------------------------------

using ChainStep = std::variant<AnchorData, HeaderData, TrieData>;

struct ProofChain
{
    uint32_t network_id = 0;  // 0=XRPL, 21337=Xahau
    std::vector<ChainStep> steps;
};

}  // namespace xprv
