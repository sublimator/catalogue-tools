#pragma once

#include <catl/core/types.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace catl::peer_client {

//------------------------------------------------------------------------------
// Errors
//------------------------------------------------------------------------------

enum class Error {
    Success = 0,
    Timeout,          // No response within deadline
    Disconnected,     // Connection lost while request in flight
    NoLedger,         // Peer doesn't have the requested ledger
    NoNode,           // Peer doesn't have the requested node(s)
    BadRequest,       // Malformed request
    FeatureDisabled,  // Peer didn't negotiate required feature (e.g. LedgerReplay)
    ParseError,       // Response couldn't be deserialized
    Cancelled,        // Request was explicitly cancelled
    DuplicateRequest, // Same correlation key already in flight
};

//------------------------------------------------------------------------------
// Request options
//------------------------------------------------------------------------------

struct RequestOptions
{
    std::chrono::seconds timeout{10};
};

//------------------------------------------------------------------------------
// Response types
//------------------------------------------------------------------------------

struct LedgerHeaderResult
{
    Hash256 ledger_hash;
    uint32_t ledger_seq = 0;
    std::vector<uint8_t> header_data;      // Raw serialized ledger header
    std::vector<uint8_t> state_root_node;  // liBASE returns state tree root too
    std::vector<uint8_t> tx_root_node;     // liBASE returns tx tree root too
};

struct ProofPathResult
{
    Hash256 key;
    Hash256 ledger_hash;
    std::vector<uint8_t> ledger_header;             // Serialized LedgerInfo
    std::vector<std::vector<uint8_t>> path;          // Leaf-to-root node blobs
};

// Reuse from lesser-peer for now — will own this later
struct SHAMapNodeID
{
    Hash256 id;
    uint8_t depth = 0;
};

struct LedgerNodesResult
{
    Hash256 ledger_hash;
    uint32_t ledger_seq = 0;

    struct Node
    {
        SHAMapNodeID node_id;
        std::vector<uint8_t> data;
    };
    std::vector<Node> nodes;
};

struct ReplayDeltaResult
{
    Hash256 ledger_hash;
    std::vector<uint8_t> ledger_header;
    std::vector<std::vector<uint8_t>> transactions;
};

struct PingResult
{
    uint32_t seq = 0;
    std::chrono::milliseconds round_trip{0};
};

}  // namespace catl::peer_client
