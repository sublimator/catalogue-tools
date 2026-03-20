#pragma once

#include "wire-node-view.h"

#include <catl/common/ledger-info.h>
#include <catl/core/types.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <vector>

// Forward declare protobuf types — avoid leaking protobuf headers
namespace protocol {
class TMLedgerData;
class TMProofPathResponse;
class TMPing;
}  // namespace protocol

namespace catl::peer_client {

//------------------------------------------------------------------------------
// Errors
//------------------------------------------------------------------------------

enum class Error {
    Success = 0,
    Timeout,
    Disconnected,
    NoLedger,
    NoNode,
    BadRequest,
    FeatureDisabled,
    ParseError,
    Cancelled,
    DuplicateRequest,
};

//------------------------------------------------------------------------------
// Request options
//------------------------------------------------------------------------------

struct RequestOptions
{
    std::chrono::seconds timeout{10};
};

//------------------------------------------------------------------------------
// LedgerHeaderResult — owns protobuf message, provides typed accessors
//------------------------------------------------------------------------------

class LedgerHeaderResult
{
    std::shared_ptr<protocol::TMLedgerData> msg_;

public:
    LedgerHeaderResult() = default;
    explicit LedgerHeaderResult(std::shared_ptr<protocol::TMLedgerData> msg);

    /// Is this result valid (has a message)?
    explicit operator bool() const { return msg_ != nullptr; }

    /// Ledger sequence number.
    uint32_t seq() const;

    /// Ledger hash — zero-copy view into the protobuf message.
    Key ledger_hash() const;
    /// Ledger hash — owned copy.
    Hash256 ledger_hash256() const { return ledger_hash().to_hash(); }

    /// Zero-copy view into the serialized ledger header (node 0).
    /// Use LedgerInfoView to access fields.
    catl::common::LedgerInfoView header() const;

    /// Raw header bytes.
    std::span<const uint8_t> header_data() const;

    /// State tree root node (node 1 from liBASE response).
    /// Returns a WireNodeView — typically an uncompressed inner (513 bytes).
    WireNodeView state_root_node() const;

    /// Transaction tree root node (node 2 from liBASE response).
    WireNodeView tx_root_node() const;

    /// Whether state/tx root nodes are present.
    bool has_state_root() const;
    bool has_tx_root() const;

    /// Number of nodes in the response.
    int node_count() const;

    /// Access the raw protobuf message.
    protocol::TMLedgerData const& raw() const { return *msg_; }
};

//------------------------------------------------------------------------------
// ProofPathResult — owns protobuf message, provides typed accessors
//------------------------------------------------------------------------------

class ProofPathResult
{
    std::shared_ptr<protocol::TMProofPathResponse> msg_;

public:
    ProofPathResult() = default;
    explicit ProofPathResult(
        std::shared_ptr<protocol::TMProofPathResponse> msg);

    explicit operator bool() const { return msg_ != nullptr; }

    /// The key that was proven.
    Key key() const;
    Hash256 key256() const { return key().to_hash(); }

    /// The ledger this proof is against.
    Key ledger_hash() const;
    Hash256 ledger_hash256() const { return ledger_hash().to_hash(); }

    /// Zero-copy view into the serialized ledger header.
    catl::common::LedgerInfoView header() const;

    /// Raw ledger header bytes.
    std::span<const uint8_t> header_data() const;

    /// Number of nodes in the proof path.
    int path_length() const;

    /// Get a node in the proof path as a WireNodeView.
    /// Index 0 = leaf (closest to the key), last = root.
    WireNodeView path_node(int index) const;

    /// Convenience: the leaf node (path_node(0)).
    /// This contains the actual proven data.
    WireNodeView leaf() const;

    /// Convenience: leaf data as a span (strips wire type byte).
    /// For account state: serialized SLE.
    /// For transactions: tx blob (+ meta if TransactionWithMeta).
    std::span<const uint8_t> leaf_data() const;

    /// Access the raw protobuf message.
    protocol::TMProofPathResponse const& raw() const { return *msg_; }
};

//------------------------------------------------------------------------------
// PingResult
//------------------------------------------------------------------------------

struct PingResult
{
    uint32_t seq = 0;
    std::chrono::steady_clock::time_point sent_at{};
    std::chrono::steady_clock::time_point received_at{};

    std::chrono::milliseconds
    rtt() const
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            received_at - sent_at);
    }
};

//------------------------------------------------------------------------------
// SHAMapNodeID (for tree node requests)
//------------------------------------------------------------------------------

struct SHAMapNodeID
{
    Hash256 id;
    uint8_t depth = 0;
};

}  // namespace catl::peer_client
