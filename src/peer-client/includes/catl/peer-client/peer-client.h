#pragma once

#include "types.h"

#include <catl/core/types.h>

#include <atomic>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>

namespace catl::peer_client {

namespace asio = boost::asio;

//------------------------------------------------------------------------------
// Callback type — Layer 1 (core)
//------------------------------------------------------------------------------

/// Callback-based result delivery.
/// On error, result may be default-constructed.
template <typename T>
using Callback = std::function<void(Error, T)>;

//------------------------------------------------------------------------------
// Forward declarations
//------------------------------------------------------------------------------

// Raw connection type — will be defined in this module eventually,
// for now forward-declared so we can start building against the interface.
class PeerConnection;

//------------------------------------------------------------------------------
// PeerClient — high-level request/response API
//
// Layer 1: Callback API (core)
// Layer 2: Future wrappers (peer-client-async.h)
// Layer 3: Coroutine wrappers (caller can build from callbacks)
//------------------------------------------------------------------------------

class PeerClient : public std::enable_shared_from_this<PeerClient>
{
public:
    PeerClient(
        std::shared_ptr<PeerConnection> connection,
        asio::io_context& io_context);

    ~PeerClient();

    // ---------------------------------------------------------------
    // Ledger headers (TMGetLedger liBASE)
    // ---------------------------------------------------------------

    /// Fetch ledger header by sequence number.
    void
    get_ledger_header(
        uint32_t ledger_seq,
        Callback<LedgerHeaderResult> callback,
        RequestOptions opts = {});

    /// Fetch ledger header by hash.
    void
    get_ledger_header(
        Hash256 const& ledger_hash,
        Callback<LedgerHeaderResult> callback,
        RequestOptions opts = {});

    // ---------------------------------------------------------------
    // Proof paths (requires LedgerReplay)
    // ---------------------------------------------------------------

    /// Proof path for a key in the transaction tree.
    void
    get_tx_proof_path(
        Hash256 const& ledger_hash,
        Hash256 const& key,
        Callback<ProofPathResult> callback,
        RequestOptions opts = {});

    /// Proof path for a key in the account state tree.
    void
    get_state_proof_path(
        Hash256 const& ledger_hash,
        Hash256 const& key,
        Callback<ProofPathResult> callback,
        RequestOptions opts = {});

    // ---------------------------------------------------------------
    // SHAMap node fetching (TMGetLedger)
    // ---------------------------------------------------------------

    /// Fetch nodes from a transaction tree.
    void
    get_tx_tree_nodes(
        Hash256 const& ledger_hash,
        std::vector<SHAMapNodeID> const& node_ids,
        Callback<LedgerNodesResult> callback,
        RequestOptions opts = {});

    /// Fetch nodes from an account state tree.
    void
    get_state_tree_nodes(
        Hash256 const& ledger_hash,
        std::vector<SHAMapNodeID> const& node_ids,
        Callback<LedgerNodesResult> callback,
        RequestOptions opts = {});

    /// Fetch nodes from a candidate transaction set.
    void
    get_txset_nodes(
        Hash256 const& txset_hash,
        std::vector<SHAMapNodeID> const& node_ids,
        Callback<LedgerNodesResult> callback,
        RequestOptions opts = {});

    // ---------------------------------------------------------------
    // Replay delta (requires LedgerReplay)
    // ---------------------------------------------------------------

    /// Full ledger delta (header + all transactions).
    void
    get_replay_delta(
        Hash256 const& ledger_hash,
        Callback<ReplayDeltaResult> callback,
        RequestOptions opts = {});

    // ---------------------------------------------------------------
    // Ping
    // ---------------------------------------------------------------

    void
    ping(Callback<PingResult> callback, RequestOptions opts = {});

    // ---------------------------------------------------------------
    // Unsolicited messages
    // ---------------------------------------------------------------

    /// Handler for messages PeerClient doesn't consume (proposals,
    /// validations, status changes, etc.). Forwarded to caller if set.
    using UnsolicitedHandler = std::function<
        void(uint16_t type, std::vector<uint8_t> const& data)>;

    void
    set_unsolicited_handler(UnsolicitedHandler handler);

    // ---------------------------------------------------------------
    // Connection state
    // ---------------------------------------------------------------

    bool
    is_connected() const;

    /// Whether peer negotiated LedgerReplay during handshake.
    bool
    has_ledger_replay() const;

    /// Cancel all in-flight requests (callbacks fire with Cancelled).
    void
    cancel_all();

    /// Number of requests currently in flight.
    size_t
    pending_count() const;

    /// Access the underlying connection.
    PeerConnection&
    raw_connection();

private:
    // ---------------------------------------------------------------
    // Sequence allocator — single counter for all correlation IDs
    // ---------------------------------------------------------------

    std::atomic<uint64_t> next_seq_{1};

    uint64_t
    allocate_seq()
    {
        return next_seq_.fetch_add(1);
    }

    /// Masked to uint32 for TMGetLedger requestCookie
    /// (proto response truncates to uint32)
    uint32_t
    allocate_cookie()
    {
        return static_cast<uint32_t>(allocate_seq() & 0xFFFFFFFF);
    }

    // ---------------------------------------------------------------
    // Request tracking (type-erased base)
    // ---------------------------------------------------------------

    struct PendingRequestBase
    {
        uint64_t id;
        std::shared_ptr<asio::steady_timer> timer;
        std::chrono::steady_clock::time_point sent_at;

        virtual ~PendingRequestBase() = default;
        virtual void cancel() = 0;
        virtual void timeout() = 0;
    };

    template <typename ResultT>
    struct PendingRequest : PendingRequestBase
    {
        Callback<ResultT> callback;
        void cancel() override;
        void timeout() override;
    };

    // ---------------------------------------------------------------
    // Correlation keys
    // ---------------------------------------------------------------

    // TMProofPathRequest: no cookie, content-match only
    struct ProofPathKey
    {
        Hash256 ledger_hash;
        Hash256 key;
        int type;

        bool operator<(ProofPathKey const& other) const;
    };

    // Pending request maps — one per correlation strategy
    std::map<uint32_t, std::unique_ptr<PendingRequest<LedgerHeaderResult>>>
        pending_headers_;       // keyed by requestCookie
    std::map<uint32_t, std::unique_ptr<PendingRequest<LedgerNodesResult>>>
        pending_nodes_;         // keyed by requestCookie
    std::map<ProofPathKey, std::unique_ptr<PendingRequest<ProofPathResult>>>
        pending_proof_paths_;   // keyed by {hash, key, type}
    std::map<Hash256, std::unique_ptr<PendingRequest<ReplayDeltaResult>>>
        pending_deltas_;        // keyed by ledgerHash
    std::map<uint32_t, std::unique_ptr<PendingRequest<PingResult>>>
        pending_pings_;         // keyed by ping seq

    mutable std::mutex mutex_;

    // ---------------------------------------------------------------
    // Packet dispatch
    // ---------------------------------------------------------------

    void
    on_packet(uint16_t type, std::vector<uint8_t> const& data);

    void dispatch_ledger_data(std::vector<uint8_t> const& data);
    void dispatch_proof_path_response(std::vector<uint8_t> const& data);
    void dispatch_replay_delta_response(std::vector<uint8_t> const& data);
    void dispatch_pong(std::vector<uint8_t> const& data);

    // ---------------------------------------------------------------
    // Timer management
    // ---------------------------------------------------------------

    void
    start_timeout(PendingRequestBase& req, std::chrono::seconds timeout);

    // ---------------------------------------------------------------
    // Members
    // ---------------------------------------------------------------

    std::shared_ptr<PeerConnection> connection_;
    asio::io_context& io_context_;
    UnsolicitedHandler unsolicited_handler_;
};

}  // namespace catl::peer_client
