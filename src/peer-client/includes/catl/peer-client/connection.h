#pragma once

#include "connection-types.h"
#include <catl/core/logger.h>

#include <array>
#include <atomic>
#include <boost/beast.hpp>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <type_traits>
#include <utility>
#include <vector>

namespace catl::peer_client {

class peer_connection : public std::enable_shared_from_this<peer_connection>
{
public:
    using connection_handler = std::function<void(boost::system::error_code)>;
    using packet_handler = std::function<
        void(packet_header const&, std::vector<std::uint8_t> const&)>;
    using disconnect_handler = std::function<void(boost::system::error_code)>;

    /// Strand-based construction. All async operations complete on the
    /// strand, so all callbacks fire on it automatically.
    peer_connection(
        strand_type strand,
        asio::ssl::context& ssl_context,
        peer_config config);

    ~peer_connection();

    // Async connect to peer
    void
    async_connect(const connection_handler& handler);

    // Async accept connection (listen mode)
    void
    async_accept(tcp::acceptor& acceptor, const connection_handler& handler);

    // Start reading packets
    void
    start_read(packet_handler handler);

    // Send packet
    void
    async_send_packet(
        packet_type type,
        std::vector<std::uint8_t> const& data,
        std::function<void(boost::system::error_code)> handler);

    // Get remote endpoint info
    std::string
    remote_endpoint() const;

    // Connection state
    bool
    is_connected() const
    {
        return connected_;
    }

    // Get server info
    std::string
    server_version() const
    {
        return server_version_;
    }

    std::string
    protocol_version() const
    {
        return protocol_version_;
    }

    std::string
    network_id() const
    {
        return network_id_;
    }

    std::map<std::string, std::string> const&
    upgrade_headers() const
    {
        return upgrade_headers_;
    }

    // Send transaction query
    void
    send_transaction_query(
        const std::string& tx_hash,
        const std::string& ledger_hash = "");

    // Get the transaction hash for a given sequence number
    std::string
    get_query_hash(uint32_t seq) const;

    // Request a candidate transaction set (root node only)
    void
    request_transaction_set(const std::string& tx_set_hash);

    // Request specific nodes from a transaction set
    void
    request_transaction_set_nodes(
        const std::string& tx_set_hash,
        const std::vector<std::string>& node_ids_wire);

    // Close connection
    void
    close();

    /// Commit the same idempotent close as close(), then wait until every
    /// connection-owned transport operation has returned its exact receipt.
    ///
    /// This is deliberately outbound-only. In listen mode the acceptor owns
    /// cancellation of async_accept(), so a peer_connection cannot truthfully
    /// mint the complete physical-drain receipt by itself.
    boost::asio::awaitable<void>
    co_close();

    // Set handler to be called on disconnect (EOF, reset, etc.)
    void
    set_disconnect_handler(disconnect_handler handler);

    // Get socket for accept operations
    ssl_socket&
    socket()
    {
        return *socket_;
    }

    /// Peer IPs from a 503 redirect response. Populated on failed connect.
    std::set<std::string> const&
    redirect_ips() const
    {
        return redirect_ips_;
    }

    /// Human-readable summary of the most recent connect/upgrade failure.
    std::string
    connect_failure_detail() const;

    /// Completion of a substituted transport write.
    using test_write_completion =
        std::function<void(boost::system::error_code)>;

    /// Test seam: substitute transport initiation only.
    ///
    /// When set, `do_write()` calls this instead of `asio::async_write` and
    /// hands over the completion, so a test can fire it at an exact point
    /// instead of racing a socket. Everything the correction is about stays on
    /// the production path: slot ownership, physical buffer lifetime, terminal
    /// selection, drain, and the successor pump. An initiator that throws also
    /// forces the initiation-exception rollback, so this one seam covers both.
    ///
    /// What it does NOT prove: that Asio itself reads the buffer after close.
    /// That is Asio's documented contract, not this class's behavior; the
    /// testable obligation here is that the bytes are still alive and unmoved
    /// when the completion runs.
    void
    set_write_initiator_for_test(
        std::function<void(boost::asio::const_buffer, test_write_completion)>
            initiator)
    {
        write_initiator_for_test_ = std::move(initiator);
    }

    enum class test_read_stage {
        header,
        header_extension,
        payload,
        payload_drain,
    };

    using test_read_completion =
        std::function<void(boost::system::error_code, std::size_t)>;

    /// Test seam: substitute steady-state transport reads while preserving the
    /// production census, successor gates, frame parser, and completion path.
    void
    set_read_initiator_for_test(std::function<void(
                                    test_read_stage,
                                    boost::asio::mutable_buffer,
                                    test_read_completion)> initiator)
    {
        read_initiator_for_test_ = std::move(initiator);
    }

    /// Test seam: run after both the resolver capability and its exact
    /// transport-operation row are installed, immediately before Asio
    /// initiation. Throwing here forces the synchronous initiation-refusal
    /// rollback without racing DNS or a socket.
    void
    set_before_resolve_initiation_for_test(std::function<void()> hook)
    {
        before_resolve_initiation_for_test_ = std::move(hook);
    }

    /// Test seam: observe registration at the exact physical-drain wait site.
    /// Runs on the connection strand after the waiter has an owned home.
    void
    set_after_physical_drain_waiter_registered_for_test(
        std::function<void(std::size_t)> observer)
    {
        after_physical_drain_waiter_registered_for_test_ = std::move(observer);
    }

    /// Test observation: the retained backing store of the write currently in
    /// flight, or `{nullptr, 0}` when none is. Lets a test assert that a close
    /// did not free bytes Asio still owns (blocker 1) without relying on ASan
    /// or on a race.
    std::pair<void const*, std::size_t>
    active_write_storage_for_test() const
    {
        if (!active_write_)
            return {nullptr, 0};
        return {active_write_->packet.data(), active_write_->packet.size()};
    }

    /// Test observation: count of uninitiated successors still queued.
    std::size_t
    queued_write_count_for_test() const
    {
        return write_queue_.size();
    }

    std::size_t
    transport_operation_count_for_test() const
    {
        return transport_operations_.size();
    }

    bool
    physical_transport_drained_for_test() const
    {
        return physical_phase_ == PhysicalPhase::drained;
    }

    bool
    resolver_installed_for_test() const
    {
        return static_cast<bool>(resolver_);
    }

private:
    void
    handle_connect(
        const boost::system::error_code& ec,
        const connection_handler& handler);
    void
    handle_handshake(
        const boost::system::error_code& ec,
        const connection_handler& handler);

    void
    async_read_header();
    void
    handle_read_header(
        boost::system::error_code ec,
        std::size_t bytes_transferred);
    void
    handle_read_payload(
        boost::system::error_code ec,
        std::size_t bytes_transferred);

    // Start the payload operation after the complete frame header has been
    // parsed. Both compressed and uncompressed headers use this path so payload
    // admission and draining policy cannot diverge between framing variants.
    void
    read_or_drain_payload();

    // Read and discard `remaining` payload bytes in small chunks (reusing the
    // read buffer at a bounded size), then resume the header loop. Used to
    // consume a frame we don't want to keep (e.g. an oversized manifest dump)
    // without allocating the full payload or dropping the peer.
    void
    drain_payload(std::size_t remaining);

    void
    perform_http_upgrade(const connection_handler& handler);

    void
    set_connect_failure_detail(std::string detail);

    void
    note_connect_failure(char const* stage, boost::system::error_code ec);

    // Generate node keys
    void
    generate_node_keys();

    // Verify the peer's Session-Signature against the shared TLS cookie
    // (sec #0053). Logs a warning on mismatch but does NOT drop the
    // connection — we make no trust decision off this signature, and a
    // false-reject would sever a legitimate peer. Detection-only for now;
    // tighten to a hard reject once live traffic confirms zero warnings.
    void
    verify_peer_session_signature(
        std::string const& public_key_b58,
        std::string const& session_signature_b64);

    // Release any bytes this connection holds against the process-wide
    // inbound budget (security #0055). Idempotent.
    void
    release_inbound_charge();

private:
    strand_type strand_;
    std::unique_ptr<ssl_socket> socket_;
    peer_config config_;
    packet_handler packet_handler_;

    // Read buffers
    std::array<std::uint8_t, 10> header_buffer_;
    std::vector<std::uint8_t> payload_buffer_;
    packet_header current_header_;

    // Bytes reserved from the process-wide inbound budget for the in-flight
    // frame (security #0055); released on dispatch or teardown. Atomic so the
    // release is exactly-once even if a cross-thread close() races a frame
    // completion (exchange-based release).
    std::atomic<std::size_t> inbound_charge_{0};

    // HTTP upgrade state
    bool http_upgraded_ = false;
    mutable std::mutex connect_failure_mutex_;
    std::string connect_failure_detail_;

    // Node identity
    std::array<std::uint8_t, 32> secret_key_;
    std::array<std::uint8_t, 33> public_key_compressed_;
    std::string node_public_key_b58_;

    // Connection state
    bool connected_ = false;
    tcp::endpoint remote_endpoint_;

    // Server info
    std::string server_version_;
    std::string protocol_version_;
    std::string network_id_;
    std::map<std::string, std::string> upgrade_headers_;

    // HTTP upgrade
    std::string session_signature_;
    // Shared TLS-finished cookie (symmetric across both peers); retained so
    // the peer's Session-Signature can be verified after the upgrade. See
    // verify_peer_session_signature (sec #0053).
    std::array<std::uint8_t, 32> session_cookie_{};
    boost::beast::http::request<boost::beast::http::string_body> http_request_;
    boost::beast::http::response<boost::beast::http::string_body>
        http_response_;

    // Transaction query tracking
    std::atomic<uint32_t> query_seq_{1};  // Start at 1
    mutable std::mutex query_map_mutex_;
    std::map<uint32_t, std::string> query_map_;  // seq -> tx_hash

    // Disconnect handler
    disconnect_handler disconnect_handler_;

    // Two facts, previously one bool called `closing_`. They were split because
    // the correct teardown ordering was INEXPRESSIBLE while they shared
    // storage: closing admission early (so a callback cannot send onto a dead
    // transport) also tripped the settlement guard, which then skipped the
    // physical close.
    //
    //   admission_closed_  the gate. Commits BEFORE any external callback runs,
    //                      on every teardown path.
    //   close_ran_         settlement idempotency: socket cancelled/closed and
    //                      terminals collected exactly once.
    bool admission_closed_ = false;
    bool close_ran_ = false;

    enum class PhysicalPhase {
        open,
        closing,
        drained,
    };

    enum class TransportOperationKind {
        resolve,
        connect,
        accept,
        tls_handshake,
        http_read,
        http_write,
        header_read,
        header_extension_read,
        payload_read,
        payload_drain,
        packet_write,
    };

    using TransportOperationId = std::uint64_t;

    struct TransportOperationCompletion
    {
        peer_connection* owner;
        TransportOperationId id;

        ~TransportOperationCompletion()
        {
            owner->complete_transport_operation(id);
        }
    };

    TransportOperationId
    begin_transport_operation(TransportOperationKind kind);

    template <typename Initiator>
    void
    initiate_transport_operation(
        TransportOperationKind kind,
        Initiator&& initiator)
    {
        //@@authority transition=prv.T0140 status=provisional scope=statement
        // bind=declaration@27bff81293db626b
        // owner=unresolved owner_hypothesis=peer_connection
        // commit=connection_strand touches=prv.S0045
        // mutation_class=running_guarded
        // label=reserve_exact_transport_operation_before_fallible_initiation
        auto const id =
            begin_transport_operation(kind);  //@@authority-target ids=prv.T0140
        try
        {
            std::invoke(std::forward<Initiator>(initiator), id);
        }
        catch (...)
        {
            // No completion handler exists when initiation throws, so this
            // frame must synchronously return the exact reservation.
            complete_transport_operation(id);
            throw;
        }
    }

    void
    complete_transport_operation(TransportOperationId id) noexcept;

    void
    release_resolver_after_non_close_terminal(
        std::shared_ptr<tcp::resolver> const& resolver) noexcept;

    void
    try_complete_physical_drain() noexcept;

    //@@authority state=prv.S0045 status=provisional scope=field
    // bind=catl::peer_client::peer_connection::transport_operations_@49066613f0f4f1c2
    // owner=unresolved owner_hypothesis=peer_connection
    // observed_home=peer_connection
    // label=exact_connection_transport_operation_census
    std::map<TransportOperationId, TransportOperationKind>
        transport_operations_;
    TransportOperationId next_transport_operation_id_ = 1;

    //@@authority state=prv.S0048 status=provisional scope=field
    //bind=catl::peer_client::peer_connection::physical_phase_@11427c92d72ac66a
    // owner=unresolved owner_hypothesis=peer_connection
    // observed_home=peer_connection
    // label=connection_physical_lifecycle_phase
    PhysicalPhase physical_phase_ = PhysicalPhase::open;

    //@@authority state=prv.S0046 status=provisional scope=field
    // bind=catl::peer_client::peer_connection::physical_drain_waiters_@3615b76151cac3cd
    // owner=unresolved owner_hypothesis=peer_connection
    // observed_home=peer_connection
    // label=shared_connection_physical_drain_receipt_waiters
    std::vector<std::shared_ptr<asio::steady_timer>> physical_drain_waiters_;

    //@@authority state=prv.S0047 status=provisional scope=field
    // bind=catl::peer_client::peer_connection::resolver_@f4e0d43498370569
    // owner=unresolved owner_hypothesis=peer_connection
    // observed_home=peer_connection
    // label=owned_outbound_resolver_cancellation_capability
    std::shared_ptr<tcp::resolver> resolver_;

    /// How this connection ends, committed the moment a cause is OBSERVED and
    /// honored by whichever frame later wins `close_ran_`.
    ///
    /// This exists because `fire_disconnect` used to be transition authority
    /// held only in a stack argument. A handler reacting to a fatal write could
    /// call close(), whose `settle_and_close(operation_aborted, false)` won the
    /// shared latch with the WEAKER disposition — the socket closed and the
    /// disconnect export, the sole commit that moves PeerClient to Failed and
    /// publishes PeerTerminalReason, was consumed without firing. Authority for
    /// a world-changing transition has to live in an owned row, not on a stack.
    ///
    /// First commit wins, which is sufficient because every path commits its
    /// disposition before running external code.
    struct CloseDisposition
    {
        boost::system::error_code cause;
        bool fire_disconnect;
    };
    std::optional<CloseDisposition> close_disposition_;

    /// Commit the terminal disposition if none is committed yet. Idempotent.
    void
    select_close_disposition(
        boost::system::error_code cause,
        bool fire_disconnect) noexcept;

    // Redirect IPs from 503 response
    std::set<std::string> redirect_ips_;

    // Outbound write queue — serializes async_write calls.
    // Without this, concurrent sends (status mirror, ping reply,
    // request) can overlap and garble frames.
    struct PendingWrite
    {
        std::vector<std::uint8_t> packet;
        std::function<void(boost::system::error_code)> handler;
    };
    // Four distinct things, deliberately not collapsed into flags:
    //
    //   write_queue_       uninitiated successors. No Asio operation has ever
    //                      seen these buffers, so teardown may destroy them.
    //   active_write_      the one composed write in flight. Asio owns its
    //                      buffer until completion, so its PHYSICAL storage
    //                      must outlive close. This optional IS the
    //                      write-in-flight latch: a separate bool could
    //                      disagree with the slot, and a bool that said "idle"
    //                      while a buffer was still owned by Asio is what
    //                      stranded the queue.
    //   active_write_->handler
    //                      the LOGICAL terminal, held as a linear capability.
    //                      Whoever delivers it MOVES it out, so possession is
    //                      the exactly-once proof and a second delivery is
    //                      unrepresentable rather than flag-guarded. This is
    //                      why close can terminalize the handler without
    //                      freeing the bytes: the two live in the same row but
    //                      are consumed by different owners at different times.
    //   admission_closed_  admission gate. Refuse before queue mutation.
    //@@authority state=peer_client.connection_write_rows v4=S51
    // owner=peer_client
    // bind=catl::peer_client::peer_connection::write_queue_@dd3d0279829e5b18
    // S51 is PeerClient.PhysicalLifecycle[GenerationId] — "socket, timers,
    // write queue, and callback gates", minted by T31. These rows are the
    // write-queue portion of it. peer_connection is deliberately NOT an owner:
    // it is constructed with PeerClient's strand, and the canonical model
    // states that a name absent from `owners:` cannot mint authority.
    //
    // MODEL DISAGREEMENT (filed, not resolved): S51 is one row for four things
    // whose release points differ. The active write's BYTES outlive its logical
    // terminal, which outlives the queued successors, and the close disposition
    // outlives all three. Three review rounds found real defects at exactly
    // those seams. This annotation binds the code honestly to the row that
    // exists; it does not claim the row is correctly decomposed.
    std::deque<PendingWrite> write_queue_;
    std::optional<PendingWrite> active_write_;

    // settle_and_close() moves the queue and one handler INSIDE its commit
    // section, after close_ran_ has latched. That is only safe while those
    // moves cannot throw: a throwing move would unwind with the idempotency
    // flag already set, the socket still open, and every later close returning
    // immediately — the exact shape that was removed from this function.
    // Asserted beside the types so a later container or allocator change cannot
    // silently invalidate the commit section.
    static_assert(
        std::is_nothrow_move_constructible_v<std::deque<PendingWrite>>,
        "settle_and_close moves write_queue_ after latching close_ran_");
    static_assert(
        std::is_nothrow_move_constructible_v<
            std::function<void(boost::system::error_code)>> &&
            std::is_nothrow_move_assignable_v<
                std::function<void(boost::system::error_code)>>,
        "settle_and_close moves the active terminal after latching close_ran_");

    /// Never set in production. See set_write_initiator_for_test().
    std::function<void(boost::asio::const_buffer, test_write_completion)>
        write_initiator_for_test_;
    /// Never set in production. See set_read_initiator_for_test().
    std::function<void(
        test_read_stage,
        boost::asio::mutable_buffer,
        test_read_completion)>
        read_initiator_for_test_;
    /// Never set in production. See
    /// set_before_resolve_initiation_for_test().
    std::function<void()> before_resolve_initiation_for_test_;
    /// Never set in production. See
    /// set_after_physical_drain_waiter_registered_for_test().
    std::function<void(std::size_t)>
        after_physical_drain_waiter_registered_for_test_;

    void
    do_write();

    void
    on_write_complete(boost::system::error_code ec);

    /// Invoke one external terminal, exactly once, containing any exception.
    /// The handler type is not `noexcept` and no contract forbids throws, so an
    /// escaping exception would otherwise abort whatever settlement loop is
    /// running. Consumes `handler`.
    void
    deliver_terminal(
        std::function<void(boost::system::error_code)>& handler,
        boost::system::error_code ec) noexcept;

    /// Strand-only: the single teardown transition. Commits closed admission,
    /// completes owner-local settlement (collect terminals, cancel and close
    /// the socket), and only then exposes external callbacks. Idempotent —
    /// guarded by close_ran_.
    void
    settle_and_close(
        boost::system::error_code terminal_ec,
        bool fire_disconnect);

    /// Strand-only: close socket and fail queued writes.
    /// Idempotent — guarded by close_ran_.
    void
    close_impl();

    /// Strand-only: centralized error teardown.
    void
    fail_and_close(boost::system::error_code ec);

    // Helper methods
    void
    handle_http_request(const connection_handler& handler);
    void
    handle_http_response(const connection_handler& handler);
    void
    send_http_request(const connection_handler& handler);
    void
    send_initial_ping();
};

}  // namespace catl::peer_client
