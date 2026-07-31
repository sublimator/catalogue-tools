#include "ripple.pb.h"
#include <catl/base58/base58.h>
#include <catl/common/utils.h>
#include <catl/core/logger.h>
#include <catl/crypto/sig-verify.h>
#include <catl/peer-client/connection.h>
#include <catl/peer-client/crypto-utils.h>
#include <catl/peer-client/node-identity.h>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>
#include <limits>
#include <openssl/ssl.h>
#include <sodium.h>
#include <span>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace catl::peer_client {

static LogPartition log_("peer-conn", LogLevel::INHERIT);

namespace beast = boost::beast;
namespace http = beast::http;

namespace {

std::string
truncate_for_log(std::string const& text, std::size_t limit = 160)
{
    if (text.size() <= limit)
        return text;
    return text.substr(0, limit) + "...";
}

boost::system::error_code
current_exception_error() noexcept
{
    try
    {
        throw;
    }
    catch (boost::system::system_error const& error)
    {
        return error.code();
    }
    catch (...)
    {
        return boost::system::errc::make_error_code(
            boost::system::errc::io_error);
    }
}

}  // namespace

peer_connection::peer_connection(
    strand_type strand,
    asio::ssl::context& ssl_context,
    peer_config config)
    : strand_(std::move(strand))
    , socket_(std::make_unique<ssl_socket>(strand_, ssl_context))
    , config_(std::move(config))
{
}

peer_connection::~peer_connection()
{
    if (socket_ && socket_->lowest_layer().is_open())
    {
        PLOGW(
            log_, "Socket still open in destructor — was close() not called?");
        boost::system::error_code ec;
        socket_->lowest_layer().close(ec);
    }

    // Safety net: normally close_impl() already released this (sec #0055).
    release_inbound_charge();

    // Wipe per-connection copies of the node identity. Bytes will be
    // freed back to the heap allocator and may be reused for other
    // data; zeroing first prevents the secret from leaking into
    // future allocations or core dumps.
    ::sodium_memzero(secret_key_.data(), secret_key_.size());
    if (!session_signature_.empty())
    {
        ::sodium_memzero(session_signature_.data(), session_signature_.size());
    }
}

//@@authority transition=prv.T0135 status=provisional scope=function
// bind=catl::peer_client::begin_transport_operation@6c450ad7a6123f9c
// owner=unresolved owner_hypothesis=peer_connection commit=connection_strand
// touches=prv.S0045,prv.S0048 mutation_class=running_guarded
// label=admit_exact_connection_transport_operation
peer_connection::TransportOperationId
peer_connection::begin_transport_operation(TransportOperationKind kind)
{
    if (physical_phase_ != PhysicalPhase::open)
        throw std::logic_error("transport operation admitted after close seal");
    if (next_transport_operation_id_ ==
        std::numeric_limits<TransportOperationId>::max())
    {
        throw std::overflow_error("transport operation identity exhausted");
    }

    auto const id = next_transport_operation_id_++;
    auto const [_, inserted] = transport_operations_.emplace(id, kind);
    if (!inserted)
        throw std::logic_error("duplicate transport operation identity");
    return id;
}

//@@authority transition=prv.T0136 status=provisional scope=function
// bind=catl::peer_client::complete_transport_operation@6035c88fee602797
// owner=unresolved owner_hypothesis=peer_connection commit=connection_strand
// touches=prv.S0045 mutation_class=terminal_receipt
// label=consume_exact_connection_transport_operation_receipt
void
peer_connection::complete_transport_operation(TransportOperationId id) noexcept
{
    transport_operations_.erase(id);
    try_complete_physical_drain();
}

//@@authority transition=prv.T0143 status=provisional scope=function
// bind=catl::peer_client::release_resolver_after_non_close_terminal@99d6b7b2c13b8bfd
// owner=unresolved owner_hypothesis=peer_connection commit=connection_strand
// touches=prv.S0047 mutation_class=terminal_receipt
// label=release_resolver_after_completion_or_initiation_refusal
void
peer_connection::release_resolver_after_non_close_terminal(
    std::shared_ptr<tcp::resolver> const& resolver) noexcept
{
    if (resolver_ != resolver)
        return;

    resolver_.reset();
    try_complete_physical_drain();
}

//@@authority transition=prv.T0138 status=provisional scope=function
// bind=catl::peer_client::try_complete_physical_drain@a27eb58fffdee9f7
// owner=unresolved owner_hypothesis=peer_connection commit=connection_strand
// touches=prv.S0045,prv.S0046,prv.S0047,prv.S0048
// mutation_class=terminal_receipt
// label=publish_connection_physical_drain_receipt_after_zero_census
void
peer_connection::try_complete_physical_drain() noexcept
{
    if (physical_phase_ != PhysicalPhase::closing ||
        !transport_operations_.empty() || resolver_)
    {
        return;
    }

    physical_phase_ = PhysicalPhase::drained;
    auto waiters = std::move(physical_drain_waiters_);
    physical_drain_waiters_.clear();
    for (auto const& waiter : waiters)
    {
        if (!waiter)
            continue;
        waiter->cancel();
    }
}

void
peer_connection::async_connect(const connection_handler& handler)
{
    set_connect_failure_detail({});

    if (config_.listen_mode)
    {
        PLOGE(log_, "Cannot call async_connect in listen mode");
        handler(boost::asio::error::invalid_argument);
        return;
    }
    if (physical_phase_ != PhysicalPhase::open)
    {
        handler(boost::asio::error::operation_aborted);
        return;
    }
    //@@authority transition=prv.T0149 status=provisional scope=statement
    // bind=if_statement@c65a0e313ebc0566
    // owner=unresolved owner_hypothesis=peer_connection
    // commit=connection_strand touches=prv.S0047
    // mutation_class=running_guarded
    // label=refuse_overlapping_outbound_resolver_capability
    if (resolver_)
    {
        handler(boost::asio::error::already_started);
        return;
    }  //@@authority-target ids=prv.T0149

    // Resolve the host asynchronously
    //@@authority transition=prv.T0141 status=provisional scope=statement
    // bind=expression_statement@5eb614c89df2ec6b
    // owner=unresolved owner_hypothesis=peer_connection
    // commit=connection_strand touches=prv.S0047 mutation_class=running_guarded
    // label=install_owned_resolver_cancellation_capability
    resolver_ = std::make_shared<tcp::resolver>(
        strand_);  //@@authority-target ids=prv.T0141
    auto const resolver = resolver_;
    try
    {
        initiate_transport_operation(
            TransportOperationKind::resolve,
            [self = shared_from_this(), handler, resolver](
                TransportOperationId resolve_id) {
                if (self->before_resolve_initiation_for_test_)
                    self->before_resolve_initiation_for_test_();
                resolver->async_resolve(
                    self->config_.host,
                    std::to_string(self->config_.port),
                    [self, handler, resolver, resolve_id](
                        boost::system::error_code ec,
                        tcp::resolver::results_type endpoints) {
                        TransportOperationCompletion resolve_complete{
                            self.get(), resolve_id};
                        self->release_resolver_after_non_close_terminal(
                            resolver);
                        if (self->physical_phase_ != PhysicalPhase::open)
                        {
                            handler(boost::asio::error::operation_aborted);
                            return;
                        }
                        if (ec)
                        {
                            self->note_connect_failure("resolve", ec);
                            handler(ec);
                            return;
                        }

                        try
                        {
                            self->initiate_transport_operation(
                                TransportOperationKind::connect,
                                [self, handler, endpoints](
                                    TransportOperationId connect_id) {
                                    asio::async_connect(
                                        self->socket_->lowest_layer(),
                                        endpoints,
                                        [self, handler, connect_id](
                                            boost::system::error_code ec,
                                            tcp::endpoint endpoint) {
                                            TransportOperationCompletion
                                                connect_complete{
                                                    self.get(), connect_id};
                                            if (self->physical_phase_ !=
                                                PhysicalPhase::open)
                                            {
                                                handler(boost::asio::error::
                                                            operation_aborted);
                                                return;
                                            }
                                            if (!ec)
                                            {
                                                self->remote_endpoint_ =
                                                    endpoint;
                                                self->handle_connect(
                                                    ec, handler);
                                            }
                                            else
                                            {
                                                self->note_connect_failure(
                                                    "tcp connect", ec);
                                                handler(ec);
                                            }
                                        });
                                });
                        }
                        catch (...)
                        {
                            auto const start_error = current_exception_error();
                            self->note_connect_failure(
                                "tcp connect initiation", start_error);
                            handler(start_error);
                        }
                    });
            });
    }
    catch (...)
    {
        // initiate_transport_operation() has already returned the exact
        // operation row. This returns the other installed authority-bearing
        // fact and re-evaluates the full drain predicate.
        release_resolver_after_non_close_terminal(resolver);
        auto const start_error = current_exception_error();
        note_connect_failure("resolve initiation", start_error);
        handler(start_error);
    }
}

void
peer_connection::async_accept(
    tcp::acceptor& acceptor,
    const connection_handler& handler)
{
    if (!config_.listen_mode)
    {
        PLOGE(log_, "Cannot call async_accept when not in listen mode");
        handler(boost::asio::error::invalid_argument);
        return;
    }
    if (physical_phase_ != PhysicalPhase::open)
    {
        handler(boost::asio::error::operation_aborted);
        return;
    }

    try
    {
        initiate_transport_operation(
            TransportOperationKind::accept,
            [self = shared_from_this(), &acceptor, handler](
                TransportOperationId accept_id) {
                acceptor.async_accept(
                    self->socket_->lowest_layer(),
                    [self, handler, accept_id](boost::system::error_code ec) {
                        TransportOperationCompletion accept_complete{
                            self.get(), accept_id};
                        if (self->physical_phase_ != PhysicalPhase::open)
                        {
                            handler(boost::asio::error::operation_aborted);
                            return;
                        }
                        if (!ec)
                        {
                            self->remote_endpoint_ =
                                self->socket_->lowest_layer().remote_endpoint();
                            self->handle_handshake(ec, handler);
                        }
                        else
                        {
                            handler(ec);
                        }
                    });
            });
    }
    catch (...)
    {
        handler(current_exception_error());
    }
}

void
peer_connection::handle_connect(
    const boost::system::error_code& ec,
    const connection_handler& handler)
{
    if (physical_phase_ != PhysicalPhase::open)
    {
        handler(boost::asio::error::operation_aborted);
        return;
    }
    if (ec)
    {
        note_connect_failure("tcp connect", ec);
        handler(ec);
        return;
    }

    // Perform SSL handshake
    try
    {
        initiate_transport_operation(
            TransportOperationKind::tls_handshake,
            [self = shared_from_this(),
             handler](TransportOperationId handshake_id) {
                self->socket_->async_handshake(
                    asio::ssl::stream_base::client,
                    [self, handler, handshake_id](
                        boost::system::error_code ec) {
                        TransportOperationCompletion handshake_complete{
                            self.get(), handshake_id};
                        if (self->physical_phase_ != PhysicalPhase::open)
                        {
                            handler(boost::asio::error::operation_aborted);
                            return;
                        }
                        self->handle_handshake(ec, handler);
                    });
            });
    }
    catch (...)
    {
        auto const start_error = current_exception_error();
        note_connect_failure("tls handshake initiation", start_error);
        handler(start_error);
    }
}

void
peer_connection::handle_handshake(
    const boost::system::error_code& ec,
    const connection_handler& handler)
{
    if (physical_phase_ != PhysicalPhase::open)
    {
        handler(boost::asio::error::operation_aborted);
        return;
    }
    if (ec)
    {
        note_connect_failure("tls handshake", ec);
        handler(ec);
        return;
    }

    // Get SSL session info for creating signature
    SSL* ssl = socket_->native_handle();

    // Get finished messages
    std::vector<std::uint8_t> finished(1024);
    std::size_t finished_len =
        SSL_get_finished(ssl, finished.data(), finished.size());
    if (finished_len < 12)
    {
        set_connect_failure_detail("tls handshake: finished message too short");
        handler(boost::asio::error::invalid_argument);
        return;
    }
    finished.resize(finished_len);

    std::vector<std::uint8_t> peer_finished(1024);
    std::size_t peer_finished_len =
        SSL_get_peer_finished(ssl, peer_finished.data(), peer_finished.size());
    if (peer_finished_len < 12)
    {
        set_connect_failure_detail(
            "tls handshake: peer finished message too short");
        handler(boost::asio::error::invalid_argument);
        return;
    }
    peer_finished.resize(peer_finished_len);

    // Generate node keys
    generate_node_keys();

    // Create cookie and signature
    crypto_utils crypto;
    auto cookie = crypto.create_ssl_cookie(finished, peer_finished);
    auto signature = crypto.create_session_signature(secret_key_, cookie);

    // Store signature for HTTP upgrade
    session_signature_ = signature;
    // Retain the shared cookie so we can verify the peer's Session-Signature
    // once the upgrade headers arrive (sec #0053). The cookie is symmetric:
    // both peers derive the same value from the TLS finished messages.
    session_cookie_ = cookie;

    // Perform HTTP upgrade
    perform_http_upgrade(handler);
}

void
peer_connection::generate_node_keys()
{
    crypto_utils crypto;

    crypto_utils::node_keys keys;

    // Precedence:
    //   1. process-global base58 seed (CATL_NODE_SEED via xprv)
    //   2. per-connection peer_config.node_private_key override
    //   3. process-global keys file path (CATL_NODE_CREDENTIALS via xprv)
    //   4. $HOME/.peermon (default — now persisted on first run)
    if (auto seed = node_identity::seed_b58(); seed.has_value())
    {
        keys = crypto.node_keys_from_private(*seed);
    }
    else if (config_.node_private_key.has_value())
    {
        keys = crypto.node_keys_from_private(config_.node_private_key.value());
    }
    else
    {
        std::string key_file;
        if (auto path = node_identity::keys_path(); path.has_value())
        {
            key_file = *path;
        }
        else
        {
            const char* home = std::getenv("HOME");
            key_file = std::string(home ? home : "") + "/.peermon";
        }
        keys = crypto.load_or_generate_node_keys(key_file);
    }

    secret_key_ = keys.secret_key;
    public_key_compressed_ = keys.public_key_compressed;
    node_public_key_b58_ = keys.public_key_b58;

    // Note: no startup log here — apply_node_identity (xprv main) logs
    // the resolved public key once at process start. Doing it again
    // per-handshake would spam Cloud Run logs.
}

void
peer_connection::verify_peer_session_signature(
    std::string const& public_key_b58,
    std::string const& session_signature_b64)
{
    if (public_key_b58.empty() || session_signature_b64.empty())
    {
        PLOGW(
            log_,
            "[",
            config_.host,
            ":",
            config_.port,
            "] peer omitted Public-Key/Session-Signature; "
            "cannot verify session identity");
        return;
    }

    // Decode the peer's node public key (base58, NODE_PUBLIC version → 33 B).
    auto pub = base58::decode_node_public(public_key_b58);
    if (!pub || pub->size() != 33)
    {
        PLOGW(
            log_,
            "[",
            config_.host,
            ":",
            config_.port,
            "] peer Public-Key not decodable; skipping session-sig verify");
        return;
    }

    // Decode the base64 (original variant) Session-Signature → DER bytes.
    // A secp256k1 DER signature is ≤ 72 bytes; 128 is comfortable headroom.
    std::array<std::uint8_t, 128> der{};
    std::size_t der_len = 0;
    if (sodium_base642bin(
            der.data(),
            der.size(),
            session_signature_b64.data(),
            session_signature_b64.size(),
            nullptr,
            &der_len,
            nullptr,
            sodium_base64_VARIANT_ORIGINAL) != 0)
    {
        PLOGW(
            log_,
            "[",
            config_.host,
            ":",
            config_.port,
            "] could not decode peer Session-Signature (bad base64 or too "
            "long); skipping verify");
        return;
    }

    // The signature is over the 32-byte shared cookie DIRECTLY (the cookie is
    // already a digest), so verify against the raw cookie — NOT verify_message,
    // which would SHA512 it again. Handle both node-key flavours.
    bool ok = false;
    switch (catl::crypto::detect_key_type(*pub))
    {
        case catl::crypto::KeyType::secp256k1:
            ok = catl::crypto::verify_secp256k1(
                *pub,
                std::span<const std::uint8_t>(der.data(), der_len),
                session_cookie_);
            break;
        case catl::crypto::KeyType::ed25519:
            ok = catl::crypto::verify_ed25519(
                std::span<const std::uint8_t>(pub->data() + 1, 32),
                std::span<const std::uint8_t>(der.data(), der_len),
                session_cookie_);
            break;
        case catl::crypto::KeyType::unknown:
            break;
    }

    if (ok)
    {
        PLOGD(
            log_,
            "[",
            config_.host,
            ":",
            config_.port,
            "] session-signature verified (",
            public_key_b58,
            ")");
    }
    else
    {
        PLOGW(
            log_,
            "[",
            config_.host,
            ":",
            config_.port,
            "] session-signature verification FAILED for ",
            public_key_b58,
            " — peer may not control the advertised node key "
            "(continuing; no trust decision keys off this)");
    }
}

void
peer_connection::release_inbound_charge()
{
    // exchange → release exactly once even if a cross-thread close() races a
    // frame completion (sec #0055).
    std::size_t prev = inbound_charge_.exchange(0, std::memory_order_relaxed);
    if (prev != 0)
    {
        global_inbound_budget().release(prev);
    }
}

void
peer_connection::perform_http_upgrade(const connection_handler& handler)
{
    if (physical_phase_ != PhysicalPhase::open)
    {
        handler(boost::asio::error::operation_aborted);
        return;
    }

    if (config_.listen_mode)
    {
        // In listen mode, we wait for the client's request first
        auto buffer = std::make_shared<beast::flat_buffer>();
        try
        {
            initiate_transport_operation(
                TransportOperationKind::http_read,
                [self = shared_from_this(), buffer, handler](
                    TransportOperationId read_id) {
                    http::async_read(
                        *self->socket_,
                        *buffer,
                        self->http_request_,
                        [self, buffer, handler, read_id](
                            boost::system::error_code ec, std::size_t) {
                            TransportOperationCompletion read_complete{
                                self.get(), read_id};
                            if (self->physical_phase_ != PhysicalPhase::open)
                            {
                                handler(boost::asio::error::operation_aborted);
                                return;
                            }
                            if (!ec)
                            {
                                self->handle_http_request(handler);
                            }
                            else
                            {
                                self->note_connect_failure(
                                    "http upgrade request read", ec);
                                handler(ec);
                            }
                        });
                });
        }
        catch (...)
        {
            auto const start_error = current_exception_error();
            note_connect_failure(
                "http upgrade request read initiation", start_error);
            handler(start_error);
        }
    }
    else
    {
        // In client mode, we send the upgrade request
        send_http_request(handler);
    }
}

void
peer_connection::send_http_request(const connection_handler& handler)
{
    if (physical_phase_ != PhysicalPhase::open)
    {
        handler(boost::asio::error::operation_aborted);
        return;
    }

    auto req = std::make_shared<http::request<http::string_body>>(
        http::verb::get, "/", 11);
    req->set(http::field::user_agent, "catl-peer-client/0.1");
    req->set(http::field::upgrade, "XRPL/2.2");
    req->set(http::field::connection, "Upgrade");
    req->set("Connect-As", "Peer");
    req->set("Crawl", "private");

    // Network ID — only send if non-zero (mainnet nodes may not expect it)
    if (config_.network_id != 0)
    {
        req->set("Network-ID", std::to_string(config_.network_id));
    }

    req->set(
        "Network-Time", std::to_string(catl::common::current_ripple_time()));

    req->set("Session-Signature", session_signature_);
    req->set("Public-Key", node_public_key_b58_);

    try
    {
        initiate_transport_operation(
            TransportOperationKind::http_write,
            [self = shared_from_this(), req, handler](
                TransportOperationId write_id) {
                http::async_write(
                    *self->socket_,
                    *req,
                    [self, req, handler, write_id](
                        boost::system::error_code ec, std::size_t) {
                        TransportOperationCompletion write_complete{
                            self.get(), write_id};
                        if (self->physical_phase_ != PhysicalPhase::open)
                        {
                            handler(boost::asio::error::operation_aborted);
                            return;
                        }
                        if (ec)
                        {
                            self->note_connect_failure(
                                "http upgrade write", ec);
                            handler(ec);
                            return;
                        }

                        // Read the response before releasing the write receipt,
                        // so close cannot observe a false zero between stages.
                        auto buffer = std::make_shared<beast::flat_buffer>();
                        try
                        {
                            self->initiate_transport_operation(
                                TransportOperationKind::http_read,
                                [self, buffer, handler](
                                    TransportOperationId read_id) {
                                    http::async_read(
                                        *self->socket_,
                                        *buffer,
                                        self->http_response_,
                                        [self, buffer, handler, read_id](
                                            boost::system::error_code ec,
                                            std::size_t) {
                                            TransportOperationCompletion
                                                read_complete{
                                                    self.get(), read_id};
                                            if (self->physical_phase_ !=
                                                PhysicalPhase::open)
                                            {
                                                handler(boost::asio::error::
                                                            operation_aborted);
                                                return;
                                            }
                                            if (!ec)
                                            {
                                                self->handle_http_response(
                                                    handler);
                                            }
                                            else
                                            {
                                                self->note_connect_failure(
                                                    "http upgrade read", ec);
                                                handler(ec);
                                            }
                                        });
                                });
                        }
                        catch (...)
                        {
                            auto const start_error = current_exception_error();
                            self->note_connect_failure(
                                "http upgrade read initiation", start_error);
                            handler(start_error);
                        }
                    });
            });
    }
    catch (...)
    {
        auto const start_error = current_exception_error();
        note_connect_failure("http upgrade write initiation", start_error);
        handler(start_error);
    }
}

void
peer_connection::handle_http_request(const connection_handler& handler)
{
    if (physical_phase_ != PhysicalPhase::open)
    {
        handler(boost::asio::error::operation_aborted);
        return;
    }

    upgrade_headers_.clear();
    for (auto const& field : http_request_)
        upgrade_headers_[std::string(field.name_string())] =
            std::string(field.value());

    // Verify the connecting peer's Session-Signature (sec #0053).
    {
        std::string peer_pubkey;
        std::string peer_sig;
        if (auto it = http_request_.find("Public-Key");
            it != http_request_.end())
            peer_pubkey = std::string(it->value());
        if (auto it = http_request_.find("Session-Signature");
            it != http_request_.end())
            peer_sig = std::string(it->value());
        verify_peer_session_signature(peer_pubkey, peer_sig);
    }

    // Send upgrade response
    auto res = std::make_shared<http::response<http::string_body>>(
        http::status::switching_protocols, 11);
    res->set(http::field::connection, "Upgrade");
    res->set(http::field::upgrade, http_request_["Upgrade"]);
    res->set("Connect-As", "Peer");
    res->set(http::field::server, "catl-peer-client/0.1");
    res->set("Crawl", "private");
    res->set("Public-Key", node_public_key_b58_);
    res->set("Session-Signature", session_signature_);

    try
    {
        initiate_transport_operation(
            TransportOperationKind::http_write,
            [self = shared_from_this(), res, handler](
                TransportOperationId write_id) {
                http::async_write(
                    *self->socket_,
                    *res,
                    [self, res, handler, write_id](
                        boost::system::error_code ec, std::size_t) {
                        TransportOperationCompletion write_complete{
                            self.get(), write_id};
                        if (self->physical_phase_ != PhysicalPhase::open)
                        {
                            handler(boost::asio::error::operation_aborted);
                            return;
                        }
                        if (!ec)
                        {
                            self->http_upgraded_ = true;
                            self->connected_ = true;

                            // Send initial ping in listen mode
                            if (self->config_.listen_mode)
                            {
                                self->send_initial_ping();
                            }

                            handler(ec);
                        }
                        else
                        {
                            self->note_connect_failure(
                                "http upgrade response write", ec);
                            handler(ec);
                        }
                    });
            });
    }
    catch (...)
    {
        auto const start_error = current_exception_error();
        note_connect_failure(
            "http upgrade response write initiation", start_error);
        handler(start_error);
    }
}

void
peer_connection::handle_http_response(const connection_handler& handler)
{
    if (http_response_.result() != http::status::switching_protocols)
    {
        auto const status = http_response_.result();
        auto const routine_http_response =
            status == http::status::service_unavailable ||
            status == http::status::bad_request;
        auto const status_code = static_cast<int>(status);

        if (routine_http_response)
        {
            PLOGD(
                log_,
                "[",
                config_.host,
                ":",
                config_.port,
                "] HTTP upgrade returned status: ",
                status_code);
        }
        else
        {
            PLOGE(
                log_,
                "[",
                config_.host,
                ":",
                config_.port,
                "] HTTP upgrade failed with status: ",
                status_code);
        }

        std::ostringstream detail;
        detail << "http upgrade status=" << status_code;
        if (!http_response_.reason().empty())
            detail << " reason=\"" << http_response_.reason() << '"';

        // Log response headers — may contain redirect or error info
        for (auto const& field : http_response_)
        {
            if (routine_http_response)
            {
                PLOGT(
                    log_,
                    "[",
                    config_.host,
                    ":",
                    config_.port,
                    "]   ",
                    field.name_string(),
                    ": ",
                    field.value());
            }
            else
            {
                PLOGD(
                    log_,
                    "[",
                    config_.host,
                    ":",
                    config_.port,
                    "]   ",
                    field.name_string(),
                    ": ",
                    field.value());
            }
        }

        // Parse body — 503 responses include {"peer-ips": [...]} redirects
        if (!http_response_.body().empty())
        {
            PLOGD(
                log_,
                "[",
                config_.host,
                ":",
                config_.port,
                "]   body: ",
                http_response_.body());

            try
            {
                auto jv = boost::json::parse(http_response_.body());
                if (jv.is_object() && jv.as_object().contains("peer-ips"))
                {
                    auto const& ips = jv.as_object().at("peer-ips").as_array();
                    for (auto const& ip : ips)
                    {
                        redirect_ips_.insert(std::string(ip.as_string()));
                    }
                    PLOGD(
                        log_,
                        "[",
                        config_.host,
                        ":",
                        config_.port,
                        "] 503 redirect: ",
                        redirect_ips_.size(),
                        " peer-ips");
                    detail << " redirect_peers=" << redirect_ips_.size();
                }
            }
            catch (...)
            {
                // Not JSON or no peer-ips — that's fine
            }

            detail << " body=\"" << truncate_for_log(http_response_.body())
                   << '"';
        }

        set_connect_failure_detail(detail.str());

        handler(boost::asio::error::invalid_argument);
        return;
    }

    // Log important response headers
    PLOGD(log_, "HTTP upgrade successful - examining response headers:");

    upgrade_headers_.clear();
    for (auto const& field : http_response_)
        upgrade_headers_[std::string(field.name_string())] =
            std::string(field.value());

    // Check protocol version
    if (auto it = http_response_.find("Upgrade"); it != http_response_.end())
    {
        PLOGD(log_, "  Protocol version: ", it->value());
        protocol_version_ = std::string(it->value());
    }

    // Check server version
    if (auto it = http_response_.find("Server"); it != http_response_.end())
    {
        PLOGD(log_, "  Server: ", it->value());
        server_version_ = std::string(it->value());
    }

    // Check feature support (X-Protocol-Ctl)
    if (auto it = http_response_.find("X-Protocol-Ctl");
        it != http_response_.end())
    {
        PLOGD(log_, "  Protocol features: ", it->value());
        // TODO: Parse features like LEDGER_REPLAY=1
    }

    // Check network ID
    if (auto it = http_response_.find("Network-ID"); it != http_response_.end())
    {
        PLOGD(log_, "  Network ID: ", it->value());
        network_id_ = std::string(it->value());
    }

    // Check public key
    if (auto it = http_response_.find("Public-Key"); it != http_response_.end())
    {
        PLOGD(log_, "  Node public key: ", it->value());
    }

    // Check closed ledger info
    if (auto it = http_response_.find("Closed-Ledger");
        it != http_response_.end())
    {
        PLOGD(log_, "  Closed ledger: ", it->value());
    }

    if (auto it = http_response_.find("Previous-Ledger");
        it != http_response_.end())
    {
        PLOGD(log_, "  Previous ledger: ", it->value());
    }

    // Log any other headers we might not know about
    for (auto const& field : http_response_)
    {
        if (field.name_string() != "Upgrade" &&
            field.name_string() != "Server" &&
            field.name_string() != "X-Protocol-Ctl" &&
            field.name_string() != "Network-ID" &&
            field.name_string() != "Public-Key" &&
            field.name_string() != "Closed-Ledger" &&
            field.name_string() != "Previous-Ledger" &&
            field.name_string() != "Connection" &&
            field.name_string() != "Connect-As")
        {
            PLOGD(log_, "  ", field.name_string(), ": ", field.value());
        }
    }

    // Verify the peer authenticated the TLS session with its node key
    // (sec #0053). Detection-only — see verify_peer_session_signature.
    {
        std::string peer_pubkey;
        std::string peer_sig;
        if (auto it = http_response_.find("Public-Key");
            it != http_response_.end())
            peer_pubkey = std::string(it->value());
        if (auto it = http_response_.find("Session-Signature");
            it != http_response_.end())
            peer_sig = std::string(it->value());
        verify_peer_session_signature(peer_pubkey, peer_sig);
    }

    http_upgraded_ = true;
    connected_ = true;
    set_connect_failure_detail({});

    // Now that upgrade is complete, we can start reading packets
    handler({});
}

void
peer_connection::start_read(packet_handler handler)
{
    if (physical_phase_ != PhysicalPhase::open)
        return;
    packet_handler_ = std::move(handler);
    async_read_header();
}

void
peer_connection::async_read_header()
{
    if (physical_phase_ != PhysicalPhase::open)
        return;

    // Read first 6 bytes of header (4 bytes size + 2 bytes type)
    try
    {
        initiate_transport_operation(
            TransportOperationKind::header_read,
            [self = shared_from_this()](TransportOperationId read_id) {
                auto const buffer =
                    asio::buffer(self->header_buffer_.data(), 6);
                test_read_completion completion =
                    [self, read_id](
                        boost::system::error_code ec,
                        std::size_t bytes_transferred) {
                        TransportOperationCompletion read_complete{
                            self.get(), read_id};
                        if (self->physical_phase_ != PhysicalPhase::open)
                            return;
                        if (!ec)
                        {
                            PLOGT(
                                log_,
                                "Read header: ",
                                bytes_transferred,
                                " bytes");
                        }
                        self->handle_read_header(ec, bytes_transferred);
                    };
                if (self->read_initiator_for_test_)
                {
                    self->read_initiator_for_test_(
                        test_read_stage::header, buffer, std::move(completion));
                }
                else
                {
                    asio::async_read(
                        *self->socket_, buffer, std::move(completion));
                }
            });
    }
    catch (...)
    {
        auto const start_error = current_exception_error();
        note_connect_failure("packet header read initiation", start_error);
        fail_and_close(start_error);
    }
}

void
peer_connection::handle_read_header(
    boost::system::error_code ec,
    std::size_t /*bytes_transferred*/)
{
    if (physical_phase_ != PhysicalPhase::open)
        return;

    if (ec)
    {
        // Log exact error code for debugging disconnects
        PLOGD(
            log_,
            remote_endpoint(),
            " error reading header: ",
            ec.message(),
            " (val=",
            ec.value(),
            ")");

        if (ec != asio::error::operation_aborted)
        {
            note_connect_failure("packet header read", ec);
            PLOGW(
                log_,
                remote_endpoint(),
                " connection closed during header read: ",
                ec.message(),
                " (val=",
                ec.value(),
                ")");
            fail_and_close(ec);
        }
        return;
    }

    // Parse header
    // First check if compressed by looking at high nibble of first byte
    current_header_.compressed = (header_buffer_[0] & 0xF0) != 0;

    // Extract payload size (masking off compression bits)
    auto const payload_size =
        catl::common::get_uint32_be(header_buffer_.data()) & 0x0FFF'FFFFu;

    if (current_header_.compressed)
    {
        PLOGT(
            log_,
            "Compressed packet detected, need to read additional header bytes");
    }

    current_header_.payload_size = payload_size;
    current_header_.type = (header_buffer_[4] << 8) | header_buffer_[5];

    PLOGT(
        log_,
        "Header parsed: type=",
        current_header_.type,
        " payload_size=",
        payload_size,
        " compressed=",
        current_header_.compressed);

    if (current_header_.compressed)
    {
        // Need to read the additional 4 bytes for uncompressed size
        try
        {
            initiate_transport_operation(
                TransportOperationKind::header_extension_read,
                [self = shared_from_this(),
                 payload_size](TransportOperationId read_id) {
                    auto const buffer =
                        asio::buffer(self->header_buffer_.data() + 6, 4);
                    test_read_completion completion =
                        [self, payload_size, read_id](
                            boost::system::error_code ec, std::size_t) {
                            TransportOperationCompletion read_complete{
                                self.get(), read_id};
                            if (self->physical_phase_ != PhysicalPhase::open)
                                return;
                            if (ec)
                            {
                                PLOGE(
                                    log_,
                                    "Error reading compressed header "
                                    "extension: ",
                                    ec.message());
                                if (ec != asio::error::operation_aborted)
                                {
                                    self->note_connect_failure(
                                        "packet header extension read", ec);
                                    self->fail_and_close(ec);
                                }
                                return;
                            }
                            self->current_header_.uncompressed_size =
                                catl::common::get_uint32_be(
                                    self->header_buffer_.data() + 6);

                            self->current_header_.payload_size = payload_size;
                            self->read_or_drain_payload();
                        };
                    if (self->read_initiator_for_test_)
                    {
                        self->read_initiator_for_test_(
                            test_read_stage::header_extension,
                            buffer,
                            std::move(completion));
                    }
                    else
                    {
                        asio::async_read(
                            *self->socket_, buffer, std::move(completion));
                    }
                });
        }
        catch (...)
        {
            auto const start_error = current_exception_error();
            note_connect_failure(
                "packet header extension read initiation", start_error);
            fail_and_close(start_error);
        }
        return;
    }

    current_header_.uncompressed_size = payload_size;
    read_or_drain_payload();
}

void
peer_connection::read_or_drain_payload()
{
    if (physical_phase_ != PhysicalPhase::open)
    {
        release_inbound_charge();
        return;
    }

    auto const payload_size = current_header_.payload_size;

    // Manifest history snapshots are optional for this client. Consume large
    // snapshots incrementally instead of growing the reusable connection buffer
    // to a peer-selected size, then continue reading subsequent peer traffic.
    if (should_drain_manifest_payload(current_header_))
    {
        // Optional manifest history is consumed raw, without inflating or
        // reserving the peer-selected size in the process-wide buffer budget.
        // This branch must precede the general frame cap: real XRPL peers can
        // send manifest snapshots larger than one buffered frame.
        drain_payload(payload_size);
        return;
    }

    // Non-manifest frames, and manifest frames small enough to dispatch, still
    // receive both wire and declared post-inflate bounds before allocation.
    if (payload_size > kMaxFramePayloadSize ||
        current_header_.uncompressed_size > kMaxFramePayloadSize)
    {
        PLOGW(
            log_,
            remote_endpoint(),
            " rejecting oversized frame: payload_size=",
            payload_size,
            " uncompressed_size=",
            current_header_.uncompressed_size,
            " > max=",
            kMaxFramePayloadSize);
        note_connect_failure("oversized frame", asio::error::message_size);
        fail_and_close(asio::error::message_size);
        return;
    }

    // Reserve this frame's bytes from the process-wide inbound budget only
    // after the complete header has established that this frame is buffered.
    if (!global_inbound_budget().try_acquire(payload_size))
    {
        PLOGW(
            log_,
            remote_endpoint(),
            " inbound budget exhausted (in_flight=",
            global_inbound_budget().in_flight(),
            " + ",
            payload_size,
            " > ",
            global_inbound_budget().ceiling(),
            "); dropping connection");
        note_connect_failure(
            "inbound budget exhausted", asio::error::no_buffer_space);
        fail_and_close(asio::error::no_buffer_space);
        return;
    }
    inbound_charge_.store(payload_size, std::memory_order_relaxed);

    // Read payload
    payload_buffer_.resize(payload_size);
    try
    {
        initiate_transport_operation(
            TransportOperationKind::payload_read,
            [self = shared_from_this()](TransportOperationId read_id) {
                auto const buffer = asio::buffer(self->payload_buffer_);
                test_read_completion completion =
                    [self, read_id](
                        boost::system::error_code ec,
                        std::size_t bytes_transferred) {
                        TransportOperationCompletion read_complete{
                            self.get(), read_id};
                        if (self->physical_phase_ != PhysicalPhase::open)
                            return;
                        self->handle_read_payload(ec, bytes_transferred);
                    };
                if (self->read_initiator_for_test_)
                {
                    self->read_initiator_for_test_(
                        test_read_stage::payload,
                        buffer,
                        std::move(completion));
                }
                else
                {
                    asio::async_read(
                        *self->socket_, buffer, std::move(completion));
                }
            });
    }
    catch (...)
    {
        auto const start_error = current_exception_error();
        note_connect_failure("packet payload read initiation", start_error);
        fail_and_close(start_error);
    }
}

void
peer_connection::handle_read_payload(
    boost::system::error_code ec,
    std::size_t bytes_read)
{
    if (physical_phase_ != PhysicalPhase::open)
        return;

    if (ec)
    {
        PLOGD(
            log_,
            remote_endpoint(),
            " error reading payload: ",
            ec.message(),
            " (val=",
            ec.value(),
            ")");
        // Check if it's EOF or connection closed
        if (ec != asio::error::operation_aborted)
        {
            note_connect_failure("packet payload read", ec);
            PLOGW(
                log_,
                remote_endpoint(),
                " connection closed during payload read: ",
                ec.message(),
                " (val=",
                ec.value(),
                ")");
            fail_and_close(ec);
        }
        return;
    }

    PLOGT(
        log_,
        "Read payload: ",
        bytes_read,
        " bytes for packet type ",
        current_header_.type);

    // Copy the callback capability before exposure. A re-entrant close clears
    // the stored handler to break its owner cycle; the local copy keeps the
    // currently executing target alive until this frame returns.
    try
    {
        auto const packet_handler = packet_handler_;
        if (packet_handler)
            packet_handler(current_header_, payload_buffer_);
    }
    catch (std::exception const& e)
    {
        PLOGE(log_, "Exception in packet handler: ", e.what());
    }

    // Frame consumed — return its bytes to the inbound budget before the
    // next read (sec #0055).
    release_inbound_charge();

    // The packet callback is external code and may synchronously close this
    // connection. Recheck the seal before it can rearm the read loop.
    //@@authority transition=prv.T0144 status=provisional scope=statement
    // bind=if_statement@78b0eb52dc803579
    // owner=unresolved owner_hypothesis=peer_connection
    // commit=connection_strand touches=prv.S0048 mutation_class=running_guarded
    // label=refuse_read_rearm_after_reentrant_close
    if (physical_phase_ != PhysicalPhase::open)
        return;  //@@authority-target ids=prv.T0144

    // Continue reading
    async_read_header();
}

void
peer_connection::drain_payload(std::size_t remaining)
{
    if (physical_phase_ != PhysicalPhase::open)
        return;

    if (remaining == 0)
    {
        // Whole frame consumed and discarded — resume the header loop.
        async_read_header();
        return;
    }

    // Reuse the read buffer at a bounded chunk size — this is what stops a
    // drained frame from ever allocating (or retaining) its full size.
    constexpr std::size_t kDrainChunk = 64u * 1024;
    std::size_t const chunk = remaining < kDrainChunk ? remaining : kDrainChunk;
    payload_buffer_.resize(chunk);
    try
    {
        initiate_transport_operation(
            TransportOperationKind::payload_drain,
            [self = shared_from_this(), remaining, chunk](
                TransportOperationId read_id) {
                auto const buffer =
                    asio::buffer(self->payload_buffer_.data(), chunk);
                test_read_completion completion =
                    [self, remaining, read_id](
                        boost::system::error_code ec, std::size_t n) {
                        TransportOperationCompletion read_complete{
                            self.get(), read_id};
                        if (self->physical_phase_ != PhysicalPhase::open)
                            return;
                        if (ec)
                        {
                            if (ec != asio::error::operation_aborted)
                            {
                                self->note_connect_failure(
                                    "manifest drain", ec);
                                self->fail_and_close(ec);
                            }
                            return;
                        }
                        self->drain_payload(remaining - n);
                    };
                if (self->read_initiator_for_test_)
                {
                    self->read_initiator_for_test_(
                        test_read_stage::payload_drain,
                        buffer,
                        std::move(completion));
                }
                else
                {
                    asio::async_read(
                        *self->socket_, buffer, std::move(completion));
                }
            });
    }
    catch (...)
    {
        auto const start_error = current_exception_error();
        note_connect_failure("manifest drain initiation", start_error);
        fail_and_close(start_error);
    }
}

//@@authority transition=peer_client.admit_write owner=peer_client commit=strand
// bind=catl::peer_client::async_send_packet@8fd1fdbed8dbc03d
// Admission authority. Refuses before any queue mutation once admission closes.
peer_connection::SendAdmission
peer_connection::async_send_packet(
    packet_type type,
    std::vector<std::uint8_t> const& data,
    std::function<void(boost::system::error_code)> handler,
    send_initiated_handler on_initiated)
{
    // Closed admission, refused BEFORE any queue mutation. Without this gate a
    // send after close_impl() was queued against a dead transport, and its
    // completion then returned on `closing_` without popping the row or
    // invoking the handler — a permanently stranded caller. Refuse here and
    // terminalize exactly once, so no committed queue entry can outlive the
    // close that was supposed to drain it.
    if (admission_closed_)
    {
        deliver_terminal(handler, asio::error::operation_aborted);
        return SendAdmission::admission_closed;
    }

    // Build the wire packet: header(6) + payload
    std::vector<std::uint8_t> packet;
    packet.reserve(6 + data.size());

    std::uint32_t payload_size = static_cast<std::uint32_t>(data.size());
    packet.push_back((payload_size >> 24) & 0x0F);
    packet.push_back((payload_size >> 16) & 0xFF);
    packet.push_back((payload_size >> 8) & 0xFF);
    packet.push_back(payload_size & 0xFF);

    std::uint16_t type_val = static_cast<std::uint16_t>(type);
    packet.push_back((type_val >> 8) & 0xFF);
    packet.push_back(type_val & 0xFF);

    packet.insert(packet.end(), data.begin(), data.end());

    // Cap write queue to prevent unbounded growth on slow/dead peers
    static constexpr size_t kMaxWriteQueue = 256;
    if (write_queue_.size() >= kMaxWriteQueue)
    {
        PLOGW(
            log_,
            remote_endpoint(),
            " write queue full (",
            kMaxWriteQueue,
            "), closing");
        // Commit the failure transition FIRST. Delivering this caller's
        // callback before teardown let a handler that retries synchronously see
        // a still-full queue and an open gate, so it could recurse through
        // queue-full refusal without bound.
        fail_and_close(boost::asio::error::no_buffer_space);
        deliver_terminal(handler, boost::asio::error::no_buffer_space);
        return SendAdmission::queue_full;
    }

    // Enqueue and pump — only one async_write in flight at a time
    write_queue_.push_back(
        {std::move(packet), std::move(handler), std::move(on_initiated)});
    do_write();
    return SendAdmission::accepted;
}

//@@authority transition=peer_client.initiate_write owner=peer_client
// commit=strand bind=catl::peer_client::do_write@ed5de5ed7752db65
// Selects one successor into the active row and hands its bytes to Asio.
void
peer_connection::do_write()
{
    // `active_write_` is the in-flight latch. A separate bool could claim
    // "idle" while Asio still owned a buffer, which is how the queue got
    // wedged.
    if (active_write_)
        return;

    // Rollback for a throwing initiation. The composed operation never started,
    // so no Asio operation owns these bytes and destroying them here is safe —
    // but it MUST happen, or the slot latches forever and every later write
    // enqueues behind a write that will never complete.
    auto rollback = [this](char const* why) {
        auto entry = std::move(*active_write_);
        active_write_.reset();
        PLOGW(
            log_,
            remote_endpoint(),
            " write initiation failed (",
            why,
            "), rolling back active slot");
        deliver_terminal(
            entry.handler,
            boost::system::errc::make_error_code(
                boost::system::errc::io_error));
    };

    // Loop rather than recurse: a persistently throwing initiation must still
    // terminalize every queued handler exactly once, and the queue shrinks each
    // iteration so this terminates.
    // `!active_write_` is re-tested every iteration, not just on entry. The
    // rollback below invokes an external handler synchronously; that handler
    // may re-enter async_send_packet -> do_write and install an active row.
    // Without this guard the outer frame resumed and assigned over an ENGAGED
    // optional, destroying the live buffer and the undelivered terminal of the
    // row the nested frame had just initiated.
    while (!active_write_ && !write_queue_.empty())
    {
        active_write_ = std::move(write_queue_.front());
        write_queue_.pop_front();

        std::optional<TransportOperationId> operation;
        send_initiated_handler on_initiated =
            std::move(active_write_->on_initiated);
        active_write_->on_initiated = nullptr;
        try
        {
            operation =
                begin_transport_operation(TransportOperationKind::packet_write);
            if (write_initiator_for_test_)
            {
                write_initiator_for_test_(
                    asio::buffer(active_write_->packet),
                    [self = shared_from_this(),
                     id = *operation](boost::system::error_code ec) {
                        TransportOperationCompletion complete{self.get(), id};
                        self->on_write_complete(ec);
                    });
            }
            else
            {
                asio::async_write(
                    *socket_,
                    asio::buffer(active_write_->packet),
                    [self = shared_from_this(), id = *operation](
                        boost::system::error_code ec, std::size_t) {
                        TransportOperationCompletion complete{self.get(), id};
                        self->on_write_complete(ec);
                    });
            }
        }
        catch (std::exception const& exc)
        {
            if (operation)
                complete_transport_operation(*operation);
            rollback(exc.what());
            continue;
        }
        catch (...)
        {
            if (operation)
                complete_transport_operation(*operation);
            rollback("unknown exception");
            continue;
        }

        // The initiation call returned normally, so Asio now owns the active
        // row's bytes. Publish that retained fact separately from queue
        // admission. The callback is exception-contained because rollback is
        // no longer legal after transport initiation succeeds.
        if (on_initiated)
        {
            try
            {
                on_initiated();
            }
            catch (std::exception const& exc)
            {
                PLOGW(log_, "write initiation observer threw: ", exc.what());
            }
            catch (...)
            {
                PLOGW(log_, "write initiation observer threw");
            }
        }
        return;  // initiated — the completion owns the rest
    }
}

//@@authority transition=peer_client.complete_write owner=peer_client
// commit=strand bind=catl::peer_client::on_write_complete@16dc470ccb9bfcd7
// The only releaser of the active row's physical bytes.
void
peer_connection::on_write_complete(boost::system::error_code ec)
{
    if (!active_write_)
        return;  // defensive: nothing completes without a slot

    // First moment the active packet's memory may be released. Physical
    // lifetime ends here and only here — this is what close() must not do
    // early.
    auto entry = std::move(*active_write_);
    active_write_.reset();

    bool const fatal = ec && ec != asio::error::operation_aborted;
    if (fatal && !close_ran_)
    {
        // Close admission BEFORE this handler runs. Otherwise a broken-pipe
        // handler could synchronously enqueue AND physically initiate another
        // write on a transport already known dead. Settlement still happens
        // below: admission and settlement are separate facts precisely so this
        // ordering is expressible.
        admission_closed_ = true;
        // Commit the failure disposition too, not just the gate. Otherwise a
        // handler that calls close() wins close_ran_ with (operation_aborted,
        // fire_disconnect=false) and the connection's sole terminal export is
        // consumed without firing.
        select_close_disposition(ec, /*fire_disconnect=*/true);
        note_connect_failure("packet write", ec);
        PLOGW(
            log_,
            remote_endpoint(),
            " connection lost during write: ",
            ec.message(),
            " (val=",
            ec.value(),
            ")");
    }

    // Logical terminal exactly once, exception-contained. No flag is consulted:
    // if a teardown already delivered it, that teardown MOVED the handler out
    // and this row holds an empty one, so a second delivery is unrepresentable.
    deliver_terminal(entry.handler, ec);

    if (fatal)
    {
        // The exposure above deliberately precedes this settlement: the write's
        // own terminal must arrive before the disconnect export, or PeerClient
        // drains the request as disconnected and then receives its terminal
        // too. Safe because the failure DISPOSITION is already committed to an
        // owned row above, so a re-entrant close() inside that handler cannot
        // downgrade it — which is the second of the two shapes the reviewer
        // allowed.
        //@@audit-ack callback_exposed_before_settlement reason="disposition
        // committed to close_disposition_ before exposure; re-entrant close
        // honors it" ref=2026-07-25-e33cc5a-verdict-B1
        fail_and_close(ec);  // settles: terminals, socket, disconnect
        return;
    }

    if (admission_closed_)
        return;  // teardown owns the remaining terminals

    // Successor-pump state, named explicitly because nothing else owns it.
    // Reached on success AND on a cancellation that arrived without a close:
    // the transport is still open there, so queued successors are still owed an
    // attempt. Returning instead would leave them parked until some later
    // async_send_packet happened to pump them.
    do_write();
}

//@@callback-funnel
// Every caller-supplied write terminal is delivered here and nowhere else, so
// "settle before you notify" and "contain what the notification throws" are
// properties of ONE function instead of properties of every call site.
// `callback-boundary-audit.py` enforces the funnel.
void
peer_connection::deliver_terminal(
    std::function<void(boost::system::error_code)>& handler,
    boost::system::error_code ec) noexcept
{
    if (!handler)
        return;
    auto owned = std::move(handler);
    handler = nullptr;
    try
    {
        owned(ec);
    }
    catch (std::exception const& exc)
    {
        PLOGW(log_, "write terminal handler threw: ", exc.what());
    }
    catch (...)
    {
        PLOGW(log_, "write terminal handler threw a non-exception");
    }
}

void
peer_connection::send_initial_ping()
{
    // Create ping message (empty payload for now - would need protobuf here)
    std::vector<std::uint8_t> ping_data;

    async_send_packet(
        packet_type::ping, ping_data, [](boost::system::error_code ec) {
            if (ec)
            {
                PLOGE(log_, "Failed to send initial ping: ", ec.message());
            }
        });
}

std::string
peer_connection::remote_endpoint() const
{
    if (connected_)
    {
        return remote_endpoint_.address().to_string() + ":" +
            std::to_string(remote_endpoint_.port());
    }
    return "not connected";
}

std::string
peer_connection::connect_failure_detail() const
{
    std::lock_guard lock(connect_failure_mutex_);
    return connect_failure_detail_;
}

void
peer_connection::set_connect_failure_detail(std::string detail)
{
    std::lock_guard lock(connect_failure_mutex_);
    connect_failure_detail_ = std::move(detail);
}

void
peer_connection::note_connect_failure(
    char const* stage,
    boost::system::error_code ec)
{
    std::string detail = stage;
    if (ec)
    {
        detail += ": ";
        detail += ec.message();
    }
    set_connect_failure_detail(std::move(detail));
}

std::string
peer_connection::get_query_hash(uint32_t seq) const
{
    std::lock_guard<std::mutex> lock(query_map_mutex_);
    auto it = query_map_.find(seq);
    if (it != query_map_.end())
    {
        return it->second;
    }
    return "";
}

void
peer_connection::send_transaction_query(
    const std::string& tx_hash,
    const std::string& ledger_hash)
{
    if (!connected_ || !socket_)
    {
        PLOGE(log_, "Cannot send transaction query: not connected");
        return;
    }

    // Convert hex string to bytes
    std::vector<uint8_t> hash_bytes;
    for (size_t i = 0; i < tx_hash.length(); i += 2)
    {
        if (i + 1 < tx_hash.length())
        {
            std::string byte_str = tx_hash.substr(i, 2);
            hash_bytes.push_back(static_cast<uint8_t>(
                std::strtol(byte_str.c_str(), nullptr, 16)));
        }
    }

    // Get a unique sequence number for this query
    uint32_t seq = query_seq_.fetch_add(1);

    // Store the mapping of seq -> tx_hash
    {
        std::lock_guard<std::mutex> lock(query_map_mutex_);
        query_map_[seq] = tx_hash;
    }

    // Only log if not in query mode
    if (!tx_hash.empty())
    {
        PLOGD(
            log_,
            "Sending query for tx ",
            tx_hash.substr(0, 16),
            "... with seq=",
            seq);
    }

    // Create TMGetObjectByHash message
    protocol::TMGetObjectByHash query;
    // Try both types: first as transaction, then add a duplicate query for
    // transaction_node
    query.set_type(protocol::TMGetObjectByHash::otTRANSACTION);
    query.set_query(true);
    query.set_seq(seq);  // Include the sequence number

    // Add ledger hash if specified
    if (!ledger_hash.empty())
    {
        std::vector<uint8_t> ledger_bytes;
        for (size_t i = 0; i < ledger_hash.length(); i += 2)
        {
            if (i + 1 < ledger_hash.length())
            {
                std::string byte_str = ledger_hash.substr(i, 2);
                ledger_bytes.push_back(static_cast<uint8_t>(
                    std::strtol(byte_str.c_str(), nullptr, 16)));
            }
        }
        query.set_ledgerhash(ledger_bytes.data(), ledger_bytes.size());
        PLOGD(
            log_,
            "  Including ledger hash: ",
            ledger_hash.substr(0, 16),
            "...");
    }

    // Add the transaction hash to query
    protocol::TMIndexedObject* obj = query.add_objects();
    obj->set_hash(hash_bytes.data(), hash_bytes.size());

    // Serialize the message
    std::string serialized;
    if (!query.SerializeToString(&serialized))
    {
        PLOGE(log_, "Failed to serialize transaction query");
        return;
    }

    // Send the query using async_send_packet
    PLOGD(log_, "  Serialized size: ", serialized.size(), " bytes");

    async_send_packet(
        packet_type::get_objects,
        std::vector<uint8_t>(serialized.begin(), serialized.end()),
        [tx_hash, seq](boost::system::error_code ec) {
            if (ec)
            {
                PLOGE(
                    log_,
                    "Failed to send query for tx ",
                    tx_hash.substr(0, 16),
                    "... (seq=",
                    seq,
                    "): ",
                    ec.message());
            }
            else
            {
                PLOGD(
                    log_,
                    "Successfully sent query for tx ",
                    tx_hash.substr(0, 16),
                    "... (seq=",
                    seq,
                    ")");
            }
        });

    // Also try querying as TRANSACTION_NODE
    uint32_t seq2 = query_seq_.fetch_add(1);
    {
        std::lock_guard<std::mutex> lock(query_map_mutex_);
        query_map_[seq2] = tx_hash + " (node)";
    }

    protocol::TMGetObjectByHash query2;
    query2.set_type(protocol::TMGetObjectByHash::otTRANSACTION_NODE);
    query2.set_query(true);
    query2.set_seq(seq2);

    // Add ledger hash if specified for node query too
    if (!ledger_hash.empty())
    {
        std::vector<uint8_t> ledger_bytes;
        for (size_t i = 0; i < ledger_hash.length(); i += 2)
        {
            if (i + 1 < ledger_hash.length())
            {
                std::string byte_str = ledger_hash.substr(i, 2);
                ledger_bytes.push_back(static_cast<uint8_t>(
                    std::strtol(byte_str.c_str(), nullptr, 16)));
            }
        }
        query2.set_ledgerhash(ledger_bytes.data(), ledger_bytes.size());
    }

    protocol::TMIndexedObject* obj2 = query2.add_objects();
    obj2->set_hash(hash_bytes.data(), hash_bytes.size());

    std::string serialized2;
    if (query2.SerializeToString(&serialized2))
    {
        PLOGD(log_, "Also trying as TRANSACTION_NODE (seq=", seq2, ")");
        async_send_packet(
            packet_type::get_objects,
            std::vector<uint8_t>(serialized2.begin(), serialized2.end()),
            [tx_hash, seq2](boost::system::error_code ec) {
                if (ec)
                {
                    PLOGE(
                        log_,
                        "Failed to send node query for tx ",
                        tx_hash.substr(0, 16),
                        "... (seq=",
                        seq2,
                        "): ",
                        ec.message());
                }
                else
                {
                    PLOGD(
                        log_,
                        "Successfully sent node query for tx ",
                        tx_hash.substr(0, 16),
                        "... (seq=",
                        seq2,
                        ")");
                }
            });
    }
}

void
peer_connection::request_transaction_set(const std::string& tx_set_hash)
{
    if (!connected_ || !socket_)
    {
        PLOGE(log_, "Cannot request transaction set: not connected");
        return;
    }

    // Convert hex string to bytes
    std::vector<uint8_t> hash_bytes;
    for (size_t i = 0; i < tx_set_hash.length(); i += 2)
    {
        if (i + 1 < tx_set_hash.length())
        {
            std::string byte_str = tx_set_hash.substr(i, 2);
            hash_bytes.push_back(static_cast<uint8_t>(
                std::strtol(byte_str.c_str(), nullptr, 16)));
        }
    }

    // Create TMGetLedger message for candidate transaction set
    protocol::TMGetLedger request;
    request.set_itype(
        protocol::liTS_CANDIDATE);  // Request candidate transaction set
    request.set_ledgerhash(hash_bytes.data(), hash_bytes.size());
    request.set_querydepth(
        3);  // Request up to 3 levels deep (should get whole set)
    request.set_querytype(
        protocol::qtINDIRECT);  // Match rippled's request format

    // Add root node ID - tells peer to start from the root of the SHAMap
    // Format: 32 bytes of zeros (root node ID) + 1 byte depth (0 for root)
    std::string root_node_id(32, '\0');  // 32 zero bytes
    root_node_id += '\0';                // depth = 0 (root)
    *(request.add_nodeids()) = root_node_id;

    PLOGI(
        log_, "Requesting transaction set: ", tx_set_hash.substr(0, 16), "...");
    PLOGI(log_, "  Full hash: ", tx_set_hash);
    PLOGI(log_, "  Using itype=", protocol::liTS_CANDIDATE, " (TS_CANDIDATE)");
    PLOGI(log_, "  Query depth: 3");
    PLOGI(log_, "  Starting from: ROOT node");

    // Serialize the message
    std::string serialized;
    if (!request.SerializeToString(&serialized))
    {
        PLOGE(log_, "Failed to serialize TMGetLedger request");
        return;
    }

    PLOGI(
        log_, "Requesting transaction set: ", tx_set_hash.substr(0, 16), "...");
    PLOGI(
        log_,
        "  itype=",
        protocol::liTS_CANDIDATE,
        " querydepth=3 root_node=yes");

    // Send the request
    async_send_packet(
        packet_type::get_ledger,
        std::vector<uint8_t>(serialized.begin(), serialized.end()),
        [tx_set_hash](boost::system::error_code ec) {
            if (ec)
            {
                PLOGE(
                    log_,
                    "Failed to request transaction set ",
                    tx_set_hash.substr(0, 16),
                    "...: ",
                    ec.message());
            }
            else
            {
                PLOGD(
                    log_,
                    "Successfully requested transaction set ",
                    tx_set_hash.substr(0, 16),
                    "...");
            }
        });
}

void
peer_connection::request_transaction_set_nodes(
    const std::string& tx_set_hash,
    const std::vector<std::string>& node_ids_wire)
{
    if (!connected_ || !socket_)
    {
        PLOGE(log_, "Cannot request transaction set nodes: not connected");
        return;
    }

    if (node_ids_wire.empty())
    {
        PLOGW(
            log_, "request_transaction_set_nodes called with empty node list");
        return;
    }

    // Convert hex string to bytes
    std::vector<uint8_t> hash_bytes;
    for (size_t i = 0; i < tx_set_hash.length(); i += 2)
    {
        if (i + 1 < tx_set_hash.length())
        {
            std::string byte_str = tx_set_hash.substr(i, 2);
            hash_bytes.push_back(static_cast<uint8_t>(
                std::strtol(byte_str.c_str(), nullptr, 16)));
        }
    }

    // Create TMGetLedger message for multiple nodes
    protocol::TMGetLedger request;
    request.set_itype(protocol::liTS_CANDIDATE);
    request.set_ledgerhash(hash_bytes.data(), hash_bytes.size());
    request.set_querydepth(3);  // Request up to 3 levels deep
    request.set_querytype(protocol::qtINDIRECT);

    // Add all node IDs (each is 33 bytes: 32-byte id + 1-byte depth)
    for (auto const& node_id_wire : node_ids_wire)
    {
        *(request.add_nodeids()) = node_id_wire;
    }

    PLOGD(
        log_,
        "Requesting ",
        node_ids_wire.size(),
        " nodes from tx set ",
        tx_set_hash.substr(0, 16),
        "...");

    // Serialize and send
    std::string serialized;
    if (!request.SerializeToString(&serialized))
    {
        PLOGE(log_, "Failed to serialize TMGetLedger nodes request");
        return;
    }

    async_send_packet(
        packet_type::get_ledger,
        std::vector<uint8_t>(serialized.begin(), serialized.end()),
        [](boost::system::error_code ec) {
            if (ec)
            {
                PLOGE(
                    log_,
                    "Failed to request transaction set nodes: ",
                    ec.message());
            }
        });
}

void
peer_connection::set_disconnect_handler(disconnect_handler handler)
{
    disconnect_handler_ = std::move(handler);
}

void
peer_connection::close()
{
    if (!strand_.running_in_this_thread())
    {
        auto self = shared_from_this();
        asio::post(strand_, [self]() { self->close_impl(); });
        return;
    }
    close_impl();
}

//@@authority transition=prv.T0139 status=provisional scope=function
// bind=catl::peer_client::co_close@afee3200600f4a6f
// owner=unresolved owner_hypothesis=connection_rider
// commit=connection_strand touches=prv.S0046,prv.S0048
// mutation_class=running_guarded
// label=join_shared_connection_physical_drain_receipt
asio::awaitable<void>
peer_connection::co_close()
{
    auto self = shared_from_this();
    co_await asio::this_coro::reset_cancellation_state(
        asio::disable_cancellation());
    co_await asio::post(strand_, asio::use_awaitable);

    if (config_.listen_mode)
    {
        throw std::logic_error(
            "peer_connection::co_close cannot own listen accept drain");
    }

    close_impl();
    while (physical_phase_ != PhysicalPhase::drained)
    {
        auto signal = std::make_shared<asio::steady_timer>(
            strand_, asio::steady_timer::time_point::max());
        physical_drain_waiters_.push_back(signal);
        if (after_physical_drain_waiter_registered_for_test_)
        {
            after_physical_drain_waiter_registered_for_test_(
                physical_drain_waiters_.size());
        }

        boost::system::error_code ec;
        co_await signal->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));
    }
}

//@@authority transition=peer_client.settle_connection owner=peer_client
// commit=strand bind=catl::peer_client::settle_and_close@d1f665b00d68defa
// The single teardown transition: commits admission closed, settles
// owner-local, then exposes callbacks.
//@@settlement
void
peer_connection::settle_and_close(
    boost::system::error_code terminal_ec,
    bool fire_disconnect)
{
    // Commit the disposition BEFORE the latch check, so a frame that loses the
    // race still records its cause if none was recorded yet.
    select_close_disposition(terminal_ec, fire_disconnect);

    if (close_ran_)
        return;
    close_ran_ = true;
    //@@authority transition=prv.T0137 status=provisional scope=statement
    // bind=expression_statement@561b30655be3bb43
    // owner=unresolved owner_hypothesis=peer_connection
    // commit=connection_strand touches=prv.S0048 mutation_class=seal_commit
    // label=seal_connection_transport_admission
    physical_phase_ =
        PhysicalPhase::closing;  //@@authority-target ids=prv.T0137

    // Admission closes as part of the same commit, before any external
    // callback. It may already be closed — the failing-write path commits it
    // earlier so its own handler cannot send onto a dead transport.
    admission_closed_ = true;
    connected_ = false;

    // Return any in-flight frame's bytes to the inbound budget (sec #0055);
    // this is the catch-all for every teardown path (eviction + errors).
    release_inbound_charge();

    // Take the owed terminals but do NOT invoke them yet. Everything up to the
    // delivery loop is owner-local settlement; a handler that throws (the type
    // is not noexcept and no contract forbids it) must not be able to leave the
    // socket open or the remaining terminals undelivered.
    //
    // Both moves are non-allocating, which is deliberate: an earlier version
    // grew a std::vector with push_back AFTER latching close_ran_, so an
    // allocation failure unwound with the idempotency flag already committed,
    // rows already popped, the socket still open, and every later close
    // returning immediately. Moving the deque wholesale removes the fallible
    // preparation instead of merely ordering it.
    auto pending = std::move(write_queue_);
    write_queue_.clear();
    std::function<void(boost::system::error_code)> active_terminal;
    if (active_write_)
    {
        active_terminal = std::move(active_write_->handler);
        active_write_->handler = nullptr;  // consumption IS the move
        // NB: the active PACKET stays put. Asio owns those bytes until
        // completion.
    }

    if (socket_ && socket_->lowest_layer().is_open())
    {
        boost::system::error_code ec;
        socket_->lowest_layer().cancel(ec);
        socket_->lowest_layer().close(ec);

        if (ec && ec != boost::asio::error::not_connected)
        {
            PLOGD(log_, "Error closing socket: ", ec.message());
        }
    }
    //@@authority transition=prv.T0142 status=provisional scope=statement
    // bind=if_statement@0434c0733359bdae
    // owner=unresolved owner_hypothesis=peer_connection
    // commit=connection_strand touches=prv.S0047 mutation_class=seal_commit
    // label=cancel_and_release_owned_resolver_capability
    if (resolver_)
    {
        resolver_->cancel();
        resolver_.reset();
    }  //@@authority-target ids=prv.T0142

    // Break shared_ptr cycles: packet_handler_ and disconnect_handler_
    // capture shared_ptr<PeerClient> which holds shared_ptr<peer_connection>.
    // Taken by move so the cycle is broken before the callback runs, and so a
    // re-entrant teardown cannot fire the same disconnect twice.
    packet_handler_ = nullptr;
    auto disconnect = std::move(disconnect_handler_);
    disconnect_handler_ = nullptr;

    // Settlement is complete and consistent. External callbacks only from here,
    // each individually contained, and carrying the COMMITTED disposition
    // rather than this frame's arguments.
    auto const disposition = *close_disposition_;
    for (auto& entry : pending)
    {
        deliver_terminal(entry.handler, disposition.cause);
    }
    deliver_terminal(active_terminal, disposition.cause);
    if (disposition.fire_disconnect)
    {
        deliver_terminal(disconnect, disposition.cause);
    }

    // External terminals are part of the close frame. The physical receipt is
    // not legal until they have returned and every Asio-owned operation has
    // released its exact census row.
    try_complete_physical_drain();
}

//@@authority transition=peer_client.select_close_disposition owner=peer_client
// commit=strand
// bind=catl::peer_client::select_close_disposition@089f284025217888
// Mints the terminal disposition into an owned row before external code runs.
void
peer_connection::select_close_disposition(
    boost::system::error_code cause,
    bool fire_disconnect) noexcept
{
    if (!close_disposition_)
    {
        close_disposition_ = CloseDisposition{cause, fire_disconnect};
    }
}

void
peer_connection::close_impl()
{
    settle_and_close(asio::error::operation_aborted, /*fire_disconnect=*/false);
}

//@@settlement
void
peer_connection::fail_and_close(boost::system::error_code ec)
{
    if (close_ran_)
        return;

    PLOGD(log_, "fail_and_close: ", ec.message());

    // One transition. The old shape drained handlers (external callbacks) while
    // admission was still open and only flipped the flag afterwards inside
    // close_impl, so a drained handler could submit new rows mid-teardown.
    settle_and_close(ec, /*fire_disconnect=*/true);
}

}  // namespace catl::peer_client
