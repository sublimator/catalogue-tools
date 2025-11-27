#include "ripple.pb.h"
#include <catl/core/logger.h>
#include <catl/peer/crypto-utils.h>
#include <catl/peer/peer-connection.h>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <chrono>
#include <openssl/ssl.h>
#include <utility>

namespace catl::peer {

namespace beast = boost::beast;
namespace http = beast::http;

peer_connection::peer_connection(
    asio::io_context& io_context,
    asio::ssl::context& ssl_context,
    peer_config config)
    : io_context_(io_context)
    , socket_(std::make_unique<ssl_socket>(io_context, ssl_context))
    , config_(std::move(config))
{
}

peer_connection::~peer_connection()
{
    close();
}

void
peer_connection::async_connect(const connection_handler& handler)
{
    if (config_.listen_mode)
    {
        LOGE("Cannot call async_connect in listen mode");
        handler(boost::asio::error::invalid_argument);
        return;
    }

    // Resolve the host asynchronously
    auto resolver = std::make_shared<tcp::resolver>(io_context_);
    resolver->async_resolve(
        config_.host,
        std::to_string(config_.port),
        [self = shared_from_this(), handler, resolver](
            boost::system::error_code ec,
            tcp::resolver::results_type endpoints) {
            if (ec)
            {
                handler(ec);
                return;
            }

            // Connect to the server
            asio::async_connect(
                self->socket_->lowest_layer(),
                endpoints,
                [self, handler](
                    boost::system::error_code ec, tcp::endpoint endpoint) {
                    if (!ec)
                    {
                        self->remote_endpoint_ = endpoint;
                        self->handle_connect(ec, handler);
                    }
                    else
                    {
                        handler(ec);
                    }
                });
        });
}

void
peer_connection::async_accept(
    tcp::acceptor& acceptor,
    const connection_handler& handler)
{
    if (!config_.listen_mode)
    {
        LOGE("Cannot call async_accept when not in listen mode");
        handler(boost::asio::error::invalid_argument);
        return;
    }

    acceptor.async_accept(
        socket_->lowest_layer(),
        [self = shared_from_this(), handler](boost::system::error_code ec) {
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
}

void
peer_connection::handle_connect(
    const boost::system::error_code& ec,
    const connection_handler& handler)
{
    if (ec)
    {
        handler(ec);
        return;
    }

    // Perform SSL handshake
    socket_->async_handshake(
        asio::ssl::stream_base::client,
        [self = shared_from_this(), handler](boost::system::error_code ec) {
            self->handle_handshake(ec, handler);
        });
}

void
peer_connection::handle_handshake(
    const boost::system::error_code& ec,
    const connection_handler& handler)
{
    if (ec)
    {
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
        handler(boost::asio::error::invalid_argument);
        return;
    }
    finished.resize(finished_len);

    std::vector<std::uint8_t> peer_finished(1024);
    std::size_t peer_finished_len =
        SSL_get_peer_finished(ssl, peer_finished.data(), peer_finished.size());
    if (peer_finished_len < 12)
    {
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

    // Perform HTTP upgrade
    perform_http_upgrade(handler);
}

void
peer_connection::generate_node_keys()
{
    crypto_utils crypto;

    crypto_utils::node_keys keys;

    // Check if a private key was provided in the config
    if (config_.node_private_key.has_value())
    {
        // Use the provided private key
        keys = crypto.node_keys_from_private(config_.node_private_key.value());
    }
    else
    {
        // Fall back to loading from file or generating new keys
        const char* home = std::getenv("HOME");
        std::string key_file = std::string(home ? home : "") + "/.peermon";
        keys = crypto.load_or_generate_node_keys(key_file);
    }

    secret_key_ = keys.secret_key;
    public_key_compressed_ = keys.public_key_compressed;
    node_public_key_b58_ = keys.public_key_b58;
}

void
peer_connection::perform_http_upgrade(const connection_handler& handler)
{
    if (config_.listen_mode)
    {
        // In listen mode, we wait for the client's request first
        auto buffer = std::make_shared<beast::flat_buffer>();
        http::async_read(
            *socket_,
            *buffer,
            http_request_,
            [self = shared_from_this(), buffer, handler](
                boost::system::error_code ec, std::size_t) {
                if (!ec)
                {
                    self->handle_http_request(handler);
                }
                else
                {
                    handler(ec);
                }
            });
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
    auto req = std::make_shared<http::request<http::string_body>>(
        http::verb::get, "/", 11);
    req->set(http::field::user_agent, "xahaud-2025.11.4-HEAD+2427");
    req->set(http::field::upgrade, "XRPL/2.2");
    req->set(http::field::connection, "Upgrade");
    req->set("Connect-As", "Peer");
    req->set("Crawl", "private");

    // Network ID (configurable)
    req->set("Network-ID", std::to_string(config_.network_id));

    // Add network time (seconds since Ripple epoch - Jan 1, 2000)
    // Ripple epoch is Unix timestamp 946684800
    static constexpr uint32_t RIPPLE_EPOCH = 946684800;
    auto unix_now = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
    uint32_t ripple_time = unix_now - RIPPLE_EPOCH;
    req->set("Network-Time", std::to_string(ripple_time));

    req->set("Session-Signature", session_signature_);
    req->set("Public-Key", node_public_key_b58_);

    // Request ledger replay feature (lowercase as per rippled source)
    req->set("X-Protocol-Ctl", "ledgerreplay=1;");

    http::async_write(
        *socket_,
        *req,
        [self = shared_from_this(), req, handler](
            boost::system::error_code ec, std::size_t) {
            if (!ec)
            {
                // Read the response
                auto buffer = std::make_shared<beast::flat_buffer>();
                http::async_read(
                    *self->socket_,
                    *buffer,
                    self->http_response_,
                    [self, buffer, handler](
                        boost::system::error_code ec, std::size_t) {
                        if (!ec)
                        {
                            self->handle_http_response(handler);
                        }
                        else
                        {
                            handler(ec);
                        }
                    });
            }
            else
            {
                handler(ec);
            }
        });
}

void
peer_connection::handle_http_request(const connection_handler& handler)
{
    // Send upgrade response
    auto res = std::make_shared<http::response<http::string_body>>(
        http::status::switching_protocols, 11);
    res->set(http::field::connection, "Upgrade");
    res->set(http::field::upgrade, http_request_["Upgrade"]);
    res->set("Connect-As", "Peer");
    res->set(http::field::server, "xahaud-2025.11.4-HEAD+2427");
    res->set("Crawl", "private");
    res->set("Public-Key", node_public_key_b58_);
    res->set("Session-Signature", session_signature_);

    http::async_write(
        *socket_,
        *res,
        [self = shared_from_this(), res, handler](
            boost::system::error_code ec, std::size_t) {
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
                handler(ec);
            }
        });
}

void
peer_connection::handle_http_response(const connection_handler& handler)
{
    if (http_response_.result() != http::status::switching_protocols)
    {
        LOGE(
            "HTTP upgrade failed with status: ",
            static_cast<int>(http_response_.result()));
        handler(boost::asio::error::invalid_argument);
        return;
    }

    // Log important response headers
    LOGI("HTTP upgrade successful - examining response headers:");

    // Check protocol version
    if (auto it = http_response_.find("Upgrade"); it != http_response_.end())
    {
        LOGI("  Protocol version: ", it->value());
        protocol_version_ = std::string(it->value());
    }

    // Check server version
    if (auto it = http_response_.find("Server"); it != http_response_.end())
    {
        LOGI("  Server: ", it->value());
        server_version_ = std::string(it->value());
    }

    // Check feature support (X-Protocol-Ctl)
    if (auto it = http_response_.find("X-Protocol-Ctl");
        it != http_response_.end())
    {
        LOGI("  Protocol features: ", it->value());
        // TODO: Parse features like LEDGER_REPLAY=1
    }

    // Check network ID
    if (auto it = http_response_.find("Network-ID"); it != http_response_.end())
    {
        LOGI("  Network ID: ", it->value());
        network_id_ = std::string(it->value());
    }

    // Check public key
    if (auto it = http_response_.find("Public-Key"); it != http_response_.end())
    {
        LOGI("  Node public key: ", it->value());
    }

    // Check closed ledger info
    if (auto it = http_response_.find("Closed-Ledger");
        it != http_response_.end())
    {
        LOGI("  Closed ledger: ", it->value());
    }

    if (auto it = http_response_.find("Previous-Ledger");
        it != http_response_.end())
    {
        LOGI("  Previous ledger: ", it->value());
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
            LOGD("  ", field.name_string(), ": ", field.value());
        }
    }

    http_upgraded_ = true;
    connected_ = true;

    // Now that upgrade is complete, we can start reading packets
    handler({});
}

void
peer_connection::start_read(packet_handler handler)
{
    packet_handler_ = std::move(handler);
    async_read_header();
}

void
peer_connection::async_read_header()
{
    // Read first 6 bytes of header (4 bytes size + 2 bytes type)
    asio::async_read(
        *socket_,
        asio::buffer(header_buffer_.data(), 6),
        [self = shared_from_this()](
            boost::system::error_code ec, std::size_t bytes_transferred) {
            if (!ec)
            {
                LOGD("Read header: ", bytes_transferred, " bytes");
            }
            self->handle_read_header(ec, bytes_transferred);
        });
}

void
peer_connection::handle_read_header(
    boost::system::error_code ec,
    std::size_t bytes_transferred)
{
    if (ec)
    {
        // Log exact error code for debugging disconnects
        LOGE("Error reading header: ", ec.message(), " (val=", ec.value(), ")");

        if (ec == asio::error::eof || ec == asio::error::connection_reset)
        {
            LOGI("🔌 Connection closed by peer (EOF/Reset) during header read");
            close();
        }
        return;
    }

    // Parse header
    // First check if compressed by looking at high nibble of first byte
    current_header_.compressed = (header_buffer_[0] & 0xF0) != 0;

    // Extract payload size (masking off compression bits)
    std::uint32_t payload_size = ((header_buffer_[0] & 0x0F) << 24) |
        (header_buffer_[1] << 16) | (header_buffer_[2] << 8) |
        header_buffer_[3];

    if (current_header_.compressed)
    {
        LOGD(
            "Compressed packet detected, need to read additional header bytes");
    }

    current_header_.payload_size = payload_size;
    current_header_.type = (header_buffer_[4] << 8) | header_buffer_[5];

    LOGD(
        "Header parsed: type=",
        current_header_.type,
        " payload_size=",
        payload_size,
        " compressed=",
        current_header_.compressed);

    if (current_header_.compressed && bytes_transferred < 10)
    {
        // Need to read the additional 4 bytes for uncompressed size
        asio::async_read(
            *socket_,
            asio::buffer(header_buffer_.data() + 6, 4),
            [self = shared_from_this(), payload_size](
                boost::system::error_code ec, std::size_t) {
                if (ec)
                {
                    LOGE(
                        "Error reading compressed header extension: ",
                        ec.message());
                    return;
                }
                self->current_header_.uncompressed_size =
                    (self->header_buffer_[6] << 24) |
                    (self->header_buffer_[7] << 16) |
                    (self->header_buffer_[8] << 8) | self->header_buffer_[9];
                self->current_header_.payload_size = payload_size;

                // Now read the payload
                self->payload_buffer_.resize(payload_size);
                asio::async_read(
                    *self->socket_,
                    asio::buffer(self->payload_buffer_),
                    [self](
                        boost::system::error_code ec,
                        std::size_t bytes_transferred) {
                        self->handle_read_payload(ec, bytes_transferred);
                    });
            });
        return;
    }
    else if (current_header_.compressed && bytes_transferred >= 10)
    {
        current_header_.uncompressed_size = (header_buffer_[6] << 24) |
            (header_buffer_[7] << 16) | (header_buffer_[8] << 8) |
            header_buffer_[9];
    }
    else
    {
        current_header_.uncompressed_size = payload_size;
    }

    // Read payload
    payload_buffer_.resize(payload_size);
    asio::async_read(
        *socket_,
        asio::buffer(payload_buffer_),
        [self = shared_from_this()](
            boost::system::error_code ec, std::size_t bytes_transferred) {
            self->handle_read_payload(ec, bytes_transferred);
        });
}

void
peer_connection::handle_read_payload(
    boost::system::error_code ec,
    std::size_t bytes_read)
{
    if (ec)
    {
        LOGE(
            "Error reading payload: ", ec.message(), " (val=", ec.value(), ")");
        // Check if it's EOF or connection closed
        if (ec == asio::error::eof || ec == asio::error::connection_reset)
        {
            LOGI("🔌 Connection closed by peer (EOF/Reset) during payload read");
            close();
        }
        return;
    }

    LOGD(
        "Read payload: ",
        bytes_read,
        " bytes for packet type ",
        current_header_.type);

    // Call packet handler
    if (packet_handler_)
    {
        try
        {
            packet_handler_(current_header_, payload_buffer_);
        }
        catch (std::exception const& e)
        {
            LOGE("Exception in packet handler: ", e.what());
        }
    }

    // Continue reading
    async_read_header();
}

void
peer_connection::async_send_packet(
    packet_type type,
    std::vector<std::uint8_t> const& data,
    std::function<void(boost::system::error_code)> handler)
{
    // Create header
    std::vector<std::uint8_t> packet;
    packet.reserve(6 + data.size());

    // Payload size (big-endian) - for uncompressed, first 4 bits are 0
    std::uint32_t payload_size = static_cast<std::uint32_t>(data.size());
    packet.push_back(
        (payload_size >> 24) & 0x0F);  // Top 4 bits are 0 for uncompressed
    packet.push_back((payload_size >> 16) & 0xFF);
    packet.push_back((payload_size >> 8) & 0xFF);
    packet.push_back(payload_size & 0xFF);

    // Packet type (big-endian)
    std::uint16_t type_val = static_cast<std::uint16_t>(type);
    packet.push_back((type_val >> 8) & 0xFF);
    packet.push_back(type_val & 0xFF);

    // Append payload
    packet.insert(packet.end(), data.begin(), data.end());

    // Send packet
    asio::async_write(
        *socket_,
        asio::buffer(packet),
        [handler](boost::system::error_code ec, std::size_t) {
            if (handler)
            {
                handler(ec);
            }
        });
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
                LOGE("Failed to send initial ping: ", ec.message());
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
        LOGE("Cannot send transaction query: not connected");
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
        LOGD(
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
        LOGD("  Including ledger hash: ", ledger_hash.substr(0, 16), "...");
    }

    // Add the transaction hash to query
    protocol::TMIndexedObject* obj = query.add_objects();
    obj->set_hash(hash_bytes.data(), hash_bytes.size());

    // Serialize the message
    std::string serialized;
    if (!query.SerializeToString(&serialized))
    {
        LOGE("Failed to serialize transaction query");
        return;
    }

    // Send the query using async_send_packet
    LOGD("  Serialized size: ", serialized.size(), " bytes");

    async_send_packet(
        packet_type::get_objects,
        std::vector<uint8_t>(serialized.begin(), serialized.end()),
        [tx_hash, seq](boost::system::error_code ec) {
            if (ec)
            {
                LOGE(
                    "Failed to send query for tx ",
                    tx_hash.substr(0, 16),
                    "... (seq=",
                    seq,
                    "): ",
                    ec.message());
            }
            else
            {
                LOGD(
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
        LOGD("Also trying as TRANSACTION_NODE (seq=", seq2, ")");
        async_send_packet(
            packet_type::get_objects,
            std::vector<uint8_t>(serialized2.begin(), serialized2.end()),
            [tx_hash, seq2](boost::system::error_code ec) {
                if (ec)
                {
                    LOGE(
                        "Failed to send node query for tx ",
                        tx_hash.substr(0, 16),
                        "... (seq=",
                        seq2,
                        "): ",
                        ec.message());
                }
                else
                {
                    LOGD(
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
        LOGE("Cannot request transaction set: not connected");
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

    LOGI("🔍 Requesting transaction set: ", tx_set_hash.substr(0, 16), "...");
    LOGI("  Full hash: ", tx_set_hash);
    LOGI("  Using itype=", protocol::liTS_CANDIDATE, " (TS_CANDIDATE)");
    LOGI("  Query depth: 3");
    LOGI("  Starting from: ROOT node");

    // Serialize the message
    std::string serialized;
    if (!request.SerializeToString(&serialized))
    {
        LOGE("Failed to serialize TMGetLedger request");
        return;
    }

    LOGI("🔍 Requesting transaction set: ", tx_set_hash.substr(0, 16), "...");
    LOGI("  itype=", protocol::liTS_CANDIDATE, " querydepth=3 root_node=yes");

    // Send the request
    async_send_packet(
        packet_type::get_ledger,
        std::vector<uint8_t>(serialized.begin(), serialized.end()),
        [tx_set_hash](boost::system::error_code ec) {
            if (ec)
            {
                LOGE(
                    "Failed to request transaction set ",
                    tx_set_hash.substr(0, 16),
                    "...: ",
                    ec.message());
            }
            else
            {
                LOGD(
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
        LOGE("Cannot request transaction set nodes: not connected");
        return;
    }

    if (node_ids_wire.empty())
    {
        LOGW("request_transaction_set_nodes called with empty node list");
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

    LOGD(
        "Requesting ",
        node_ids_wire.size(),
        " nodes from tx set ",
        tx_set_hash.substr(0, 16),
        "...");

    // Serialize and send
    std::string serialized;
    if (!request.SerializeToString(&serialized))
    {
        LOGE("Failed to serialize TMGetLedger nodes request");
        return;
    }

    async_send_packet(
        packet_type::get_ledger,
        std::vector<uint8_t>(serialized.begin(), serialized.end()),
        [](boost::system::error_code ec) {
            if (ec)
            {
                LOGE("Failed to request transaction set nodes: ", ec.message());
            }
        });
}

void
peer_connection::close()
{
    connected_ = false;

    if (socket_ && socket_->lowest_layer().is_open())
    {
        boost::system::error_code ec;

        // Cancel any pending operations
        socket_->lowest_layer().cancel(ec);

        // Close the socket
        socket_->lowest_layer().close(ec);

        if (ec && ec != boost::asio::error::not_connected)
        {
            LOGD("Error closing socket: ", ec.message());
        }
    }
}

}  // namespace catl::peer
