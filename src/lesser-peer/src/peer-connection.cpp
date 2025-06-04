#include <catl/core/logger.h>
#include <catl/peer/crypto-utils.h>
#include <catl/peer/peer-connection.h>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
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

    // Resolve the host
    tcp::resolver resolver(io_context_);
    auto endpoints =
        resolver.resolve(config_.host, std::to_string(config_.port));

    // Connect to the server
    asio::async_connect(
        socket_->lowest_layer(),
        endpoints,
        [self = shared_from_this(), handler](
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
    const char* home = std::getenv("HOME");
    std::string key_file = std::string(home ? home : "") + "/.peermon";
    auto keys = crypto.load_or_generate_node_keys(key_file);

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
    req->set(http::field::user_agent, "rippled-2.2.2");
    req->set(http::field::upgrade, "XRPL/2.1");
    req->set(http::field::connection, "Upgrade");
    req->set("Connect-As", "Peer");
    req->set("Crawl", "private");
    req->set("Session-Signature", session_signature_);
    req->set("Public-Key", node_public_key_b58_);

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
    res->set(http::field::server, "rippled-2.2.2");
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
        handler(boost::asio::error::invalid_argument);
        return;
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
        LOGE("Error reading header: ", ec.message());
        return;
    }

    // Parse header
    std::uint32_t payload_size = (header_buffer_[0] << 24) |
        (header_buffer_[1] << 16) | (header_buffer_[2] << 8) |
        header_buffer_[3];

    current_header_.compressed = (payload_size >> 28) != 0;
    if (current_header_.compressed)
    {
        payload_size &= 0x0FFFFFFF;
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
        LOGE("Error reading payload: ", ec.message(), " (", ec.value(), ")");
        // Check if it's EOF or connection closed
        if (ec == asio::error::eof || ec == asio::error::connection_reset)
        {
            LOGI("Connection closed by peer");
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

    // Payload size (big-endian)
    std::uint32_t payload_size = static_cast<std::uint32_t>(data.size());
    packet.push_back((payload_size >> 24) & 0xFF);
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