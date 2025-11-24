#pragma once

#include "types.h"
#include <catl/core/logger.h>

#include <array>
#include <atomic>
#include <boost/beast.hpp>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace catl::peer {

class peer_connection : public std::enable_shared_from_this<peer_connection>
{
public:
    using connection_handler = std::function<void(boost::system::error_code)>;
    using packet_handler = std::function<
        void(packet_header const&, std::vector<std::uint8_t> const&)>;

    peer_connection(
        asio::io_context& io_context,
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

    // Get socket for accept operations
    ssl_socket&
    socket()
    {
        return *socket_;
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

    void
    perform_http_upgrade(const connection_handler& handler);

    // Generate node keys
    void
    generate_node_keys();

private:
    asio::io_context& io_context_;
    std::unique_ptr<ssl_socket> socket_;
    peer_config config_;
    packet_handler packet_handler_;

    // Read buffers
    std::array<std::uint8_t, 10> header_buffer_;
    std::vector<std::uint8_t> payload_buffer_;
    packet_header current_header_;

    // HTTP upgrade state
    bool http_upgraded_ = false;

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

    // HTTP upgrade
    std::string session_signature_;
    boost::beast::http::request<boost::beast::http::string_body> http_request_;
    boost::beast::http::response<boost::beast::http::string_body>
        http_response_;

    // Transaction query tracking
    std::atomic<uint32_t> query_seq_{1};  // Start at 1
    mutable std::mutex query_map_mutex_;
    std::map<uint32_t, std::string> query_map_;  // seq -> tx_hash

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

}  // namespace catl::peer