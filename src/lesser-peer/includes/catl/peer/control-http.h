#pragma once

#include "catl/peer/peer-manager.h"

#include <boost/asio.hpp>

#include <atomic>
#include <memory>
#include <string>

namespace catl::peer {

/**
 * Lightweight HTTP control plane for PeerManager
 *
 * Endpoints (JSON):
 *   GET  /peers                     -> { peers: [id...] }
 *   POST /peers {host,port,...}     -> { id: "peer-1" }
 *   DELETE /peers/{id}              -> { ok: true }
 *
 * Host/port fields are required; optional fields match peer_config keys like
 * cert_path/key_path/node_private_key.
 *
 * Must be managed via shared_ptr for safe async lifetime management.
 */
class ControlHttpServer : public std::enable_shared_from_this<ControlHttpServer>
{
public:
    // Factory method - enforces shared_ptr ownership for safe async lifetime
    static std::shared_ptr<ControlHttpServer>
    create(
        boost::asio::io_context& io_context,
        PeerManager& manager,
        std::uint16_t port);

    void
    start();

    void
    stop();

private:
    ControlHttpServer(
        boost::asio::io_context& io_context,
        PeerManager& manager,
        std::uint16_t port);

    void
    do_accept();
    void
    handle_session(boost::asio::ip::tcp::socket socket);

private:
    boost::asio::io_context& io_context_;
    PeerManager& manager_;
    std::uint16_t port_;
    std::unique_ptr<boost::asio::ip::tcp::acceptor> acceptor_;
    std::atomic<bool> running_{false};
};

}  // namespace catl::peer
