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
 */
class ControlHttpServer
{
public:
    ControlHttpServer(
        boost::asio::io_context& io_context,
        PeerManager& manager,
        std::uint16_t port);

    void
    start();

    void
    stop();

private:
    void
    do_accept();

private:
    boost::asio::io_context& io_context_;
    PeerManager& manager_;
    std::uint16_t port_;
    std::unique_ptr<boost::asio::ip::tcp::acceptor> acceptor_;
    std::atomic<bool> running_{false};
};

}  // namespace catl::peer
