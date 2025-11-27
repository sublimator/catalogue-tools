#pragma once

#include <catl/core/logger.h>
#include <catl/peer/monitor/types.h>
#include <catl/peer/peer-events.h>

namespace catl::peer::monitor {

class PacketLogger
{
public:
    PacketLogger(monitor_config const& config);

    void
    on_event(PeerEvent const& event);

private:
    void
    log_packet(PeerPacketEvent const& pkt, std::string const& peer_id);
    void
    log_state(PeerStateEvent const& state, std::string const& peer_id);

    // Packet specific loggers (peer_tag is shortened peer_id for display)
    void
    log_manifests(
        std::vector<std::uint8_t> const& payload,
        std::string const& peer_tag);
    void
    log_transaction(
        std::vector<std::uint8_t> const& payload,
        std::string const& peer_tag);
    void
    log_ledger_data(
        std::vector<std::uint8_t> const& payload,
        std::string const& peer_tag);
    void
    log_validation(
        std::vector<std::uint8_t> const& payload,
        std::string const& peer_tag);
    void
    log_proposal(
        std::vector<std::uint8_t> const& payload,
        std::string const& peer_tag);
    void
    log_status(
        std::vector<std::uint8_t> const& payload,
        std::string const& peer_tag);
    void
    log_ping(
        std::vector<std::uint8_t> const& payload,
        std::string const& peer_tag);
    void
    log_endpoints(
        std::vector<std::uint8_t> const& payload,
        std::string const& peer_tag);

    // Helpers
    void
    print_hex(std::uint8_t const* data, std::size_t len) const;
    std::string
    get_sto_json(std::vector<std::uint8_t> const& data) const;

private:
    monitor_config config_;
};

}  // namespace catl::peer::monitor
