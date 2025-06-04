#pragma once

#include "peer-connection.h"
#include "types.h"
#include <functional>
#include <memory>

namespace catl::peer {

class packet_processor
{
public:
    packet_processor(
        connection_config const& config,
        packet_filter const& filter);

    // Process incoming packet
    void
    process_packet(
        std::shared_ptr<peer_connection> connection,
        packet_header const& header,
        std::vector<std::uint8_t> const& payload);

    // Get statistics
    packet_counters const&
    get_stats() const
    {
        return counters_;
    }

    // Set custom packet handler for specific types
    using custom_handler =
        std::function<void(packet_type, std::vector<std::uint8_t> const&)>;
    void
    set_custom_handler(packet_type type, custom_handler handler);

private:
    void
    handle_ping(
        std::shared_ptr<peer_connection> connection,
        std::vector<std::uint8_t> const& payload);
    void
    handle_manifests(std::vector<std::uint8_t> const& payload);
    void
    handle_transaction(std::vector<std::uint8_t> const& payload);
    void
    handle_get_ledger(std::vector<std::uint8_t> const& payload);
    void
    handle_propose_ledger(std::vector<std::uint8_t> const& payload);
    void
    handle_status_change(std::vector<std::uint8_t> const& payload);
    void
    handle_validation(std::vector<std::uint8_t> const& payload);
    void
    update_stats(packet_type type, std::size_t bytes);
    bool
    should_display_packet(packet_type type) const;
    void
    display_stats() const;

    void
    print_hex(std::uint8_t const* data, std::size_t len) const;
    void
    print_sto(std::string const& st) const;
    std::string
    format_bytes(double bytes) const;

private:
    connection_config config_;
    packet_filter filter_;
    packet_counters counters_;
    std::map<packet_type, custom_handler> custom_handlers_;

    std::chrono::steady_clock::time_point start_time_;
    std::chrono::steady_clock::time_point last_display_time_;
};

}  // namespace catl::peer