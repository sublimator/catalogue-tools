#include <boost/json.hpp>
#include <catl/peer/monitor/packet-logger.h>
#include <catl/peer/packet-names.h>
#include <catl/peer/wire-format.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/slice-cursor.h>
#include <iomanip>
#include <sstream>

#include "ripple.pb.h"

namespace catl::peer::monitor {

// Log Partitions
static LogPartition packet_log("packet");
static LogPartition dump_log("dump");

#define PACKET_TYPE BOLD_CYAN

PacketLogger::PacketLogger(monitor_config const& config) : config_(config)
{
}

void
PacketLogger::on_event(PeerEvent const& event)
{
    switch (event.type)
    {
        case PeerEventType::Packet:
            log_packet(std::get<PeerPacketEvent>(event.data), event.peer_id);
            break;
        case PeerEventType::State:
            log_state(std::get<PeerStateEvent>(event.data), event.peer_id);
            break;
        default:
            break;
    }
}

void
PacketLogger::log_state(PeerStateEvent const& state, std::string const& peer_id)
{
    if (state.state == PeerStateEvent::State::Connected)
    {
        PLOGI(packet_log, "✅ Peer Connected: ", peer_id);
    }
    else if (state.state == PeerStateEvent::State::Disconnected)
    {
        PLOGW(
            packet_log, "❌ Peer Disconnected: ", peer_id, " - ", state.message);
    }
    else if (state.state == PeerStateEvent::State::Error)
    {
        PLOGE(
            packet_log,
            "⚠️ Peer Error: ",
            peer_id,
            " - ",
            state.message,
            " (code=",
            state.error.value(),
            ")");
    }
}

void
PacketLogger::log_packet(PeerPacketEvent const& pkt, std::string const& peer_id)
{
    auto type = static_cast<packet_type>(pkt.header.type);

    // Create short peer tag (first 8 chars of peer_id)
    std::string peer_tag = peer_id.size() > 8 ? peer_id.substr(0, 8) : peer_id;

    // Basic summary log (if enabled by partition)
    // Note: We rely on LogPartition levels to filter this

    switch (type)
    {
        case packet_type::manifests:
            log_manifests(pkt.payload, peer_tag);
            break;
        case packet_type::transaction:
            log_transaction(pkt.payload, peer_tag);
            break;
        case packet_type::ledger_data:
            log_ledger_data(pkt.payload, peer_tag);
            break;
        case packet_type::validation:
            log_validation(pkt.payload, peer_tag);
            break;
        case packet_type::propose_ledger:
            log_proposal(pkt.payload, peer_tag);
            break;
        case packet_type::status_change:
            log_status(pkt.payload, peer_tag);
            break;
        case packet_type::ping:
            log_ping(pkt.payload, peer_tag);
            break;
        case packet_type::endpoints:
            log_endpoints(pkt.payload, peer_tag);
            break;

        // Default handler for others
        default: {
            auto name = get_packet_name(pkt.header.type);
            PLOGI(
                packet_log,
                "[",
                peer_tag,
                "] ",
                COLORED(PACKET_TYPE, name),
                " [",
                pkt.header.type,
                "] size=",
                pkt.header.payload_size);
            print_hex(
                pkt.payload.data(), std::min<size_t>(pkt.payload.size(), 128));
            break;
        }
    }
}

void
PacketLogger::log_ping(
    std::vector<std::uint8_t> const& payload,
    std::string const& peer_tag)
{
    protocol::TMPing ping;
    if (!ping.ParseFromArray(payload.data(), payload.size()))
        return;

    if (ping.type() == protocol::TMPing_pingType_ptPING)
    {
        PLOGI(
            packet_log,
            "[",
            peer_tag,
            "] ",
            COLORED(PACKET_TYPE, "mtPING"),
            " - replying PONG");
    }
    else
    {
        PLOGI(
            packet_log,
            "[",
            peer_tag,
            "] ",
            COLORED(PACKET_TYPE, "mtPING"),
            " - received PONG");
    }
}

void
PacketLogger::log_manifests(
    std::vector<std::uint8_t> const& payload,
    std::string const& peer_tag)
{
    protocol::TMManifests manifests;
    if (!manifests.ParseFromArray(payload.data(), payload.size()))
        return;

    PLOGI(
        packet_log,
        "[",
        peer_tag,
        "] ",
        COLORED(PACKET_TYPE, "mtMANIFESTS"),
        " count=",
        manifests.list_size());

    for (int i = 0; i < manifests.list_size(); ++i)
    {
        auto const& sto = manifests.list(i).stobject();
        PLOGD(
            dump_log,
            "[",
            peer_tag,
            "] Manifest ",
            i,
            " (",
            sto.size(),
            " bytes): ",
            get_sto_json(std::vector<uint8_t>(sto.begin(), sto.end())));
        print_hex(reinterpret_cast<const uint8_t*>(sto.data()), sto.size());
    }
}

void
PacketLogger::log_transaction(
    std::vector<std::uint8_t> const& payload,
    std::string const& peer_tag)
{
    protocol::TMTransaction txn;
    if (!txn.ParseFromArray(payload.data(), payload.size()))
        return;

    auto const& raw = txn.rawtransaction();
    PLOGI(
        packet_log,
        "[",
        peer_tag,
        "] ",
        COLORED(PACKET_TYPE, "mtTRANSACTION"),
        " size=",
        raw.size());
    PLOGD(
        dump_log,
        "[",
        peer_tag,
        "] ",
        get_sto_json(std::vector<uint8_t>(raw.begin(), raw.end())));
    print_hex(reinterpret_cast<const uint8_t*>(raw.data()), raw.size());
}

void
PacketLogger::log_validation(
    std::vector<std::uint8_t> const& payload,
    std::string const& peer_tag)
{
    protocol::TMValidation val;
    if (!val.ParseFromArray(payload.data(), payload.size()))
        return;

    auto const& data = val.validation();
    PLOGI(
        packet_log,
        "[",
        peer_tag,
        "] ",
        COLORED(PACKET_TYPE, "mtVALIDATION"),
        " size=",
        data.size());
    PLOGD(
        dump_log,
        "[",
        peer_tag,
        "] ",
        get_sto_json(std::vector<uint8_t>(data.begin(), data.end())));
    print_hex(reinterpret_cast<const uint8_t*>(data.data()), data.size());
}

void
PacketLogger::log_proposal(
    std::vector<std::uint8_t> const& payload,
    std::string const& peer_tag)
{
    protocol::TMProposeSet ps;
    if (!ps.ParseFromArray(payload.data(), payload.size()))
        return;

    PLOGI(
        packet_log,
        "[",
        peer_tag,
        "] ",
        COLORED(PACKET_TYPE, "mtPROPOSE_LEDGER"),
        " seq=",
        ps.proposeseq(),
        " txs_added=",
        ps.addedtransactions_size(),
        " txs_removed=",
        ps.removedtransactions_size());
}

void
PacketLogger::log_status(
    std::vector<std::uint8_t> const& payload,
    std::string const& peer_tag)
{
    protocol::TMStatusChange status;
    if (!status.ParseFromArray(payload.data(), payload.size()))
        return;

    // Build the suffix separately (stringstream can't handle COLORED macro)
    std::string suffix;
    if (status.has_newstatus())
        suffix += " status=" + std::to_string(status.newstatus());
    if (status.has_newevent())
        suffix += " event=" + std::to_string(status.newevent());
    if (status.has_ledgerseq())
        suffix += " seq=" + std::to_string(status.ledgerseq());

    PLOGI(
        packet_log,
        "[",
        peer_tag,
        "] ",
        COLORED(PACKET_TYPE, "mtSTATUS_CHANGE"),
        suffix);
}

void
PacketLogger::log_ledger_data(
    std::vector<std::uint8_t> const& payload,
    std::string const& peer_tag)
{
    protocol::TMLedgerData ld;
    if (!ld.ParseFromArray(payload.data(), payload.size()))
        return;

    PLOGI(
        packet_log,
        "[",
        peer_tag,
        "] ",
        COLORED(PACKET_TYPE, "mtLEDGER_DATA"),
        " type=",
        ld.type(),
        " seq=",
        ld.ledgerseq(),
        " nodes=",
        ld.nodes_size());
}

void
PacketLogger::log_endpoints(
    std::vector<std::uint8_t> const& payload,
    std::string const& peer_tag)
{
    protocol::TMEndpoints eps;
    if (!eps.ParseFromArray(payload.data(), payload.size()))
        return;

    PLOGI(
        packet_log,
        "[",
        peer_tag,
        "] ",
        COLORED(PACKET_TYPE, "mtENDPOINTS"),
        " count=",
        eps.endpoints_v2_size());

    for (int i = 0; i < eps.endpoints_v2_size(); ++i)
    {
        PLOGD(dump_log, "[", peer_tag, "]   ", eps.endpoints_v2(i).endpoint());
    }
}

void
PacketLogger::print_hex(std::uint8_t const* data, std::size_t len) const
{
    (void)data;
    (void)len;
    // Hex dump disabled for now to keep logs lightweight.
}

std::string
PacketLogger::get_sto_json(std::vector<std::uint8_t> const& data) const
{
    try
    {
        Slice slice(data.data(), data.size());
        xdata::SliceCursor cursor{slice, 0};

        // Using default embedded protocol
        auto protocol = xdata::Protocol::load_embedded_xahau_protocol(
            xdata::ProtocolOptions{.network_id = config_.peer.network_id});

        xdata::JsonVisitor visitor(protocol);
        xdata::ParserContext ctx{cursor};
        xdata::parse_with_visitor(ctx, protocol, visitor);

        return boost::json::serialize(visitor.get_result());
    }
    catch (...)
    {
        return "<parse error>";
    }
}

}  // namespace catl::peer::monitor
