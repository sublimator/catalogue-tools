#include <boost/json.hpp>
#include <catl/common/utils.h>
#include <catl/core/logger.h>
#include <catl/peer/monitor/packet-processor.h>
#include <catl/peer/packet-names.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/slice-cursor.h>

#include "ripple.pb.h"  // Generated from ripple.proto

#include <iomanip>
#include <iostream>
#include <sstream>

namespace catl::peer::monitor {

constexpr const char* PACKET_TYPE = color::BOLD_CYAN;

packet_processor::packet_processor(
    connection_config const& config,
    packet_filter const& filter)
    : config_(config)
    , filter_(filter)
    , start_time_(std::chrono::steady_clock::now())
    , last_display_time_(start_time_)
{
}

void
packet_processor::process_packet(
    std::shared_ptr<peer_connection> connection,
    packet_header const& header,
    std::vector<std::uint8_t> const& payload)
{
    auto type = static_cast<packet_type>(header.type);
    update_stats(type, header.payload_size);

    // Check if we should display this packet
    if (!should_display_packet(type) && !config_.no_dump)
    {
        return;
    }

    // Handle specific packet types
    switch (type)
    {
        case packet_type::ping:
            handle_ping(connection, payload);
            break;
        case packet_type::manifests:
            if (!config_.no_dump)
                handle_manifests(payload);
            break;
        case packet_type::transaction:
            if (!config_.no_dump)
                handle_transaction(payload);
            break;
        case packet_type::get_ledger:
            if (!config_.no_dump)
                handle_get_ledger(payload);
            break;
        case packet_type::propose_ledger:
            if (!config_.no_dump)
                handle_propose_ledger(payload);
            break;
        case packet_type::status_change:
            if (!config_.no_dump)
                handle_status_change(payload);
            break;
        case packet_type::validation:
            if (!config_.no_dump)
                handle_validation(payload);
            break;
        default:
            // Check for custom handler
            if (auto it = custom_handlers_.find(type);
                it != custom_handlers_.end())
            {
                it->second(type, payload);
            }
            else
            {
                // Unknown packet
                if (!config_.no_dump)
                {
                    auto packet_name = get_packet_name(header.type);
                    LOGI(
                        COLORED_WITH(PACKET_TYPE, packet_name),
                        " [Unknown packet ",
                        header.type,
                        "] size = ",
                        header.payload_size);
                    if (header.compressed)
                    {
                        LOGI(
                            " (compressed, uncompressed size = ",
                            header.uncompressed_size,
                            ")");
                    }
                    print_hex(
                        payload.data(),
                        std::min<std::size_t>(payload.size(), 128));
                }
                LOGD("Unknown packet type: ", header.type);
            }
    }

    // Display stats if needed
    display_stats();

    // Check manifests-only mode
    if (config_.manifests_only && type != packet_type::manifests)
    {
        LOGI("Exiting due to manifests-only mode");
        std::exit(0);
    }
}

void
packet_processor::handle_ping(
    std::shared_ptr<peer_connection> connection,
    std::vector<std::uint8_t> const& payload)
{
    protocol::TMPing ping;
    if (!ping.ParseFromArray(payload.data(), payload.size()))
    {
        LOGE("Failed to parse TMPing");
        return;
    }

    if (!config_.no_dump)
    {
        if (ping.type() == protocol::TMPing_pingType_ptPING)
        {
            LOGI(COLORED_WITH(PACKET_TYPE, "mtPING"), " - replying PONG");

            // Send pong response
            ping.set_type(protocol::TMPing_pingType_ptPONG);
            std::vector<std::uint8_t> pong_data(ping.ByteSizeLong());
            ping.SerializeToArray(pong_data.data(), pong_data.size());

            connection->async_send_packet(
                packet_type::ping, pong_data, [](boost::system::error_code ec) {
                    if (ec)
                    {
                        LOGE("Failed to send PONG: ", ec.message());
                    }
                });
        }
        else
        {
            LOGI(COLORED_WITH(PACKET_TYPE, "mtPING"), " - received PONG");
        }
    }
}

void
packet_processor::handle_manifests(std::vector<std::uint8_t> const& payload)
{
    protocol::TMManifests manifests;
    if (!manifests.ParseFromArray(payload.data(), payload.size()))
    {
        LOGI("Failed to parse manifests");
        return;
    }

    LOGI(
        COLORED_WITH(PACKET_TYPE, "mtManifests"),
        " contains ",
        manifests.list_size(),
        " manifests");
    for (int i = 0; i < manifests.list_size(); ++i)
    {
        auto const& manifest = manifests.list(i);
        auto const& sto = manifest.stobject();

        if (!config_.no_json)
        {
            LOGI(
                "Manifest ",
                i,
                " is ",
                sto.size(),
                " bytes: ",
                get_sto_json(sto));
        }
        else
        {
            LOGI("Manifest ", i, " is ", sto.size(), " bytes:");
        }

        print_hex(
            reinterpret_cast<std::uint8_t const*>(sto.data()), sto.size());
    }
}

void
packet_processor::handle_transaction(std::vector<std::uint8_t> const& payload)
{
    protocol::TMTransaction txn;
    if (!txn.ParseFromArray(payload.data(), payload.size()))
    {
        LOGI(COLORED_WITH(PACKET_TYPE, "mtTRANSACTION"), " <error parsing>");
        return;
    }

    auto const& raw_txn = txn.rawtransaction();

    if (!config_.no_json)
    {
        LOGI(
            COLORED_WITH(PACKET_TYPE, "mtTRANSACTION"),
            " ",
            get_sto_json(raw_txn));
    }
    else
    {
        LOGI(COLORED_WITH(PACKET_TYPE, "mtTRANSACTION"));
    }

    print_hex(
        reinterpret_cast<std::uint8_t const*>(raw_txn.data()), raw_txn.size());
}

void
packet_processor::handle_get_ledger(std::vector<std::uint8_t> const& payload)
{
    protocol::TMGetLedger gl;
    if (!gl.ParseFromArray(payload.data(), payload.size()))
    {
        LOGI("Failed to parse TMGetLedger");
        return;
    }

    std::stringstream hash_str;
    auto const& hash = gl.ledgerhash();
    for (std::size_t i = 0; i < hash.size() && i < 32; ++i)
    {
        hash_str << std::hex << std::setw(2) << std::setfill('0')
                 << static_cast<int>(static_cast<std::uint8_t>(hash[i]));
    }
    LOGI(
        COLORED_WITH(PACKET_TYPE, "mtGET_LEDGER"),
        " seq=",
        gl.ledgerseq(),
        " hash=",
        hash_str.str(),
        " itype=",
        gl.itype(),
        " ltype=",
        gl.ltype());
}

void
packet_processor::handle_propose_ledger(
    std::vector<std::uint8_t> const& payload)
{
    protocol::TMProposeSet ps;
    if (!ps.ParseFromArray(payload.data(), payload.size()))
    {
        LOGI("Failed to parse TMProposeSet");
        return;
    }

    std::stringstream hash_str;
    auto const& hash = ps.currenttxhash();
    for (std::size_t i = 0; i < hash.size() && i < 32; ++i)
    {
        hash_str << std::hex << std::setw(2) << std::setfill('0')
                 << static_cast<int>(static_cast<std::uint8_t>(hash[i]));
    }

    std::stringstream pub_str;
    auto const& pubkey = ps.nodepubkey();
    for (std::size_t i = 0; i < pubkey.size(); ++i)
    {
        pub_str << std::hex << std::setw(2) << std::setfill('0')
                << static_cast<int>(static_cast<std::uint8_t>(pubkey[i]));
    }

    LOGI(
        COLORED_WITH(PACKET_TYPE, "mtPROPOSE_LEDGER"),
        " seq=",
        ps.proposeseq(),
        " set=",
        hash_str.str(),
        " pub=",
        pub_str.str(),
        "time=",
        common::format_ripple_time(ps.closetime()));
}

void
packet_processor::handle_status_change(std::vector<std::uint8_t> const& payload)
{
    protocol::TMStatusChange status;
    if (!status.ParseFromArray(payload.data(), payload.size()))
    {
        LOGI(COLORED_WITH(PACKET_TYPE, "mtSTATUS_CHANGE"), " <error parsing>");
        return;
    }

    std::stringstream msg;
    msg << PACKET_TYPE << "mtSTATUS_CHANGE" << catl::color::RESET;

    if (status.has_newstatus())
    {
        msg << " stat=" << status.newstatus();
        switch (status.newstatus())
        {
            case 1:
                msg << " CONNECTING";
                break;
            case 2:
                msg << " CONNECTED";
                break;
            case 3:
                msg << " MONITORING";
                break;
            case 4:
                msg << " VALIDATING";
                break;
            case 5:
                msg << " SHUTTING";
                break;
            default:
                msg << " UNKNOWN_STATUS";
        }
    }

    if (status.has_newevent())
    {
        msg << " evnt=" << status.newevent();
        switch (status.newevent())
        {
            case 1:
                msg << " CLOSING_LEDGER";
                break;
            case 2:
                msg << " ACCEPTED_LEDGER";
                break;
            case 3:
                msg << " SWITCHED_LEDGER";
                break;
            case 4:
                msg << " LOST_SYNC";
                break;
            default:
                msg << " UNKNOWN_EVENT";
        }
    }

    if (status.has_ledgerseq())
    {
        msg << " seq=" << status.ledgerseq();
    }

    if (status.has_ledgerhash())
    {
        msg << " hash=";
        auto const& hash = status.ledgerhash();
        for (std::size_t i = 0; i < hash.size() && i < 32; ++i)
        {
            msg << std::hex << std::setw(2) << std::setfill('0')
                << static_cast<int>(static_cast<std::uint8_t>(hash[i]));
        }
    }

    LOGI(msg.str());
}

void
packet_processor::handle_validation(std::vector<std::uint8_t> const& payload)
{
    protocol::TMValidation validation;
    if (!validation.ParseFromArray(payload.data(), payload.size()))
    {
        LOGI(COLORED_WITH(PACKET_TYPE, "mtVALIDATION"), " <error parsing>");
        return;
    }

    auto const& val = validation.validation();

    if (!config_.no_json)
    {
        LOGI(COLORED_WITH(PACKET_TYPE, "mtVALIDATION"), " ", get_sto_json(val));
    }
    else
    {
        LOGI(COLORED_WITH(PACKET_TYPE, "mtVALIDATION"));
    }

    print_hex(reinterpret_cast<std::uint8_t const*>(val.data()), val.size());
}

void
packet_processor::print_hex(std::uint8_t const* data, std::size_t len) const
{
    if (config_.no_hex)
        return;

    for (std::size_t j = 0; j < len; ++j)
    {
        if (j % 16 == 0 && !config_.raw_hex)
        {
            std::cout << "0x" << std::hex << std::setw(8) << std::setfill('0')
                      << j << ":\t";
        }

        std::cout << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(data[j]);

        if (!config_.raw_hex)
        {
            if (j % 16 == 15)
                std::cout << "\n";
            else if (j % 4 == 3)
                std::cout << "  ";
            else if (j % 2 == 1)
                std::cout << " ";
        }
    }
    std::cout << std::dec << "\n";
}

std::string
packet_processor::get_sto_json(std::string const& st) const
{
    try
    {
        // Use xdata to parse the STObject
        Slice slice(
            reinterpret_cast<std::uint8_t const*>(st.data()), st.size());
        xdata::SliceCursor cursor{slice, 0};

        // Load protocol definitions (cached)
        static auto protocol =
            xdata::Protocol::load_from_file(config_.protocol_definitions_path);

        // Use JsonVisitor to format the output
        xdata::JsonVisitor visitor(protocol);
        xdata::ParserContext ctx{cursor};
        xdata::parse_with_visitor(ctx, protocol, visitor);

        // Convert JSON to string
        return boost::json::serialize(visitor.get_result());
    }
    catch (std::exception const& e)
    {
        return std::string("Could not deserialize STObject: ") + e.what();
    }
}

void
packet_processor::print_sto(std::string const& st) const
{
    LOGI(get_sto_json(st));
}

void
packet_processor::update_stats(packet_type type, std::size_t bytes)
{
    auto type_val = static_cast<int>(type);
    if (auto it = counters_.find(type_val); it != counters_.end())
    {
        it->second.packet_count++;
        it->second.total_bytes += bytes;
    }
    else
    {
        counters_[type_val] = {1, bytes};
    }
}

bool
packet_processor::should_display_packet(packet_type type) const
{
    auto type_val = static_cast<int>(type);

    if (!filter_.show.empty())
    {
        return filter_.show.count(type_val) > 0;
    }

    if (!filter_.hide.empty())
    {
        return filter_.hide.count(type_val) == 0;
    }

    return true;
}

void
packet_processor::display_stats() const
{
    if (config_.no_stats)
        return;

    auto now = std::chrono::steady_clock::now();
    auto should_display = !config_.slow ||
        std::chrono::duration_cast<std::chrono::seconds>(
            now - last_display_time_)
                .count() >= 5;

    if (!should_display)
        return;

    const_cast<std::chrono::steady_clock::time_point&>(last_display_time_) =
        now;

    // Clear screen if needed
    if (config_.use_cls)
    {
        std::cout << "\033c";
    }

    auto elapsed =
        std::chrono::duration_cast<std::chrono::seconds>(now - start_time_)
            .count();
    if (elapsed <= 0)
        elapsed = 1;

    std::cout << "XRPL Peer Monitor -- Connected for " << elapsed << " sec\n\n";
    std::cout << "Packet                    Total               Per second     "
                 "     Total Bytes         Data rate\n";
    std::cout << "-------------------------------------------------------------"
                 "-----------------------------------------\n";

    std::uint64_t total_packets = 0;
    std::uint64_t total_bytes = 0;

    for (auto const& [type_val, stats] : counters_)
    {
        double packets_per_sec =
            static_cast<double>(stats.packet_count) / elapsed;
        double bytes_per_sec = static_cast<double>(stats.total_bytes) / elapsed;

        total_packets += stats.packet_count;
        total_bytes += stats.total_bytes;

        std::cout << std::left << std::setw(26)
                  << packet_type_to_string(
                         static_cast<packet_type>(type_val), true)
                  << std::setw(20) << stats.packet_count << std::setw(20)
                  << std::fixed << std::setprecision(2) << packets_per_sec
                  << std::setw(20) << format_bytes(stats.total_bytes)
                  << format_bytes(bytes_per_sec) << "/s\n";
    }

    std::cout << "-------------------------------------------------------------"
                 "-----------------------------------------\n";
    std::cout << std::left << std::setw(26) << "Totals" << std::setw(20)
              << total_packets << std::setw(20) << std::fixed
              << std::setprecision(2)
              << static_cast<double>(total_packets) / elapsed << std::setw(20)
              << format_bytes(total_bytes)
              << format_bytes(static_cast<double>(total_bytes) / elapsed)
              << "/s\n";
}

std::string
packet_processor::format_bytes(double bytes) const
{
    constexpr const char* suffixes[] = {"B", "K", "M", "G", "T"};
    int i = 0;
    while (bytes > 1024 && i < 4)
    {
        bytes /= 1024;
        i++;
    }

    std::ostringstream ss;
    ss << std::fixed << std::setprecision(2) << bytes << " " << suffixes[i];
    return ss.str();
}

void
packet_processor::set_custom_handler(packet_type type, custom_handler handler)
{
    custom_handlers_[type] = handler;
}

}  // namespace catl::peer::monitor