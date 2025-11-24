#include <boost/json.hpp>
#include <catl/base58/base58.h>
#include <catl/common/utils.h>
#include <catl/core/logger.h>
#include <catl/peer/monitor/packet-processor.h>
#include <catl/peer/packet-names.h>
#include <catl/peer/wire-format.h>
#include <catl/xdata-json/parse_transaction.h>
#include <catl/xdata-json/pretty_print.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/slice-cursor.h>

#include "ripple.pb.h"  // Generated from ripple.proto

#include <algorithm>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace catl::peer::monitor {

// Logging partition for transaction JSON output
// Can be disabled with LOG_TX_JSON=0 environment variable
static LogPartition tx_json_partition("tx_json", []() -> LogLevel {
    const char* env = std::getenv("LOG_TX_JSON");
    if (env && std::string(env) == "0")
    {
        return LogLevel::NONE;  // Disable transaction JSON logging
    }
    return LogLevel::INFO;  // Default to INFO
}());

#define PACKET_TYPE BOLD_CYAN

packet_processor::packet_processor(monitor_config const& config)
    : config_(config)
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

    // Update dashboard if we have one
    if (dashboard_)
    {
        PeerDashboard::Stats stats;
        stats.peer_address = connection->remote_endpoint();
        stats.connected = connection->is_connected();
        stats.peer_version = connection->server_version();
        stats.protocol_version = connection->protocol_version();
        stats.network_id = connection->network_id();

        // Convert packet counters to dashboard format
        for (auto const& [type_val, counter] : counters_)
        {
            auto type_name = packet_type_to_string(
                static_cast<packet_type>(type_val), false);
            stats.packet_counts[std::string(type_name)] = counter.packet_count;
            stats.packet_bytes[std::string(type_name)] = counter.total_bytes;
            stats.total_packets += counter.packet_count;
            stats.total_bytes += counter.total_bytes;
        }

        auto now = std::chrono::steady_clock::now();
        stats.elapsed_seconds =
            std::chrono::duration_cast<std::chrono::seconds>(now - start_time_)
                .count();
        stats.last_packet_time = now;
        stats.connection_state = "Connected";

        dashboard_->update_stats(stats);
    }

    // Skip packet-specific handling and logging when dashboard is active
    if (dashboard_ || (config_.display.no_dump && !config_.display.query_mode))
    {
        // Only handle ping for automatic pong response
        if (type == packet_type::ping)
        {
            handle_ping(connection, payload);
        }
        return;
    }

    // In query mode, only process GET_OBJECTS packets
    if (config_.display.query_mode)
    {
        if (type == packet_type::ping)
        {
            handle_ping(connection, payload);
        }
        else if (type == packet_type::get_objects)
        {
            handle_get_objects(connection, payload);
        }
        // Skip all other packets in query mode
        return;
    }

    // Handle specific packet types (only when dashboard is not active)
    switch (type)
    {
        case packet_type::ping:
            handle_ping(connection, payload);
            break;
        case packet_type::manifests:
            if (should_display_packet(type))
                handle_manifests(payload);
            break;
        case packet_type::transaction:
            if (should_display_packet(type) && !config_.display.no_dump)
                handle_transaction(payload);
            break;
        case packet_type::get_ledger:
            if (should_display_packet(type) && !config_.display.no_dump)
                handle_get_ledger(payload);
            break;
        case packet_type::ledger_data:
            if (should_display_packet(packet_type::ledger_data) ||
                should_display_packet(
                    packet_type::propose_ledger))  // Show if monitoring
                                                   // proposals
            {
                handle_ledger_data(payload);
            }
            break;
        case packet_type::propose_ledger:
            handle_propose_ledger(connection, payload);
            break;
        case packet_type::status_change:
            if (should_display_packet(type) && !config_.display.no_dump)
                handle_status_change(payload);
            break;
        case packet_type::validation:
            if (should_display_packet(type) && !config_.display.no_dump)
                handle_validation(payload);
            break;
        case packet_type::get_objects:
            if (config_.display.query_mode)  // Only in query mode
                handle_get_objects(connection, payload);
            break;
        default:
            // Check for custom handler
            if (auto it = custom_handlers_.find(type);
                it != custom_handlers_.end())
            {
                it->second(type, payload);
            }
            else if (should_display_packet(type))
            {
                // Unknown packet (only show if not filtered)
                if (!config_.display.no_dump)
                {
                    auto packet_name = get_packet_name(header.type);
                    LOGI(
                        COLORED(PACKET_TYPE, packet_name),
                        " [Unhandled packet ",
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

    // Display stats if needed (but skip if monitoring proposals)
    if (!config_.display.no_stats && !config_.display.query_mode)
    {
        display_stats();
    }

    // Check manifests-only mode
    if (config_.display.manifests_only && type != packet_type::manifests)
    {
        LOGI(
            "Received non-manifest packet in manifests-only mode, requesting "
            "shutdown");
        if (shutdown_callback_)
        {
            shutdown_callback_();
        }
    }
}

void
packet_processor::handle_ping(
    std::shared_ptr<peer_connection>
        connection,  // NOLINT(performance-unnecessary-value-param)
                     // - need shared ownership
    std::vector<std::uint8_t> const& payload)
{
    protocol::TMPing ping;
    // NOLINTNEXTLINE(bugprone-narrowing-conversions) - protobuf API uses int
    // for size
    if (!ping.ParseFromArray(payload.data(), payload.size()))
    {
        LOGE("Failed to parse TMPing");
        return;
    }

    if (!config_.display.no_dump)
    {
        if (ping.type() == protocol::TMPing_pingType_ptPING)
        {
            LOGI(COLORED(PACKET_TYPE, "mtPING"), " - replying PONG");

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
            LOGI(COLORED(PACKET_TYPE, "mtPING"), " - received PONG");
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
        COLORED(PACKET_TYPE, "mtManifests"),
        " contains ",
        manifests.list_size(),
        " manifests");

    for (int i = 0; i < manifests.list_size(); ++i)
    {
        auto const& manifest = manifests.list(i);
        auto const& sto = manifest.stobject();

        // Process manifest with the tracker
        manifest_tracker_.process_manifest(
            reinterpret_cast<const uint8_t*>(sto.data()), sto.size());

        if (!config_.display.no_json)
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

    LOGI("  📊 Tracking ", manifest_tracker_.validator_count(), " validators");
}

void
packet_processor::handle_transaction(std::vector<std::uint8_t> const& payload)
{
    protocol::TMTransaction txn;
    if (!txn.ParseFromArray(payload.data(), payload.size()))
    {
        LOGI(COLORED(PACKET_TYPE, "mtTRANSACTION"), " <error parsing>");
        return;
    }

    auto const& raw_txn = txn.rawtransaction();

    if (!config_.display.no_json)
    {
        LOGI(COLORED(PACKET_TYPE, "mtTRANSACTION"), " ", get_sto_json(raw_txn));
    }
    else
    {
        LOGI(COLORED(PACKET_TYPE, "mtTRANSACTION"));
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
        COLORED(PACKET_TYPE, "mtGET_LEDGER"),
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
packet_processor::handle_ledger_data(std::vector<std::uint8_t> const& payload)
{
    LOGI("📨 RECEIVED mtLEDGER_DATA packet!");

    protocol::TMLedgerData ld;
    if (!ld.ParseFromArray(payload.data(), payload.size()))
    {
        LOGE("Failed to parse TMLedgerData");
        return;
    }

    // Log basic info first
    LOGI(
        "  Type: ",
        ld.type(),
        " (",
        ld.type() == protocol::liTS_CANDIDATE
            ? "TS_CANDIDATE"
            : ld.type() == protocol::liBASE ? "BASE"
                                            : ld.type() == protocol::liTX_NODE
                    ? "TX_NODE"
                    : ld.type() == protocol::liAS_NODE ? "AS_NODE" : "UNKNOWN",
        ")");

    // Check if there's an error
    if (ld.has_error())
    {
        LOGI("  ❌ ERROR in response: code=", ld.error());
        return;
    }

    // Convert ledger hash to hex
    std::stringstream hash_str;
    auto const& hash = ld.ledgerhash();
    for (std::size_t i = 0; i < hash.size() && i < 32; ++i)
    {
        hash_str << std::hex << std::setw(2) << std::setfill('0')
                 << static_cast<int>(static_cast<std::uint8_t>(hash[i]));
    }

    LOGI("  Hash: ", hash_str.str());
    LOGI("  Nodes: ", ld.nodes_size());

    // Check if this is a candidate transaction set response
    if (ld.type() == protocol::liTS_CANDIDATE)
    {
        // Find the acquirer for this transaction set
        auto it = txset_acquirers_.find(hash_str.str());
        if (it == txset_acquirers_.end())
        {
            LOGI(
                "  ⚠️ Received TMLedgerData for unknown transaction set: ",
                hash_str.str().substr(0, 16),
                "...");
            return;
        }

        auto& acquirer = it->second;

        LOGI("  📨 Received ", ld.nodes_size(), " node(s) for transaction set");

        // Feed each node to the acquirer
        for (int i = 0; i < ld.nodes_size(); ++i)
        {
            const auto& node = ld.nodes(i);

            if (!node.has_nodeid() || !node.has_nodedata())
            {
                LOGE("  ❌ Node ", i, " missing nodeid or nodedata");
                continue;
            }

            const auto& nodeid = node.nodeid();
            const auto& data = node.nodedata();

            // Parse the nodeid (33 bytes: 32-byte id + 1-byte depth)
            if (nodeid.size() != 33)
            {
                LOGE(
                    "  ❌ Invalid nodeid size: ",
                    nodeid.size(),
                    " (expected 33)");
                continue;
            }

            // Extract the SHAMapNodeID
            Hash256 id(reinterpret_cast<const uint8_t*>(nodeid.data()));
            uint8_t depth = static_cast<uint8_t>(nodeid[32]);
            SHAMapNodeID node_id(id, depth);

            // Feed to the acquirer
            acquirer->on_node_received(
                node_id,
                reinterpret_cast<const uint8_t*>(data.data()),
                data.size());
        }
    }
    else
    {
        // Other ledger data types
        LOGI(
            COLORED(PACKET_TYPE, "mtLEDGER_DATA"),
            " seq=",
            ld.ledgerseq(),
            " hash=",
            hash_str.str(),
            " type=",
            ld.type(),
            " nodes=",
            ld.nodes_size());
    }
}

void
packet_processor::handle_propose_ledger(
    std::shared_ptr<peer_connection> connection,
    std::vector<std::uint8_t> const& payload)
{
    protocol::TMProposeSet ps;
    if (!ps.ParseFromArray(payload.data(), payload.size()))
    {
        LOGI("Failed to parse TMProposeSet");
        return;
    }

    // Check if we should display this packet type based on filters
    if (!should_display_packet(packet_type::propose_ledger))
    {
        // Still update dashboard but don't log
        if (dashboard_ && ps.proposeseq() > 0)
        {
            std::stringstream prev_hash_str;
            auto const& prev_hash = ps.previousledger();
            for (std::size_t i = 0; i < prev_hash.size() && i < 32; ++i)
            {
                prev_hash_str << std::hex << std::setw(2) << std::setfill('0')
                              << static_cast<int>(
                                     static_cast<std::uint8_t>(prev_hash[i]));
            }
            dashboard_->update_ledger_info(
                ps.proposeseq() - 1, prev_hash_str.str(), 1);
        }
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

    // Also encode as base58 validator public key
    std::string validator_key;
    std::string master_validator_key;
    if (pubkey.size() == 33)  // Compressed public key
    {
        validator_key = catl::base58::encode_node_public(
            reinterpret_cast<const uint8_t*>(pubkey.data()), pubkey.size());

        // Try to look up the master key
        auto master_key_opt = manifest_tracker_.get_master_key(pub_str.str());
        if (master_key_opt)
        {
            master_validator_key = *master_key_opt;
            LOGD(
                "  Found master key: ",
                master_validator_key,
                " for ephemeral: ",
                validator_key);
        }
    }
    else
    {
        LOGD(
            "  Unexpected pubkey size: ",
            pubkey.size(),
            " bytes (expected 33)");
    }

    // Extract previous ledger hash for tracking
    std::stringstream prev_hash_str;
    auto const& prev_hash = ps.previousledger();
    for (std::size_t i = 0; i < prev_hash.size() && i < 32; ++i)
    {
        prev_hash_str << std::hex << std::setw(2) << std::setfill('0')
                      << static_cast<int>(
                             static_cast<std::uint8_t>(prev_hash[i]));
    }

    // Update dashboard with proposed ledger info
    // Proposals are for the NEXT ledger, so the previous ledger is what's been
    // accepted
    if (dashboard_ && ps.proposeseq() > 0)
    {
        // Track the previous ledger as it's the one that's been closed
        dashboard_->update_ledger_info(
            ps.proposeseq() - 1, prev_hash_str.str(), 1);
    }

    LOGI(
        COLORED(PACKET_TYPE, "mtPROPOSE_LEDGER"),
        " seq=",
        ps.proposeseq(),
        " set=",
        hash_str.str().substr(0, 16) + "...",
        " prev=",
        prev_hash_str.str().substr(0, 16) + "...",
        " validator=",
        !master_validator_key.empty()
            ? master_validator_key
            : (!validator_key.empty() ? validator_key + " (ephemeral)"
                                      : pub_str.str()),
        " time=",
        common::format_ripple_time(ps.closetime()));

    // Check if this is an empty transaction set
    bool is_empty_set =
        (hash_str.str() ==
         "0000000000000000000000000000000000000000000000000000000000000000");

    if (is_empty_set)
    {
        LOGI("  ⚠️  EMPTY TRANSACTION SET - No transactions proposed");
    }
    else
    {
        // Create an acquirer for this transaction set if we don't have one
        // already
        if (txset_acquirers_.find(hash_str.str()) == txset_acquirers_.end())
        {
            LOGI(
                "  📦 Creating transaction set acquirer for ",
                hash_str.str().substr(0, 16),
                "...");

            auto acquirer = std::make_unique<TransactionSetAcquirer>(
                hash_str.str(),
                connection.get(),
                // Transaction callback
                [this](std::string const& tx_hash, Slice const& tx_data) {
                    LOGI(
                        "    💰 Transaction found: ",
                        tx_hash,
                        " (",
                        tx_data.size(),
                        " bytes)");

                    // Dump raw hex to see what we have
                    std::stringstream hex_stream;
                    for (size_t j = 0; j < tx_data.size(); ++j)
                    {
                        hex_stream << std::hex << std::setw(2)
                                   << std::setfill('0')
                                   << static_cast<int>(tx_data.data()[j]);
                    }
                    LOGI(
                        "      Raw hex (",
                        tx_data.size(),
                        " bytes): ",
                        hex_stream.str());

                    try
                    {
                        // Load protocol definitions (cached)
                        // Using Xahau testnet protocol (network ID 21338)
                        static auto protocol =
                            xdata::Protocol::load_embedded_xahau_protocol(
                                xdata::ProtocolOptions{.network_id = 21338});

                        // Parse the transaction set leaf node
                        auto json_value = xdata::json::parse_txset_transaction(
                            tx_data, protocol);

                        // Pretty print to string
                        std::stringstream ss;
                        xdata::json::pretty_print(ss, json_value);

                        PLOGI(
                            tx_json_partition,
                            "      Transaction JSON:\n",
                            ss.str());
                    }
                    catch (std::exception const& e)
                    {
                        LOGE("      Failed to parse transaction: ", e.what());
                    }
                },
                // Completion callback
                [this, set_hash = hash_str.str()](
                    bool success, size_t num_transactions) {
                    if (success)
                    {
                        LOGI(
                            "  ✅ Transaction set acquisition complete: ",
                            num_transactions,
                            " transactions");
                    }
                    else
                    {
                        LOGI("  ❌ Transaction set acquisition FAILED");
                    }
                    // Remove the acquirer when done
                    txset_acquirers_.erase(set_hash);
                });

            // Start the acquisition
            acquirer->start();

            // Store the acquirer
            txset_acquirers_[hash_str.str()] = std::move(acquirer);
        }

        // Log the added transactions (these are the transaction hashes in the
        // proposal)
        if (ps.addedtransactions_size() > 0)
        {
            LOGI("  Added transactions (", ps.addedtransactions_size(), "):");
            for (int i = 0; i < ps.addedtransactions_size(); ++i)
            {
                auto const& tx_hash = ps.addedtransactions(i);
                std::stringstream tx_str;
                for (std::size_t j = 0; j < tx_hash.size() && j < 32; ++j)
                {
                    tx_str << std::hex << std::setw(2) << std::setfill('0')
                           << static_cast<int>(
                                  static_cast<std::uint8_t>(tx_hash[j]));
                }
                LOGI("    [", i, "] ", tx_str.str());

                // Check if this is one of our disputed transactions
                std::string tx_upper = tx_str.str();
                std::transform(
                    tx_upper.begin(),
                    tx_upper.end(),
                    tx_upper.begin(),
                    ::toupper);
                if (tx_upper ==
                        "93A8C30D8E380D8E3D78FBAF129F6A42A6F53F2178F0FCF7B1A654"
                        "4A77BDC84C" ||
                    tx_upper ==
                        "5697CC215A76AC664C3D39948DAE3DF606F4E2F6246E29369509D5"
                        "F20BC3CB56" ||
                    tx_upper ==
                        "15D3CF191DF46DB2AA1C89D52CADCBDC1B8F843B77FEFE34FDFB31"
                        "11682DC929")
                {
                    LOGI("    ^^^ DISPUTED TRANSACTION FOUND! ^^^");
                }
            }
        }
        else if (!is_empty_set)
        {
            LOGI("  Transaction set hash: ", hash_str.str());
            LOGI(
                "  But no individual transaction hashes provided (large set?)");
        }
    }

    // Log removed transactions
    if (ps.removedtransactions_size() > 0)
    {
        LOGI("  Removed transactions (", ps.removedtransactions_size(), "):");
        for (int i = 0; i < ps.removedtransactions_size(); ++i)
        {
            auto const& tx_hash = ps.removedtransactions(i);
            std::stringstream tx_str;
            for (std::size_t j = 0; j < tx_hash.size() && j < 32; ++j)
            {
                tx_str << std::hex << std::setw(2) << std::setfill('0')
                       << static_cast<int>(
                              static_cast<std::uint8_t>(tx_hash[j]));
            }
            LOGI("    [-] ", tx_str.str());

            // Check if this is one of our disputed transactions being REMOVED
            std::string tx_upper = tx_str.str();
            std::transform(
                tx_upper.begin(), tx_upper.end(), tx_upper.begin(), ::toupper);
            if (tx_upper ==
                    "93A8C30D8E380D8E3D78FBAF129F6A42A6F53F2178F0FCF7B1A6544A77"
                    "BDC84C" ||
                tx_upper ==
                    "5697CC215A76AC664C3D39948DAE3DF606F4E2F6246E29369509D5F20B"
                    "C3CB56" ||
                tx_upper ==
                    "15D3CF191DF46DB2AA1C89D52CADCBDC1B8F843B77FEFE34FDFB311168"
                    "2DC929")
            {
                LOGI("    ⚠️ ^^^ DISPUTED TRANSACTION BEING REMOVED! ^^^");
            }
        }
    }
}

void
packet_processor::handle_status_change(std::vector<std::uint8_t> const& payload)
{
    protocol::TMStatusChange status;
    if (!status.ParseFromArray(payload.data(), payload.size()))
    {
        LOGI(COLORED(PACKET_TYPE, "mtSTATUS_CHANGE"), " <error parsing>");
        return;
    }

    std::stringstream msg;
    msg << catl::color::PACKET_TYPE << "mtSTATUS_CHANGE" << catl::color::RESET;

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

    std::string hash_str;
    if (status.has_ledgerhash())
    {
        msg << " hash=";
        auto const& hash = status.ledgerhash();
        std::stringstream hash_ss;
        for (std::size_t i = 0; i < hash.size() && i < 32; ++i)
        {
            hash_ss << std::hex << std::setw(2) << std::setfill('0')
                    << static_cast<int>(static_cast<std::uint8_t>(hash[i]));
        }
        hash_str = hash_ss.str();
        msg << hash_str;
    }

    // Update dashboard with ledger info on various events
    if (dashboard_ && status.has_ledgerseq() && status.has_ledgerhash())
    {
        // Track ledger info on ACCEPTED_LEDGER (2) or SWITCHED_LEDGER (3)
        // events
        if (status.has_newevent() &&
            (status.newevent() == 2 || status.newevent() == 3))
        {
            dashboard_->update_ledger_info(status.ledgerseq(), hash_str, 0);
        }
        // Also track any status change with ledger info
        else if (!hash_str.empty())
        {
            dashboard_->update_ledger_info(status.ledgerseq(), hash_str, 0);
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
        LOGI(COLORED(PACKET_TYPE, "mtVALIDATION"), " <error parsing>");
        return;
    }

    auto const& val = validation.validation();

    // Track validation count for the dashboard
    // Note: We're incrementing validation count for the current ledger
    // In a full implementation, we'd decode the validation to get the specific
    // ledger sequence
    if (dashboard_)
    {
        // Get current ledger and increment its validation count
        auto current_ledger = dashboard_->get_current_ledger();
        if (current_ledger.sequence > 0)
        {
            dashboard_->update_ledger_info(
                current_ledger.sequence,
                current_ledger.hash,
                current_ledger.validation_count + 1);
        }
    }

    if (!config_.display.no_json)
    {
        LOGI(COLORED(PACKET_TYPE, "mtVALIDATION"), " ", get_sto_json(val));
    }
    else
    {
        LOGI(COLORED(PACKET_TYPE, "mtVALIDATION"));
    }

    print_hex(reinterpret_cast<std::uint8_t const*>(val.data()), val.size());
}

void
packet_processor::handle_get_objects(
    std::shared_ptr<peer_connection> connection,
    std::vector<std::uint8_t> const& payload)
{
    protocol::TMGetObjectByHash response;
    if (!response.ParseFromArray(payload.data(), payload.size()))
    {
        LOGE("Error parsing transaction query response");
        return;
    }

    // Check if this is a response (not a query)
    if (response.query())
    {
        // Incoming query from peer - ignore silently
        return;
    }

    // This is a response to our transaction query
    if (response.type() == protocol::TMGetObjectByHash::otTRANSACTION ||
        response.type() == protocol::TMGetObjectByHash::otTRANSACTION_NODE)
    {
        // Get the original transaction hash from the sequence number
        std::string tx_hash = "";
        if (response.has_seq())
        {
            tx_hash = connection->get_query_hash(response.seq());
        }

        std::string obj_type =
            (response.type() == protocol::TMGetObjectByHash::otTRANSACTION)
            ? "TRANSACTION"
            : "TRANSACTION_NODE";

        if (response.objects_size() == 0)
        {
            LOGI("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            if (!tx_hash.empty())
            {
                LOGI("Query Response for:");
                LOGI("  ", tx_hash);
                LOGI("  Type: ", obj_type);
                LOGI("  Seq: ", response.seq());
            }
            else
            {
                LOGI("Query Response (seq=", response.seq(), "):");
                LOGI("  Type: ", obj_type);
            }
            LOGI("  Result: NOT FOUND");
            LOGI("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            return;
        }

        for (int i = 0; i < response.objects_size(); ++i)
        {
            const auto& obj = response.objects(i);

            // Display transaction hash
            std::stringstream hash_str;
            if (obj.has_hash())
            {
                const auto& hash = obj.hash();
                for (size_t j = 0; j < hash.size() && j < 32; ++j)
                {
                    hash_str << std::hex << std::setw(2) << std::setfill('0')
                             << static_cast<int>(static_cast<uint8_t>(hash[j]));
                }
            }

            LOGI("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            if (!tx_hash.empty())
            {
                LOGI("Query Response FOUND for:");
                LOGI("  ", tx_hash);
                LOGI("  Type: ", obj_type);
                LOGI("  Seq: ", response.seq());
            }
            else
            {
                LOGI(
                    "Transaction: ",
                    hash_str.str().empty() ? "UNKNOWN" : hash_str.str());
                LOGI("  Type: ", obj_type);
                LOGI("  Seq: ", response.seq());
            }

            // Decode and display transaction data
            if (obj.has_data())
            {
                const auto& data = obj.data();
                LOGI("Size: ", data.size(), " bytes");

                // Try to decode as JSON if possible (always show for queries)
                std::string json_str = get_sto_json(data);
                if (!json_str.empty())
                {
                    LOGI("Data: ", json_str);
                }
                else
                {
                    // Show hex if JSON decode failed
                    LOGI("Hex Data:");
                    print_hex(
                        reinterpret_cast<const uint8_t*>(data.data()),
                        data.size());
                }
            }
            else
            {
                LOGI("Status: NOT FOUND");
                LOGI("(Transaction not in peer's memory/database)");
            }
            LOGI("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        }
    }
    else
    {
        LOGI("  Type: ", static_cast<int>(response.type()));
    }
}

void
packet_processor::print_hex(std::uint8_t const* data, std::size_t len) const
{
    if (config_.display.no_hex)
        return;

    for (std::size_t j = 0; j < len; ++j)
    {
        if (j % 16 == 0 && !config_.display.raw_hex)
        {
            std::cout << "0x" << std::hex << std::setw(8) << std::setfill('0')
                      << j << ":\t";
        }

        std::cout << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(data[j]);

        if (!config_.display.raw_hex)
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
        // Using Xahau testnet protocol (network ID 21338)
        static auto protocol = xdata::Protocol::load_embedded_xahau_protocol(
            xdata::ProtocolOptions{.network_id = 21338});

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

    if (!config_.filter.show.empty())
    {
        return config_.filter.show.count(type_val) > 0;
    }

    if (!config_.filter.hide.empty())
    {
        return config_.filter.hide.count(type_val) == 0;
    }

    return true;
}

void
packet_processor::display_stats() const
{
    if (config_.display.no_stats)
        return;

    // Skip stats display entirely for now - it's broken with huge numbers
    return;

    auto now = std::chrono::steady_clock::now();
    auto should_display = !config_.display.slow ||
        std::chrono::duration_cast<std::chrono::seconds>(
            now - last_display_time_)
                .count() >= 5;

    if (!should_display)
        return;

    const_cast<std::chrono::steady_clock::time_point&>(last_display_time_) =
        now;

    // Clear screen if needed
    if (config_.display.use_cls)
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