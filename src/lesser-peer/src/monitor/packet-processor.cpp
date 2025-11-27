#include <catl/base58/base58.h>
#include <catl/common/utils.h>
#include <catl/core/logger.h>
#include <catl/crypto/sha512-half-hasher.h>
#include <catl/peer/monitor/packet-processor.h>
#include <catl/peer/packet-names.h>
#include <catl/peer/wire-format.h>

#include "ripple.pb.h"  // Generated from ripple.proto

#include <algorithm>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace catl::peer::monitor {

packet_processor::packet_processor(monitor_config const& config)
    : config_(config)
    , start_time_(std::chrono::steady_clock::now())
    , manifest_tracker_(config_.peer.network_id)
{
}

void
packet_processor::process_packet(
    std::string const& peer_id,
    std::shared_ptr<peer_connection> connection,
    packet_header const& header,
    std::vector<std::uint8_t> const& payload)
{
    // peer_id is used for routing but stats tracking moved to monitor
    (void)peer_id;

    auto type = static_cast<packet_type>(header.type);
    update_stats(type, header.payload_size);

    // Handle PINGs always (critical for liveness)
    if (type == packet_type::ping)
    {
        handle_ping(connection, payload);
    }

    // Handle specific packet types for logic/state maintenance
    switch (type)
    {
        case packet_type::manifests:
            handle_manifests(payload);
            break;
        case packet_type::ledger_data:
            handle_ledger_data(payload);
            break;
        case packet_type::propose_ledger:
            handle_propose_ledger(connection, payload);
            break;
        case packet_type::status_change:
            handle_status_change(payload);
            break;
        case packet_type::validation:
            handle_validation(payload);
            break;
        case packet_type::endpoints:
            handle_endpoints(payload);
            break;
        case packet_type::get_objects:
            if (config_.mode == MonitorMode::Query)
                handle_get_objects(connection, payload);
            break;
        default:
            break;
    }

    // Check for custom handler
    if (auto it = custom_handlers_.find(type); it != custom_handlers_.end())
    {
        it->second(type, payload);
    }

    // Check manifests-only mode
    if (config_.mode == MonitorMode::Harvest &&
        type != packet_type::manifests && type != packet_type::ping)
    {
        if (shutdown_callback_)
        {
            shutdown_callback_();
        }
    }
}

void
packet_processor::handle_ping(
    std::shared_ptr<peer_connection> connection,
    std::vector<std::uint8_t> const& payload)
{
    protocol::TMPing ping;
    if (!ping.ParseFromArray(payload.data(), payload.size()))
        return;

    if (ping.type() == protocol::TMPing_pingType_ptPING)
    {
        // Send pong response
        ping.set_type(protocol::TMPing_pingType_ptPONG);
        std::vector<std::uint8_t> pong_data(ping.ByteSizeLong());
        ping.SerializeToArray(pong_data.data(), pong_data.size());

        connection->async_send_packet(
            packet_type::ping, pong_data, [](boost::system::error_code) {});
    }
}

void
packet_processor::handle_manifests(std::vector<std::uint8_t> const& payload)
{
    protocol::TMManifests manifests;
    if (!manifests.ParseFromArray(payload.data(), payload.size()))
        return;

    for (int i = 0; i < manifests.list_size(); ++i)
    {
        auto const& manifest = manifests.list(i);
        auto const& sto = manifest.stobject();
        // Process manifest with the tracker
        manifest_tracker_.process_manifest(
            reinterpret_cast<const uint8_t*>(sto.data()), sto.size());
    }
}

void
packet_processor::handle_ledger_data(std::vector<std::uint8_t> const& payload)
{
    protocol::TMLedgerData ld;
    if (!ld.ParseFromArray(payload.data(), payload.size()))
        return;

    // Convert ledger hash to hex
    std::stringstream hash_str;
    auto const& hash = ld.ledgerhash();
    for (std::size_t i = 0; i < hash.size() && i < 32; ++i)
    {
        hash_str << std::hex << std::setw(2) << std::setfill('0')
                 << static_cast<int>(static_cast<std::uint8_t>(hash[i]));
    }

    // Check if this is a candidate transaction set response
    if (ld.type() == protocol::liTS_CANDIDATE)
    {
        if (!config_.enable_txset_acquire)
            return;

        auto it = txset_acquirers_.find(hash_str.str());
        if (it == txset_acquirers_.end())
            return;

        auto acquirer = it->second;

        for (int i = 0; i < ld.nodes_size(); ++i)
        {
            const auto& node = ld.nodes(i);
            if (!node.has_nodeid() || !node.has_nodedata())
                continue;

            const auto& nodeid = node.nodeid();
            const auto& data = node.nodedata();

            if (nodeid.size() != 33)
                continue;

            Hash256 id(reinterpret_cast<const uint8_t*>(nodeid.data()));
            uint8_t depth = static_cast<uint8_t>(nodeid[32]);
            SHAMapNodeID node_id(id, depth);

            acquirer->on_node_received(
                node_id,
                reinterpret_cast<const uint8_t*>(data.data()),
                data.size());
        }
    }
}

void
packet_processor::handle_propose_ledger(
    std::shared_ptr<peer_connection> connection,
    std::vector<std::uint8_t> const& payload)
{
    protocol::TMProposeSet ps;
    if (!ps.ParseFromArray(payload.data(), payload.size()))
        return;

    std::stringstream hash_str;
    auto const& hash = ps.currenttxhash();
    for (std::size_t i = 0; i < hash.size() && i < 32; ++i)
    {
        hash_str << std::hex << std::setw(2) << std::setfill('0')
                 << static_cast<int>(static_cast<std::uint8_t>(hash[i]));
    }

    std::stringstream prev_hash_str;
    auto const& prev_hash = ps.previousledger();
    for (std::size_t i = 0; i < prev_hash.size() && i < 32; ++i)
    {
        prev_hash_str << std::hex << std::setw(2) << std::setfill('0')
                      << static_cast<int>(
                             static_cast<std::uint8_t>(prev_hash[i]));
    }

    if (dashboard_ && ps.proposeseq() > 0)
    {
        dashboard_->update_ledger_info(
            ps.proposeseq() - 1, prev_hash_str.str(), 1);
    }

    bool is_empty_set =
        (hash_str.str() ==
         "0000000000000000000000000000000000000000000000000000000000000000");

    if (!is_empty_set && config_.enable_txset_acquire)
    {
        if (txset_acquirers_.find(hash_str.str()) == txset_acquirers_.end())
        {
            auto acquirer = std::make_shared<TransactionSetAcquirer>(
                hash_str.str(),
                connection.get(),
                [](std::string const&, Slice const&) {},  // No-op tx callback
                [this, set_hash = hash_str.str()](bool, size_t) {
                    txset_acquirers_.erase(set_hash);
                });

            acquirer->start();
            txset_acquirers_[hash_str.str()] = std::move(acquirer);
        }
    }
}

void
packet_processor::handle_status_change(std::vector<std::uint8_t> const& payload)
{
    protocol::TMStatusChange status;
    if (!status.ParseFromArray(payload.data(), payload.size()))
        return;

    std::string hash_str;
    if (status.has_ledgerhash())
    {
        auto const& hash = status.ledgerhash();
        std::stringstream hash_ss;
        for (std::size_t i = 0; i < hash.size() && i < 32; ++i)
        {
            hash_ss << std::hex << std::setw(2) << std::setfill('0')
                    << static_cast<int>(static_cast<std::uint8_t>(hash[i]));
        }
        hash_str = hash_ss.str();
    }

    if (dashboard_ && status.has_ledgerseq() && status.has_ledgerhash())
    {
        if (status.has_newevent() &&
            (status.newevent() == 2 || status.newevent() == 3))
        {
            dashboard_->update_ledger_info(status.ledgerseq(), hash_str, 0);
        }
        else if (!hash_str.empty())
        {
            dashboard_->update_ledger_info(status.ledgerseq(), hash_str, 0);
        }
    }
}

void
packet_processor::handle_validation(std::vector<std::uint8_t> const& payload)
{
    if (!dashboard_)
        return;

    // Get current ledger sequence from dashboard
    auto current = dashboard_->get_current_ledger();
    if (current.sequence == 0)
        return;  // No ledger to associate with

    uint32_t ledger_seq = current.sequence;

    // Hash validation payload for deduplication
    crypto::Sha512HalfHasher hasher;
    hasher.update(payload.data(), payload.size());
    auto hash = hasher.finalize();
    std::string key = hash.hex();

    // Check if we've seen this validation for this ledger
    auto& ledger_validations = validations_by_ledger_[ledger_seq];
    auto [it, inserted] = ledger_validations.insert(key);
    if (!inserted)
        return;  // Duplicate, ignore

    // Prune old ledgers - keep only recent ones
    while (validations_by_ledger_.size() > MAX_LEDGERS_TRACKED)
    {
        // Remove oldest (lowest sequence)
        validations_by_ledger_.erase(validations_by_ledger_.begin());
    }

    // Update dashboard with validation count for this ledger
    dashboard_->update_ledger_info(
        ledger_seq,
        current.hash,
        static_cast<uint32_t>(ledger_validations.size()));
}

void
packet_processor::handle_endpoints(std::vector<std::uint8_t> const& payload)
{
    protocol::TMEndpoints eps;
    if (!eps.ParseFromArray(payload.data(), payload.size()))
        return;

    bool updated = false;
    for (int i = 0; i < eps.endpoints_v2_size(); ++i)
    {
        const auto& ep = eps.endpoints_v2(i);
        std::string ep_str = ep.endpoint();
        auto has_real_chars =
            ep_str.find_first_not_of("[]:") != std::string::npos;
        if (!ep_str.empty() && has_real_chars)
        {
            auto [it, inserted] = available_endpoints_.insert(ep_str);
            if (inserted)
                updated = true;
        }
    }

    if (updated && dashboard_)
    {
        std::vector<std::string> endpoints(
            available_endpoints_.begin(), available_endpoints_.end());
        dashboard_->update_available_endpoints(endpoints);
    }
}

void
packet_processor::handle_get_objects(
    std::shared_ptr<peer_connection> /* connection */,
    std::vector<std::uint8_t> const& /* payload */)
{
    // Only relevant for logic if we need to process responses programmatically
    // For now, this is mostly a logging concern, handled by PacketLogger
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

void
packet_processor::set_custom_handler(packet_type type, custom_handler handler)
{
    custom_handlers_[type] = handler;
}

}  // namespace catl::peer::monitor