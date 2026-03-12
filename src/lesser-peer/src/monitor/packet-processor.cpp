#include <boost/json.hpp>
#include <catl/base58/base58.h>
#include <catl/common/utils.h>
#include <catl/core/logger.h>
#include <catl/crypto/sha512-half-hasher.h>
#include <catl/peer/monitor/packet-processor.h>
#include <catl/peer/packet-names.h>
#include <catl/peer/wire-format.h>
#include <catl/xdata-json/parse_transaction.h>

#include "ripple.pb.h"  // Generated from ripple.proto

#include <algorithm>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace catl::peer::monitor {

static LogPartition pproc_log("pproc", LogLevel::INFO);

packet_processor::packet_processor(monitor_config const& config)
    : config_(config)
    , start_time_(std::chrono::steady_clock::now())
    , manifest_tracker_(config_.peer.network_id)
{
}

packet_processor::~packet_processor()
{
    stop();
}

void
packet_processor::stop()
{
    // Clear all acquirers to prevent callbacks accessing destroyed objects
    txset_acquirers_.clear();
    active_connections_.clear();
    dashboard_.reset();
}

void
packet_processor::reset_state()
{
    seen_proposals_.clear();
    txset_acquirers_.clear();
    shared_node_cache_.clear();
    manifest_tracker_.clear();
    available_endpoints_.clear();
}

void
packet_processor::process_packet(
    std::string const& peer_id,
    std::shared_ptr<peer_connection> connection,
    packet_header const& header,
    std::vector<std::uint8_t> const& payload)
{
    // Track active connection for parallel txset acquisition
    if (connection)
    {
        active_connections_[peer_id] = connection;
    }

    auto type = static_cast<packet_type>(header.type);
    update_stats(type, header.payload_size);

    // Handle PINGs always (critical for liveness)
    if (type == packet_type::ping)
    {
        handle_ping(peer_id, connection, payload);
    }

    // Handle specific packet types for logic/state maintenance
    switch (type)
    {
        case packet_type::manifests:
            handle_manifests(payload);
            break;
        case packet_type::ledger_data:
            handle_ledger_data(connection, payload);
            break;
        case packet_type::propose_ledger:
            handle_propose_ledger(peer_id, connection, payload);
            break;
        case packet_type::status_change:
            handle_status_change(payload);
            break;
        case packet_type::validation:
            handle_validation(peer_id, payload);
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
    std::string const& peer_id,
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
    else if (ping.type() == protocol::TMPing_pingType_ptPONG)
    {
        // Forward to dashboard for RTT tracking
        if (dashboard_ && ping.has_seq())
        {
            dashboard_->record_pong(peer_id, ping.seq());
        }
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
        auto info = manifest_tracker_.process_manifest(
            reinterpret_cast<const uint8_t*>(sto.data()), sto.size());

        // Reconcile: if this manifest established an ephemeral→master mapping,
        // sweep dashboard state to replace any entries stored under the raw
        // ephemeral hex key with the resolved master key.
        if (info && dashboard_ && !info->ephemeral_key_hex.empty() &&
            !info->master_key.empty())
        {
            dashboard_->reconcile_key(
                info->ephemeral_key_hex, info->master_key);
        }
    }
}

void
packet_processor::handle_ledger_data(
    std::shared_ptr<peer_connection> connection,
    std::vector<std::uint8_t> const& payload)
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

    // Log ALL ledger_data packets for debugging
    PLOGI(
        proposal_log(),
        "📦 Received mtLEDGER_DATA type=",
        ld.type(),
        " hash=",
        hash_str.str().substr(0, 16),
        "... nodes=",
        ld.nodes_size());

    // Check if this is a candidate transaction set response
    if (ld.type() == protocol::liTS_CANDIDATE)
    {
        PLOGI(
            proposal_log(),
            "📥 Received liTS_CANDIDATE for hash=",
            hash_str.str().substr(0, 16),
            "... nodes=",
            ld.nodes_size(),
            " acquirers=",
            txset_acquirers_.size());

        // Find acquirer - COPY the shared_ptr to prevent use-after-free
        // if the acquirer completes and erases itself during processing
        auto it = txset_acquirers_.find(hash_str.str());
        if (it == txset_acquirers_.end())
        {
            PLOGI(proposal_log(), "  ⚠️ No acquirer found for this hash");
            return;
        }

        auto acquirer = it->second;  // Copy, not reference!

        // Record reply received (count actual nodes)
        if (dashboard_ && ld.nodes_size() > 0)
            dashboard_->record_txset_reply(hash_str.str(), ld.nodes_size());

        // Record successful response for this connection (latency tracking)
        if (connection)
            acquirer->record_response(connection.get());

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
    std::string const& peer_id,
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
    // Parse RNG extension fields from ExtendedPosition.
    // Layout: [32B txSetHash] [1B flags] [optional 32B hashes...]
    // Flags: 0x01=commitSetHash, 0x02=entropySetHash,
    //        0x04=myCommitment,  0x08=myReveal
    bool has_commitment = false;
    bool has_reveal = false;
    if (hash.size() > 32)
    {
        auto flags = static_cast<uint8_t>(hash[32]);
        // Walk past optional hashes to validate size, and extract flags
        size_t expected = 33;
        if (flags & 0x01)
            expected += 32;  // commitSetHash
        if (flags & 0x02)
            expected += 32;  // entropySetHash
        if (flags & 0x04)
            expected += 32;  // myCommitment
        if (flags & 0x08)
            expected += 32;  // myReveal

        if (hash.size() >= expected)
        {
            has_commitment = (flags & 0x04) != 0;
            has_reveal = (flags & 0x08) != 0;
        }
    }

    LOGD(
        "[PROP-PKT] seq=",
        ps.proposeseq(),
        " tx_hash=",
        hash_str.str().substr(0, 16),
        "... prev=",
        prev_hash_str.str().substr(0, 16),
        "...",
        has_commitment ? " [C]" : "",
        has_reveal ? " [R]" : "");

    // Extract nodePubKey and resolve to master key
    std::string validator_key;
    if (ps.has_nodepubkey())
    {
        auto const& pub_key = ps.nodepubkey();
        std::stringstream key_str;
        for (std::size_t i = 0; i < pub_key.size() && i < 33; ++i)
        {
            key_str << std::hex << std::setw(2) << std::setfill('0')
                    << static_cast<int>(static_cast<std::uint8_t>(pub_key[i]));
        }
        auto ephemeral_key_hex = key_str.str();

        // Resolve ephemeral key to master key via manifest tracker
        auto master_key = manifest_tracker_.get_master_key(ephemeral_key_hex);
        validator_key = master_key ? *master_key : ephemeral_key_hex;
    }

    // Duplicate detection: same (prev_hash, validator, seq) with different
    // tx_hash
    if (!validator_key.empty())
    {
        ProposalKey key{prev_hash_str.str(), validator_key, ps.proposeseq()};
        auto it = seen_proposals_.find(key);
        if (it != seen_proposals_.end())
        {
            if (it->second != hash_str.str())
            {
                PLOGE(
                    proposal_log(),
                    "CONFLICT! validator=",
                    validator_key.substr(0, 16),
                    "... seq=",
                    ps.proposeseq(),
                    " prev=",
                    prev_hash_str.str().substr(0, 16),
                    "... OLD_HASH=",
                    it->second.substr(0, 16),
                    "... NEW_HASH=",
                    hash_str.str().substr(0, 16),
                    "...");
            }
        }
        else
        {
            seen_proposals_[key] = hash_str.str();
            PLOGI(
                proposal_log(),
                "NEW seq=",
                ps.proposeseq(),
                " validator=",
                validator_key.substr(0, 12),
                "... tx=",
                hash_str.str().substr(0, 12),
                "...");
        }
    }

    // Record proposal in dashboard if we have a valid validator key
    if (dashboard_ && !validator_key.empty())
    {
        dashboard_->record_proposal(
            prev_hash_str.str(),
            hash_str.str(),
            validator_key,
            ps.proposeseq(),
            peer_id,
            has_commitment,
            has_reveal);
    }

    bool is_empty_set =
        (hash_str.str() ==
         "0000000000000000000000000000000000000000000000000000000000000000");

    // ONE acquirer per txset hash globally
    // Start acquiring if we don't have an acquirer AND don't already have the
    // data
    bool should_acquire = dashboard_ && !is_empty_set &&
        txset_acquirers_.find(hash_str.str()) == txset_acquirers_.end() &&
        !dashboard_->get_txset(hash_str.str());  // Not already acquired

    if (should_acquire)
    {
        // Notify dashboard of acquisition start (pass prev_hash for ledger_seq
        // inference)
        dashboard_->record_txset_start(hash_str.str(), prev_hash_str.str());

        // Build connection pool
        std::vector<peer_connection*> conn_pool;
        if (connection)
            conn_pool.push_back(connection.get());
        for (auto& [pid, weak_conn] : active_connections_)
        {
            if (pid == peer_id)
                continue;
            auto conn = weak_conn.lock();
            if (conn && conn->is_connected())
                conn_pool.push_back(conn.get());
        }

        PLOGI(
            proposal_log(),
            "📋 Acquiring txset hash=",
            hash_str.str().substr(0, 16),
            "... (",
            conn_pool.size(),
            " peers)");

        auto acquirer = std::make_shared<TransactionSetAcquirer>(
            hash_str.str(),
            conn_pool,
            [this, set_hash = hash_str.str()](
                std::string const&, Slice const& tx_data) {
                if (tx_data.size() >= 3 && dashboard_)
                {
                    auto* data = tx_data.data();
                    if (data[0] == 0x12)
                    {
                        uint16_t tx_type =
                            (static_cast<uint16_t>(data[1]) << 8) |
                            static_cast<uint16_t>(data[2]);
                        dashboard_->record_txset_transaction(set_hash, tx_type);

                        // For Shuffle (88), extract extra data
                        if (tx_type == 88)
                        {
                            auto* proto = dashboard_->get_protocol();
                            if (proto)
                            {
                                try
                                {
                                    auto json =
                                        xdata::json::parse_txset_transaction(
                                            tx_data, *proto);
                                    if (json.is_object())
                                    {
                                        auto& obj = json.as_object();
                                        uint32_t ls = 0;
                                        std::string ph;
                                        if (auto* v = obj.if_contains(
                                                "LedgerSequence"))
                                        {
                                            if (v->is_uint64())
                                                ls = static_cast<uint32_t>(
                                                    v->as_uint64());
                                            else if (v->is_int64())
                                                ls = static_cast<uint32_t>(
                                                    v->as_int64());
                                        }
                                        if (auto* v =
                                                obj.if_contains("ParentHash"))
                                        {
                                            if (v->is_string())
                                                ph = v->as_string();
                                        }
                                        dashboard_->record_txset_shuffle_data(
                                            set_hash, ls, ph);
                                    }
                                }
                                catch (...)
                                {
                                }
                            }
                        }
                        return;
                    }
                    dashboard_->record_txset_transaction(set_hash, 0);
                }
            },
            [this, set_hash = hash_str.str()](
                bool success,
                size_t,
                size_t peers_used,
                size_t unique_requested,
                size_t unique_received,
                std::string const& computed_hash) {
                if (dashboard_)
                {
                    dashboard_->record_txset_peers(set_hash, peers_used);
                    dashboard_->record_txset_unique_counts(
                        set_hash, unique_requested, unique_received);
                    if (!computed_hash.empty())
                        dashboard_->record_txset_computed_hash(
                            set_hash, computed_hash);
                    if (!success)
                        dashboard_->record_txset_error(set_hash);
                    dashboard_->record_txset_complete(set_hash, success);
                }
                txset_acquirers_.erase(set_hash);
            },
            nullptr);

        if (dashboard_)
        {
            acquirer->set_request_callback(
                [this, set_hash = hash_str.str()](size_t num_nodes) {
                    dashboard_->record_txset_request(set_hash, num_nodes);
                });
        }

        acquirer->start();
        txset_acquirers_[hash_str.str()] = std::move(acquirer);
    }

    if (dashboard_ && ps.proposeseq() > 0)
    {
        dashboard_->update_ledger_info(
            ps.proposeseq() - 1, prev_hash_str.str(), 1);
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
packet_processor::handle_validation(
    std::string const& peer_id,
    std::vector<std::uint8_t> const& payload)
{
    if (!dashboard_)
        return;

    // Parse the TMValidation protobuf wrapper
    protocol::TMValidation val;
    if (!val.ParseFromArray(payload.data(), payload.size()))
        return;

    auto const& validation_bytes = val.validation();
    auto const* data =
        reinterpret_cast<const uint8_t*>(validation_bytes.data());
    size_t size = validation_bytes.size();

    if (size < 10)
        return;  // Too small to be valid

    // Parse STObject fields from validation
    // Field codes:
    //   LedgerSequence: type=2 (UInt32), field=6 -> 0x26 (single byte when
    //   field<16) LedgerHash: type=5 (Hash256), field=1 -> 0x51 SigningPubKey:
    //   type=7 (Blob), field=3 -> 0x73

    uint32_t ledger_seq = 0;
    std::string ledger_hash;
    std::string signing_key_hex;

    for (size_t i = 0; i < size - 4;)
    {
        uint8_t type_field = data[i];
        uint8_t type_code = (type_field >> 4) & 0x0F;
        uint8_t field_code = type_field & 0x0F;

        // Handle extended type/field encoding
        size_t header_len = 1;
        if (type_code == 0 && i + 1 < size)
        {
            type_code = data[i + 1];
            header_len = 2;
        }
        if (field_code == 0 && i + header_len < size)
        {
            field_code = data[i + header_len];
            header_len++;
        }

        i += header_len;
        if (i >= size)
            break;

        // LedgerSequence: type=2, field=6 (0x26)
        if (type_code == 2 && field_code == 6 && i + 4 <= size)
        {
            ledger_seq = (static_cast<uint32_t>(data[i]) << 24) |
                (static_cast<uint32_t>(data[i + 1]) << 16) |
                (static_cast<uint32_t>(data[i + 2]) << 8) |
                static_cast<uint32_t>(data[i + 3]);
            i += 4;
            continue;
        }

        // LedgerHash: type=5, field=1 (0x51)
        if (type_code == 5 && field_code == 1 && i + 32 <= size)
        {
            std::stringstream ss;
            for (size_t j = 0; j < 32; ++j)
            {
                ss << std::hex << std::setw(2) << std::setfill('0')
                   << static_cast<int>(data[i + j]);
            }
            ledger_hash = ss.str();
            i += 32;
            continue;
        }

        // SigningPubKey: type=7, field=3 (0x73) - VL encoded
        if (type_code == 7 && field_code == 3 && i < size)
        {
            // Read VL length
            uint8_t len_byte = data[i];
            size_t vl_len = 0;
            size_t len_bytes = 1;

            if (len_byte <= 192)
            {
                vl_len = len_byte;
            }
            else if (len_byte <= 240 && i + 1 < size)
            {
                vl_len = 193 + ((len_byte - 193) * 256) + data[i + 1];
                len_bytes = 2;
            }
            else if (len_byte <= 254 && i + 2 < size)
            {
                vl_len = 12481 + ((len_byte - 241) * 65536) +
                    (data[i + 1] * 256) + data[i + 2];
                len_bytes = 3;
            }

            i += len_bytes;
            if (i + vl_len <= size && vl_len == 33)
            {
                std::stringstream ss;
                for (size_t j = 0; j < vl_len; ++j)
                {
                    ss << std::hex << std::setw(2) << std::setfill('0')
                       << static_cast<int>(data[i + j]);
                }
                signing_key_hex = ss.str();
                i += vl_len;
                continue;
            }
            i += vl_len;
            continue;
        }

        // Skip other fields based on type
        switch (type_code)
        {
            case 1:
                i += 2;
                break;  // UInt16
            case 2:
                i += 4;
                break;  // UInt32
            case 3:
                i += 8;
                break;  // UInt64
            case 4:
                i += 16;
                break;  // Hash128
            case 5:
                i += 32;
                break;  // Hash256
            case 6:
                i += 8;
                break;  // Amount (simplified)
            case 7:
            case 8: {  // Blob, AccountID - VL encoded
                if (i < size)
                {
                    uint8_t lb = data[i];
                    size_t vl = 0, lbytes = 1;
                    if (lb <= 192)
                        vl = lb;
                    else if (lb <= 240 && i + 1 < size)
                    {
                        vl = 193 + ((lb - 193) * 256) + data[i + 1];
                        lbytes = 2;
                    }
                    i += lbytes + vl;
                }
                break;
            }
            default:
                // Unknown type - try to skip 1 byte and continue
                i += 1;
                break;
        }
    }

    // Must have at least ledger sequence and signing key
    if (ledger_seq == 0 || signing_key_hex.empty())
        return;

    // Resolve ephemeral key to master key via manifest tracker
    std::string validator_key = signing_key_hex;
    auto master_key = manifest_tracker_.get_master_key(signing_key_hex);
    if (master_key)
    {
        validator_key = *master_key;
    }

    // Record the validation in the dashboard
    dashboard_->record_validation(
        ledger_seq, ledger_hash, validator_key, signing_key_hex, peer_id);
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

std::shared_ptr<peer_connection>
packet_processor::get_connection(std::string const& peer_id)
{
    auto it = active_connections_.find(peer_id);
    if (it == active_connections_.end())
        return nullptr;
    auto conn = it->second.lock();
    if (!conn || !conn->is_connected())
    {
        active_connections_.erase(it);
        return nullptr;
    }
    return conn;
}

std::vector<std::pair<std::string, std::shared_ptr<peer_connection>>>
packet_processor::get_connections_for_acquisition(
    std::string const& exclude_peer_id,
    size_t max_count)
{
    std::vector<std::pair<std::string, std::shared_ptr<peer_connection>>>
        result;

    for (auto it = active_connections_.begin();
         it != active_connections_.end() && result.size() < max_count;)
    {
        if (it->first == exclude_peer_id)
        {
            ++it;
            continue;
        }

        auto conn = it->second.lock();
        if (!conn || !conn->is_connected())
        {
            // Remove stale entry
            it = active_connections_.erase(it);
            continue;
        }

        result.emplace_back(it->first, conn);
        ++it;
    }

    return result;
}

}  // namespace catl::peer::monitor