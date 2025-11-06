#include "ripple.pb.h"
#include <algorithm>
#include <catl/common/ledger-info.h>
#include <catl/core/logger.h>
#include <catl/peer/log-keeper/log-keeper.h>
#include <catl/peer/packet-names.h>
#include <iomanip>
#include <sstream>

namespace catl::peer::log_keeper {

// Helper: Convert bytes to hex string
std::string
bytes_to_hex(const unsigned char* data, size_t len)
{
    std::ostringstream oss;
    for (size_t i = 0; i < len; ++i)
        oss << std::hex << std::setw(2) << std::setfill('0') << (int)data[i];
    return oss.str();
}

log_keeper::log_keeper(peer_config config)
    : config_(std::move(config))
    , ssl_context_(
          std::make_unique<asio::ssl::context>(asio::ssl::context::tlsv12))
{
    setup_ssl_context();
}

log_keeper::~log_keeper()
{
    // Ensure cleanup on destruction
    request_stop();

    // Wait a bit for graceful shutdown
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Force stop if needed
    try
    {
        stop();
    }
    catch (...)
    {
        // Ignore exceptions in destructor
    }
}

void
log_keeper::setup_ssl_context()
{
    ssl_context_->set_options(
        asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
        asio::ssl::context::no_sslv3 | asio::ssl::context::single_dh_use);

    // Set verification mode to none (we're not verifying peer certificates)
    ssl_context_->set_verify_mode(asio::ssl::verify_none);

    // Enable ECDH
    SSL_CTX_set_ecdh_auto(ssl_context_->native_handle(), 1);
}

void
log_keeper::run()
{
    {
        std::lock_guard<std::mutex> lock(shutdown_mutex_);
        if (running_)
        {
            LOGE("Log keeper is already running");
            return;
        }
        running_ = true;
        work_guard_ = std::make_unique<
            asio::executor_work_guard<asio::io_context::executor_type>>(
            asio::make_work_guard(io_context_));
    }

    try
    {
        // Connect to peer
        connect_to_peer();

        // Start IO threads
        for (std::size_t i = 0; i < config_.io_threads; ++i)
        {
            io_threads_.emplace_back([this]() {
                try
                {
                    io_context_.run();
                }
                catch (std::exception const& e)
                {
                    LOGE("IO thread exception: ", e.what());
                }
            });
        }

        // Wait for all IO threads to complete
        for (auto& thread : io_threads_)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
        io_threads_.clear();
    }
    catch (std::exception const& e)
    {
        LOGE("Fatal error: ", e.what());
        // Clean up threads even on error
        try
        {
            stop();
        }
        catch (...)
        {
        }
    }
}

void
log_keeper::request_stop()
{
    bool expected = false;
    if (stopping_.compare_exchange_strong(expected, true))
    {
        running_ = false;

        // Reset work guard to allow io_context to finish
        work_guard_.reset();

        // Stop the io_context
        io_context_.stop();
    }
}

void
log_keeper::stop()
{
    // Request stop first
    request_stop();

    // Use a lock to ensure we only join threads once
    std::lock_guard<std::mutex> lock(shutdown_mutex_);

    // Wait for threads
    for (auto& thread : io_threads_)
    {
        if (thread.joinable())
        {
            try
            {
                thread.join();
            }
            catch (std::exception const& e)
            {
                LOGE("Error joining thread: ", e.what());
            }
        }
    }

    // Clear the threads vector
    io_threads_.clear();
}

void
log_keeper::connect_to_peer()
{
    LOGI("Connecting to ", config_.host, ":", config_.port);

    connection_ =
        std::make_shared<peer_connection>(io_context_, *ssl_context_, config_);

    connection_->async_connect([this](boost::system::error_code ec) {
        if (!ec)
        {
            LOGI("Connected successfully");
            handle_connection(connection_);
        }
        else
        {
            LOGE("Connection failed: ", ec.message());
            request_stop();
        }
    });
}

void
log_keeper::handle_connection(std::shared_ptr<peer_connection> connection)
{
    // Start reading packets
    connection->start_read([this](
                               packet_header const& header,
                               std::vector<std::uint8_t> const& payload) {
        handle_packet(header, payload);
    });

    // Send initial ping to test connectivity
    LOGI("Sending initial ping to test connectivity");
    send_ping();

    // Tell the peer we're monitoring
    LOGI("Sending status to peer");
    send_status();
}

void
log_keeper::handle_packet(
    packet_header const& header,
    std::vector<std::uint8_t> const& payload)
{
    auto type = static_cast<packet_type>(header.type);

    // Always log packet info to see what we're getting
    LOGI(
        "Received packet: ",
        get_packet_name(header.type),
        " (",
        header.type,
        ") size=",
        header.payload_size);

    switch (type)
    {
        case packet_type::status_change:
            handle_status_change(payload);
            break;

        case packet_type::transaction:
            handle_transaction(payload);
            break;

        case packet_type::ledger_data:
            LOGI(COLORED(BOLD_CYAN, "*** LEDGER DATA RESPONSE RECEIVED! ***"));
            handle_ledger_data(payload);
            break;

        case packet_type::replay_delta_response:
            handle_replay_delta_response(payload);
            break;

        case packet_type::ping:
            handle_ping(payload);
            break;

        default:
            LOGW(
                COLORED(YELLOW, "Unhandled packet type: "),
                get_packet_name(header.type),
                " (",
                header.type,
                ") - might be relevant!");
            break;
    }
}

void
log_keeper::handle_status_change(std::vector<std::uint8_t> const& payload)
{
    protocol::TMStatusChange status;
    if (!status.ParseFromArray(payload.data(), payload.size()))
    {
        LOGE("Failed to parse TMStatusChange");
        return;
    }

    // Log status details for debugging
    LOGD(
        "Status change details:",
        " has_newstatus=",
        status.has_newstatus(),
        " has_newevent=",
        status.has_newevent(),
        " has_ledgerseq=",
        status.has_ledgerseq());

    if (status.has_newevent())
    {
        std::string event_name;
        switch (status.newevent())
        {
            case protocol::NodeEvent::neCLOSING_LEDGER:
                event_name = "CLOSING_LEDGER";
                break;
            case protocol::NodeEvent::neACCEPTED_LEDGER:
                event_name = "ACCEPTED_LEDGER";
                break;
            case protocol::NodeEvent::neSWITCHED_LEDGER:
                event_name = "SWITCHED_LEDGER";
                break;
            case protocol::NodeEvent::neLOST_SYNC:
                event_name = "LOST_SYNC";
                break;
            default:
                event_name = "UNKNOWN";
                break;
        }
        LOGI("Status event: ", event_name);
    }

    // Create our own status message mirroring the peer's status
    protocol::TMStatusChange mirror_status;

    // Copy all the fields from the received status
    if (status.has_newstatus())
        mirror_status.set_newstatus(status.newstatus());
    if (status.has_newevent())
        mirror_status.set_newevent(status.newevent());
    if (status.has_ledgerseq())
        mirror_status.set_ledgerseq(status.ledgerseq());
    if (status.has_ledgerhash())
        mirror_status.set_ledgerhash(status.ledgerhash());
    if (status.has_ledgerhashprevious())
        mirror_status.set_ledgerhashprevious(status.ledgerhashprevious());
    if (status.has_networktime())
        mirror_status.set_networktime(status.networktime());
    if (status.has_firstseq())
        mirror_status.set_firstseq(status.firstseq());
    if (status.has_lastseq())
        mirror_status.set_lastseq(status.lastseq());

    // Serialize and send the mirrored status
    std::vector<std::uint8_t> mirror_data(mirror_status.ByteSizeLong());
    if (!mirror_status.SerializeToArray(mirror_data.data(), mirror_data.size()))
    {
        LOGE("Failed to serialize mirror status");
        return;
    }

    LOGI("Mirroring status back to peer");
    if (connection_)
    {
        connection_->async_send_packet(
            packet_type::status_change,
            mirror_data,
            [](boost::system::error_code ec) {
                if (ec)
                {
                    LOGE("Failed to mirror status: ", ec.message());
                }
                else
                {
                    LOGI("Mirrored status successfully");
                }
            });
    }

    // Check for accepted ledger event
    if (status.has_newevent() &&
        status.newevent() == protocol::NodeEvent::neACCEPTED_LEDGER &&
        status.has_ledgerhash())
    {
        auto const& hash = status.ledgerhash();
        if (hash.size() == 32)
        {
            std::ranges::copy(hash, current_ledger_hash_.begin());

            if (status.has_ledgerseq())
            {
                current_ledger_seq_ = status.ledgerseq();
            }

            // Convert hash to hex for logging
            std::stringstream hash_hex;
            for (unsigned char c : hash)
            {
                hash_hex << std::hex << std::setw(2) << std::setfill('0')
                         << (int)c;
            }

            LOGI(
                COLORED(BOLD_GREEN, "Ledger accepted:"),
                " seq=",
                current_ledger_seq_,
                " hash=",
                hash_hex.str());

            // Request full ledger data (since replay delta isn't supported)
            request_ledger_data(hash);
        }
    }
    else if (status.has_ledgerseq())
    {
        current_ledger_seq_ = status.ledgerseq();
        LOGD("Current ledger: ", current_ledger_seq_);
    }
}

void
log_keeper::handle_transaction(std::vector<std::uint8_t> const& payload)
{
    protocol::TMTransaction txn;
    if (!txn.ParseFromArray(payload.data(), payload.size()))
    {
        LOGE("Failed to parse TMTransaction");
        return;
    }

    // TODO: Process and store transaction
    LOGD("Received transaction, size=", txn.rawtransaction().size());
}

void
log_keeper::handle_ledger_data(std::vector<std::uint8_t> const& payload)
{
    protocol::TMLedgerData data;
    if (!data.ParseFromArray(payload.data(), payload.size()))
    {
        LOGE("Failed to parse TMLedgerData");
        return;
    }

    // Decode the type for better logging
    std::string type_name;
    switch (data.type())
    {
        case protocol::TMLedgerInfoType::liBASE:
            type_name = "BASE";
            break;
        case protocol::TMLedgerInfoType::liTX_NODE:
            type_name = "TX_NODE";
            break;
        case protocol::TMLedgerInfoType::liAS_NODE:
            type_name = "AS_NODE";
            break;
        case protocol::TMLedgerInfoType::liTS_CANDIDATE:
            type_name = "TS_CANDIDATE";
            break;
        default:
            type_name = "UNKNOWN(" + std::to_string(data.type()) + ")";
            break;
    }

    LOGI(
        COLORED(CYAN, "Ledger data details:"),
        " seq=",
        data.ledgerseq(),
        " nodes=",
        data.nodes_size(),
        " type=",
        type_name,
        " request_cookie=",
        data.has_requestcookie() ? std::to_string(data.requestcookie())
                                 : "none");

    if (data.has_error())
    {
        LOGW("Ledger data error code: ", data.error());
        return;
    }

    if (data.nodes_size() > 0)
    {
        LOGI("Ledger.NodesReceived: ", data.nodes_size());
        for (int i = 0; i < data.nodes_size() && i < 5; i++)
        {
            auto const& node = data.nodes(i);
            LOGI(
                "Ledger.Node[",
                i,
                "].Size: ",
                node.nodedata().size(),
                " bytes",
                node.has_nodeid() ? " (has nodeid)" : " (no nodeid)");

            // For 513-byte nodes, show the last byte (wire type)
            if (node.nodedata().size() == 513)
            {
                unsigned char last_byte = node.nodedata()[512];
                LOGI(
                    "Ledger.Node[",
                    i,
                    "].WireType: 0x",
                    bytes_to_hex(&last_byte, 1),
                    " (decimal: ",
                    (int)last_byte,
                    ")");

                // Show first few bytes of first hash
                LOGI(
                    "Ledger.Node[",
                    i,
                    "].FirstHash: ",
                    bytes_to_hex(
                        reinterpret_cast<const unsigned char*>(
                            node.nodedata().data()),
                        8),
                    "...");
            }
        }

        // For BASE type, the first node should be the ledger header
        if (data.type() == protocol::TMLedgerInfoType::liBASE &&
            data.nodes_size() > 0)
        {
            LOGI(
                COLORED(GREEN, "Got ledger header! Size: "),
                data.nodes(0).nodedata().size(),
                " bytes");

            // Parse the ledger header
            auto const& node_data = data.nodes(0).nodedata();
            if (node_data.size() == 118)
            {
                // Print raw hex for debugging
                LOGI("");
                LOGI(
                    "Ledger.HeaderHex32: ",
                    bytes_to_hex(
                        reinterpret_cast<const unsigned char*>(
                            node_data.data()),
                        32));

                // Pass the actual size so LedgerInfoView knows there's no hash
                catl::common::LedgerInfoView ledger_view(
                    reinterpret_cast<const uint8_t*>(node_data.data()),
                    node_data.size());

                LOGI(COLORED(BOLD_GREEN, "Parsed Ledger Header:"));
                LOGI("Expected seq from TMLedgerData: ", data.ledgerseq());
                LOGI(
                    "Ledger.Sequence: ",
                    ledger_view.seq(),
                    " (hex: 0x",
                    bytes_to_hex(
                        reinterpret_cast<const unsigned char*>(&node_data[0]),
                        4),
                    ")");
                if (auto h = ledger_view.hash())
                {
                    LOGI("Ledger.Hash: ", h->hex());
                }
                else
                {
                    LOGI("Ledger.Hash: <not present>");
                }
                LOGI("Ledger.Parent Hash: ", ledger_view.parent_hash().hex());
                LOGI("Ledger.Close Time: ", ledger_view.close_time());
                LOGI("Ledger.Drops: ", ledger_view.drops());

                // Optionally print full details
                LOGD("Full ledger info:\n", ledger_view.to_string());
            }
            else
            {
                LOGW(
                    "Unexpected ledger header size: ",
                    node_data.size(),
                    " (expected ",
                    sizeof(catl::common::LedgerInfo),
                    ")");
            }
        }
    }

    // TODO: Process and store ledger nodes
}

void
log_keeper::handle_replay_delta_response(
    std::vector<std::uint8_t> const& payload)
{
    protocol::TMReplayDeltaResponse response;
    if (!response.ParseFromArray(payload.data(), payload.size()))
    {
        LOGE("Failed to parse TMReplayDeltaResponse");
        return;
    }

    if (response.has_error())
    {
        LOGE(
            "Replay delta error: ",
            static_cast<int>(response.error()),
            " (",
            response.error() == protocol::TMReplyError::reNO_LEDGER
                ? "NO_LEDGER"
                : response.error() == protocol::TMReplyError::reNO_NODE
                    ? "NO_NODE"
                    : response.error() == protocol::TMReplyError::reBAD_REQUEST
                        ? "BAD_REQUEST"
                        : "UNKNOWN",
            ")");
        return;
    }

    LOGI(
        "Received replay delta response: ",
        response.transaction_size(),
        " transactions");

    // Process each transaction
    for (auto const& txn_bytes : response.transaction())
    {
        // TODO: Parse and store transaction
        LOGD("Transaction size: ", txn_bytes.size());
    }

    if (response.has_ledgerheader())
    {
        LOGD("Ledger header size: ", response.ledgerheader().size());
        // TODO: Parse and store ledger header
    }
}

void
log_keeper::request_ledger_transactions(std::string const& ledger_hash)
{
    LOGD("Preparing to request transactions for ledger hash");

    protocol::TMReplayDeltaRequest request;
    request.set_ledgerhash(ledger_hash);

    // Serialize the request
    std::vector<std::uint8_t> request_data(request.ByteSizeLong());
    if (!request.SerializeToArray(request_data.data(), request_data.size()))
    {
        LOGE("Failed to serialize TMReplayDeltaRequest");
        return;
    }

    LOGD("Serialized replay delta request, size=", request_data.size());

    // Send the request
    if (connection_)
    {
        connection_->async_send_packet(
            packet_type::replay_delta_req,
            request_data,
            [](boost::system::error_code ec) {
                if (ec)
                {
                    LOGE("Failed to send replay delta request: ", ec.message());
                }
                else
                {
                    LOGI("Sent replay delta request successfully");
                }
            });
    }
    else
    {
        LOGE("Cannot send replay delta request - no connection");
    }
}

void
log_keeper::handle_ping(std::vector<std::uint8_t> const& payload)
{
    protocol::TMPing ping;
    if (!ping.ParseFromArray(payload.data(), payload.size()))
    {
        LOGE("Failed to parse TMPing");
        return;
    }

    if (ping.type() == protocol::TMPing_pingType_ptPING)
    {
        LOGI("Received PING, sending PONG");

        // Send pong response
        ping.set_type(protocol::TMPing_pingType_ptPONG);
        std::vector<std::uint8_t> pong_data(ping.ByteSizeLong());
        ping.SerializeToArray(pong_data.data(), pong_data.size());

        if (connection_)
        {
            connection_->async_send_packet(
                packet_type::ping, pong_data, [](boost::system::error_code ec) {
                    if (ec)
                    {
                        LOGE("Failed to send PONG: ", ec.message());
                    }
                    else
                    {
                        LOGI("Sent PONG successfully");
                    }
                });
        }
    }
    else
    {
        LOGI("Received PONG - connection is working!");
    }
}

void
log_keeper::send_ping()
{
    protocol::TMPing ping;
    ping.set_type(protocol::TMPing_pingType_ptPING);
    ping.set_seq(1);  // Simple sequence number

    std::vector<std::uint8_t> ping_data(ping.ByteSizeLong());
    if (!ping.SerializeToArray(ping_data.data(), ping_data.size()))
    {
        LOGE("Failed to serialize ping");
        return;
    }

    if (connection_)
    {
        connection_->async_send_packet(
            packet_type::ping, ping_data, [](boost::system::error_code ec) {
                if (ec)
                {
                    LOGE("Failed to send PING: ", ec.message());
                }
                else
                {
                    LOGI("Sent PING successfully");
                }
            });
    }
    else
    {
        LOGE("Cannot send ping - no connection");
    }
}

void
log_keeper::send_status()
{
    // Send initial monitoring status
    protocol::TMStatusChange status;
    status.set_newstatus(protocol::NodeStatus::nsMONITORING);

    std::vector<std::uint8_t> status_data(status.ByteSizeLong());
    if (!status.SerializeToArray(status_data.data(), status_data.size()))
    {
        LOGE("Failed to serialize status");
        return;
    }

    LOGI("Sending initial monitoring status");

    if (connection_)
    {
        connection_->async_send_packet(
            packet_type::status_change,
            status_data,
            [](boost::system::error_code ec) {
                if (ec)
                {
                    LOGE("Failed to send status: ", ec.message());
                }
                else
                {
                    LOGI("Sent status successfully");
                }
            });
    }
    else
    {
        LOGE("Cannot send status - no connection");
    }
}

void
log_keeper::request_ledger_data(std::string const& ledger_hash)
{
    // Generate unique cookie for this request
    auto cookie = request_cookie_.fetch_add(1);

    LOGI(
        "Requesting TMGetLedger with cookie=",
        cookie,
        " for ledger seq=",
        current_ledger_seq_);
    // Note: Currently using sequence number instead of hash
    (void)ledger_hash;  // Suppress unused parameter warning

    protocol::TMGetLedger request;
    request.set_itype(protocol::TMLedgerInfoType::liBASE);  // Just basic info
    // Don't set ltype - let it default
    // Use sequence number instead of hash
    // request.set_ledgerseq(current_ledger_seq_);
    request.set_ledgerhash(ledger_hash);
    request.set_requestcookie(cookie);
    // Don't set querytype or querydepth

    // Serialize the request
    std::vector<std::uint8_t> request_data(request.ByteSizeLong());
    if (!request.SerializeToArray(request_data.data(), request_data.size()))
    {
        LOGE("Failed to serialize TMGetLedger");
        return;
    }

    LOGD("Serialized TMGetLedger request, size=", request_data.size());

    // Send the request
    if (connection_)
    {
        connection_->async_send_packet(
            packet_type::get_ledger,
            request_data,
            [cookie](boost::system::error_code ec) {
                if (ec)
                {
                    LOGE(
                        "Failed to send TMGetLedger (cookie=",
                        cookie,
                        "): ",
                        ec.message());
                }
                else
                {
                    LOGI(
                        "Sent TMGetLedger request successfully (cookie=",
                        cookie,
                        ")");
                }
            });
    }
    else
    {
        LOGE("Cannot send TMGetLedger - no connection");
    }
}

}  // namespace catl::peer::log_keeper