#include "xprv/validation-buffer.h"
#include "xprv/proof-chain-json.h"

#include <catl/core/base64.h>
#include <catl/peer-client/connection-types.h>
#include <catl/xdata/protocol.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/json.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <cctype>
#include <fstream>
#include <set>
#include <sstream>
#include <string>

namespace asio = boost::asio;

namespace {

catl::xdata::Protocol
test_protocol()
{
    return catl::xdata::Protocol::load_embedded_xrpl_protocol();
}

std::string
lower_ascii(std::string value)
{
    for (auto& ch : value)
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    return value;
}

std::vector<uint8_t>
wrap_validation(std::vector<uint8_t> const& stvalidation)
{
    std::vector<uint8_t> packet;
    packet.push_back(0x0A);

    std::size_t length = stvalidation.size();
    while (length >= 0x80)
    {
        packet.push_back(
            static_cast<uint8_t>((length & 0x7F) | 0x80));
        length >>= 7;
    }
    packet.push_back(static_cast<uint8_t>(length));

    packet.insert(packet.end(), stvalidation.begin(), stvalidation.end());
    return packet;
}

xprv::AnchorData
load_anchor_fixture()
{
    std::string const path =
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/proof.json";
    std::ifstream input(path);
    if (!input.is_open())
        throw std::runtime_error("proof.json not found at " + path);

    std::stringstream buffer;
    buffer << input.rdbuf();
    auto const chain = xprv::from_json(boost::json::parse(buffer.str()));
    if (chain.steps.empty() ||
        !std::holds_alternative<xprv::AnchorData>(chain.steps.front()))
    {
        throw std::runtime_error("proof.json does not start with an anchor");
    }
    return std::get<xprv::AnchorData>(chain.steps.front());
}

}  // namespace

TEST(ValidationBuffer, TimedOutWaiterIsRemoved)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);

    bool done = false;
    bool timed_out = false;
    std::string error;
    xprv::ValidationBuffer::Stats stats;

    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            bool local_timed_out = false;
            std::string local_error;
            try
            {
                (void)co_await buffer->co_wait_quorum(std::chrono::seconds(0));
            }
            catch (std::exception const& e)
            {
                local_timed_out = true;
                local_error = std::string(e.what());
            }

            stats = co_await buffer->co_stats();
            timed_out = local_timed_out;
            error = std::move(local_error);
            done = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(timed_out);
    EXPECT_NE(error.find("Timed out waiting for validation quorum"), std::string::npos);
    EXPECT_EQ(stats.waiters, 0u);
}

TEST(ValidationBuffer, CancelledWaiterIsRemoved)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);

    bool done = false;
    bool cancel_branch_won = false;
    xprv::ValidationBuffer::Stats stats;

    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto result =
                co_await asio::experimental::make_parallel_group(
                    asio::co_spawn(
                        ex,
                        buffer->co_wait_quorum(std::chrono::seconds(30)),
                        asio::deferred),
                    asio::co_spawn(
                        ex,
                        []() -> asio::awaitable<void> {
                            auto ex = co_await asio::this_coro::executor;
                            asio::steady_timer timer(ex, std::chrono::milliseconds(10));
                            boost::system::error_code ec;
                            co_await timer.async_wait(
                                asio::redirect_error(asio::use_awaitable, ec));
                        },
                        asio::deferred))
                    .async_wait(
                        asio::experimental::wait_for_one(),
                        asio::use_awaitable);

            cancel_branch_won = (std::get<0>(result)[0] == 1);

            // Give the canceled waiter branch one turn to run its cleanup.
            asio::steady_timer drain(ex, std::chrono::milliseconds(1));
            boost::system::error_code drain_ec;
            co_await drain.async_wait(
                asio::redirect_error(asio::use_awaitable, drain_ec));

            stats = co_await buffer->co_stats();
            done = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(cancel_branch_won);
    EXPECT_EQ(stats.waiters, 0u);
}

TEST(ValidationBuffer, LingerCollectsAdditionalValidationsForChosenLedger)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);
    auto const anchor = load_anchor_fixture();

    std::string blob_json(anchor.blob.begin(), anchor.blob.end());
    auto const blob = boost::json::parse(blob_json).as_object();
    auto const& validators_json = blob.at("validators").as_array();

    std::set<std::string> validation_keys;
    for (auto const& [key, _] : anchor.validations)
        validation_keys.insert(lower_ascii(key));

    std::vector<catl::vl::Manifest> manifests;
    for (auto const& entry : validators_json)
    {
        auto const& obj = entry.as_object();
        auto manifest_bytes = catl::base64_decode(
            std::string(obj.at("manifest").as_string()));
        auto manifest = catl::vl::parse_manifest(manifest_bytes);
        if (!manifest.signing_public_key.empty() &&
            validation_keys.count(lower_ascii(manifest.signing_key_hex())) > 0)
            manifests.push_back(std::move(manifest));
    }

    ASSERT_FALSE(manifests.empty());
    ASSERT_FALSE(anchor.validations.empty());

    std::vector<std::vector<uint8_t>> raw_validations;
    raw_validations.reserve(anchor.validations.size());
    for (auto const& [_, raw] : anchor.validations)
        raw_validations.push_back(raw);

    auto const threshold =
        (static_cast<int>(manifests.size()) * 90 + 99) / 100;
    auto const selected_count =
        std::min<int>(static_cast<int>(raw_validations.size()), threshold + 2);

    ASSERT_GT(selected_count, threshold)
        << "fixture needs more validations than the 90% threshold";

    buffer->set_unl(manifests);

    bool done = false;
    std::size_t initial_count = 0;
    std::size_t expanded_count = 0;

    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto initial = co_await buffer->co_wait_quorum(
                std::chrono::seconds(5),
                xprv::ValidationCollector::QuorumMode::proof);
            initial_count = initial.validations.size();

            auto expanded = co_await buffer->co_collect_quorum_validations(
                initial.ledger_hash,
                initial.ledger_seq,
                std::chrono::milliseconds(500),
                xprv::ValidationCollector::QuorumMode::proof);
            expanded_count = expanded.quorum.validations.size();
            done = true;
            io.stop();
        },
        asio::detached);

    for (int i = 0; i < threshold; ++i)
    {
        auto packet = wrap_validation(raw_validations[i]);
        buffer->on_packet(
            static_cast<uint16_t>(catl::peer_client::packet_type::validation),
            packet);
    }

    asio::steady_timer extra_timer(io, std::chrono::milliseconds(250));
    extra_timer.async_wait([&, buffer](boost::system::error_code ec) {
        if (ec)
            return;
        for (int i = threshold; i < selected_count; ++i)
        {
            auto packet = wrap_validation(raw_validations[i]);
            buffer->on_packet(
                static_cast<uint16_t>(
                    catl::peer_client::packet_type::validation),
                packet);
        }
    });

    io.run();

    EXPECT_TRUE(done);
    EXPECT_EQ(initial_count, static_cast<std::size_t>(threshold));
    EXPECT_EQ(expanded_count, static_cast<std::size_t>(selected_count));
}
