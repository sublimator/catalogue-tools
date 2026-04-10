#include "xprv/validation-buffer.h"
#include "xprv/validation-collector.h"
#include "xprv/proof-chain-json.h"

#include <catl/peer-client/connection-types.h>
#include <catl/xdata/protocol.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <fstream>
#include <sstream>
#include <string>

namespace asio = boost::asio;

namespace {

catl::xdata::Protocol
test_protocol()
{
    return catl::xdata::Protocol::load_embedded_xrpl_protocol();
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

struct QuorumFixture
{
    std::vector<catl::vl::Manifest> manifests;
    std::vector<std::vector<uint8_t>> quorum_packets;
    uint32_t quorum_seq = 0;
    std::size_t threshold = 0;
};

QuorumFixture
prepare_quorum_fixture(
    catl::xdata::Protocol const& protocol,
    std::map<std::string, std::vector<uint8_t>> const& validations)
{
    constexpr uint16_t kValidationType =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);

    xprv::ValidationCollector collector(protocol, 0);
    for (auto const& [_, raw] : validations)
        collector.on_packet(kValidationType, wrap_validation(raw));

    QuorumFixture fixture;
    std::vector<xprv::ValidationCollector::Entry> best_entries;

    for (auto const& [hash, entries] : collector.by_ledger)
    {
        if (entries.empty())
            continue;

        auto const seq = entries.front().ledger_seq;
        auto ledger_validations = collector.get_ledger_validations(
            hash, xprv::ValidationCollector::QuorumMode::live);

        if (!best_entries.empty())
        {
            if (ledger_validations.size() < best_entries.size())
                continue;

            if (ledger_validations.size() == best_entries.size() &&
                seq < fixture.quorum_seq)
            {
                continue;
            }
        }

        fixture.quorum_seq = seq;
        best_entries = std::move(ledger_validations);
    }

    fixture.manifests.reserve(best_entries.size());
    fixture.quorum_packets.reserve(best_entries.size());
    for (auto const& validation : best_entries)
    {
        catl::vl::Manifest manifest;
        manifest.master_public_key = validation.signing_key;
        manifest.signing_public_key = validation.signing_key;
        fixture.manifests.push_back(manifest);
        fixture.quorum_packets.push_back(wrap_validation(validation.raw));
    }
    fixture.threshold =
        (static_cast<int>(fixture.manifests.size()) * 90 + 99) / 100;

    return fixture;
}

}  // namespace

TEST(ValidationBuffer, BestQuorumTimesOutWithNoValidations)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);

    bool done = false;
    xprv::QuorumCollectStopReason stop_reason =
        xprv::QuorumCollectStopReason::full;

    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto result = co_await buffer->co_wait_best_quorum(
                std::chrono::seconds(0));
            stop_reason = result.stop_reason;

            auto stats = co_await buffer->co_stats();
            EXPECT_EQ(stats.pending_callbacks, 0u);
            done = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_EQ(stop_reason, xprv::QuorumCollectStopReason::timeout);
}

TEST(ValidationBuffer, BestQuorumCancelledWaiterIsRemoved)
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
                        buffer->co_wait_best_quorum(
                            std::chrono::seconds(30)),
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
    EXPECT_EQ(stats.pending_callbacks, 0u);
}

TEST(ValidationBuffer, BestQuorumCollectsAndFinalizesAtFullQuorum)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationBuffer::create(io, protocol, 0);
    auto const anchor = load_anchor_fixture();
    ASSERT_FALSE(anchor.validations.empty());

    auto const fixture =
        prepare_quorum_fixture(protocol, anchor.validations);
    ASSERT_FALSE(fixture.manifests.empty())
        << "fixture must contain at least one ledger with validations";
    ASSERT_GE(fixture.quorum_packets.size(), fixture.threshold)
        << "fixture must provide a threshold quorum for one ledger";

    buffer->set_unl(fixture.manifests);
    io.run();
    io.restart();

    bool done = false;
    std::size_t collected_count = 0;
    uint32_t collected_seq = 0;
    xprv::QuorumCollectStopReason stop_reason =
        xprv::QuorumCollectStopReason::timeout;

    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto result = co_await buffer->co_wait_best_quorum(
                std::chrono::seconds(5));
            collected_count = result.quorum.validations.size();
            collected_seq = result.quorum.ledger_seq;
            stop_reason = result.stop_reason;
            done = true;
            io.stop();
        },
        asio::detached);

    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto const packet_type = static_cast<uint16_t>(
                catl::peer_client::packet_type::validation);

            while (true)
            {
                auto stats = co_await buffer->co_stats();
                if (stats.pending_callbacks > 0)
                    break;
                co_await asio::post(ex, asio::use_awaitable);
            }

            for (auto const& packet : fixture.quorum_packets)
                buffer->on_packet(packet_type, packet);
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_EQ(collected_count, fixture.quorum_packets.size());
    EXPECT_EQ(collected_seq, fixture.quorum_seq);
    EXPECT_EQ(stop_reason, xprv::QuorumCollectStopReason::full);
}
