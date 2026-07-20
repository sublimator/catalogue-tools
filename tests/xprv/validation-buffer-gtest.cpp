#include "xprv/proof-chain-json.h"
#include "xprv/validation-buffer.h"
#include "xprv/validation-collector.h"

#include <catl/core/base64.h>
#include <catl/peer-client/connection-types.h>
#include <catl/xdata/protocol.h>

#include "ripple.pb.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <fstream>
#include <future>
#include <optional>
#include <sstream>
#include <string>
#include <thread>

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
        packet.push_back(static_cast<uint8_t>((length & 0x7F) | 0x80));
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

xprv::ValidationBuffer::Stats
drain_validation_ingress(
    asio::io_context& io,
    std::shared_ptr<xprv::ValidationBuffer> const& buffer,
    std::size_t expected_packets)
{
    xprv::ValidationBuffer::Stats final_stats;
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto const deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(5);
            do
            {
                final_stats = co_await buffer->co_stats();
                auto const accounted =
                    final_stats.validation_packets_processed +
                    final_stats.validation_packets_deduplicated +
                    final_stats.validation_packets_dropped;
                if (final_stats.validation_packets_received ==
                        expected_packets &&
                    accounted == expected_packets &&
                    final_stats.queued_validations == 0)
                {
                    break;
                }

                asio::steady_timer timer(ex, std::chrono::milliseconds(1));
                co_await timer.async_wait(asio::use_awaitable);
            } while (std::chrono::steady_clock::now() < deadline);

            buffer->stop();
            io.stop();
        },
        asio::detached);
    io.run();
    return final_stats;
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
            auto result =
                co_await buffer->co_wait_best_quorum(std::chrono::seconds(0));
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
                        buffer->co_wait_best_quorum(std::chrono::seconds(30)),
                        asio::deferred),
                    asio::co_spawn(
                        ex,
                        []() -> asio::awaitable<void> {
                            auto ex = co_await asio::this_coro::executor;
                            asio::steady_timer timer(
                                ex, std::chrono::milliseconds(10));
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

TEST(ValidationBuffer, ListChangeCancelsMismatchedProofWaiter)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationBuffer::create(io, protocol, 0);
    buffer->set_unl({}, 1);
    io.run();
    io.restart();

    bool done = false;
    xprv::QuorumCollectStopReason stop_reason =
        xprv::QuorumCollectStopReason::timeout;

    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto result = co_await buffer->co_wait_best_quorum(
                std::chrono::seconds(30), 1);
            stop_reason = result.stop_reason;
            done = true;
        },
        asio::detached);

    asio::co_spawn(
        io,
        [buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            while ((co_await buffer->co_stats()).pending_callbacks == 0)
                co_await asio::post(ex, asio::use_awaitable);
            buffer->set_unl({}, 2);
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_EQ(
        stop_reason, xprv::QuorumCollectStopReason::validator_list_changed);
}

TEST(ValidationBuffer, BestQuorumCollectsAndFinalizesAtFullQuorum)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationBuffer::create(io, protocol, 0);
    auto const anchor = load_anchor_fixture();
    ASSERT_FALSE(anchor.validations.empty());

    auto const fixture = prepare_quorum_fixture(protocol, anchor.validations);
    ASSERT_FALSE(fixture.manifests.empty())
        << "fixture must contain at least one ledger with validations";
    ASSERT_GE(fixture.quorum_packets.size(), fixture.threshold)
        << "fixture must provide a threshold quorum for one ledger";

    buffer->set_unl(fixture.manifests, 1);
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
            auto result =
                co_await buffer->co_wait_best_quorum(std::chrono::seconds(5));
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

TEST(ValidationBuffer, ManifestWorkerReplaysRotationAcrossListRefresh)
{
    asio::io_context io;
    auto const xrpl_protocol = test_protocol();
    auto buffer = xprv::ValidationBuffer::create(io, xrpl_protocol, 0);
    auto const anchor = load_anchor_fixture();

    std::string const blob_text(anchor.blob.begin(), anchor.blob.end());
    auto const blob = boost::json::parse(blob_text).as_object();

    std::optional<catl::vl::Manifest> live;
    std::vector<uint8_t> validation;
    for (auto const& entry : blob.at("validators").as_array())
    {
        auto const raw = catl::base64_decode(
            std::string(entry.as_object().at("manifest").as_string()));
        auto manifest = catl::vl::parse_manifest(raw);
        auto const found =
            anchor.validations.find(lower_ascii(manifest.signing_key_hex()));
        auto validation_it = found;
        if (validation_it == anchor.validations.end())
        {
            validation_it = std::find_if(
                anchor.validations.begin(),
                anchor.validations.end(),
                [&manifest](auto const& item) {
                    return lower_ascii(item.first) ==
                        lower_ascii(manifest.signing_key_hex());
                });
        }
        if (manifest.sequence > 0 && validation_it != anchor.validations.end())
        {
            live = std::move(manifest);
            validation = validation_it->second;
            break;
        }
    }
    ASSERT_TRUE(live.has_value());
    ASSERT_FALSE(validation.empty());

    auto stale = *live;
    stale.signing_public_key.clear();
    --stale.sequence;
    buffer->set_unl({stale}, 1);
    io.run();
    io.restart();

    protocol::TMManifests manifests;
    manifests.add_list()->set_stobject(
        std::string(live->raw.begin(), live->raw.end()));
    std::string manifest_bytes;
    ASSERT_TRUE(manifests.SerializeToString(&manifest_bytes));

    buffer->on_packet(
        static_cast<uint16_t>(catl::peer_client::packet_type::manifests),
        std::vector<uint8_t>(manifest_bytes.begin(), manifest_bytes.end()));
    // Queue a newer list snapshot behind manifest admission but ahead of the
    // worker completion. The verified rotation must be applied against the
    // current trust set rather than discarded merely because the list changed.
    buffer->set_unl({stale}, 2);
    buffer->on_packet(
        static_cast<uint16_t>(catl::peer_client::packet_type::validation),
        wrap_validation(validation));

    bool observed = false;
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto const deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (std::chrono::steady_clock::now() < deadline)
            {
                if (co_await buffer->co_has_live_only_quorum())
                {
                    observed = true;
                    break;
                }
                asio::steady_timer timer(ex, std::chrono::milliseconds(1));
                co_await timer.async_wait(asio::use_awaitable);
            }
            buffer->stop();
            io.stop();
        },
        asio::detached);

    io.run();
    EXPECT_TRUE(observed);
}

TEST(ValidationBuffer, ConcurrentDuplicateIngressIsCoalesced)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);
    auto const packet_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);
    std::vector<uint8_t> const packet(52, 0);

    constexpr std::size_t kProducerCount = 8;
    constexpr std::size_t kPacketsPerProducer = 256;
    std::vector<std::thread> producers;
    producers.reserve(kProducerCount);
    for (std::size_t producer = 0; producer < kProducerCount; ++producer)
    {
        producers.emplace_back([buffer, &packet]() {
            for (std::size_t i = 0; i < kPacketsPerProducer; ++i)
                buffer->on_packet(packet_type, packet);
        });
    }
    for (auto& producer : producers)
        producer.join();

    constexpr auto kTotalPackets = kProducerCount * kPacketsPerProducer;
    auto const stats = drain_validation_ingress(io, buffer, kTotalPackets);
    EXPECT_EQ(stats.validation_packets_received, kTotalPackets);
    EXPECT_EQ(stats.validation_packets_processed, 1u);
    EXPECT_EQ(stats.validation_packets_deduplicated, kTotalPackets - 1);
    EXPECT_EQ(stats.validation_packets_dropped, 0u);
    EXPECT_EQ(stats.queued_validations, 0u);
    EXPECT_EQ(stats.queued_validation_bytes, 0u);
}

TEST(ValidationBuffer, ManifestAdmissionPrecedesQuorumPublication)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationBuffer::create(io, protocol, 0);
    auto const anchor = load_anchor_fixture();
    auto const fixture = prepare_quorum_fixture(protocol, anchor.validations);
    ASSERT_FALSE(fixture.manifests.empty());
    ASSERT_FALSE(fixture.quorum_packets.empty());

    buffer->set_unl({fixture.manifests.front()}, 1);
    io.run();
    io.restart();

    std::promise<void> validation_entered_promise;
    auto validation_entered = validation_entered_promise.get_future();
    std::promise<void> validation_release_promise;
    auto validation_release = validation_release_promise.get_future().share();
    std::atomic_bool validation_hook_seen = false;
    buffer->set_validation_commit_hook_for_test([&]() {
        if (!validation_hook_seen.exchange(true))
        {
            validation_entered_promise.set_value();
            validation_release.wait();
        }
    });

    std::promise<void> manifest_entered_promise;
    auto manifest_entered = manifest_entered_promise.get_future();
    std::promise<void> manifest_release_promise;
    auto manifest_release = manifest_release_promise.get_future().share();
    std::atomic_bool manifest_hook_seen = false;
    buffer->set_manifest_apply_hook_for_test([&]() {
        if (!manifest_hook_seen.exchange(true))
        {
            manifest_entered_promise.set_value();
            manifest_release.wait();
        }
    });

    std::promise<void> waiter_ready_promise;
    auto waiter_ready = waiter_ready_promise.get_future();
    std::promise<xprv::QuorumCollectResult> result_promise;
    auto result = result_promise.get_future();
    std::atomic_bool waiter_resolved = false;
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto collected = co_await buffer->co_wait_best_quorum(
                std::chrono::seconds(5), 1);
            waiter_resolved.store(true);
            result_promise.set_value(std::move(collected));
        },
        asio::detached);
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            while ((co_await buffer->co_stats()).pending_callbacks == 0)
                co_await asio::post(ex, asio::use_awaitable);
            waiter_ready_promise.set_value();
        },
        asio::detached);

    auto work = asio::make_work_guard(io);
    std::thread io_thread([&]() { io.run(); });
    auto const timeout = std::chrono::seconds(5);

    bool const waiter_was_ready =
        waiter_ready.wait_for(timeout) == std::future_status::ready;
    if (waiter_was_ready)
    {
        buffer->on_packet(
            static_cast<uint16_t>(catl::peer_client::packet_type::validation),
            fixture.quorum_packets.front());
    }

    bool const validation_was_active = waiter_was_ready &&
        validation_entered.wait_for(timeout) == std::future_status::ready;
    if (validation_was_active)
    {
        buffer->on_packet(
            static_cast<uint16_t>(catl::peer_client::packet_type::manifests),
            {});
    }
    validation_release_promise.set_value();

    bool const manifest_reached_apply = validation_was_active &&
        manifest_entered.wait_for(timeout) == std::future_status::ready;
    bool const resolved_before_manifest_apply = waiter_resolved.load();
    manifest_release_promise.set_value();

    bool const result_was_ready = manifest_reached_apply &&
        result.wait_for(timeout) == std::future_status::ready;
    std::optional<xprv::QuorumCollectResult> collected;
    if (result_was_ready)
        collected = result.get();

    buffer->stop();
    work.reset();
    io.stop();
    io_thread.join();

    EXPECT_TRUE(waiter_was_ready);
    EXPECT_TRUE(validation_was_active);
    EXPECT_TRUE(manifest_reached_apply);
    EXPECT_FALSE(resolved_before_manifest_apply);
    ASSERT_TRUE(collected.has_value());
    EXPECT_EQ(collected->stop_reason, xprv::QuorumCollectStopReason::full);
}

TEST(ValidationBuffer, ValidationIngressEvictsAndReadmitsAtCountLimit)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);
    auto const packet_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);

    constexpr std::size_t kQueueLimit = 1024;
    constexpr std::size_t kTotalPackets = kQueueLimit + 176;
    auto make_packet = [](std::size_t i) {
        std::vector<uint8_t> packet(52, 0);
        packet[48] = static_cast<uint8_t>(i >> 24);
        packet[49] = static_cast<uint8_t>(i >> 16);
        packet[50] = static_cast<uint8_t>(i >> 8);
        packet[51] = static_cast<uint8_t>(i);
        return packet;
    };
    for (std::size_t i = 0; i < kTotalPackets; ++i)
        buffer->on_packet(packet_type, make_packet(i));

    // The first packet was evicted and must no longer poison the dedupe index.
    buffer->on_packet(packet_type, make_packet(0));

    constexpr auto kSubmittedPackets = kTotalPackets + 1;
    auto const stats = drain_validation_ingress(io, buffer, kSubmittedPackets);
    EXPECT_EQ(stats.validation_packets_received, kSubmittedPackets);
    EXPECT_EQ(stats.validation_packets_processed, kQueueLimit);
    EXPECT_EQ(stats.validation_packets_deduplicated, 0u);
    EXPECT_EQ(
        stats.validation_packets_dropped, kSubmittedPackets - kQueueLimit);
    EXPECT_EQ(stats.queued_validations, 0u);
    EXPECT_EQ(stats.queued_validation_bytes, 0u);
}

TEST(ValidationBuffer, ValidationIngressEvictsOldestAtByteLimit)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);
    auto const packet_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);

    constexpr std::size_t kPacketCount = 3;
    for (std::size_t i = 0; i < kPacketCount; ++i)
    {
        std::vector<uint8_t> packet(600 * 1024, 0);
        packet.back() = static_cast<uint8_t>(i);
        buffer->on_packet(packet_type, packet);
    }

    auto const stats = drain_validation_ingress(io, buffer, kPacketCount);
    EXPECT_EQ(stats.validation_packets_received, kPacketCount);
    EXPECT_EQ(stats.validation_packets_processed, 1u);
    EXPECT_EQ(stats.validation_packets_deduplicated, 0u);
    EXPECT_EQ(stats.validation_packets_dropped, kPacketCount - 1);
    EXPECT_EQ(stats.queued_validations, 0u);
    EXPECT_EQ(stats.queued_validation_bytes, 0u);
}

TEST(ValidationBuffer, ManifestAdmissionIsBoundedAndResumesValidations)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);
    auto const manifest_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::manifests);
    auto const validation_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);

    std::vector<uint8_t> const empty_manifest;
    buffer->on_packet(manifest_type, empty_manifest);
    buffer->on_packet(manifest_type, empty_manifest);
    buffer->on_packet(manifest_type, empty_manifest);
    buffer->on_packet(validation_type, std::vector<uint8_t>(52, 0));

    auto const stats = drain_validation_ingress(io, buffer, 1);
    EXPECT_EQ(stats.manifest_packets_received, 3u);
    EXPECT_EQ(stats.manifest_packets_admitted, 2u);
    EXPECT_EQ(stats.manifest_packets_processed, 2u);
    EXPECT_EQ(stats.manifest_packets_dropped, 1u);
    EXPECT_FALSE(stats.manifest_in_flight);
    EXPECT_FALSE(stats.manifest_pending);
    EXPECT_EQ(stats.validation_packets_processed, 1u);
}

TEST(ValidationBuffer, StopCancelsHeartbeatWork)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);
    auto const validation_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);
    buffer->on_packet(validation_type, std::vector<uint8_t>(52, 0));

    bool stopped = false;
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            while ((co_await buffer->co_stats()).validation_packets_processed ==
                   0)
            {
                asio::steady_timer timer(ex, std::chrono::milliseconds(1));
                co_await timer.async_wait(asio::use_awaitable);
            }
            buffer->stop();
            stopped = true;
        },
        asio::detached);

    io.run_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(stopped);
    EXPECT_TRUE(io.stopped());
    io.stop();
}

TEST(ValidationBuffer, StopAccountsQueuedValidationsAsDropped)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);
    auto const validation_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);

    constexpr std::size_t kPacketCount = 32;
    for (std::size_t i = 0; i < kPacketCount; ++i)
    {
        std::vector<uint8_t> packet(52, 0);
        packet.back() = static_cast<uint8_t>(i);
        buffer->on_packet(validation_type, packet);
    }
    buffer->stop();

    xprv::ValidationBuffer::Stats stats;
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            stats = co_await buffer->co_stats();
            io.stop();
        },
        asio::detached);
    io.run();

    EXPECT_EQ(stats.validation_packets_received, kPacketCount);
    EXPECT_EQ(stats.validation_packets_processed, 0u);
    EXPECT_EQ(stats.validation_packets_deduplicated, 0u);
    EXPECT_EQ(stats.validation_packets_dropped, kPacketCount);
    EXPECT_EQ(stats.queued_validations, 0u);
    EXPECT_EQ(stats.queued_validation_bytes, 0u);
}

TEST(ValidationBuffer, StopDuringActiveValidationCleansAdmissionState)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);
    auto const validation_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);

    std::promise<void> entered_promise;
    auto entered = entered_promise.get_future();
    std::promise<void> release_promise;
    auto release = release_promise.get_future().share();
    buffer->set_validation_commit_hook_for_test([&]() {
        entered_promise.set_value();
        release.wait();
    });

    auto work = asio::make_work_guard(io);
    std::thread io_thread([&]() { io.run(); });
    buffer->on_packet(validation_type, std::vector<uint8_t>(52, 0));

    bool const became_active =
        entered.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
    buffer->stop();

    std::promise<xprv::ValidationBuffer::Stats> stats_promise;
    auto stats = stats_promise.get_future();
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            stats_promise.set_value(co_await buffer->co_stats());
        },
        asio::detached);
    release_promise.set_value();

    bool const stats_ready =
        stats.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
    std::optional<xprv::ValidationBuffer::Stats> final_stats;
    if (stats_ready)
        final_stats = stats.get();

    work.reset();
    io.stop();
    io_thread.join();

    EXPECT_TRUE(became_active);
    ASSERT_TRUE(final_stats.has_value());
    EXPECT_EQ(final_stats->validation_packets_received, 1u);
    EXPECT_EQ(final_stats->validation_packets_processed, 1u);
    EXPECT_EQ(final_stats->validation_packets_deduplicated, 0u);
    EXPECT_EQ(final_stats->validation_packets_dropped, 0u);
    EXPECT_EQ(final_stats->queued_validations, 0u);
    EXPECT_EQ(final_stats->queued_validation_bytes, 0u);
}

TEST(ValidationBuffer, StopAccountsAdmittedManifestsAsAbandoned)
{
    asio::io_context io;
    auto buffer = xprv::ValidationBuffer::create(io, test_protocol(), 0);
    auto const manifest_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::manifests);

    constexpr std::size_t kPacketCount = 3;
    for (std::size_t i = 0; i < kPacketCount; ++i)
        buffer->on_packet(manifest_type, {});
    buffer->stop();

    xprv::ValidationBuffer::Stats stats;
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            stats = co_await buffer->co_stats();
            io.stop();
        },
        asio::detached);
    io.run();

    EXPECT_EQ(stats.manifest_packets_received, kPacketCount);
    EXPECT_EQ(stats.manifest_packets_admitted, 2u);
    EXPECT_EQ(stats.manifest_packets_processed, 0u);
    EXPECT_EQ(stats.manifest_packets_dropped, 1u);
    EXPECT_EQ(stats.manifest_packets_abandoned, 2u);
    EXPECT_FALSE(stats.manifest_in_flight);
    EXPECT_FALSE(stats.manifest_pending);
}
