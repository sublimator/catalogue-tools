#include "xprv/proof-chain-json.h"
#include "xprv/proof-resolver.h"
#include "xprv/validation-collector.h"
#include "xprv/validation-quorum-tracker.h"

#include <catl/core/base64.h>
#include <catl/peer-client/connection-types.h>
#include <catl/xdata/protocol.h>

#include "../test-support/io-context-runners.h"

#include "ripple.pb.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <fstream>
#include <future>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
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

xprv::ValidationQuorumTracker::Stats
drain_validation_ingress(
    asio::io_context& io,
    std::shared_ptr<xprv::ValidationQuorumTracker> const& buffer,
    std::size_t expected_packets)
{
    xprv::ValidationQuorumTracker::Stats final_stats;
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

TEST(ValidationQuorumTracker, BestQuorumTimesOutWithNoValidations)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);

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

TEST(ValidationQuorumTracker, BestQuorumCancelledWaiterIsRemoved)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);

    bool done = false;
    bool cancel_branch_won = false;
    xprv::ValidationQuorumTracker::Stats stats;

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

TEST(ValidationQuorumTracker, BestQuorumCleanupStaysOnOwnerStrand)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto caller_strand = asio::make_strand(io);
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
    std::atomic<bool> resumed_on_owner{false};
    std::atomic<bool> returned_on_caller{false};
    std::promise<void> done;
    auto done_future = done.get_future();

    buffer->set_quorum_wait_resume_hook_for_test(
        [&resumed_on_owner](bool on_owner) {
            resumed_on_owner.store(on_owner, std::memory_order_release);
        });

    asio::co_spawn(
        caller_strand,
        [buffer, caller_strand, &returned_on_caller, &done]()
            -> asio::awaitable<void> {
            try
            {
                (void)co_await buffer->co_wait_best_quorum(30s);
                returned_on_caller.store(
                    caller_strand.running_in_this_thread(),
                    std::memory_order_release);
                done.set_value();
            }
            catch (...)
            {
                done.set_exception(std::current_exception());
            }
        },
        asio::detached);

    asio::co_spawn(
        io,
        [buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            while ((co_await buffer->co_stats()).pending_callbacks == 0)
                co_await asio::post(ex, asio::use_awaitable);
            buffer->stop();
        },
        asio::detached);

    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });

    auto const status = done_future.wait_for(2s);
    EXPECT_EQ(status, std::future_status::ready);
    if (status == std::future_status::ready)
    {
        try
        {
            done_future.get();
        }
        catch (std::exception const& e)
        {
            ADD_FAILURE() << "quorum waiter failed: " << e.what();
        }
        catch (...)
        {
            ADD_FAILURE() << "quorum waiter failed with unknown exception";
        }
    }

    EXPECT_TRUE(resumed_on_owner.load(std::memory_order_acquire));
    EXPECT_TRUE(returned_on_caller.load(std::memory_order_acquire));

    work.reset();
    io.stop();
    first_runner.join();
    second_runner.join();
}

TEST(ValidationQuorumTracker, TransientOwnerHopStillUsesOwnerCoroutine)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
    std::atomic<bool> entered_during_owner_dispatch{false};
    std::atomic<bool> cleanup_on_owner{false};
    std::promise<void> done;
    auto done_future = done.get_future();

    buffer->set_quorum_wait_resume_hook_for_test(
        [&cleanup_on_owner](bool on_owner) {
            cleanup_on_owner.store(on_owner, std::memory_order_release);
        });

    asio::co_spawn(
        io,
        [buffer,
         &entered_during_owner_dispatch,
         &done]() -> asio::awaitable<void> {
            try
            {
                // The outer coroutine was born on raw io, but calls the public
                // method during a one-time owner-strand dispatch. Dynamic
                // running_in_this_thread() must not admit it as owner-born.
                co_await asio::post(
                    buffer->owner_strand_for_test(), asio::use_awaitable);
                entered_during_owner_dispatch.store(
                    buffer->owner_strand_for_test().running_in_this_thread(),
                    std::memory_order_release);

                (void)co_await buffer->co_wait_best_quorum(30s);
                done.set_value();
            }
            catch (...)
            {
                done.set_exception(std::current_exception());
            }
        },
        asio::detached);

    asio::co_spawn(
        io,
        [buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            while ((co_await buffer->co_stats()).pending_callbacks == 0)
                co_await asio::post(ex, asio::use_awaitable);
            buffer->stop();
        },
        asio::detached);

    std::thread first_runner([&]() { io.run(); });
    std::thread second_runner([&]() { io.run(); });

    auto const status = done_future.wait_for(2s);
    EXPECT_EQ(status, std::future_status::ready);
    if (status == std::future_status::ready)
        EXPECT_NO_THROW(done_future.get());
    EXPECT_TRUE(entered_during_owner_dispatch.load(std::memory_order_acquire));
    EXPECT_TRUE(cleanup_on_owner.load(std::memory_order_acquire));

    work.reset();
    io.stop();
    first_runner.join();
    second_runner.join();
}

TEST(ValidationQuorumTracker, PendingIdentityExhaustionRefusesBeforeMutation)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto caller_strand = asio::make_strand(io);
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
    bool saw_overflow = false;
    bool returned_on_caller = false;
    std::size_t pending_after = 1;
    std::uint64_t identity_after = 0;

    asio::co_spawn(
        caller_strand,
        [&, buffer, caller_strand]() -> asio::awaitable<void> {
            co_await asio::co_spawn(
                buffer->owner_strand_for_test(),
                [buffer]() -> asio::awaitable<void> {
                    buffer->set_next_pending_id_for_test(
                        std::numeric_limits<std::uint64_t>::max());
                    co_return;
                },
                asio::use_awaitable);

            try
            {
                (void)co_await buffer->co_wait_best_quorum(30s);
            }
            catch (std::overflow_error const&)
            {
                saw_overflow = true;
            }

            pending_after = (co_await buffer->co_stats()).pending_callbacks;
            identity_after = co_await asio::co_spawn(
                buffer->owner_strand_for_test(),
                [buffer]() -> asio::awaitable<std::uint64_t> {
                    co_return buffer->next_pending_id_for_test();
                },
                asio::use_awaitable);
            returned_on_caller = caller_strand.running_in_this_thread();
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(saw_overflow);
    EXPECT_EQ(pending_after, 0u);
    EXPECT_EQ(identity_after, std::numeric_limits<std::uint64_t>::max());
    EXPECT_TRUE(returned_on_caller);
}

TEST(ValidationQuorumTracker, DeadlineArmFailureRollsBackExactPendingRow)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto caller_strand = asio::make_strand(io);
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
    bool saw_failure = false;
    bool returned_on_caller = false;
    std::size_t pending_after = 1;

    buffer->set_quorum_deadline_arm_hook_for_test(
        []() { throw std::runtime_error("forced deadline arm failure"); });

    asio::co_spawn(
        caller_strand,
        [&, buffer, caller_strand]() -> asio::awaitable<void> {
            try
            {
                (void)co_await buffer->co_wait_best_quorum(30s);
            }
            catch (std::runtime_error const&)
            {
                saw_failure = true;
            }
            pending_after = (co_await buffer->co_stats()).pending_callbacks;
            returned_on_caller = caller_strand.running_in_this_thread();
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(saw_failure);
    EXPECT_EQ(pending_after, 0u);
    EXPECT_TRUE(returned_on_caller);
}

TEST(ValidationQuorumTracker, ListChangeCancelsMismatchedProofWaiter)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationQuorumTracker::create(io, protocol, 0);
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

TEST(ValidationQuorumTracker, BestQuorumCollectsAndFinalizesAtFullQuorum)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationQuorumTracker::create(io, protocol, 0);
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

TEST(ValidationQuorumTracker, LockedQuorumSurvivesVoteRetention)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);

    auto make_key = [](uint8_t marker) {
        std::vector<uint8_t> key(33, 0);
        key.front() = 0x02;
        key.back() = marker;
        return key;
    };
    std::vector<catl::vl::Manifest> manifests;
    for (uint8_t marker = 1; marker <= 10; ++marker)
    {
        catl::vl::Manifest manifest;
        manifest.master_public_key = make_key(marker);
        manifest.signing_public_key = make_key(marker);
        manifests.push_back(std::move(manifest));
    }
    buffer->set_unl(manifests, 1);
    io.run();
    io.restart();

    auto make_hash = [](uint8_t marker) {
        std::array<uint8_t, 32> bytes{};
        bytes.back() = marker;
        return Hash256(bytes);
    };
    auto make_entry = [&](Hash256 const& hash,
                          uint32_t sequence,
                          catl::vl::Manifest const& manifest) {
        xprv::ValidationCollector::Entry entry;
        entry.ledger_hash = hash;
        entry.ledger_seq = sequence;
        entry.signing_key_hex = lower_ascii(manifest.signing_key_hex());
        return entry;
    };

    auto const quorum_hash = make_hash(1);
    bool done = false;
    bool all_inserted = true;
    xprv::QuorumCollectResult collected;

    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            collected = co_await buffer->co_wait_best_quorum(
                std::chrono::seconds(5), 1);
            done = true;
            buffer->stop();
            io.stop();
        },
        asio::detached);

    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            while ((co_await buffer->co_stats()).pending_callbacks == 0)
                co_await asio::post(ex, asio::use_awaitable);

            // Fill one validator's retention allowance, then let its vote
            // establish the threshold quorum selected by the waiter.
            for (uint8_t marker = 10; marker <= 12; ++marker)
            {
                auto const inserted =
                    co_await buffer->co_insert_validation_for_test(
                        make_entry(make_hash(marker), 100, manifests.back()));
                all_inserted = inserted && all_inserted;
            }
            for (std::size_t i = 0; i < 8; ++i)
            {
                auto const inserted =
                    co_await buffer->co_insert_validation_for_test(
                        make_entry(quorum_hash, 100, manifests[i]));
                all_inserted = inserted && all_inserted;
            }
            auto inserted = co_await buffer->co_insert_validation_for_test(
                make_entry(quorum_hash, 100, manifests.back()));
            all_inserted = inserted && all_inserted;

            // This newer vote evicts the validator's retained quorum vote.
            // The waiter must still return the threshold-backed snapshot it
            // already locked, not the collector's reduced live view.
            inserted = co_await buffer->co_insert_validation_for_test(
                make_entry(make_hash(2), 101, manifests.back()));
            all_inserted = inserted && all_inserted;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(all_inserted);
    EXPECT_EQ(
        collected.stop_reason, xprv::QuorumCollectStopReason::next_ledger);
    ASSERT_EQ(collected.quorum.validations.size(), 9u);
    EXPECT_TRUE(std::all_of(
        collected.quorum.validations.begin(),
        collected.quorum.validations.end(),
        [&](auto const& entry) {
            return entry.ledger_hash == quorum_hash && entry.ledger_seq == 100;
        }));
}

TEST(ValidationQuorumTracker, CoveredProofQuorumRespectsTransportBoundary)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);

    auto make_key = [](uint8_t marker) {
        std::vector<uint8_t> key(33, 0);
        key.front() = 0x02;
        key.back() = marker;
        return key;
    };
    std::vector<catl::vl::Manifest> manifests;
    for (uint8_t marker = 1; marker <= 10; ++marker)
    {
        catl::vl::Manifest manifest;
        manifest.master_public_key = make_key(marker);
        manifest.signing_public_key = make_key(marker);
        manifests.push_back(std::move(manifest));
    }
    buffer->set_unl(manifests, 7);
    io.run();
    io.restart();

    auto make_hash = [](uint8_t marker) {
        std::array<uint8_t, 32> bytes{};
        bytes.back() = marker;
        return Hash256(bytes);
    };
    auto make_entry = [&](Hash256 const& hash,
                          uint32_t sequence,
                          catl::vl::Manifest const& manifest) {
        xprv::ValidationCollector::Entry entry;
        entry.ledger_hash = hash;
        entry.ledger_seq = sequence;
        entry.signing_key_hex = lower_ascii(manifest.signing_key_hex());
        return entry;
    };

    bool done = false;
    bool all_inserted = true;
    std::optional<xprv::QuorumEntry> below_both;
    std::optional<xprv::QuorumEntry> covered_older;
    std::optional<xprv::QuorumEntry> covered_newer;
    std::optional<xprv::QuorumEntry> wrong_vl;
    auto const older_hash = make_hash(1);
    auto const newer_hash = make_hash(2);

    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            for (std::size_t i = 0; i < 9; ++i)
            {
                all_inserted =
                    (co_await buffer->co_insert_validation_for_test(
                        make_entry(older_hash, 100, manifests[i]))) &&
                    all_inserted;
                all_inserted =
                    (co_await buffer->co_insert_validation_for_test(
                        make_entry(newer_hash, 110, manifests[i]))) &&
                    all_inserted;
            }

            below_both = co_await buffer->co_latest_quorum_at_or_before(99, 7);
            covered_older =
                co_await buffer->co_latest_quorum_at_or_before(105, 7);
            covered_newer =
                co_await buffer->co_latest_quorum_at_or_before(110, 7);
            wrong_vl = co_await buffer->co_latest_quorum_at_or_before(110, 8);
            done = true;
            buffer->stop();
            io.stop();
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_TRUE(all_inserted);
    EXPECT_FALSE(below_both.has_value());
    ASSERT_TRUE(covered_older.has_value());
    EXPECT_EQ(covered_older->ledger_hash, older_hash);
    EXPECT_EQ(covered_older->ledger_seq, 100u);
    EXPECT_EQ(covered_older->validations.size(), 9u);
    ASSERT_TRUE(covered_newer.has_value());
    EXPECT_EQ(covered_newer->ledger_hash, newer_hash);
    EXPECT_EQ(covered_newer->ledger_seq, 110u);
    EXPECT_EQ(covered_newer->validations.size(), 9u);
    EXPECT_FALSE(wrong_vl.has_value());
}

TEST(ValidationQuorumTracker, ManifestWorkerReplaysRotationAcrossListRefresh)
{
    asio::io_context io;
    auto const xrpl_protocol = test_protocol();
    auto buffer = xprv::ValidationQuorumTracker::create(io, xrpl_protocol, 0);
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

TEST(ValidationQuorumTracker, ConcurrentDuplicateIngressIsCoalesced)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
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

TEST(ValidationQuorumTracker, ManifestAdmissionPrecedesQuorumPublication)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationQuorumTracker::create(io, protocol, 0);
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

TEST(ValidationQuorumTracker, FailedManifestFlushPreservesActiveListWaiter)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationQuorumTracker::create(io, protocol, 0);
    auto const fixture =
        prepare_quorum_fixture(protocol, load_anchor_fixture().validations);
    ASSERT_GE(fixture.manifests.size(), 2u);
    ASSERT_FALSE(fixture.quorum_packets.empty());

    buffer->set_unl({fixture.manifests[1]}, 1);
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

    std::promise<void> parse_entered_promise;
    auto parse_entered = parse_entered_promise.get_future();
    std::promise<void> parse_release_promise;
    auto parse_release = parse_release_promise.get_future().share();
    std::atomic_bool parse_hook_seen = false;
    buffer->set_manifest_parse_hook_for_test([&]() {
        if (!parse_hook_seen.exchange(true))
        {
            parse_entered_promise.set_value();
            parse_release.wait();
        }
        throw std::runtime_error("injected manifest parse failure");
    });

    auto const validation_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);
    auto const manifest_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::manifests);
    auto work = asio::make_work_guard(io);
    std::thread io_thread([&]() { io.run(); });
    buffer->on_packet(validation_type, fixture.quorum_packets.front());

    auto const timeout = std::chrono::seconds(5);
    bool const validation_was_active =
        validation_entered.wait_for(timeout) == std::future_status::ready;
    if (validation_was_active)
    {
        buffer->set_unl({fixture.manifests.front()}, 2);
        buffer->on_packet(manifest_type, {});
    }
    validation_release_promise.set_value();

    bool const parse_was_active = validation_was_active &&
        parse_entered.wait_for(timeout) == std::future_status::ready;

    std::promise<void> waiter_ready_promise;
    auto waiter_ready = waiter_ready_promise.get_future();
    std::promise<xprv::QuorumCollectResult> result_promise;
    auto result = result_promise.get_future();
    std::promise<xprv::QuorumCollectResult> unbound_result_promise;
    auto unbound_result = unbound_result_promise.get_future();
    std::atomic_uint waiters_resolved = 0;
    if (parse_was_active)
    {
        asio::co_spawn(
            io,
            [&, buffer]() -> asio::awaitable<void> {
                auto collected = co_await buffer->co_wait_best_quorum(
                    std::chrono::seconds(5), 2);
                ++waiters_resolved;
                result_promise.set_value(std::move(collected));
            },
            asio::detached);
        asio::co_spawn(
            io,
            [&, buffer]() -> asio::awaitable<void> {
                auto collected = co_await buffer->co_wait_best_quorum(
                    std::chrono::seconds(5));
                ++waiters_resolved;
                unbound_result_promise.set_value(std::move(collected));
            },
            asio::detached);
        asio::co_spawn(
            io,
            [&, buffer]() -> asio::awaitable<void> {
                auto ex = co_await asio::this_coro::executor;
                while ((co_await buffer->co_stats()).pending_callbacks < 2)
                    co_await asio::post(ex, asio::use_awaitable);
                waiter_ready_promise.set_value();
            },
            asio::detached);
    }

    bool const waiter_was_ready = parse_was_active &&
        waiter_ready.wait_for(timeout) == std::future_status::ready;
    if (waiter_was_ready)
        buffer->on_packet(validation_type, fixture.quorum_packets.front());
    auto const resolved_before_manifest_completion = waiters_resolved.load();
    parse_release_promise.set_value();

    bool const result_was_ready = waiter_was_ready &&
        result.wait_for(timeout) == std::future_status::ready;
    bool const unbound_result_was_ready = waiter_was_ready &&
        unbound_result.wait_for(timeout) == std::future_status::ready;
    std::optional<xprv::QuorumCollectResult> collected;
    std::optional<xprv::QuorumCollectResult> unbound_collected;
    if (result_was_ready)
        collected = result.get();
    if (unbound_result_was_ready)
        unbound_collected = unbound_result.get();

    buffer->stop();
    work.reset();
    io.stop();
    io_thread.join();

    EXPECT_TRUE(validation_was_active);
    EXPECT_TRUE(parse_was_active);
    EXPECT_TRUE(waiter_was_ready);
    EXPECT_EQ(resolved_before_manifest_completion, 0u);
    ASSERT_TRUE(collected.has_value());
    EXPECT_EQ(collected->stop_reason, xprv::QuorumCollectStopReason::full);
    EXPECT_EQ(collected->quorum.vl_sequence, 2u);
    EXPECT_EQ(collected->quorum.validations.size(), 1u);
    ASSERT_TRUE(unbound_collected.has_value());
    EXPECT_EQ(
        unbound_collected->stop_reason, xprv::QuorumCollectStopReason::full);
    EXPECT_EQ(unbound_collected->quorum.vl_sequence, 2u);
    EXPECT_EQ(unbound_collected->quorum.validations.size(), 1u);
}

TEST(ValidationQuorumTracker, PublicationGateExcludesLateManifestAdmission)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationQuorumTracker::create(io, protocol, 0);
    auto const fixture =
        prepare_quorum_fixture(protocol, load_anchor_fixture().validations);
    ASSERT_FALSE(fixture.manifests.empty());
    ASSERT_FALSE(fixture.quorum_packets.empty());

    buffer->set_unl({fixture.manifests.front()}, 1);
    io.run();
    io.restart();

    std::promise<void> publish_entered_promise;
    auto publish_entered = publish_entered_promise.get_future();
    std::promise<void> publish_release_promise;
    auto publish_release = publish_release_promise.get_future().share();
    buffer->set_validation_publish_hook_for_test([&]() {
        publish_entered_promise.set_value();
        publish_release.wait();
    });

    auto work = asio::make_work_guard(io);
    std::thread io_thread([&]() { io.run(); });
    buffer->on_packet(
        static_cast<uint16_t>(catl::peer_client::packet_type::validation),
        fixture.quorum_packets.front());

    auto const timeout = std::chrono::seconds(5);
    bool const reached_publish_boundary =
        publish_entered.wait_for(timeout) == std::future_status::ready;
    bool const gate_was_held = reached_publish_boundary &&
        !buffer->publication_gate_available_for_test();

    std::promise<void> manifest_started_promise;
    auto manifest_started = manifest_started_promise.get_future();
    std::atomic_bool manifest_returned = false;
    std::thread manifest_thread;
    if (reached_publish_boundary)
    {
        manifest_thread = std::thread([&]() {
            manifest_started_promise.set_value();
            buffer->on_packet(
                static_cast<uint16_t>(
                    catl::peer_client::packet_type::manifests),
                {});
            manifest_returned.store(true);
        });
    }
    bool const manifest_call_started = reached_publish_boundary &&
        manifest_started.wait_for(timeout) == std::future_status::ready;
    bool const manifest_returned_before_release = manifest_returned.load();

    publish_release_promise.set_value();
    if (manifest_thread.joinable())
        manifest_thread.join();
    bool const manifest_returned_after_release = manifest_returned.load();

    buffer->stop();
    work.reset();
    io.stop();
    io_thread.join();

    EXPECT_TRUE(reached_publish_boundary);
    EXPECT_TRUE(gate_was_held);
    EXPECT_TRUE(manifest_call_started);
    EXPECT_FALSE(manifest_returned_before_release);
    EXPECT_TRUE(manifest_returned_after_release);
}

TEST(ValidationQuorumTracker, PublicationGateExcludesLateStop)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationQuorumTracker::create(io, protocol, 0);
    auto const fixture =
        prepare_quorum_fixture(protocol, load_anchor_fixture().validations);
    ASSERT_FALSE(fixture.manifests.empty());
    ASSERT_FALSE(fixture.quorum_packets.empty());

    buffer->set_unl({fixture.manifests.front()}, 1);
    io.run();
    io.restart();

    std::promise<void> publish_entered_promise;
    auto publish_entered = publish_entered_promise.get_future();
    std::promise<void> publish_release_promise;
    auto publish_release = publish_release_promise.get_future().share();
    buffer->set_validation_publish_hook_for_test([&]() {
        publish_entered_promise.set_value();
        publish_release.wait();
    });

    auto work = asio::make_work_guard(io);
    std::thread io_thread([&]() { io.run(); });
    buffer->on_packet(
        static_cast<uint16_t>(catl::peer_client::packet_type::validation),
        fixture.quorum_packets.front());

    auto const timeout = std::chrono::seconds(5);
    bool const reached_publish_boundary =
        publish_entered.wait_for(timeout) == std::future_status::ready;
    bool const gate_was_held = reached_publish_boundary &&
        !buffer->publication_gate_available_for_test();

    std::promise<void> stop_started_promise;
    auto stop_started = stop_started_promise.get_future();
    std::atomic_bool stop_returned = false;
    std::thread stop_thread;
    if (reached_publish_boundary)
    {
        stop_thread = std::thread([&]() {
            stop_started_promise.set_value();
            buffer->stop();
            stop_returned.store(true);
        });
    }
    bool const stop_call_started = reached_publish_boundary &&
        stop_started.wait_for(timeout) == std::future_status::ready;
    bool const stop_returned_before_release = stop_returned.load();

    publish_release_promise.set_value();
    if (stop_thread.joinable())
        stop_thread.join();
    else
        buffer->stop();
    bool const stop_returned_after_release = stop_returned.load();

    work.reset();
    io.stop();
    io_thread.join();

    EXPECT_TRUE(reached_publish_boundary);
    EXPECT_TRUE(gate_was_held);
    EXPECT_TRUE(stop_call_started);
    EXPECT_FALSE(stop_returned_before_release);
    EXPECT_TRUE(stop_returned_after_release);
}

TEST(ValidationQuorumTracker, DeadlinePublicationGateExcludesLateStop)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);

    std::promise<void> publish_entered_promise;
    auto publish_entered = publish_entered_promise.get_future();
    std::promise<void> publish_release_promise;
    auto publish_release = publish_release_promise.get_future().share();
    buffer->set_deadline_publish_hook_for_test([&]() {
        publish_entered_promise.set_value();
        publish_release.wait();
    });

    std::promise<xprv::QuorumCollectResult> result_promise;
    auto result = result_promise.get_future();
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            result_promise.set_value(
                co_await buffer->co_wait_best_quorum(std::chrono::seconds(0)));
        },
        asio::detached);

    auto work = asio::make_work_guard(io);
    std::thread io_thread([&]() { io.run(); });
    auto const timeout = std::chrono::seconds(5);
    bool const reached_publish_boundary =
        publish_entered.wait_for(timeout) == std::future_status::ready;

    std::promise<void> stop_started_promise;
    auto stop_started = stop_started_promise.get_future();
    std::atomic_bool stop_returned = false;
    std::thread stop_thread;
    if (reached_publish_boundary)
    {
        stop_thread = std::thread([&]() {
            stop_started_promise.set_value();
            buffer->stop();
            stop_returned.store(true);
        });
    }
    bool const stop_call_started = reached_publish_boundary &&
        stop_started.wait_for(timeout) == std::future_status::ready;
    bool const stop_returned_before_release = stop_returned.load();

    publish_release_promise.set_value();
    if (stop_thread.joinable())
        stop_thread.join();
    else
        buffer->stop();
    bool const result_was_ready =
        result.wait_for(timeout) == std::future_status::ready;
    std::optional<xprv::QuorumCollectResult> collected;
    if (result_was_ready)
        collected = result.get();

    work.reset();
    io.stop();
    io_thread.join();

    EXPECT_TRUE(reached_publish_boundary);
    EXPECT_TRUE(stop_call_started);
    EXPECT_FALSE(stop_returned_before_release);
    EXPECT_TRUE(stop_returned.load());
    ASSERT_TRUE(collected.has_value());
    EXPECT_EQ(collected->stop_reason, xprv::QuorumCollectStopReason::timeout);
}

TEST(ValidationQuorumTracker, ValidatorListAdmissionAdvancesValidationEpoch)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationQuorumTracker::create(io, protocol, 0);
    auto const fixture =
        prepare_quorum_fixture(protocol, load_anchor_fixture().validations);
    ASSERT_GE(fixture.manifests.size(), 2u);
    ASSERT_GE(fixture.quorum_packets.size(), 2u);

    buffer->set_unl({fixture.manifests[1]}, 1);
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

    auto const validation_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);
    auto work = asio::make_work_guard(io);
    std::thread io_thread([&]() { io.run(); });
    buffer->on_packet(validation_type, fixture.quorum_packets.front());

    auto const timeout = std::chrono::seconds(5);
    bool const validation_was_active =
        validation_entered.wait_for(timeout) == std::future_status::ready;
    if (validation_was_active)
    {
        buffer->set_unl({fixture.manifests.front()}, 2);
        buffer->on_packet(validation_type, fixture.quorum_packets.front());
    }
    validation_release_promise.set_value();

    xprv::ValidationQuorumTracker::Stats stats;
    std::optional<xprv::QuorumEntry> quorum;
    std::promise<void> observed_promise;
    auto observed = observed_promise.get_future();
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto const deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(5);
            do
            {
                stats = co_await buffer->co_stats();
                quorum = co_await buffer->co_latest_quorum();
                if (stats.validation_packets_processed == 2 && quorum)
                    break;
                asio::steady_timer timer(ex, std::chrono::milliseconds(1));
                co_await timer.async_wait(asio::use_awaitable);
            } while (std::chrono::steady_clock::now() < deadline);
            observed_promise.set_value();
        },
        asio::detached);
    bool const observation_completed =
        observed.wait_for(timeout + std::chrono::seconds(1)) ==
        std::future_status::ready;

    buffer->stop();
    work.reset();
    io.stop();
    io_thread.join();

    EXPECT_TRUE(validation_was_active);
    EXPECT_TRUE(observation_completed);
    EXPECT_EQ(stats.validation_packets_received, 2u);
    EXPECT_EQ(stats.validation_packets_processed, 2u);
    EXPECT_EQ(stats.validation_packets_deduplicated, 0u);
    EXPECT_EQ(stats.validation_packets_dropped, 0u);
    ASSERT_TRUE(quorum.has_value());
    EXPECT_EQ(quorum->ledger_seq, fixture.quorum_seq);
}

TEST(ValidationQuorumTracker, ValidatorListAdmissionBlocksNewEpochDrain)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationQuorumTracker::create(io, protocol, 0);
    auto const fixture =
        prepare_quorum_fixture(protocol, load_anchor_fixture().validations);
    ASSERT_GE(fixture.manifests.size(), 2u);
    ASSERT_FALSE(fixture.quorum_packets.empty());

    buffer->set_unl({fixture.manifests[1]}, 1);
    io.run();
    io.restart();

    std::promise<void> admission_entered_promise;
    auto admission_entered = admission_entered_promise.get_future();
    std::promise<void> admission_release_promise;
    auto admission_release = admission_release_promise.get_future().share();
    buffer->set_unl_admission_hook_for_test([&]() {
        admission_entered_promise.set_value();
        admission_release.wait();
    });

    std::promise<void> validation_entered_promise;
    auto validation_entered = validation_entered_promise.get_future();
    std::atomic_bool validation_hook_seen = false;
    buffer->set_validation_commit_hook_for_test([&]() {
        if (!validation_hook_seen.exchange(true))
            validation_entered_promise.set_value();
    });

    auto work = asio::make_work_guard(io);
    std::thread io_thread([&]() { io.run(); });
    std::thread update_thread(
        [&]() { buffer->set_unl({fixture.manifests.front()}, 2); });

    auto const timeout = std::chrono::seconds(5);
    bool const update_was_admitted =
        admission_entered.wait_for(timeout) == std::future_status::ready;
    if (update_was_admitted)
    {
        buffer->on_packet(
            static_cast<uint16_t>(catl::peer_client::packet_type::validation),
            fixture.quorum_packets.front());
    }
    bool const drained_before_apply =
        validation_entered.wait_for(std::chrono::milliseconds(100)) ==
        std::future_status::ready;

    admission_release_promise.set_value();
    update_thread.join();
    bool const drained_after_apply =
        validation_entered.wait_for(timeout) == std::future_status::ready;

    xprv::ValidationQuorumTracker::Stats stats;
    std::optional<xprv::QuorumEntry> quorum;
    std::promise<void> observed_promise;
    auto observed = observed_promise.get_future();
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            auto const deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(5);
            do
            {
                stats = co_await buffer->co_stats();
                quorum = co_await buffer->co_latest_quorum();
                if (stats.validation_packets_processed == 1 && quorum)
                    break;
                asio::steady_timer timer(ex, std::chrono::milliseconds(1));
                co_await timer.async_wait(asio::use_awaitable);
            } while (std::chrono::steady_clock::now() < deadline);
            observed_promise.set_value();
        },
        asio::detached);
    bool const observation_completed =
        observed.wait_for(timeout + std::chrono::seconds(1)) ==
        std::future_status::ready;

    buffer->stop();
    work.reset();
    io.stop();
    io_thread.join();

    EXPECT_TRUE(update_was_admitted);
    EXPECT_FALSE(drained_before_apply);
    EXPECT_TRUE(drained_after_apply);
    EXPECT_TRUE(observation_completed);
    EXPECT_EQ(stats.validation_packets_received, 1u);
    EXPECT_EQ(stats.validation_packets_processed, 1u);
    ASSERT_TRUE(quorum.has_value());
    EXPECT_EQ(quorum->ledger_seq, fixture.quorum_seq);
    EXPECT_EQ(quorum->vl_sequence, 2u);
}

TEST(ValidationQuorumTracker, ValidationBeforeListIsNotRetained)
{
    asio::io_context io;
    auto const protocol = test_protocol();
    auto buffer = xprv::ValidationQuorumTracker::create(io, protocol, 0);
    auto const fixture =
        prepare_quorum_fixture(protocol, load_anchor_fixture().validations);
    ASSERT_FALSE(fixture.quorum_packets.empty());

    buffer->on_packet(
        static_cast<uint16_t>(catl::peer_client::packet_type::validation),
        fixture.quorum_packets.front());
    auto const stats = drain_validation_ingress(io, buffer, 1);

    EXPECT_EQ(stats.validation_packets_processed, 1u);
    EXPECT_EQ(stats.collector_validations, 0u);
    EXPECT_EQ(stats.recent_quorums, 0u);
}

TEST(ValidationQuorumTracker, ValidationIngressEvictsAndReadmitsAtCountLimit)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
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

TEST(ValidationQuorumTracker, ValidationIngressEvictsOldestAtByteLimit)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
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

TEST(ValidationQuorumTracker, ManifestAdmissionIsBoundedAndResumesValidations)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
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

TEST(ValidationQuorumTracker, StopCancelsHeartbeatWork)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
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

TEST(ValidationQuorumTracker, CoStopConsumesPendingWaiterBeforeReceipt)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);

    bool done = false;
    std::size_t pending_at_receipt = 1;
    xprv::QuorumCollectStopReason stop_reason =
        xprv::QuorumCollectStopReason::full;
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto result =
                co_await buffer->co_wait_best_quorum(std::chrono::seconds(30));
            stop_reason = result.stop_reason;
            done = true;
        },
        asio::detached);
    asio::co_spawn(
        io,
        [buffer, &pending_at_receipt]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            while ((co_await buffer->co_stats()).pending_callbacks == 0)
                co_await asio::post(ex, asio::use_awaitable);
            co_await buffer->co_stop();
            pending_at_receipt =
                (co_await buffer->co_stats()).pending_callbacks;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_EQ(stop_reason, xprv::QuorumCollectStopReason::timeout);
    EXPECT_EQ(pending_at_receipt, 0u);
}

TEST(ValidationQuorumTracker, CoStopCannotOvertakeQueuedOwnerWork)
{
    using namespace std::chrono_literals;

    for (auto const runner_count : {2u, 4u})
    {
        SCOPED_TRACE(testing::Message() << "runners=" << runner_count);

        asio::io_context io;
        auto work = asio::make_work_guard(io);
        auto buffer =
            xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);

        std::promise<void> owner_entered;
        std::promise<void> release_owner;
        auto release_owner_future = release_owner.get_future().share();
        asio::post(buffer->owner_strand_for_test(), [&]() {
            owner_entered.set_value();
            release_owner_future.wait();
        });

        std::promise<void> stop_entered;
        auto stopped = asio::co_spawn(
            io,
            [buffer, &stop_entered]() -> asio::awaitable<void> {
                stop_entered.set_value();
                co_await buffer->co_stop();
            },
            asio::use_future);

        catl::test::IoContextRunners runners(io, runner_count);
        auto const owner_status = owner_entered.get_future().wait_for(5s);
        auto const stop_status = stop_entered.get_future().wait_for(5s);
        EXPECT_EQ(owner_status, std::future_status::ready);
        EXPECT_EQ(stop_status, std::future_status::ready);

        if (owner_status == std::future_status::ready &&
            stop_status == std::future_status::ready)
        {
            EXPECT_EQ(stopped.wait_for(50ms), std::future_status::timeout);
        }

        release_owner.set_value();
        auto const stopped_status = stopped.wait_for(5s);
        EXPECT_EQ(stopped_status, std::future_status::ready);
        if (stopped_status == std::future_status::ready)
            EXPECT_NO_THROW(stopped.get());

        work.reset();
        runners.join();
    }
}

TEST(ValidationQuorumTracker, WaitAfterStopReturnsImmediately)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
    buffer->stop();

    bool done = false;
    xprv::QuorumCollectStopReason stop_reason =
        xprv::QuorumCollectStopReason::full;
    asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            auto result =
                co_await buffer->co_wait_best_quorum(std::chrono::seconds(30));
            stop_reason = result.stop_reason;
            done = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(done);
    EXPECT_EQ(stop_reason, xprv::QuorumCollectStopReason::timeout);
}

TEST(ValidationQuorumTracker, StopAccountsQueuedValidationsAsDropped)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
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

    xprv::ValidationQuorumTracker::Stats stats;
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

TEST(ValidationQuorumTracker, StopDuringActiveValidationCleansAdmissionState)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
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

    std::promise<xprv::ValidationQuorumTracker::Stats> stats_promise;
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
    std::optional<xprv::ValidationQuorumTracker::Stats> final_stats;
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

TEST(ValidationQuorumTracker, StopAccountsAdmittedManifestsAsAbandoned)
{
    asio::io_context io;
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
    auto const manifest_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::manifests);

    constexpr std::size_t kPacketCount = 3;
    for (std::size_t i = 0; i < kPacketCount; ++i)
        buffer->on_packet(manifest_type, {});
    buffer->stop();

    xprv::ValidationQuorumTracker::Stats stats;
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

TEST(ValidationQuorumTracker, HeartbeatEffectWinsBeforeAdmissionSeal)
{
    using namespace std::chrono_literals;

    for (auto const runner_count : {2u, 4u})
    {
        SCOPED_TRACE(testing::Message() << "runners=" << runner_count);

        asio::io_context io;
        auto work = asio::make_work_guard(io);
        auto buffer =
            xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);

        std::promise<void> publication_entered;
        auto entered = publication_entered.get_future();
        std::promise<void> release_publication;
        auto release = release_publication.get_future().share();
        std::atomic<unsigned> effects{0};
        buffer->set_inside_heartbeat_publication_hook_for_test([&]() {
            publication_entered.set_value();
            release.wait();
        });
        buffer->set_heartbeat_effect_hook_for_test(
            [&]() { effects.fetch_add(1, std::memory_order_relaxed); });

        buffer->post_heartbeat_tick_for_test();
        catl::test::IoContextRunners runners(io, runner_count);
        auto const entered_status = entered.wait_for(5s);

        std::future<void> sealed;
        if (entered_status == std::future_status::ready)
        {
            sealed = std::async(std::launch::async, [buffer]() {
                buffer->seal_admission_and_join_worker();
            });
            EXPECT_EQ(sealed.wait_for(50ms), std::future_status::timeout);
        }

        release_publication.set_value();
        auto const sealed_status =
            sealed.valid() ? sealed.wait_for(5s) : std::future_status::deferred;
        if (sealed.valid() && sealed_status == std::future_status::ready)
            EXPECT_NO_THROW(sealed.get());

        work.reset();
        io.stop();
        runners.join();

        EXPECT_EQ(entered_status, std::future_status::ready);
        EXPECT_EQ(sealed_status, std::future_status::ready);
        EXPECT_EQ(effects.load(std::memory_order_relaxed), 1u);
    }
}

TEST(ValidationQuorumTracker, AdmissionSealWinsBeforeHeartbeatEffect)
{
    using namespace std::chrono_literals;

    for (auto const runner_count : {2u, 4u})
    {
        SCOPED_TRACE(testing::Message() << "runners=" << runner_count);

        asio::io_context io;
        auto work = asio::make_work_guard(io);
        auto buffer =
            xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);

        std::promise<void> before_publication;
        auto entered = before_publication.get_future();
        std::promise<void> release_publication;
        auto release = release_publication.get_future().share();
        std::atomic<unsigned> effects{0};
        buffer->set_before_heartbeat_publication_hook_for_test([&]() {
            before_publication.set_value();
            release.wait();
        });
        buffer->set_heartbeat_effect_hook_for_test(
            [&]() { effects.fetch_add(1, std::memory_order_relaxed); });

        buffer->post_heartbeat_tick_for_test();
        catl::test::IoContextRunners runners(io, runner_count);
        auto const entered_status = entered.wait_for(5s);

        std::future<void> sealed;
        if (entered_status == std::future_status::ready)
        {
            sealed = std::async(std::launch::async, [buffer]() {
                buffer->seal_admission_and_join_worker();
            });
        }
        auto const sealed_status =
            sealed.valid() ? sealed.wait_for(5s) : std::future_status::deferred;

        std::promise<void> owner_crossed;
        auto crossed = owner_crossed.get_future();
        asio::post(buffer->owner_strand_for_test(), [&]() {
            owner_crossed.set_value();
        });
        release_publication.set_value();
        auto const crossed_status = crossed.wait_for(5s);

        if (sealed.valid() && sealed_status == std::future_status::ready)
            EXPECT_NO_THROW(sealed.get());

        work.reset();
        io.stop();
        runners.join();

        EXPECT_EQ(entered_status, std::future_status::ready);
        EXPECT_EQ(sealed_status, std::future_status::ready);
        EXPECT_EQ(crossed_status, std::future_status::ready);
        EXPECT_EQ(effects.load(std::memory_order_relaxed), 0u);
    }
}

// ─── Proof quorum at 80% (review controls) ─────────────────────────
//
// Proof anchors need ceil(80%) of the embedded list, matching the reference
// verifiers; live tracking stays at 90% and the full cache at 100%. These
// fail if the proof selectors fall back to the live 90%.

namespace {

using namespace std::chrono_literals;

struct ProofQuorumHarness
{
    static constexpr uint32_t kVlSequence = 7;

    asio::io_context io;
    catl::xdata::Protocol protocol = test_protocol();
    std::shared_ptr<xprv::ValidationQuorumTracker> tracker =
        xprv::ValidationQuorumTracker::create(io, protocol, 0);
    std::vector<catl::vl::Manifest> members;

    ProofQuorumHarness()
    {
        for (unsigned i = 1; i <= 35; ++i)
        {
            catl::vl::Manifest m;
            m.master_public_key = std::vector<uint8_t>(33, 0);
            m.master_public_key.front() = 0x02;
            m.master_public_key.back() = static_cast<uint8_t>(i);
            m.signing_public_key = m.master_public_key;
            m.signing_public_key.back() = static_cast<uint8_t>(i + 100);
            members.push_back(std::move(m));
        }
        tracker->set_unl(members, kVlSequence);
        io.run();
        io.restart();
    }

    static Hash256
    ledger_hash(uint8_t marker)
    {
        std::array<uint8_t, 32> bytes{};
        bytes.back() = marker;
        return Hash256(bytes);
    }

    asio::awaitable<void>
    vote(unsigned member, uint32_t seq = 1000, uint8_t marker = 1)
    {
        xprv::ValidationCollector::Entry e;
        e.ledger_hash = ledger_hash(marker);
        e.ledger_seq = seq;
        e.signing_key_hex = members.at(member).signing_key_hex();
        EXPECT_TRUE(co_await tracker->co_insert_validation_for_test(
            std::move(e)));
    }

    asio::awaitable<void>
    votes(unsigned count)
    {
        for (unsigned i = 0; i < count; ++i)
            co_await vote(i);
    }

    void
    run(asio::awaitable<void> op)
    {
        auto future = asio::co_spawn(io, std::move(op), asio::use_future);
        io.run();
        future.get();
        io.restart();
    }
};

xprv::ProofChain
load_chain_fixture()
{
    std::ifstream input(
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/proof.json");
    std::stringstream buffer;
    buffer << input.rdbuf();
    return xprv::from_json(boost::json::parse(buffer.str()));
}

std::vector<catl::vl::Manifest>
list_members(xprv::AnchorData const& anchor)
{
    std::vector<catl::vl::Manifest> result;
    auto const blob =
        boost::json::parse(std::string(anchor.blob.begin(), anchor.blob.end()))
            .as_object();
    for (auto const& entry : blob.at("validators").as_array())
    {
        result.push_back(catl::vl::parse_manifest(catl::base64_decode(
            std::string(entry.as_object().at("manifest").as_string()))));
    }
    return result;
}

asio::awaitable<void>
await_stats(
    std::shared_ptr<xprv::ValidationQuorumTracker> const& tracker,
    std::function<bool(xprv::ValidationQuorumTracker::Stats const&)> done)
{
    auto const deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (done(co_await tracker->co_stats()))
            co_return;
        co_await asio::post(asio::use_awaitable);
    }
    ADD_FAILURE() << "tracker did not reach the expected state in time";
}

}  // namespace

TEST(ProofQuorumAt80, Boundary27Fails28PassesLiveStays90FullStays100)
{
    ProofQuorumHarness h;
    constexpr auto vl = ProofQuorumHarness::kVlSequence;
    h.run([&]() -> asio::awaitable<void> {
        EXPECT_EQ(xprv::ValidationQuorumTracker::proof_quorum_threshold(35), 28u);
        co_await h.votes(27);
        EXPECT_FALSE(co_await h.tracker->co_latest_quorum_at_or_before(1000, vl));
        auto below = co_await h.tracker->co_wait_best_quorum(0s, vl);
        EXPECT_TRUE(below.quorum.validations.empty());

        co_await h.vote(27);
        auto selected = co_await h.tracker->co_latest_quorum_at_or_before(1000, vl);
        EXPECT_TRUE(selected.has_value());
        if (!selected)
            co_return;
        EXPECT_EQ(selected->validations.size(), 28u);
        EXPECT_FALSE(co_await h.tracker->co_latest_quorum());
        auto waited = co_await h.tracker->co_wait_best_quorum(0s, vl);
        EXPECT_EQ(waited.quorum.validations.size(), 28u);

        for (unsigned i = 28; i < 32; ++i)
            co_await h.vote(i);
        EXPECT_TRUE(co_await h.tracker->co_latest_quorum());
        EXPECT_FALSE(co_await h.tracker->co_last_full_quorum(10s, vl));

        for (unsigned i = 32; i < 35; ++i)
            co_await h.vote(i);
        auto full = co_await h.tracker->co_last_full_quorum(10s, vl);
        EXPECT_TRUE(full.has_value());
        if (full)
            EXPECT_EQ(full->validations.size(), 35u);
    }());
}

TEST(ProofQuorumAt80, ColdWaiterLocks30ThenCompletesOnNextLedger)
{
    ProofQuorumHarness h;
    constexpr auto vl = ProofQuorumHarness::kVlSequence;
    auto waiter = asio::co_spawn(
        h.io, h.tracker->co_wait_best_quorum(1s, vl), asio::use_future);
    h.run([&]() -> asio::awaitable<void> {
        while ((co_await h.tracker->co_stats()).pending_callbacks == 0)
            co_await asio::post(asio::use_awaitable);
        co_await h.votes(30);
        EXPECT_EQ((co_await h.tracker->co_stats()).pending_callbacks, 1u);
        EXPECT_FALSE(co_await h.tracker->co_latest_quorum());
        co_await h.vote(0, 1001, 2);
    }());
    auto result = waiter.get();
    EXPECT_EQ(result.stop_reason, xprv::QuorumCollectStopReason::next_ledger);
    EXPECT_EQ(result.quorum.validations.size(), 30u);
    EXPECT_EQ(result.quorum.ledger_seq, 1000u);
    EXPECT_EQ(result.quorum.vl_sequence, vl);
}

TEST(ProofQuorumAt80, WarmWaiterReturns30OnTimeoutOrSeenNextLedger)
{
    ProofQuorumHarness h;
    constexpr auto vl = ProofQuorumHarness::kVlSequence;
    h.run([&]() -> asio::awaitable<void> {
        co_await h.votes(30);
        auto timed = co_await h.tracker->co_wait_best_quorum(0s, vl);
        EXPECT_EQ(timed.stop_reason, xprv::QuorumCollectStopReason::timeout);
        EXPECT_EQ(timed.quorum.validations.size(), 30u);

        co_await h.vote(0, 1001, 2);
        auto next = co_await h.tracker->co_wait_best_quorum(0s, vl);
        EXPECT_EQ(next.stop_reason, xprv::QuorumCollectStopReason::next_ledger);
        EXPECT_EQ(next.quorum.validations.size(), 30u);
        EXPECT_FALSE(co_await h.tracker->co_latest_quorum_at_or_before(999, vl));
        EXPECT_FALSE(
            co_await h.tracker->co_latest_quorum_at_or_before(1000, vl + 1));
    }());
}

// Thirty real signed validations, admitted through on_packet under the
// fixture's 35-member list, give a 30-vote anchor that verifies.
TEST(ProofQuorumAt80, ThirtyRealSignedValidationsGiveAVerifiableAnchor)
{
    auto chain = load_chain_fixture();
    auto& anchor = std::get<xprv::AnchorData>(chain.steps.front());
    auto members = list_members(anchor);
    ASSERT_EQ(members.size(), 35u);
    ASSERT_GE(anchor.validations.size(), 30u);
    while (anchor.validations.size() > 30)
        anchor.validations.erase(std::prev(anchor.validations.end()));

    asio::io_context io;
    auto protocol = test_protocol();
    auto tracker = xprv::ValidationQuorumTracker::create(io, protocol, 0);
    tracker->set_unl(members, 7);
    io.run();
    io.restart();
    for (auto const& [_, raw] : anchor.validations)
    {
        tracker->on_packet(
            static_cast<uint16_t>(catl::peer_client::packet_type::validation),
            wrap_validation(raw));
    }

    auto future = asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            co_await await_stats(tracker, [](auto const& s) {
                return s.validation_packets_processed == 30 &&
                    s.queued_validations == 0;
            });
            auto q = co_await tracker->co_latest_quorum_at_or_before(
                anchor.ledger_index, 7);
            EXPECT_TRUE(q.has_value());
            if (q)
                EXPECT_EQ(q->validations.size(), 30u);
            auto collected = co_await tracker->co_wait_best_quorum(0s, 7);
            EXPECT_EQ(collected.quorum.validations.size(), 30u);
            co_await tracker->co_stop();
        },
        asio::use_future);
    io.run();
    future.get();
    EXPECT_TRUE(
        xprv::resolve_proof_chain(chain, protocol, anchor.publisher_key_hex));
}

// The live-only diagnostic reports a live quorum without a proof quorum only
// when proof-eligible votes fall below 80%: false at 30 of 35, true at 27.
TEST(ProofQuorumAt80, LiveOnlyDiagnosticUsesProof80AndLive90)
{
    auto const chain = load_chain_fixture();
    auto const& anchor = std::get<xprv::AnchorData>(chain.steps.front());
    auto const members = list_members(anchor);
    ASSERT_EQ(members.size(), 35u);

    std::vector<unsigned> voted;
    for (unsigned i = 0; i < members.size(); ++i)
    {
        auto const signing = members[i].signing_key_hex();
        if (std::any_of(
                anchor.validations.begin(),
                anchor.validations.end(),
                [&](auto const& entry) {
                    return lower_ascii(entry.first) == signing;
                }))
        {
            voted.push_back(i);
        }
    }
    ASSERT_GE(voted.size(), 32u);
    ASSERT_EQ(voted.size(), anchor.validations.size());

    for (unsigned proof_votes : {30u, 27u})
    {
        // Rotate members out of the list's view: the list keeps an older,
        // keyless manifest and peers relay the real (newer) one.
        auto stale = members;
        protocol::TMManifests relayed;
        for (unsigned j = 0; j < voted.size() - proof_votes; ++j)
        {
            auto const i = voted[j];
            ASSERT_GT(stale[i].sequence, 0u);
            stale[i].signing_public_key.clear();
            --stale[i].sequence;
            relayed.add_list()->set_stobject(
                std::string(members[i].raw.begin(), members[i].raw.end()));
        }
        std::string relayed_bytes;
        ASSERT_TRUE(relayed.SerializeToString(&relayed_bytes));

        asio::io_context io;
        auto protocol = test_protocol();
        auto tracker = xprv::ValidationQuorumTracker::create(io, protocol, 0);
        tracker->set_unl(stale, 7);
        io.run();
        io.restart();
        tracker->on_packet(
            static_cast<uint16_t>(catl::peer_client::packet_type::manifests),
            std::vector<uint8_t>(relayed_bytes.begin(), relayed_bytes.end()));

        auto future = asio::co_spawn(
            io,
            [&]() -> asio::awaitable<void> {
                co_await await_stats(tracker, [](auto const& s) {
                    return s.manifest_packets_processed == 1 &&
                        !s.manifest_in_flight && !s.manifest_pending;
                });
                // The newest manifest per listed master, including the
                // relayed rotations, is published for verifiers on this
                // server (§1.3 ambient material).
                auto const held = tracker->held_manifests();
                EXPECT_EQ(held->size(), members.size());
                for (unsigned j = 0; j < voted.size() - proof_votes; ++j)
                {
                    auto const& raw = members[voted[j]].raw;
                    EXPECT_NE(
                        std::find(held->begin(), held->end(), raw),
                        held->end());
                }
                for (auto const& [_, raw] : anchor.validations)
                {
                    tracker->on_packet(
                        static_cast<uint16_t>(
                            catl::peer_client::packet_type::validation),
                        wrap_validation(raw));
                }
                auto const expected = anchor.validations.size();
                co_await await_stats(tracker, [expected](auto const& s) {
                    return s.validation_packets_processed == expected &&
                        s.queued_validations == 0;
                });
                EXPECT_TRUE(co_await tracker->co_latest_quorum());
                // Best-effort trust view (§1.3): relayed newer manifests count
                // for proofs, so there is no live-only gap, and every vote
                // counts whether or not the list's own keys could explain it.
                EXPECT_FALSE(co_await tracker->co_has_live_only_quorum());
                auto proof = co_await tracker->co_latest_quorum_at_or_before(
                    anchor.ledger_index, 7);
                EXPECT_TRUE(proof.has_value());
                if (proof)
                    EXPECT_EQ(proof->validations.size(), voted.size());
                co_await tracker->co_stop();
            },
            asio::use_future);
        io.run();
        future.get();
    }
}

// astra A1: the held-manifest snapshot is republished when a list is applied,
// not only on peer packets, so a list-borne revocation reaches /verify.
TEST(ProofQuorumAt80, ListUpdateRepublishesHeldManifests)
{
    auto make = [](uint8_t signing, uint32_t sequence, uint8_t tag) {
        catl::vl::Manifest m;
        m.master_public_key = std::vector<uint8_t>(33, 0);
        m.master_public_key.front() = 0x02;
        if (signing != 0)
        {
            m.signing_public_key = std::vector<uint8_t>(33, 0);
            m.signing_public_key.front() = signing;
        }
        m.sequence = sequence;
        m.raw = {tag};
        return m;
    };
    auto const listed = make(0x03, 1, 0xA0);
    auto const revocation =
        make(0, std::numeric_limits<uint32_t>::max(), 0xC0);

    asio::io_context io;
    auto protocol = test_protocol();
    auto tracker = xprv::ValidationQuorumTracker::create(io, protocol, 0);
    tracker->set_unl({listed}, 7);
    io.run();
    io.restart();
    auto held = tracker->held_manifests();
    EXPECT_NE(
        std::find(held->begin(), held->end(), listed.raw), held->end());

    tracker->set_unl({revocation}, 8);
    io.run();
    io.restart();
    held = tracker->held_manifests();
    EXPECT_NE(
        std::find(held->begin(), held->end(), revocation.raw), held->end());
    EXPECT_EQ(
        std::find(held->begin(), held->end(), listed.raw), held->end());

    auto future = asio::co_spawn(io, tracker->co_stop(), asio::use_future);
    io.run();
    future.get();
}
