#include "peer-client-test-access.h"

#include "../test-support/io-context-runners.h"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <catl/peer-client/connection-types.h>
#include <catl/peer-client/endpoint-tracker.h>
#include <catl/peer-client/peer-client-connect-coro.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-client.h>
#include <catl/peer-client/peer-crawl-client.h>
#include <catl/peer-client/types.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <future>
#include <limits>
#include <stdexcept>
#include <thread>

namespace catl::peer_client {
TEST(PeerClientLifecycle, PublicCoConnectFunctionTypeRemainsStable)
{
    using PublicCoConnect = asio::awaitable<std::uint32_t> (*)(
        asio::io_context&,
        std::string const&,
        std::uint16_t,
        PeerClient::ConnectOptions,
        std::shared_ptr<PeerClient>&,
        int,
        std::function<void(std::shared_ptr<PeerClient> const&)>);

    PublicCoConnect function = &co_connect;
    EXPECT_NE(function, nullptr);
}

TEST(PeerClientTypes, ErrorEnumValues)
{
    EXPECT_EQ(static_cast<int>(Error::Success), 0);
    EXPECT_NE(static_cast<int>(Error::Timeout), 0);
    EXPECT_NE(static_cast<int>(Error::FeatureDisabled), 0);
}

TEST(PeerClientTypes, DefaultRequestOptions)
{
    RequestOptions opts;
    EXPECT_EQ(opts.timeout, std::chrono::seconds{5});
}

TEST(PeerClientStatusRange, PublicationIsOneCoherentFact)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    constexpr AdvertisedLedgerRange old_range{
        .first_seq = 100,
        .last_seq = 200,
    };
    constexpr AdvertisedLedgerRange new_range{
        .first_seq = 10'000,
        .last_seq = 20'000,
    };
    PeerClientTestAccess::publish_advertised_ledger_range(*client, old_range);

    std::atomic<bool> start = false;
    std::atomic<bool> done = false;
    std::thread writer([&] {
        while (!start.load(std::memory_order_acquire))
        {
        }
        for (int i = 0; i < 200'000; ++i)
        {
            PeerClientTestAccess::publish_advertised_ledger_range(
                *client, (i & 1) ? old_range : new_range);
        }
        done.store(true, std::memory_order_release);
    });

    bool synthesized = false;
    start.store(true, std::memory_order_release);
    while (!done.load(std::memory_order_acquire))
    {
        auto const observed = client->advertised_ledger_range();
        if (observed != old_range && observed != new_range)
        {
            synthesized = true;
            break;
        }
    }
    writer.join();

    EXPECT_FALSE(synthesized)
        << "readers must never combine endpoints from separate status updates";
}

TEST(PeerClientStatusRange, QueuedRevocationRefusesRawNodeWriteAtCommit)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    std::size_t writes = 0;
    client->raw_connection().set_write_initiator_for_test(
        [&writes](
            asio::const_buffer,
            peer_connection::test_write_completion complete) {
            ++writes;
            complete({});
        });

    std::vector<boost::system::error_code> errors;
    asio::post(client->strand(), [client] {
        PeerClientTestAccess::publish_advertised_ledger_range(
            *client, {.first_seq = 6'000, .last_seq = 7'000});
    });
    client->send_get_nodes(
        {},
        5'500,
        static_cast<int>(NodeTree::account_state),
        {SHAMapNodeID{}},
        [&errors](boost::system::error_code ec) { errors.push_back(ec); });

    io.run();

    EXPECT_EQ(writes, 0u);
    ASSERT_EQ(errors.size(), 1u);
    EXPECT_EQ(errors.front(), boost::asio::error::operation_aborted);
}

TEST(PeerClientStatusRange, QueuedRevocationRefusesManagedNodeWriteAtCommit)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    std::size_t writes = 0;
    client->raw_connection().set_write_initiator_for_test(
        [&writes](
            asio::const_buffer,
            peer_connection::test_write_completion complete) {
            ++writes;
            complete({});
        });

    std::vector<NodeBatchWriteReceipt> receipts;
    asio::post(client->strand(), [client] {
        PeerClientTestAccess::publish_advertised_ledger_range(
            *client, {.first_seq = 6'000, .last_seq = 7'000});
    });
    EXPECT_TRUE(client->submit_node_batch(
        PeerClientTestAccess::node_batch_command(
            41, 7, {}, NodeTree::account_state, {SHAMapNodeID{}}, 1, 5'500),
        [&receipts](NodeBatchWriteReceipt receipt) {
            receipts.push_back(std::move(receipt));
        }));

    io.run();

    EXPECT_EQ(writes, 0u);
    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(
        receipts.front().outcome(),
        NodeBatchWriteOutcome::ledger_not_advertised);
    EXPECT_EQ(receipts.front().receiver_commit_sequence(), 0u);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientStatusRange, QueuedRevocationRefusesHeaderWriteAtCommit)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    std::size_t writes = 0;
    client->raw_connection().set_write_initiator_for_test(
        [&writes](
            asio::const_buffer,
            peer_connection::test_write_completion complete) {
            ++writes;
            complete({});
        });

    std::vector<Error> completions;
    asio::post(client->strand(), [client] {
        PeerClientTestAccess::publish_advertised_ledger_range(
            *client, {.first_seq = 6'000, .last_seq = 7'000});
    });
    client->get_ledger_header(
        5'500, [&completions](Error error, LedgerHeaderResult) {
            completions.push_back(error);
        });

    io.run();

    EXPECT_EQ(writes, 0u);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions.front(), Error::NoLedger);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerConnectionFrames, ManifestDrainPolicyIsCompressionIndependent)
{
    packet_header header{
        .payload_size = kMaxBufferedManifestBytes + 1,
        .type = static_cast<std::uint16_t>(packet_type::manifests),
        .compressed = false,
        .uncompressed_size = kMaxBufferedManifestBytes + 1,
    };

    EXPECT_TRUE(should_drain_manifest_payload(header));

    header.compressed = true;
    EXPECT_TRUE(should_drain_manifest_payload(header));

    header.payload_size = kMaxBufferedManifestBytes;
    EXPECT_FALSE(should_drain_manifest_payload(header));

    header.payload_size = kMaxBufferedManifestBytes + 1;
    header.type = static_cast<std::uint16_t>(packet_type::validation);
    EXPECT_FALSE(should_drain_manifest_payload(header));
}

TEST(PeerClientTypes, PeerClientExceptionIncludesDetail)
{
    PeerClientException ex{
        Error::Disconnected, "http upgrade status=503 redirect_peers=4"};
    EXPECT_NE(
        std::string(ex.what()).find("http upgrade status=503"),
        std::string::npos);
    EXPECT_EQ(ex.detail, "http upgrade status=503 redirect_peers=4");
}

TEST(ConnectAttemptState, DeadlineAndCompletionAreFirstWins)
{
    asio::io_context io;
    detail::ConnectAttemptState deadline_first(io.get_executor());
    EXPECT_TRUE(deadline_first
                    .try_commit_initial(
                        {detail::ConnectAttemptTerminalKind::deadline,
                         asio::error::timed_out,
                         0,
                         10},
                        []() noexcept {})
                    .terminal_committed);
    EXPECT_FALSE(deadline_first
                     .try_commit_peer_terminal(
                         PeerLogicalTerminalCause::peer_failure,
                         {detail::ConnectAttemptTerminalKind::peer_failure,
                          asio::error::connection_reset,
                          0,
                          0},
                         []() noexcept {})
                     .terminal_committed);
    ASSERT_TRUE(deadline_first.snapshot());
    EXPECT_EQ(
        deadline_first.snapshot()->kind,
        detail::ConnectAttemptTerminalKind::deadline);
    EXPECT_EQ(
        deadline_first.terminal_cause(),
        PeerLogicalTerminalCause::peer_failure);

    detail::ConnectAttemptState completion_first(io.get_executor());
    EXPECT_TRUE(completion_first
                    .try_commit_peer_terminal(
                        PeerLogicalTerminalCause::peer_failure,
                        {detail::ConnectAttemptTerminalKind::peer_failure,
                         asio::error::connection_reset,
                         0,
                         0},
                        []() noexcept {})
                    .terminal_committed);
    EXPECT_FALSE(completion_first
                     .try_commit_initial(
                         {detail::ConnectAttemptTerminalKind::deadline,
                          asio::error::timed_out,
                          0,
                          10},
                         []() noexcept {})
                     .terminal_committed);
    ASSERT_TRUE(completion_first.snapshot());
    EXPECT_EQ(
        completion_first.snapshot()->kind,
        detail::ConnectAttemptTerminalKind::peer_failure);
}

TEST(ConnectAttemptState, EveryInitialOutcomePairRetainsTheFirstWinner)
{
    using Kind = detail::ConnectAttemptTerminalKind;
    auto const kinds = std::array{
        Kind::ready,
        Kind::peer_failure,
        Kind::owner_disconnect,
        Kind::deadline,
        Kind::caller_cancelled,
        Kind::infrastructure_failure};

    for (auto const first : kinds)
    {
        for (auto const second : kinds)
        {
            asio::io_context io;
            detail::ConnectAttemptState attempt(io.get_executor());
            auto commit = [&attempt](Kind kind) {
                auto terminal = detail::ConnectAttemptTerminal{kind, {}, 0, 0};
                if (kind == Kind::peer_failure ||
                    kind == Kind::owner_disconnect)
                {
                    auto const cause = kind == Kind::peer_failure
                        ? PeerLogicalTerminalCause::peer_failure
                        : PeerLogicalTerminalCause::owner_disconnect;
                    return attempt.try_commit_peer_terminal(
                        cause, terminal, []() noexcept {});
                }
                return attempt.try_commit_initial(terminal, []() noexcept {});
            };

            ASSERT_TRUE(commit(first).terminal_committed);
            EXPECT_FALSE(commit(second).terminal_committed);
            auto const terminal = attempt.snapshot();
            ASSERT_TRUE(terminal);
            EXPECT_EQ(terminal->kind, first);
        }
    }
}

TEST(ConnectAttemptState, ReadyAndLaterSessionCauseRemainDistinctAtHandoff)
{
    asio::io_context io;
    detail::ConnectAttemptState attempt(io.get_executor());
    ASSERT_TRUE(attempt
                    .try_commit_initial(
                        {detail::ConnectAttemptTerminalKind::ready, {}, 7, 0},
                        []() noexcept {})
                    .terminal_committed);
    auto const later = attempt.try_commit_peer_terminal(
        PeerLogicalTerminalCause::peer_failure,
        {detail::ConnectAttemptTerminalKind::peer_failure,
         asio::error::connection_reset,
         0,
         0},
        []() noexcept {});
    EXPECT_TRUE(later.event_committed);
    EXPECT_FALSE(later.terminal_committed);

    auto const handoff = attempt.snapshot_for_handoff();
    ASSERT_TRUE(handoff.initial_terminal);
    EXPECT_EQ(
        handoff.initial_terminal->kind,
        detail::ConnectAttemptTerminalKind::ready);
    EXPECT_EQ(handoff.initial_terminal->peer_seq, 7u);
    EXPECT_EQ(
        handoff.session_terminal_cause, PeerLogicalTerminalCause::peer_failure);
}

TEST(ConnectAttemptState, TerminalPublicationBeforeReceiveIsRetained)
{
    for (auto const runners_count : {1u, 2u, 4u})
    {
        asio::io_context io;
        auto attempt =
            std::make_shared<detail::ConnectAttemptState>(io.get_executor());
        ASSERT_TRUE(attempt
                        ->try_commit_initial(
                            {detail::ConnectAttemptTerminalKind::peer_failure,
                             asio::error::connection_reset,
                             0,
                             0},
                            []() noexcept {})
                        .terminal_committed);

        std::atomic<bool> received{false};
        std::exception_ptr failure;
        std::promise<void> completed;
        auto completed_future = completed.get_future();
        asio::co_spawn(
            io,
            [attempt, &received]() -> asio::awaitable<void> {
                auto const ec = co_await attempt->wait_terminal();
                if (ec)
                    throw boost::system::system_error(ec);
                received.store(true, std::memory_order_release);
            },
            [&failure, &completed](std::exception_ptr error) {
                failure = std::move(error);
                completed.set_value();
            });

        catl::test::IoContextRunners runners(io, runners_count);
        auto const status = completed_future.wait_for(std::chrono::seconds(1));
        if (status != std::future_status::ready)
            io.stop();
        runners.join();
        ASSERT_EQ(status, std::future_status::ready)
            << "sticky publication must have a finite failure oracle";
        EXPECT_EQ(failure, nullptr);
        EXPECT_TRUE(received.load(std::memory_order_acquire));
    }
}

TEST(ConnectAttemptState, DeadlineControllerPublishesOnRawRunnerModes)
{
    for (auto const runners_count : {1u, 2u, 4u})
    {
        asio::io_context io;
        auto client = PeerClientTestAccess::make(io, State::Connecting);
        auto attempt = client->initial_connect_attempt();
        auto deadline = std::make_shared<detail::ConnectDeadlineController>(
            io.get_executor(), attempt, client, 0);
        deadline->start();

        std::atomic<bool> received{false};
        std::exception_ptr failure;
        asio::co_spawn(
            io,
            [attempt, &received]() -> asio::awaitable<void> {
                auto const ec = co_await attempt->wait_terminal();
                if (ec)
                    throw boost::system::system_error(ec);
                received.store(true, std::memory_order_release);
            },
            [&failure](std::exception_ptr error) {
                failure = std::move(error);
            });

        catl::test::IoContextRunners runners(io, runners_count);
        runners.join();
        ASSERT_EQ(failure, nullptr);
        ASSERT_TRUE(received.load(std::memory_order_acquire));
        auto const terminal = attempt->snapshot();
        ASSERT_TRUE(terminal);
        EXPECT_EQ(terminal->kind, detail::ConnectAttemptTerminalKind::deadline);
        EXPECT_EQ(terminal->deadline_seconds, 0);
    }
}

TEST(ConnectAttemptState, FirstDeadlineGrantsPostHandshakeGrace)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Connected);
    auto attempt = client->initial_connect_attempt();
    auto deadline = std::make_shared<detail::ConnectDeadlineController>(
        io.get_executor(),
        attempt,
        client,
        0,
        std::chrono::milliseconds(1),
        15);

    deadline->start();
    ASSERT_EQ(io.poll_one(), 1u)
        << "the controller must first install its owner-strand timer";
    ASSERT_EQ(io.poll_one(), 1u)
        << "the first expiry must execute the grace decision";
    EXPECT_FALSE(attempt->snapshot())
        << "post-handshake grace is not a terminal connect result";

    io.restart();
    io.run_for(std::chrono::milliseconds(50));
    auto const terminal = attempt->snapshot();
    ASSERT_TRUE(terminal)
        << "the one-time grace must itself end at a bounded deadline";
    EXPECT_EQ(terminal->kind, detail::ConnectAttemptTerminalKind::deadline);
    EXPECT_EQ(terminal->deadline_seconds, 15);

    deadline->cancel();
    io.restart();
    io.run();
}

TEST(
    ConnectAttemptState,
    DeadlineArmAndGraceRearmFailuresPublishInfrastructureTerminal)
{
    for (bool const fail_initial_arm : {true, false})
    {
        asio::io_context io;
        auto client = PeerClientTestAccess::make(
            io, fail_initial_arm ? State::Connecting : State::Connected);
        auto attempt = client->initial_connect_attempt();
        std::vector<bool> arm_kinds;
        auto deadline = std::make_shared<detail::ConnectDeadlineController>(
            io.get_executor(),
            attempt,
            client,
            0,
            std::chrono::milliseconds(1),
            15,
            [&](bool allow_post_handshake_grace) {
                arm_kinds.push_back(allow_post_handshake_grace);
                if (allow_post_handshake_grace == fail_initial_arm)
                {
                    throw std::runtime_error(
                        fail_initial_arm ? "initial deadline arm failed"
                                         : "deadline grace rearm failed");
                }
            });

        deadline->start();
        io.run();

        auto const terminal = attempt->snapshot();
        ASSERT_TRUE(terminal);
        EXPECT_EQ(
            terminal->kind,
            detail::ConnectAttemptTerminalKind::infrastructure_failure);
        ASSERT_TRUE(terminal->failure);
        try
        {
            std::rethrow_exception(terminal->failure);
            FAIL() << "infrastructure terminal must retain the exact failure";
        }
        catch (std::runtime_error const& e)
        {
            EXPECT_STREQ(
                e.what(),
                fail_initial_arm ? "initial deadline arm failed"
                                 : "deadline grace rearm failed");
        }

        if (fail_initial_arm)
            EXPECT_EQ(arm_kinds, std::vector<bool>{true});
        else
            EXPECT_EQ(arm_kinds, (std::vector<bool>{true, false}));
    }
}

TEST(PeerClientLifecycle, CoConnectCallerCancellationDrainsPublishedClient)
{
    asio::io_context io;
    asio::cancellation_signal cancellation;
    std::shared_ptr<PeerClient> client;
    peer_connection::test_write_completion held_completion;
    std::optional<Error> observed_error;
    std::exception_ptr completion_failure;

    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            try
            {
                PeerClient::ConnectOptions options;
                co_await co_connect(
                    io,
                    "cancel-before-connect-wait.invalid",
                    443,
                    std::move(options),
                    client,
                    10,
                    [&](std::shared_ptr<PeerClient> const& created) {
                        asio::post(
                            created->strand(), [created, &held_completion] {
                                created->raw_connection()
                                    .set_write_initiator_for_test(
                                        [&held_completion](
                                            asio::const_buffer,
                                            peer_connection::
                                                test_write_completion
                                                    completion) {
                                            held_completion =
                                                std::move(completion);
                                        });
                                created->raw_connection().async_send_packet(
                                    packet_type::ping,
                                    std::vector<std::uint8_t>(64, 0xC7),
                                    [](boost::system::error_code) {});
                            });
                        cancellation.emit(asio::cancellation_type::all);
                    });
            }
            catch (PeerClientException const& e)
            {
                observed_error = e.error;
                EXPECT_FALSE(e.connect_disposition);
            }
        },
        asio::bind_cancellation_slot(
            cancellation.slot(),
            [&completion_failure](std::exception_ptr failure) {
                completion_failure = std::move(failure);
            }));

    io.run_for(std::chrono::milliseconds(50));
    io.restart();

    ASSERT_TRUE(held_completion);
    EXPECT_FALSE(observed_error)
        << "cancellation intent cannot escape before physical drain";
    EXPECT_FALSE(
        client->raw_connection().physical_transport_drained_for_test());

    asio::post(
        client->strand(), [completion = std::move(held_completion)]() mutable {
            completion(asio::error::operation_aborted);
        });
    io.run();

    EXPECT_EQ(completion_failure, nullptr);
    ASSERT_TRUE(client);
    ASSERT_TRUE(observed_error);
    EXPECT_EQ(*observed_error, Error::Cancelled);
    EXPECT_EQ(client->state(), State::Disconnected);
    EXPECT_TRUE(client->raw_connection().physical_transport_drained_for_test());
}

TEST(PeerClientLifecycle, CoConnectPublicationFailureDrainsPublishedClient)
{
    asio::io_context io;
    std::shared_ptr<PeerClient> client;
    peer_connection::test_write_completion held_completion;
    std::optional<std::string> observed_failure;
    std::exception_ptr completion_failure;

    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            try
            {
                PeerClient::ConnectOptions options;
                co_await co_connect(
                    io,
                    "publication-failure.invalid",
                    443,
                    std::move(options),
                    client,
                    10,
                    [&held_completion](
                        std::shared_ptr<PeerClient> const& created) {
                        asio::post(
                            created->strand(), [created, &held_completion] {
                                created->raw_connection()
                                    .set_write_initiator_for_test(
                                        [&held_completion](
                                            asio::const_buffer,
                                            peer_connection::
                                                test_write_completion
                                                    completion) {
                                            held_completion =
                                                std::move(completion);
                                        });
                                created->raw_connection().async_send_packet(
                                    packet_type::ping,
                                    std::vector<std::uint8_t>(64, 0xC7),
                                    [](boost::system::error_code) {});
                            });
                        throw std::runtime_error("publication refused");
                    });
            }
            catch (std::runtime_error const& e)
            {
                observed_failure = e.what();
            }
        },
        [&completion_failure](std::exception_ptr failure) {
            completion_failure = std::move(failure);
        });

    io.run_for(std::chrono::milliseconds(50));
    io.restart();

    ASSERT_TRUE(held_completion);
    EXPECT_FALSE(observed_failure)
        << "setup failure cannot escape before the exact physical receipt";
    EXPECT_FALSE(
        client->raw_connection().physical_transport_drained_for_test());

    asio::post(
        client->strand(), [completion = std::move(held_completion)]() mutable {
            completion(asio::error::operation_aborted);
        });
    io.run();

    EXPECT_EQ(completion_failure, nullptr);
    ASSERT_TRUE(client);
    ASSERT_TRUE(observed_failure);
    EXPECT_EQ(*observed_failure, "publication refused");
    EXPECT_EQ(client->state(), State::Disconnected);
    EXPECT_TRUE(client->raw_connection().physical_transport_drained_for_test());
}

TEST(PeerClientLifecycle, CoConnectDeadlineArmFailureDrainsPublishedClient)
{
    asio::io_context io;
    std::shared_ptr<PeerClient> client;
    peer_connection::test_write_completion held_completion;
    std::optional<std::string> observed_failure;
    std::exception_ptr completion_failure;
    detail::ConnectAttemptSetupHooks hooks;
    hooks.before_deadline_arm = [](bool) {
        throw std::runtime_error("deadline arm refused");
    };

    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            try
            {
                PeerClient::ConnectOptions options;
                co_await detail::co_connect_with_setup_hooks(
                    io,
                    "deadline-arm-failure.invalid",
                    443,
                    std::move(options),
                    client,
                    10,
                    [&held_completion](
                        std::shared_ptr<PeerClient> const& created) {
                        asio::post(
                            created->strand(), [created, &held_completion] {
                                created->raw_connection()
                                    .set_write_initiator_for_test(
                                        [&held_completion](
                                            asio::const_buffer,
                                            peer_connection::
                                                test_write_completion
                                                    completion) {
                                            held_completion =
                                                std::move(completion);
                                        });
                                created->raw_connection().async_send_packet(
                                    packet_type::ping,
                                    std::vector<std::uint8_t>(64, 0xC8),
                                    [](boost::system::error_code) {});
                            });
                    },
                    std::move(hooks));
            }
            catch (std::runtime_error const& e)
            {
                observed_failure = e.what();
            }
        },
        [&completion_failure](std::exception_ptr failure) {
            completion_failure = std::move(failure);
        });

    io.run_for(std::chrono::milliseconds(50));
    io.restart();

    ASSERT_TRUE(held_completion);
    EXPECT_FALSE(observed_failure)
        << "controller failure cannot escape before the physical receipt";
    EXPECT_FALSE(
        client->raw_connection().physical_transport_drained_for_test());

    asio::post(
        client->strand(), [completion = std::move(held_completion)]() mutable {
            completion(asio::error::operation_aborted);
        });
    io.run();

    EXPECT_EQ(completion_failure, nullptr);
    ASSERT_TRUE(client);
    ASSERT_TRUE(observed_failure);
    EXPECT_EQ(*observed_failure, "deadline arm refused");
    EXPECT_EQ(client->state(), State::Disconnected);
    EXPECT_TRUE(client->raw_connection().physical_transport_drained_for_test());
}

TEST(PeerClientLifecycle, EveryPostPublicationSetupFailureDrainsAndRethrows)
{
    for (int stage = 0; stage != 3; ++stage)
    {
        asio::io_context io;
        std::shared_ptr<PeerClient> client;
        std::optional<std::string> observed_failure;
        std::exception_ptr completion_failure;

        detail::ConnectAttemptSetupHooks hooks;
        auto const message = "setup stage " + std::to_string(stage);
        auto throw_stage = [message] { throw std::runtime_error(message); };
        if (stage == 0)
            hooks.before_deadline_construction = throw_stage;
        else if (stage == 1)
            hooks.before_deadline_start = throw_stage;
        else
            hooks.before_terminal_wait = throw_stage;

        asio::co_spawn(
            io,
            [&]() -> asio::awaitable<void> {
                try
                {
                    PeerClient::ConnectOptions options;
                    co_await detail::co_connect_with_setup_hooks(
                        io,
                        "setup-failure.invalid",
                        443,
                        std::move(options),
                        client,
                        10,
                        {},
                        std::move(hooks));
                }
                catch (std::runtime_error const& e)
                {
                    observed_failure = e.what();
                }
            },
            [&completion_failure](std::exception_ptr failure) {
                completion_failure = std::move(failure);
            });

        io.run();

        EXPECT_EQ(completion_failure, nullptr) << "stage " << stage;
        ASSERT_TRUE(client) << "stage " << stage;
        ASSERT_TRUE(observed_failure) << "stage " << stage;
        EXPECT_EQ(*observed_failure, message) << "stage " << stage;
        EXPECT_EQ(client->state(), State::Disconnected) << "stage " << stage;
        EXPECT_TRUE(
            client->raw_connection().physical_transport_drained_for_test())
            << "stage " << stage;
    }
}

TEST(PeerClientLifecycle, ReadyObserversRunBeforeStickyPublication)
{
    asio::io_context io;
    std::shared_ptr<PeerClient> client;
    std::vector<std::string> observer_order;
    std::optional<std::uint32_t> result;
    std::exception_ptr completion_failure;

    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            PeerClient::ConnectOptions options;
            result = co_await co_connect(
                io,
                "ready-observer-order.invalid",
                443,
                std::move(options),
                client,
                10,
                [&](std::shared_ptr<PeerClient> const& created) {
                    PeerClientTestAccess::set_initial_connect_observers(
                        *created,
                        [&, created](std::uint32_t) {
                            observer_order.emplace_back("ready");
                            created->disconnect();
                        },
                        [&](boost::system::error_code ec, std::uint32_t) {
                            EXPECT_FALSE(ec);
                            observer_order.emplace_back("complete");
                        });
                    asio::post(created->strand(), [created] {
                        PeerClientTestAccess::become_ready_on_strand(*created);
                    });
                });
        },
        [&completion_failure](std::exception_ptr failure) {
            completion_failure = std::move(failure);
        });

    io.run();

    EXPECT_EQ(completion_failure, nullptr);
    ASSERT_TRUE(result);
    EXPECT_EQ(*result, 0u);
    EXPECT_EQ(observer_order, (std::vector<std::string>{"ready", "complete"}));
    ASSERT_TRUE(client);
    auto const terminal =
        PeerClientTestAccess::initial_connect_terminal(*client);
    ASSERT_TRUE(terminal);
    EXPECT_EQ(terminal->kind, detail::ConnectAttemptTerminalKind::ready);
    EXPECT_EQ(
        client->terminal_cause(), PeerLogicalTerminalCause::owner_disconnect);
}

TEST(PeerClientLifecycle, ReadyObserverReleasesCaptureWhenConsumed)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::ExchangingStatus);
    auto capture = std::make_shared<int>(42);
    std::weak_ptr<int> weak_capture = capture;
    PeerClientTestAccess::set_initial_connect_observers(
        *client,
        [capture](std::uint32_t) {},
        [](boost::system::error_code, std::uint32_t) {});
    capture.reset();

    asio::post(client->strand(), [client] {
        PeerClientTestAccess::become_ready_on_strand(*client);
    });
    io.run();

    EXPECT_TRUE(weak_capture.expired())
        << "the consumed observer slot must release its captures";
}

TEST(PeerClientLifecycle, ThrowingReadyObserversCannotEraseReadyTerminal)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::ExchangingStatus);
    std::size_t ready_calls = 0;
    std::size_t completion_calls = 0;
    PeerClientTestAccess::set_initial_connect_observers(
        *client,
        [&ready_calls](std::uint32_t) {
            ++ready_calls;
            throw std::runtime_error("ready observer failure");
        },
        [&completion_calls](boost::system::error_code, std::uint32_t) {
            ++completion_calls;
            throw std::runtime_error("completion observer failure");
        });

    asio::post(client->strand(), [client] {
        PeerClientTestAccess::become_ready_on_strand(*client);
    });
    EXPECT_NO_THROW(io.run());

    auto const terminal =
        PeerClientTestAccess::initial_connect_terminal(*client);
    ASSERT_TRUE(terminal);
    EXPECT_EQ(terminal->kind, detail::ConnectAttemptTerminalKind::ready);
    EXPECT_EQ(client->state(), State::Ready);
    EXPECT_EQ(completion_calls, 1u);
    EXPECT_EQ(ready_calls, 1u);
}

TEST(PeerClientTypes, SummarizeCrawlErrorStripsBoostSourceNoise)
{
    EXPECT_EQ(
        summarize_crawl_error(
            "Connection refused [system:61 at /tmp/file.hpp:97:37 in "
            "function 'foo']"),
        "Connection refused");
    EXPECT_EQ(
        summarize_crawl_error("Host not found (authoritative) [asio.netdb:1]"),
        "Host not found (authoritative)");
    EXPECT_EQ(
        summarize_crawl_error("crawl returned HTTP 500"),
        "crawl returned HTTP 500");
    EXPECT_EQ(
        summarize_crawl_error(
            "crawl https-read: The socket was closed due to a timeout "
            "[boost.beast:1 at /tmp/file.hpp:123]"),
        "crawl https-read: The socket was closed due to a timeout");
    EXPECT_EQ(
        summarize_crawl_error(
            "crawl fallback failed: https=crawl https-connect: Host not found "
            "(authoritative); http=crawl http-connect: Connection refused"),
        "crawl fallback failed: https=crawl https-connect: Host not found "
        "(authoritative); http=crawl http-connect: Connection refused");
}

TEST(PeerClientTypes, CanonicalEndpointNormalizesMappedIpv4AndIpv6)
{
    EXPECT_EQ(
        EndpointTracker::canonical_endpoint("[::ffff:192.0.2.10]:51235"),
        "192.0.2.10:51235");
    EXPECT_EQ(
        EndpointTracker::canonical_endpoint("[2001:db8::1]:51235"),
        "[2001:db8::1]:51235");
}

TEST(
    PeerClientTypes,
    NormalizeDiscoveredEndpointRejectsPrivateAndSpecialLiterals)
{
    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_endpoint("10.179.19.37:2459")
            .has_value());
    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_endpoint("192.168.1.8:51235")
            .has_value());
    EXPECT_FALSE(EndpointTracker::normalize_discovered_endpoint("[::1]:51235")
                     .has_value());
    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_endpoint("[fc00::1]:51235")
            .has_value());
}

TEST(
    PeerClientTypes,
    NormalizeDiscoveredEndpointAcceptsPublicLiteralsAndHostnames)
{
    auto const public_ip =
        EndpointTracker::normalize_discovered_endpoint("54.37.85.99:21337");
    ASSERT_TRUE(public_ip.has_value());
    EXPECT_EQ(*public_ip, "54.37.85.99:21337");

    auto const hostname = EndpointTracker::normalize_discovered_endpoint(
        "hub.xrpl-commons.org:51235");
    ASSERT_TRUE(hostname.has_value());
    EXPECT_EQ(*hostname, "hub.xrpl-commons.org:51235");

    auto const repaired = EndpointTracker::normalize_discovered_endpoint(
        "217.182.197.33:21337:21337");
    ASSERT_TRUE(repaired.has_value());
    EXPECT_EQ(*repaired, "217.182.197.33:21337");
}

TEST(
    PeerClientTypes,
    NormalizeDiscoveredHostUsesSuppliedPortAndFiltersPrivateLiterals)
{
    auto const public_ip =
        EndpointTracker::normalize_discovered_host("54.37.85.99", 21337);
    ASSERT_TRUE(public_ip.has_value());
    EXPECT_EQ(*public_ip, "54.37.85.99:21337");

    auto const public_ipv6 = EndpointTracker::normalize_discovered_host(
        "2606:4700:4700::1111", 51235);
    ASSERT_TRUE(public_ipv6.has_value());
    EXPECT_EQ(*public_ipv6, "[2606:4700:4700::1111]:51235");

    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_host("10.179.19.37", 2459)
            .has_value());
    EXPECT_FALSE(EndpointTracker::normalize_discovered_host("fc00::1", 51235)
                     .has_value());

    auto const prequalified = EndpointTracker::normalize_discovered_host(
        "217.182.197.33:21337", 21337);
    ASSERT_TRUE(prequalified.has_value());
    EXPECT_EQ(*prequalified, "217.182.197.33:21337");
}

TEST(PeerClientLifecycle, SubmissionThenDisconnectDrainsDeferredRequest)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Connecting);
    std::vector<Error> completions;

    client->get_ledger_header(
        1, [&completions](Error error, LedgerHeaderResult) {
            completions.push_back(error);
        });
    client->disconnect();

    io.run();

    ASSERT_EQ(completions.size(), 1);
    EXPECT_EQ(completions.front(), Error::Cancelled);
    EXPECT_EQ(client->state(), State::Disconnected);
    EXPECT_EQ(client->pending_count(), 0);
    EXPECT_EQ(PeerClientTestAccess::deferred_count(*client), 0);
}

TEST(PeerClientLifecycle, DisconnectThenSubmissionRejectsWithoutQueueing)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Connecting);
    std::vector<Error> completions;

    client->disconnect();
    client->get_ledger_header(
        1, [&completions](Error error, LedgerHeaderResult) {
            completions.push_back(error);
        });

    io.run();

    ASSERT_EQ(completions.size(), 1);
    EXPECT_EQ(completions.front(), Error::Cancelled);
    EXPECT_EQ(client->pending_count(), 0);
    EXPECT_EQ(PeerClientTestAccess::deferred_count(*client), 0);
}

TEST(PeerClientLifecycle, FailedLogicalStateStillClosesPhysicalTransport)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Failed);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    ASSERT_TRUE(client->raw_connection().socket().lowest_layer().is_open());

    client->disconnect();
    io.run();

    EXPECT_EQ(client->state(), State::Disconnected);
    EXPECT_FALSE(client->raw_connection().socket().lowest_layer().is_open());
}

TEST(
    PeerClientLifecycle,
    PublicConnectOwnsSynchronousResolveInitiationFailureOnStrand)
{
    asio::io_context io;
    std::shared_ptr<PeerClient> client;
    bool observed_installed_authority = false;
    bool completion_on_strand = false;
    std::vector<boost::system::error_code> completions;

    PeerClient::ConnectOptions opts;
    opts.on_complete = [&](boost::system::error_code ec, std::uint32_t) {
        completion_on_strand = client->strand().running_in_this_thread();
        completions.push_back(ec);
    };

    // The public entry runs on this ordinary test thread, before io.run().
    client = PeerClient::connect(
        io, "resolver-initiation.invalid", 443, std::move(opts));
    ASSERT_FALSE(client->strand().running_in_this_thread());
    client->raw_connection().set_before_resolve_initiation_for_test([&] {
        observed_installed_authority = true;
        EXPECT_TRUE(client->strand().running_in_this_thread());
        EXPECT_EQ(
            client->raw_connection().transport_operation_count_for_test(), 1u);
        EXPECT_TRUE(client->raw_connection().resolver_installed_for_test());
        throw std::runtime_error("forced resolver initiation failure");
    });

    io.run();

    ASSERT_TRUE(observed_installed_authority);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_TRUE(completions.front());
    EXPECT_TRUE(completion_on_strand);
    EXPECT_EQ(client->state(), State::Failed);
    EXPECT_EQ(
        client->raw_connection().transport_operation_count_for_test(), 0u);
    EXPECT_FALSE(client->raw_connection().resolver_installed_for_test());
    EXPECT_FALSE(
        client->raw_connection().physical_transport_drained_for_test());

    int joins = 0;
    std::exception_ptr join_failure;
    asio::co_spawn(
        io,
        [client]() -> asio::awaitable<void> {
            co_await client->co_disconnect();
        },
        [&](std::exception_ptr failure) {
            join_failure = std::move(failure);
            ++joins;
        });
    io.restart();
    io.run();

    EXPECT_EQ(join_failure, nullptr);
    EXPECT_EQ(joins, 1);
    EXPECT_TRUE(client->raw_connection().physical_transport_drained_for_test());
}

TEST(PeerClientLifecycle, CoDisconnectJoinsOnePhysicalReceiptForAllCallers)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    peer_connection::test_write_completion held_completion;
    client->raw_connection().set_write_initiator_for_test(
        [&held_completion](
            asio::const_buffer,
            peer_connection::test_write_completion completion) {
            held_completion = std::move(completion);
        });

    std::vector<boost::system::error_code> write_results;
    asio::post(client->strand(), [client, &write_results] {
        client->raw_connection().async_send_packet(
            packet_type::ping,
            std::vector<std::uint8_t>(64, 0xAC),
            [&write_results](boost::system::error_code ec) {
                write_results.push_back(ec);
            });
    });
    io.run();

    ASSERT_TRUE(held_completion);
    ASSERT_EQ(
        client->raw_connection().transport_operation_count_for_test(), 1u);
    ASSERT_FALSE(
        client->raw_connection().physical_transport_drained_for_test());

    int joins = 0;
    std::vector<std::exception_ptr> failures;
    auto launch_join = [&] {
        asio::co_spawn(
            io,
            [client]() -> asio::awaitable<void> {
                co_await client->co_disconnect();
            },
            [&joins, &failures](std::exception_ptr failure) {
                if (failure)
                    failures.push_back(failure);
                else
                    ++joins;
            });
    };

    launch_join();
    launch_join();
    asio::cancellation_signal cancellation;
    bool cancellation_join_done = false;
    std::exception_ptr cancellation_failure;
    asio::co_spawn(
        io,
        [client]() -> asio::awaitable<void> {
            co_await client->co_disconnect();
        },
        asio::bind_cancellation_slot(
            cancellation.slot(),
            [&cancellation_join_done,
             &cancellation_failure](std::exception_ptr failure) {
                cancellation_failure = std::move(failure);
                cancellation_join_done = true;
            }));
    io.restart();
    io.poll();

    EXPECT_EQ(client->state(), State::Disconnected);
    EXPECT_FALSE(client->raw_connection().socket().lowest_layer().is_open());
    EXPECT_EQ(write_results.size(), 1u);
    EXPECT_EQ(write_results.front(), asio::error::operation_aborted);
    EXPECT_EQ(
        client->raw_connection().transport_operation_count_for_test(), 1u);
    EXPECT_FALSE(
        client->raw_connection().physical_transport_drained_for_test());
    EXPECT_EQ(joins, 0);
    EXPECT_TRUE(failures.empty());

    cancellation.emit(asio::cancellation_type::all);
    io.restart();
    io.poll();
    EXPECT_FALSE(cancellation_join_done)
        << "caller cancellation must not abandon the physical receipt";

    auto completion = std::move(held_completion);
    asio::post(client->strand(), [completion = std::move(completion)] {
        completion(asio::error::operation_aborted);
    });
    io.restart();
    io.run();

    EXPECT_EQ(
        client->raw_connection().transport_operation_count_for_test(), 0u);
    EXPECT_TRUE(client->raw_connection().physical_transport_drained_for_test());
    EXPECT_EQ(joins, 2);
    EXPECT_TRUE(failures.empty());
    EXPECT_TRUE(cancellation_join_done);
    EXPECT_EQ(cancellation_failure, nullptr);

    launch_join();
    io.restart();
    io.run();
    EXPECT_EQ(joins, 3);
    EXPECT_TRUE(failures.empty());
}

class PeerClientPhysicalRunnerModes
    : public ::testing::TestWithParam<std::size_t>
{
};

TEST(IoContextRunners, PartialConstructionNeverStartsIo)
{
    asio::io_context io;
    std::atomic<bool> handler_ran{false};
    asio::post(io, [&handler_ran] {
        handler_ran.store(true, std::memory_order_release);
    });

    std::size_t attempts = 0;
    catl::test::IoContextRunners::RunnerFactory make_runner =
        [&attempts](std::function<void()> run) -> std::thread {
        ++attempts;
        if (attempts == 2)
            throw std::runtime_error("expected thread creation refusal");
        return std::thread(std::move(run));
    };

    EXPECT_THROW(
        (catl::test::IoContextRunners(io, 2, make_runner)), std::runtime_error);
    EXPECT_EQ(attempts, 2u);
    EXPECT_FALSE(handler_ran.load(std::memory_order_acquire));

    io.poll();
    EXPECT_TRUE(handler_ran.load(std::memory_order_acquire));
}

TEST_P(
    PeerClientPhysicalRunnerModes,
    HeldPhysicalCompletionDominatesConcurrentDisconnectCallers)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    peer_connection::test_write_completion held_completion;
    client->raw_connection().set_write_initiator_for_test(
        [&held_completion](
            asio::const_buffer,
            peer_connection::test_write_completion completion) {
            held_completion = std::move(completion);
        });

    std::promise<boost::system::error_code> logical_close;
    auto logical_close_future = logical_close.get_future();
    asio::post(client->strand(), [client, &logical_close] {
        client->raw_connection().async_send_packet(
            packet_type::ping,
            std::vector<std::uint8_t>(64, 0xBD),
            [&logical_close](boost::system::error_code error) {
                logical_close.set_value(error);
            });
    });
    io.run();

    ASSERT_TRUE(held_completion);
    ASSERT_EQ(
        client->raw_connection().transport_operation_count_for_test(), 1u);
    ASSERT_FALSE(
        client->raw_connection().physical_transport_drained_for_test());

    constexpr std::size_t caller_count = 8;
    std::atomic<std::size_t> joined{0};
    std::atomic<std::size_t> failed{0};
    std::promise<void> all_callers_waiting;
    auto all_callers_waiting_future = all_callers_waiting.get_future();
    client->raw_connection()
        .set_after_physical_drain_waiter_registered_for_test(
            [&all_callers_waiting](std::size_t count) {
                if (count == caller_count)
                    all_callers_waiting.set_value();
            });
    for (std::size_t index = 0; index < caller_count; ++index)
    {
        asio::co_spawn(
            io,
            [client]() -> asio::awaitable<void> {
                co_await client->co_disconnect();
            },
            [&joined, &failed](std::exception_ptr failure) {
                if (failure)
                    failed.fetch_add(1, std::memory_order_acq_rel);
                else
                    joined.fetch_add(1, std::memory_order_acq_rel);
            });
    }

    io.restart();
    catl::test::IoContextRunners runners(io, GetParam());
    auto const all_waiting =
        all_callers_waiting_future.wait_for(std::chrono::seconds(2)) ==
        std::future_status::ready;
    if (!all_waiting)
    {
        io.stop();
        runners.join();
    }
    ASSERT_TRUE(all_waiting);

    io.stop();
    runners.join();
    client->raw_connection()
        .set_after_physical_drain_waiter_registered_for_test({});
    io.restart();
    while (io.poll() != 0)
    {
    }

    ASSERT_EQ(
        logical_close_future.wait_for(std::chrono::seconds(0)),
        std::future_status::ready);
    EXPECT_EQ(logical_close_future.get(), asio::error::operation_aborted);
    EXPECT_EQ(joined.load(std::memory_order_acquire), 0u)
        << "logical close cannot counterfeit the held physical receipt";
    EXPECT_EQ(failed.load(std::memory_order_acquire), 0u);
    EXPECT_EQ(
        client->raw_connection().transport_operation_count_for_test(), 1u);
    EXPECT_FALSE(
        client->raw_connection().physical_transport_drained_for_test());

    auto completion = std::move(held_completion);
    asio::post(
        client->strand(), [completion = std::move(completion)]() mutable {
            completion(asio::error::operation_aborted);
        });
    catl::test::IoContextRunners release_runners(io, GetParam());
    release_runners.join();

    EXPECT_EQ(joined.load(std::memory_order_acquire), caller_count);
    EXPECT_EQ(failed.load(std::memory_order_acquire), 0u);
    EXPECT_EQ(
        client->raw_connection().transport_operation_count_for_test(), 0u);
    EXPECT_TRUE(client->raw_connection().physical_transport_drained_for_test());
}

TEST_P(
    PeerClientPhysicalRunnerModes,
    OracleExposesDetachedPhysicalWaitBeforeReceipt)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    peer_connection::test_write_completion held_completion;
    client->raw_connection().set_write_initiator_for_test(
        [&held_completion](
            asio::const_buffer,
            peer_connection::test_write_completion completion) {
            held_completion = std::move(completion);
        });
    asio::post(client->strand(), [client] {
        client->raw_connection().async_send_packet(
            packet_type::ping,
            std::vector<std::uint8_t>(64, 0xBD),
            [](boost::system::error_code) {});
    });
    io.run();
    ASSERT_TRUE(held_completion);

    constexpr std::size_t caller_count = 8;
    std::promise<void> all_waiters_registered;
    auto all_waiters_registered_future = all_waiters_registered.get_future();
    client->raw_connection()
        .set_after_physical_drain_waiter_registered_for_test(
            [&all_waiters_registered](std::size_t count) {
                if (count == caller_count)
                    all_waiters_registered.set_value();
            });

    std::atomic<std::size_t> returned_before_receipt{0};
    std::atomic<std::size_t> failures{0};
    auto connection = client->raw_connection().shared_from_this();
    for (std::size_t index = 0; index < caller_count; ++index)
    {
        asio::co_spawn(
            io,
            [connection, &failures]() -> asio::awaitable<void> {
                auto executor = co_await asio::this_coro::executor;
                asio::co_spawn(
                    executor,
                    connection->co_close(),
                    [&failures](std::exception_ptr failure) {
                        if (failure)
                            failures.fetch_add(1, std::memory_order_acq_rel);
                    });
                co_return;
            },
            [&returned_before_receipt, &failures](std::exception_ptr failure) {
                if (failure)
                    failures.fetch_add(1, std::memory_order_acq_rel);
                returned_before_receipt.fetch_add(1, std::memory_order_acq_rel);
            });
    }

    io.restart();
    catl::test::IoContextRunners runners(io, GetParam());
    auto const all_registered =
        all_waiters_registered_future.wait_for(std::chrono::seconds(2)) ==
        std::future_status::ready;
    if (!all_registered)
    {
        io.stop();
        runners.join();
    }
    ASSERT_TRUE(all_registered);

    io.stop();
    runners.join();
    client->raw_connection()
        .set_after_physical_drain_waiter_registered_for_test({});
    io.restart();
    while (io.poll() != 0)
    {
    }

    EXPECT_EQ(
        returned_before_receipt.load(std::memory_order_acquire), caller_count)
        << "the production control's pre-receipt zero assertion must reject "
           "this detached-wait mutant";
    EXPECT_EQ(failures.load(std::memory_order_acquire), 0u);
    EXPECT_EQ(
        client->raw_connection().transport_operation_count_for_test(), 1u);
    EXPECT_FALSE(
        client->raw_connection().physical_transport_drained_for_test());

    auto completion = std::move(held_completion);
    asio::post(
        client->strand(), [completion = std::move(completion)]() mutable {
            completion(asio::error::operation_aborted);
        });
    catl::test::IoContextRunners release_runners(io, GetParam());
    release_runners.join();

    EXPECT_EQ(
        client->raw_connection().transport_operation_count_for_test(), 0u);
    EXPECT_TRUE(client->raw_connection().physical_transport_drained_for_test());
    EXPECT_EQ(failures.load(std::memory_order_acquire), 0u);
}

INSTANTIATE_TEST_SUITE_P(
    OneTwoAndFourRunners,
    PeerClientPhysicalRunnerModes,
    ::testing::Values(1u, 2u, 4u));

TEST(PeerClientLifecycle, FailureTransitionCannotOverwriteTerminalState)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Disconnected);
    std::vector<std::optional<State>> previous;

    asio::post(client->strand(), [client, &previous] {
        previous.push_back(
            PeerClientTestAccess::commit_failed_on_strand(*client));
    });
    io.run();

    ASSERT_EQ(previous.size(), 1u);
    EXPECT_FALSE(previous.front());
    EXPECT_EQ(client->state(), State::Disconnected);

    client = PeerClientTestAccess::make(io, State::Ready);
    io.restart();
    asio::post(client->strand(), [client, &previous] {
        previous.push_back(
            PeerClientTestAccess::commit_failed_on_strand(*client));
    });
    io.run();

    ASSERT_EQ(previous.size(), 2u);
    ASSERT_TRUE(previous.back());
    EXPECT_EQ(*previous.back(), State::Ready);
    EXPECT_EQ(client->state(), State::Failed);
}

TEST(PeerClientLifecycle, FirstLogicalTerminalCauseSurvivesPhysicalClose)
{
    asio::io_context io;
    auto failure_first = PeerClientTestAccess::make(io, State::Connecting);
    asio::post(failure_first->strand(), [failure_first] {
        EXPECT_TRUE(
            PeerClientTestAccess::commit_failed_on_strand(*failure_first));
        PeerClientTestAccess::disconnect_on_strand(*failure_first);
    });
    io.run();

    EXPECT_EQ(failure_first->state(), State::Disconnected);
    EXPECT_EQ(
        failure_first->terminal_cause(),
        PeerLogicalTerminalCause::peer_failure);

    auto owner_first = PeerClientTestAccess::make(io, State::Connecting);
    io.restart();
    asio::post(owner_first->strand(), [owner_first] {
        PeerClientTestAccess::disconnect_on_strand(*owner_first);
        EXPECT_FALSE(
            PeerClientTestAccess::commit_failed_on_strand(*owner_first));
    });
    io.run();

    EXPECT_EQ(owner_first->state(), State::Disconnected);
    EXPECT_EQ(
        owner_first->terminal_cause(),
        PeerLogicalTerminalCause::owner_disconnect);
}

TEST(PeerClientLifecycle, FailedClientRejectsWithoutQueueing)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Failed);
    std::vector<Error> completions;

    client->get_ledger_header(
        1, [&completions](Error error, LedgerHeaderResult) {
            completions.push_back(error);
        });

    io.run();

    ASSERT_EQ(completions.size(), 1);
    EXPECT_EQ(completions.front(), Error::Disconnected);
    EXPECT_EQ(client->pending_count(), 0);
    EXPECT_EQ(PeerClientTestAccess::deferred_count(*client), 0);
}

TEST(
    PeerClientNodeBatchWrite,
    PublicationIsQueuedAndWrongGenerationRefusesExactly)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    EXPECT_TRUE(client->submit_node_batch(
        PeerClientTestAccess::node_batch_command(41, 8),
        [&receipts](NodeBatchWriteReceipt receipt) {
            receipts.push_back(std::move(receipt));
        }));
    EXPECT_TRUE(receipts.empty());

    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(receipts.front().attempt_id().value(), 41u);
    EXPECT_EQ(receipts.front().peer_generation().value(), 8u);
    EXPECT_EQ(
        receipts.front().outcome(), NodeBatchWriteOutcome::generation_mismatch);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, PreReadyStateRefusesWithoutInstallingState)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::ExchangingStatus, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    EXPECT_TRUE(client->submit_node_batch(
        PeerClientTestAccess::node_batch_command(42, 7),
        [&receipts](NodeBatchWriteReceipt receipt) {
            receipts.push_back(std::move(receipt));
        }));

    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(receipts.front().outcome(), NodeBatchWriteOutcome::not_ready);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, PublicIngressProjectionIsBoundedBeforeStrandRuns)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteOutcome> outcomes;

    for (std::size_t i = 0; i < kMaxNodeBatchIngressProjections; ++i)
    {
        EXPECT_TRUE(client->submit_node_batch(
            PeerClientTestAccess::node_batch_command(10000 + i, 7),
            [&outcomes](NodeBatchWriteReceipt receipt) {
                outcomes.push_back(receipt.outcome());
            }));
    }
    EXPECT_EQ(client->pending_count(), kMaxNodeBatchIngressProjections);
    EXPECT_FALSE(client->submit_node_batch(
        PeerClientTestAccess::node_batch_command(20000, 7),
        [&outcomes](NodeBatchWriteReceipt receipt) {
            outcomes.push_back(receipt.outcome());
        }));

    io.run();

    ASSERT_EQ(outcomes.size(), kMaxNodeBatchIngressProjections);
    EXPECT_TRUE(std::all_of(
        outcomes.begin(), outcomes.end(), [](NodeBatchWriteOutcome outcome) {
            return outcome == NodeBatchWriteOutcome::disconnected;
        }));
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, InvalidCommandIsNotPublished)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::size_t receipts = 0;

    EXPECT_FALSE(client->submit_node_batch(
        PeerClientTestAccess::node_batch_command(0, 7),
        [&receipts](NodeBatchWriteReceipt) { ++receipts; }));

    io.run();
    EXPECT_EQ(receipts, 0u);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, OversizedNodeVectorIsRejectedBeforeIngress)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::size_t receipts = 0;
    std::vector<SHAMapNodeID> node_ids(
        kMaxNodeBatchRequestNodes + 1, SHAMapNodeID{});

    EXPECT_FALSE(client->submit_node_batch(
        PeerClientTestAccess::node_batch_command(
            40, 7, {}, NodeTree::transaction, std::move(node_ids)),
        [&receipts](NodeBatchWriteReceipt) { ++receipts; }));

    EXPECT_EQ(client->pending_count(), 0u);
    io.run();
    EXPECT_EQ(receipts, 0u);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(
    PeerClientNodeBatchWrite,
    ActiveAttemptJoinsExactDuplicateAndRejectsIdentityConflict)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    std::vector<NodeBatchWriteReceipt> original;
    std::vector<NodeBatchWriteReceipt> duplicate;
    std::vector<NodeBatchWriteOutcome> conflict;
    std::vector<NodeBatchWriteOutcome> sequence_conflict;
    asio::post(
        client->strand(),
        [client, &original, &duplicate, &conflict, &sequence_conflict]() {
            auto command = PeerClientTestAccess::node_batch_command(43, 7);
            PeerClientTestAccess::seed_pending_node_batch(
                *client, command, [&original](NodeBatchWriteReceipt receipt) {
                    original.push_back(std::move(receipt));
                });

            PeerClientTestAccess::submit_node_batch_on_strand(
                *client, command, [&duplicate](NodeBatchWriteReceipt receipt) {
                    duplicate.push_back(std::move(receipt));
                });
            PeerClientTestAccess::submit_node_batch_on_strand(
                *client,
                PeerClientTestAccess::node_batch_command(
                    43, 7, {}, NodeTree::account_state),
                [&conflict](NodeBatchWriteReceipt receipt) {
                    conflict.push_back(receipt.outcome());
                });
            PeerClientTestAccess::submit_node_batch_on_strand(
                *client,
                PeerClientTestAccess::node_batch_command(
                    43, 7, {}, NodeTree::transaction, {SHAMapNodeID{}}, 1, 2),
                [&sequence_conflict](NodeBatchWriteReceipt receipt) {
                    sequence_conflict.push_back(receipt.outcome());
                });

            client->disconnect();
        });

    io.run();

    ASSERT_EQ(conflict.size(), 1u);
    EXPECT_EQ(conflict.front(), NodeBatchWriteOutcome::identity_conflict);
    ASSERT_EQ(sequence_conflict.size(), 1u);
    EXPECT_EQ(
        sequence_conflict.front(), NodeBatchWriteOutcome::identity_conflict);
    ASSERT_EQ(duplicate.size(), 1u);
    EXPECT_EQ(duplicate.front().outcome(), NodeBatchWriteOutcome::cancelled);
    ASSERT_EQ(original.size(), 1u);
    EXPECT_EQ(original.front().outcome(), NodeBatchWriteOutcome::cancelled);
    EXPECT_EQ(client->pending_count(), 1u);

    EXPECT_TRUE(client->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(original.front())));
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, TerminalDrainMakesLateWriteCallbackInert)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        PeerClientTestAccess::seed_pending_node_batch(
            *client,
            PeerClientTestAccess::node_batch_command(44, 7),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(client->pending_count(), 1u);

        client->disconnect();
        PeerClientTestAccess::complete_node_batch_write(*client, 44);
    });

    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(receipts.front().outcome(), NodeBatchWriteOutcome::cancelled);
    EXPECT_EQ(client->pending_count(), 1u);

    EXPECT_TRUE(client->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(receipts.front())));
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, WriteCompletionPublishesOneExactReceipt)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        auto command = PeerClientTestAccess::node_batch_command(45, 7);
        auto const digest = command.payload_digest();
        PeerClientTestAccess::seed_pending_node_batch(
            *client,
            std::move(command),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });

        PeerClientTestAccess::complete_node_batch_write(*client, 45);
        PeerClientTestAccess::complete_node_batch_write(*client, 45);

        ASSERT_EQ(receipts.size(), 1u);
        EXPECT_EQ(receipts.front().payload_digest(), digest);
        EXPECT_TRUE(client->acknowledge_node_batch_receipt(
            PeerClientTestAccess::acknowledgment(receipts.front())));
    });

    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(receipts.front().attempt_id().value(), 45u);
    EXPECT_EQ(receipts.front().peer_generation().value(), 7u);
    EXPECT_EQ(receipts.front().outcome(), NodeBatchWriteOutcome::written);
    EXPECT_FALSE(receipts.front().error());
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, TerminalReceiptReplaysUntilExactAck)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        auto command = PeerClientTestAccess::node_batch_command(46, 7);
        PeerClientTestAccess::seed_pending_node_batch(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*client, 46);
        ASSERT_EQ(client->pending_count(), 1u);

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(client->pending_count(), 1u);
        ASSERT_EQ(receipts.size(), 2u);
        EXPECT_EQ(receipts[0].outcome(), receipts[1].outcome());
        EXPECT_EQ(receipts[0].payload_digest(), receipts[1].payload_digest());

        EXPECT_TRUE(client->acknowledge_node_batch_receipt(
            PeerClientTestAccess::acknowledgment(receipts.back())));
    });

    io.run();

    ASSERT_EQ(receipts.size(), 2u);
    EXPECT_EQ(receipts.front().outcome(), NodeBatchWriteOutcome::written);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(
    PeerClientNodeBatchWrite,
    TerminalReceiptSurvivesDisconnectAndReplaysBeforeAck)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;
    auto command = PeerClientTestAccess::node_batch_command(460, 7);

    asio::post(client->strand(), [client, command, &receipts]() mutable {
        PeerClientTestAccess::seed_pending_node_batch(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*client, 460);
        client->disconnect();

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
    });

    io.run();

    ASSERT_EQ(receipts.size(), 2u);
    EXPECT_EQ(receipts[0].outcome(), NodeBatchWriteOutcome::written);
    EXPECT_EQ(receipts[1].outcome(), receipts[0].outcome());
    EXPECT_EQ(
        receipts[1].receiver_commit_sequence(),
        receipts[0].receiver_commit_sequence());
    EXPECT_EQ(client->pending_count(), 1u);

    EXPECT_TRUE(client->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(receipts.back())));
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, TerminalIdentityConflictCannotReplaceReceipt)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        PeerClientTestAccess::seed_pending_node_batch(
            *client,
            PeerClientTestAccess::node_batch_command(49, 7),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*client, 49);

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client,
            PeerClientTestAccess::node_batch_command(
                49, 7, {}, NodeTree::account_state),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(receipts.size(), 2u);
        EXPECT_EQ(
            receipts.back().outcome(),
            NodeBatchWriteOutcome::identity_conflict);

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client,
            PeerClientTestAccess::node_batch_command(49, 7),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(receipts.size(), 3u);
        EXPECT_EQ(receipts.back().outcome(), NodeBatchWriteOutcome::written);
        EXPECT_TRUE(client->acknowledge_node_batch_receipt(
            PeerClientTestAccess::acknowledgment(receipts.back())));
    });

    io.run();

    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, WrongGenerationAckCannotRetireTerminalReceipt)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        auto command = PeerClientTestAccess::node_batch_command(50, 7);
        PeerClientTestAccess::seed_pending_node_batch(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*client, 50);

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client,
            PeerClientTestAccess::node_batch_command(50, 8),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(receipts.size(), 2u);
        EXPECT_EQ(
            receipts.back().outcome(),
            NodeBatchWriteOutcome::generation_mismatch);
        EXPECT_EQ(receipts.back().receiver_commit_sequence(), 0u);

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
    });

    io.run();

    ASSERT_EQ(receipts.size(), 3u);
    EXPECT_EQ(receipts.back().outcome(), NodeBatchWriteOutcome::written);
    EXPECT_EQ(client->pending_count(), 1u);

    EXPECT_TRUE(client->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(receipts.back())));
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, ForeignReceiverAckCannotRetireMatchingRow)
{
    asio::io_context io;
    auto first = PeerClientTestAccess::make(io, State::Ready, 7);
    auto second = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> first_receipts;
    std::vector<NodeBatchWriteReceipt> second_receipts;

    asio::post(first->strand(), [first, &first_receipts]() {
        PeerClientTestAccess::seed_pending_node_batch(
            *first,
            PeerClientTestAccess::node_batch_command(500, 7),
            [&first_receipts](NodeBatchWriteReceipt receipt) {
                first_receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*first, 500);
    });
    asio::post(second->strand(), [second, &second_receipts]() {
        PeerClientTestAccess::seed_pending_node_batch(
            *second,
            PeerClientTestAccess::node_batch_command(500, 7),
            [&second_receipts](NodeBatchWriteReceipt receipt) {
                second_receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*second, 500);
    });
    io.run();

    ASSERT_EQ(first_receipts.size(), 1u);
    ASSERT_EQ(second_receipts.size(), 1u);
    EXPECT_NE(
        first_receipts.front().receiver_instance_id(),
        second_receipts.front().receiver_instance_id());
    EXPECT_TRUE(second->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(first_receipts.front())));
    io.restart();
    io.run();
    EXPECT_EQ(second->pending_count(), 1u);

    EXPECT_TRUE(first->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(first_receipts.front())));
    EXPECT_TRUE(second->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(second_receipts.front())));
    io.restart();
    io.run();
    EXPECT_EQ(first->pending_count(), 0u);
    EXPECT_EQ(second->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, AckIngressIsBoundedBeforeStrandRuns)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        PeerClientTestAccess::seed_pending_node_batch(
            *client,
            PeerClientTestAccess::node_batch_command(505, 7),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*client, 505);
    });
    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    auto const ack = PeerClientTestAccess::acknowledgment(receipts.front());
    for (std::size_t i = 0; i < kMaxNodeBatchIngressProjections; ++i)
        EXPECT_TRUE(client->acknowledge_node_batch_receipt(ack));
    EXPECT_EQ(client->pending_count(), kMaxNodeBatchIngressProjections + 1);
    EXPECT_FALSE(client->acknowledge_node_batch_receipt(ack));

    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, WriteFailureReceiptReplaysExactly)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        auto command = PeerClientTestAccess::node_batch_command(51, 7);
        PeerClientTestAccess::seed_pending_node_batch(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(
            *client, 51, boost::asio::error::connection_reset);

        client->disconnect();
        PeerClientTestAccess::submit_node_batch_on_strand(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(receipts.size(), 2u);
        EXPECT_EQ(receipts[0].outcome(), NodeBatchWriteOutcome::write_failed);
        EXPECT_EQ(receipts[1].outcome(), receipts[0].outcome());
        EXPECT_EQ(receipts[1].error(), receipts[0].error());
        EXPECT_TRUE(client->acknowledge_node_batch_receipt(
            PeerClientTestAccess::acknowledgment(receipts.back())));
    });

    io.run();

    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, PendingReceiptReservationPreventsSequenceWrap)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);
    std::vector<NodeBatchWriteReceipt> terminal;
    std::vector<NodeBatchWriteReceipt> refused;

    asio::post(client->strand(), [client, &terminal, &refused]() {
        PeerClientTestAccess::seed_receipt_sequence(
            *client, std::numeric_limits<std::uint64_t>::max() - 1);
        PeerClientTestAccess::seed_pending_node_batch(
            *client,
            PeerClientTestAccess::node_batch_command(510, 7),
            [&terminal](NodeBatchWriteReceipt receipt) {
                terminal.push_back(std::move(receipt));
            });

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client,
            PeerClientTestAccess::node_batch_command(511, 7),
            [&refused](NodeBatchWriteReceipt receipt) {
                refused.push_back(std::move(receipt));
            });
        ASSERT_EQ(refused.size(), 1u);
        EXPECT_EQ(
            refused.front().outcome(),
            NodeBatchWriteOutcome::receipt_sequence_exhausted);
        EXPECT_EQ(refused.front().receiver_commit_sequence(), 0u);

        PeerClientTestAccess::complete_node_batch_write(*client, 510);
    });

    io.run();

    ASSERT_EQ(terminal.size(), 1u);
    EXPECT_EQ(
        terminal.front().receiver_commit_sequence(),
        std::numeric_limits<std::uint64_t>::max());
    EXPECT_TRUE(client->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(terminal.front())));
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(
    PeerClientNodeBatchWrite,
    DuplicateWaiterOverflowCannotPublishAContradictoryTerminal)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    auto command = PeerClientTestAccess::node_batch_command(520, 7);
    std::vector<NodeBatchWriteReceipt> receipts;
    std::size_t overflow_completions = 0;

    asio::post(
        client->strand(),
        [client, command, &receipts, &overflow_completions]() mutable {
            PeerClientTestAccess::seed_pending_node_batch(
                *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                    receipts.push_back(std::move(receipt));
                });
            for (std::size_t i = 1; i < kMaxNodeBatchReceiptWaiters; ++i)
            {
                PeerClientTestAccess::submit_node_batch_on_strand(
                    *client,
                    command,
                    [&receipts](NodeBatchWriteReceipt receipt) {
                        receipts.push_back(std::move(receipt));
                    });
            }
            PeerClientTestAccess::submit_node_batch_on_strand(
                *client,
                command,
                [&overflow_completions](NodeBatchWriteReceipt) {
                    ++overflow_completions;
                });

            PeerClientTestAccess::complete_node_batch_write(*client, 520);
            ASSERT_EQ(receipts.size(), kMaxNodeBatchReceiptWaiters);
            EXPECT_EQ(overflow_completions, 0u);
            EXPECT_TRUE(std::all_of(
                receipts.begin(),
                receipts.end(),
                [](NodeBatchWriteReceipt const& receipt) {
                    return receipt.outcome() == NodeBatchWriteOutcome::written;
                }));

            PeerClientTestAccess::submit_node_batch_on_strand(
                *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                    receipts.push_back(std::move(receipt));
                });
            ASSERT_EQ(receipts.size(), kMaxNodeBatchReceiptWaiters + 1);
            EXPECT_TRUE(client->acknowledge_node_batch_receipt(
                PeerClientTestAccess::acknowledgment(receipts.back())));
        });

    io.run();

    EXPECT_EQ(overflow_completions, 0u);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, ThrowingHandlerLeavesReceiptReplayable)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        auto command = PeerClientTestAccess::node_batch_command(47, 7);
        PeerClientTestAccess::seed_pending_node_batch(
            *client, command, [](NodeBatchWriteReceipt) {
                throw std::runtime_error("expected test throw");
            });
        PeerClientTestAccess::submit_node_batch_on_strand(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*client, 47);
        ASSERT_EQ(receipts.size(), 1u);
        EXPECT_TRUE(client->acknowledge_node_batch_receipt(
            PeerClientTestAccess::acknowledgment(receipts.front())));
    });

    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(receipts.front().outcome(), NodeBatchWriteOutcome::written);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, RealQueuedWriteIsCancelledBeforeSocketClose)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        PeerClientTestAccess::submit_node_batch_on_strand(
            *client,
            PeerClientTestAccess::node_batch_command(48, 7),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(client->pending_count(), 1u);
        client->disconnect();
    });

    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(receipts.front().outcome(), NodeBatchWriteOutcome::cancelled);
    EXPECT_EQ(client->pending_count(), 1u);

    EXPECT_TRUE(client->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(receipts.front())));
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(
    PeerClientNodeBatchWrite,
    ReceiptRowsAreBoundedAndSurviveTransportCloseUntilAck)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);
    std::vector<NodeBatchWriteReceipt> drained;
    std::vector<NodeBatchWriteOutcome> refused;

    asio::post(client->strand(), [client, &drained, &refused]() {
        for (std::size_t i = 0; i < kMaxNodeBatchReceiptRows; ++i)
        {
            PeerClientTestAccess::seed_pending_node_batch(
                *client,
                PeerClientTestAccess::node_batch_command(1000 + i, 7),
                [&drained](NodeBatchWriteReceipt receipt) {
                    if (receipt.outcome() == NodeBatchWriteOutcome::cancelled)
                        drained.push_back(std::move(receipt));
                });
        }
        ASSERT_EQ(client->pending_count(), kMaxNodeBatchReceiptRows);

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client,
            PeerClientTestAccess::node_batch_command(9999, 7),
            [&refused](NodeBatchWriteReceipt receipt) {
                refused.push_back(receipt.outcome());
            });
        ASSERT_EQ(refused.size(), 1u);
        EXPECT_EQ(
            refused.front(), NodeBatchWriteOutcome::receipt_capacity_exhausted);
        EXPECT_EQ(client->pending_count(), kMaxNodeBatchReceiptRows);

        client->disconnect();
    });

    io.run();

    ASSERT_EQ(drained.size(), kMaxNodeBatchReceiptRows);
    EXPECT_EQ(client->pending_count(), kMaxNodeBatchReceiptRows);

    for (auto const& receipt : drained)
    {
        EXPECT_TRUE(client->acknowledge_node_batch_receipt(
            PeerClientTestAccess::acknowledgment(receipt)));
    }
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientLifecycle, DisconnectConsumesPendingCallbackBeforeTimer)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    std::vector<Error> completions;

    asio::post(client->strand(), [client, &completions]() {
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_header(
            *client, [&completions](Error error, LedgerHeaderResult) {
                completions.push_back(error);
            }));
        client->disconnect();
    });

    io.run();

    ASSERT_EQ(completions.size(), 1);
    EXPECT_EQ(completions.front(), Error::Cancelled);
    EXPECT_EQ(client->pending_count(), 0);
}

TEST(PeerClientLifecycle, ResponseThenDisconnectCompletesExactlyOnce)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    std::vector<Error> completions;

    asio::post(client->strand(), [client, &completions]() {
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_header(
            *client, [&completions](Error error, LedgerHeaderResult) {
                completions.push_back(error);
            }));
        PeerClientTestAccess::resolve_pending_header(*client);
        client->disconnect();
    });

    io.run();

    ASSERT_EQ(completions.size(), 1);
    EXPECT_EQ(completions.front(), Error::Success);
    EXPECT_EQ(client->pending_count(), 0);
}

TEST(PeerClientLifecycle, IncarnationExhaustionRefusesBeforeInstallingState)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    std::vector<Error> completions;

    asio::post(client->strand(), [client, &completions]() {
        PeerClientTestAccess::seed_incarnations(
            *client,
            std::numeric_limits<std::uint64_t>::max(),
            std::numeric_limits<std::uint64_t>::max());

        EXPECT_FALSE(PeerClientTestAccess::seed_pending_header(
            *client, [&completions](Error error, LedgerHeaderResult) {
                completions.push_back(error);
            }));
        EXPECT_EQ(client->pending_count(), 0);
    });

    io.run();

    ASSERT_EQ(completions.size(), 1);
    EXPECT_EQ(completions.front(), Error::ResourceExhausted);
    EXPECT_EQ(client->pending_count(), 0);
}

TEST(PeerClientLifecycle, CallbackExhaustionCannotMutateSharedEntry)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    std::vector<Error> first_completions;
    std::vector<Error> refused_completions;

    asio::post(
        client->strand(), [client, &first_completions, &refused_completions]() {
            ASSERT_TRUE(PeerClientTestAccess::seed_pending_header(
                *client, [&first_completions](Error error, LedgerHeaderResult) {
                    first_completions.push_back(error);
                }));
            ASSERT_EQ(client->pending_count(), 1);

            PeerClientTestAccess::seed_incarnations(
                *client, 2, std::numeric_limits<std::uint64_t>::max());
            EXPECT_FALSE(PeerClientTestAccess::seed_pending_header(
                *client,
                [&refused_completions](Error error, LedgerHeaderResult) {
                    refused_completions.push_back(error);
                }));
            EXPECT_EQ(client->pending_count(), 1);

            PeerClientTestAccess::resolve_pending_header(*client);
        });

    io.run();

    ASSERT_EQ(refused_completions.size(), 1);
    EXPECT_EQ(refused_completions.front(), Error::ResourceExhausted);
    ASSERT_EQ(first_completions.size(), 1);
    EXPECT_EQ(first_completions.front(), Error::Success);
    EXPECT_EQ(client->pending_count(), 0);
}

TEST(PeerClientLifecycle, OverlappingNonDedupedKeyRefusesWithoutOverwrite)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    std::vector<Error> first_completions;
    std::vector<Error> second_completions;

    asio::post(
        client->strand(), [client, &first_completions, &second_completions]() {
            ASSERT_TRUE(PeerClientTestAccess::seed_pending_header(
                *client, [&first_completions](Error error, LedgerHeaderResult) {
                    first_completions.push_back(error);
                }));
            EXPECT_FALSE(PeerClientTestAccess::seed_pending_header(
                *client,
                [&second_completions](Error error, LedgerHeaderResult) {
                    second_completions.push_back(error);
                },
                std::chrono::seconds{30},
                false));
            client->disconnect();
        });

    io.run();

    ASSERT_EQ(first_completions.size(), 1);
    EXPECT_EQ(first_completions.front(), Error::Cancelled);
    ASSERT_EQ(second_completions.size(), 1);
    EXPECT_EQ(second_completions.front(), Error::DuplicateRequest);
    EXPECT_EQ(client->pending_count(), 0);
}

TEST(PeerClientLifecycle, ReentrantTerminalDrainDuringFlushIsExactlyOnce)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Connecting);
    std::vector<Error> first_completions;
    std::vector<Error> second_completions;

    asio::post(
        client->strand(), [client, &first_completions, &second_completions]() {
            PeerClientTestAccess::defer(
                *client, [client, &first_completions]() {
                    EXPECT_TRUE(PeerClientTestAccess::seed_pending_header(
                        *client,
                        [&first_completions](Error error, LedgerHeaderResult) {
                            first_completions.push_back(error);
                        },
                        std::chrono::seconds{30},
                        false));
                });
            PeerClientTestAccess::defer(
                *client, [client, &second_completions]() {
                    EXPECT_FALSE(PeerClientTestAccess::seed_pending_header(
                        *client,
                        [client, &second_completions](
                            Error error, LedgerHeaderResult) {
                            second_completions.push_back(error);
                            client->disconnect();
                        },
                        std::chrono::seconds{30},
                        false));
                });

            ASSERT_EQ(PeerClientTestAccess::deferred_count(*client), 2u);
            PeerClientTestAccess::become_ready_and_flush(*client);
        });

    io.run();

    ASSERT_EQ(first_completions.size(), 1u);
    EXPECT_EQ(first_completions.front(), Error::Cancelled);
    ASSERT_EQ(second_completions.size(), 1u);
    EXPECT_EQ(second_completions.front(), Error::DuplicateRequest);
    EXPECT_EQ(client->state(), State::Disconnected);
    EXPECT_EQ(client->pending_count(), 0u);
    EXPECT_EQ(PeerClientTestAccess::deferred_count(*client), 0u);
}

}  // namespace catl::peer_client
